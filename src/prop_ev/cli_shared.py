"""Shared CLI helper types and utility functions."""

from __future__ import annotations

import argparse
import re
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from prop_ev.cli_config import (
    env_bool_from_runtime,
    env_float_from_runtime,
    env_int_from_runtime,
)
from prop_ev.cli_config import (
    resolve_bookmakers as _resolve_bookmakers_impl,
)
from prop_ev.cli_config import (
    runtime_nba_data_dir as _runtime_nba_data_dir_impl,
)
from prop_ev.cli_config import (
    runtime_odds_api_default_max_credits as _runtime_odds_api_default_max_credits_impl,
)
from prop_ev.cli_config import (
    runtime_odds_data_dir as _runtime_odds_data_dir_impl,
)
from prop_ev.cli_config import (
    runtime_runtime_dir as _runtime_runtime_dir_impl,
)
from prop_ev.cli_config import (
    runtime_strategy_probabilistic_profile as _runtime_strategy_probabilistic_profile_impl,
)
from prop_ev.cli_data_helpers import (
    build_status_summary_payload as _build_status_summary_payload_impl,
)
from prop_ev.cli_data_helpers import (
    dataset_day_names as _dataset_day_names_impl,
)
from prop_ev.cli_data_helpers import (
    dataset_days_dir as _dataset_days_dir_impl,
)
from prop_ev.cli_data_helpers import (
    dataset_dir as _dataset_dir_impl,
)
from prop_ev.cli_data_helpers import (
    dataset_root as _dataset_root_impl,
)
from prop_ev.cli_data_helpers import (
    dataset_spec_from_payload as _dataset_spec_from_payload_impl,
)
from prop_ev.cli_data_helpers import (
    dataset_spec_path as _dataset_spec_path_impl,
)
from prop_ev.cli_data_helpers import (
    day_row_from_status as _day_row_from_status_impl,
)
from prop_ev.cli_data_helpers import (
    discover_dataset_ids as _discover_dataset_ids_impl,
)
from prop_ev.cli_data_helpers import (
    incomplete_reason_code as _incomplete_reason_code_impl,
)
from prop_ev.cli_data_helpers import (
    load_dataset_spec_or_error as _load_dataset_spec_or_error_impl,
)
from prop_ev.cli_data_helpers import (
    load_day_status_for_dataset as _load_day_status_for_dataset_impl,
)
from prop_ev.cli_data_helpers import (
    load_json_object as _load_json_object_impl,
)
from prop_ev.cli_data_helpers import (
    parse_allow_incomplete_days as _parse_allow_incomplete_days_impl,
)
from prop_ev.cli_data_helpers import (
    parse_allow_incomplete_reasons as _parse_allow_incomplete_reasons_impl,
)
from prop_ev.cli_data_helpers import (
    print_day_rows as _print_day_rows_impl,
)
from prop_ev.cli_data_helpers import (
    print_warnings as _print_warnings_impl,
)
from prop_ev.cli_internal import default_window
from prop_ev.odds_client import (
    OddsResponse,
    parse_csv,
)
from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.errors import OfflineCacheMiss, SpendBlockedError
from prop_ev.odds_data.policy import SpendPolicy
from prop_ev.odds_data.repo import OddsRepository
from prop_ev.odds_data.request import OddsRequest
from prop_ev.odds_data.spec import DatasetSpec
from prop_ev.storage import SnapshotStore
from prop_ev.strategies import get_strategy, resolve_strategy_id
from prop_ev.strategies.base import (
    normalize_strategy_id,
)
from prop_ev.time_utils import iso_z, utc_now


class CLIError(RuntimeError):
    """User-facing CLI error."""


class CreditLimitError(CLIError):
    """Raised when estimated credits exceed configured cap."""


class OfflineCacheMissError(CLIError):
    """Raised when offline mode is active and cache is missing."""


def _resolve_bookmakers(explicit: str, *, allow_config: bool = True) -> tuple[str, str]:
    return _resolve_bookmakers_impl(explicit, allow_config=allow_config)


def _utc_now() -> datetime:
    return utc_now()


def _iso(dt: datetime) -> str:
    return iso_z(dt)


def _default_window() -> tuple[str, str]:
    return default_window()


def _runtime_odds_data_dir() -> str:
    return _runtime_odds_data_dir_impl()


def _runtime_nba_data_dir() -> str:
    return _runtime_nba_data_dir_impl()


def _runtime_runtime_dir() -> str:
    return _runtime_runtime_dir_impl()


def _runtime_strategy_probabilistic_profile() -> str:
    return _runtime_strategy_probabilistic_profile_impl()


def _runtime_odds_api_default_max_credits() -> int:
    return _runtime_odds_api_default_max_credits_impl()


def _env_bool(name: str, default: bool) -> bool:
    return env_bool_from_runtime(name, default)


def _env_int(name: str, default: int) -> int:
    return env_int_from_runtime(name, default)


def _env_float(name: str, default: float) -> float:
    return env_float_from_runtime(name, default)


def _resolve_strategy_id(raw: str, *, default_id: str) -> str:
    requested = raw.strip() if isinstance(raw, str) else ""
    candidate = requested or default_id.strip() or "s001"
    plugin = get_strategy(resolve_strategy_id(candidate))
    return normalize_strategy_id(plugin.info.id)


def _quota_from_headers(headers: dict[str, str]) -> dict[str, str]:
    return {
        "remaining": headers.get("x-requests-remaining", ""),
        "used": headers.get("x-requests-used", ""),
        "last": headers.get("x-requests-last", ""),
    }


def _print_estimate(estimate: int, max_credits: int) -> None:
    print(f"estimated_credits={estimate} max_credits={max_credits}")


def _enforce_credit_cap(estimate: int, max_credits: int, force: bool) -> None:
    if estimate > max_credits and not force:
        raise CreditLimitError(
            f"estimated credits {estimate} exceed max {max_credits}; use --force to proceed"
        )


def _execute_request(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    label: str,
    path: str,
    params: dict[str, Any],
    fetcher: Callable[[], OddsResponse],
    offline: bool,
    block_paid: bool,
    is_paid: bool,
    refresh: bool,
    resume: bool,
) -> tuple[Any, dict[str, str], str, str]:
    repo = OddsRepository(store=store, cache=GlobalCacheStore(store.root))
    req = OddsRequest(
        method="GET",
        path=path,
        params=params,
        label=label,
        is_paid=is_paid,
    )
    policy = SpendPolicy(
        offline=offline,
        max_credits=1_000_000,
        no_spend=False,
        refresh=refresh,
        resume=resume,
        block_paid=block_paid,
        force=True,
    )
    try:
        result = repo.get_or_fetch(
            snapshot_id=snapshot_id,
            req=req,
            fetcher=fetcher,
            policy=policy,
        )
    except OfflineCacheMiss as exc:
        raise OfflineCacheMissError(str(exc)) from exc
    except SpendBlockedError as exc:
        raise OfflineCacheMissError(str(exc)) from exc
    return result.data, result.headers, result.status, result.key


def _parse_markets(value: str) -> list[str]:
    markets = parse_csv(value)
    if not markets:
        raise CLIError("at least one market is required")
    return markets


def _parse_positive_float_csv(value: str, *, default: list[float], flag_name: str) -> list[float]:
    raw_values = parse_csv(value)
    if not raw_values:
        return default
    parsed_values: list[float] = []
    for raw in raw_values:
        try:
            parsed = float(raw)
        except ValueError as exc:
            raise CLIError(f"{flag_name} must contain comma-separated numeric values") from exc
        if parsed <= 0.0:
            raise CLIError(f"{flag_name} values must be > 0")
        parsed_values.append(parsed)
    return sorted(set(parsed_values))


def _sanitize_analysis_run_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("._-")
    return cleaned


def _resolve_days(
    *,
    days: int,
    from_day: str,
    to_day: str,
    tz_name: str,
) -> list[str]:
    if from_day or to_day:
        if not from_day or not to_day:
            raise CLIError("--from and --to must be provided together")
        try:
            start = date.fromisoformat(from_day)
            end = date.fromisoformat(to_day)
        except ValueError as exc:
            raise CLIError("invalid --from/--to day format; expected YYYY-MM-DD") from exc
        if end < start:
            raise CLIError("--to must be on or after --from")
        span = (end - start).days
        return [(start + timedelta(days=offset)).isoformat() for offset in range(span + 1)]

    count = max(1, int(days))
    tz = ZoneInfo(tz_name)
    today_local = datetime.now(tz).date()
    start = today_local - timedelta(days=count - 1)
    return [(start + timedelta(days=offset)).isoformat() for offset in range(count)]


def _dataset_spec_from_args(args: argparse.Namespace) -> DatasetSpec:
    markets = _parse_markets(str(getattr(args, "markets", "")))
    bookmakers, _ = _resolve_bookmakers(
        str(getattr(args, "bookmakers", "")),
        allow_config=not bool(getattr(args, "ignore_bookmaker_config", False)),
    )
    regions = str(getattr(args, "regions", "")).strip()
    historical = bool(getattr(args, "historical", False))
    historical_anchor_hour_local = int(getattr(args, "historical_anchor_hour_local", 12))
    if historical_anchor_hour_local < 0 or historical_anchor_hour_local > 23:
        raise CLIError("--historical-anchor-hour-local must be within [0, 23]")
    historical_pre_tip_minutes = int(getattr(args, "historical_pre_tip_minutes", 60))
    if historical_pre_tip_minutes < 0:
        raise CLIError("--historical-pre-tip-minutes must be >= 0")
    return DatasetSpec(
        sport_key=str(getattr(args, "sport_key", "basketball_nba")).strip() or "basketball_nba",
        markets=markets,
        regions=regions or None,
        bookmakers=bookmakers or None,
        include_links=bool(getattr(args, "include_links", False)),
        include_sids=bool(getattr(args, "include_sids", False)),
        odds_format="american",
        date_format="iso",
        historical=historical,
        historical_anchor_hour_local=historical_anchor_hour_local,
        historical_pre_tip_minutes=historical_pre_tip_minutes,
    )


def _spend_policy_from_args(args: argparse.Namespace) -> SpendPolicy:
    max_credits = int(getattr(args, "max_credits", 20))
    no_spend = bool(getattr(args, "no_spend", False))
    if no_spend:
        max_credits = 0
    return SpendPolicy(
        offline=bool(getattr(args, "offline", False)),
        max_credits=max_credits,
        no_spend=no_spend,
        refresh=bool(getattr(args, "refresh", False)),
        resume=bool(getattr(args, "resume", False)),
        block_paid=bool(getattr(args, "block_paid", False)),
        force=bool(getattr(args, "force", False)),
    )


def _write_derived(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    filename: str,
    rows: list[dict[str, Any]],
) -> None:
    path = store.derived_path(snapshot_id, filename)
    store.write_jsonl(path, rows)


def _dataset_root(data_root: Path) -> Path:
    return _dataset_root_impl(data_root)


def _dataset_dir(data_root: Path, dataset_id_value: str) -> Path:
    return _dataset_dir_impl(data_root, dataset_id_value)


def _dataset_spec_path(data_root: Path, dataset_id_value: str) -> Path:
    return _dataset_spec_path_impl(data_root, dataset_id_value)


def _dataset_days_dir(data_root: Path, dataset_id_value: str) -> Path:
    return _dataset_days_dir_impl(data_root, dataset_id_value)


def _load_json_object(path: Path) -> dict[str, Any] | None:
    return _load_json_object_impl(path)


def _discover_dataset_ids(data_root: Path) -> list[str]:
    return _discover_dataset_ids_impl(data_root)


def _dataset_day_names(data_root: Path, dataset_id_value: str) -> list[str]:
    return _dataset_day_names_impl(data_root, dataset_id_value)


def _dataset_spec_from_payload(payload: dict[str, Any], *, source: str) -> DatasetSpec:
    try:
        return _dataset_spec_from_payload_impl(payload, source=source)
    except RuntimeError as exc:
        raise CLIError(str(exc)) from exc


def _load_dataset_spec_or_error(data_root: Path, dataset_id_value: str) -> tuple[DatasetSpec, Path]:
    try:
        return _load_dataset_spec_or_error_impl(data_root, dataset_id_value)
    except RuntimeError as exc:
        raise CLIError(str(exc)) from exc


def _load_day_status_for_dataset(
    data_root: Path,
    *,
    dataset_id_value: str,
    day: str,
) -> dict[str, Any] | None:
    return _load_day_status_for_dataset_impl(data_root, dataset_id_value=dataset_id_value, day=day)


def _day_row_from_status(day: str, status: dict[str, Any]) -> dict[str, Any]:
    return _day_row_from_status_impl(day, status)


def _incomplete_reason_code(row: dict[str, Any]) -> str:
    return _incomplete_reason_code_impl(row)


def _parse_allow_incomplete_days(raw_values: list[str]) -> set[str]:
    try:
        return _parse_allow_incomplete_days_impl(raw_values)
    except RuntimeError as exc:
        raise CLIError(str(exc)) from exc


def _parse_allow_incomplete_reasons(raw_values: list[str]) -> set[str]:
    return _parse_allow_incomplete_reasons_impl(raw_values)


def _build_status_summary_payload(
    *,
    dataset_id_value: str,
    spec: DatasetSpec,
    rows: list[dict[str, Any]],
    from_day: str,
    to_day: str,
    tz_name: str,
    warnings: list[dict[str, str]],
) -> dict[str, Any]:
    return _build_status_summary_payload_impl(
        dataset_id_value=dataset_id_value,
        spec=spec,
        rows=rows,
        from_day=from_day,
        to_day=to_day,
        tz_name=tz_name,
        warnings=warnings,
        generated_at_utc=iso_z(_utc_now()),
    )


def _print_day_rows(rows: list[dict[str, Any]]) -> None:
    _print_day_rows_impl(rows)


def _print_warnings(warnings: list[dict[str, str]]) -> None:
    _print_warnings_impl(warnings)
