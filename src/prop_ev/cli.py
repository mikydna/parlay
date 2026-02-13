"""CLI entrypoint for prop-ev."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from shutil import copy2
from typing import Any
from zoneinfo import ZoneInfo

from prop_ev.backtest import ROW_SELECTIONS, write_backtest_artifacts
from prop_ev.budget import current_month_utc
from prop_ev.cli_internal import (
    default_window,
    env_bool,
    env_float,
    env_int,
    teams_in_scope_from_events,
)
from prop_ev.context_health import (
    official_rows_count,
    official_source_ready,
    secondary_source_ready,
)
from prop_ev.discovery_execution import (
    build_discovery_execution_report,
    write_discovery_execution_reports,
)
from prop_ev.execution_projection import ExecutionProjectionConfig, project_execution_report
from prop_ev.identity_map import load_identity_map, update_identity_map
from prop_ev.nba_data.repo import NBARepository
from prop_ev.normalize import normalize_event_odds, normalize_featured_odds
from prop_ev.odds_client import (
    OddsAPIClient,
    OddsAPIError,
    OddsResponse,
    estimate_event_credits,
    estimate_featured_credits,
    parse_csv,
    regions_equivalent,
)
from prop_ev.odds_data.backfill import backfill_days
from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.day_index import (
    canonicalize_day_status,
    compute_day_status_from_cache,
    load_day_status,
    primary_incomplete_reason_code,
)
from prop_ev.odds_data.errors import CreditBudgetExceeded, OfflineCacheMiss, SpendBlockedError
from prop_ev.odds_data.policy import SpendPolicy
from prop_ev.odds_data.repo import OddsRepository
from prop_ev.odds_data.request import OddsRequest
from prop_ev.odds_data.spec import DatasetSpec, dataset_id
from prop_ev.playbook import (
    budget_snapshot,
    compute_live_window,
    generate_brief_for_snapshot,
    report_outputs_root,
)
from prop_ev.quote_table import EVENT_PROPS_TABLE
from prop_ev.settings import Settings
from prop_ev.settlement import settle_snapshot
from prop_ev.snapshot_artifacts import (
    lake_snapshot_derived,
    pack_snapshot,
    unpack_snapshot,
    verify_snapshot_derived_contracts,
)
from prop_ev.state_keys import (
    playbook_mode_key,
    strategy_health_state_key,
    strategy_title,
)
from prop_ev.storage import SnapshotStore, make_snapshot_id, request_hash
from prop_ev.strategies import get_strategy, list_strategies, resolve_strategy_id
from prop_ev.strategies.base import (
    StrategyInputs,
    StrategyRunConfig,
    decorate_report,
    normalize_strategy_id,
)
from prop_ev.strategy import (
    build_strategy_report,
    load_jsonl,
    write_strategy_reports,
    write_tagged_strategy_reports,
)
from prop_ev.time_utils import iso_z, utc_now


class CLIError(RuntimeError):
    """User-facing CLI error."""


class CreditLimitError(CLIError):
    """Raised when estimated credits exceed configured cap."""


class OfflineCacheMissError(CLIError):
    """Raised when offline mode is active and cache is missing."""


def _load_bookmaker_whitelist(path: Path) -> list[str]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return []
    enabled = payload.get("enabled", True)
    if isinstance(enabled, bool) and not enabled:
        return []
    raw_books = payload.get("bookmakers", [])
    if not isinstance(raw_books, list):
        return []
    books: list[str] = []
    for value in raw_books:
        if not isinstance(value, str):
            continue
        normalized = value.strip()
        if normalized:
            books.append(normalized)
    # Stable de-duplication while preserving order.
    return list(dict.fromkeys(books))


def _resolve_bookmakers(explicit: str, *, allow_config: bool = True) -> tuple[str, str]:
    explicit_books = parse_csv(explicit)
    if explicit_books:
        return ",".join(explicit_books), "cli"

    if not allow_config:
        return "", "none"

    config_path = Path(
        os.environ.get("PROP_EV_BOOKMAKERS_CONFIG_PATH", "config/bookmakers.json")
    ).resolve()
    books = _load_bookmaker_whitelist(config_path)
    if books:
        return ",".join(books), f"config:{config_path}"
    return "", "none"


def _utc_now() -> datetime:
    return utc_now()


def _iso(dt: datetime) -> str:
    return iso_z(dt)


def _default_window() -> tuple[str, str]:
    return default_window()


def _env_bool(name: str, default: bool) -> bool:
    return env_bool(name, default)


def _env_int(name: str, default: int) -> int:
    return env_int(name, default)


def _env_float(name: str, default: float) -> float:
    return env_float(name, default)


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


def _cmd_snapshot_slate(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    cache = GlobalCacheStore(store.root)
    snapshot_id = args.snapshot_id or make_snapshot_id()
    default_from, default_to = _default_window()
    commence_from = args.commence_from or default_from
    commence_to = args.commence_to or default_to
    markets = _parse_markets(args.markets)
    bookmakers, bookmakers_source = _resolve_bookmakers(
        args.bookmakers,
        allow_config=not bool(getattr(args, "ignore_bookmaker_config", False)),
    )
    regions_factor = regions_equivalent(args.regions, bookmakers)
    path = f"/sports/{args.sport_key}/odds"
    params: dict[str, Any] = {
        "markets": ",".join(sorted(set(markets))),
        "oddsFormat": "american",
        "dateFormat": "iso",
        "commenceTimeFrom": commence_from,
        "commenceTimeTo": commence_to,
    }
    if bookmakers:
        params["bookmakers"] = bookmakers
    elif args.regions:
        params["regions"] = args.regions
    request_key = request_hash("GET", path, params)
    cached_hit = not args.refresh and (
        store.has_response(snapshot_id, request_key) or cache.has_response(request_key)
    )
    estimate = 0 if cached_hit else estimate_featured_credits(markets, regions_factor)
    _print_estimate(estimate, args.max_credits)
    _enforce_credit_cap(estimate, args.max_credits, args.force)
    if args.dry_run:
        print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")
        return 0

    settings = Settings.from_env()
    run_config = {
        "mode": "snapshot_slate",
        "sport_key": args.sport_key,
        "markets": markets,
        "regions": args.regions,
        "bookmakers": bookmakers,
        "bookmakers_source": bookmakers_source,
        "commence_from": commence_from,
        "commence_to": commence_to,
    }
    with store.lock_snapshot(snapshot_id):
        store.ensure_snapshot(snapshot_id, run_config=run_config)

        with OddsAPIClient(settings) as client:
            data, headers, status, key = _execute_request(
                store=store,
                snapshot_id=snapshot_id,
                label="slate_odds",
                path=path,
                params=params,
                fetcher=lambda: client.get_featured_odds(
                    sport_key=args.sport_key,
                    markets=markets,
                    regions=args.regions,
                    bookmakers=bookmakers,
                    commence_from=commence_from,
                    commence_to=commence_to,
                ),
                offline=args.offline,
                block_paid=bool(getattr(args, "block_paid", False)),
                is_paid=True,
                refresh=args.refresh,
                resume=args.resume,
            )
            rows = normalize_featured_odds(data, snapshot_id=snapshot_id, provider="odds_api")
            _write_derived(
                store=store,
                snapshot_id=snapshot_id,
                filename="featured_odds.jsonl",
                rows=rows,
            )
            print(f"snapshot_id={snapshot_id} request_key={key} status={status}")
            print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")
            print(
                "x_requests_last={} x_requests_remaining={}".format(
                    headers.get("x-requests-last", ""),
                    headers.get("x-requests-remaining", ""),
                )
            )
    return 0


def _cmd_snapshot_props(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    cache = GlobalCacheStore(store.root)
    snapshot_id = args.snapshot_id or make_snapshot_id()
    default_from, default_to = _default_window()
    commence_from = args.commence_from or default_from
    commence_to = args.commence_to or default_to
    markets = _parse_markets(args.markets)
    bookmakers, bookmakers_source = _resolve_bookmakers(
        args.bookmakers,
        allow_config=not bool(getattr(args, "ignore_bookmaker_config", False)),
    )

    with store.lock_snapshot(snapshot_id):
        run_config = {
            "mode": "snapshot_props",
            "sport_key": args.sport_key,
            "markets": markets,
            "regions": args.regions,
            "bookmakers": bookmakers,
            "bookmakers_source": bookmakers_source,
            "commence_from": commence_from,
            "commence_to": commence_to,
            "include_links": args.include_links,
            "include_sids": args.include_sids,
            "max_events": args.max_events,
        }
        store.ensure_snapshot(snapshot_id, run_config=run_config)
        if args.dry_run:
            regions_factor = regions_equivalent(args.regions, bookmakers)
            event_count = args.max_events or 0
            estimate = estimate_event_credits(markets, regions_factor, event_count)
            _print_estimate(estimate, args.max_credits)
            print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")
            return 0

        counters: Counter[str] = Counter()
        all_rows: list[dict[str, Any]] = []
        settings = Settings.from_env()
        with OddsAPIClient(settings) as client:
            events_path = f"/sports/{args.sport_key}/events"
            events_params: dict[str, Any] = {
                "dateFormat": "iso",
                "commenceTimeFrom": commence_from,
                "commenceTimeTo": commence_to,
            }
            events_data, events_headers, _, events_key = _execute_request(
                store=store,
                snapshot_id=snapshot_id,
                label="events_list",
                path=events_path,
                params=events_params,
                fetcher=lambda: client.list_events(
                    sport_key=args.sport_key,
                    commence_from=commence_from,
                    commence_to=commence_to,
                ),
                offline=args.offline,
                block_paid=bool(getattr(args, "block_paid", False)),
                is_paid=False,
                refresh=args.refresh,
                resume=args.resume,
            )
            event_list = events_data if isinstance(events_data, list) else []
            event_ids = [str(item.get("id", "")) for item in event_list if isinstance(item, dict)]
            event_ids = [event_id for event_id in event_ids if event_id]
            if args.max_events:
                event_ids = event_ids[: args.max_events]
            event_request_params: dict[str, Any] = {
                "markets": ",".join(sorted(set(markets))),
                "oddsFormat": "american",
                "dateFormat": "iso",
            }
            if bookmakers:
                event_request_params["bookmakers"] = bookmakers
            elif args.regions:
                event_request_params["regions"] = args.regions
            if args.include_links:
                event_request_params["includeLinks"] = "true"
            if args.include_sids:
                event_request_params["includeSids"] = "true"
            missing_event_ids: list[str] = []
            for event_id in event_ids:
                event_path = f"/sports/{args.sport_key}/events/{event_id}/odds"
                event_key = request_hash("GET", event_path, event_request_params)
                cached_hit = not args.refresh and (
                    store.has_response(snapshot_id, event_key) or cache.has_response(event_key)
                )
                if not cached_hit:
                    missing_event_ids.append(event_id)
            regions_factor = regions_equivalent(args.regions, bookmakers)
            estimate = estimate_event_credits(markets, regions_factor, len(missing_event_ids))
            _print_estimate(estimate, args.max_credits)
            _enforce_credit_cap(estimate, args.max_credits, args.force)
            print(
                "events_key={} x_requests_last={} x_requests_remaining={}".format(
                    events_key,
                    events_headers.get("x-requests-last", ""),
                    events_headers.get("x-requests-remaining", ""),
                )
            )
            print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")

            for event_id in event_ids:
                path = f"/sports/{args.sport_key}/events/{event_id}/odds"
                params = dict(event_request_params)
                try:
                    data, headers, status, key = _execute_request(
                        store=store,
                        snapshot_id=snapshot_id,
                        label=f"event_odds:{event_id}",
                        path=path,
                        params=params,
                        fetcher=lambda event_id=event_id: client.get_event_odds(
                            sport_key=args.sport_key,
                            event_id=event_id,
                            markets=markets,
                            regions=args.regions,
                            bookmakers=bookmakers,
                            include_links=args.include_links,
                            include_sids=args.include_sids,
                        ),
                        offline=args.offline,
                        block_paid=bool(getattr(args, "block_paid", False)),
                        is_paid=True,
                        refresh=args.refresh,
                        resume=args.resume,
                    )
                    rows = normalize_event_odds(data, snapshot_id=snapshot_id, provider="odds_api")
                    all_rows.extend(rows)
                    counters[status] += 1
                    print(
                        (
                            "event_id={} request_key={} status={} "
                            "x_requests_last={} x_requests_remaining={}"
                        ).format(
                            event_id,
                            key,
                            status,
                            headers.get("x-requests-last", ""),
                            headers.get("x-requests-remaining", ""),
                        )
                    )
                except (OddsAPIError, OfflineCacheMissError, ValueError) as exc:
                    counters["failed"] += 1
                    request_key = request_hash("GET", path, params)
                    store.mark_request(
                        snapshot_id,
                        request_key,
                        label=f"event_odds:{event_id}",
                        path=path,
                        params=params,
                        status="failed",
                        error=str(exc),
                    )
                    print(f"event_id={event_id} status=failed error={exc}")

        _write_derived(
            store=store,
            snapshot_id=snapshot_id,
            filename="event_props.jsonl",
            rows=all_rows,
        )
        print(
            "snapshot_id={} succeeded={} cached={} skipped={} failed={}".format(
                snapshot_id,
                counters.get("ok", 0),
                counters.get("cached", 0),
                counters.get("skipped", 0),
                counters.get("failed", 0),
            )
        )
        return 2 if counters.get("failed", 0) > 0 else 0


def _cmd_snapshot_ls(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    if not store.snapshots_dir.exists():
        print("no snapshots")
        return 0

    snapshots = sorted(path for path in store.snapshots_dir.iterdir() if path.is_dir())
    if not snapshots:
        print("no snapshots")
        return 0
    for snapshot_dir in snapshots:
        manifest_path = snapshot_dir / "manifest.json"
        if not manifest_path.exists():
            print(f"{snapshot_dir.name} created_at=unknown requests=0")
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        request_count = len(manifest.get("requests", {}))
        created_at = str(manifest.get("created_at_utc", ""))
        print(f"{snapshot_dir.name} created_at={created_at} requests={request_count}")
    return 0


def _cmd_snapshot_show(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    manifest = store.load_manifest(args.snapshot_id)
    requests = manifest.get("requests", {})
    counts: Counter[str] = Counter()
    if isinstance(requests, dict):
        for value in requests.values():
            if isinstance(value, dict):
                status = str(value.get("status", ""))
                counts[status] += 1
    output = {
        "snapshot_id": manifest.get("snapshot_id", args.snapshot_id),
        "created_at_utc": manifest.get("created_at_utc", ""),
        "schema_version": manifest.get("schema_version", ""),
        "client_version": manifest.get("client_version", ""),
        "quota": manifest.get("quota", {}),
        "request_counts": dict(counts),
    }
    print(json.dumps(output, sort_keys=True, indent=2))
    return 0


def _cmd_snapshot_diff(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    a_dir = store.derived_path(args.a, "")
    b_dir = store.derived_path(args.b, "")
    a_files = {path.name for path in a_dir.glob("*.jsonl")} if a_dir.exists() else set()
    b_files = {path.name for path in b_dir.glob("*.jsonl")} if b_dir.exists() else set()
    all_files = sorted(a_files | b_files)
    if not all_files:
        print("no derived files to diff")
        return 0
    for filename in all_files:
        a_path = a_dir / filename
        b_path = b_dir / filename
        a_lines = set(a_path.read_text(encoding="utf-8").splitlines()) if a_path.exists() else set()
        b_lines = set(b_path.read_text(encoding="utf-8").splitlines()) if b_path.exists() else set()
        added = len(b_lines - a_lines)
        removed = len(a_lines - b_lines)
        print(f"{filename} added={added} removed={removed}")
    return 0


def _cmd_snapshot_verify(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_dir = store.snapshot_dir(args.snapshot_id)
    manifest = store.load_manifest(args.snapshot_id)
    requests = manifest.get("requests", {})
    if not isinstance(requests, dict):
        print("invalid manifest: requests must be object")
        return 2

    missing = 0
    for request_key in requests:
        request_path = snapshot_dir / "requests" / f"{request_key}.json"
        response_path = snapshot_dir / "responses" / f"{request_key}.json"
        meta_path = snapshot_dir / "meta" / f"{request_key}.json"
        if not request_path.exists() or not response_path.exists() or not meta_path.exists():
            missing += 1
            print(
                f"missing_artifacts request_key={request_key} "
                f"request={request_path.exists()} "
                f"response={response_path.exists()} "
                f"meta={meta_path.exists()}"
            )

    derived_issues: list[dict[str, str]] = []
    if bool(getattr(args, "check_derived", False)):
        required_tables = tuple(
            sorted(
                {
                    str(item).strip()
                    for item in getattr(args, "require_table", [])
                    if str(item).strip()
                }
            )
        )
        derived_issues = verify_snapshot_derived_contracts(
            snapshot_dir=snapshot_dir,
            require_parquet=bool(getattr(args, "require_parquet", False)),
            required_tables=required_tables,
        )
        for issue in derived_issues:
            print(
                "derived_issue code={} table={} detail={}".format(
                    str(issue.get("code", "")),
                    str(issue.get("table", "")),
                    str(issue.get("detail", "")),
                )
            )

    print(
        f"snapshot_id={args.snapshot_id} checked={len(requests)} missing={missing} "
        f"derived_issues={len(derived_issues)}"
    )
    return 2 if (missing or derived_issues) else 0


def _cmd_snapshot_lake(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_dir = store.snapshot_dir(args.snapshot_id)
    parquet_paths = lake_snapshot_derived(snapshot_dir)
    print(f"snapshot_id={args.snapshot_id} parquet_files={len(parquet_paths)}")
    for path in parquet_paths:
        print(f"parquet={path}")
    return 0


def _cmd_snapshot_pack(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    out_path = Path(str(args.out)).expanduser() if str(args.out).strip() else None
    bundle_path, sidecar_path = pack_snapshot(
        data_root=store.root,
        snapshot_id=args.snapshot_id,
        out_path=out_path,
    )
    print(f"snapshot_id={args.snapshot_id}")
    print(f"bundle={bundle_path}")
    print(f"bundle_meta={sidecar_path}")
    return 0


def _cmd_snapshot_unpack(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    payload = unpack_snapshot(
        data_root=store.root,
        bundle_path=Path(str(args.bundle)).expanduser(),
    )
    snapshot_ids = payload.get("snapshot_ids", [])
    snapshot_list = ",".join(snapshot_ids) if isinstance(snapshot_ids, list) else ""
    print(f"bundle={payload.get('bundle_path', '')}")
    print(f"files_extracted={payload.get('files_extracted', 0)}")
    print(f"snapshot_ids={snapshot_list}")
    return 0


def _cmd_credits_report(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    month = args.month or _utc_now().strftime("%Y-%m")
    usage_path = store.usage_dir / f"usage-{month}.jsonl"
    if not usage_path.exists():
        print(f"no usage ledger for month={month}")
        return 0

    total = 0
    endpoint_totals: Counter[str] = Counter()
    remaining = ""
    for line in usage_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        endpoint = str(item.get("endpoint", ""))
        last_raw = str(item.get("x_requests_last", "0"))
        remaining = str(item.get("x_requests_remaining", remaining))
        try:
            spent = int(last_raw)
        except ValueError:
            spent = 0
        total += spent
        endpoint_totals[endpoint] += spent

    print(f"month={month} total_credits={total} remaining={remaining}")
    for endpoint, spent in endpoint_totals.most_common(10):
        print(f"endpoint={endpoint} credits={spent}")
    return 0


def _cmd_credits_budget(args: argparse.Namespace) -> int:
    markets = _parse_markets(args.markets)
    bookmakers, bookmakers_source = _resolve_bookmakers(args.bookmakers)
    factor = regions_equivalent(args.regions, bookmakers)
    featured = estimate_featured_credits(markets, factor)
    event = estimate_event_credits(markets, factor, args.events)
    print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")
    print(f"regions_equivalent={factor}")
    print(f"featured_estimate={featured}")
    print(f"event_estimate={event}")
    print(f"recommended_max_credits={max(featured, event)}")
    return 0


def _dataset_root(data_root: Path) -> Path:
    return data_root / "datasets"


def _dataset_dir(data_root: Path, dataset_id_value: str) -> Path:
    return _dataset_root(data_root) / dataset_id_value


def _dataset_spec_path(data_root: Path, dataset_id_value: str) -> Path:
    return _dataset_dir(data_root, dataset_id_value) / "spec.json"


def _dataset_days_dir(data_root: Path, dataset_id_value: str) -> Path:
    return _dataset_dir(data_root, dataset_id_value) / "days"


def _load_json_object(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    return payload


def _discover_dataset_ids(data_root: Path) -> list[str]:
    root = _dataset_root(data_root)
    if not root.exists():
        return []
    ids: list[str] = []
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        if (entry / "spec.json").exists() or (entry / "days").exists():
            ids.append(entry.name)
    return sorted(set(ids))


def _dataset_day_names(data_root: Path, dataset_id_value: str) -> list[str]:
    days_dir = _dataset_days_dir(data_root, dataset_id_value)
    if not days_dir.exists():
        return []
    names: list[str] = []
    for path in days_dir.glob("*.json"):
        candidate = path.stem.strip()
        try:
            date.fromisoformat(candidate)
        except ValueError:
            continue
        names.append(candidate)
    return sorted(set(names))


def _dataset_spec_from_payload(payload: dict[str, Any], *, source: str) -> DatasetSpec:
    sport_key = str(payload.get("sport_key", "")).strip() or "basketball_nba"
    markets_raw = payload.get("markets", [])
    markets: list[str]
    if isinstance(markets_raw, list):
        markets = [str(item).strip() for item in markets_raw if str(item).strip()]
    else:
        markets = parse_csv(str(markets_raw))
    if not markets:
        raise CLIError(f"invalid dataset spec at {source}: markets must be a non-empty list")
    regions = str(payload.get("regions", "")).strip() or None
    bookmakers = str(payload.get("bookmakers", "")).strip() or None
    return DatasetSpec(
        sport_key=sport_key,
        markets=markets,
        regions=regions,
        bookmakers=bookmakers,
        include_links=bool(payload.get("include_links", False)),
        include_sids=bool(payload.get("include_sids", False)),
        odds_format=str(payload.get("odds_format", "american")).strip() or "american",
        date_format=str(payload.get("date_format", "iso")).strip() or "iso",
        historical=bool(payload.get("historical", False)),
        historical_anchor_hour_local=int(payload.get("historical_anchor_hour_local", 12)),
        historical_pre_tip_minutes=int(payload.get("historical_pre_tip_minutes", 60)),
    )


def _load_dataset_spec_or_error(data_root: Path, dataset_id_value: str) -> tuple[DatasetSpec, Path]:
    path = _dataset_spec_path(data_root, dataset_id_value)
    payload = _load_json_object(path)
    if payload is None:
        raise CLIError(f"missing dataset spec: {path}")
    return _dataset_spec_from_payload(payload, source=str(path)), path


def _load_day_status_for_dataset(
    data_root: Path,
    *,
    dataset_id_value: str,
    day: str,
) -> dict[str, Any] | None:
    return _load_json_object(_dataset_days_dir(data_root, dataset_id_value) / f"{day}.json")


def _day_row_from_status(day: str, status: dict[str, Any]) -> dict[str, Any]:
    normalized = canonicalize_day_status(status, day=day)
    return {
        "day": day,
        "complete": bool(normalized.get("complete", False)),
        "missing_count": int(normalized.get("missing_count", 0)),
        "total_events": int(normalized.get("total_events", 0)),
        "snapshot_id": str(normalized.get("snapshot_id_for_day", "")),
        "note": str(normalized.get("note", "")),
        "error": str(normalized.get("error", "")),
        "error_code": str(normalized.get("error_code", "")),
        "status_code": str(normalized.get("status_code", "")),
        "reason_codes": [
            str(item)
            for item in normalized.get("reason_codes", [])
            if isinstance(item, str) and str(item).strip()
        ],
        "odds_coverage_ratio": float(normalized.get("odds_coverage_ratio", 0.0)),
        "updated_at_utc": str(normalized.get("updated_at_utc", "")),
    }


def _incomplete_reason_code(row: dict[str, Any]) -> str:
    error_code = str(row.get("error_code", "")).strip()
    if error_code:
        return error_code
    reason_codes_raw = row.get("reason_codes", [])
    if isinstance(reason_codes_raw, list):
        reason_codes = [str(item).strip() for item in reason_codes_raw if str(item).strip()]
        if reason_codes:
            return primary_incomplete_reason_code(reason_codes)
    status_code = str(row.get("status_code", "")).strip()
    if status_code.startswith("incomplete_"):
        return status_code.removeprefix("incomplete_")
    error_text = str(row.get("error", "")).strip().lower()
    if error_text:
        if "404" in error_text:
            return "upstream_404"
        if "exceed remaining budget" in error_text:
            return "budget_exceeded"
        if "blocked" in error_text:
            return "spend_blocked"
        return "error"
    note_text = str(row.get("note", "")).strip().lower()
    if note_text == "missing events list response":
        return "missing_events_list"
    if note_text == "missing day status":
        return "missing_day_status"
    if note_text:
        return "note"
    if int(row.get("missing_count", 0)) > 0:
        return "missing_event_odds"
    return "incomplete_unknown"


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
    complete_days = [row["day"] for row in rows if bool(row["complete"])]
    incomplete_days = [row["day"] for row in rows if not bool(row["complete"])]
    incomplete_reason_counts: Counter[str] = Counter(
        _incomplete_reason_code(row) for row in rows if not bool(row["complete"])
    )
    incomplete_error_code_counts: Counter[str] = Counter(
        (str(row.get("error_code", "")).strip() or _incomplete_reason_code(row))
        for row in rows
        if not bool(row["complete"])
    )
    coverage_values = [float(row.get("odds_coverage_ratio", 0.0)) for row in rows]
    payload: dict[str, Any] = {
        "dataset_id": dataset_id_value,
        "sport_key": spec.sport_key,
        "markets": sorted(set(spec.markets)),
        "regions": spec.regions,
        "bookmakers": spec.bookmakers,
        "historical": bool(spec.historical),
        "from_day": from_day,
        "to_day": to_day,
        "tz_name": tz_name,
        "total_days": len(rows),
        "complete_count": len(complete_days),
        "incomplete_count": len(incomplete_days),
        "missing_events_total": sum(int(row["missing_count"]) for row in rows),
        "avg_odds_coverage_ratio": (
            sum(coverage_values) / len(coverage_values) if coverage_values else 0.0
        ),
        "minimum_odds_coverage_ratio": min(coverage_values) if coverage_values else 0.0,
        "complete_days": complete_days,
        "incomplete_days": incomplete_days,
        "incomplete_reason_counts": dict(sorted(incomplete_reason_counts.items())),
        "incomplete_error_code_counts": dict(sorted(incomplete_error_code_counts.items())),
        "days": rows,
        "generated_at_utc": iso_z(_utc_now()),
    }
    if warnings:
        payload["warnings"] = warnings
    return payload


def _print_day_rows(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        reason_code = (
            _incomplete_reason_code(row) if not bool(row.get("complete", False)) else "complete"
        )
        error_code = str(row.get("error_code", "")).strip() or reason_code
        print(
            (
                "day={} complete={} reason={} error_code={} missing={} events={} coverage={} "
                "snapshot_id={} note={}"
            ).format(
                row["day"],
                str(row["complete"]).lower(),
                reason_code,
                error_code,
                row["missing_count"],
                row["total_events"],
                f"{float(row.get('odds_coverage_ratio', 0.0)):.3f}",
                row["snapshot_id"],
                row["note"],
            )
        )


def _print_warnings(warnings: list[dict[str, str]]) -> None:
    for warning in warnings:
        code = str(warning.get("code", "")).strip()
        detail = str(warning.get("detail", "")).strip()
        hint = str(warning.get("hint", "")).strip()
        print(f"warning={code} detail={detail} hint={hint}")


def _cmd_data_datasets_ls(args: argparse.Namespace) -> int:
    data_root = Path(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    dataset_ids = _discover_dataset_ids(data_root)
    summaries: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []

    for dataset_id_value in dataset_ids:
        day_names = _dataset_day_names(data_root, dataset_id_value)
        updated_at_values: list[str] = []
        try:
            spec, _ = _load_dataset_spec_or_error(data_root, dataset_id_value)
        except CLIError as exc:
            summaries.append(
                {
                    "dataset_id": dataset_id_value,
                    "day_count": len(day_names),
                    "complete_count": 0,
                    "incomplete_count": len(day_names),
                    "from_day": day_names[0] if day_names else "",
                    "to_day": day_names[-1] if day_names else "",
                    "error": str(exc),
                }
            )
            warnings.append(
                {
                    "code": "invalid_dataset_spec",
                    "detail": dataset_id_value,
                    "hint": "run `prop-ev data datasets show --dataset-id <id>`",
                }
            )
            continue

        rows: list[dict[str, Any]] = []
        for day in day_names:
            status = _load_day_status_for_dataset(
                data_root,
                dataset_id_value=dataset_id_value,
                day=day,
            )
            if not isinstance(status, dict):
                rows.append(
                    {
                        "day": day,
                        "complete": False,
                        "missing_count": 0,
                        "total_events": 0,
                        "snapshot_id": "",
                        "note": "missing day status",
                        "error": "",
                        "error_code": "missing_day_status",
                        "status_code": "incomplete_missing_day_status",
                        "reason_codes": ["missing_day_status"],
                        "odds_coverage_ratio": 0.0,
                        "updated_at_utc": "",
                    }
                )
                continue
            row = _day_row_from_status(day, status)
            rows.append(row)
            updated_at = str(row.get("updated_at_utc", "")).strip()
            if updated_at:
                updated_at_values.append(updated_at)

        complete_count = sum(1 for row in rows if bool(row.get("complete", False)))
        reason_counts: Counter[str] = Counter(
            _incomplete_reason_code(row) for row in rows if not bool(row.get("complete", False))
        )
        error_code_counts: Counter[str] = Counter(
            (str(row.get("error_code", "")).strip() or _incomplete_reason_code(row))
            for row in rows
            if not bool(row.get("complete", False))
        )
        coverage_values = [float(row.get("odds_coverage_ratio", 0.0)) for row in rows]
        summary = {
            "dataset_id": dataset_id_value,
            "sport_key": spec.sport_key,
            "markets": sorted(set(spec.markets)),
            "regions": spec.regions,
            "bookmakers": spec.bookmakers,
            "historical": bool(spec.historical),
            "day_count": len(day_names),
            "complete_count": complete_count,
            "incomplete_count": len(day_names) - complete_count,
            "missing_events_total": sum(int(row.get("missing_count", 0)) for row in rows),
            "avg_odds_coverage_ratio": (
                sum(coverage_values) / len(coverage_values) if coverage_values else 0.0
            ),
            "minimum_odds_coverage_ratio": min(coverage_values) if coverage_values else 0.0,
            "incomplete_reason_counts": dict(sorted(reason_counts.items())),
            "incomplete_error_code_counts": dict(sorted(error_code_counts.items())),
            "from_day": day_names[0] if day_names else "",
            "to_day": day_names[-1] if day_names else "",
            "updated_at_utc": max(updated_at_values) if updated_at_values else "",
        }
        summaries.append(summary)

    if bool(getattr(args, "json_output", False)):
        payload: dict[str, Any] = {
            "generated_at_utc": iso_z(_utc_now()),
            "dataset_count": len(summaries),
            "datasets": summaries,
        }
        if warnings:
            payload["warnings"] = warnings
        print(json.dumps(payload, sort_keys=True))
        return 0

    if warnings:
        _print_warnings(warnings)
    for row in summaries:
        markets = ",".join(row.get("markets", [])) if isinstance(row.get("markets"), list) else ""
        scope_label = "bookmakers" if str(row.get("bookmakers", "")).strip() else "regions"
        scope_value = str(row.get("bookmakers", "")).strip() or str(row.get("regions", "")).strip()
        print(
            (
                "dataset_id={} sport_key={} markets={} {}={} historical={} days={} "
                "complete={} incomplete={} from={} to={}"
            ).format(
                row.get("dataset_id", ""),
                row.get("sport_key", ""),
                markets,
                scope_label,
                scope_value,
                str(bool(row.get("historical", False))).lower(),
                int(row.get("day_count", 0)),
                int(row.get("complete_count", 0)),
                int(row.get("incomplete_count", 0)),
                row.get("from_day", ""),
                row.get("to_day", ""),
            )
        )
    return 0


def _cmd_data_datasets_show(args: argparse.Namespace) -> int:
    data_root = Path(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    dataset_id_value = str(getattr(args, "dataset_id", "")).strip()
    if not dataset_id_value:
        raise CLIError("--dataset-id is required")

    spec, _ = _load_dataset_spec_or_error(data_root, dataset_id_value)
    available_days = _dataset_day_names(data_root, dataset_id_value)
    from_day = str(getattr(args, "from_day", "")).strip()
    to_day = str(getattr(args, "to_day", "")).strip()
    if (from_day and not to_day) or (to_day and not from_day):
        raise CLIError("--from and --to must be provided together")

    if from_day and to_day:
        selected_days = _resolve_days(
            days=1,
            from_day=from_day,
            to_day=to_day,
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    else:
        selected_days = available_days

    rows: list[dict[str, Any]] = []
    for day in selected_days:
        status = _load_day_status_for_dataset(
            data_root,
            dataset_id_value=dataset_id_value,
            day=day,
        )
        if not isinstance(status, dict):
            rows.append(
                {
                    "day": day,
                    "complete": False,
                    "missing_count": 0,
                    "total_events": 0,
                    "snapshot_id": "",
                    "note": "missing day status",
                    "error": "",
                    "error_code": "missing_day_status",
                    "status_code": "incomplete_missing_day_status",
                    "reason_codes": ["missing_day_status"],
                    "odds_coverage_ratio": 0.0,
                    "updated_at_utc": "",
                }
            )
            continue
        rows.append(_day_row_from_status(day, status))

    payload = _build_status_summary_payload(
        dataset_id_value=dataset_id_value,
        spec=spec,
        rows=rows,
        from_day=selected_days[0] if selected_days else "",
        to_day=selected_days[-1] if selected_days else "",
        tz_name=str(getattr(args, "tz_name", "America/New_York")),
        warnings=[],
    )
    payload["available_day_count"] = len(available_days)
    payload["available_from_day"] = available_days[0] if available_days else ""
    payload["available_to_day"] = available_days[-1] if available_days else ""

    if bool(getattr(args, "json_output", False)):
        print(json.dumps(payload, sort_keys=True))
        return 0

    markets = ",".join(sorted(set(spec.markets)))
    scope_label = "bookmakers" if spec.bookmakers else "regions"
    scope_value = spec.bookmakers or (spec.regions or "")
    print(
        f"dataset_id={dataset_id_value} sport_key={spec.sport_key} markets={markets} "
        f"{scope_label}={scope_value} historical={str(bool(spec.historical)).lower()} "
        f"available_days={len(available_days)}"
    )
    _print_day_rows(rows)
    return 0


def _cmd_data_status(args: argparse.Namespace) -> int:
    data_root = Path(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    warnings: list[dict[str, str]] = []
    dataset_id_override = str(getattr(args, "dataset_id", "")).strip()
    if dataset_id_override:
        spec, _ = _load_dataset_spec_or_error(data_root, dataset_id_override)
        warnings.append(
            {
                "code": "dataset_id_override",
                "detail": dataset_id_override,
                "hint": "status uses stored dataset spec; CLI spec args are ignored",
            }
        )
    else:
        spec = _dataset_spec_from_args(args)
    dataset_id_value = dataset_id_override or dataset_id(spec)
    try:
        days = _resolve_days(
            days=int(getattr(args, "days", 10)),
            from_day=str(getattr(args, "from_day", "")),
            to_day=str(getattr(args, "to_day", "")),
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    except (KeyError, ValueError) as exc:
        raise CLIError(str(exc)) from exc

    if (
        not dataset_id_override
        and not bool(getattr(args, "refresh", False))
        and not _dataset_day_names(data_root, dataset_id_value)
    ):
        discovered = [item for item in _discover_dataset_ids(data_root) if item != dataset_id_value]
        if discovered:
            warnings.append(
                {
                    "code": "dataset_not_found_for_spec",
                    "detail": dataset_id_value,
                    "hint": "run `prop-ev data datasets ls` and pick --dataset-id",
                }
            )

    rows: list[dict[str, Any]] = []
    for day in days:
        status: dict[str, Any] | None = None
        if not bool(getattr(args, "refresh", False)):
            if dataset_id_override:
                status = _load_day_status_for_dataset(
                    data_root,
                    dataset_id_value=dataset_id_value,
                    day=day,
                )
            else:
                status = load_day_status(data_root, spec, day)
        if not isinstance(status, dict):
            status = compute_day_status_from_cache(
                data_root=data_root,
                store=store,
                cache=cache,
                spec=spec,
                day=day,
                tz_name=str(getattr(args, "tz_name", "America/New_York")),
            )
        row = _day_row_from_status(day, status)
        rows.append(row)

    if bool(getattr(args, "json_summary", False)):
        payload = _build_status_summary_payload(
            dataset_id_value=dataset_id_value,
            spec=spec,
            rows=rows,
            from_day=days[0] if days else "",
            to_day=days[-1] if days else "",
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
            warnings=warnings,
        )
        print(json.dumps(payload, sort_keys=True))
    else:
        if warnings:
            _print_warnings(warnings)
        _print_day_rows(rows)
    return 0


def _cmd_data_backfill(args: argparse.Namespace) -> int:
    data_root = Path(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    spec = _dataset_spec_from_args(args)
    try:
        days = _resolve_days(
            days=int(getattr(args, "days", 10)),
            from_day=str(getattr(args, "from_day", "")),
            to_day=str(getattr(args, "to_day", "")),
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    except (KeyError, ValueError) as exc:
        raise CLIError(str(exc)) from exc

    policy = _spend_policy_from_args(args)
    summaries = backfill_days(
        data_root=data_root,
        spec=spec,
        days=days,
        tz_name=str(getattr(args, "tz_name", "America/New_York")),
        policy=policy,
        dry_run=bool(getattr(args, "dry_run", False)),
    )

    had_error = False
    for row in summaries:
        error = str(row.get("error", "")).strip()
        error_code = str(row.get("error_code", "")).strip()
        if error:
            had_error = True
        print(
            (
                "day={} snapshot_id={} complete={} missing={} events={} "
                "estimated_paid_credits={} actual_paid_credits={} remaining_credits={} "
                "error_code={} error={}"
            ).format(
                str(row.get("day", "")),
                str(row.get("snapshot_id", "")),
                str(bool(row.get("complete", False))).lower(),
                int(row.get("missing", 0)),
                int(row.get("events", 0)),
                int(row.get("estimated_paid_credits", 0)),
                int(row.get("actual_paid_credits", 0)),
                int(row.get("remaining_credits", 0)),
                error_code,
                error,
            )
        )
    return 2 if had_error else 0


def _cmd_data_verify(args: argparse.Namespace) -> int:
    data_root = Path(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    dataset_id_value = str(getattr(args, "dataset_id", "")).strip()
    if not dataset_id_value:
        raise CLIError("--dataset-id is required")

    spec, _ = _load_dataset_spec_or_error(data_root, dataset_id_value)
    available_days = _dataset_day_names(data_root, dataset_id_value)
    from_day = str(getattr(args, "from_day", "")).strip()
    to_day = str(getattr(args, "to_day", "")).strip()
    if (from_day and not to_day) or (to_day and not from_day):
        raise CLIError("--from and --to must be provided together")

    if from_day and to_day:
        selected_days = _resolve_days(
            days=1,
            from_day=from_day,
            to_day=to_day,
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    else:
        selected_days = available_days

    day_reports: list[dict[str, Any]] = []
    issue_count = 0
    checked_complete_days = 0
    for day in selected_days:
        row_issues: list[dict[str, str]] = []
        status = _load_day_status_for_dataset(
            data_root,
            dataset_id_value=dataset_id_value,
            day=day,
        )
        if not isinstance(status, dict):
            row = {
                "day": day,
                "complete": False,
                "missing_count": 0,
                "total_events": 0,
                "snapshot_id": "",
                "note": "missing day status",
                "error": "",
                "error_code": "missing_day_status",
                "status_code": "incomplete_missing_day_status",
                "reason_codes": ["missing_day_status"],
                "odds_coverage_ratio": 0.0,
                "updated_at_utc": "",
            }
            row_issues.append(
                {
                    "code": "missing_day_status",
                    "detail": (
                        _dataset_days_dir(data_root, dataset_id_value) / f"{day}.json"
                    ).as_posix(),
                }
            )
        else:
            row = _day_row_from_status(day, status)

        if bool(getattr(args, "require_complete", False)) and not bool(row.get("complete", False)):
            row_issues.append(
                {
                    "code": "incomplete_day",
                    "detail": _incomplete_reason_code(row),
                }
            )

        if bool(row.get("complete", False)):
            checked_complete_days += 1
            snapshot_id = str(row.get("snapshot_id", "")).strip()
            if not snapshot_id:
                row_issues.append({"code": "missing_snapshot_id", "detail": day})
            else:
                snapshot_dir = data_root / "snapshots" / snapshot_id
                if not snapshot_dir.exists():
                    row_issues.append(
                        {
                            "code": "missing_snapshot_dir",
                            "detail": snapshot_dir.as_posix(),
                        }
                    )
                else:
                    derived_issues = verify_snapshot_derived_contracts(
                        snapshot_dir=snapshot_dir,
                        require_parquet=bool(getattr(args, "require_parquet", False)),
                        required_tables=(EVENT_PROPS_TABLE,),
                    )
                    row_issues.extend(derived_issues)

        issue_count += len(row_issues)
        day_reports.append(
            {
                **row,
                "issues": row_issues,
                "issue_count": len(row_issues),
            }
        )

    payload: dict[str, Any] = {
        "dataset_id": dataset_id_value,
        "sport_key": spec.sport_key,
        "markets": sorted(set(spec.markets)),
        "regions": spec.regions,
        "bookmakers": spec.bookmakers,
        "historical": bool(spec.historical),
        "available_day_count": len(available_days),
        "available_from_day": available_days[0] if available_days else "",
        "available_to_day": available_days[-1] if available_days else "",
        "checked_days": len(selected_days),
        "checked_complete_days": checked_complete_days,
        "issue_count": issue_count,
        "require_complete": bool(getattr(args, "require_complete", False)),
        "require_parquet": bool(getattr(args, "require_parquet", False)),
        "days": day_reports,
        "generated_at_utc": iso_z(_utc_now()),
    }

    if bool(getattr(args, "json_output", False)):
        print(json.dumps(payload, sort_keys=True))
    else:
        print(
            "dataset_id={} checked_days={} checked_complete_days={} issue_count={} "
            "require_complete={} require_parquet={}".format(
                dataset_id_value,
                len(selected_days),
                checked_complete_days,
                issue_count,
                str(bool(getattr(args, "require_complete", False))).lower(),
                str(bool(getattr(args, "require_parquet", False))).lower(),
            )
        )
        for row in day_reports:
            issue_codes = ",".join(
                str(item.get("code", ""))
                for item in row.get("issues", [])
                if isinstance(item, dict) and str(item.get("code", "")).strip()
            )
            print(
                "day={} complete={} reason={} coverage={} issues={} issue_codes={}".format(
                    str(row.get("day", "")),
                    str(bool(row.get("complete", False))).lower(),
                    _incomplete_reason_code(row)
                    if not bool(row.get("complete", False))
                    else "complete",
                    f"{float(row.get('odds_coverage_ratio', 0.0)):.3f}",
                    int(row.get("issue_count", 0)),
                    issue_codes,
                )
            )

    return 2 if issue_count else 0


def _latest_snapshot_id(store: SnapshotStore) -> str:
    snapshots = sorted(path for path in store.snapshots_dir.iterdir() if path.is_dir())
    if not snapshots:
        raise CLIError("no snapshots found")
    return snapshots[-1].name


def _teams_in_scope(event_context: dict[str, dict[str, str]]) -> set[str]:
    teams: set[str] = set()
    for row in event_context.values():
        if not isinstance(row, dict):
            continue
        home = str(row.get("home_team", "")).strip()
        away = str(row.get("away_team", "")).strip()
        if home:
            teams.add(home)
        if away:
            teams.add(away)
    return teams


def _teams_in_scope_from_events(events: list[dict[str, Any]]) -> set[str]:
    return teams_in_scope_from_events(events)


def _official_rows_count(official: dict[str, Any]) -> int:
    return official_rows_count(official)


def _official_source_ready(official: dict[str, Any]) -> bool:
    return official_source_ready(official)


def _secondary_source_ready(secondary: dict[str, Any]) -> bool:
    return secondary_source_ready(secondary)


def _allow_secondary_injuries_override(*, cli_flag: bool) -> bool:
    return cli_flag or _env_bool("PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES", False)


def _official_injury_hard_fail_message() -> str:
    return (
        "official injury report unavailable; refusing to continue without override. "
        "Use --allow-secondary-injuries or set "
        "PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES=true to allow secondary fallback."
    )


def _preflight_context_for_snapshot(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    teams_in_scope: set[str],
    refresh_context: bool,
    require_official_injuries: bool,
    allow_secondary_injuries: bool,
    require_fresh_context: bool,
    injuries_stale_hours: float,
    roster_stale_hours: float,
) -> dict[str, Any]:
    repo = NBARepository.from_store(store=store, snapshot_id=snapshot_id)
    injuries, roster, injuries_path, roster_path = repo.load_strategy_context(
        teams_in_scope=sorted(teams_in_scope),
        offline=False,
        refresh=refresh_context,
        injuries_stale_hours=injuries_stale_hours,
        roster_stale_hours=roster_stale_hours,
    )

    official = injuries.get("official", {}) if isinstance(injuries, dict) else {}
    secondary = injuries.get("secondary", {}) if isinstance(injuries, dict) else {}
    health_gates: list[str] = []
    official_ready = _official_source_ready(official) if isinstance(official, dict) else False
    secondary_ready = _secondary_source_ready(secondary) if isinstance(secondary, dict) else False
    if (
        require_official_injuries
        and not official_ready
        and not (allow_secondary_injuries and secondary_ready)
    ):
        health_gates.append("official_injury_missing")
    injuries_stale = bool(injuries.get("stale", False)) if isinstance(injuries, dict) else True
    roster_stale = bool(roster.get("stale", False)) if isinstance(roster, dict) else True
    if require_fresh_context and injuries_stale:
        health_gates.append("injuries_context_stale")
    if require_fresh_context and roster_stale:
        health_gates.append("roster_context_stale")

    return {
        "health_gates": health_gates,
        "injuries_status": (
            str(official.get("status", "missing")) if isinstance(official, dict) else "missing"
        ),
        "roster_status": (
            str(roster.get("status", "missing")) if isinstance(roster, dict) else "missing"
        ),
        "injuries_path": str(injuries_path),
        "roster_path": str(roster_path),
    }


def _strategy_policy_from_env() -> dict[str, Any]:
    return {
        "require_official_injuries": _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True),
        "allow_secondary_injuries": _env_bool(
            "PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES",
            False,
        ),
        "stale_quote_minutes": _env_int("PROP_EV_STRATEGY_STALE_QUOTE_MINUTES", 20),
        "injuries_stale_hours": _env_float("PROP_EV_CONTEXT_INJURIES_STALE_HOURS", 6.0),
        "roster_stale_hours": _env_float("PROP_EV_CONTEXT_ROSTER_STALE_HOURS", 24.0),
        "require_fresh_context": _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True),
    }


def _resolve_strategy_runtime_policy(
    *, mode: str, stale_quote_minutes: int, require_fresh_context: bool
) -> tuple[str, int, bool]:
    mode_key = str(mode).strip().lower() or "auto"
    if mode_key not in {"auto", "live", "replay"}:
        raise CLIError(f"invalid strategy run mode: {mode}")
    resolved_stale = int(stale_quote_minutes)
    resolved_fresh_context = bool(require_fresh_context)
    if mode_key == "replay":
        resolved_stale = max(resolved_stale, 1_000_000)
        resolved_fresh_context = False
    return mode_key, resolved_stale, resolved_fresh_context


def _execution_projection_tag(bookmakers: tuple[str, ...]) -> str:
    cleaned = [re.sub(r"[^a-z0-9._-]+", "-", book.strip().lower()) for book in bookmakers]
    cleaned = [book.strip("._-") for book in cleaned if book.strip("._-")]
    if not cleaned:
        return "execution"
    return f"execution-{'-'.join(cleaned)}"


def _load_strategy_context(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    teams_in_scope: list[str],
    offline: bool,
    refresh_context: bool,
    injuries_stale_hours: float,
    roster_stale_hours: float,
) -> tuple[dict[str, Any], dict[str, Any], Path, Path]:
    repo = NBARepository.from_store(store=store, snapshot_id=snapshot_id)
    injuries, roster, injuries_path, roster_path = repo.load_strategy_context(
        teams_in_scope=teams_in_scope,
        offline=offline,
        refresh=refresh_context,
        injuries_stale_hours=injuries_stale_hours,
        roster_stale_hours=roster_stale_hours,
    )
    return injuries, roster, injuries_path, roster_path


def _coerce_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _coerce_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _count_status(candidates: list[dict[str, Any]], *, field: str, value: str) -> int:
    count = 0
    for row in candidates:
        if not isinstance(row, dict):
            continue
        if str(row.get(field, "")) == value:
            count += 1
    return count


def _health_recommendations(
    *, status: str, gates: list[str], missing_injury: int, stale_inputs: int
) -> list[str]:
    recommendations: list[str] = []
    if status == "broken":
        recommendations.append("Do not produce picks until all broken checks pass.")
        recommendations.append(
            "Run `prop-ev strategy health --refresh-context` to rebuild context."
        )
    elif status == "degraded":
        recommendations.append("Watchlist-only is recommended until degraded checks clear.")
        recommendations.append("Re-run with `--refresh-context` and verify source freshness.")
    else:
        recommendations.append("All required checks passed; strategy run can proceed normally.")
    if stale_inputs > 0:
        recommendations.append("Refresh context and odds snapshots to clear stale input flags.")
    if "roster_fallback_used" in gates:
        recommendations.append(
            "Monitor roster fallback usage; primary roster feed did not fully cover scope."
        )
    if "official_injury_secondary_override" in gates:
        recommendations.append(
            "Official injury source override is active; monitor for official report recovery."
        )
    if missing_injury > 0:
        recommendations.append(
            "Missing injury rows are informational in this mode; "
            "review counts before increasing exposure."
        )
    return recommendations


def _cmd_strategy_health(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    manifest = store.load_manifest(snapshot_id)
    derived_path = snapshot_dir / "derived" / "event_props.jsonl"
    if not derived_path.exists():
        raise CLIError(f"missing derived props file: {derived_path}")

    rows = load_jsonl(derived_path)
    event_context = _load_event_context(store, snapshot_id, manifest)
    slate_rows = _load_slate_rows(store, snapshot_id)
    policy = _strategy_policy_from_env()
    allow_secondary_injuries = _allow_secondary_injuries_override(
        cli_flag=bool(getattr(args, "allow_secondary_injuries", False))
    )
    teams_in_scope = sorted(_teams_in_scope(event_context))
    injuries, roster, injuries_path, roster_path = _load_strategy_context(
        store=store,
        snapshot_id=snapshot_id,
        teams_in_scope=teams_in_scope,
        offline=bool(args.offline),
        refresh_context=bool(args.refresh_context),
        injuries_stale_hours=float(policy["injuries_stale_hours"]),
        roster_stale_hours=float(policy["roster_stale_hours"]),
    )
    official_for_policy = (
        _coerce_dict(injuries.get("official")) if isinstance(injuries, dict) else {}
    )
    secondary_for_policy = (
        _coerce_dict(injuries.get("secondary")) if isinstance(injuries, dict) else {}
    )
    official_ready_for_policy = _official_source_ready(official_for_policy)
    secondary_ready_for_policy = _secondary_source_ready(secondary_for_policy)
    injury_override_active = (
        allow_secondary_injuries and secondary_ready_for_policy and not official_ready_for_policy
    )
    effective_require_official = bool(policy["require_official_injuries"]) and not (
        injury_override_active
    )

    report = build_strategy_report(
        snapshot_id=snapshot_id,
        manifest=manifest,
        rows=rows,
        top_n=5,
        injuries=injuries,
        roster=roster,
        event_context=event_context,
        slate_rows=slate_rows,
        player_identity_map=None,
        min_ev=0.01,
        allow_tier_b=False,
        require_official_injuries=effective_require_official,
        stale_quote_minutes=int(policy["stale_quote_minutes"]),
        require_fresh_context=bool(policy["require_fresh_context"]),
    )

    health_report = _coerce_dict(report.get("health_report"))
    official = official_for_policy
    secondary = secondary_for_policy
    roster_details = _coerce_dict(roster) if isinstance(roster, dict) else {}
    candidates = [row for row in _coerce_list(report.get("candidates")) if isinstance(row, dict)]
    contracts = _coerce_dict(health_report.get("contracts"))
    props_contract = _coerce_dict(contracts.get("props_rows"))
    odds_health = _coerce_dict(health_report.get("odds"))

    unknown_event = _count_status(candidates, field="roster_status", value="unknown_event")
    unknown_roster = _count_status(candidates, field="roster_status", value="unknown_roster")
    missing_injury = _count_status(candidates, field="injury_status", value="unknown")
    stale_inputs = int(bool(injuries.get("stale", False))) if isinstance(injuries, dict) else 1
    stale_inputs += int(bool(roster.get("stale", False))) if isinstance(roster, dict) else 1
    stale_inputs += int(bool(odds_health.get("odds_stale", False)))

    missing_event_mappings = [
        value
        for value in _coerce_list(contracts.get("missing_event_mappings"))
        if isinstance(value, str) and value
    ]
    missing_roster_teams = [
        value
        for value in _coerce_list(roster_details.get("missing_roster_teams"))
        if isinstance(value, str) and value
    ]
    roster_fallback = _coerce_dict(roster_details.get("fallback"))
    roster_fallback_used = bool(roster_fallback)
    roster_fallback_ok = str(roster_fallback.get("status", "")) == "ok"
    fallback_count_teams = int(roster_fallback.get("count_teams", 0) or 0)
    fallback_covers_missing = (
        roster_fallback_used
        and roster_fallback_ok
        and fallback_count_teams >= len(missing_roster_teams)
    )
    official_rows_count = _official_rows_count(official)
    official_parse_status = str(official.get("parse_status", ""))
    official_parse_coverage_raw = official.get("parse_coverage", 0.0)
    if isinstance(official_parse_coverage_raw, (int, float)):
        official_parse_coverage = float(official_parse_coverage_raw)
    elif isinstance(official_parse_coverage_raw, str):
        try:
            official_parse_coverage = float(official_parse_coverage_raw.strip())
        except ValueError:
            official_parse_coverage = 0.0
    else:
        official_parse_coverage = 0.0

    injury_check_pass = (official_ready_for_policy or injury_override_active) and len(
        _coerce_list(official.get("pdf_links"))
    ) > 0
    roster_check_pass = (
        str(roster_details.get("status", "")) == "ok"
        and int(roster_details.get("count_teams", 0)) > 0
        and (not missing_roster_teams or fallback_covers_missing)
    )
    mapping_check_pass = (
        len(missing_event_mappings) == 0
        and unknown_event == 0
        and int(props_contract.get("invalid_count", 0)) == 0
    )

    broken_gates: list[str] = []
    degraded_gates: list[str] = []
    if not injury_check_pass:
        broken_gates.append("injury_source_failed")
    if not roster_check_pass:
        broken_gates.append("roster_source_failed")
    if not mapping_check_pass:
        broken_gates.append("event_mapping_failed")
    if stale_inputs > 0:
        degraded_gates.append("stale_inputs")
    if unknown_roster > 0:
        degraded_gates.append("unknown_roster_detected")
    if roster_fallback_used:
        degraded_gates.append("roster_fallback_used")
    if injury_override_active:
        degraded_gates.append("official_injury_secondary_override")
    for gate in _coerce_list(health_report.get("health_gates")):
        if (
            isinstance(gate, str)
            and gate
            and gate not in broken_gates
            and gate not in degraded_gates
        ):
            degraded_gates.append(gate)

    gates = broken_gates + [gate for gate in degraded_gates if gate not in broken_gates]
    if broken_gates:
        status = "broken"
        exit_code = 2
    elif degraded_gates:
        status = "degraded"
        exit_code = 1
    else:
        status = "healthy"
        exit_code = 0

    checks = {
        "injuries": {
            "pass": injury_check_pass,
            "status": str(official.get("status", "missing")),
            "count": int(official.get("count", 0)),
            "pdf_links": len(_coerce_list(official.get("pdf_links"))),
            "rows_count": official_rows_count,
            "parse_status": official_parse_status,
            "parse_coverage": official_parse_coverage,
            "secondary_override": injury_override_active,
        },
        "roster": {
            "pass": roster_check_pass,
            "status": str(roster_details.get("status", "missing")),
            "count_teams": int(roster_details.get("count_teams", 0)),
            "missing_roster_teams": missing_roster_teams,
            "fallback_used": roster_fallback_used,
            "fallback_status": str(roster_fallback.get("status", "")) if roster_fallback else "",
            "fallback_covers_missing": fallback_covers_missing,
        },
        "event_mapping": {
            "pass": mapping_check_pass,
            "missing_event_mappings": missing_event_mappings,
            "unknown_event": unknown_event,
            "invalid_props_rows": int(props_contract.get("invalid_count", 0)),
        },
        "freshness": {
            "pass": stale_inputs == 0,
            "stale_inputs": stale_inputs,
            "injuries_stale": bool(injuries.get("stale", False))
            if isinstance(injuries, dict)
            else True,
            "roster_stale": bool(roster.get("stale", False)) if isinstance(roster, dict) else True,
            "odds_stale": bool(odds_health.get("odds_stale", False)),
        },
    }

    counts = {
        "unknown_event": unknown_event,
        "unknown_roster": unknown_roster,
        "missing_injury": missing_injury,
        "stale_inputs": stale_inputs,
    }
    source_details = {
        "injuries": {
            "source": str(official.get("source", "")),
            "url": str(official.get("url", "")),
            "status": str(official.get("status", "missing")),
            "fetched_at_utc": str(official.get("fetched_at_utc", "")),
            "stale": bool(injuries.get("stale", False)) if isinstance(injuries, dict) else True,
            "pdf_download_status": str(official.get("pdf_download_status", "")),
            "selected_pdf_url": str(official.get("selected_pdf_url", "")),
            "count": int(official.get("count", 0)),
            "rows_count": official_rows_count,
            "parse_status": official_parse_status,
            "parse_coverage": official_parse_coverage,
            "report_generated_at_utc": str(official.get("report_generated_at_utc", "")),
            "secondary_override": injury_override_active,
            "secondary_status": str(secondary.get("status", "missing")),
            "secondary_count": int(secondary.get("count", 0) or 0),
        },
        "roster": {
            "source": str(roster_details.get("source", "")),
            "url": str(roster_details.get("url", "")),
            "status": str(roster_details.get("status", "missing")),
            "fetched_at_utc": str(roster_details.get("fetched_at_utc", "")),
            "stale": bool(roster.get("stale", False)) if isinstance(roster, dict) else True,
            "count_teams": int(roster_details.get("count_teams", 0)),
            "missing_roster_teams": missing_roster_teams,
            "fallback": {
                "used": roster_fallback_used,
                "status": str(roster_fallback.get("status", "")) if roster_fallback else "",
                "count_teams": (
                    int(roster_fallback.get("count_teams", 0)) if roster_fallback_ok else 0
                ),
                "covers_missing": fallback_covers_missing,
            },
        },
        "mapping": {
            "events_in_rows": len(
                {
                    str(row.get("event_id", ""))
                    for row in rows
                    if isinstance(row, dict) and str(row.get("event_id", "")).strip()
                }
            ),
            "events_in_context": len(event_context),
            "missing_event_mappings": missing_event_mappings,
        },
        "odds": {
            "status": str(odds_health.get("status", "")),
            "latest_quote_utc": str(odds_health.get("latest_quote_utc", "")),
            "age_latest_min": odds_health.get("age_latest_min"),
            "stale_after_min": int(policy["stale_quote_minutes"]),
        },
    }
    payload = {
        "status": status,
        "exit_code": exit_code,
        "snapshot_id": snapshot_id,
        "run_date_utc": _iso(_utc_now()),
        "checks": checks,
        "counts": counts,
        "gates": gates,
        "source_details": source_details,
        "recommendations": _health_recommendations(
            status=status,
            gates=gates,
            missing_injury=missing_injury,
            stale_inputs=stale_inputs,
        ),
        "paths": {
            "injuries_context": str(injuries_path),
            "roster_context": str(roster_path),
        },
        "state_key": strategy_health_state_key(),
    }
    if bool(getattr(args, "json_output", True)):
        print(json.dumps(payload, sort_keys=True, indent=2))
    else:
        print(
            "snapshot_id={} status={} exit_code={} gates={}".format(
                snapshot_id,
                status,
                exit_code,
                ",".join(gates) if gates else "none",
            )
        )
    return exit_code


def _build_discovery_execution_report(
    *,
    discovery_snapshot_id: str,
    execution_snapshot_id: str,
    discovery_report: dict[str, Any],
    execution_report: dict[str, Any],
    top_n: int,
) -> dict[str, Any]:
    return build_discovery_execution_report(
        discovery_snapshot_id=discovery_snapshot_id,
        execution_snapshot_id=execution_snapshot_id,
        discovery_report=discovery_report,
        execution_report=execution_report,
        top_n=top_n,
    )


def _write_discovery_execution_reports(
    *,
    store: SnapshotStore,
    execution_snapshot_id: str,
    report: dict[str, Any],
) -> tuple[Path, Path]:
    return write_discovery_execution_reports(
        store=store,
        execution_snapshot_id=execution_snapshot_id,
        report=report,
    )


def _load_strategy_inputs(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    offline: bool,
    block_paid: bool,
    refresh_context: bool,
) -> tuple[
    Path,
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, dict[str, str]],
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    snapshot_dir = store.snapshot_dir(snapshot_id)
    manifest = store.load_manifest(snapshot_id)
    derived_path = snapshot_dir / "derived" / "event_props.jsonl"
    if not derived_path.exists():
        raise CLIError(f"missing derived props file: {derived_path}")

    rows = load_jsonl(derived_path)
    event_context = _load_event_context(store, snapshot_id, manifest)
    slate_rows = _load_slate_rows(store, snapshot_id)
    if not slate_rows and not offline and not block_paid:
        _hydrate_slate_for_strategy(store, snapshot_id, manifest)
        manifest = store.load_manifest(snapshot_id)
        event_context = _load_event_context(store, snapshot_id, manifest)
        slate_rows = _load_slate_rows(store, snapshot_id)

    injuries_stale_hours = _env_float("PROP_EV_CONTEXT_INJURIES_STALE_HOURS", 6.0)
    roster_stale_hours = _env_float("PROP_EV_CONTEXT_ROSTER_STALE_HOURS", 24.0)
    reference_dir = store.root / "reference"
    identity_map_path = reference_dir / "player_identity_map.json"
    teams_in_scope = sorted(_teams_in_scope(event_context))
    injuries, roster, _, _ = _load_strategy_context(
        store=store,
        snapshot_id=snapshot_id,
        teams_in_scope=teams_in_scope,
        offline=offline,
        refresh_context=refresh_context,
        injuries_stale_hours=injuries_stale_hours,
        roster_stale_hours=roster_stale_hours,
    )
    update_identity_map(
        path=identity_map_path,
        rows=rows,
        roster=roster if isinstance(roster, dict) else None,
        event_context=event_context,
    )
    player_identity_map = load_identity_map(identity_map_path)
    return (
        snapshot_dir,
        manifest,
        rows,
        event_context,
        slate_rows,
        injuries,
        roster,
        player_identity_map,
    )


def _cmd_strategy_run(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    allow_secondary_injuries = _allow_secondary_injuries_override(
        cli_flag=bool(getattr(args, "allow_secondary_injuries", False))
    )
    stale_quote_minutes_env = _env_int("PROP_EV_STRATEGY_STALE_QUOTE_MINUTES", 20)
    require_fresh_context_env = _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True)
    (
        strategy_run_mode,
        stale_quote_minutes,
        require_fresh_context,
    ) = _resolve_strategy_runtime_policy(
        mode=str(getattr(args, "mode", "auto")),
        stale_quote_minutes=stale_quote_minutes_env,
        require_fresh_context=require_fresh_context_env,
    )
    (
        snapshot_dir,
        manifest,
        rows,
        event_context,
        slate_rows,
        injuries,
        roster,
        player_identity_map,
    ) = _load_strategy_inputs(
        store=store,
        snapshot_id=snapshot_id,
        offline=bool(args.offline),
        block_paid=bool(getattr(args, "block_paid", False)),
        refresh_context=bool(args.refresh_context),
    )
    official = _coerce_dict(injuries.get("official")) if isinstance(injuries, dict) else {}
    secondary = _coerce_dict(injuries.get("secondary")) if isinstance(injuries, dict) else {}
    official_ready = _official_source_ready(official)
    secondary_ready = _secondary_source_ready(secondary)
    secondary_override_active = allow_secondary_injuries and secondary_ready and not official_ready
    if require_official_injuries and not official_ready and not secondary_override_active:
        raise CLIError(_official_injury_hard_fail_message())
    effective_require_official = require_official_injuries and not secondary_override_active
    if secondary_override_active:
        print("note=official_injury_missing_using_secondary_override")

    strategy_requested = str(getattr(args, "strategy", "s001"))
    plugin = get_strategy(strategy_requested)
    config = StrategyRunConfig(
        top_n=int(args.top_n),
        min_ev=float(args.min_ev),
        allow_tier_b=bool(args.allow_tier_b),
        require_official_injuries=bool(effective_require_official),
        stale_quote_minutes=int(stale_quote_minutes),
        require_fresh_context=bool(require_fresh_context),
    )
    inputs = StrategyInputs(
        snapshot_id=snapshot_id,
        manifest=manifest,
        rows=rows,
        injuries=injuries if isinstance(injuries, dict) else None,
        roster=roster if isinstance(roster, dict) else None,
        event_context=event_context if isinstance(event_context, dict) else None,
        slate_rows=slate_rows,
        player_identity_map=player_identity_map if isinstance(player_identity_map, dict) else None,
    )
    result = plugin.run(inputs=inputs, config=config)
    report = decorate_report(result.report, strategy=plugin.info, config=result.config)
    strategy_id = normalize_strategy_id(plugin.info.id)
    write_canonical_raw = getattr(args, "write_canonical", None)
    if write_canonical_raw is None:
        write_canonical = bool(strategy_id == "s001")
    else:
        write_canonical = bool(write_canonical_raw)

    json_path, md_path = write_strategy_reports(
        snapshot_dir=snapshot_dir,
        report=report,
        top_n=args.top_n,
        strategy_id=strategy_id,
        write_canonical=write_canonical,
    )
    backtest = write_backtest_artifacts(
        snapshot_dir=snapshot_dir,
        report=report,
        selection="eligible",
        top_n=0,
        strategy_id=strategy_id,
        write_canonical=write_canonical,
    )
    summary = report.get("summary", {})
    health = (
        report.get("health_report", {}) if isinstance(report.get("health_report"), dict) else {}
    )
    health_gates = (
        health.get("health_gates", []) if isinstance(health.get("health_gates"), list) else []
    )
    print(
        (
            "snapshot_id={} strategy_status={} strategy_mode={} events={} candidate_lines={} "
            "tier_a={} tier_b={} eligible={}"
        ).format(
            snapshot_id,
            report.get("strategy_status", ""),
            report.get("strategy_mode", ""),
            summary.get("events", 0),
            summary.get("candidate_lines", 0),
            summary.get("tier_a_lines", 0),
            summary.get("tier_b_lines", 0),
            summary.get("eligible_lines", 0),
        )
    )
    print(f"strategy_id={strategy_id}")
    title = strategy_title(strategy_id)
    if title:
        print(f"strategy_title={title}")
    print(f"strategy_run_mode={strategy_run_mode}")
    print(f"stale_quote_minutes={stale_quote_minutes}")
    print(f"require_fresh_context={str(bool(require_fresh_context)).lower()}")
    print(f"health_gates={','.join(health_gates) if health_gates else 'none'}")
    print(f"report_json={json_path}")
    print(f"report_md={md_path}")
    card = snapshot_dir / "reports" / "strategy-card.md"
    if not write_canonical:
        card = card.with_name(f"{card.stem}.{strategy_id}{card.suffix}")
    print(f"report_card={card}")
    print(f"backtest_seed_jsonl={backtest['seed_jsonl']}")
    print(f"backtest_results_template_csv={backtest['results_template_csv']}")
    print(f"backtest_readiness_json={backtest['readiness_json']}")
    reference_dir = store.root / "reference"
    identity_map_path = reference_dir / "player_identity_map.json"
    identity_map = load_identity_map(identity_map_path)
    entries = (
        len(identity_map.get("players", {})) if isinstance(identity_map.get("players"), dict) else 0
    )
    print(f"identity_map={identity_map_path} entries={entries}")
    print(f"injuries_context={snapshot_dir / 'context' / 'injuries.json'}")
    print(f"roster_context={snapshot_dir / 'context' / 'roster.json'}")

    execution_books = tuple(parse_csv(str(getattr(args, "execution_bookmakers", ""))))
    if execution_books:
        execution_top_n_raw = int(getattr(args, "execution_top_n", 0))
        execution_top_n = execution_top_n_raw if execution_top_n_raw > 0 else int(args.top_n)
        execution_config = ExecutionProjectionConfig(
            bookmakers=execution_books,
            top_n=max(0, execution_top_n),
            requires_pre_bet_ready=bool(getattr(args, "execution_requires_pre_bet_ready", False)),
            requires_meets_play_to=bool(getattr(args, "execution_requires_meets_play_to", False)),
            tier_a_min_ev=float(getattr(args, "execution_tier_a_min_ev", 0.03)),
            tier_b_min_ev=float(getattr(args, "execution_tier_b_min_ev", 0.05)),
        )
        projected_report = project_execution_report(
            report=report,
            event_prop_rows=rows,
            config=execution_config,
        )
        execution_tag = _execution_projection_tag(execution_books)
        execution_json, execution_md = write_tagged_strategy_reports(
            snapshot_dir=snapshot_dir,
            report=projected_report,
            top_n=max(0, execution_top_n),
            tag=execution_tag,
        )
        execution_summary = (
            projected_report.get("summary", {})
            if isinstance(projected_report.get("summary"), dict)
            else {}
        )
        print(f"execution_bookmakers={','.join(execution_books)}")
        print(f"execution_tag={execution_tag}")
        print(
            "execution_candidates={} execution_eligible={}".format(
                execution_summary.get("candidate_lines", 0),
                execution_summary.get("eligible_lines", 0),
            )
        )
        print(f"execution_report_json={execution_json}")
        print(f"execution_report_md={execution_md}")
    return 0


def _cmd_strategy_ls(args: argparse.Namespace) -> int:
    del args
    for plugin in list_strategies():
        strategy_id = normalize_strategy_id(plugin.info.id)
        print(f"{strategy_id}\t{plugin.info.name}\t{plugin.info.description}")
    return 0


def _parse_strategy_ids(raw: str) -> list[str]:
    values = [item.strip() for item in (raw or "").split(",") if item.strip()]
    seen: set[str] = set()
    parsed: list[str] = []
    for value in values:
        plugin = get_strategy(resolve_strategy_id(value))
        canonical_id = normalize_strategy_id(plugin.info.id)
        if canonical_id in seen:
            continue
        seen.add(canonical_id)
        parsed.append(canonical_id)
    return parsed


def _ranked_key(row: dict[str, Any]) -> tuple[str, str, str, float, str]:
    event_id = str(row.get("event_id", "")).strip()
    player = str(row.get("player", "")).strip()
    market = str(row.get("market", "")).strip()
    point_raw = row.get("point")
    point = float(point_raw) if isinstance(point_raw, (int, float)) else 0.0
    side = str(row.get("recommended_side", "")).strip().lower()
    return (event_id, player, market, point, side)


def _render_strategy_compare_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    rows = report.get("strategies", []) if isinstance(report.get("strategies"), list) else []
    overlap = (
        report.get("ranked_overlap", {}) if isinstance(report.get("ranked_overlap"), dict) else {}
    )

    lines: list[str] = []
    lines.append("# Strategy Compare")
    lines.append("")
    lines.append(f"- snapshot_id: `{summary.get('snapshot_id', '')}`")
    lines.append(f"- strategies: `{summary.get('strategy_count', 0)}`")
    lines.append(f"- ranked_top_n: `{summary.get('top_n', 0)}`")
    lines.append("")

    if rows:
        lines.append("| Strategy | Mode | Eligible | Candidate | TierA | TierB | Gates |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for row in rows:
            if not isinstance(row, dict):
                continue
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} |".format(
                    row.get("strategy_id", ""),
                    row.get("strategy_mode", ""),
                    row.get("eligible_lines", 0),
                    row.get("candidate_lines", 0),
                    row.get("tier_a_lines", 0),
                    row.get("tier_b_lines", 0),
                    row.get("health_gate_count", 0),
                )
            )
        lines.append("")

    lines.append("## Ranked Overlap")
    lines.append("")
    lines.append(f"- intersection_all: `{overlap.get('intersection_all', 0)}`")
    lines.append(f"- union_all: `{overlap.get('union_all', 0)}`")
    lines.append("")
    return "\n".join(lines)


def _cmd_strategy_compare(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)

    strategy_ids = _parse_strategy_ids(getattr(args, "strategies", ""))
    if len(strategy_ids) < 2:
        raise CLIError("compare requires --strategies with at least 2 unique ids")

    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    stale_quote_minutes = _env_int("PROP_EV_STRATEGY_STALE_QUOTE_MINUTES", 20)
    require_fresh_context = _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True)

    (
        snapshot_dir,
        manifest,
        rows,
        event_context,
        slate_rows,
        injuries,
        roster,
        player_identity_map,
    ) = _load_strategy_inputs(
        store=store,
        snapshot_id=snapshot_id,
        offline=bool(args.offline),
        block_paid=bool(getattr(args, "block_paid", False)),
        refresh_context=bool(args.refresh_context),
    )

    base_config = StrategyRunConfig(
        top_n=int(args.top_n),
        min_ev=float(args.min_ev),
        allow_tier_b=bool(args.allow_tier_b),
        require_official_injuries=bool(require_official_injuries),
        stale_quote_minutes=int(stale_quote_minutes),
        require_fresh_context=bool(require_fresh_context),
    )
    inputs = StrategyInputs(
        snapshot_id=snapshot_id,
        manifest=manifest,
        rows=rows,
        injuries=injuries if isinstance(injuries, dict) else None,
        roster=roster if isinstance(roster, dict) else None,
        event_context=event_context if isinstance(event_context, dict) else None,
        slate_rows=slate_rows,
        player_identity_map=player_identity_map if isinstance(player_identity_map, dict) else None,
    )

    compare_rows: list[dict[str, Any]] = []
    ranked_sets: dict[str, set[tuple[str, str, str, float, str]]] = {}
    for requested in strategy_ids:
        plugin = get_strategy(requested)
        result = plugin.run(inputs=inputs, config=base_config)
        report = decorate_report(result.report, strategy=plugin.info, config=result.config)
        strategy_id = normalize_strategy_id(report.get("strategy_id", plugin.info.id))

        write_strategy_reports(
            snapshot_dir=snapshot_dir,
            report=report,
            top_n=int(args.top_n),
            strategy_id=strategy_id,
            write_canonical=False,
        )
        write_backtest_artifacts(
            snapshot_dir=snapshot_dir,
            report=report,
            selection="eligible",
            top_n=0,
            strategy_id=strategy_id,
            write_canonical=False,
        )

        summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
        compare_rows.append(
            {
                "strategy_id": strategy_id,
                "strategy_mode": str(report.get("strategy_mode", "")),
                "candidate_lines": int(summary.get("candidate_lines", 0)),
                "eligible_lines": int(summary.get("eligible_lines", 0)),
                "tier_a_lines": int(summary.get("tier_a_lines", 0)),
                "tier_b_lines": int(summary.get("tier_b_lines", 0)),
                "health_gate_count": int(summary.get("health_gate_count", 0)),
            }
        )

        ranked = (
            report.get("ranked_plays", []) if isinstance(report.get("ranked_plays"), list) else []
        )
        ranked_sets[strategy_id] = {
            _ranked_key(row)
            for row in ranked
            if isinstance(row, dict) and str(row.get("event_id", "")).strip()
        }

    intersection: set[tuple[str, str, str, float, str]] | None = None
    union: set[tuple[str, str, str, float, str]] = set()
    for keys in ranked_sets.values():
        union |= keys
        intersection = keys if intersection is None else (intersection & keys)
    intersection_count = len(intersection or set())

    compare_report = {
        "generated_at_utc": _iso(_utc_now()),
        "summary": {
            "snapshot_id": snapshot_id,
            "strategy_count": len(strategy_ids),
            "top_n": int(args.top_n),
        },
        "strategies": sorted(compare_rows, key=lambda row: row.get("strategy_id", "")),
        "ranked_overlap": {
            "intersection_all": intersection_count,
            "union_all": len(union),
        },
    }
    reports_dir = snapshot_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "strategy-compare.json"
    md_path = reports_dir / "strategy-compare.md"
    json_path.write_text(
        json.dumps(compare_report, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    md_path.write_text(_render_strategy_compare_markdown(compare_report), encoding="utf-8")

    print(f"snapshot_id={snapshot_id}")
    print(f"strategies={','.join(strategy_ids)}")
    print(f"compare_json={json_path}")
    print(f"compare_md={md_path}")
    return 0


def _render_backtest_summary_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    rows = report.get("strategies", []) if isinstance(report.get("strategies"), list) else []
    winner = report.get("winner", {}) if isinstance(report.get("winner"), dict) else {}

    lines: list[str] = []
    lines.append("# Backtest Summary")
    lines.append("")
    lines.append(f"- snapshot_id: `{summary.get('snapshot_id', '')}`")
    lines.append(f"- strategies: `{summary.get('strategy_count', 0)}`")
    lines.append(f"- min_graded: `{summary.get('min_graded', 0)}`")
    lines.append(f"- bin_size: `{summary.get('bin_size', '')}`")
    lines.append("")

    if rows:
        lines.append("| Strategy | Graded | ROI | W | L | P | Brier |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for row in rows:
            if not isinstance(row, dict):
                continue
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} |".format(
                    row.get("strategy_id", ""),
                    row.get("rows_graded", 0),
                    row.get("roi", ""),
                    row.get("wins", 0),
                    row.get("losses", 0),
                    row.get("pushes", 0),
                    row.get("brier", ""),
                )
            )
        lines.append("")

    if winner:
        lines.append("## Winner")
        lines.append("")
        lines.append(f"- strategy_id: `{winner.get('strategy_id', '')}`")
        lines.append(f"- roi: `{winner.get('roi', '')}`")
        lines.append(f"- rows_graded: `{winner.get('rows_graded', 0)}`")
        lines.append("")

    return "\n".join(lines)


def _cmd_strategy_backtest_summarize(args: argparse.Namespace) -> int:
    from prop_ev.backtest_summary import load_backtest_csv, pick_winner, summarize_backtest_rows

    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    reports_dir = snapshot_dir / "reports"

    bin_size = float(getattr(args, "bin_size", 0.05))
    min_graded = int(getattr(args, "min_graded", 0))

    paths: list[tuple[str, Path]] = []
    explicit_results = getattr(args, "results", None)
    if isinstance(explicit_results, list) and explicit_results:
        for raw in explicit_results:
            path = Path(str(raw))
            rows = load_backtest_csv(path)
            strategy = ""
            for row in rows:
                candidate = str(row.get("strategy_id", "")).strip()
                if candidate:
                    strategy = normalize_strategy_id(candidate)
                    break
            if not strategy:
                strategy = normalize_strategy_id(path.stem.replace(".", "_"))
            paths.append((strategy, path))
    else:
        strategy_ids = _parse_strategy_ids(getattr(args, "strategies", ""))
        if not strategy_ids:
            raise CLIError("backtest-summarize requires --strategies or --results")
        for strategy_id in strategy_ids:
            path = reports_dir / f"backtest-results-template.{strategy_id}.csv"
            if strategy_id == "s001" and not path.exists():
                path = reports_dir / "backtest-results-template.csv"
            paths.append((strategy_id, path))

    summaries: list[dict[str, Any]] = []
    computed = []
    for strategy_id, path in paths:
        if not path.exists():
            raise CLIError(f"missing backtest CSV: {path}")
        rows = load_backtest_csv(path)
        summary = summarize_backtest_rows(rows, strategy_id=strategy_id, bin_size=bin_size)
        computed.append(summary)
        summaries.append(summary.to_dict())

    winner = pick_winner(computed, min_graded=min_graded)
    report = {
        "generated_at_utc": _iso(_utc_now()),
        "summary": {
            "snapshot_id": snapshot_id,
            "strategy_count": len(summaries),
            "min_graded": min_graded,
            "bin_size": bin_size,
        },
        "strategies": sorted(summaries, key=lambda row: row.get("strategy_id", "")),
        "winner": winner.to_dict() if winner is not None else {},
    }

    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "backtest-summary.json"
    md_path = reports_dir / "backtest-summary.md"
    json_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(_render_backtest_summary_markdown(report), encoding="utf-8")

    print(f"snapshot_id={snapshot_id}")
    print(f"summary_json={json_path}")
    print(f"summary_md={md_path}")
    if winner is not None:
        print(
            f"winner_strategy_id={winner.strategy_id} roi={winner.roi} graded={winner.rows_graded}"
        )
    return 0


def _cmd_strategy_backtest_prep(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    reports_dir = snapshot_dir / "reports"
    requested = str(getattr(args, "strategy", "") or "").strip()
    write_canonical = True
    strategy_id: str | None = None
    report_path = reports_dir / "strategy-report.json"
    if requested:
        strategy_id = normalize_strategy_id(requested)
        suffixed_path = reports_dir / f"strategy-report.{strategy_id}.json"
        if suffixed_path.exists():
            report_path = suffixed_path
            write_canonical = False
        elif report_path.exists():
            canonical_payload = json.loads(report_path.read_text(encoding="utf-8"))
            if not isinstance(canonical_payload, dict):
                raise CLIError(f"invalid strategy report payload: {report_path}")
            canonical_id = normalize_strategy_id(str(canonical_payload.get("strategy_id", "s001")))
            if canonical_id != strategy_id:
                raise CLIError(
                    f"missing strategy report: {suffixed_path} (canonical is {canonical_id})"
                )
        else:
            report_path = suffixed_path
            write_canonical = False
    if not report_path.exists():
        raise CLIError(f"missing strategy report: {report_path}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise CLIError(f"invalid strategy report payload: {report_path}")

    result = write_backtest_artifacts(
        snapshot_dir=snapshot_dir,
        report=report,
        selection=args.selection,
        top_n=max(0, int(args.top_n)),
        strategy_id=strategy_id,
        write_canonical=write_canonical,
    )
    print(f"snapshot_id={snapshot_id}")
    print(f"selection_mode={result['selection_mode']} top_n={result['top_n']}")
    print(f"seed_rows={result['seed_rows']}")
    print(f"backtest_seed_jsonl={result['seed_jsonl']}")
    print(f"backtest_results_template_csv={result['results_template_csv']}")
    print(f"backtest_readiness_json={result['readiness_json']}")
    print(
        "ready_for_backtest_seed={} ready_for_settlement={}".format(
            result["ready_for_backtest_seed"], result["ready_for_settlement"]
        )
    )
    return 0


def _cmd_strategy_settle(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    seed_path = (
        Path(str(args.seed_path)).expanduser()
        if str(getattr(args, "seed_path", "")).strip()
        else snapshot_dir / "reports" / "backtest-seed.jsonl"
    )

    report = settle_snapshot(
        snapshot_dir=snapshot_dir,
        snapshot_id=snapshot_id,
        seed_path=seed_path,
        offline=bool(args.offline),
        refresh_results=bool(args.refresh_results),
        write_csv=bool(args.write_csv),
        results_source=getattr(args, "results_source", None),
    )

    if bool(getattr(args, "json_output", True)):
        print(json.dumps(report, sort_keys=True, indent=2))
    else:
        counts = report.get("counts", {}) if isinstance(report.get("counts"), dict) else {}
        artifacts = report.get("artifacts", {}) if isinstance(report.get("artifacts"), dict) else {}
        print(
            (
                "snapshot_id={} status={} exit_code={} total={} win={} loss={} push={} "
                "pending={} unresolved={} pdf_status={}"
            ).format(
                snapshot_id,
                report.get("status", ""),
                report.get("exit_code", 1),
                counts.get("total", 0),
                counts.get("win", 0),
                counts.get("loss", 0),
                counts.get("push", 0),
                counts.get("pending", 0),
                counts.get("unresolved", 0),
                report.get("pdf_status", ""),
            )
        )
        print(f"settlement_json={artifacts.get('json', '')}")
        print(f"settlement_md={artifacts.get('md', '')}")
        print(f"settlement_tex={artifacts.get('tex', '')}")
        print(f"settlement_pdf={artifacts.get('pdf', '')}")
        print(f"settlement_meta={artifacts.get('meta', '')}")
        csv_artifact = str(artifacts.get("csv", "")).strip()
        if csv_artifact:
            print(f"settlement_csv={csv_artifact}")

    return int(report.get("exit_code", 1))


def _load_slate_rows(store: SnapshotStore, snapshot_id: str) -> list[dict[str, Any]]:
    path = store.derived_path(snapshot_id, "featured_odds.jsonl")
    if not path.exists():
        return []
    return load_jsonl(path)


def _derive_window_from_events(
    event_context: dict[str, dict[str, str]] | None,
) -> tuple[str, str]:
    default_from, default_to = _default_window()
    if not isinstance(event_context, dict) or not event_context:
        return default_from, default_to

    times: list[datetime] = []
    for row in event_context.values():
        if not isinstance(row, dict):
            continue
        raw = str(row.get("commence_time", ""))
        if not raw:
            continue
        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        times.append(parsed.astimezone(UTC))

    if not times:
        return default_from, default_to

    start = min(times).replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)
    end = max(times).replace(minute=0, second=0, microsecond=0) + timedelta(hours=4)
    return _iso(start), _iso(end)


def _hydrate_slate_for_strategy(
    store: SnapshotStore, snapshot_id: str, manifest: dict[str, Any]
) -> None:
    run_config = manifest.get("run_config", {}) if isinstance(manifest, dict) else {}
    if not isinstance(run_config, dict):
        run_config = {}

    event_context = _load_event_context(store, snapshot_id, manifest)
    commence_from, commence_to = _derive_window_from_events(event_context)
    sport_key = str(run_config.get("sport_key", "basketball_nba")) or "basketball_nba"
    regions = str(run_config.get("regions", "us")) or "us"
    bookmakers = str(run_config.get("bookmakers", ""))

    args = argparse.Namespace(
        sport_key=sport_key,
        markets="spreads,totals",
        regions=regions,
        bookmakers=bookmakers,
        snapshot_id=snapshot_id,
        commence_from=commence_from,
        commence_to=commence_to,
        max_credits=10,
        force=False,
        refresh=False,
        resume=True,
        offline=False,
        dry_run=False,
    )
    code = _cmd_snapshot_slate(args)
    if code != 0:
        print(
            "warning: could not fetch slate featured odds during strategy run; "
            "continuing without spread/total context",
            file=sys.stderr,
        )


def _load_event_context(
    store: SnapshotStore, snapshot_id: str, manifest: dict[str, Any]
) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    requests = manifest.get("requests", {})
    if not isinstance(requests, dict):
        return result
    for request_key, row in requests.items():
        if not isinstance(row, dict):
            continue
        label = str(row.get("label", ""))
        payload = store.load_response(snapshot_id, str(request_key))
        if label == "events_list" and isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                event_id = str(item.get("id", ""))
                if not event_id:
                    continue
                result[event_id] = {
                    "home_team": str(item.get("home_team", "")),
                    "away_team": str(item.get("away_team", "")),
                    "commence_time": str(item.get("commence_time", "")),
                }
            continue

        if label == "slate_odds" and isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                event_id = str(item.get("id", ""))
                if not event_id:
                    continue
                result[event_id] = {
                    "home_team": str(item.get("home_team", "")),
                    "away_team": str(item.get("away_team", "")),
                    "commence_time": str(item.get("commence_time", "")),
                }
            continue

        if not label.startswith("event_odds:"):
            continue

        if not isinstance(payload, dict):
            continue
        event_id = str(payload.get("id", ""))
        if not event_id:
            continue
        result[event_id] = {
            "home_team": str(payload.get("home_team", "")),
            "away_team": str(payload.get("away_team", "")),
            "commence_time": str(payload.get("commence_time", "")),
        }
    return result


def _run_snapshot_props_for_playbook(args: argparse.Namespace, snapshot_id: str) -> int:
    snapshot_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets=args.markets,
        regions=args.regions,
        bookmakers=args.bookmakers,
        snapshot_id=snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        include_links=args.include_links,
        include_sids=args.include_sids,
        max_events=args.max_events,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=bool(getattr(args, "block_paid", False)),
        dry_run=False,
    )
    return _cmd_snapshot_props(snapshot_args)


def _run_snapshot_slate_for_playbook(args: argparse.Namespace, snapshot_id: str) -> int:
    snapshot_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets="spreads,totals",
        regions=args.regions,
        bookmakers=args.bookmakers,
        snapshot_id=snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        max_credits=max(2, min(args.max_credits, 10)),
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=bool(getattr(args, "block_paid", False)),
        dry_run=False,
    )
    return _cmd_snapshot_slate(snapshot_args)


def _run_snapshot_bundle_for_playbook(args: argparse.Namespace, snapshot_id: str) -> int:
    slate_code = _run_snapshot_slate_for_playbook(args, snapshot_id)
    props_code = _run_snapshot_props_for_playbook(args, snapshot_id)
    return 0 if slate_code == 0 and props_code == 0 else 2


def _run_strategy_for_playbook(
    *,
    snapshot_id: str,
    strategy_id: str,
    top_n: int,
    min_ev: float,
    allow_tier_b: bool,
    offline: bool,
    block_paid: bool,
    refresh_context: bool,
    strategy_mode: str = "auto",
    allow_secondary_injuries: bool = False,
    write_canonical: bool = True,
) -> int:
    strategy_args = argparse.Namespace(
        snapshot_id=snapshot_id,
        strategy=strategy_id,
        top_n=top_n,
        min_ev=min_ev,
        allow_tier_b=allow_tier_b,
        offline=offline,
        block_paid=block_paid,
        refresh_context=refresh_context,
        mode=strategy_mode,
        allow_secondary_injuries=allow_secondary_injuries,
        write_canonical=write_canonical,
        execution_bookmakers="",
        execution_top_n=0,
        execution_requires_pre_bet_ready=False,
        execution_requires_meets_play_to=False,
        execution_tier_a_min_ev=0.03,
        execution_tier_b_min_ev=0.05,
    )
    return _cmd_strategy_run(strategy_args)


def _cmd_playbook_run(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    settings = Settings.from_env()
    month = args.month or current_month_utc()
    strategy_id = _resolve_strategy_id(
        str(getattr(args, "strategy", "")),
        default_id=settings.strategy_default_id,
    )
    top_n = args.top_n if args.top_n > 0 else settings.playbook_top_n
    per_game_top_n = (
        args.per_game_top_n if args.per_game_top_n > 0 else settings.playbook_per_game_top_n
    )
    start_budget = budget_snapshot(store=store, settings=settings, month=month)
    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    allow_secondary_injuries = _allow_secondary_injuries_override(
        cli_flag=bool(getattr(args, "allow_secondary_injuries", False))
    )
    require_fresh_context = _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True)
    injuries_stale_hours = _env_float("PROP_EV_CONTEXT_INJURIES_STALE_HOURS", 6.0)
    roster_stale_hours = _env_float("PROP_EV_CONTEXT_ROSTER_STALE_HOURS", 24.0)

    snapshot_id = args.snapshot_id
    mode = "explicit_snapshot" if snapshot_id else ""
    live_window: dict[str, Any] = {
        "status": "not_checked",
        "within_window": False,
        "event_count": 0,
    }
    preflight_context: dict[str, Any] = {"status": "not_run", "health_gates": []}

    if not snapshot_id:
        if args.offline:
            snapshot_id = _latest_snapshot_id(store)
            mode = "offline_forced_latest"
        else:
            default_from, default_to = _default_window()
            commence_from = args.commence_from or default_from
            commence_to = args.commence_to or default_to
            events: list[dict[str, Any]] = []
            try:
                with OddsAPIClient(settings) as client:
                    response = client.list_events(
                        sport_key=args.sport_key,
                        commence_from=commence_from,
                        commence_to=commence_to,
                    )
                events_data = response.data if isinstance(response.data, list) else []
                events = [item for item in events_data if isinstance(item, dict)]
                live_window = compute_live_window(
                    events,
                    now=_utc_now(),
                    pre_tip_h=settings.playbook_live_window_pre_tip_h,
                    post_tip_h=settings.playbook_live_window_post_tip_h,
                )
            except (OddsAPIError, ValueError) as exc:
                live_window = {
                    "status": "events_lookup_failed",
                    "within_window": False,
                    "event_count": 0,
                    "error": str(exc),
                }

            if (
                bool(getattr(args, "exit_on_no_games", False))
                and str(live_window.get("status", "")) == "no_events"
            ):
                mode = "no_games_exit"
                print("snapshot_id=")
                print(f"mode={mode}")
                mode_desc = playbook_mode_key().get(mode, "")
                if mode_desc:
                    print(f"mode_desc={mode_desc}")
                print("event_count=0")
                print(f"within_window={str(bool(live_window.get('within_window', False))).lower()}")
                print("exit_reason=no_games")
                return 0

            odds_cap_reached = bool(start_budget["odds"].get("cap_reached", False))
            block_paid = bool(getattr(args, "block_paid", False))
            should_live = (
                bool(live_window.get("within_window", False))
                and not odds_cap_reached
                and not block_paid
            )
            if should_live:
                snapshot_id = make_snapshot_id()
                preflight_context = _preflight_context_for_snapshot(
                    store=store,
                    snapshot_id=snapshot_id,
                    teams_in_scope=_teams_in_scope_from_events(events),
                    refresh_context=args.refresh_context,
                    require_official_injuries=require_official_injuries,
                    allow_secondary_injuries=allow_secondary_injuries,
                    require_fresh_context=require_fresh_context,
                    injuries_stale_hours=injuries_stale_hours,
                    roster_stale_hours=roster_stale_hours,
                )
                preflight_gates = preflight_context.get("health_gates", [])
                if (
                    isinstance(preflight_gates, list)
                    and "official_injury_missing" in preflight_gates
                    and not allow_secondary_injuries
                ):
                    raise CLIError(_official_injury_hard_fail_message()) from None
                if isinstance(preflight_gates, list) and preflight_gates:
                    try:
                        snapshot_id = _latest_snapshot_id(store)
                        mode = "offline_context_gate"
                    except CLIError:
                        raise CLIError(
                            "context preflight failed and no cached snapshot is available; "
                            f"gates={','.join(preflight_gates)}"
                        ) from None
                else:
                    mode = "live_snapshot"
                    snapshot_code = _run_snapshot_bundle_for_playbook(args, snapshot_id)
                    if snapshot_code != 0:
                        raise CLIError(
                            f"live snapshot failed with exit code {snapshot_code} for {snapshot_id}"
                        )
            else:
                try:
                    snapshot_id = _latest_snapshot_id(store)
                    if block_paid:
                        mode = "offline_paid_block"
                    elif odds_cap_reached:
                        mode = "offline_odds_cap"
                    else:
                        mode = "offline_outside_window"
                except CLIError:
                    if odds_cap_reached or block_paid:
                        raise CLIError(
                            "paid odds calls are blocked (or odds cap reached) and no cached "
                            "snapshot is available"
                        ) from None
                    snapshot_id = make_snapshot_id()
                    mode = "live_bootstrap"
                    preflight_context = _preflight_context_for_snapshot(
                        store=store,
                        snapshot_id=snapshot_id,
                        teams_in_scope=_teams_in_scope_from_events(events),
                        refresh_context=args.refresh_context,
                        require_official_injuries=require_official_injuries,
                        allow_secondary_injuries=allow_secondary_injuries,
                        require_fresh_context=require_fresh_context,
                        injuries_stale_hours=injuries_stale_hours,
                        roster_stale_hours=roster_stale_hours,
                    )
                    preflight_gates = preflight_context.get("health_gates", [])
                    if (
                        isinstance(preflight_gates, list)
                        and "official_injury_missing" in preflight_gates
                        and not allow_secondary_injuries
                    ):
                        raise CLIError(_official_injury_hard_fail_message()) from None
                    if isinstance(preflight_gates, list) and preflight_gates:
                        raise CLIError(
                            "context preflight failed; refusing paid odds fetch in bootstrap mode; "
                            f"gates={','.join(preflight_gates)}"
                        ) from None
                    snapshot_code = _run_snapshot_bundle_for_playbook(args, snapshot_id)
                    if snapshot_code != 0:
                        raise CLIError(
                            "bootstrap live snapshot failed with exit code "
                            f"{snapshot_code} for {snapshot_id}"
                        ) from None

    if not snapshot_id:
        raise CLIError("failed to resolve snapshot id")

    strategy_offline = bool(args.offline or mode.startswith("offline"))
    refresh_context = bool(args.refresh_context and not strategy_offline)
    if bool(args.refresh_context) and strategy_offline:
        print(f"note=refresh_context_ignored_in_offline_mode snapshot_id={snapshot_id}")
    strategy_code = _run_strategy_for_playbook(
        snapshot_id=snapshot_id,
        strategy_id=strategy_id,
        top_n=args.strategy_top_n,
        min_ev=args.min_ev,
        allow_tier_b=args.allow_tier_b,
        offline=strategy_offline,
        block_paid=bool(getattr(args, "block_paid", False)),
        refresh_context=refresh_context,
        strategy_mode=str(getattr(args, "strategy_mode", "auto")),
        allow_secondary_injuries=allow_secondary_injuries,
        write_canonical=True,
    )
    if strategy_code != 0:
        return strategy_code

    brief = generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=snapshot_id,
        top_n=top_n,
        llm_refresh=args.refresh_llm,
        llm_offline=args.offline,
        per_game_top_n=per_game_top_n,
        game_card_min_ev=max(0.0, args.min_ev),
        month=month,
    )
    end_budget = budget_snapshot(store=store, settings=settings, month=month)
    print(
        "snapshot_id={} mode={} within_window={} odds_cap_reached={}".format(
            snapshot_id,
            mode,
            live_window.get("within_window", False),
            end_budget["odds"].get("cap_reached", False),
        )
    )
    mode_desc = playbook_mode_key().get(mode, "")
    if mode_desc:
        print(f"mode_desc={mode_desc}")
    print(f"strategy_id={strategy_id}")
    title = strategy_title(strategy_id)
    if title:
        print(f"strategy_title={title}")
    preflight_gates = (
        preflight_context.get("health_gates", [])
        if isinstance(preflight_context.get("health_gates"), list)
        else []
    )
    if preflight_gates:
        print(f"context_preflight_gates={','.join(preflight_gates)}")
    print(f"strategy_brief_md={brief['report_markdown']}")
    print(f"strategy_brief_tex={brief['report_tex']}")
    print(f"strategy_brief_pdf={brief['report_pdf']}")
    print(f"strategy_brief_meta={brief['report_meta']}")
    print(
        "llm_pass1_status={} llm_pass2_status={} pdf_status={}".format(
            brief.get("llm_pass1_status", ""),
            brief.get("llm_pass2_status", ""),
            brief.get("pdf_status", ""),
        )
    )
    return 0


def _cmd_playbook_render(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    settings = Settings.from_env()
    month = args.month or current_month_utc()
    strategy_id = _resolve_strategy_id(
        str(getattr(args, "strategy", "")),
        default_id=settings.strategy_default_id,
    )
    top_n = args.top_n if args.top_n > 0 else settings.playbook_top_n
    per_game_top_n = (
        args.per_game_top_n if args.per_game_top_n > 0 else settings.playbook_per_game_top_n
    )
    snapshot_id = args.snapshot_id
    reports_dir = store.snapshot_dir(snapshot_id) / "reports"
    strategy_report_file = (
        str(getattr(args, "strategy_report_file", "strategy-report.json")).strip()
        or "strategy-report.json"
    )
    candidate_report_path = Path(strategy_report_file).expanduser()
    if candidate_report_path.is_absolute():
        strategy_report_path = candidate_report_path
    else:
        strategy_report_path = reports_dir / candidate_report_path
    canonical_strategy_path = reports_dir / "strategy-report.json"
    is_canonical_strategy_report = strategy_report_path.resolve(
        strict=False
    ) == canonical_strategy_path.resolve(strict=False)
    brief_tag = str(getattr(args, "brief_tag", "")).strip()
    refresh_context = bool(args.refresh_context and not args.offline)
    if bool(args.refresh_context) and bool(args.offline):
        print(f"note=refresh_context_ignored_in_offline_mode snapshot_id={snapshot_id}")
    if is_canonical_strategy_report:
        strategy_needs_refresh = not canonical_strategy_path.exists()
        if not strategy_needs_refresh:
            try:
                existing = json.loads(canonical_strategy_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                strategy_needs_refresh = True
            else:
                existing_id = ""
                if isinstance(existing, dict):
                    existing_id = str(existing.get("strategy_id", "")).strip()
                existing_id = normalize_strategy_id(existing_id) if existing_id else "s001"
                strategy_needs_refresh = existing_id != strategy_id
        if strategy_needs_refresh:
            code = _run_strategy_for_playbook(
                snapshot_id=snapshot_id,
                strategy_id=strategy_id,
                top_n=args.strategy_top_n,
                min_ev=args.min_ev,
                allow_tier_b=args.allow_tier_b,
                offline=args.offline,
                block_paid=bool(getattr(args, "block_paid", False)),
                refresh_context=refresh_context,
                strategy_mode=str(getattr(args, "strategy_mode", "auto")),
                allow_secondary_injuries=_allow_secondary_injuries_override(
                    cli_flag=bool(getattr(args, "allow_secondary_injuries", False))
                ),
                write_canonical=True,
            )
            if code != 0:
                return code
    elif not strategy_report_path.exists():
        raise CLIError(f"missing strategy report file: {strategy_report_path}")

    brief = generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=snapshot_id,
        top_n=top_n,
        llm_refresh=args.refresh_llm,
        llm_offline=args.offline,
        per_game_top_n=per_game_top_n,
        game_card_min_ev=max(0.0, args.min_ev),
        month=month,
        strategy_report_path=strategy_report_path,
        artifact_tag=brief_tag,
    )
    print(f"snapshot_id={snapshot_id}")
    print(f"strategy_id={strategy_id}")
    title = strategy_title(strategy_id)
    if title:
        print(f"strategy_title={title}")
    print(f"strategy_report_path={strategy_report_path}")
    if brief_tag:
        print(f"brief_tag={brief_tag}")
    print(f"strategy_brief_md={brief['report_markdown']}")
    print(f"strategy_brief_tex={brief['report_tex']}")
    print(f"strategy_brief_pdf={brief['report_pdf']}")
    print(f"strategy_brief_meta={brief['report_meta']}")
    return 0


_COMPACT_PLAYBOOK_REPORTS: tuple[str, ...] = (
    "strategy-report.json",
    "strategy-brief.meta.json",
    "strategy-brief.pdf",
)


def _snapshot_date(snapshot_id: str) -> str:
    if len(snapshot_id) >= 10:
        date_prefix = snapshot_id[:10]
        try:
            date.fromisoformat(date_prefix)
            return date_prefix
        except ValueError:
            pass
    daily_match = re.match(r"^daily-(\d{4})-?(\d{2})-?(\d{2})", snapshot_id)
    if daily_match:
        year, month, day = daily_match.groups()
        candidate = f"{year}-{month}-{day}"
        try:
            date.fromisoformat(candidate)
            return candidate
        except ValueError:
            pass
    return _utc_now().strftime("%Y-%m-%d")


def _publish_compact_playbook_outputs(
    *, store: SnapshotStore, snapshot_id: str
) -> tuple[list[str], Path, Path, Path]:
    reports_dir = store.snapshot_dir(snapshot_id) / "reports"
    if not reports_dir.exists():
        raise CLIError(f"missing reports directory: {reports_dir}")

    snapshot_day = _snapshot_date(snapshot_id)
    reports_root = report_outputs_root(store)
    daily_dir = reports_root / "daily" / snapshot_day / f"snapshot={snapshot_id}"
    latest_dir = reports_root / "latest"
    daily_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)

    published: list[str] = []
    for filename in _COMPACT_PLAYBOOK_REPORTS:
        src = reports_dir / filename
        if not src.exists():
            continue
        copy2(src, daily_dir / filename)
        copy2(src, latest_dir / filename)
        published.append(filename)

    if not published:
        raise CLIError(
            "no compact reports found; run `prop-ev playbook run` or "
            "`prop-ev playbook render` first"
        )

    pointer = {
        "snapshot_id": snapshot_id,
        "updated_at_utc": _iso(_utc_now()),
        "files": published,
    }
    latest_json = latest_dir / "latest.json"
    latest_json.write_text(json.dumps(pointer, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    publish_json = daily_dir / "publish.json"
    publish_json.write_text(json.dumps(pointer, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return published, daily_dir, latest_dir, latest_json


def _cmd_playbook_publish(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    published, daily_dir, latest_dir, latest_json = _publish_compact_playbook_outputs(
        store=store,
        snapshot_id=args.snapshot_id,
    )
    print(f"snapshot_id={args.snapshot_id}")
    print(f"published_files={','.join(published)}")
    print(f"daily_dir={daily_dir}")
    print(f"latest_dir={latest_dir}")
    print(f"latest_json={latest_json}")
    return 0


def _cmd_playbook_budget(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    settings = Settings.from_env()
    month = args.month or current_month_utc()
    payload = budget_snapshot(store=store, settings=settings, month=month)
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0


def _cmd_playbook_discover_execute(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    settings = Settings.from_env()
    month = args.month or current_month_utc()
    strategy_id = _resolve_strategy_id(
        str(getattr(args, "strategy", "")),
        default_id=settings.strategy_default_id,
    )
    base_snapshot = args.base_snapshot_id or make_snapshot_id()
    discovery_snapshot_id = f"{base_snapshot}-discover"
    execution_snapshot_id = f"{base_snapshot}-execute"

    discovery_slate_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets="spreads,totals",
        regions=args.discovery_regions,
        bookmakers="",
        ignore_bookmaker_config=True,
        snapshot_id=discovery_snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=False,
        dry_run=False,
    )
    discovery_props_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets=args.markets,
        regions=args.discovery_regions,
        bookmakers="",
        ignore_bookmaker_config=True,
        snapshot_id=discovery_snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        include_links=args.include_links,
        include_sids=args.include_sids,
        max_events=args.max_events,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=False,
        dry_run=False,
    )
    execution_slate_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets="spreads,totals",
        regions=args.execution_regions,
        bookmakers=args.execution_bookmakers,
        ignore_bookmaker_config=False,
        snapshot_id=execution_snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=False,
        dry_run=False,
    )
    execution_props_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets=args.markets,
        regions=args.execution_regions,
        bookmakers=args.execution_bookmakers,
        ignore_bookmaker_config=False,
        snapshot_id=execution_snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        include_links=args.include_links,
        include_sids=args.include_sids,
        max_events=args.max_events,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=False,
        dry_run=False,
    )

    if _cmd_snapshot_slate(discovery_slate_args) != 0:
        return 2
    if _cmd_snapshot_props(discovery_props_args) != 0:
        return 2
    if (
        _run_strategy_for_playbook(
            snapshot_id=discovery_snapshot_id,
            strategy_id=strategy_id,
            top_n=args.strategy_top_n,
            min_ev=args.min_ev,
            allow_tier_b=args.allow_tier_b,
            offline=False,
            block_paid=False,
            refresh_context=args.refresh_context,
            strategy_mode=str(getattr(args, "strategy_mode", "auto")),
            allow_secondary_injuries=_allow_secondary_injuries_override(
                cli_flag=bool(getattr(args, "allow_secondary_injuries", False))
            ),
            write_canonical=True,
        )
        != 0
    ):
        return 2

    if _cmd_snapshot_slate(execution_slate_args) != 0:
        return 2
    if _cmd_snapshot_props(execution_props_args) != 0:
        return 2
    if (
        _run_strategy_for_playbook(
            snapshot_id=execution_snapshot_id,
            strategy_id=strategy_id,
            top_n=args.strategy_top_n,
            min_ev=args.min_ev,
            allow_tier_b=args.allow_tier_b,
            offline=False,
            block_paid=False,
            refresh_context=args.refresh_context,
            strategy_mode=str(getattr(args, "strategy_mode", "auto")),
            allow_secondary_injuries=_allow_secondary_injuries_override(
                cli_flag=bool(getattr(args, "allow_secondary_injuries", False))
            ),
            write_canonical=True,
        )
        != 0
    ):
        return 2

    discovery_report_path = (
        store.snapshot_dir(discovery_snapshot_id) / "reports" / "strategy-report.json"
    )
    execution_report_path = (
        store.snapshot_dir(execution_snapshot_id) / "reports" / "strategy-report.json"
    )
    discovery_report = json.loads(discovery_report_path.read_text(encoding="utf-8"))
    execution_report = json.loads(execution_report_path.read_text(encoding="utf-8"))
    compare_report = _build_discovery_execution_report(
        discovery_snapshot_id=discovery_snapshot_id,
        execution_snapshot_id=execution_snapshot_id,
        discovery_report=discovery_report,
        execution_report=execution_report,
        top_n=args.top_n,
    )
    compare_json, compare_md = _write_discovery_execution_reports(
        store=store,
        execution_snapshot_id=execution_snapshot_id,
        report=compare_report,
    )

    brief = generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=execution_snapshot_id,
        top_n=max(1, args.top_n),
        llm_refresh=args.refresh_llm,
        llm_offline=args.offline,
        per_game_top_n=max(1, getattr(args, "per_game_top_n", 5)),
        game_card_min_ev=max(0.0, args.min_ev),
        month=month,
    )

    summary = compare_report.get("summary", {})
    print(f"discovery_snapshot_id={discovery_snapshot_id}")
    print(f"execution_snapshot_id={execution_snapshot_id}")
    print(f"strategy_id={strategy_id}")
    title = strategy_title(strategy_id)
    if title:
        print(f"strategy_title={title}")
    print(
        "actionable_rows={} matched_rows={} discovery_eligible_rows={}".format(
            summary.get("actionable_rows", 0),
            summary.get("matched_execution_rows", 0),
            summary.get("discovery_eligible_rows", 0),
        )
    )
    print(f"discovery_execution_json={compare_json}")
    print(f"discovery_execution_md={compare_md}")
    print(f"strategy_brief_md={brief['report_markdown']}")
    print(f"strategy_brief_pdf={brief['report_pdf']}")
    return 0


def _extract_global_overrides(argv: list[str]) -> tuple[list[str], str, str]:
    cleaned: list[str] = []
    data_dir = ""
    reports_dir = ""
    idx = 0
    while idx < len(argv):
        token = argv[idx]
        if token == "--data-dir":
            if idx + 1 >= len(argv):
                raise CLIError("--data-dir requires a value")
            data_dir = str(argv[idx + 1]).strip()
            idx += 2
            continue
        if token.startswith("--data-dir="):
            data_dir = token.split("=", 1)[1].strip()
            idx += 1
            continue
        if token == "--reports-dir":
            if idx + 1 >= len(argv):
                raise CLIError("--reports-dir requires a value")
            reports_dir = str(argv[idx + 1]).strip()
            idx += 2
            continue
        if token.startswith("--reports-dir="):
            reports_dir = token.split("=", 1)[1].strip()
            idx += 1
            continue
        cleaned.append(token)
        idx += 1
    return cleaned, data_dir, reports_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prop-ev")
    parser.add_argument(
        "--data-dir",
        default="",
        help="Override PROP_EV_DATA_DIR for this command invocation.",
    )
    parser.add_argument(
        "--reports-dir",
        default="",
        help=(
            "Override PROP_EV_REPORTS_DIR for report publishing/output. "
            "Default is sibling reports dir of the odds-api data dir."
        ),
    )
    subparsers = parser.add_subparsers(dest="command")

    snapshot = subparsers.add_parser("snapshot", help="Create and inspect snapshots")
    snapshot_subparsers = snapshot.add_subparsers(dest="snapshot_command")

    snapshot_slate = snapshot_subparsers.add_parser("slate", help="Fetch slate featured odds")
    snapshot_slate.set_defaults(func=_cmd_snapshot_slate)
    snapshot_slate.add_argument("--sport-key", default="basketball_nba")
    snapshot_slate.add_argument("--markets", default="spreads,totals")
    snapshot_slate.add_argument("--regions", default="us")
    snapshot_slate.add_argument("--bookmakers", default="")
    snapshot_slate.add_argument("--snapshot-id", default="")
    snapshot_slate.add_argument("--commence-from", default="")
    snapshot_slate.add_argument("--commence-to", default="")
    snapshot_slate.add_argument("--max-credits", type=int, default=20)
    snapshot_slate.add_argument("--force", action="store_true")
    snapshot_slate.add_argument("--refresh", action="store_true")
    snapshot_slate.add_argument("--resume", action="store_true")
    snapshot_slate.add_argument("--offline", action="store_true")
    snapshot_slate.add_argument("--block-paid", action="store_true")
    snapshot_slate.add_argument("--dry-run", action="store_true")

    snapshot_props = snapshot_subparsers.add_parser("props", help="Fetch per-event prop odds")
    snapshot_props.set_defaults(func=_cmd_snapshot_props)
    snapshot_props.add_argument("--sport-key", default="basketball_nba")
    snapshot_props.add_argument("--markets", default="player_points")
    snapshot_props.add_argument("--regions", default="us")
    snapshot_props.add_argument("--bookmakers", default="")
    snapshot_props.add_argument("--snapshot-id", default="")
    snapshot_props.add_argument("--commence-from", default="")
    snapshot_props.add_argument("--commence-to", default="")
    snapshot_props.add_argument("--include-links", action="store_true")
    snapshot_props.add_argument("--include-sids", action="store_true")
    snapshot_props.add_argument("--max-events", type=int, default=0)
    snapshot_props.add_argument("--max-credits", type=int, default=20)
    snapshot_props.add_argument("--force", action="store_true")
    snapshot_props.add_argument("--refresh", action="store_true")
    snapshot_props.add_argument("--resume", action="store_true")
    snapshot_props.add_argument("--offline", action="store_true")
    snapshot_props.add_argument("--block-paid", action="store_true")
    snapshot_props.add_argument("--dry-run", action="store_true")

    snapshot_ls = snapshot_subparsers.add_parser("ls", help="List snapshots")
    snapshot_ls.set_defaults(func=_cmd_snapshot_ls)

    snapshot_show = snapshot_subparsers.add_parser("show", help="Show snapshot summary")
    snapshot_show.set_defaults(func=_cmd_snapshot_show)
    snapshot_show.add_argument("--snapshot-id", required=True)

    snapshot_diff = snapshot_subparsers.add_parser("diff", help="Diff derived snapshot outputs")
    snapshot_diff.set_defaults(func=_cmd_snapshot_diff)
    snapshot_diff.add_argument("--a", required=True)
    snapshot_diff.add_argument("--b", required=True)

    snapshot_verify = snapshot_subparsers.add_parser("verify", help="Verify snapshot artifacts")
    snapshot_verify.set_defaults(func=_cmd_snapshot_verify)
    snapshot_verify.add_argument("--snapshot-id", required=True)
    snapshot_verify.add_argument(
        "--check-derived",
        action="store_true",
        help="Also verify derived quote-table contracts and parity",
    )
    snapshot_verify.add_argument(
        "--require-parquet",
        action="store_true",
        help="Fail if known derived tables are missing parquet mirrors",
    )
    snapshot_verify.add_argument(
        "--require-table",
        action="append",
        default=[],
        help="Require one derived JSONL table (repeatable, e.g. event_props)",
    )

    snapshot_lake = snapshot_subparsers.add_parser(
        "lake", help="Convert derived JSONL artifacts to Parquet lake format"
    )
    snapshot_lake.set_defaults(func=_cmd_snapshot_lake)
    snapshot_lake.add_argument("--snapshot-id", required=True)

    snapshot_pack = snapshot_subparsers.add_parser(
        "pack", help="Pack one snapshot into a compressed tar bundle"
    )
    snapshot_pack.set_defaults(func=_cmd_snapshot_pack)
    snapshot_pack.add_argument("--snapshot-id", required=True)
    snapshot_pack.add_argument("--out", default="")

    snapshot_unpack = snapshot_subparsers.add_parser(
        "unpack", help="Unpack a snapshot bundle into the data directory"
    )
    snapshot_unpack.set_defaults(func=_cmd_snapshot_unpack)
    snapshot_unpack.add_argument("--bundle", required=True)

    data_cmd = subparsers.add_parser("data", help="Dataset status and backfill tools")
    data_subparsers = data_cmd.add_subparsers(dest="data_command")

    data_status = data_subparsers.add_parser("status", help="Show day completeness status")
    data_status.set_defaults(func=_cmd_data_status)
    data_status.add_argument(
        "--dataset-id",
        default="",
        help="Use an existing dataset id and ignore spec flags",
    )
    data_status.add_argument("--sport-key", default="basketball_nba")
    data_status.add_argument("--markets", default="player_points")
    data_status.add_argument("--regions", default="us")
    data_status.add_argument("--bookmakers", default="")
    data_status.add_argument("--include-links", action="store_true")
    data_status.add_argument("--include-sids", action="store_true")
    data_status.add_argument("--days", type=int, default=10)
    data_status.add_argument("--from", dest="from_day", default="")
    data_status.add_argument("--to", dest="to_day", default="")
    data_status.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_status.add_argument("--historical", action="store_true")
    data_status.add_argument("--historical-anchor-hour-local", type=int, default=12)
    data_status.add_argument("--historical-pre-tip-minutes", type=int, default=60)
    data_status.add_argument("--refresh", action="store_true")
    data_status.add_argument(
        "--json-summary",
        action="store_true",
        help="Emit machine-readable day summary JSON",
    )

    data_datasets = data_subparsers.add_parser(
        "datasets", help="Inspect stored dataset specs and day indexes"
    )
    data_datasets_subparsers = data_datasets.add_subparsers(dest="datasets_command")

    data_datasets_ls = data_datasets_subparsers.add_parser("ls", help="List known datasets")
    data_datasets_ls.set_defaults(func=_cmd_data_datasets_ls)
    data_datasets_ls.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    data_datasets_show = data_datasets_subparsers.add_parser(
        "show", help="Show one dataset spec and indexed day rows"
    )
    data_datasets_show.set_defaults(func=_cmd_data_datasets_show)
    data_datasets_show.add_argument("--dataset-id", required=True)
    data_datasets_show.add_argument("--from", dest="from_day", default="")
    data_datasets_show.add_argument("--to", dest="to_day", default="")
    data_datasets_show.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_datasets_show.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    data_verify = data_subparsers.add_parser(
        "verify",
        help="Verify dataset day-index and derived quote-table contracts",
    )
    data_verify.set_defaults(func=_cmd_data_verify)
    data_verify.add_argument("--dataset-id", required=True)
    data_verify.add_argument("--from", dest="from_day", default="")
    data_verify.add_argument("--to", dest="to_day", default="")
    data_verify.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_verify.add_argument(
        "--require-complete",
        action="store_true",
        help="Fail when selected days are incomplete",
    )
    data_verify.add_argument(
        "--require-parquet",
        action="store_true",
        help="Fail when complete-day snapshots are missing required parquet mirrors",
    )
    data_verify.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    data_backfill = data_subparsers.add_parser("backfill", help="Backfill day snapshots")
    data_backfill.set_defaults(func=_cmd_data_backfill)
    data_backfill.add_argument("--sport-key", default="basketball_nba")
    data_backfill.add_argument("--markets", default="player_points")
    data_backfill.add_argument("--regions", default="us")
    data_backfill.add_argument("--bookmakers", default="")
    data_backfill.add_argument("--include-links", action="store_true")
    data_backfill.add_argument("--include-sids", action="store_true")
    data_backfill.add_argument("--days", type=int, default=10)
    data_backfill.add_argument("--from", dest="from_day", default="")
    data_backfill.add_argument("--to", dest="to_day", default="")
    data_backfill.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_backfill.add_argument("--historical", action="store_true")
    data_backfill.add_argument("--historical-anchor-hour-local", type=int, default=12)
    data_backfill.add_argument("--historical-pre-tip-minutes", type=int, default=60)
    data_backfill.add_argument(
        "--max-credits",
        type=int,
        default=_env_int("PROP_EV_ODDS_API_DEFAULT_MAX_CREDITS", 20),
    )
    data_backfill.add_argument("--no-spend", action="store_true")
    data_backfill.add_argument("--offline", action="store_true")
    data_backfill.add_argument("--refresh", action="store_true")
    data_backfill.add_argument("--resume", action="store_true", default=True)
    data_backfill.add_argument("--block-paid", action="store_true")
    data_backfill.add_argument("--force", action="store_true")
    data_backfill.add_argument("--dry-run", action="store_true")

    credits = subparsers.add_parser("credits", help="Credit tooling")
    credits_subparsers = credits.add_subparsers(dest="credits_command")

    credits_report = credits_subparsers.add_parser("report", help="Report usage ledger")
    credits_report.set_defaults(func=_cmd_credits_report)
    credits_report.add_argument("--month", default="")

    credits_budget = credits_subparsers.add_parser("budget", help="Estimate budget")
    credits_budget.set_defaults(func=_cmd_credits_budget)
    credits_budget.add_argument("--events", type=int, default=0)
    credits_budget.add_argument("--markets", default="player_points")
    credits_budget.add_argument("--regions", default="us")
    credits_budget.add_argument("--bookmakers", default="")

    strategy = subparsers.add_parser("strategy", help="Run offline strategy reports")
    strategy_subparsers = strategy.add_subparsers(dest="strategy_command")

    strategy_ls = strategy_subparsers.add_parser("ls", help="List available strategy plugins")
    strategy_ls.set_defaults(func=_cmd_strategy_ls)

    strategy_run = strategy_subparsers.add_parser("run", help="Generate strategy report")
    strategy_run.set_defaults(func=_cmd_strategy_run)
    strategy_run.add_argument("--snapshot-id", default="")
    strategy_run.add_argument("--strategy", default="s001")
    strategy_run.add_argument("--top-n", type=int, default=25)
    strategy_run.add_argument("--min-ev", type=float, default=0.01)
    strategy_run.add_argument(
        "--mode",
        choices=("auto", "live", "replay"),
        default="auto",
        help="Strategy runtime mode (replay relaxes freshness gates for historical reruns).",
    )
    strategy_run.add_argument("--allow-tier-b", action="store_true")
    strategy_run.add_argument("--offline", action="store_true")
    strategy_run.add_argument("--block-paid", action="store_true")
    strategy_run.add_argument("--refresh-context", action="store_true")
    strategy_run.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Allow secondary injury source when official report is unavailable.",
    )
    strategy_run.add_argument(
        "--execution-bookmakers",
        default="",
        help="Comma-separated execution books for projected strategy outputs.",
    )
    strategy_run.add_argument(
        "--execution-top-n",
        type=int,
        default=0,
        help="Projected execution report top-N (defaults to --top-n when omitted).",
    )
    strategy_run.add_argument(
        "--execution-requires-pre-bet-ready",
        action="store_true",
        help="Require pre_bet_ready=true for execution eligibility.",
    )
    strategy_run.add_argument(
        "--execution-requires-meets-play-to",
        action="store_true",
        help="Require selected execution price to meet play_to_american.",
    )
    strategy_run.add_argument("--execution-tier-a-min-ev", type=float, default=0.03)
    strategy_run.add_argument("--execution-tier-b-min-ev", type=float, default=0.05)

    strategy_health = strategy_subparsers.add_parser(
        "health", help="Report injury/roster/mapping health for a snapshot"
    )
    strategy_health.set_defaults(func=_cmd_strategy_health)
    strategy_health.add_argument("--snapshot-id", default="")
    strategy_health.add_argument("--offline", action="store_true")
    strategy_health.add_argument("--refresh-context", action="store_true")
    strategy_health.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Treat secondary injuries as explicit override when official report is unavailable.",
    )
    strategy_health.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Emit JSON output (default)",
    )
    strategy_health.add_argument(
        "--no-json",
        dest="json_output",
        action="store_false",
        help="Emit compact text output",
    )

    strategy_compare = strategy_subparsers.add_parser(
        "compare", help="Run multiple strategies for the same snapshot"
    )
    strategy_compare.set_defaults(func=_cmd_strategy_compare)
    strategy_compare.add_argument("--snapshot-id", default="")
    strategy_compare.add_argument("--strategies", required=True)
    strategy_compare.add_argument("--top-n", type=int, default=25)
    strategy_compare.add_argument("--min-ev", type=float, default=0.01)
    strategy_compare.add_argument("--allow-tier-b", action="store_true")
    strategy_compare.add_argument("--offline", action="store_true")
    strategy_compare.add_argument("--block-paid", action="store_true")
    strategy_compare.add_argument("--refresh-context", action="store_true")

    strategy_backtest_prep = strategy_subparsers.add_parser(
        "backtest-prep", help="Write backtest seed/readiness artifacts for a snapshot"
    )
    strategy_backtest_prep.set_defaults(func=_cmd_strategy_backtest_prep)
    strategy_backtest_prep.add_argument("--snapshot-id", default="")
    strategy_backtest_prep.add_argument("--strategy", default="")
    strategy_backtest_prep.add_argument(
        "--selection", choices=sorted(ROW_SELECTIONS), default="eligible"
    )
    strategy_backtest_prep.add_argument("--top-n", type=int, default=0)

    strategy_settle = strategy_subparsers.add_parser(
        "settle", help="Grade backtest seed tickets using live NBA boxscore results"
    )
    strategy_settle.set_defaults(func=_cmd_strategy_settle)
    strategy_settle.add_argument("--snapshot-id", default="")
    strategy_settle.add_argument(
        "--seed-path",
        default="",
        help="Optional override path to backtest seed jsonl",
    )
    strategy_settle.add_argument("--offline", action="store_true")
    strategy_settle.add_argument("--refresh-results", action="store_true")
    strategy_settle.add_argument(
        "--results-source",
        choices=["auto", "historical", "live", "cache_only"],
        default=None,
        help="Unified NBA source policy for settlement.",
    )
    strategy_settle.add_argument("--write-csv", action="store_true")
    strategy_settle.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Emit JSON output (default)",
    )
    strategy_settle.add_argument(
        "--no-json",
        dest="json_output",
        action="store_false",
        help="Emit compact text output",
    )
    strategy_backtest_summarize = strategy_subparsers.add_parser(
        "backtest-summarize", help="Summarize graded backtest CSVs for one snapshot"
    )
    strategy_backtest_summarize.set_defaults(func=_cmd_strategy_backtest_summarize)
    strategy_backtest_summarize.add_argument("--snapshot-id", default="")
    strategy_backtest_summarize.add_argument("--strategies", default="")
    strategy_backtest_summarize.add_argument("--results", action="append", default=[])
    strategy_backtest_summarize.add_argument("--min-graded", type=int, default=200)
    strategy_backtest_summarize.add_argument("--bin-size", type=float, default=0.05)

    playbook = subparsers.add_parser("playbook", help="Run playbook briefs")
    playbook_subparsers = playbook.add_subparsers(dest="playbook_command")

    playbook_run = playbook_subparsers.add_parser("run", help="Run live/offline playbook flow")
    playbook_run.set_defaults(func=_cmd_playbook_run)
    playbook_run.add_argument("--snapshot-id", default="")
    playbook_run.add_argument("--sport-key", default="basketball_nba")
    playbook_run.add_argument("--markets", default="player_points")
    playbook_run.add_argument("--regions", default="us")
    playbook_run.add_argument("--bookmakers", default="")
    playbook_run.add_argument("--commence-from", default="")
    playbook_run.add_argument("--commence-to", default="")
    playbook_run.add_argument(
        "--include-links", dest="include_links", action="store_true", default=True
    )
    playbook_run.add_argument("--no-include-links", dest="include_links", action="store_false")
    playbook_run.add_argument(
        "--include-sids", dest="include_sids", action="store_true", default=True
    )
    playbook_run.add_argument("--no-include-sids", dest="include_sids", action="store_false")
    playbook_run.add_argument("--max-events", type=int, default=10)
    playbook_run.add_argument("--max-credits", type=int, default=20)
    playbook_run.add_argument("--force", action="store_true")
    playbook_run.add_argument("--refresh", action="store_true")
    playbook_run.add_argument("--resume", action="store_true")
    playbook_run.add_argument("--offline", action="store_true")
    playbook_run.add_argument("--block-paid", action="store_true")
    playbook_run.add_argument("--refresh-context", action="store_true")
    playbook_run.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Allow run when official injuries are unavailable and secondary source is healthy.",
    )
    playbook_run.add_argument("--refresh-llm", action="store_true")
    playbook_run.add_argument("--strategy", default="")
    playbook_run.add_argument("--top-n", type=int, default=0)
    playbook_run.add_argument("--per-game-top-n", type=int, default=0)
    playbook_run.add_argument("--strategy-top-n", type=int, default=25)
    playbook_run.add_argument("--min-ev", type=float, default=0.01)
    playbook_run.add_argument("--allow-tier-b", action="store_true")
    playbook_run.add_argument(
        "--strategy-mode",
        choices=("auto", "live", "replay"),
        default="auto",
        help="Strategy runtime mode used for playbook strategy execution.",
    )
    playbook_run.add_argument(
        "--exit-on-no-games",
        action="store_true",
        help="Exit 0 early when events lookup returns no games in the selected window",
    )
    playbook_run.add_argument("--month", default="")

    playbook_render = playbook_subparsers.add_parser(
        "render", help="Render playbook briefs for an existing snapshot"
    )
    playbook_render.set_defaults(func=_cmd_playbook_render)
    playbook_render.add_argument("--snapshot-id", required=True)
    playbook_render.add_argument("--offline", action="store_true")
    playbook_render.add_argument("--block-paid", action="store_true")
    playbook_render.add_argument("--refresh-context", action="store_true")
    playbook_render.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Allow run when official injuries are unavailable and secondary source is healthy.",
    )
    playbook_render.add_argument("--refresh-llm", action="store_true")
    playbook_render.add_argument("--strategy", default="")
    playbook_render.add_argument("--top-n", type=int, default=0)
    playbook_render.add_argument("--per-game-top-n", type=int, default=0)
    playbook_render.add_argument("--strategy-top-n", type=int, default=25)
    playbook_render.add_argument("--min-ev", type=float, default=0.01)
    playbook_render.add_argument("--allow-tier-b", action="store_true")
    playbook_render.add_argument(
        "--strategy-report-file",
        default="strategy-report.json",
        help="Strategy report file name (relative to snapshot reports/) or absolute path.",
    )
    playbook_render.add_argument(
        "--brief-tag",
        default="",
        help="Optional artifact tag for writing non-canonical brief outputs.",
    )
    playbook_render.add_argument(
        "--strategy-mode",
        choices=("auto", "live", "replay"),
        default="auto",
        help="Strategy runtime mode if render triggers canonical strategy refresh.",
    )
    playbook_render.add_argument("--month", default="")

    playbook_publish = playbook_subparsers.add_parser(
        "publish", help="Publish compact reports for one snapshot"
    )
    playbook_publish.set_defaults(func=_cmd_playbook_publish)
    playbook_publish.add_argument("--snapshot-id", required=True)

    playbook_budget = playbook_subparsers.add_parser(
        "budget", help="Show odds + LLM monthly budget status"
    )
    playbook_budget.set_defaults(func=_cmd_playbook_budget)
    playbook_budget.add_argument("--month", default="")

    playbook_discover_execute = playbook_subparsers.add_parser(
        "discover-execute",
        help="Run all-books discovery + execution-book comparison in one flow",
    )
    playbook_discover_execute.set_defaults(func=_cmd_playbook_discover_execute)
    playbook_discover_execute.add_argument("--base-snapshot-id", default="")
    playbook_discover_execute.add_argument("--sport-key", default="basketball_nba")
    playbook_discover_execute.add_argument(
        "--markets",
        default="player_points,player_rebounds,player_assists,player_threes,player_points_rebounds_assists",
    )
    playbook_discover_execute.add_argument("--discovery-regions", default="us")
    playbook_discover_execute.add_argument("--execution-regions", default="us")
    playbook_discover_execute.add_argument("--execution-bookmakers", default="draftkings,fanduel")
    playbook_discover_execute.add_argument("--commence-from", default="")
    playbook_discover_execute.add_argument("--commence-to", default="")
    playbook_discover_execute.add_argument(
        "--include-links", dest="include_links", action="store_true", default=True
    )
    playbook_discover_execute.add_argument(
        "--no-include-links", dest="include_links", action="store_false"
    )
    playbook_discover_execute.add_argument(
        "--include-sids", dest="include_sids", action="store_true", default=True
    )
    playbook_discover_execute.add_argument(
        "--no-include-sids", dest="include_sids", action="store_false"
    )
    playbook_discover_execute.add_argument("--max-events", type=int, default=6)
    playbook_discover_execute.add_argument("--max-credits", type=int, default=40)
    playbook_discover_execute.add_argument("--force", action="store_true")
    playbook_discover_execute.add_argument("--refresh", action="store_true")
    playbook_discover_execute.add_argument("--resume", action="store_true")
    playbook_discover_execute.add_argument("--offline", action="store_true")
    playbook_discover_execute.add_argument("--refresh-context", action="store_true")
    playbook_discover_execute.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Allow run when official injuries are unavailable and secondary source is healthy.",
    )
    playbook_discover_execute.add_argument("--refresh-llm", action="store_true")
    playbook_discover_execute.add_argument("--strategy", default="")
    playbook_discover_execute.add_argument("--top-n", type=int, default=25)
    playbook_discover_execute.add_argument("--per-game-top-n", type=int, default=5)
    playbook_discover_execute.add_argument("--strategy-top-n", type=int, default=50)
    playbook_discover_execute.add_argument("--min-ev", type=float, default=0.01)
    playbook_discover_execute.add_argument("--allow-tier-b", action="store_true")
    playbook_discover_execute.add_argument(
        "--strategy-mode",
        choices=("auto", "live", "replay"),
        default="auto",
        help="Strategy runtime mode used by both discovery and execution strategy runs.",
    )
    playbook_discover_execute.add_argument("--month", default="")

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""
    raw_argv = list(argv) if isinstance(argv, list) else sys.argv[1:]
    parsed_argv, data_dir_override, reports_dir_override = _extract_global_overrides(raw_argv)
    prev_data_dir = os.environ.get("PROP_EV_DATA_DIR")
    prev_reports_dir = os.environ.get("PROP_EV_REPORTS_DIR")

    try:
        if data_dir_override:
            os.environ["PROP_EV_DATA_DIR"] = str(Path(data_dir_override).expanduser())
        if reports_dir_override:
            os.environ["PROP_EV_REPORTS_DIR"] = str(Path(reports_dir_override).expanduser())

        parser = _build_parser()
        args = parser.parse_args(parsed_argv)
        explicit_data_dir = str(getattr(args, "data_dir", "")).strip()
        if explicit_data_dir:
            os.environ["PROP_EV_DATA_DIR"] = str(Path(explicit_data_dir).expanduser())
        explicit_reports_dir = str(getattr(args, "reports_dir", "")).strip()
        if explicit_reports_dir:
            os.environ["PROP_EV_REPORTS_DIR"] = str(Path(explicit_reports_dir).expanduser())
        func = getattr(args, "func", None)
        if func is None:
            parser.print_help()
            return 0
        try:
            return int(func(args))
        except (
            CLIError,
            OddsAPIError,
            CreditBudgetExceeded,
            OfflineCacheMiss,
            SpendBlockedError,
            FileNotFoundError,
            ValueError,
        ) as exc:
            print(str(exc), file=sys.stderr)
            return 2
    finally:
        if prev_data_dir is None:
            os.environ.pop("PROP_EV_DATA_DIR", None)
        else:
            os.environ["PROP_EV_DATA_DIR"] = prev_data_dir
        if prev_reports_dir is None:
            os.environ.pop("PROP_EV_REPORTS_DIR", None)
        else:
            os.environ["PROP_EV_REPORTS_DIR"] = prev_reports_dir


if __name__ == "__main__":
    raise SystemExit(main())
