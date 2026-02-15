"""Shared helpers for strategy CLI commands."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prop_ev.cli_internal import teams_in_scope_from_events
from prop_ev.cli_playbook_publish import snapshot_date as _snapshot_date_impl
from prop_ev.cli_shared import (
    CLIError,
    _env_bool,
    _env_float,
    _env_int,
    _runtime_strategy_probabilistic_profile,
)
from prop_ev.context_health import (
    official_rows_count,
    official_source_ready,
    secondary_source_ready,
)
from prop_ev.nba_data.repo import NBARepository
from prop_ev.report_paths import (
    report_outputs_root,
)
from prop_ev.rolling_priors import build_rolling_priors
from prop_ev.storage import SnapshotStore
from prop_ev.strategies import get_strategy


def _snapshot_date(snapshot_id: str) -> str:
    return _snapshot_date_impl(snapshot_id)


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


def _allow_secondary_injuries_override(*, cli_flag: bool, default: bool) -> bool:
    return cli_flag or default


def _resolve_probabilistic_profile(value: str) -> str:
    profile = value.strip().lower() or "off"
    if profile not in {"off", "minutes_v1"}:
        raise CLIError(f"invalid probabilistic profile: {value} (expected off|minutes_v1)")
    return profile


def _strategy_recipe_probabilistic_profile(strategy_id: str) -> str:
    plugin = get_strategy(strategy_id)
    recipe = getattr(plugin, "recipe", None)
    raw = str(getattr(recipe, "probabilistic_profile", "") or "").strip()
    if not raw:
        return "off"
    return _resolve_probabilistic_profile(raw)


def _resolve_input_probabilistic_profile(
    *,
    default_profile: str,
    probabilistic_profile_arg: str,
    strategy_ids: list[str],
) -> str:
    explicit = str(probabilistic_profile_arg).strip()
    if explicit:
        return _resolve_probabilistic_profile(explicit)
    default_resolved = _resolve_probabilistic_profile(default_profile)
    required: set[str] = {default_resolved}
    for strategy_id in strategy_ids:
        required.add(_strategy_recipe_probabilistic_profile(strategy_id))
    if "minutes_v1" in required:
        return "minutes_v1"
    return "off"


def _official_injury_hard_fail_message() -> str:
    return (
        "official injury report unavailable; refusing to continue without override. "
        "Use --allow-secondary-injuries or set "
        "`strategy.allow_secondary_injuries=true` in runtime config."
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


def _strategy_policy_from_runtime() -> dict[str, Any]:
    return {
        "require_official_injuries": _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True),
        "allow_secondary_injuries": _env_bool(
            "PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES",
            False,
        ),
        "stale_quote_minutes": _env_int("PROP_EV_STRATEGY_STALE_QUOTE_MINUTES", 20),
        "probabilistic_profile": _resolve_probabilistic_profile(
            str(_runtime_strategy_probabilistic_profile())
        ),
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


def _replay_quote_now_from_manifest(manifest: dict[str, Any]) -> str | None:
    raw = str(manifest.get("created_at_utc", "")).strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _execution_projection_tag(bookmakers: tuple[str, ...]) -> str:
    cleaned = [re.sub(r"[^a-z0-9._-]+", "-", book.strip().lower()) for book in bookmakers]
    cleaned = [book.strip("._-") for book in cleaned if book.strip("._-")]
    if not cleaned:
        return "execution"
    return f"execution-{'-'.join(cleaned)}"


def _load_rolling_priors_for_strategy(
    *,
    store: SnapshotStore,
    strategy_id: str,
    snapshot_id: str,
) -> dict[str, Any]:
    return build_rolling_priors(
        reports_root=report_outputs_root(store),
        strategy_id=strategy_id,
        as_of_day=_snapshot_date(snapshot_id),
        window_days=max(1, _env_int("PROP_EV_STRATEGY_ROLLING_PRIOR_WINDOW_DAYS", 21)),
        min_samples=max(1, _env_int("PROP_EV_STRATEGY_ROLLING_PRIOR_MIN_SAMPLES", 25)),
        max_abs_delta=max(0.0, _env_float("PROP_EV_STRATEGY_ROLLING_PRIOR_MAX_DELTA", 0.02)),
        calibration_bin_size=max(
            0.02,
            min(0.5, _env_float("PROP_EV_STRATEGY_CALIBRATION_BIN_SIZE", 0.1)),
        ),
        calibration_min_bin_samples=max(
            1,
            _env_int("PROP_EV_STRATEGY_CALIBRATION_MIN_BIN_SAMPLES", 10),
        ),
        calibration_max_abs_delta=max(
            0.0,
            _env_float(
                "PROP_EV_STRATEGY_CALIBRATION_MAX_DELTA",
                _env_float("PROP_EV_STRATEGY_ROLLING_PRIOR_MAX_DELTA", 0.02),
            ),
        ),
        calibration_shrink_k=max(
            1,
            _env_int("PROP_EV_STRATEGY_CALIBRATION_SHRINK_K", 100),
        ),
        calibration_bucket_weight=max(
            0.0,
            min(1.0, _env_float("PROP_EV_STRATEGY_CALIBRATION_BUCKET_WEIGHT", 0.3)),
        ),
    )


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
