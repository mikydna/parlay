"""Strategy CLI command implementations."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from prop_ev.backtest import build_backtest_seed_rows, write_backtest_artifacts
from prop_ev.cli_ablation_helpers import (
    ablation_compare_cache_valid as _ablation_compare_cache_valid,
)
from prop_ev.cli_ablation_helpers import (
    ablation_count_seed_rows as _ablation_count_seed_rows,
)
from prop_ev.cli_ablation_helpers import (
    ablation_git_head as _ablation_git_head,
)
from prop_ev.cli_ablation_helpers import (
    ablation_prune_cap_root as _ablation_prune_cap_root,
)
from prop_ev.cli_ablation_helpers import (
    ablation_state_dir as _ablation_state_dir,
)
from prop_ev.cli_ablation_helpers import (
    ablation_strategy_cache_valid as _ablation_strategy_cache_valid,
)
from prop_ev.cli_ablation_helpers import (
    ablation_write_state as _ablation_write_state,
)
from prop_ev.cli_ablation_helpers import (
    build_ablation_analysis_run_id as _build_ablation_analysis_run_id,
)
from prop_ev.cli_ablation_helpers import (
    build_ablation_input_hash as _build_ablation_input_hash,
)
from prop_ev.cli_ablation_helpers import (
    parse_cli_kv as _parse_cli_kv,
)
from prop_ev.cli_ablation_helpers import (
    parse_positive_int_csv as _parse_positive_int_csv_impl,
)
from prop_ev.cli_ablation_helpers import (
    sha256_file as _sha256_file,
)
from prop_ev.cli_data_helpers import (
    complete_day_snapshots as _complete_day_snapshots_impl,
)
from prop_ev.cli_data_helpers import (
    resolve_complete_day_dataset_id as _resolve_complete_day_dataset_id_impl,
)
from prop_ev.cli_internal import teams_in_scope_from_events
from prop_ev.cli_markdown import (
    render_backtest_summary_markdown as _render_backtest_summary_markdown,
)
from prop_ev.cli_markdown import (
    render_strategy_compare_markdown as _render_strategy_compare_markdown,
)
from prop_ev.cli_playbook_publish import snapshot_date as _snapshot_date_impl
from prop_ev.cli_shared import (
    CLIError,
    _default_window,
    _env_bool,
    _env_float,
    _env_int,
    _iso,
    _parse_positive_float_csv,
    _runtime_nba_data_dir,
    _runtime_odds_data_dir,
    _runtime_runtime_dir,
    _runtime_strategy_probabilistic_profile,
    _sanitize_analysis_run_id,
    _utc_now,
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
from prop_ev.nba_data.minutes_prob import load_minutes_prob_index_for_snapshot
from prop_ev.nba_data.repo import NBARepository
from prop_ev.nba_data.store.layout import build_layout as build_nba_layout
from prop_ev.odds_client import (
    parse_csv,
)
from prop_ev.report_paths import (
    report_outputs_root,
    snapshot_reports_dir,
)
from prop_ev.rolling_priors import build_rolling_priors
from prop_ev.settlement import settle_snapshot
from prop_ev.state_keys import (
    strategy_health_state_key,
    strategy_title,
)
from prop_ev.storage import SnapshotStore
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
    write_execution_plan,
    write_strategy_reports,
    write_tagged_strategy_reports,
)


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


def _cmd_strategy_health(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    manifest = store.load_manifest(snapshot_id)
    derived_path = snapshot_dir / "derived" / "event_props.jsonl"
    if not derived_path.exists():
        raise CLIError(f"missing derived props file: {derived_path}")

    rows = load_jsonl(derived_path)
    event_context = _load_event_context(store, snapshot_id, manifest)
    slate_rows = _load_slate_rows(store, snapshot_id)
    policy = _strategy_policy_from_runtime()
    allow_secondary_injuries = _allow_secondary_injuries_override(
        cli_flag=bool(getattr(args, "allow_secondary_injuries", False)),
        default=bool(policy["allow_secondary_injuries"]),
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
    write_markdown: bool = False,
) -> tuple[Path, Path | None]:
    return write_discovery_execution_reports(
        store=store,
        execution_snapshot_id=execution_snapshot_id,
        report=report,
        write_markdown=write_markdown,
    )


def _load_strategy_inputs(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    offline: bool,
    block_paid: bool,
    refresh_context: bool,
    probabilistic_profile: str,
) -> tuple[
    Path,
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, dict[str, str]],
    list[dict[str, Any]],
    dict[str, Any],
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
    context_repo = NBARepository.from_store(store=store, snapshot_id=snapshot_id)
    identity_map_path = context_repo.identity_map_path()
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
    minutes_probabilities = load_minutes_prob_index_for_snapshot(
        layout=build_nba_layout(Path(_runtime_nba_data_dir()).expanduser().resolve()),
        snapshot_day=_snapshot_date(snapshot_id),
        probabilistic_profile=probabilistic_profile,
        auto_build=True,
    )
    return (
        snapshot_dir,
        manifest,
        rows,
        event_context,
        slate_rows,
        injuries,
        roster,
        player_identity_map,
        minutes_probabilities,
    )


def _cmd_strategy_run(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    strategy_requested = str(getattr(args, "strategy", "s001"))
    plugin = get_strategy(strategy_requested)
    strategy_id = normalize_strategy_id(plugin.info.id)
    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    allow_secondary_injuries = _allow_secondary_injuries_override(
        cli_flag=bool(getattr(args, "allow_secondary_injuries", False)),
        default=_env_bool("PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES", False),
    )
    stale_quote_minutes_env = _env_int("PROP_EV_STRATEGY_STALE_QUOTE_MINUTES", 20)
    require_fresh_context_env = _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True)
    probabilistic_profile = _resolve_input_probabilistic_profile(
        default_profile=str(_runtime_strategy_probabilistic_profile()),
        probabilistic_profile_arg=str(getattr(args, "probabilistic_profile", "")),
        strategy_ids=[strategy_id],
    )
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
        minutes_probabilities,
    ) = _load_strategy_inputs(
        store=store,
        snapshot_id=snapshot_id,
        offline=bool(args.offline),
        block_paid=bool(getattr(args, "block_paid", False)),
        refresh_context=bool(args.refresh_context),
        probabilistic_profile=probabilistic_profile,
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

    max_picks_default = _env_int("PROP_EV_STRATEGY_MAX_PICKS_DEFAULT", 5)
    requested_max_picks = int(getattr(args, "max_picks", 0))
    resolved_max_picks = (
        requested_max_picks if requested_max_picks > 0 else max(0, int(max_picks_default))
    )
    rolling_priors: dict[str, Any] = {}
    strategy_recipe = getattr(plugin, "recipe", None)
    if bool(getattr(strategy_recipe, "use_rolling_priors", False)):
        rolling_source_strategy_id = str(
            getattr(strategy_recipe, "rolling_priors_source_strategy_id", "") or strategy_id
        )
        rolling_source_strategy_id = normalize_strategy_id(rolling_source_strategy_id)
        rolling_priors = _load_rolling_priors_for_strategy(
            store=store,
            strategy_id=rolling_source_strategy_id,
            snapshot_id=snapshot_id,
        )
    replay_quote_now_utc = (
        _replay_quote_now_from_manifest(manifest) if strategy_run_mode == "replay" else None
    )
    config = StrategyRunConfig(
        top_n=int(args.top_n),
        max_picks=resolved_max_picks,
        min_ev=float(args.min_ev),
        allow_tier_b=bool(args.allow_tier_b),
        require_official_injuries=bool(effective_require_official),
        stale_quote_minutes=int(stale_quote_minutes),
        require_fresh_context=bool(require_fresh_context),
        probabilistic_profile=probabilistic_profile,
        quote_now_utc=replay_quote_now_utc,
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
        rolling_priors=rolling_priors,
        minutes_probabilities=(
            minutes_probabilities if isinstance(minutes_probabilities, dict) else None
        ),
    )
    result = plugin.run(inputs=inputs, config=config)
    report = decorate_report(result.report, strategy=plugin.info, config=result.config)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    write_markdown = bool(getattr(args, "write_markdown", False))
    write_canonical_raw = getattr(args, "write_canonical", None)
    if write_canonical_raw is None:
        write_canonical = bool(strategy_id == "s001")
    else:
        write_canonical = bool(write_canonical_raw)

    json_path, md_path = write_strategy_reports(
        reports_dir=reports_dir,
        report=report,
        top_n=args.top_n,
        strategy_id=strategy_id,
        write_canonical=write_canonical,
        write_markdown=write_markdown,
    )
    execution_plan_path = write_execution_plan(
        reports_dir=reports_dir,
        report=report,
        strategy_id=strategy_id,
        write_canonical=write_canonical,
    )
    write_backtest_artifacts_flag = bool(getattr(args, "write_backtest_artifacts", False))
    backtest: dict[str, Any] = {
        "seed_jsonl": "",
        "results_template_csv": "",
        "readiness_json": "",
        "ready_for_backtest_seed": False,
        "ready_for_settlement": False,
    }
    if write_backtest_artifacts_flag:
        backtest = write_backtest_artifacts(
            snapshot_dir=snapshot_dir,
            reports_dir=reports_dir,
            report=report,
            selection="ranked",
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
    print(f"strategy_max_picks={resolved_max_picks}")
    print(
        "rolling_priors_window_days={} rolling_priors_rows_used={}".format(
            int(rolling_priors.get("window_days", 0)),
            int(rolling_priors.get("rows_used", 0)),
        )
    )
    print(f"probabilistic_profile={probabilistic_profile}")
    print(f"stale_quote_minutes={stale_quote_minutes}")
    print(f"require_fresh_context={str(bool(require_fresh_context)).lower()}")
    print(f"health_gates={','.join(health_gates) if health_gates else 'none'}")
    print(f"report_json={json_path}")
    print(f"execution_plan_json={execution_plan_path}")
    if write_markdown:
        print(f"report_md={md_path}")
    card = reports_dir / "strategy-card.md"
    if not write_canonical:
        card = card.with_name(f"{card.stem}.{strategy_id}{card.suffix}")
    if write_markdown:
        print(f"report_card={card}")
    if write_backtest_artifacts_flag:
        print(f"backtest_seed_jsonl={backtest['seed_jsonl']}")
        print(f"backtest_results_template_csv={backtest['results_template_csv']}")
        print(f"backtest_readiness_json={backtest['readiness_json']}")
    context_repo = NBARepository.from_store(store=store, snapshot_id=snapshot_id)
    identity_map_path = context_repo.identity_map_path()
    identity_map = load_identity_map(identity_map_path)
    entries = (
        len(identity_map.get("players", {})) if isinstance(identity_map.get("players"), dict) else 0
    )
    print(f"identity_map={identity_map_path} entries={entries}")
    injuries_context_path, roster_context_path, _results_context_path = context_repo.context_paths()
    print(f"injuries_context={injuries_context_path}")
    print(f"roster_context={roster_context_path}")

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
            reports_dir=reports_dir,
            report=projected_report,
            top_n=max(0, execution_top_n),
            tag=execution_tag,
            write_markdown=write_markdown,
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
        if write_markdown:
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


def _cmd_strategy_compare(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    write_markdown = bool(getattr(args, "write_markdown", False))

    strategy_ids = _parse_strategy_ids(getattr(args, "strategies", ""))
    if len(strategy_ids) < 2:
        raise CLIError("compare requires --strategies with at least 2 unique ids")

    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    stale_quote_minutes_env = _env_int("PROP_EV_STRATEGY_STALE_QUOTE_MINUTES", 20)
    require_fresh_context_env = _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True)
    probabilistic_profile = _resolve_input_probabilistic_profile(
        default_profile=str(_runtime_strategy_probabilistic_profile()),
        probabilistic_profile_arg=str(getattr(args, "probabilistic_profile", "")),
        strategy_ids=strategy_ids,
    )
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
        minutes_probabilities,
    ) = _load_strategy_inputs(
        store=store,
        snapshot_id=snapshot_id,
        offline=bool(args.offline),
        block_paid=bool(getattr(args, "block_paid", False)),
        refresh_context=bool(args.refresh_context),
        probabilistic_profile=probabilistic_profile,
    )
    replay_quote_now_utc = (
        _replay_quote_now_from_manifest(manifest) if strategy_run_mode == "replay" else None
    )

    base_config = StrategyRunConfig(
        top_n=int(args.top_n),
        max_picks=(
            int(args.max_picks)
            if int(args.max_picks) > 0
            else _env_int("PROP_EV_STRATEGY_MAX_PICKS_DEFAULT", 5)
        ),
        min_ev=float(args.min_ev),
        allow_tier_b=bool(args.allow_tier_b),
        require_official_injuries=bool(require_official_injuries),
        stale_quote_minutes=int(stale_quote_minutes),
        require_fresh_context=bool(require_fresh_context),
        probabilistic_profile=probabilistic_profile,
        quote_now_utc=replay_quote_now_utc,
    )
    compare_rows: list[dict[str, Any]] = []
    ranked_sets: dict[str, set[tuple[str, str, str, float, str]]] = {}
    for requested in strategy_ids:
        plugin = get_strategy(requested)
        strategy_id = normalize_strategy_id(plugin.info.id)
        rolling_priors: dict[str, Any] = {}
        strategy_recipe = getattr(plugin, "recipe", None)
        if bool(getattr(strategy_recipe, "use_rolling_priors", False)):
            rolling_source_strategy_id = str(
                getattr(strategy_recipe, "rolling_priors_source_strategy_id", "") or strategy_id
            )
            rolling_source_strategy_id = normalize_strategy_id(rolling_source_strategy_id)
            rolling_priors = _load_rolling_priors_for_strategy(
                store=store,
                strategy_id=rolling_source_strategy_id,
                snapshot_id=snapshot_id,
            )
        inputs = StrategyInputs(
            snapshot_id=snapshot_id,
            manifest=manifest,
            rows=rows,
            injuries=injuries if isinstance(injuries, dict) else None,
            roster=roster if isinstance(roster, dict) else None,
            event_context=event_context if isinstance(event_context, dict) else None,
            slate_rows=slate_rows,
            player_identity_map=(
                player_identity_map if isinstance(player_identity_map, dict) else None
            ),
            rolling_priors=rolling_priors,
            minutes_probabilities=(
                minutes_probabilities if isinstance(minutes_probabilities, dict) else None
            ),
        )
        result = plugin.run(inputs=inputs, config=base_config)
        report = decorate_report(result.report, strategy=plugin.info, config=result.config)
        strategy_id = normalize_strategy_id(report.get("strategy_id", strategy_id))

        write_strategy_reports(
            reports_dir=reports_dir,
            report=report,
            top_n=int(args.top_n),
            strategy_id=strategy_id,
            write_canonical=False,
            write_markdown=write_markdown,
        )
        write_execution_plan(
            reports_dir=reports_dir,
            report=report,
            strategy_id=strategy_id,
            write_canonical=False,
        )
        write_backtest_artifacts(
            snapshot_dir=snapshot_dir,
            reports_dir=reports_dir,
            report=report,
            selection="ranked",
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
                "rolling_priors_rows_used": int(summary.get("rolling_priors_rows_used", 0)),
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
            "max_picks": int(base_config.max_picks),
        },
        "strategies": sorted(compare_rows, key=lambda row: row.get("strategy_id", "")),
        "ranked_overlap": {
            "intersection_all": intersection_count,
            "union_all": len(union),
        },
    }
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "strategy-compare.json"
    md_path = reports_dir / "strategy-compare.md"
    json_path.write_text(
        json.dumps(compare_report, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    md_path.write_text(_render_strategy_compare_markdown(compare_report), encoding="utf-8")

    print(f"snapshot_id={snapshot_id}")
    print(f"strategies={','.join(strategy_ids)}")
    print(f"strategy_max_picks={int(base_config.max_picks)}")
    print(f"probabilistic_profile={probabilistic_profile}")
    print(f"compare_json={json_path}")
    print(f"compare_md={md_path}")
    return 0


def _resolve_complete_day_dataset_id(data_root: Path, requested: str) -> str:
    try:
        return _resolve_complete_day_dataset_id_impl(data_root, requested)
    except RuntimeError as exc:
        raise CLIError(str(exc)) from exc


def _complete_day_snapshots(data_root: Path, dataset_id_value: str) -> list[tuple[str, str]]:
    return _complete_day_snapshots_impl(data_root, dataset_id_value)


def _parse_positive_int_csv(value: str, *, default: list[int], flag_name: str) -> list[int]:
    try:
        return _parse_positive_int_csv_impl(value, default=default, flag_name=flag_name)
    except RuntimeError as exc:
        raise CLIError(str(exc)) from exc


def _run_cli_subcommand(
    *,
    args: list[str],
    env: dict[str, str] | None,
    cwd: Path,
    global_cli_args: Sequence[str] | None = None,
) -> str:
    cmd = [sys.executable, "-m", "prop_ev.cli"]
    if global_cli_args:
        cmd.extend(global_cli_args)
    cmd.extend(args)
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=cwd,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        stdout = proc.stdout.strip()
        details = stderr or stdout or f"exit={proc.returncode}"
        raise CLIError(f"subcommand failed ({' '.join(args)}): {details}")
    return proc.stdout


def _cmd_strategy_ablation(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    data_root = store.root
    dataset_id_value = _resolve_complete_day_dataset_id(
        data_root, str(getattr(args, "dataset_id", ""))
    )
    complete_rows = _complete_day_snapshots(data_root, dataset_id_value)
    if not complete_rows:
        raise CLIError(f"dataset has no complete indexed days: {dataset_id_value}")

    strategy_ids = _parse_strategy_ids(str(getattr(args, "strategies", "")))
    if not strategy_ids:
        raise CLIError("ablation requires --strategies")
    if len(strategy_ids) < 2:
        raise CLIError("ablation requires at least 2 strategies")
    caps = _parse_positive_int_csv(
        str(getattr(args, "caps", "")),
        default=[1, 2, 5],
        flag_name="--caps",
    )
    force_days = {item.strip() for item in parse_csv(str(getattr(args, "force_days", ""))) if item}
    force_strategies = set(_parse_strategy_ids(str(getattr(args, "force_strategies", ""))))
    force_all = bool(getattr(args, "force", False))
    reuse_existing = bool(getattr(args, "reuse_existing", True)) and not force_all

    default_profile = str(_runtime_strategy_probabilistic_profile())
    probabilistic_profile = _resolve_input_probabilistic_profile(
        default_profile=default_profile,
        probabilistic_profile_arg=str(getattr(args, "probabilistic_profile", "")),
        strategy_ids=list(strategy_ids),
    )

    reports_root_raw = str(getattr(args, "reports_root", "")).strip()
    base_reports_root = (
        Path(reports_root_raw).expanduser().resolve()
        if reports_root_raw
        else report_outputs_root(store)
    )
    run_id_raw = str(getattr(args, "run_id", "")).strip()
    if run_id_raw:
        run_id = _sanitize_analysis_run_id(run_id_raw)
        if not run_id:
            raise CLIError("--run-id must contain letters, numbers, '_' '-' or '.'")
    else:
        run_id = "latest"
    if not run_id:
        raise CLIError("failed to build run id")
    run_root = base_reports_root / "ablation" / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    cwd = Path.cwd()
    code_revision = _ablation_git_head()
    nba_data_dir = str(Path(_runtime_nba_data_dir()).expanduser().resolve())
    runtime_dir = str(Path(_runtime_runtime_dir()).expanduser().resolve())

    manifest_hashes = {
        snapshot_id: _sha256_file(store.snapshot_dir(snapshot_id) / "manifest.json")
        for _, snapshot_id in complete_rows
    }

    prebuild_minutes_cache = bool(getattr(args, "prebuild_minutes_cache", True))
    if prebuild_minutes_cache and probabilistic_profile == "minutes_v1":
        nba_dir = Path(_runtime_nba_data_dir()).expanduser().resolve()
        nba_layout = build_nba_layout(nba_dir)
        prebuild_workers = max(1, int(getattr(args, "max_workers", 6)))

        def _prebuild_one(day_value: str) -> tuple[str, int]:
            payload = load_minutes_prob_index_for_snapshot(
                layout=nba_layout,
                snapshot_day=day_value,
                probabilistic_profile="minutes_v1",
                auto_build=True,
            )
            meta = payload.get("meta", {}) if isinstance(payload.get("meta"), dict) else {}
            rows = int(meta.get("rows", 0) or 0)
            return day_value, rows

        with ThreadPoolExecutor(max_workers=prebuild_workers) as executor:
            futures = {executor.submit(_prebuild_one, day): day for day, _ in complete_rows}
            for future in as_completed(futures):
                day_value, rows = future.result()
                print(f"minutes_cache_day={day_value} rows={rows}")

    mode = str(getattr(args, "mode", "replay"))
    top_n = max(0, int(getattr(args, "top_n", 10)))
    min_ev = float(getattr(args, "min_ev", 0.01))
    allow_tier_b = bool(getattr(args, "allow_tier_b", False))
    offline = bool(getattr(args, "offline", True))
    block_paid = bool(getattr(args, "block_paid", True))
    refresh_context = bool(getattr(args, "refresh_context", False))
    results_source = str(getattr(args, "results_source", "historical")).strip() or "historical"
    prune_intermediate = bool(getattr(args, "prune_intermediate", True))
    write_scoreboard_pdf = bool(getattr(args, "write_scoreboard_pdf", True))
    keep_scoreboard_tex = bool(getattr(args, "keep_scoreboard_tex", False))
    max_workers = max(1, int(getattr(args, "max_workers", 6)))
    cap_workers = max(1, int(getattr(args, "cap_workers", 3)))
    cap_workers = min(cap_workers, len(caps))

    min_graded = max(0, int(getattr(args, "min_graded", 0)))
    bin_size = float(getattr(args, "bin_size", 0.1))
    require_scored_fraction = float(getattr(args, "require_scored_fraction", 0.9))
    ece_slack = float(getattr(args, "ece_slack", 0.01))
    brier_slack = float(getattr(args, "brier_slack", 0.01))
    power_alpha = float(getattr(args, "power_alpha", 0.05))
    power_level = float(getattr(args, "power_level", 0.8))
    power_target_uplifts = str(getattr(args, "power_target_uplifts", "0.01,0.02,0.03,0.05")).strip()
    power_target_uplift_gate = float(getattr(args, "power_target_uplift_gate", 0.02))
    if power_target_uplift_gate <= 0.0:
        raise CLIError("--power-target-uplift-gate must be > 0")
    require_power_gate = bool(getattr(args, "require_power_gate", False))
    calibration_map_mode = str(getattr(args, "calibration_map_mode", "walk_forward")).strip()
    analysis_prefix_raw = str(getattr(args, "analysis_run_prefix", "ablation")).strip()
    analysis_prefix = _sanitize_analysis_run_id(analysis_prefix_raw)
    if not analysis_prefix:
        raise CLIError("--analysis-run-prefix must contain letters, numbers, '_' '-' or '.'")
    snapshot_id_for_summary = str(getattr(args, "snapshot_id", "")).strip() or complete_rows[-1][1]

    cap_results: list[dict[str, Any]] = []

    def _cap_worker(cap: int) -> dict[str, Any]:
        cap_root = run_root / f"cap-max{cap}"
        cap_root.mkdir(parents=True, exist_ok=True)
        cap_global_cli_args = [
            "--data-dir",
            str(store.root),
            "--reports-dir",
            str(cap_root),
            "--nba-data-dir",
            nba_data_dir,
            "--runtime-dir",
            runtime_dir,
        ]
        state_dir = _ablation_state_dir(cap_root)
        state_dir.mkdir(parents=True, exist_ok=True)

        cap_summary: dict[str, Any] = {
            "cap": cap,
            "compare_ran": 0,
            "compare_skipped": 0,
            "settled": 0,
            "settle_skipped": 0,
            "no_seed_rows": 0,
            "analysis_scoreboard_pdf": "",
            "analysis_scoreboard_pdf_status": "",
            "pruned_dirs": 0,
            "pruned_files": 0,
        }

        def _snapshot_worker(day_snapshot: tuple[str, str]) -> dict[str, int]:
            day_value, snapshot_id = day_snapshot
            reports_dir = snapshot_reports_dir(store, snapshot_id, reports_root=cap_root)
            reports_dir.mkdir(parents=True, exist_ok=True)
            manifest_hash = manifest_hashes.get(snapshot_id, "")
            forced_day = force_all or day_value in force_days or snapshot_id in force_days

            compare_payload = {
                "kind": "compare",
                "snapshot_id": snapshot_id,
                "day": day_value,
                "strategies": list(strategy_ids),
                "cap": cap,
                "top_n": top_n,
                "min_ev": min_ev,
                "mode": mode,
                "allow_tier_b": allow_tier_b,
                "probabilistic_profile": probabilistic_profile,
                "manifest_hash": manifest_hash,
                "code_revision": code_revision,
            }
            compare_hash = _build_ablation_input_hash(payload=compare_payload)
            compare_state_path = state_dir / f"{snapshot_id}.compare.json"
            compare_cached = (
                reuse_existing
                and not forced_day
                and _ablation_compare_cache_valid(
                    reports_dir=reports_dir,
                    state_path=compare_state_path,
                    expected_hash=compare_hash,
                    strategy_ids=strategy_ids,
                )
            )

            strategy_hash_by_id: dict[str, str] = {}
            strategy_cached_by_id: dict[str, bool] = {}
            strategy_core_ready: dict[str, bool] = {}
            for strategy_id in strategy_ids:
                strategy_payload = {
                    "kind": "strategy",
                    "snapshot_id": snapshot_id,
                    "day": day_value,
                    "strategy_id": strategy_id,
                    "cap": cap,
                    "top_n": top_n,
                    "min_ev": min_ev,
                    "mode": mode,
                    "allow_tier_b": allow_tier_b,
                    "probabilistic_profile": probabilistic_profile,
                    "results_source": results_source,
                    "manifest_hash": manifest_hash,
                    "code_revision": code_revision,
                }
                strategy_hash = _build_ablation_input_hash(payload=strategy_payload)
                strategy_hash_by_id[strategy_id] = strategy_hash
                strategy_state_path = state_dir / f"{snapshot_id}.{strategy_id}.json"
                strategy_cached_by_id[strategy_id] = (
                    reuse_existing
                    and not forced_day
                    and strategy_id not in force_strategies
                    and _ablation_strategy_cache_valid(
                        reports_dir=reports_dir,
                        state_path=strategy_state_path,
                        expected_hash=strategy_hash,
                        strategy_id=strategy_id,
                    )
                )
                strategy_core_ready[strategy_id] = (
                    reports_dir / f"strategy-report.{strategy_id}.json"
                ).exists() and (reports_dir / f"backtest-seed.{strategy_id}.jsonl").exists()

            needs_compare = (
                not compare_cached
                or forced_day
                or any(strategy_id in force_strategies for strategy_id in strategy_ids)
                or any(
                    not strategy_core_ready.get(strategy_id, False) for strategy_id in strategy_ids
                )
            )
            local_summary: dict[str, int] = {
                "compare_ran": 0,
                "compare_skipped": 0,
                "settled": 0,
                "settle_skipped": 0,
                "no_seed_rows": 0,
            }
            if needs_compare:
                compare_cmd = [
                    "strategy",
                    "compare",
                    "--snapshot-id",
                    snapshot_id,
                    "--strategies",
                    ",".join(strategy_ids),
                    "--top-n",
                    str(top_n),
                    "--max-picks",
                    str(cap),
                    "--min-ev",
                    str(min_ev),
                    "--mode",
                    mode,
                    "--probabilistic-profile",
                    probabilistic_profile,
                ]
                if allow_tier_b:
                    compare_cmd.append("--allow-tier-b")
                if offline:
                    compare_cmd.append("--offline")
                if block_paid:
                    compare_cmd.append("--block-paid")
                if refresh_context:
                    compare_cmd.append("--refresh-context")
                _run_cli_subcommand(
                    args=compare_cmd,
                    env=None,
                    cwd=cwd,
                    global_cli_args=cap_global_cli_args,
                )
                _ablation_write_state(
                    compare_state_path,
                    {
                        "input_hash": compare_hash,
                        "snapshot_id": snapshot_id,
                        "day": day_value,
                        "cap": cap,
                        "strategies": list(strategy_ids),
                        "generated_at_utc": _iso(_utc_now()),
                    },
                )
                local_summary["compare_ran"] += 1
            else:
                local_summary["compare_skipped"] += 1

            for strategy_id in strategy_ids:
                strategy_state_path = state_dir / f"{snapshot_id}.{strategy_id}.json"
                strategy_hash = strategy_hash_by_id[strategy_id]
                forced_strategy = forced_day or strategy_id in force_strategies
                if (
                    reuse_existing
                    and not forced_strategy
                    and _ablation_strategy_cache_valid(
                        reports_dir=reports_dir,
                        state_path=strategy_state_path,
                        expected_hash=strategy_hash,
                        strategy_id=strategy_id,
                    )
                ):
                    local_summary["settle_skipped"] += 1
                    continue

                seed_path = reports_dir / f"backtest-seed.{strategy_id}.jsonl"
                seed_rows = _ablation_count_seed_rows(seed_path)
                if seed_rows == 0:
                    _ablation_write_state(
                        strategy_state_path,
                        {
                            "input_hash": strategy_hash,
                            "snapshot_id": snapshot_id,
                            "day": day_value,
                            "cap": cap,
                            "strategy_id": strategy_id,
                            "seed_rows": 0,
                            "generated_at_utc": _iso(_utc_now()),
                        },
                    )
                    local_summary["no_seed_rows"] += 1
                    continue

                settle_cmd = [
                    "strategy",
                    "settle",
                    "--snapshot-id",
                    snapshot_id,
                    "--strategy-report-file",
                    f"strategy-report.{strategy_id}.json",
                    "--results-source",
                    results_source,
                    "--write-csv",
                    "--no-pdf",
                    "--no-json",
                ]
                if offline:
                    settle_cmd.append("--offline")
                _run_cli_subcommand(
                    args=settle_cmd,
                    env=None,
                    cwd=cwd,
                    global_cli_args=cap_global_cli_args,
                )
                _ablation_write_state(
                    strategy_state_path,
                    {
                        "input_hash": strategy_hash,
                        "snapshot_id": snapshot_id,
                        "day": day_value,
                        "cap": cap,
                        "strategy_id": strategy_id,
                        "seed_rows": seed_rows,
                        "generated_at_utc": _iso(_utc_now()),
                    },
                )
                local_summary["settled"] += 1

            return local_summary

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_snapshot_worker, day_snapshot): day_snapshot
                for day_snapshot in complete_rows
            }
            aggregate_keys = (
                "compare_ran",
                "compare_skipped",
                "settled",
                "settle_skipped",
                "no_seed_rows",
                "pruned_dirs",
                "pruned_files",
            )
            for future in as_completed(futures):
                local = future.result()
                for key in aggregate_keys:
                    cap_summary[key] += int(local.get(key, 0))

        analysis_run_id = _sanitize_analysis_run_id(
            _build_ablation_analysis_run_id(
                analysis_prefix=analysis_prefix,
                run_id=run_id,
                cap=cap,
            )
        )
        if not analysis_run_id:
            raise CLIError("failed to build analysis run id")
        summarize_cmd = [
            "strategy",
            "backtest-summarize",
            "--snapshot-id",
            snapshot_id_for_summary,
            "--strategies",
            ",".join(strategy_ids),
            "--all-complete-days",
            "--dataset-id",
            dataset_id_value,
            "--min-graded",
            str(min_graded),
            "--bin-size",
            str(bin_size),
            "--require-scored-fraction",
            str(require_scored_fraction),
            "--ece-slack",
            str(ece_slack),
            "--brier-slack",
            str(brier_slack),
            "--power-alpha",
            str(power_alpha),
            "--power-level",
            str(power_level),
            "--power-picks-per-day",
            str(cap),
            "--power-target-uplifts",
            power_target_uplifts,
            "--power-target-uplift-gate",
            str(power_target_uplift_gate),
            "--write-analysis-scoreboard",
            "--analysis-run-id",
            analysis_run_id,
            "--write-calibration-map",
            "--calibration-map-mode",
            calibration_map_mode,
        ]
        if write_scoreboard_pdf:
            summarize_cmd.append("--write-analysis-pdf")
        if keep_scoreboard_tex:
            summarize_cmd.append("--keep-analysis-tex")
        if require_power_gate:
            summarize_cmd.append("--require-power-gate")
        summarize_stdout = _run_cli_subcommand(
            args=summarize_cmd,
            env=None,
            cwd=cwd,
            global_cli_args=cap_global_cli_args,
        )
        kv = _parse_cli_kv(summarize_stdout)
        cap_summary["summary_json"] = kv.get("summary_json", "")
        cap_summary["analysis_scoreboard_json"] = kv.get("analysis_scoreboard_json", "")
        cap_summary["analysis_scoreboard_pdf"] = kv.get("analysis_scoreboard_pdf", "")
        cap_summary["analysis_scoreboard_pdf_status"] = kv.get("analysis_scoreboard_pdf_status", "")
        cap_summary["calibration_map_json"] = kv.get("calibration_map_json", "")
        cap_summary["winner_strategy_id"] = kv.get("winner_strategy_id", "")
        cap_summary["reports_root"] = str(cap_root)
        if prune_intermediate:
            prune_stats = _ablation_prune_cap_root(cap_root)
            cap_summary["pruned_dirs"] = int(prune_stats.get("removed_dirs", 0))
            cap_summary["pruned_files"] = int(prune_stats.get("removed_files", 0))
        print(
            f"ablation_cap={cap} compare_ran={cap_summary['compare_ran']} "
            f"settle_ran={cap_summary['settled']} "
            f"settle_skipped={cap_summary['settle_skipped']} "
            f"no_seed_rows={cap_summary['no_seed_rows']} "
            f"pruned_files={cap_summary['pruned_files']}"
        )
        print(f"ablation_cap_reports_root={cap_root}")
        if cap_summary["analysis_scoreboard_json"]:
            print(f"ablation_cap_scoreboard_json={cap_summary['analysis_scoreboard_json']}")
        if cap_summary["analysis_scoreboard_pdf"]:
            print(f"ablation_cap_scoreboard_pdf={cap_summary['analysis_scoreboard_pdf']}")
        if cap_summary["analysis_scoreboard_pdf_status"]:
            print(
                "ablation_cap_scoreboard_pdf_status="
                f"{cap_summary['analysis_scoreboard_pdf_status']}"
            )
        return cap_summary

    with ThreadPoolExecutor(max_workers=cap_workers) as executor:
        futures = {executor.submit(_cap_worker, cap): cap for cap in caps}
        for future in as_completed(futures):
            cap_results.append(future.result())

    cap_results.sort(key=lambda row: int(row.get("cap", 0)))
    run_summary = {
        "schema_version": 1,
        "report_kind": "ablation_run",
        "generated_at_utc": _iso(_utc_now()),
        "run_id": run_id,
        "dataset_id": dataset_id_value,
        "snapshot_count": len(complete_rows),
        "strategies": list(strategy_ids),
        "caps": caps,
        "probabilistic_profile": probabilistic_profile,
        "results_source": results_source,
        "reuse_existing": reuse_existing,
        "prune_intermediate": prune_intermediate,
        "reports_root": str(run_root),
        "caps_summary": cap_results,
    }
    summary_path = run_root / "ablation-run.json"
    summary_path.write_text(
        json.dumps(run_summary, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    print(f"ablation_run_id={run_id}")
    print(f"ablation_summary_json={summary_path}")
    for row in cap_results:
        cap_value = int(row.get("cap", 0))
        winner = str(row.get("winner_strategy_id", ""))
        scoreboard = str(row.get("analysis_scoreboard_json", ""))
        print(
            f"ablation_cap_result cap={cap_value} "
            f"winner_strategy_id={winner} "
            f"scoreboard={scoreboard}"
        )
    return 0


def _cmd_strategy_backtest_summarize(args: argparse.Namespace) -> int:
    from prop_ev.backtest_summary import load_backtest_csv, summarize_backtest_rows
    from prop_ev.calibration_map import CalibrationMode, build_calibration_map
    from prop_ev.eval_scoreboard import (
        PromotionThresholds,
        build_power_gate,
        build_promotion_gate,
        pick_execution_winner,
        pick_promotion_winner,
        resolve_baseline_strategy_id,
    )
    from prop_ev.power_guidance import PowerGuidanceAssumptions, build_power_guidance

    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    reports_dir = snapshot_reports_dir(store, snapshot_id)

    def _resolve_results_csv(reports_dir: Path, strategy_id: str) -> Path:
        settlement_path = reports_dir / f"settlement.{strategy_id}.csv"
        if settlement_path.exists():
            return settlement_path
        if strategy_id == "s001":
            settlement_path = reports_dir / "settlement.csv"
            if settlement_path.exists():
                return settlement_path
        template_path = reports_dir / f"backtest-results-template.{strategy_id}.csv"
        if template_path.exists():
            return template_path
        if strategy_id == "s001":
            template_path = reports_dir / "backtest-results-template.csv"
        return template_path

    bin_size = float(getattr(args, "bin_size", 0.05))
    min_graded = max(0, int(getattr(args, "min_graded", 0)))
    require_scored_fraction = float(getattr(args, "require_scored_fraction", 0.9))
    if require_scored_fraction < 0.0 or require_scored_fraction > 1.0:
        raise CLIError("--require-scored-fraction must be between 0 and 1")
    ece_slack = max(0.0, float(getattr(args, "ece_slack", 0.01)))
    brier_slack = max(0.0, float(getattr(args, "brier_slack", 0.01)))
    power_alpha = float(getattr(args, "power_alpha", 0.05))
    power_level = float(getattr(args, "power_level", 0.8))
    if power_alpha <= 0.0 or power_alpha >= 1.0:
        raise CLIError("--power-alpha must be between 0 and 1 (exclusive)")
    if power_level <= 0.0 or power_level >= 1.0:
        raise CLIError("--power-level must be between 0 and 1 (exclusive)")
    power_picks_per_day = max(1, int(getattr(args, "power_picks_per_day", 5)))
    power_target_uplifts = _parse_positive_float_csv(
        str(getattr(args, "power_target_uplifts", "0.01,0.02,0.03,0.05")),
        default=[0.01, 0.02, 0.03, 0.05],
        flag_name="--power-target-uplifts",
    )
    power_target_uplift_gate = float(getattr(args, "power_target_uplift_gate", 0.02))
    if power_target_uplift_gate <= 0.0:
        raise CLIError("--power-target-uplift-gate must be > 0")
    require_power_gate = bool(getattr(args, "require_power_gate", False))
    write_analysis_scoreboard = bool(getattr(args, "write_analysis_scoreboard", False))
    write_analysis_pdf = bool(getattr(args, "write_analysis_pdf", False))
    keep_analysis_tex = bool(getattr(args, "keep_analysis_tex", False))
    analysis_run_id_raw = str(getattr(args, "analysis_run_id", "")).strip()
    all_complete_days = bool(getattr(args, "all_complete_days", False))
    write_calibration_map = bool(getattr(args, "write_calibration_map", False))
    calibration_map_mode = (
        str(getattr(args, "calibration_map_mode", "walk_forward")).strip().lower()
    )
    if calibration_map_mode not in {"walk_forward", "in_sample"}:
        raise CLIError("--calibration-map-mode must be one of: walk_forward,in_sample")
    explicit_results = getattr(args, "results", None)
    computed = []
    day_coverage: dict[str, Any] = {}
    rows_for_map: dict[str, list[dict[str, str]]] = {}
    daily_pnl_by_strategy: dict[str, dict[str, float]] = {}
    dataset_id_value = ""
    if all_complete_days:
        if isinstance(explicit_results, list) and explicit_results:
            raise CLIError("--all-complete-days cannot be combined with --results")
        strategy_ids = _parse_strategy_ids(getattr(args, "strategies", ""))
        if not strategy_ids:
            raise CLIError("--all-complete-days requires --strategies")

        data_root = store.root
        dataset_id_value = _resolve_complete_day_dataset_id(
            data_root,
            str(getattr(args, "dataset_id", "")),
        )
        complete_days = _complete_day_snapshots(data_root, dataset_id_value)
        if not complete_days:
            raise CLIError(f"dataset has no complete indexed days: {dataset_id_value}")

        rows_by_strategy: dict[str, list[dict[str, str]]] = {sid: [] for sid in strategy_ids}
        daily_pnl_by_strategy = {sid: {} for sid in strategy_ids}
        skipped_days: list[dict[str, str]] = []
        days_with_any_results: set[str] = set()
        for day, day_snapshot_id in complete_days:
            day_reports_dir = snapshot_reports_dir(store, day_snapshot_id)
            for strategy_id in strategy_ids:
                path = _resolve_results_csv(day_reports_dir, strategy_id)
                if not path.exists():
                    skipped_days.append(
                        {
                            "day": day,
                            "snapshot_id": day_snapshot_id,
                            "strategy_id": strategy_id,
                            "reason": "missing_backtest_csv",
                        }
                    )
                    continue
                rows = load_backtest_csv(path)
                if rows:
                    rows_by_strategy[strategy_id].extend(rows)
                    day_summary = summarize_backtest_rows(
                        rows,
                        strategy_id=strategy_id,
                        bin_size=bin_size,
                    )
                    if day_summary.rows_graded > 0:
                        daily_pnl_by_strategy[strategy_id][day] = float(day_summary.total_pnl_units)
                    days_with_any_results.add(day)

        for strategy_id in strategy_ids:
            summary = summarize_backtest_rows(
                rows_by_strategy[strategy_id],
                strategy_id=strategy_id,
                bin_size=bin_size,
            )
            computed.append(summary)

        if not any(item.rows_total > 0 for item in computed):
            raise CLIError(
                "no backtest rows found for selected complete days/strategies; "
                "run `prop-ev strategy backtest-prep` and "
                "`prop-ev strategy settle --write-csv` first"
            )
        rows_for_map = rows_by_strategy
        day_coverage = {
            "all_complete_days": True,
            "dataset_id": dataset_id_value,
            "complete_days": len(complete_days),
            "days_with_any_results": len(days_with_any_results),
            "skipped_rows": len(skipped_days),
            "skipped": skipped_days[:200],
        }
    else:
        paths: list[tuple[str, Path]] = []
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
                paths.append((strategy_id, _resolve_results_csv(reports_dir, strategy_id)))

        for strategy_id, path in paths:
            if not path.exists():
                raise CLIError(f"missing backtest CSV: {path}")
            rows = load_backtest_csv(path)
            rows_for_map[strategy_id] = rows
            summary = summarize_backtest_rows(rows, strategy_id=strategy_id, bin_size=bin_size)
            computed.append(summary)

    requested_baseline = str(getattr(args, "baseline_strategy", "")).strip()
    if requested_baseline:
        requested_baseline = normalize_strategy_id(requested_baseline)
    baseline_strategy_id = resolve_baseline_strategy_id(
        requested=requested_baseline,
        available_strategy_ids=[item.strategy_id for item in computed],
    )
    baseline_summary = next(
        (item for item in computed if item.strategy_id == baseline_strategy_id),
        None,
    )
    thresholds = PromotionThresholds(
        min_graded=min_graded,
        min_scored_fraction=require_scored_fraction,
        ece_slack=ece_slack,
        brier_slack=brier_slack,
    )
    power_guidance: dict[str, Any] = {}
    if all_complete_days and baseline_summary is not None and daily_pnl_by_strategy:
        power_guidance = build_power_guidance(
            daily_pnl_by_strategy=daily_pnl_by_strategy,
            baseline_strategy_id=baseline_strategy_id,
            assumptions=PowerGuidanceAssumptions(
                alpha=power_alpha,
                power=power_level,
                picks_per_day=power_picks_per_day,
                target_roi_uplifts_per_bet=tuple(power_target_uplifts),
            ),
        )

    strategy_rows: list[dict[str, Any]] = []
    for summary in computed:
        row = summary.to_dict()
        promotion_gate = build_promotion_gate(
            summary=summary,
            baseline_summary=baseline_summary,
            baseline_required=bool(baseline_strategy_id),
            thresholds=thresholds,
        )
        row["promotion_gate"] = promotion_gate
        if power_guidance:
            power_gate = build_power_gate(
                summary=summary,
                power_guidance=power_guidance,
                target_roi_uplift_per_bet=power_target_uplift_gate,
            )
            row["power_gate"] = power_gate
            if bool(require_power_gate) and power_gate.get("status") == "fail":
                reasons = promotion_gate.get("reasons", [])
                if not isinstance(reasons, list):
                    reasons = []
                if "underpowered_for_target_uplift" not in reasons:
                    reasons = [*reasons, "underpowered_for_target_uplift"]
                promotion_gate["status"] = "fail"
                promotion_gate["reasons"] = sorted({str(value) for value in reasons if value})
        strategy_rows.append(row)

    winner = pick_execution_winner(strategy_rows)
    promotion_winner = pick_promotion_winner(strategy_rows)
    report = {
        "schema_version": 1,
        "report_kind": "backtest_summary",
        "generated_at_utc": _iso(_utc_now()),
        "summary": {
            "snapshot_id": snapshot_id,
            "strategy_count": len(strategy_rows),
            "min_graded": min_graded,
            "bin_size": bin_size,
            "baseline_strategy_id": baseline_strategy_id,
            "baseline_found": baseline_summary is not None,
            "require_scored_fraction": require_scored_fraction,
            "ece_slack": ece_slack,
            "brier_slack": brier_slack,
            "power_target_uplift_gate": power_target_uplift_gate,
            "require_power_gate": require_power_gate,
            "power_picks_per_day": power_picks_per_day,
            **day_coverage,
        },
        "strategies": sorted(strategy_rows, key=lambda row: row.get("strategy_id", "")),
        "winner": winner if winner is not None else {},
        "promotion_winner": promotion_winner if promotion_winner is not None else {},
    }
    if power_guidance:
        report["power_guidance"] = power_guidance
    calibration_map_payload: dict[str, Any] | None = None
    if write_calibration_map and rows_for_map:
        calibration_map_payload = build_calibration_map(
            rows_by_strategy=rows_for_map,
            bin_size=bin_size,
            mode=cast(CalibrationMode, calibration_map_mode),
            dataset_id=dataset_id_value,
        )
        report["calibration_map"] = {
            "mode": calibration_map_mode,
            "strategy_count": len(calibration_map_payload.get("strategies", {})),
        }

    reports_dir.mkdir(parents=True, exist_ok=True)
    write_markdown = bool(getattr(args, "write_markdown", False))
    json_path = reports_dir / "backtest-summary.json"
    md_path = reports_dir / "backtest-summary.md"
    calibration_map_path = reports_dir / "backtest-calibration-map.json"
    analysis_json_path: Path | None = None
    analysis_md_path: Path | None = None
    analysis_pdf_path: Path | None = None
    analysis_pdf_status = ""
    analysis_tex_path: Path | None = None
    analysis_pdf_message = ""
    json_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    if calibration_map_payload is not None:
        calibration_map_path.write_text(
            json.dumps(calibration_map_payload, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    elif calibration_map_path.exists():
        calibration_map_path.unlink()
    if write_markdown:
        md_path.write_text(_render_backtest_summary_markdown(report), encoding="utf-8")
    elif md_path.exists():
        md_path.unlink()

    if write_analysis_scoreboard:
        if analysis_run_id_raw:
            analysis_run_id = _sanitize_analysis_run_id(analysis_run_id_raw)
            if not analysis_run_id:
                raise CLIError("--analysis-run-id must contain letters, numbers, '_' '-' or '.'")
        elif all_complete_days and dataset_id_value:
            analysis_run_id = f"eval-scoreboard-dataset-{dataset_id_value[:8]}"
        else:
            analysis_run_id = f"eval-scoreboard-snapshot-{snapshot_id}"

        analysis_dir = report_outputs_root(store) / "analysis" / analysis_run_id
        analysis_dir.mkdir(parents=True, exist_ok=True)
        analysis_json_path = analysis_dir / "aggregate-scoreboard.json"
        analysis_payload = dict(report)
        analysis_payload["report_kind"] = "aggregate_scoreboard"
        analysis_payload["analysis_run_id"] = analysis_run_id
        analysis_json_path.write_text(
            json.dumps(analysis_payload, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        if write_markdown:
            analysis_md_path = analysis_dir / "aggregate-scoreboard.md"
            analysis_md_path.write_text(
                _render_backtest_summary_markdown(analysis_payload),
                encoding="utf-8",
            )
        else:
            stale_md_path = analysis_dir / "aggregate-scoreboard.md"
            if stale_md_path.exists():
                stale_md_path.unlink()
        if write_analysis_pdf:
            from prop_ev.scoreboard_pdf import render_aggregate_scoreboard_pdf

            analysis_pdf_path = analysis_dir / "aggregate-scoreboard.pdf"
            analysis_tex_source = analysis_dir / "aggregate-scoreboard.tex"
            pdf_result = render_aggregate_scoreboard_pdf(
                analysis_payload=analysis_payload,
                tex_path=analysis_tex_source,
                pdf_path=analysis_pdf_path,
                keep_tex=keep_analysis_tex,
            )
            analysis_pdf_status = str(pdf_result.get("status", "")).strip()
            analysis_pdf_message = str(pdf_result.get("message", "")).strip()
            tex_path_value = str(pdf_result.get("tex_path", "")).strip()
            if tex_path_value:
                analysis_tex_path = Path(tex_path_value)

    print(f"snapshot_id={snapshot_id}")
    print(f"summary_json={json_path}")
    if calibration_map_payload is not None:
        print(f"calibration_map_json={calibration_map_path}")
    if write_markdown:
        print(f"summary_md={md_path}")
    if analysis_json_path is not None:
        print(f"analysis_scoreboard_json={analysis_json_path}")
    if analysis_md_path is not None:
        print(f"analysis_scoreboard_md={analysis_md_path}")
    if analysis_pdf_path is not None:
        print(f"analysis_scoreboard_pdf={analysis_pdf_path}")
        print(f"analysis_scoreboard_pdf_status={analysis_pdf_status}")
        if analysis_pdf_message:
            print(f"analysis_scoreboard_pdf_message={analysis_pdf_message}")
    if analysis_tex_path is not None:
        print(f"analysis_scoreboard_tex={analysis_tex_path}")
    if winner is not None:
        print(
            "winner_strategy_id={} roi={} graded={}".format(
                winner.get("strategy_id", ""),
                winner.get("roi", ""),
                winner.get("rows_graded", 0),
            )
        )
    if promotion_winner is not None:
        print(
            "promotion_winner_strategy_id={} roi={} graded={}".format(
                promotion_winner.get("strategy_id", ""),
                promotion_winner.get("roi", ""),
                promotion_winner.get("rows_graded", 0),
            )
        )
    return 0


def _cmd_strategy_backtest_prep(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
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
        reports_dir=reports_dir,
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


def _resolve_settlement_strategy_report_path(
    *, reports_dir: Path, strategy_report_file: str
) -> Path | None:
    requested = strategy_report_file.strip()
    if requested:
        candidate = Path(requested).expanduser()
        return candidate if candidate.is_absolute() else (reports_dir / candidate)

    brief_meta_path = reports_dir / "strategy-brief.meta.json"
    if brief_meta_path.exists():
        try:
            payload = json.loads(brief_meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        if isinstance(payload, dict):
            raw = str(payload.get("strategy_report_path", "")).strip()
            if raw:
                candidate = Path(raw).expanduser()
                if not candidate.is_absolute():
                    candidate = reports_dir / candidate
                if candidate.exists():
                    return candidate

    default_path = reports_dir / "strategy-report.json"
    if default_path.exists():
        return default_path
    return None


def _cmd_strategy_settle(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    seed_path = (
        Path(str(args.seed_path)).expanduser()
        if str(getattr(args, "seed_path", "")).strip()
        else reports_dir / "backtest-seed.jsonl"
    )
    seed_rows_override: list[dict[str, Any]] | None = None
    strategy_report_for_settlement = ""
    strategy_report_path = _resolve_settlement_strategy_report_path(
        reports_dir=reports_dir,
        strategy_report_file=str(getattr(args, "strategy_report_file", "")),
    )
    using_default_seed_path = not str(getattr(args, "seed_path", "")).strip()
    if using_default_seed_path and strategy_report_path is not None:
        try:
            payload = json.loads(strategy_report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CLIError(f"invalid strategy report: {strategy_report_path}") from exc
        if isinstance(payload, dict):
            selection = "eligible"
            ranked = payload.get("ranked_plays")
            if isinstance(ranked, list) and ranked:
                selection = "ranked"
            seed_rows_override = build_backtest_seed_rows(
                report=payload,
                selection=selection,
                top_n=0,
            )
            strategy_report_for_settlement = str(strategy_report_path)
    if using_default_seed_path and seed_rows_override is None and not seed_path.exists():
        if strategy_report_path is None:
            raise CLIError(f"missing backtest seed file: {seed_path}")
        raise CLIError(
            f"could not derive settlement rows from strategy report: {strategy_report_path}"
        )

    def _resolve_settlement_suffix(
        *,
        seed_rows: list[dict[str, Any]] | None,
        seed_path: Path,
        using_default_seed_path: bool,
        strategy_report_path: Path | None,
    ) -> str:
        if using_default_seed_path and (
            strategy_report_path is None or strategy_report_path.name == "strategy-report.json"
        ):
            return ""
        resolved_rows = seed_rows
        if resolved_rows is None and seed_path.exists():
            try:
                resolved_rows = load_jsonl(seed_path)
            except OSError:
                resolved_rows = None
        if resolved_rows:
            for row in resolved_rows:
                if not isinstance(row, dict):
                    continue
                candidate = str(row.get("strategy_id", "")).strip()
                if candidate:
                    return normalize_strategy_id(candidate)
        return ""

    output_suffix = _resolve_settlement_suffix(
        seed_rows=seed_rows_override,
        seed_path=seed_path,
        using_default_seed_path=using_default_seed_path,
        strategy_report_path=strategy_report_path,
    )

    report = settle_snapshot(
        snapshot_dir=snapshot_dir,
        reports_dir=reports_dir,
        snapshot_id=snapshot_id,
        seed_path=seed_path,
        offline=bool(args.offline),
        refresh_results=bool(args.refresh_results),
        write_csv=bool(args.write_csv),
        results_source=str(getattr(args, "results_source", "auto")),
        write_markdown=bool(getattr(args, "write_markdown", False)),
        keep_tex=bool(getattr(args, "keep_tex", False)),
        write_pdf=not bool(getattr(args, "no_pdf", False)),
        output_suffix=output_suffix,
        seed_rows_override=seed_rows_override,
        strategy_report_path=strategy_report_for_settlement,
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
        settlement_md = str(artifacts.get("md", "")).strip()
        if settlement_md:
            print(f"settlement_md={settlement_md}")
        settlement_tex = str(artifacts.get("tex", "")).strip()
        if settlement_tex:
            print(f"settlement_tex={settlement_tex}")
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
    from prop_ev import cli_commands as cli_commands_module

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
    code = int(cli_commands_module._cmd_snapshot_slate(args))
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
