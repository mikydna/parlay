"""Strategy health command implementation."""

from __future__ import annotations

import argparse
import json

from prop_ev.cli_shared import (
    CLIError,
    _iso,
    _runtime_odds_data_dir,
    _utc_now,
)
from prop_ev.cli_strategy.context import _load_event_context, _load_slate_rows
from prop_ev.cli_strategy.shared import (
    _allow_secondary_injuries_override,
    _coerce_dict,
    _coerce_list,
    _count_status,
    _health_recommendations,
    _latest_snapshot_id,
    _load_strategy_context,
    _official_rows_count,
    _official_source_ready,
    _secondary_source_ready,
    _strategy_policy_from_runtime,
    _teams_in_scope,
)
from prop_ev.state_keys import (
    strategy_health_state_key,
)
from prop_ev.storage import SnapshotStore
from prop_ev.strategy import (
    build_strategy_report,
    load_jsonl,
)


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
