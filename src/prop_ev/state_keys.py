"""Human-readable descriptions for stable machine state ids."""

from __future__ import annotations

from typing import Any

STRATEGY_STATUS_KEY = {
    "modeled_with_gates": "Deterministic strategy report generated with reliability gates applied.",
}

STRATEGY_MODE_KEY = {
    "full_board": "No global health gate blocks; ranked eligible board is active.",
    "watchlist_only": (
        "One or more global health gates are active; all candidates are watchlist-only."
    ),
}

STRATEGY_HEALTH_GATE_KEY = {
    "official_injury_missing": "Official NBA injury source is unavailable or unusable.",
    "odds_snapshot_stale": "Latest quote timestamp exceeds configured staleness threshold.",
    "injuries_context_stale": "Injury context cache exceeds configured freshness TTL.",
    "roster_context_stale": "Roster context cache exceeds configured freshness TTL.",
}

STRATEGY_HEALTH_STATUS_KEY = {
    "healthy": "All required source and mapping checks passed.",
    "degraded": "Core checks passed but degraded/risk gates are active.",
    "broken": "One or more required checks failed; operationally blocked.",
}

STRATEGY_HEALTH_GATE_DETAIL_KEY = {
    "injury_source_failed": "Injury source checks failed required policy.",
    "roster_source_failed": "Roster source checks failed required policy.",
    "event_mapping_failed": "Event mapping or props-row contract checks failed.",
    "stale_inputs": "One or more inputs are stale relative to configured thresholds.",
    "unknown_roster_detected": "Roster resolution contains unknown players/teams.",
    "roster_fallback_used": "Roster fallback source was used for coverage.",
    "official_injury_secondary_override": "Secondary injury source override is active.",
    "official_injury_missing": "Official NBA injury source is unavailable or unusable.",
    "odds_snapshot_stale": "Latest quote timestamp exceeds configured staleness threshold.",
    "injuries_context_stale": "Injury context cache exceeds configured freshness TTL.",
    "roster_context_stale": "Roster context cache exceeds configured freshness TTL.",
}

PLAYBOOK_MODE_KEY = {
    "explicit_snapshot": "Used the exact snapshot id passed by the operator.",
    "offline_forced_latest": "Offline mode forced reuse of latest cached snapshot.",
    "live_snapshot": "Inside live window and allowed to fetch paid odds snapshot.",
    "offline_context_gate": "Context preflight failed; reused latest cached snapshot.",
    "offline_paid_block": "Paid odds calls blocked; reused latest cached snapshot.",
    "offline_odds_cap": "Monthly odds cap reached; reused latest cached snapshot.",
    "offline_outside_window": "Outside live window; reused latest cached snapshot.",
    "live_bootstrap": "No cached snapshot available; bootstrapped with live snapshot.",
}


def strategy_report_state_key() -> dict[str, dict[str, str]]:
    return {
        "strategy_status": dict(STRATEGY_STATUS_KEY),
        "strategy_mode": dict(STRATEGY_MODE_KEY),
        "health_gates": dict(STRATEGY_HEALTH_GATE_KEY),
    }


def strategy_health_state_key() -> dict[str, dict[str, str]]:
    return {
        "status": dict(STRATEGY_HEALTH_STATUS_KEY),
        "gates": dict(STRATEGY_HEALTH_GATE_DETAIL_KEY),
    }


def playbook_mode_key() -> dict[str, str]:
    return dict(PLAYBOOK_MODE_KEY)


def strategy_meta(
    *, strategy_id: str, strategy_name: str, strategy_description: str
) -> dict[str, str]:
    return {
        "id": strategy_id,
        "name": strategy_name,
        "description": strategy_description,
    }


def attach_strategy_id_key(
    state_key: dict[str, Any] | None, *, strategy_id: str, strategy_description: str
) -> dict[str, Any]:
    """Ensure state key includes a strategy-id description map."""
    base = dict(state_key) if isinstance(state_key, dict) else {}
    strategy_map = base.get("strategy_id", {})
    if not isinstance(strategy_map, dict):
        strategy_map = {}
    strategy_map = dict(strategy_map)
    strategy_map[strategy_id] = strategy_description
    base["strategy_id"] = strategy_map
    return base
