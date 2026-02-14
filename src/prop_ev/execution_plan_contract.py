"""ExecutionPlan artifact validation and contract helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any

from prop_ev.nba_data.normalize import normalize_person_name

EXECUTION_PLAN_SCHEMA_VERSION = 1


def _safe_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return 0
        try:
            return int(raw)
        except ValueError:
            return 0
    return 0


def validate_execution_plan(plan: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required_keys = {
        "schema_version",
        "snapshot_id",
        "generated_at_utc",
        "constraints",
        "counts",
        "selected",
        "excluded",
    }
    missing = sorted(key for key in required_keys if key not in plan)
    if missing:
        errors.append(f"missing_keys:{','.join(missing)}")
        return errors

    schema_version = _safe_int(plan.get("schema_version"))
    if schema_version != EXECUTION_PLAN_SCHEMA_VERSION:
        errors.append(f"invalid_schema_version:{schema_version}")

    constraints = plan.get("constraints")
    counts = plan.get("counts")
    selected = plan.get("selected")
    excluded = plan.get("excluded")

    if not isinstance(constraints, dict):
        errors.append("constraints_not_object")
        return errors
    if not isinstance(counts, dict):
        errors.append("counts_not_object")
        return errors
    if not isinstance(selected, list):
        errors.append("selected_not_list")
        return errors
    if not isinstance(excluded, list):
        errors.append("excluded_not_list")
        return errors

    max_picks = max(0, _safe_int(constraints.get("max_picks")))
    max_per_player = max(0, _safe_int(constraints.get("max_per_player")))
    max_per_game = max(0, _safe_int(constraints.get("max_per_game")))

    if len(selected) > max_picks:
        errors.append("selected_exceeds_max_picks")

    selected_lines = max(0, _safe_int(counts.get("selected_lines")))
    excluded_lines = max(0, _safe_int(counts.get("excluded_lines")))
    if selected_lines != len(selected):
        errors.append("selected_count_mismatch")
    if excluded_lines != len(excluded):
        errors.append("excluded_count_mismatch")

    player_counts: Counter[str] = Counter()
    game_counts: Counter[str] = Counter()
    seen_ranks: set[int] = set()
    for row in selected:
        if not isinstance(row, dict):
            errors.append("selected_row_not_object")
            continue
        rank = _safe_int(row.get("portfolio_rank"))
        if rank <= 0:
            errors.append("selected_rank_missing")
        elif rank in seen_ranks:
            errors.append("selected_rank_duplicate")
        else:
            seen_ranks.add(rank)
        reason = str(row.get("portfolio_reason", "")).strip()
        if reason:
            errors.append("selected_has_portfolio_reason")
        player_key = normalize_person_name(str(row.get("player", "")))
        if player_key:
            player_counts[player_key] += 1
        event_id = str(row.get("event_id", "")).strip()
        if event_id:
            game_counts[event_id] += 1

    for row in excluded:
        if not isinstance(row, dict):
            errors.append("excluded_row_not_object")
            continue
        reason = str(row.get("portfolio_reason", "")).strip()
        if not reason:
            errors.append("excluded_missing_portfolio_reason")

    if max_per_player > 0 and any(count > max_per_player for count in player_counts.values()):
        errors.append("selected_exceeds_player_cap")
    if max_per_game > 0 and any(count > max_per_game for count in game_counts.values()):
        errors.append("selected_exceeds_game_cap")

    return sorted(set(errors))


def assert_execution_plan(plan: dict[str, Any]) -> None:
    errors = validate_execution_plan(plan)
    if errors:
        joined = ", ".join(errors)
        raise ValueError(f"execution_plan_contract_violation: {joined}")
