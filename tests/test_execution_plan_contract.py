from __future__ import annotations

import pytest

from prop_ev.execution_plan_contract import (
    EXECUTION_PLAN_SCHEMA_VERSION,
    assert_execution_plan,
    validate_execution_plan,
)


def _plan() -> dict[str, object]:
    return {
        "schema_version": EXECUTION_PLAN_SCHEMA_VERSION,
        "snapshot_id": "snap-1",
        "strategy_id": "s007",
        "generated_at_utc": "2026-02-14T00:00:00Z",
        "constraints": {"max_picks": 2, "max_per_player": 1, "max_per_game": 2},
        "counts": {
            "candidate_lines": 3,
            "eligible_lines": 3,
            "selected_lines": 2,
            "excluded_lines": 1,
        },
        "selected": [
            {
                "event_id": "e1",
                "player": "Player A",
                "portfolio_rank": 1,
                "portfolio_reason": "",
            },
            {
                "event_id": "e2",
                "player": "Player B",
                "portfolio_rank": 2,
                "portfolio_reason": "",
            },
        ],
        "excluded": [
            {
                "event_id": "e3",
                "player": "Player C",
                "portfolio_rank": 0,
                "portfolio_reason": "portfolio_cap_daily",
            }
        ],
        "exclusion_reason_counts": {"portfolio_cap_daily": 1},
    }


def test_validate_execution_plan_accepts_valid_payload() -> None:
    errors = validate_execution_plan(_plan())
    assert errors == []


def test_validate_execution_plan_rejects_constraint_violations() -> None:
    plan = _plan()
    selected = plan["selected"]
    assert isinstance(selected, list)
    selected.append(
        {
            "event_id": "e1",
            "player": "Player A",
            "portfolio_rank": 2,
            "portfolio_reason": "",
        }
    )
    counts = plan["counts"]
    assert isinstance(counts, dict)
    counts["selected_lines"] = 3

    errors = set(validate_execution_plan(plan))
    assert "selected_rank_duplicate" in errors
    assert "selected_exceeds_max_picks" in errors
    assert "selected_exceeds_player_cap" in errors


def test_assert_execution_plan_raises_on_invalid_payload() -> None:
    plan = _plan()
    excluded = plan["excluded"]
    assert isinstance(excluded, list)
    excluded[0]["portfolio_reason"] = ""

    with pytest.raises(ValueError, match="execution_plan_contract_violation"):
        assert_execution_plan(plan)
