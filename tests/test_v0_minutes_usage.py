from __future__ import annotations

import pytest

from prop_ev.models.v0_minutes_usage import market_side_adjustment_v0, minutes_usage_v0


def test_minutes_usage_v0_standard_case() -> None:
    projection = minutes_usage_v0(
        market="player_points",
        injury_status="questionable",
        roster_status="active",
        teammate_counts={"out": 2, "out_for_season": 0, "doubtful": 1, "questionable": 0},
        spread_abs=9.0,
    )

    assert projection == {
        "baseline_minutes": 31.0,
        "projected_minutes": 29.7,
        "minutes_delta": -1.3,
        "usage_delta": 0.02,
    }


def test_minutes_usage_v0_clamps_usage_and_minutes() -> None:
    projection = minutes_usage_v0(
        market="player_points",
        injury_status="doubtful",
        roster_status="unknown_roster",
        teammate_counts={"out": 10, "out_for_season": 0, "doubtful": 4, "questionable": 0},
        spread_abs=13.0,
    )

    assert projection == {
        "baseline_minutes": 31.0,
        "projected_minutes": 25.5,
        "minutes_delta": -5.5,
        "usage_delta": 0.08,
    }


def test_market_side_adjustment_v0_standard_case() -> None:
    delta = market_side_adjustment_v0(
        market="player_points",
        minutes_projection={"minutes_delta": -1.3, "usage_delta": 0.02},
        opponent_counts={"out": 1, "out_for_season": 0},
    )

    assert delta == pytest.approx(0.0086, abs=1e-6)


def test_market_side_adjustment_v0_caps_bounds() -> None:
    positive = market_side_adjustment_v0(
        market="player_points_rebounds_assists",
        minutes_projection={"minutes_delta": 20.0, "usage_delta": 0.5},
        opponent_counts={"out": 10, "out_for_season": 0},
    )
    negative = market_side_adjustment_v0(
        market="player_turnovers",
        minutes_projection={"minutes_delta": -30.0, "usage_delta": -0.5},
        opponent_counts={"out": 10, "out_for_season": 0},
    )

    assert positive == 0.12
    assert negative == -0.12
