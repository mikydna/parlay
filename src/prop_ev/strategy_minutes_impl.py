"""Minutes and probability adjustment helper functions for strategy generation."""

from __future__ import annotations

from typing import Any

from prop_ev.models.core_minutes_usage import market_side_adjustment_core, minutes_usage_core
from prop_ev.nba_data.normalize import normalize_person_name


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def minutes_usage(
    *,
    market: str,
    injury_status: str,
    roster_status: str,
    teammate_counts: dict[str, int],
    spread_abs: float | None,
) -> dict[str, float]:
    return minutes_usage_core(
        market=market,
        injury_status=injury_status,
        roster_status=roster_status,
        teammate_counts=teammate_counts,
        spread_abs=spread_abs,
    )


def market_side_adjustment(
    *,
    market: str,
    minutes_projection: dict[str, float],
    opponent_counts: dict[str, int],
) -> float:
    return market_side_adjustment_core(
        market=market,
        minutes_projection=minutes_projection,
        opponent_counts=opponent_counts,
    )


def market_minutes_weight(market: str) -> float:
    return {
        "player_points": 0.008,
        "player_rebounds": 0.007,
        "player_assists": 0.008,
        "player_threes": 0.006,
        "player_points_rebounds_assists": 0.009,
        "player_points_rebounds": 0.008,
        "player_points_assists": 0.008,
        "player_rebounds_assists": 0.007,
        "player_turnovers": 0.005,
        "player_blocks": 0.004,
        "player_steals": 0.004,
        "player_blocks_steals": 0.004,
    }.get(market, 0.007)


def minutes_prob_lookup(
    minutes_probabilities: dict[str, Any] | None,
    *,
    event_id: str,
    player: str,
    market: str,
) -> dict[str, Any]:
    if not isinstance(minutes_probabilities, dict):
        return {}
    player_norm = normalize_person_name(player)
    if not player_norm:
        return {}
    exact = minutes_probabilities.get("exact", {})
    if isinstance(exact, dict):
        key = f"{event_id}|{player_norm}|{market.strip().lower()}"
        payload = exact.get(key)
        if isinstance(payload, dict):
            return payload
    by_player = minutes_probabilities.get("player", {})
    if isinstance(by_player, dict):
        payload = by_player.get(player_norm)
        if isinstance(payload, dict):
            return payload
    return {}


def minutes_prob_adjustment_over(
    *,
    market: str,
    projected_minutes: float | None,
    minutes_p50: float | None,
    p_active: float | None,
    confidence_score: float | None,
) -> float:
    if projected_minutes is None or minutes_p50 is None:
        return 0.0
    weight = market_minutes_weight(market)
    minutes_delta = (minutes_p50 - projected_minutes) * weight * 0.75
    active_penalty = ((p_active if p_active is not None else 1.0) - 1.0) * 0.2
    confidence = 0.0 if confidence_score is None else clamp(confidence_score, 0.0, 1.0)
    adjusted = (minutes_delta + active_penalty) * max(0.1, confidence)
    return clamp(adjusted, -0.08, 0.08)


def probability_adjustment(
    *,
    injury_status: str,
    roster_status: str,
    teammate_counts: dict[str, int],
    opponent_counts: dict[str, int],
    spread_abs: float | None,
) -> float:
    if roster_status in {"inactive", "not_on_roster"}:
        return -0.49
    if injury_status in {"out_for_season", "out"}:
        return -0.49

    adjustment = 0.0
    if injury_status == "doubtful":
        adjustment -= 0.12
    elif injury_status == "questionable":
        adjustment -= 0.06
    elif injury_status == "day_to_day":
        adjustment -= 0.04
    elif injury_status == "probable":
        adjustment -= 0.02

    teammate_boost = (
        (teammate_counts.get("out", 0) * 0.015)
        + (teammate_counts.get("out_for_season", 0) * 0.015)
        + (teammate_counts.get("doubtful", 0) * 0.01)
        + (teammate_counts.get("questionable", 0) * 0.005)
    )
    opponent_boost = (
        (opponent_counts.get("out", 0) * 0.008)
        + (opponent_counts.get("out_for_season", 0) * 0.008)
        + (opponent_counts.get("doubtful", 0) * 0.005)
    )
    adjustment += min(teammate_boost, 0.05)
    adjustment += min(opponent_boost, 0.03)

    if spread_abs is not None and spread_abs >= 8.0:
        adjustment -= 0.015
    if spread_abs is not None and spread_abs >= 12.0:
        adjustment -= 0.01
    return adjustment
