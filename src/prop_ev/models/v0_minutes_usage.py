"""Deterministic v0 minutes/usage projection helpers."""

from __future__ import annotations


def minutes_usage_v0(
    *,
    market: str,
    injury_status: str,
    roster_status: str,
    teammate_counts: dict[str, int],
    spread_abs: float | None,
) -> dict[str, float]:
    """Simple deterministic minutes/usage projection layer (v0)."""
    baseline_minutes_by_market = {
        "player_points": 31.0,
        "player_rebounds": 30.0,
        "player_assists": 31.0,
        "player_threes": 30.0,
        "player_points_rebounds_assists": 32.0,
    }
    baseline = baseline_minutes_by_market.get(market, 30.0)
    projected = baseline

    teammate_out = teammate_counts.get("out", 0) + teammate_counts.get("out_for_season", 0)
    teammate_doubtful = teammate_counts.get("doubtful", 0)
    projected += min(4.0, (teammate_out * 1.1) + (teammate_doubtful * 0.5))

    if injury_status == "doubtful":
        projected -= 6.0
    elif injury_status == "questionable":
        projected -= 3.0
    elif injury_status == "day_to_day":
        projected -= 2.0
    elif injury_status == "probable":
        projected -= 0.5

    if roster_status in {"unknown_roster", "unknown_event"}:
        projected -= 1.5

    if spread_abs is not None and spread_abs >= 8.0:
        projected -= 1.0
    if spread_abs is not None and spread_abs >= 12.0:
        projected -= 1.0

    projected = max(10.0, min(40.0, projected))
    minutes_delta = projected - baseline
    usage_delta = min(0.09, max(-0.08, (teammate_out * 0.012) + (teammate_doubtful * 0.006)))
    if injury_status in {"questionable", "doubtful"}:
        usage_delta -= 0.01

    return {
        "baseline_minutes": round(baseline, 2),
        "projected_minutes": round(projected, 2),
        "minutes_delta": round(minutes_delta, 2),
        "usage_delta": round(usage_delta, 4),
    }


def market_side_adjustment_v0(
    *,
    market: str,
    minutes_projection: dict[str, float],
    opponent_counts: dict[str, int],
) -> float:
    """Convert minutes/usage/opponent context into probability delta for OVER side."""
    minutes_delta = float(minutes_projection.get("minutes_delta", 0.0))
    usage_delta = float(minutes_projection.get("usage_delta", 0.0))
    opponent_out = opponent_counts.get("out", 0) + opponent_counts.get("out_for_season", 0)

    minutes_weight = {
        "player_points": 0.008,
        "player_rebounds": 0.007,
        "player_assists": 0.008,
        "player_threes": 0.006,
        "player_points_rebounds_assists": 0.009,
        "player_turnovers": 0.005,
        "player_blocks": 0.004,
        "player_steals": 0.004,
        "player_blocks_steals": 0.004,
    }.get(market, 0.007)

    usage_weight = {
        "player_points": 0.65,
        "player_rebounds": 0.35,
        "player_assists": 0.45,
        "player_threes": 0.5,
        "player_points_rebounds_assists": 0.75,
        "player_turnovers": 0.4,
        "player_blocks": 0.2,
        "player_steals": 0.2,
        "player_blocks_steals": 0.2,
    }.get(market, 0.4)

    opponent_weight = {
        "player_points": 0.006,
        "player_rebounds": 0.004,
        "player_assists": 0.004,
        "player_threes": 0.003,
        "player_points_rebounds_assists": 0.007,
        "player_turnovers": -0.005,
        "player_blocks": -0.003,
        "player_steals": -0.003,
        "player_blocks_steals": -0.003,
    }.get(market, 0.003)

    delta = (minutes_delta * minutes_weight) + (usage_delta * usage_weight)
    delta += opponent_out * opponent_weight
    return max(-0.12, min(0.12, delta))
