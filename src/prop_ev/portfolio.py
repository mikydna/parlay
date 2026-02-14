"""Deterministic portfolio selection for ranked strategy picks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from prop_ev.nba_data.normalize import normalize_person_name

PORTFOLIO_REASON_DAILY_CAP = "portfolio_cap_daily"
PORTFOLIO_REASON_PLAYER_CAP = "portfolio_cap_player"
PORTFOLIO_REASON_GAME_CAP = "portfolio_cap_game"

PortfolioRanking = Literal["default", "best_ev", "ev_low_quality_weighted", "calibrated_ev_low"]


@dataclass(frozen=True)
class PortfolioConstraints:
    """Hard constraints for one daily ticket portfolio."""

    max_picks: int
    max_per_player: int = 1
    max_per_game: int = 2


def _safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _selection_sort_key(row: dict[str, Any], ranking: PortfolioRanking) -> tuple[Any, ...]:
    ev_low = _safe_float(row.get("ev_low"))
    ev_low_calibrated = _safe_float(row.get("ev_low_calibrated"))
    prior_delta = _safe_float(row.get("historical_prior_delta")) or 0.0
    calibration_confidence = _safe_float(row.get("calibration_confidence"))
    quality = _safe_float(row.get("quality_score"))
    best_ev = _safe_float(row.get("best_ev"))
    quote_age_minutes = _safe_float(row.get("quote_age_minutes"))
    point = _safe_float(row.get("point")) or 0.0

    base_prior_weight = 0.25
    if ranking == "best_ev":
        ev_primary = (best_ev if best_ev is not None else -1.0) + (prior_delta * base_prior_weight)
    elif ranking == "calibrated_ev_low":
        confidence = max(0.0, min(1.0, calibration_confidence or 0.0))
        base_component = ev_low if ev_low is not None else -1.0
        calibrated_component = (
            ev_low_calibrated if ev_low_calibrated is not None else base_component
        )
        calibration_weight = 0.3 * confidence
        ev_component = ((1.0 - calibration_weight) * base_component) + (
            calibration_weight * calibrated_component
        )
        ev_primary = ev_component + (prior_delta * base_prior_weight)
    elif ranking == "ev_low_quality_weighted":
        quality_factor = quality if quality is not None else 0.0
        ev_component = ev_low if ev_low is not None else -1.0
        ev_primary = (ev_component * (0.5 + (0.5 * quality_factor))) + (
            prior_delta * base_prior_weight
        )
    else:
        ev_primary = (ev_low if ev_low is not None else -1.0) + (prior_delta * base_prior_weight)

    return (
        -ev_primary,
        -(quality if quality is not None else -1.0),
        -(best_ev if best_ev is not None else -1.0),
        quote_age_minutes if quote_age_minutes is not None else 1_000_000.0,
        str(row.get("event_id", "")),
        normalize_person_name(str(row.get("player", ""))),
        str(row.get("market", "")),
        point,
        str(row.get("recommended_side", "")),
    )


def select_portfolio_candidates(
    *,
    eligible_rows: list[dict[str, Any]],
    constraints: PortfolioConstraints,
    ranking: PortfolioRanking = "default",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Select one deterministic portfolio and track excluded eligible rows."""
    max_picks = max(0, int(constraints.max_picks))
    max_per_player = max(0, int(constraints.max_per_player))
    max_per_game = max(0, int(constraints.max_per_game))

    if ranking not in {"default", "best_ev", "ev_low_quality_weighted", "calibrated_ev_low"}:
        raise ValueError(f"invalid portfolio ranking: {ranking}")
    sorted_rows = sorted(eligible_rows, key=lambda row: _selection_sort_key(row, ranking))
    selected: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    player_counts: dict[str, int] = {}
    game_counts: dict[str, int] = {}

    for row in sorted_rows:
        candidate = dict(row)
        event_id = str(candidate.get("event_id", "")).strip()
        player_key = normalize_person_name(str(candidate.get("player", "")))
        reason = ""

        if len(selected) >= max_picks:
            reason = PORTFOLIO_REASON_DAILY_CAP
        elif (
            max_per_player > 0 and player_key and player_counts.get(player_key, 0) >= max_per_player
        ):
            reason = PORTFOLIO_REASON_PLAYER_CAP
        elif max_per_game > 0 and event_id and game_counts.get(event_id, 0) >= max_per_game:
            reason = PORTFOLIO_REASON_GAME_CAP

        if reason:
            candidate["portfolio_selected"] = False
            candidate["portfolio_reason"] = reason
            excluded.append(candidate)
            continue

        candidate["portfolio_selected"] = True
        candidate["portfolio_reason"] = ""
        candidate["portfolio_rank"] = len(selected) + 1
        selected.append(candidate)
        if player_key:
            player_counts[player_key] = player_counts.get(player_key, 0) + 1
        if event_id:
            game_counts[event_id] = game_counts.get(event_id, 0) + 1

    return selected, excluded
