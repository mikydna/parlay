"""Shared strategy report helpers and contracts."""
# ruff: noqa: F401

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import prop_ev.strategy_output_impl as _strategy_output_impl
from prop_ev.brief_builder import TEAM_ABBREVIATIONS
from prop_ev.context_health import official_rows_count
from prop_ev.execution_plan_contract import EXECUTION_PLAN_SCHEMA_VERSION, assert_execution_plan
from prop_ev.nba_data.normalize import canonical_team_name, normalize_person_name
from prop_ev.odds_math import (
    american_to_decimal,
    decimal_to_american,
    ev_from_prob_and_price,
    implied_prob_from_american,
    normalize_prob_pair,
)
from prop_ev.portfolio import PortfolioConstraints, PortfolioRanking, select_portfolio_candidates
from prop_ev.pricing_core import (
    extract_book_fair_pairs,
    resolve_baseline_selection,
    summarize_line_pricing,
)
from prop_ev.pricing_reference import ReferencePoint, estimate_reference_probability
from prop_ev.rolling_priors import calibration_feedback
from prop_ev.state_keys import strategy_report_state_key
from prop_ev.strategy_context_impl import count_team_status as _count_team_status_impl
from prop_ev.strategy_context_impl import injuries_by_team as _injuries_by_team_impl
from prop_ev.strategy_context_impl import injury_index as _injury_index_impl
from prop_ev.strategy_context_impl import injury_source_rows as _injury_source_rows_impl
from prop_ev.strategy_context_impl import merged_injury_rows as _merged_injury_rows_impl
from prop_ev.strategy_context_impl import resolve_player_team as _resolve_player_team_impl
from prop_ev.strategy_context_impl import roster_status as _roster_status_impl
from prop_ev.strategy_minutes_impl import market_minutes_weight as _market_minutes_weight_impl
from prop_ev.strategy_minutes_impl import market_side_adjustment as _market_side_adjustment_impl
from prop_ev.strategy_minutes_impl import (
    minutes_prob_adjustment_over as _minutes_prob_adjustment_over_impl,
)
from prop_ev.strategy_minutes_impl import minutes_prob_lookup as _minutes_prob_lookup_impl
from prop_ev.strategy_minutes_impl import minutes_usage as _minutes_usage_impl
from prop_ev.strategy_minutes_impl import probability_adjustment as _probability_adjustment_impl
from prop_ev.time_utils import parse_iso_z, utc_now_str
from prop_ev.util.parsing import safe_float as _safe_float
from prop_ev.util.parsing import to_price as _to_price

ET_ZONE = ZoneInfo("America/New_York")
PORTFOLIO_MAX_PER_PLAYER = 1
PORTFOLIO_MAX_PER_GAME = 2
HISTORICAL_PRIOR_SCORE_WEIGHT = 250.0
MARKET_LABELS = {
    "player_points": "P",
    "player_rebounds": "R",
    "player_assists": "A",
    "player_threes": "3PM",
    "player_points_rebounds_assists": "PRA",
    "player_points_rebounds": "P+R",
    "player_points_assists": "P+A",
    "player_rebounds_assists": "R+A",
    "player_blocks": "BLK",
    "player_steals": "STL",
    "player_blocks_steals": "Stocks",
    "player_turnovers": "TO",
}
TEAM_NBA_ROSTER_SLUGS = {
    "atlanta hawks": "hawks",
    "boston celtics": "celtics",
    "brooklyn nets": "nets",
    "charlotte hornets": "hornets",
    "chicago bulls": "bulls",
    "cleveland cavaliers": "cavaliers",
    "dallas mavericks": "mavericks",
    "denver nuggets": "nuggets",
    "detroit pistons": "pistons",
    "golden state warriors": "warriors",
    "houston rockets": "rockets",
    "indiana pacers": "pacers",
    "los angeles clippers": "clippers",
    "los angeles lakers": "lakers",
    "memphis grizzlies": "grizzlies",
    "miami heat": "heat",
    "milwaukee bucks": "bucks",
    "minnesota timberwolves": "timberwolves",
    "new orleans pelicans": "pelicans",
    "new york knicks": "knicks",
    "oklahoma city thunder": "thunder",
    "orlando magic": "magic",
    "philadelphia 76ers": "sixers",
    "phoenix suns": "suns",
    "portland trail blazers": "blazers",
    "sacramento kings": "kings",
    "san antonio spurs": "spurs",
    "toronto raptors": "raptors",
    "utah jazz": "jazz",
    "washington wizards": "wizards",
}

__all__ = [
    "Any",
    "Counter",
    "ET_ZONE",
    "EXECUTION_PLAN_SCHEMA_VERSION",
    "HISTORICAL_PRIOR_SCORE_WEIGHT",
    "MARKET_LABELS",
    "PORTFOLIO_MAX_PER_GAME",
    "PORTFOLIO_MAX_PER_PLAYER",
    "PortfolioConstraints",
    "PortfolioRanking",
    "ReferencePoint",
    "TEAM_NBA_ROSTER_SLUGS",
    "UTC",
    "_american_to_decimal",
    "_audit_entries",
    "_availability_notes",
    "_best_side",
    "_bool",
    "_build_sgp_candidates",
    "_clamp",
    "_compose_rationale",
    "_compose_risk_notes",
    "_count_team_status",
    "_decimal_to_american",
    "_et_date_label",
    "_ev_and_kelly",
    "_event_line_index",
    "_execution_plan_row",
    "_execution_plan_sort_key",
    "_fmt_american",
    "_fmt_point",
    "_implied_prob_from_american",
    "_injuries_by_team",
    "_injury_index",
    "_injury_source_rows",
    "_line_key",
    "_market_label",
    "_market_minutes_weight",
    "_market_side_adjustment_core",
    "_mean",
    "_median",
    "_merged_injury_rows",
    "_minutes_prob_adjustment_over",
    "_minutes_prob_lookup",
    "_minutes_usage_core",
    "_nba_roster_link",
    "_normalize_prob_pair",
    "_now_utc",
    "_odds_health",
    "_official_rows_count",
    "_parse_quote_time",
    "_play_to",
    "_pre_bet_readiness",
    "_prior_key",
    "_prior_payload",
    "_probability_adjustment",
    "_prop_label",
    "_quote_age_minutes",
    "_resolve_max_picks",
    "_resolve_player_team",
    "_roster_resolution_detail",
    "_roster_status",
    "_roster_warning",
    "_safe_float",
    "_sgp_haircut",
    "_short_game_label",
    "_spread_display",
    "_team_abbrev",
    "_tip_et",
    "_to_price",
    "_validate_rows_contract",
    "assert_execution_plan",
    "calibration_feedback",
    "canonical_team_name",
    "datetime",
    "estimate_reference_probability",
    "extract_book_fair_pairs",
    "load_jsonl",
    "normalize_person_name",
    "parse_iso_z",
    "resolve_baseline_selection",
    "select_portfolio_candidates",
    "strategy_report_state_key",
    "summarize_line_pricing",
]


def _now_utc() -> str:
    return utc_now_str()


def _et_date_label(event_context: dict[str, dict[str, str]] | None) -> str:
    tips: list[datetime] = []
    if isinstance(event_context, dict):
        for row in event_context.values():
            if not isinstance(row, dict):
                continue
            tip = parse_iso_z(str(row.get("commence_time", "")))
            if tip is not None:
                tips.append(tip)
    anchor = min(tips) if tips else datetime.now(UTC)
    return anchor.astimezone(ET_ZONE).strftime("%A, %b %d, %Y (ET)")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    """Load JSONL rows from disk."""
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _fmt_point(value: Any) -> str:
    num = _safe_float(value)
    if num is None:
        return ""
    if float(num).is_integer():
        return str(int(num))
    return f"{num:.1f}"


def _fmt_american(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return f"+{value}" if value > 0 else str(value)
    parsed = _to_price(value)
    if parsed is None:
        return ""
    return f"+{parsed}" if parsed > 0 else str(parsed)


def _implied_prob_from_american(price: int | None) -> float | None:
    return implied_prob_from_american(price)


def _american_to_decimal(price: int | None) -> float | None:
    return american_to_decimal(price)


def _decimal_to_american(decimal_odds: float | None) -> int | None:
    return decimal_to_american(decimal_odds)


def _normalize_prob_pair(over_prob: float, under_prob: float) -> tuple[float, float]:
    return normalize_prob_pair(over_prob, under_prob)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _prior_key(*, market: str, side: str) -> str:
    return f"{market.strip().lower()}::{side.strip().lower()}"


def _prior_payload(
    rolling_priors: dict[str, Any] | None, *, market: str, side: str
) -> dict[str, Any]:
    if not isinstance(rolling_priors, dict):
        return {}
    adjustments = rolling_priors.get("adjustments", {})
    if not isinstance(adjustments, dict):
        return {}
    payload = adjustments.get(_prior_key(market=market, side=side))
    return payload if isinstance(payload, dict) else {}


def _execution_plan_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": str(row.get("event_id", "")),
        "game": str(row.get("game", "")),
        "tip_et": str(row.get("tip_et", "")),
        "player": str(row.get("player", "")),
        "market": str(row.get("market", "")),
        "side": str(row.get("recommended_side", "")),
        "point": _safe_float(row.get("point")),
        "tier": str(row.get("tier", "")),
        "selected_book": str(row.get("selected_book", "")),
        "selected_price_american": _to_price(row.get("selected_price")),
        "play_to_american": _to_price(row.get("play_to_american")),
        "best_ev": _safe_float(row.get("best_ev")),
        "ev_low": _safe_float(row.get("ev_low")),
        "quality_score": _safe_float(row.get("quality_score")),
        "historical_prior_delta": _safe_float(row.get("historical_prior_delta")) or 0.0,
        "historical_prior_sample_size": int(row.get("historical_prior_sample_size", 0) or 0),
        "portfolio_reason": str(row.get("portfolio_reason", "")),
        "portfolio_rank": int(row.get("portfolio_rank", 0) or 0),
    }


def _execution_plan_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(row.get("portfolio_rank", 0) or 0),
        str(row.get("event_id", "")),
        str(row.get("market", "")),
        str(row.get("player", "")),
        str(row.get("recommended_side", "")),
        _safe_float(row.get("point")) or 0.0,
        str(row.get("selected_book", "")),
        _to_price(row.get("selected_price")) or 0,
    )


def _resolve_max_picks(*, top_n: int, max_picks: int) -> int:
    top_limit = max(0, int(top_n))
    if top_limit <= 0:
        return 0
    configured = int(max_picks)
    if configured <= 0:
        return top_limit
    return min(configured, top_limit)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _line_key(row: dict[str, Any]) -> tuple[str, str, str, float]:
    event_id = str(row.get("event_id", ""))
    market = str(row.get("market", ""))
    player = str(row.get("player", ""))
    point = _safe_float(row.get("point")) or 0.0
    return event_id, market, player, point


def _best_side(
    rows: list[dict[str, Any]], *, exclude_book_keys: frozenset[str] | None = None
) -> dict[str, Any]:
    excluded = exclude_book_keys or frozenset()
    if not rows:
        return {
            "price": None,
            "book": "",
            "link": "",
            "books": 0,
            "shop_delta": 0,
            "last_update": "",
        }
    price_rows: list[tuple[int, dict[str, Any]]] = []
    books: set[str] = set()
    for row in rows:
        book = str(row.get("book", ""))
        if book in excluded:
            continue
        if book:
            books.add(book)
        parsed = _to_price(row.get("price"))
        if parsed is not None:
            price_rows.append((parsed, row))
    if not price_rows:
        return {
            "price": None,
            "book": "",
            "link": "",
            "books": len(books),
            "shop_delta": 0,
            "last_update": "",
        }
    best_price, best_row = max(price_rows, key=lambda item: item[0])
    worst_price = min(price_rows, key=lambda item: item[0])[0]
    return {
        "price": best_price,
        "book": str(best_row.get("book", "")),
        "link": str(best_row.get("link", "")),
        "books": len(books),
        "shop_delta": best_price - worst_price,
        "last_update": str(best_row.get("last_update", "")),
    }


def _injury_source_rows(source: Any, *, default_source: str) -> list[dict[str, Any]]:
    return _injury_source_rows_impl(source, default_source=default_source)


def _merged_injury_rows(injuries: dict[str, Any] | None) -> list[dict[str, Any]]:
    return _merged_injury_rows_impl(injuries)


def _official_rows_count(official: dict[str, Any] | None) -> int:
    return official_rows_count(official)


def _injury_index(injuries: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    return _injury_index_impl(injuries)


def _injuries_by_team(injuries: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    return _injuries_by_team_impl(injuries)


def _roster_status(
    *,
    player_name: str,
    event_id: str,
    event_context: dict[str, dict[str, str]] | None,
    roster: dict[str, Any] | None,
    player_identity_map: dict[str, Any] | None = None,
) -> str:
    return _roster_status_impl(
        player_name=player_name,
        event_id=event_id,
        event_context=event_context,
        roster=roster,
        player_identity_map=player_identity_map,
    )


def _resolve_player_team(
    *,
    player_name: str,
    event_id: str,
    event_context: dict[str, dict[str, str]] | None,
    roster: dict[str, Any] | None,
    injury_row: dict[str, Any],
    player_identity_map: dict[str, Any] | None = None,
) -> str:
    return _resolve_player_team_impl(
        player_name=player_name,
        event_id=event_id,
        event_context=event_context,
        roster=roster,
        injury_row=injury_row,
        player_identity_map=player_identity_map,
    )


def _count_team_status(rows: list[dict[str, Any]], exclude_player_norm: str) -> dict[str, int]:
    return _count_team_status_impl(rows, exclude_player_norm)


def _minutes_usage_core(
    *,
    market: str,
    injury_status: str,
    roster_status: str,
    teammate_counts: dict[str, int],
    spread_abs: float | None,
) -> dict[str, float]:
    return _minutes_usage_impl(
        market=market,
        injury_status=injury_status,
        roster_status=roster_status,
        teammate_counts=teammate_counts,
        spread_abs=spread_abs,
    )


def _market_side_adjustment_core(
    *,
    market: str,
    minutes_projection: dict[str, float],
    opponent_counts: dict[str, int],
) -> float:
    return _market_side_adjustment_impl(
        market=market,
        minutes_projection=minutes_projection,
        opponent_counts=opponent_counts,
    )


def _market_minutes_weight(market: str) -> float:
    return _market_minutes_weight_impl(market)


def _minutes_prob_lookup(
    minutes_probabilities: dict[str, Any] | None,
    *,
    event_id: str,
    player: str,
    market: str,
) -> dict[str, Any]:
    return _minutes_prob_lookup_impl(
        minutes_probabilities,
        event_id=event_id,
        player=player,
        market=market,
    )


def _minutes_prob_adjustment_over(
    *,
    market: str,
    projected_minutes: float | None,
    minutes_p50: float | None,
    p_active: float | None,
    confidence_score: float | None,
) -> float:
    return _minutes_prob_adjustment_over_impl(
        market=market,
        projected_minutes=projected_minutes,
        minutes_p50=minutes_p50,
        p_active=p_active,
        confidence_score=confidence_score,
    )


def _probability_adjustment(
    *,
    injury_status: str,
    roster_status: str,
    teammate_counts: dict[str, int],
    opponent_counts: dict[str, int],
    spread_abs: float | None,
) -> float:
    return _probability_adjustment_impl(
        injury_status=injury_status,
        roster_status=roster_status,
        teammate_counts=teammate_counts,
        opponent_counts=opponent_counts,
        spread_abs=spread_abs,
    )


def _ev_and_kelly(
    probability: float | None, american_price: int | None
) -> tuple[float | None, float | None]:
    ev = ev_from_prob_and_price(probability, american_price)
    if ev is None:
        return None, None
    decimal_odds = _american_to_decimal(american_price)
    if decimal_odds is None:
        return None, None
    profit_if_win = decimal_odds - 1.0
    if profit_if_win <= 0:
        return None, None
    kelly = ev / profit_if_win
    return round(ev, 6), round(kelly, 6)


def _sgp_haircut(legs: list[dict[str, Any]]) -> float:
    """Apply a conservative correlation haircut for same-game combinations."""
    if not legs:
        return 0.0
    players = [str(leg.get("player", "")) for leg in legs]
    markets = [str(leg.get("market", "")) for leg in legs]
    sides = [str(leg.get("recommended_side", "")) for leg in legs]
    teams = [str(leg.get("player_team", "")) for leg in legs]

    if len(set(players)) < len(players):
        return 0.15
    if len(set(teams)) == 1 and all(side == "over" for side in sides):
        return 0.12
    if any(
        market in {"player_points", "player_assists", "player_points_rebounds_assists"}
        for market in markets
    ):
        return 0.08
    return 0.05


def _build_sgp_candidates(eligible_rows: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    """Build simple 2-leg SGP candidates with correlation haircut."""
    by_event: dict[str, list[dict[str, Any]]] = {}
    for row in eligible_rows:
        if not isinstance(row, dict):
            continue
        event_id = str(row.get("event_id", ""))
        if not event_id:
            continue
        by_event.setdefault(event_id, []).append(row)

    candidates: list[dict[str, Any]] = []
    for event_rows in by_event.values():
        sorted_rows = sorted(event_rows, key=lambda row: -(row.get("best_ev") or -999.0))
        for i in range(min(len(sorted_rows), 8)):
            for j in range(i + 1, min(len(sorted_rows), 8)):
                leg_a = sorted_rows[i]
                leg_b = sorted_rows[j]
                p_a = _safe_float(leg_a.get("model_p_hit"))
                p_b = _safe_float(leg_b.get("model_p_hit"))
                d_a = _american_to_decimal(_to_price(leg_a.get("selected_price")))
                d_b = _american_to_decimal(_to_price(leg_b.get("selected_price")))
                if p_a is None or p_b is None or d_a is None or d_b is None:
                    continue
                indep_p = p_a * p_b
                haircut = _sgp_haircut([leg_a, leg_b])
                adj_p = indep_p * (1.0 - haircut)
                decimal_combo = d_a * d_b
                ev = (adj_p * (decimal_combo - 1.0)) - (1.0 - adj_p)
                american_combo = _decimal_to_american(decimal_combo)
                _, quarter_kelly = _ev_and_kelly(adj_p, american_combo)
                candidates.append(
                    {
                        "event_id": str(leg_a.get("event_id", "")),
                        "game": str(leg_a.get("game", "")),
                        "legs": [
                            {
                                "player": str(leg_a.get("player", "")),
                                "market": str(leg_a.get("market", "")),
                                "point": leg_a.get("point"),
                                "side": str(leg_a.get("recommended_side", "")),
                                "price": leg_a.get("selected_price"),
                                "p_hit": p_a,
                            },
                            {
                                "player": str(leg_b.get("player", "")),
                                "market": str(leg_b.get("market", "")),
                                "point": leg_b.get("point"),
                                "side": str(leg_b.get("recommended_side", "")),
                                "price": leg_b.get("selected_price"),
                                "p_hit": p_b,
                            },
                        ],
                        "independence_joint_p": round(indep_p, 6),
                        "haircut": round(haircut, 4),
                        "adjusted_joint_p": round(adj_p, 6),
                        "unboosted_decimal": round(decimal_combo, 6),
                        "unboosted_american": american_combo,
                        "ev_per_100": round(ev * 100.0, 3),
                        "recommended_fractional_kelly": (
                            round((quarter_kelly / 2.0), 6) if quarter_kelly is not None else None
                        ),
                    }
                )

    candidates.sort(key=lambda row: -(row.get("ev_per_100") or -999.0))
    return candidates[: max(0, top_n)]


def _play_to(probability: float | None, target_roi: float) -> tuple[float | None, int | None]:
    if probability is None or probability <= 0:
        return None, None
    minimum_decimal = (1.0 + target_roi) / probability
    return round(minimum_decimal, 6), _decimal_to_american(minimum_decimal)


def _bool(value: bool) -> str:
    return "yes" if value else "no"


def _parse_quote_time(value: str) -> datetime | None:
    return parse_iso_z(value)


def _quote_age_minutes(*, quote_utc: str, now_utc: datetime) -> float | None:
    parsed = _parse_quote_time(quote_utc)
    if parsed is None:
        return None
    age = (now_utc - parsed).total_seconds() / 60.0
    return round(max(0.0, age), 6)


def _odds_health(
    candidates: list[dict[str, Any]], stale_after_min: int, *, now_utc: datetime
) -> dict[str, Any]:
    timestamps: list[datetime] = []
    for row in candidates:
        if not isinstance(row, dict):
            continue
        raw = str(row.get("selected_last_update", "")).strip()
        parsed = _parse_quote_time(raw)
        if parsed is not None:
            timestamps.append(parsed)

    if not timestamps:
        return {
            "status": "missing_last_update",
            "odds_stale": True,
            "stale_after_min": stale_after_min,
            "latest_quote_utc": "",
            "oldest_quote_utc": "",
            "age_latest_min": None,
            "age_oldest_min": None,
        }

    newest = max(timestamps)
    oldest = min(timestamps)
    age_latest = (now_utc - newest).total_seconds() / 60.0
    age_oldest = (now_utc - oldest).total_seconds() / 60.0
    odds_stale = age_latest > max(0, stale_after_min)
    return {
        "status": "ok" if not odds_stale else "stale",
        "odds_stale": odds_stale,
        "stale_after_min": stale_after_min,
        "latest_quote_utc": newest.isoformat().replace("+00:00", "Z"),
        "oldest_quote_utc": oldest.isoformat().replace("+00:00", "Z"),
        "age_latest_min": round(age_latest, 2),
        "age_oldest_min": round(age_oldest, 2),
    }


def _validate_rows_contract(rows: list[dict[str, Any]]) -> dict[str, Any]:
    required = ["event_id", "market", "player", "side", "book", "price", "point"]
    invalid: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            invalid.append({"reason": "row_not_object"})
            continue
        missing = [field for field in required if not str(row.get(field, "")).strip()]
        if missing:
            invalid.append(
                {
                    "reason": "missing_fields",
                    "missing": missing,
                    "event_id": str(row.get("event_id", "")),
                    "player": str(row.get("player", "")),
                    "market": str(row.get("market", "")),
                }
            )
    return {
        "status": "ok" if not invalid else "invalid_rows",
        "row_count": len(rows),
        "invalid_count": len(invalid),
        "invalid_examples": invalid[:10],
    }


def _roster_resolution_detail(status: str) -> str:
    if status == "unknown_event":
        return "event_id_missing_from_event_context"
    if status == "unknown_roster":
        return "event_teams_missing_from_roster_snapshot"
    if status == "not_on_roster":
        return "player_not_found_on_event_team_rosters"
    if status == "inactive":
        return "player_marked_inactive_in_roster_snapshot"
    return "ok"


def _pre_bet_readiness(*, injury_status: str, roster_status: str) -> tuple[bool, str]:
    clean_injury = injury_status in {"available", "available_unlisted"}
    clean_roster = roster_status in {"active", "rostered"}
    if clean_injury and clean_roster:
        return True, "ok"
    if not clean_injury:
        return False, f"injury_status={injury_status}"
    return False, f"roster_status={roster_status}"


def _market_label(market: str) -> str:
    return MARKET_LABELS.get(market, market.replace("_", " ").upper())


def _team_abbrev(team_name: str) -> str:
    raw = team_name.strip()
    if not raw:
        return ""
    canonical = canonical_team_name(raw)
    if canonical in TEAM_ABBREVIATIONS:
        return TEAM_ABBREVIATIONS[canonical]
    token = raw.replace(".", "").strip()
    if 2 <= len(token) <= 4 and token.isalpha():
        return token.upper()
    words = canonical.split()
    if words:
        return words[-1][:3].upper()
    return raw[:3].upper()


def _short_game_label(game: str) -> str:
    raw = game.strip()
    if not raw:
        return ""
    if "@" not in raw:
        return _team_abbrev(raw) or raw
    away_raw, home_raw = raw.split("@", 1)
    away = _team_abbrev(away_raw)
    home = _team_abbrev(home_raw)
    if away and home:
        return f"{away} @ {home}"
    return raw


def _prop_label(player: str, side: str, point: float, market: str) -> str:
    return f"{player} {side.upper()} {point:.1f} {_market_label(market)}"


def _tip_et(value: str) -> str:
    tip = parse_iso_z(value)
    if tip is None:
        return ""
    return tip.astimezone(ET_ZONE).strftime("%I:%M %p ET")


def _spread_display(home_team: str, home_spread: float | None) -> str:
    home = _team_abbrev(home_team) or home_team
    if home_spread is None:
        return ""
    if abs(home_spread) < 0.05:
        return f"{home} PK"
    sign = "+" if home_spread > 0 else ""
    return f"{home} {sign}{home_spread:.1f}"


def _event_line_index(
    slate_rows: list[dict[str, Any]], event_context: dict[str, dict[str, str]] | None
) -> tuple[list[dict[str, str]], dict[str, dict[str, Any]]]:
    rows_by_event: dict[str, dict[str, list[float]]] = {}
    for row in slate_rows:
        if not isinstance(row, dict):
            continue
        event_id = str(row.get("event_id", row.get("game_id", "")))
        if not event_id:
            continue
        market = str(row.get("market", ""))
        point = _safe_float(row.get("point"))
        if point is None:
            continue
        bucket = rows_by_event.setdefault(event_id, {"totals": [], "home_spreads": []})
        if market == "totals":
            bucket["totals"].append(point)
            continue
        if market != "spreads":
            continue

        ctx = event_context.get(event_id, {}) if isinstance(event_context, dict) else {}
        home_team = canonical_team_name(str(ctx.get("home_team", "")))
        away_team = canonical_team_name(str(ctx.get("away_team", "")))
        side = canonical_team_name(str(row.get("side", "")))
        if side and side == home_team:
            bucket["home_spreads"].append(point)
        elif side and side == away_team:
            bucket["home_spreads"].append(-point)

    slate_snapshot: list[dict[str, str]] = []
    event_lines: dict[str, dict[str, Any]] = {}
    event_ids: set[str] = set(rows_by_event)
    if isinstance(event_context, dict):
        event_ids.update(event_context.keys())

    def _sort_key(event_id: str) -> tuple[int, str]:
        commence = ""
        if isinstance(event_context, dict):
            commence = str(event_context.get(event_id, {}).get("commence_time", ""))
        parsed = parse_iso_z(commence)
        if parsed is None:
            return (2, event_id)
        return (1, parsed.isoformat())

    for event_id in sorted(event_ids, key=_sort_key):
        ctx = event_context.get(event_id, {}) if isinstance(event_context, dict) else {}
        home_team = str(ctx.get("home_team", ""))
        away_team = str(ctx.get("away_team", ""))
        line_rows = rows_by_event.get(event_id, {})
        totals = line_rows.get("totals", [])
        home_spreads = line_rows.get("home_spreads", [])
        total = round(median(totals), 1) if totals else None
        home_spread = round(median(home_spreads), 1) if home_spreads else None
        tip_et = _tip_et(str(ctx.get("commence_time", "")))
        game = f"{away_team} @ {home_team}".strip()
        slate_snapshot.append(
            {
                "event_id": event_id,
                "tip_et": tip_et,
                "away_home": game,
                "spread": _spread_display(home_team or "Home", home_spread),
                "total": "" if total is None else f"{total:.1f}",
            }
        )
        event_lines[event_id] = {
            "home_team": home_team,
            "away_team": away_team,
            "tip_et": tip_et,
            "home_spread": home_spread,
            "spread_abs": abs(home_spread) if home_spread is not None else None,
            "total": total,
        }
    return slate_snapshot, event_lines


def _roster_warning(status: str, player: str, event_id: str) -> str | None:
    if status == "unknown_roster":
        return f"{player} ({event_id}) has unknown_roster: active/inactive status not found."
    if status == "unknown_event":
        return f"{player} ({event_id}) has unknown_event: missing event team context."
    if status == "inactive":
        return f"{player} ({event_id}) is marked inactive in roster feed."
    if status == "not_on_roster":
        return f"{player} ({event_id}) is not on either event roster feed."
    return None


def _nba_roster_link(team_name: str, player: str) -> str:
    team_key = canonical_team_name(team_name)
    slug = TEAM_NBA_ROSTER_SLUGS.get(team_key)
    if slug:
        return f"https://www.nba.com/{slug}/roster"
    query = quote_plus(f"{player} {team_name} nba")
    return f"https://www.espn.com/search/_/q/{query}"


def _availability_notes(
    *, injuries: dict[str, Any] | None, roster: dict[str, Any] | None, teams_in_scope: set[str]
) -> dict[str, Any]:
    official = injuries.get("official", {}) if isinstance(injuries, dict) else {}
    secondary = injuries.get("secondary", {}) if isinstance(injuries, dict) else {}
    merged_rows = _merged_injury_rows(injuries)

    key_rows: list[dict[str, str]] = []
    if merged_rows:
        scored: list[tuple[int, str, dict[str, str]]] = []
        severity = {
            "out_for_season": 5,
            "out": 4,
            "doubtful": 3,
            "questionable": 2,
            "day_to_day": 1,
            "probable": 0,
            "available": 0,
            "unknown": 0,
        }
        for row in merged_rows:
            if not isinstance(row, dict):
                continue
            team_norm = canonical_team_name(str(row.get("team_norm", row.get("team", ""))))
            if teams_in_scope and team_norm and team_norm not in teams_in_scope:
                continue
            status = str(row.get("status", "unknown"))
            if status not in {"out_for_season", "out", "doubtful", "questionable"}:
                continue
            entry = {
                "player": str(row.get("player", "")),
                "team": str(row.get("team", "")),
                "status": status,
                "note": str(row.get("note", "")),
                "date_update": str(row.get("date_update", "")),
            }
            scored.append((severity.get(status, 0), entry["date_update"], entry))
        scored.sort(key=lambda item: (-item[0], item[1], item[2]["player"]))
        key_rows = [item[2] for item in scored[:20]]

    return {
        "official": {
            "status": str(official.get("status", "missing"))
            if isinstance(official, dict)
            else "missing",
            "url": str(official.get("url", "")) if isinstance(official, dict) else "",
            "fetched_at_utc": str(official.get("fetched_at_utc", ""))
            if isinstance(official, dict)
            else "",
            "count": int(official.get("count", 0)) if isinstance(official, dict) else 0,
            "rows_count": _official_rows_count(official if isinstance(official, dict) else None),
            "parse_status": (
                str(official.get("parse_status", "")) if isinstance(official, dict) else ""
            ),
            "parse_coverage": (
                (_safe_float(official.get("parse_coverage")) or 0.0)
                if isinstance(official, dict)
                else 0.0
            ),
            "pdf_links": official.get("pdf_links", []) if isinstance(official, dict) else [],
            "pdf_download_status": (
                str(official.get("pdf_download_status", "")) if isinstance(official, dict) else ""
            ),
            "pdf_cached_path": (
                str(official.get("pdf_cached_path", "")) if isinstance(official, dict) else ""
            ),
            "selected_pdf_url": (
                str(official.get("selected_pdf_url", "")) if isinstance(official, dict) else ""
            ),
        },
        "secondary": {
            "status": str(secondary.get("status", "missing"))
            if isinstance(secondary, dict)
            else "missing",
            "url": str(secondary.get("url", "")) if isinstance(secondary, dict) else "",
            "fetched_at_utc": str(secondary.get("fetched_at_utc", ""))
            if isinstance(secondary, dict)
            else "",
            "count": int(secondary.get("count", 0)) if isinstance(secondary, dict) else 0,
        },
        "roster": {
            "status": str(roster.get("status", "missing"))
            if isinstance(roster, dict)
            else "missing",
            "source": str(roster.get("source", "")) if isinstance(roster, dict) else "",
            "url": str(roster.get("url", "")) if isinstance(roster, dict) else "",
            "fetched_at_utc": str(roster.get("fetched_at_utc", ""))
            if isinstance(roster, dict)
            else "",
            "count_teams": int(roster.get("count_teams", 0)) if isinstance(roster, dict) else 0,
        },
        "key_injuries": key_rows,
    }


def _compose_rationale(
    *,
    player: str,
    market: str,
    side: str,
    p_hit: float | None,
    p_fair: float | None,
    injury_status: str,
    teammate_counts: dict[str, int],
    spread_abs: float | None,
    total: float | None,
    projected_minutes: float | None,
    usage_delta: float | None,
) -> str:
    bits: list[str] = []
    if p_hit is not None and p_fair is not None:
        market_text = _market_label(market)
        bits.append(
            f"Model p({side}) {p_hit:.3f} vs no-vig baseline {p_fair:.3f} "
            f"for {player} {market_text}."
        )
    teammate_out = teammate_counts.get("out", 0) + teammate_counts.get("out_for_season", 0)
    if teammate_out > 0:
        bits.append(
            f"Usage proxy lifts due to {teammate_out} unavailable teammate(s) on his team today."
        )
    if projected_minutes is not None and usage_delta is not None:
        bits.append(
            f"Minutes/usage core model projects about {projected_minutes:.1f} minutes "
            f"with usage delta {usage_delta:+.2%}."
        )
    if injury_status in {"questionable", "doubtful", "day_to_day"}:
        bits.append(f"Availability is {injury_status}, so confidence is trimmed.")
    if spread_abs is not None and spread_abs >= 8.0:
        bits.append("Blowout risk adjustment applied because spread is 8+ points.")
    if total is not None:
        bits.append(f"Game environment anchor uses market total around {total:.1f}.")
    if not bits:
        bits.append(
            "Edge comes from price shopping across books versus normalized market baseline."
        )
    return " ".join(bits[:2])


def _compose_risk_notes(
    *, injury_status: str, roster_status: str, spread_abs: float | None, tier: str
) -> str:
    risks: list[str] = []
    if injury_status in {"questionable", "doubtful", "day_to_day", "unknown"}:
        risks.append(f"injury={injury_status}")
    if roster_status not in {"active", "rostered"}:
        risks.append(f"roster={roster_status}")
    if spread_abs is not None and spread_abs >= 8.0:
        risks.append("blowout_risk")
    if tier == "B":
        risks.append("single_book_quote")
    return ", ".join(risks) if risks else "none"


def _audit_entries(
    *,
    manifest: dict[str, Any],
    availability: dict[str, Any],
    top_plays: list[dict[str, Any]],
    one_source_edges: list[dict[str, Any]],
) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    requests = manifest.get("requests", {})
    if isinstance(requests, dict):
        for row in requests.values():
            if not isinstance(row, dict):
                continue
            label = str(row.get("label", ""))
            if not (
                label == "slate_odds" or label == "events_list" or label.startswith("event_odds:")
            ):
                continue
            entries.append(
                {
                    "category": "odds_api",
                    "label": label,
                    "url": str(row.get("path", "")),
                    "timestamp_utc": str(row.get("updated_at_utc", "")),
                    "note": f"status={row.get('status', '')}",
                }
            )

    official = availability.get("official", {})
    if isinstance(official, dict):
        url = str(official.get("url", ""))
        if url:
            entries.append(
                {
                    "category": "injury",
                    "label": "official_nba_injury_report",
                    "url": url,
                    "timestamp_utc": str(official.get("fetched_at_utc", "")),
                    "note": f"status={official.get('status', '')}",
                }
            )

    secondary = availability.get("secondary", {})
    if isinstance(secondary, dict):
        url = str(secondary.get("url", ""))
        if url:
            entries.append(
                {
                    "category": "injury",
                    "label": "secondary_injury_source",
                    "url": url,
                    "timestamp_utc": str(secondary.get("fetched_at_utc", "")),
                    "note": f"status={secondary.get('status', '')}",
                }
            )

    roster = availability.get("roster", {})
    if isinstance(roster, dict):
        url = str(roster.get("url", ""))
        if url:
            entries.append(
                {
                    "category": "roster",
                    "label": "roster_context",
                    "url": url,
                    "timestamp_utc": str(roster.get("fetched_at_utc", "")),
                    "note": f"status={roster.get('status', '')}",
                }
            )

    for item in top_plays + one_source_edges:
        link = str(item.get("selected_link", ""))
        if not link:
            continue
        entries.append(
            {
                "category": "price",
                "label": (
                    f"{item.get('player', '')} {item.get('market', '')} "
                    f"{item.get('recommended_side', '')}"
                ),
                "url": link,
                "timestamp_utc": str(item.get("selected_last_update", "")),
                "note": (
                    f"book={item.get('selected_book', '')} "
                    f"price={_fmt_american(item.get('selected_price'))}"
                ),
            }
        )

    entries.sort(
        key=lambda row: (row.get("category", ""), row.get("label", ""), row.get("url", ""))
    )
    return entries
