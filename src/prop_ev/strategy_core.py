"""Deterministic strategy report generation from snapshot-derived odds rows."""

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
from prop_ev.identity_map import name_aliases
from prop_ev.models.core_minutes_usage import market_side_adjustment_core, minutes_usage_core
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


def _now_utc() -> str:
    return utc_now_str()


def _parse_iso_utc(value: str) -> datetime | None:
    return parse_iso_z(value)


def _et_date_label(event_context: dict[str, dict[str, str]] | None) -> str:
    tips: list[datetime] = []
    if isinstance(event_context, dict):
        for row in event_context.values():
            if not isinstance(row, dict):
                continue
            tip = _parse_iso_utc(str(row.get("commence_time", "")))
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
    if not isinstance(source, dict):
        return []
    rows = source.get("rows", [])
    if not isinstance(rows, list):
        return []
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        player = str(row.get("player", "")).strip()
        player_norm = str(row.get("player_norm", "")).strip() or normalize_person_name(player)
        if not player_norm:
            continue
        team_norm = canonical_team_name(str(row.get("team_norm", row.get("team", ""))))
        item = dict(row)
        item["player"] = player
        item["player_norm"] = player_norm
        item["team_norm"] = team_norm
        item["source"] = str(row.get("source", default_source))
        cleaned.append(item)
    return cleaned


def _merged_injury_rows(injuries: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(injuries, dict):
        return []
    official_rows = _injury_source_rows(
        injuries.get("official"),
        default_source="official_nba_pdf",
    )
    secondary_rows = _injury_source_rows(
        injuries.get("secondary"),
        default_source="secondary_injuries",
    )
    merged: dict[str, dict[str, Any]] = {}
    for row in secondary_rows:
        merged[str(row.get("player_norm", ""))] = row
    for row in official_rows:
        key = str(row.get("player_norm", ""))
        previous = merged.get(key)
        if previous is not None:
            item = dict(previous)
            item.update(row)
            # Keep structured team identity from secondary when available.
            # Official rows drive status and note fields.
            item["team"] = str(previous.get("team", row.get("team", "")))
            item["team_norm"] = canonical_team_name(
                str(previous.get("team_norm", previous.get("team", "")))
            )
            merged[key] = item
        else:
            merged[key] = row
    return list(merged.values())


def _official_rows_count(official: dict[str, Any] | None) -> int:
    return official_rows_count(official)


def _injury_index(injuries: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}
    rows = _merged_injury_rows(injuries)
    if not rows:
        return index
    severity = {
        "unknown": 0,
        "available": 1,
        "day_to_day": 1,
        "probable": 2,
        "questionable": 3,
        "doubtful": 4,
        "out": 5,
        "out_for_season": 6,
    }
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("player_norm", "")) or normalize_person_name(str(row.get("player", "")))
        if not key:
            continue
        status = str(row.get("status", "unknown"))
        date_update = str(row.get("date_update", ""))
        team_norm = canonical_team_name(str(row.get("team_norm", row.get("team", ""))))
        current = index.get(key)
        candidate = {
            "status": status,
            "date_update": date_update,
            "source": str(row.get("source", "")),
            "note": str(row.get("note", "")),
            "team_norm": team_norm,
            "team": str(row.get("team", "")),
        }
        if current is None:
            index[key] = candidate
            continue
        if severity.get(status, 0) > severity.get(current.get("status", "unknown"), 0):
            index[key] = candidate
            continue
        if date_update > current.get("date_update", ""):
            index[key] = candidate
    return index


def _injuries_by_team(injuries: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in _merged_injury_rows(injuries):
        team_norm = canonical_team_name(str(row.get("team_norm", row.get("team", ""))))
        if not team_norm:
            continue
        grouped.setdefault(team_norm, []).append(row)
    return grouped


def _roster_status(
    *,
    player_name: str,
    event_id: str,
    event_context: dict[str, dict[str, str]] | None,
    roster: dict[str, Any] | None,
    player_identity_map: dict[str, Any] | None = None,
) -> str:
    if not isinstance(event_context, dict):
        return "unknown_event"
    ctx = event_context.get(event_id)
    if not isinstance(ctx, dict):
        return "unknown_event"
    if not isinstance(roster, dict):
        return "unknown_roster"
    teams = roster.get("teams", {})
    if not isinstance(teams, dict):
        return "unknown_roster"

    home = canonical_team_name(str(ctx.get("home_team", "")))
    away = canonical_team_name(str(ctx.get("away_team", "")))
    if not home or not away:
        return "unknown_event"
    home_row = teams.get(home)
    away_row = teams.get(away)
    if not isinstance(home_row, dict) or not isinstance(away_row, dict):
        return "unknown_roster"

    player_norm = normalize_person_name(player_name)
    aliases = set(name_aliases(player_name)) | {player_norm}
    if isinstance(player_identity_map, dict):
        player_rows = player_identity_map.get("players", {})
        if isinstance(player_rows, dict):
            for alias in list(aliases):
                row = player_rows.get(alias)
                if isinstance(row, dict):
                    alias_rows = row.get("aliases", [])
                    if isinstance(alias_rows, list):
                        aliases.update(item for item in alias_rows if isinstance(item, str))
    home_active = set(home_row.get("active", []))
    away_active = set(away_row.get("active", []))
    home_inactive = set(home_row.get("inactive", []))
    away_inactive = set(away_row.get("inactive", []))
    home_all = set(home_row.get("all", []))
    away_all = set(away_row.get("all", []))

    if aliases & home_inactive or aliases & away_inactive:
        return "inactive"
    if aliases & home_active or aliases & away_active:
        return "active"
    if aliases & home_all or aliases & away_all:
        return "rostered"
    return "not_on_roster"


def _resolve_player_team(
    *,
    player_name: str,
    event_id: str,
    event_context: dict[str, dict[str, str]] | None,
    roster: dict[str, Any] | None,
    injury_row: dict[str, Any],
    player_identity_map: dict[str, Any] | None = None,
) -> str:
    ctx = event_context.get(event_id, {}) if isinstance(event_context, dict) else {}
    home = canonical_team_name(str(ctx.get("home_team", "")))
    away = canonical_team_name(str(ctx.get("away_team", "")))

    if isinstance(roster, dict):
        teams = roster.get("teams", {})
        if isinstance(teams, dict) and home and away:
            home_row = teams.get(home, {})
            away_row = teams.get(away, {})
            if isinstance(home_row, dict) and isinstance(away_row, dict):
                player_norm = normalize_person_name(player_name)
                aliases = set(name_aliases(player_name)) | {player_norm}
                if isinstance(player_identity_map, dict):
                    player_rows = player_identity_map.get("players", {})
                    if isinstance(player_rows, dict):
                        for alias in list(aliases):
                            row = player_rows.get(alias)
                            if isinstance(row, dict):
                                alias_rows = row.get("aliases", [])
                                if isinstance(alias_rows, list):
                                    aliases.update(
                                        item for item in alias_rows if isinstance(item, str)
                                    )
                home_all = set(home_row.get("all", []))
                away_all = set(away_row.get("all", []))
                if aliases & home_all and not (aliases & away_all):
                    return home
                if aliases & away_all and not (aliases & home_all):
                    return away

    injury_team = canonical_team_name(str(injury_row.get("team_norm", "")))
    if injury_team and injury_team in {home, away}:
        return injury_team

    if isinstance(player_identity_map, dict):
        players = player_identity_map.get("players", {})
        if isinstance(players, dict):
            for alias in name_aliases(player_name):
                row = players.get(alias)
                if not isinstance(row, dict):
                    continue
                teams = row.get("teams", [])
                if not isinstance(teams, list):
                    continue
                for team in teams:
                    team_norm = canonical_team_name(str(team))
                    if team_norm in {home, away}:
                        return team_norm
    return ""


def _count_team_status(rows: list[dict[str, Any]], exclude_player_norm: str) -> dict[str, int]:
    counts = {
        "out": 0,
        "out_for_season": 0,
        "doubtful": 0,
        "questionable": 0,
    }
    for row in rows:
        if not isinstance(row, dict):
            continue
        if normalize_person_name(str(row.get("player", ""))) == exclude_player_norm:
            continue
        status = str(row.get("status", "unknown"))
        if status in counts:
            counts[status] += 1
    return counts


def _minutes_usage_core(
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


def _market_side_adjustment_core(
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


def _market_minutes_weight(market: str) -> float:
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


def _minutes_prob_lookup(
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


def _minutes_prob_adjustment_over(
    *,
    market: str,
    projected_minutes: float | None,
    minutes_p50: float | None,
    p_active: float | None,
    confidence_score: float | None,
) -> float:
    if projected_minutes is None or minutes_p50 is None:
        return 0.0
    market_weight = _market_minutes_weight(market)
    minutes_delta = (minutes_p50 - projected_minutes) * market_weight * 0.75
    active_penalty = ((p_active if p_active is not None else 1.0) - 1.0) * 0.2
    confidence = 0.0 if confidence_score is None else _clamp(confidence_score, 0.0, 1.0)
    adjusted = (minutes_delta + active_penalty) * max(0.1, confidence)
    return _clamp(adjusted, -0.08, 0.08)


def _probability_adjustment(
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
    return _parse_iso_utc(value)


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
    tip = _parse_iso_utc(value)
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
        parsed = _parse_iso_utc(commence)
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


def build_strategy_report(
    *,
    snapshot_id: str,
    manifest: dict[str, Any],
    rows: list[dict[str, Any]],
    top_n: int,
    max_picks: int = 0,
    injuries: dict[str, Any] | None = None,
    roster: dict[str, Any] | None = None,
    event_context: dict[str, dict[str, str]] | None = None,
    slate_rows: list[dict[str, Any]] | None = None,
    player_identity_map: dict[str, Any] | None = None,
    rolling_priors: dict[str, Any] | None = None,
    minutes_probabilities: dict[str, Any] | None = None,
    min_ev: float = 0.01,
    allow_tier_b: bool = False,
    require_official_injuries: bool = True,
    stale_quote_minutes: int = 20,
    require_fresh_context: bool = True,
    portfolio_ranking: PortfolioRanking = "default",
    market_baseline_method: str = "best_sides",
    market_baseline_fallback: str = "best_sides",
    exclude_selected_book_from_baseline: bool = False,
    tier_b_min_other_books_for_baseline: int | None = None,
    min_book_pairs: int = 0,
    hold_cap: float | None = None,
    p_over_iqr_cap: float | None = None,
    min_quality_score: float | None = None,
    min_ev_low: float | None = None,
    max_uncertainty_band: float | None = None,
    probabilistic_profile: str = "off",
    min_prob_confidence: float | None = None,
    max_minutes_band: float | None = None,
    quote_now_utc: str | datetime | None = None,
) -> dict[str, Any]:
    """Create an audit-ready, deterministic NBA prop strategy report."""
    baseline_method = market_baseline_method.strip().lower()
    if baseline_method not in {"best_sides", "median_book"}:
        raise ValueError(f"invalid market_baseline_method: {market_baseline_method}")
    baseline_fallback = market_baseline_fallback.strip().lower()
    if baseline_fallback not in {"best_sides", "none"}:
        raise ValueError(f"invalid market_baseline_fallback: {market_baseline_fallback}")
    if (
        tier_b_min_other_books_for_baseline is not None
        and int(tier_b_min_other_books_for_baseline) <= 0
    ):
        raise ValueError("tier_b_min_other_books_for_baseline must be > 0")
    tier_b_min_other_books_for_baseline = (
        int(tier_b_min_other_books_for_baseline)
        if tier_b_min_other_books_for_baseline is not None
        else None
    )
    min_book_pairs = max(0, int(min_book_pairs))
    if hold_cap is not None and hold_cap < 0:
        raise ValueError("hold_cap must be >= 0")
    if p_over_iqr_cap is not None and p_over_iqr_cap < 0:
        raise ValueError("p_over_iqr_cap must be >= 0")
    if min_quality_score is not None and not (0.0 <= min_quality_score <= 1.0):
        raise ValueError("min_quality_score must be in [0, 1]")
    if max_uncertainty_band is not None and max_uncertainty_band < 0:
        raise ValueError("max_uncertainty_band must be >= 0")
    if min_prob_confidence is not None and not (0.0 <= min_prob_confidence <= 1.0):
        raise ValueError("min_prob_confidence must be in [0, 1]")
    if max_minutes_band is not None and max_minutes_band < 0:
        raise ValueError("max_minutes_band must be >= 0")
    if int(max_picks) < 0:
        raise ValueError("max_picks must be >= 0")
    probabilistic_profile = probabilistic_profile.strip().lower() or "off"

    strategy_now_utc = datetime.now(UTC)
    if isinstance(quote_now_utc, datetime):
        if quote_now_utc.tzinfo is None:
            strategy_now_utc = quote_now_utc.replace(tzinfo=UTC)
        else:
            strategy_now_utc = quote_now_utc.astimezone(UTC)
    elif isinstance(quote_now_utc, str):
        parsed_quote_now_utc = _parse_iso_utc(quote_now_utc)
        if isinstance(parsed_quote_now_utc, datetime):
            strategy_now_utc = parsed_quote_now_utc
    resolved_max_picks = _resolve_max_picks(top_n=top_n, max_picks=max_picks)

    grouped: dict[tuple[str, str, str, float], list[dict[str, Any]]] = {}
    for row in rows:
        key = _line_key(row)
        grouped.setdefault(key, []).append(row)

    line_groups_by_identity: dict[
        tuple[str, str, str], list[tuple[float, list[dict[str, Any]]]]
    ] = {}
    for key, group_rows in grouped.items():
        event_id, market, player, point = key
        identity = (event_id, market, player)
        line_groups_by_identity.setdefault(identity, []).append((float(point), group_rows))
    for points in line_groups_by_identity.values():
        points.sort(key=lambda item: item[0])

    reference_points_cache: dict[tuple[str, str, str, tuple[str, ...]], list[ReferencePoint]] = {}
    reference_books_cache: dict[tuple[str, str, str, tuple[str, ...]], tuple[str, ...]] = {}

    def _reference_points_for_identity(
        *,
        identity: tuple[str, str, str],
        exclude_book_keys: frozenset[str],
    ) -> list[ReferencePoint]:
        cache_key = (identity[0], identity[1], identity[2], tuple(sorted(exclude_book_keys)))
        cached = reference_points_cache.get(cache_key)
        if cached is not None:
            return cached
        out: list[ReferencePoint] = []
        for point, point_rows in line_groups_by_identity.get(identity, []):
            point_book_pairs = extract_book_fair_pairs(
                point_rows, exclude_book_keys=exclude_book_keys
            )
            p_over_values = [
                pair.p_over_fair for pair in point_book_pairs if isinstance(pair.p_over_fair, float)
            ]
            if not p_over_values:
                continue
            p_over_median = _median(p_over_values)
            if p_over_median is None:
                continue
            hold_values = [pair.hold for pair in point_book_pairs if isinstance(pair.hold, float)]
            hold_median = _median(hold_values)
            out.append(
                ReferencePoint(
                    point=float(point),
                    p_over=float(p_over_median),
                    hold=hold_median,
                    weight=float(max(len(point_book_pairs), 1)),
                )
            )
        reference_points_cache[cache_key] = out
        return out

    def _reference_books_for_identity(
        *,
        identity: tuple[str, str, str],
        exclude_book_keys: frozenset[str],
    ) -> tuple[str, ...]:
        cache_key = (identity[0], identity[1], identity[2], tuple(sorted(exclude_book_keys)))
        cached = reference_books_cache.get(cache_key)
        if cached is not None:
            return cached
        books: set[str] = set()
        for _, point_rows in line_groups_by_identity.get(identity, []):
            for pair in extract_book_fair_pairs(point_rows, exclude_book_keys=exclude_book_keys):
                books.add(pair.book)
        resolved = tuple(sorted(books))
        reference_books_cache[cache_key] = resolved
        return resolved

    slate_rows = slate_rows or []
    slate_snapshot, event_lines = _event_line_index(slate_rows, event_context)
    teams_in_scope = {
        canonical_team_name(str(line.get("home_team", "")))
        for line in event_lines.values()
        if isinstance(line, dict)
    } | {
        canonical_team_name(str(line.get("away_team", "")))
        for line in event_lines.values()
        if isinstance(line, dict)
    }

    injuries_by_player = _injury_index(injuries)
    injuries_by_team = _injuries_by_team(injuries)
    official = injuries.get("official", {}) if isinstance(injuries, dict) else {}
    secondary = injuries.get("secondary", {}) if isinstance(injuries, dict) else {}
    official_rows_count = _official_rows_count(official if isinstance(official, dict) else None)
    official_parse_status = (
        str(official.get("parse_status", "")) if isinstance(official, dict) else ""
    )
    official_ready = (
        isinstance(official, dict)
        and official.get("status") == "ok"
        and official_rows_count > 0
        and official_parse_status in {"", "ok"}
    )
    official_player_norms: set[str] = set()
    if isinstance(official, dict):
        official_rows = official.get("rows", [])
        if isinstance(official_rows, list):
            for row in official_rows:
                if not isinstance(row, dict):
                    continue
                player = str(row.get("player", ""))
                player_norm = str(row.get("player_norm", "")) or normalize_person_name(player)
                if player_norm:
                    official_player_norms.add(player_norm)

    candidates: list[dict[str, Any]] = []
    tier_a_count = 0
    tier_b_count = 0
    eligible_count = 0
    probabilistic_rows_used = 0

    tier_a_min_ev = max(min_ev, 0.03)
    tier_b_min_ev = max(min_ev, 0.05)

    for key, group_rows in grouped.items():
        event_id, market, player, point = key
        books = sorted({str(item.get("book", "")) for item in group_rows if item.get("book", "")})
        book_count = len(books)
        tier = "A" if book_count >= 2 else "B"
        if tier == "A":
            tier_a_count += 1
        else:
            tier_b_count += 1

        over_rows = [
            item
            for item in group_rows
            if str(item.get("side", "")).strip().lower() in {"over", "o"}
        ]
        under_rows = [
            item
            for item in group_rows
            if str(item.get("side", "")).strip().lower() in {"under", "u"}
        ]
        over = _best_side(over_rows)
        under = _best_side(under_rows)

        over_prob_imp_best = _implied_prob_from_american(_to_price(over["price"]))
        under_prob_imp_best = _implied_prob_from_american(_to_price(under["price"]))
        p_over_fair_best: float | None = None
        p_under_fair_best: float | None = None
        hold_best: float | None = None
        if over_prob_imp_best is not None and under_prob_imp_best is not None:
            p_over_fair_best, p_under_fair_best = _normalize_prob_pair(
                over_prob_imp_best, under_prob_imp_best
            )
            hold_best = (over_prob_imp_best + under_prob_imp_best) - 1.0

        pricing_quality = summarize_line_pricing(
            group_rows=group_rows,
            now_utc=strategy_now_utc,
            stale_quote_minutes=stale_quote_minutes,
            hold_fallback=hold_best,
        )
        book_pair_count = pricing_quality.book_pair_count
        p_over_book_median = pricing_quality.p_over_median
        hold_book_median = pricing_quality.hold_median
        p_over_book_iqr = pricing_quality.p_over_iqr
        p_over_book_range = pricing_quality.p_over_range
        line_identity = (event_id, market, player)
        reference_estimate = estimate_reference_probability(
            _reference_points_for_identity(
                identity=line_identity,
                exclude_book_keys=frozenset(),
            ),
            target_point=float(point),
        )
        reference_points_count = reference_estimate.points_used
        reference_line_method = reference_estimate.method
        freshest_quote_utc = pricing_quality.freshest_quote_utc
        quote_age_minutes = pricing_quality.quote_age_minutes
        depth_score = pricing_quality.depth_score
        hold_score = pricing_quality.hold_score
        dispersion_score = pricing_quality.dispersion_score
        freshness_score = pricing_quality.freshness_score
        quality_score = pricing_quality.quality_score
        uncertainty_band = pricing_quality.uncertainty_band

        p_over_fair: float | None = None
        p_under_fair: float | None = None
        hold: float | None = None
        baseline_used = "best_sides"
        baseline_selection = resolve_baseline_selection(
            baseline_method=baseline_method,
            baseline_fallback=baseline_fallback,
            p_over_fair_best=p_over_fair_best,
            p_under_fair_best=p_under_fair_best,
            hold_best=hold_best,
            p_over_book_median=p_over_book_median,
            hold_book_median=hold_book_median,
            reference_estimate=reference_estimate,
        )
        p_over_fair = baseline_selection.p_over_fair
        p_under_fair = baseline_selection.p_under_fair
        hold = baseline_selection.hold
        baseline_used = baseline_selection.baseline_used
        reference_line_method = baseline_selection.reference_line_method
        line_source = baseline_selection.line_source
        baseline_excluded_books: list[str] = []
        books_used_exact = list(pricing_quality.books_used)
        baseline_books_used_set: set[str] = set()
        if baseline_used in {"best_sides", "best_sides_fallback"}:
            over_book = str(over.get("book", ""))
            under_book = str(under.get("book", ""))
            if over_book:
                baseline_books_used_set.add(over_book)
            if under_book:
                baseline_books_used_set.add(under_book)
        elif baseline_used == "median_book_interpolated":
            baseline_books_used_set.update(
                _reference_books_for_identity(
                    identity=line_identity,
                    exclude_book_keys=frozenset(),
                )
            )
        else:
            baseline_books_used_set.update(pricing_quality.books_used)
        baseline_books_used = sorted(baseline_books_used_set)
        baseline_books_used_count = len(baseline_books_used)
        baseline_method_effective = baseline_used
        baseline_is_independent_of_selected_book = False
        baseline_insufficient_after_exclusion = False

        player_norm = normalize_person_name(player)
        injury_row = injuries_by_player.get(player_norm, {})
        injury_status = str(injury_row.get("status", "unknown"))
        injury_note = str(injury_row.get("note", ""))
        roster_status = _roster_status(
            player_name=player,
            event_id=event_id,
            event_context=event_context,
            roster=roster,
            player_identity_map=player_identity_map,
        )
        if (
            official_ready
            and player_norm not in official_player_norms
            and roster_status in {"active", "rostered"}
        ):
            injury_status = "available_unlisted"
            if not injury_note:
                injury_note = "Not listed on official NBA injury report."
        player_team = _resolve_player_team(
            player_name=player,
            event_id=event_id,
            event_context=event_context,
            roster=roster,
            injury_row=injury_row,
            player_identity_map=player_identity_map,
        )
        line_meta = event_lines.get(event_id, {})
        home_team = str(line_meta.get("home_team", ""))
        away_team = str(line_meta.get("away_team", ""))
        if player_team:
            home_norm = canonical_team_name(home_team)
            away_norm = canonical_team_name(away_team)
            if player_team == home_norm:
                opponent_team = away_norm
            elif player_team == away_norm:
                opponent_team = home_norm
            else:
                opponent_team = ""
        else:
            opponent_team = ""

        teammate_counts = _count_team_status(
            injuries_by_team.get(player_team, []), normalize_person_name(player)
        )
        opponent_counts = _count_team_status(injuries_by_team.get(opponent_team, []), "")
        spread_abs = _safe_float(line_meta.get("spread_abs"))
        total = _safe_float(line_meta.get("total"))

        eligible = True
        reason = ""
        if tier == "B" and not allow_tier_b:
            eligible = False
            reason = "tier_b_blocked"
        if roster_status in {"inactive", "not_on_roster", "unknown_roster", "unknown_event"}:
            eligible = False
            reason = "roster_gate"
        if injury_status in {"out", "out_for_season"}:
            eligible = False
            reason = "injury_gate"
        pre_bet_ready, pre_bet_reason = _pre_bet_readiness(
            injury_status=injury_status,
            roster_status=roster_status,
        )

        adjustment = _probability_adjustment(
            injury_status=injury_status,
            roster_status=roster_status,
            teammate_counts=teammate_counts,
            opponent_counts=opponent_counts,
            spread_abs=spread_abs,
        )
        minutes_projection = _minutes_usage_core(
            market=market,
            injury_status=injury_status,
            roster_status=roster_status,
            teammate_counts=teammate_counts,
            spread_abs=spread_abs,
        )
        market_delta = _market_side_adjustment_core(
            market=market,
            minutes_projection=minutes_projection,
            opponent_counts=opponent_counts,
        )
        p_over_model: float | None = None
        p_under_model: float | None = None
        p_over_low: float | None = None
        p_over_high: float | None = None
        p_under_low: float | None = None
        p_under_high: float | None = None
        minutes_p10: float | None = None
        minutes_p50: float | None = None
        minutes_p90: float | None = None
        p_active: float | None = None
        confidence_score: float | None = None
        prob_source = "off"
        minutes_band: float | None = None
        minutes_prob_delta_over = 0.0
        data_quality_flags = ""
        if p_over_fair is not None and p_under_fair is not None:
            p_over_model = _clamp(p_over_fair + adjustment + market_delta, 0.01, 0.99)
            p_under_model = 1.0 - p_over_model
            if probabilistic_profile == "minutes_v1":
                minutes_prob_row = _minutes_prob_lookup(
                    minutes_probabilities,
                    event_id=event_id,
                    player=player,
                    market=market,
                )
                minutes_p10 = _safe_float(minutes_prob_row.get("minutes_p10"))
                minutes_p50 = _safe_float(minutes_prob_row.get("minutes_p50"))
                minutes_p90 = _safe_float(minutes_prob_row.get("minutes_p90"))
                p_active = _safe_float(minutes_prob_row.get("p_active"))
                confidence_score = _safe_float(minutes_prob_row.get("confidence_score"))
                data_quality_flags = str(minutes_prob_row.get("data_quality_flags", ""))
                if (
                    minutes_p10 is not None
                    and minutes_p90 is not None
                    and minutes_p90 >= minutes_p10
                ):
                    minutes_band = round(minutes_p90 - minutes_p10, 6)
                projected_minutes = _safe_float(minutes_projection.get("projected_minutes"))
                minutes_prob_delta_over = _minutes_prob_adjustment_over(
                    market=market,
                    projected_minutes=projected_minutes,
                    minutes_p50=minutes_p50,
                    p_active=p_active,
                    confidence_score=confidence_score,
                )
                if minutes_prob_delta_over != 0.0:
                    p_over_model = _clamp(p_over_model + minutes_prob_delta_over, 0.01, 0.99)
                    p_under_model = 1.0 - p_over_model
                if minutes_p50 is not None:
                    probabilistic_rows_used += 1
                    prob_source = "minutes_v1_model"
                else:
                    prob_source = "minutes_v1_missing"
            p_over_low = round(_clamp(p_over_model - uncertainty_band, 0.01, 0.99), 6)
            p_over_high = round(_clamp(p_over_model + uncertainty_band, 0.01, 0.99), 6)
            p_under_low = round(_clamp(1.0 - p_over_high, 0.01, 0.99), 6)
            p_under_high = round(_clamp(1.0 - p_over_low, 0.01, 0.99), 6)
        elif probabilistic_profile == "minutes_v1":
            prob_source = "minutes_v1_baseline_missing"

        ev_over, kelly_over = _ev_and_kelly(p_over_model, _to_price(over["price"]))
        ev_under, kelly_under = _ev_and_kelly(p_under_model, _to_price(under["price"]))
        ev_over_low, _ = _ev_and_kelly(p_over_low, _to_price(over["price"]))
        ev_over_high, _ = _ev_and_kelly(p_over_high, _to_price(over["price"]))
        ev_under_low, _ = _ev_and_kelly(p_under_low, _to_price(under["price"]))
        ev_under_high, _ = _ev_and_kelly(p_under_high, _to_price(under["price"]))
        side_scenarios: dict[str, dict[str, Any]] = {}
        if exclude_selected_book_from_baseline:

            def _candidate_side_scenario(
                *,
                candidate_side: str,
                candidate_quote: dict[str, Any],
                over_rows: list[dict[str, Any]] = over_rows,
                under_rows: list[dict[str, Any]] = under_rows,
                group_rows: list[dict[str, Any]] = group_rows,
                line_identity: tuple[str, str, str] = line_identity,
                point: float = point,
                adjustment: float = adjustment,
                market_delta: float = market_delta,
                strategy_now_utc: datetime = strategy_now_utc,
                stale_quote_minutes: int = stale_quote_minutes,
                minutes_prob_delta_over: float = minutes_prob_delta_over,
            ) -> dict[str, Any]:
                selected_book_local = str(candidate_quote.get("book", ""))
                excluded_books_local = (
                    frozenset({selected_book_local}) if selected_book_local else frozenset()
                )

                baseline_over = _best_side(over_rows, exclude_book_keys=excluded_books_local)
                baseline_under = _best_side(under_rows, exclude_book_keys=excluded_books_local)
                over_prob_imp_local = _implied_prob_from_american(_to_price(baseline_over["price"]))
                under_prob_imp_local = _implied_prob_from_american(
                    _to_price(baseline_under["price"])
                )
                p_over_fair_best_local: float | None = None
                p_under_fair_best_local: float | None = None
                hold_best_local: float | None = None
                if over_prob_imp_local is not None and under_prob_imp_local is not None:
                    p_over_fair_best_local, p_under_fair_best_local = _normalize_prob_pair(
                        over_prob_imp_local, under_prob_imp_local
                    )
                    hold_best_local = (over_prob_imp_local + under_prob_imp_local) - 1.0

                pricing_quality_local = summarize_line_pricing(
                    group_rows=group_rows,
                    now_utc=strategy_now_utc,
                    stale_quote_minutes=stale_quote_minutes,
                    hold_fallback=hold_best_local,
                    exclude_book_keys=excluded_books_local,
                )
                reference_estimate_local = estimate_reference_probability(
                    _reference_points_for_identity(
                        identity=line_identity,
                        exclude_book_keys=excluded_books_local,
                    ),
                    target_point=float(point),
                )
                baseline_selection_local = resolve_baseline_selection(
                    baseline_method=baseline_method,
                    baseline_fallback=baseline_fallback,
                    p_over_fair_best=p_over_fair_best_local,
                    p_under_fair_best=p_under_fair_best_local,
                    hold_best=hold_best_local,
                    p_over_book_median=pricing_quality_local.p_over_median,
                    hold_book_median=pricing_quality_local.hold_median,
                    reference_estimate=reference_estimate_local,
                )
                baseline_used_local = baseline_selection_local.baseline_used
                if (
                    baseline_selection_local.p_over_fair is None
                    or baseline_selection_local.p_under_fair is None
                ):
                    baseline_used_local = "missing"
                p_over_model_local: float | None = None
                p_under_model_local: float | None = None
                p_over_low_local: float | None = None
                p_over_high_local: float | None = None
                p_under_low_local: float | None = None
                p_under_high_local: float | None = None
                if (
                    baseline_selection_local.p_over_fair is not None
                    and baseline_selection_local.p_under_fair is not None
                ):
                    p_over_model_local = _clamp(
                        baseline_selection_local.p_over_fair + adjustment + market_delta,
                        0.01,
                        0.99,
                    )
                    p_under_model_local = 1.0 - p_over_model_local
                    if minutes_prob_delta_over != 0.0:
                        p_over_model_local = _clamp(
                            p_over_model_local + minutes_prob_delta_over, 0.01, 0.99
                        )
                        p_under_model_local = 1.0 - p_over_model_local
                    p_over_low_local = round(
                        _clamp(
                            p_over_model_local - pricing_quality_local.uncertainty_band, 0.01, 0.99
                        ),
                        6,
                    )
                    p_over_high_local = round(
                        _clamp(
                            p_over_model_local + pricing_quality_local.uncertainty_band, 0.01, 0.99
                        ),
                        6,
                    )
                    p_under_low_local = round(_clamp(1.0 - p_over_high_local, 0.01, 0.99), 6)
                    p_under_high_local = round(_clamp(1.0 - p_over_low_local, 0.01, 0.99), 6)

                if candidate_side == "over":
                    model_p_hit_local = p_over_model_local
                    fair_p_hit_local = baseline_selection_local.p_over_fair
                    p_hit_low_local = p_over_low_local
                    p_hit_high_local = p_over_high_local
                else:
                    model_p_hit_local = p_under_model_local
                    fair_p_hit_local = baseline_selection_local.p_under_fair
                    p_hit_low_local = p_under_low_local
                    p_hit_high_local = p_under_high_local
                selected_price_local = _to_price(candidate_quote.get("price"))
                best_ev_local, best_kelly_local = _ev_and_kelly(
                    model_p_hit_local, selected_price_local
                )
                ev_low_local, _ = _ev_and_kelly(p_hit_low_local, selected_price_local)
                ev_high_local, _ = _ev_and_kelly(p_hit_high_local, selected_price_local)
                baseline_books_used_set: set[str] = set()
                if baseline_used_local in {"best_sides", "best_sides_fallback"}:
                    over_book_local = str(baseline_over.get("book", ""))
                    under_book_local = str(baseline_under.get("book", ""))
                    if over_book_local:
                        baseline_books_used_set.add(over_book_local)
                    if under_book_local:
                        baseline_books_used_set.add(under_book_local)
                elif baseline_used_local == "median_book_interpolated":
                    baseline_books_used_set.update(
                        _reference_books_for_identity(
                            identity=line_identity,
                            exclude_book_keys=excluded_books_local,
                        )
                    )
                else:
                    baseline_books_used_set.update(pricing_quality_local.books_used)
                baseline_books_used_local = sorted(baseline_books_used_set)

                return {
                    "selected_price": selected_price_local,
                    "selected_book": selected_book_local,
                    "selected_link": str(candidate_quote.get("link", "")),
                    "selected_last_update": str(candidate_quote.get("last_update", "")),
                    "best_ev": best_ev_local,
                    "best_kelly": best_kelly_local,
                    "ev_low": ev_low_local,
                    "ev_high": ev_high_local,
                    "model_p_hit": model_p_hit_local,
                    "fair_p_hit": fair_p_hit_local,
                    "p_hit_low": p_hit_low_local,
                    "p_hit_high": p_hit_high_local,
                    "p_over_model": p_over_model_local,
                    "p_under_model": p_under_model_local,
                    "p_over_low": p_over_low_local,
                    "p_over_high": p_over_high_local,
                    "p_under_low": p_under_low_local,
                    "p_under_high": p_under_high_local,
                    "p_over_fair": baseline_selection_local.p_over_fair,
                    "p_under_fair": baseline_selection_local.p_under_fair,
                    "hold": baseline_selection_local.hold,
                    "hold_best_sides": hold_best_local,
                    "baseline_used": baseline_used_local,
                    "line_source": (
                        "missing"
                        if baseline_used_local == "missing"
                        else baseline_selection_local.line_source
                    ),
                    "reference_line_method": baseline_selection_local.reference_line_method,
                    "reference_points_count": reference_estimate_local.points_used,
                    "books_used_exact": list(pricing_quality_local.books_used),
                    "book_pair_count": pricing_quality_local.book_pair_count,
                    "p_over_book_median": pricing_quality_local.p_over_median,
                    "hold_book_median": pricing_quality_local.hold_median,
                    "p_over_book_iqr": pricing_quality_local.p_over_iqr,
                    "p_over_book_range": pricing_quality_local.p_over_range,
                    "freshest_quote_utc": pricing_quality_local.freshest_quote_utc,
                    "quote_age_minutes": pricing_quality_local.quote_age_minutes,
                    "depth_score": pricing_quality_local.depth_score,
                    "hold_score": pricing_quality_local.hold_score,
                    "dispersion_score": pricing_quality_local.dispersion_score,
                    "freshness_score": pricing_quality_local.freshness_score,
                    "quality_score": pricing_quality_local.quality_score,
                    "uncertainty_band": pricing_quality_local.uncertainty_band,
                    "baseline_excluded_books": sorted(excluded_books_local),
                    "baseline_books_used": baseline_books_used_local,
                    "baseline_books_used_count": len(baseline_books_used_local),
                    "baseline_method_effective": baseline_used_local,
                    "baseline_is_independent_of_selected_book": (
                        selected_book_local not in baseline_books_used_local
                        if selected_book_local
                        else True
                    ),
                    "baseline_insufficient_after_exclusion": baseline_used_local == "missing",
                }

            side_scenarios["over"] = _candidate_side_scenario(
                candidate_side="over",
                candidate_quote=over,
            )
            side_scenarios["under"] = _candidate_side_scenario(
                candidate_side="under",
                candidate_quote=under,
            )
            ev_over = _safe_float(side_scenarios["over"].get("best_ev"))
            kelly_over = _safe_float(side_scenarios["over"].get("best_kelly"))
            ev_under = _safe_float(side_scenarios["under"].get("best_ev"))
            kelly_under = _safe_float(side_scenarios["under"].get("best_kelly"))
            ev_over_low = _safe_float(side_scenarios["over"].get("ev_low"))
            ev_over_high = _safe_float(side_scenarios["over"].get("ev_high"))
            ev_under_low = _safe_float(side_scenarios["under"].get("ev_low"))
            ev_under_high = _safe_float(side_scenarios["under"].get("ev_high"))

        side = "none"
        best_ev: float | None = None
        best_kelly: float | None = None
        selected_price: int | None = None
        selected_book = ""
        selected_link = ""
        selected_last_update = ""
        model_p_hit: float | None = None
        fair_p_hit: float | None = None
        p_hit_low: float | None = None
        p_hit_high: float | None = None
        ev_low: float | None = None
        ev_high: float | None = None
        if ev_over is not None or ev_under is not None:
            over_value = ev_over if ev_over is not None else -999.0
            under_value = ev_under if ev_under is not None else -999.0
            if over_value >= under_value:
                side = "over"
                best_ev = ev_over
                best_kelly = kelly_over
                selected_price = _to_price(over["price"])
                selected_book = str(over["book"])
                selected_link = str(over["link"])
                selected_last_update = str(over.get("last_update", ""))
                model_p_hit = p_over_model
                fair_p_hit = p_over_fair
                p_hit_low = p_over_low
                p_hit_high = p_over_high
                ev_low = ev_over_low
                ev_high = ev_over_high
            else:
                side = "under"
                best_ev = ev_under
                best_kelly = kelly_under
                selected_price = _to_price(under["price"])
                selected_book = str(under["book"])
                selected_link = str(under["link"])
                selected_last_update = str(under.get("last_update", ""))
                model_p_hit = p_under_model
                fair_p_hit = p_under_fair
                p_hit_low = p_under_low
                p_hit_high = p_under_high
                ev_low = ev_under_low
                ev_high = ev_under_high

        selected_scenario = side_scenarios.get(side)
        if selected_scenario is None and side_scenarios:
            selected_scenario = side_scenarios.get("over") or side_scenarios.get("under")
        if isinstance(selected_scenario, dict):
            selected_price = _to_price(selected_scenario.get("selected_price"))
            selected_book = str(selected_scenario.get("selected_book", ""))
            selected_link = str(selected_scenario.get("selected_link", ""))
            selected_last_update = str(selected_scenario.get("selected_last_update", ""))
            best_ev = _safe_float(selected_scenario.get("best_ev"))
            best_kelly = _safe_float(selected_scenario.get("best_kelly"))
            ev_low = _safe_float(selected_scenario.get("ev_low"))
            ev_high = _safe_float(selected_scenario.get("ev_high"))
            model_p_hit = _safe_float(selected_scenario.get("model_p_hit"))
            fair_p_hit = _safe_float(selected_scenario.get("fair_p_hit"))
            p_hit_low = _safe_float(selected_scenario.get("p_hit_low"))
            p_hit_high = _safe_float(selected_scenario.get("p_hit_high"))
            p_over_model = _safe_float(selected_scenario.get("p_over_model"))
            p_under_model = _safe_float(selected_scenario.get("p_under_model"))
            p_over_low = _safe_float(selected_scenario.get("p_over_low"))
            p_over_high = _safe_float(selected_scenario.get("p_over_high"))
            p_under_low = _safe_float(selected_scenario.get("p_under_low"))
            p_under_high = _safe_float(selected_scenario.get("p_under_high"))
            p_over_fair = _safe_float(selected_scenario.get("p_over_fair"))
            p_under_fair = _safe_float(selected_scenario.get("p_under_fair"))
            hold = _safe_float(selected_scenario.get("hold"))
            hold_best = _safe_float(selected_scenario.get("hold_best_sides"))
            baseline_used = str(selected_scenario.get("baseline_used", baseline_used))
            baseline_method_effective = str(
                selected_scenario.get("baseline_method_effective", baseline_used)
            )
            books_used_exact = [str(item) for item in selected_scenario.get("books_used_exact", [])]
            line_source = str(selected_scenario.get("line_source", line_source))
            reference_line_method = str(
                selected_scenario.get("reference_line_method", reference_line_method)
            )
            reference_points_count = int(
                _safe_float(selected_scenario.get("reference_points_count"))
                or reference_points_count
            )
            book_pair_count = int(_safe_float(selected_scenario.get("book_pair_count")) or 0)
            p_over_book_median = _safe_float(selected_scenario.get("p_over_book_median"))
            hold_book_median = _safe_float(selected_scenario.get("hold_book_median"))
            p_over_book_iqr = _safe_float(selected_scenario.get("p_over_book_iqr"))
            p_over_book_range = _safe_float(selected_scenario.get("p_over_book_range"))
            freshest_quote_utc = str(selected_scenario.get("freshest_quote_utc", ""))
            quote_age_minutes = _safe_float(selected_scenario.get("quote_age_minutes"))
            depth_score = _safe_float(selected_scenario.get("depth_score")) or 0.0
            hold_score = _safe_float(selected_scenario.get("hold_score")) or 0.0
            dispersion_score = _safe_float(selected_scenario.get("dispersion_score")) or 0.0
            freshness_score = _safe_float(selected_scenario.get("freshness_score")) or 0.0
            quality_score = _safe_float(selected_scenario.get("quality_score")) or 0.0
            uncertainty_band = _safe_float(selected_scenario.get("uncertainty_band")) or 0.2
            baseline_excluded_books = [
                str(item) for item in selected_scenario.get("baseline_excluded_books", [])
            ]
            baseline_books_used = [
                str(item) for item in selected_scenario.get("baseline_books_used", [])
            ]
            baseline_books_used_count = int(
                _safe_float(selected_scenario.get("baseline_books_used_count"))
                or len(baseline_books_used)
            )
            baseline_is_independent_of_selected_book = bool(
                selected_scenario.get("baseline_is_independent_of_selected_book")
            )
            baseline_insufficient_after_exclusion = bool(
                selected_scenario.get("baseline_insufficient_after_exclusion")
            )
            if baseline_insufficient_after_exclusion and probabilistic_profile == "minutes_v1":
                prob_source = "minutes_v1_baseline_missing"
        if selected_book:
            baseline_is_independent_of_selected_book = selected_book not in baseline_books_used

        calibration_hit = calibration_feedback(
            rolling_priors=rolling_priors,
            market=market,
            side=side,
            model_probability=model_p_hit,
        )
        calibration_low = calibration_feedback(
            rolling_priors=rolling_priors,
            market=market,
            side=side,
            model_probability=p_hit_low,
        )
        p_hit_calibrated = _safe_float(calibration_hit.get("p_calibrated"))
        p_hit_low_calibrated = _safe_float(calibration_low.get("p_calibrated"))
        if p_hit_low_calibrated is None:
            p_hit_low_calibrated = p_hit_calibrated
        ev_calibrated, _ = _ev_and_kelly(p_hit_calibrated, selected_price)
        ev_low_calibrated, _ = _ev_and_kelly(p_hit_low_calibrated, selected_price)

        if eligible and baseline_used == "missing":
            eligible = False
            reason = (
                "baseline_insufficient_coverage_after_exclusion"
                if baseline_insufficient_after_exclusion and baseline_excluded_books
                else "baseline_missing"
            )
        if (
            eligible
            and tier == "B"
            and tier_b_min_other_books_for_baseline is not None
            and baseline_books_used_count < tier_b_min_other_books_for_baseline
        ):
            eligible = False
            reason = "tier_b_baseline_not_independent"
        if eligible and min_book_pairs > 0 and book_pair_count < min_book_pairs:
            eligible = False
            reason = "book_pairs_gate"
        if eligible and hold_cap is not None:
            if hold_book_median is None:
                eligible = False
                reason = "hold_missing"
            elif hold_book_median > hold_cap:
                eligible = False
                reason = "hold_cap"
        if eligible and p_over_iqr_cap is not None:
            if p_over_book_iqr is None:
                eligible = False
                reason = "dispersion_missing"
            elif p_over_book_iqr > p_over_iqr_cap:
                eligible = False
                reason = "dispersion_iqr"
        if eligible and min_quality_score is not None and quality_score < min_quality_score:
            eligible = False
            reason = "quality_score_gate"
        if (
            eligible
            and max_uncertainty_band is not None
            and uncertainty_band > max_uncertainty_band
        ):
            eligible = False
            reason = "uncertainty_band_gate"

        min_ev_for_line = tier_a_min_ev if tier == "A" else tier_b_min_ev
        if best_ev is None or best_ev < min_ev_for_line:
            if eligible:
                reason = "ev_below_threshold"
            eligible = False
        if eligible and min_ev_low is not None:
            if ev_low is None:
                eligible = False
                reason = "ev_low_missing"
            elif ev_low < min_ev_low:
                eligible = False
                reason = "ev_low_below_threshold"
        if eligible and probabilistic_profile == "minutes_v1":
            if min_prob_confidence is not None:
                if confidence_score is None:
                    eligible = False
                    reason = "prob_confidence_missing"
                elif confidence_score < min_prob_confidence:
                    eligible = False
                    reason = "prob_confidence_gate"
            if eligible and max_minutes_band is not None:
                if minutes_band is None:
                    eligible = False
                    reason = "minutes_band_missing"
                elif minutes_band > max_minutes_band:
                    eligible = False
                    reason = "minutes_band_gate"

        if eligible:
            eligible_count += 1

        target_roi = 0.03 if tier == "A" else 0.05
        play_to_decimal, play_to_american = _play_to(model_p_hit, target_roi)
        breakeven_decimal, breakeven_american = _play_to(model_p_hit, 0.0)

        fair_decimal = round((1.0 / model_p_hit), 6) if model_p_hit else None
        fair_american = _decimal_to_american(fair_decimal)

        prior_payload = (
            _prior_payload(rolling_priors, market=market, side=side)
            if side in {"over", "under"}
            else {}
        )
        historical_prior_delta = _safe_float(prior_payload.get("delta")) or 0.0
        historical_prior_sample_size = int(prior_payload.get("sample_size", 0) or 0)
        historical_prior_hit_rate = _safe_float(prior_payload.get("hit_rate"))

        hold_penalty = 20.0 if hold is None else hold * 100.0
        shop_value = int(over["shop_delta"]) + int(under["shop_delta"])
        score_base = (
            ((best_ev or -0.5) * 1000.0) + (book_count * 5.0) + (shop_value / 10.0) - hold_penalty
        )
        score = score_base + (historical_prior_delta * HISTORICAL_PRIOR_SCORE_WEIGHT)

        rationale = _compose_rationale(
            player=player,
            market=market,
            side=side,
            p_hit=model_p_hit,
            p_fair=fair_p_hit,
            injury_status=injury_status,
            teammate_counts=teammate_counts,
            spread_abs=spread_abs,
            total=total,
            projected_minutes=_safe_float(minutes_projection.get("projected_minutes")),
            usage_delta=_safe_float(minutes_projection.get("usage_delta")),
        )
        risk_notes = _compose_risk_notes(
            injury_status=injury_status,
            roster_status=roster_status,
            spread_abs=spread_abs,
            tier=tier,
        )

        candidates.append(
            {
                "event_id": event_id,
                "home_team": home_team,
                "away_team": away_team,
                "tip_et": str(line_meta.get("tip_et", "")),
                "game": f"{away_team} @ {home_team}".strip(),
                "market": market,
                "player": player,
                "point": point,
                "tier": tier,
                "books": books,
                "book_count": book_count,
                "over_best_price": over["price"],
                "over_best_book": over["book"],
                "over_link": over["link"],
                "over_last_update": over.get("last_update", ""),
                "over_book_count": over["books"],
                "over_shop_delta": over["shop_delta"],
                "under_best_price": under["price"],
                "under_best_book": under["book"],
                "under_link": under["link"],
                "under_last_update": under.get("last_update", ""),
                "under_book_count": under["books"],
                "under_shop_delta": under["shop_delta"],
                "p_over_fair": p_over_fair,
                "p_under_fair": p_under_fair,
                "baseline_used": baseline_used,
                "baseline_method_effective": baseline_method_effective,
                "baseline_excluded_books": baseline_excluded_books,
                "baseline_books_used": baseline_books_used,
                "baseline_books_used_count": baseline_books_used_count,
                "baseline_is_independent_of_selected_book": (
                    baseline_is_independent_of_selected_book
                ),
                "line_source": line_source,
                "reference_line_method": reference_line_method,
                "reference_points_count": reference_points_count,
                "books_used": books_used_exact,
                "book_pair_count": book_pair_count,
                "p_over_book_median": p_over_book_median,
                "hold_book_median": hold_book_median,
                "p_over_book_iqr": p_over_book_iqr,
                "p_over_book_range": p_over_book_range,
                "p_over_model": p_over_model,
                "p_under_model": p_under_model,
                "p_over_low": p_over_low,
                "p_over_high": p_over_high,
                "p_under_low": p_under_low,
                "p_under_high": p_under_high,
                "ev_over": ev_over,
                "ev_under": ev_under,
                "ev_over_low": ev_over_low,
                "ev_under_low": ev_under_low,
                "ev_over_high": ev_over_high,
                "ev_under_high": ev_under_high,
                "kelly_over": kelly_over,
                "kelly_under": kelly_under,
                "recommended_side": side,
                "selected_price": selected_price,
                "selected_book": selected_book,
                "selected_link": selected_link,
                "selected_last_update": selected_last_update,
                "model_p_hit": model_p_hit,
                "p_hit_low": p_hit_low,
                "p_hit_high": p_hit_high,
                "p_hit_calibrated": p_hit_calibrated,
                "p_hit_low_calibrated": p_hit_low_calibrated,
                "fair_p_hit": fair_p_hit,
                "fair_decimal": fair_decimal,
                "fair_american": fair_american,
                "edge_pct": round((best_ev or 0.0) * 100.0, 3) if best_ev is not None else None,
                "ev_per_100": round((best_ev or 0.0) * 100.0, 3) if best_ev is not None else None,
                "ev_low": ev_low,
                "ev_high": ev_high,
                "ev_calibrated": ev_calibrated,
                "ev_low_calibrated": ev_low_calibrated,
                "calibration_source": str(calibration_hit.get("source", "")),
                "calibration_sample_size": int(calibration_hit.get("sample_size", 0) or 0),
                "calibration_confidence": _safe_float(calibration_hit.get("confidence")) or 0.0,
                "calibration_delta": _safe_float(calibration_hit.get("delta")),
                "calibration_bucket_index": calibration_hit.get("bucket_index"),
                "calibration_bucket_low": calibration_hit.get("bucket_low"),
                "calibration_bucket_high": calibration_hit.get("bucket_high"),
                "play_to_decimal": play_to_decimal,
                "play_to_american": play_to_american,
                "breakeven_decimal": breakeven_decimal,
                "breakeven_american": breakeven_american,
                "target_roi": target_roi,
                "best_ev": best_ev,
                "best_kelly": best_kelly,
                "full_kelly": best_kelly,
                "quarter_kelly": round((best_kelly / 4.0), 6) if best_kelly is not None else None,
                "hold": hold,
                "hold_best_sides": hold_best,
                "quote_age_minutes": quote_age_minutes,
                "freshest_quote_utc": freshest_quote_utc,
                "depth_score": round(depth_score, 6),
                "hold_score": round(hold_score, 6),
                "dispersion_score": round(dispersion_score, 6),
                "freshness_score": round(freshness_score, 6),
                "quality_score": quality_score,
                "uncertainty_band": uncertainty_band,
                "injury_status": injury_status,
                "injury_note": injury_note,
                "roster_status": roster_status,
                "pre_bet_ready": pre_bet_ready,
                "pre_bet_reason": pre_bet_reason,
                "roster_resolution_detail": _roster_resolution_detail(roster_status),
                "baseline_minutes": minutes_projection.get("baseline_minutes"),
                "projected_minutes": minutes_projection.get("projected_minutes"),
                "minutes_delta": minutes_projection.get("minutes_delta"),
                "usage_delta": minutes_projection.get("usage_delta"),
                "market_delta": round(market_delta, 6),
                "probabilistic_profile": probabilistic_profile,
                "prob_source": prob_source,
                "minutes_prob_delta_over": round(minutes_prob_delta_over, 6),
                "minutes_p10": minutes_p10,
                "minutes_p50": minutes_p50,
                "minutes_p90": minutes_p90,
                "minutes_band": minutes_band,
                "p_active": p_active,
                "confidence_score": confidence_score,
                "data_quality_flags": data_quality_flags,
                "player_team": player_team,
                "opponent_team": opponent_team,
                "mapping_suggestion": {
                    "player_norm": normalize_person_name(player),
                    "event_id": event_id,
                    "suggested_team": player_team,
                },
                "teammate_out_count": teammate_counts.get("out", 0)
                + teammate_counts.get("out_for_season", 0),
                "teammate_doubtful_count": teammate_counts.get("doubtful", 0),
                "opponent_out_count": opponent_counts.get("out", 0)
                + opponent_counts.get("out_for_season", 0),
                "spread_abs": spread_abs,
                "total": total,
                "eligible": eligible,
                "reason": reason,
                "score_base": round(score_base, 6),
                "score": round(score, 6),
                "historical_prior_delta": round(historical_prior_delta, 6),
                "historical_prior_sample_size": historical_prior_sample_size,
                "historical_prior_hit_rate": (
                    round(historical_prior_hit_rate, 6)
                    if historical_prior_hit_rate is not None
                    else None
                ),
                "rationale": rationale,
                "risk_notes": risk_notes,
                "prop_label": _prop_label(player, side, point, market),
                "book_price": f"{selected_book} {_fmt_american(selected_price)}".strip(),
            }
        )

    request_counts: dict[str, int] = {}
    requests = manifest.get("requests", {})
    if isinstance(requests, dict):
        for value in requests.values():
            if isinstance(value, dict):
                status = str(value.get("status", ""))
                request_counts[status] = request_counts.get(status, 0) + 1

    availability = _availability_notes(
        injuries=injuries, roster=roster, teams_in_scope=teams_in_scope
    )
    roster_ok = isinstance(roster, dict) and roster.get("status") == "ok"
    roster_count = int(roster.get("count_teams", 0)) if isinstance(roster, dict) else 0

    contract_rows = _validate_rows_contract(rows)
    event_ids_in_rows = {
        str(row.get("event_id", "")).strip()
        for row in rows
        if isinstance(row, dict) and str(row.get("event_id", "")).strip()
    }
    event_ids_in_context = set(event_context.keys()) if isinstance(event_context, dict) else set()
    missing_event_mappings = sorted(event_ids_in_rows - event_ids_in_context)

    if missing_event_mappings:
        for item in candidates:
            event_id = str(item.get("event_id", ""))
            if event_id in missing_event_mappings:
                item["eligible"] = False
                item["reason"] = "event_mapping_missing"

    odds_health = _odds_health(
        candidates,
        stale_quote_minutes,
        now_utc=strategy_now_utc,
    )
    health_gates: list[str] = []
    if require_official_injuries and not official_ready:
        health_gates.append("official_injury_missing")
    if bool(odds_health.get("odds_stale", False)):
        health_gates.append("odds_snapshot_stale")
    injuries_stale = bool(injuries.get("stale", False)) if isinstance(injuries, dict) else True
    roster_stale = bool(roster.get("stale", False)) if isinstance(roster, dict) else True
    if require_fresh_context and injuries_stale:
        health_gates.append("injuries_context_stale")
    if require_fresh_context and roster_stale:
        health_gates.append("roster_context_stale")

    if health_gates:
        for item in candidates:
            if bool(item.get("eligible")):
                item["eligible"] = False
                item["reason"] = f"health_gate:{','.join(health_gates)}"

    candidates.sort(
        key=lambda row: (
            not bool(row.get("eligible")),
            -(row.get("best_ev") or -999.0),
            -(row.get("score") or -9999.0),
            row.get("event_id", ""),
            row.get("player", ""),
            row.get("point", 0.0),
        )
    )
    eligible_rows = [item for item in candidates if item.get("eligible")]
    eligible_count = len(eligible_rows)
    portfolio_constraints = PortfolioConstraints(
        max_picks=resolved_max_picks,
        max_per_player=PORTFOLIO_MAX_PER_PLAYER,
        max_per_game=PORTFOLIO_MAX_PER_GAME,
    )
    ranked, portfolio_exclusions = select_portfolio_candidates(
        eligible_rows=eligible_rows,
        constraints=portfolio_constraints,
        ranking=portfolio_ranking,
    )
    watchlist = [item for item in candidates if not item.get("eligible")][: max(0, top_n)]
    portfolio_watchlist = portfolio_exclusions[: max(0, top_n)]
    top_ev_plays = [item for item in ranked if item.get("tier") == "A"][: max(0, top_n)]
    one_source_edges = [item for item in ranked if item.get("tier") == "B"][: max(0, top_n)]
    sgp_candidates = _build_sgp_candidates(eligible_rows, top_n=min(10, max(top_n, 5)))

    qualified_unders = [item for item in eligible_rows if item.get("recommended_side") == "under"]
    closest_under_misses = [
        item
        for item in candidates
        if item.get("recommended_side") == "under" and not item.get("eligible")
    ]
    closest_under_misses.sort(key=lambda row: -(row.get("best_ev") or -999.0))
    under_sweep = {
        "qualified_count": len(qualified_unders),
        "qualified": qualified_unders[:5],
        "closest_misses": closest_under_misses[:5],
        "status": "ok" if len(qualified_unders) >= 2 else "insufficient",
        "note": (
            "No Unders >= threshold; showing closest misses with PLAY-TO numbers."
            if len(qualified_unders) < 2
            else "Under sweep satisfied with at least two qualified unders."
        ),
    }

    price_dependent_watchlist = []
    for item in candidates:
        if item.get("eligible"):
            continue
        if item.get("reason") not in {"ev_below_threshold", "tier_b_blocked"}:
            continue
        play_to = item.get("play_to_american")
        if play_to is None:
            continue
        price_dependent_watchlist.append(
            {
                "event_id": item.get("event_id", ""),
                "game": item.get("game", ""),
                "player": item.get("player", ""),
                "market": item.get("market", ""),
                "point": item.get("point", 0.0),
                "side": item.get("recommended_side", ""),
                "current_price": item.get("selected_price"),
                "play_to_american": play_to,
                "play_to_decimal": item.get("play_to_decimal"),
                "target_roi": item.get("target_roi"),
                "best_ev": item.get("best_ev"),
                "reason": item.get("reason", ""),
                "tier": item.get("tier", ""),
            }
        )

    kelly_summary = [
        {
            "event_id": item.get("event_id", ""),
            "game": item.get("game", ""),
            "player": item.get("player", ""),
            "market": item.get("market", ""),
            "point": item.get("point", 0.0),
            "side": item.get("recommended_side", ""),
            "book": item.get("selected_book", ""),
            "price": item.get("selected_price"),
            "full_kelly": item.get("full_kelly"),
            "quarter_kelly": item.get("quarter_kelly"),
        }
        for item in ranked[: max(top_n, 10)]
    ]

    verified_players: list[dict[str, Any]] = []
    seen_verified: set[tuple[str, str]] = set()
    for item in top_ev_plays + one_source_edges:
        player = str(item.get("player", ""))
        team = str(item.get("player_team", ""))
        key = (normalize_person_name(player), team)
        if key in seen_verified:
            continue
        seen_verified.add(key)
        verified_players.append(
            {
                "player": player,
                "team": team,
                "event_id": str(item.get("event_id", "")),
                "roster_status": str(item.get("roster_status", "")),
                "verification_source": "nba_roster_page",
                "verification_link": _nba_roster_link(team, player),
            }
        )

    warnings: list[str] = []
    for item in candidates:
        warning = _roster_warning(
            str(item.get("roster_status", "")),
            str(item.get("player", "")),
            str(item.get("event_id", "")),
        )
        if warning:
            warnings.append(warning)
    roster_warnings = sorted(set(warnings))[:30]

    gaps: list[str] = []
    if not official_ready:
        gaps.append(
            "Official NBA injury report data was not cleanly parsed "
            "(missing links, download failure, or empty parse rows)."
        )
    if not isinstance(secondary, dict) or secondary.get("status") != "ok":
        gaps.append("Secondary injury feed unavailable (optional fallback).")
    if not roster_ok:
        gaps.append("Roster verification feed was not available.")
    elif roster_count == 0:
        gaps.append("Roster feed returned no team/player rows for these events.")
    if not slate_rows:
        gaps.append("Slate spreads/totals were unavailable in this snapshot.")
    if injuries_stale:
        gaps.append("Injury context cache is stale (TTL exceeded).")
    if roster_stale:
        gaps.append("Roster context cache is stale (TTL exceeded).")
    if health_gates:
        gaps.append("Health gates triggered watchlist-only mode for this snapshot.")
    gaps.extend(
        [
            "Model uses market-implied fair probabilities with injury/roster/opponent adjustments.",
            "Minutes/usage projection uses deterministic core rules, not learned distributions.",
            "SGP/SGPx correlation uses deterministic haircut rules (core model).",
        ]
    )

    audit_trail = _audit_entries(
        manifest=manifest,
        availability=availability,
        top_plays=top_ev_plays,
        one_source_edges=one_source_edges,
    )
    unresolved_players: list[dict[str, Any]] = []
    seen_unresolved: set[tuple[str, str, str]] = set()
    for item in candidates:
        status = str(item.get("roster_status", ""))
        if status not in {"unknown_event", "unknown_roster", "not_on_roster"}:
            continue
        event_id = str(item.get("event_id", ""))
        player_name = str(item.get("player", ""))
        dedupe_key = (event_id, player_name, status)
        if dedupe_key in seen_unresolved:
            continue
        seen_unresolved.add(dedupe_key)
        unresolved_players.append(
            {
                "event_id": event_id,
                "player": player_name,
                "roster_status": status,
                "detail": str(item.get("roster_resolution_detail", "")),
                "mapping_suggestion": item.get("mapping_suggestion", {}),
            }
        )

    strategy_mode = "watchlist_only" if health_gates else "full_board"
    health_report = {
        "strategy_mode": strategy_mode,
        "health_gates": health_gates,
        "require_official_injuries": require_official_injuries,
        "require_fresh_context": require_fresh_context,
        "odds": odds_health,
        "contracts": {
            "props_rows": contract_rows,
            "missing_event_mappings": missing_event_mappings,
        },
        "feeds": {
            "official_injuries": str(official.get("status", "missing"))
            if isinstance(official, dict)
            else "missing",
            "official_injuries_parse": str(official.get("parse_status", "missing"))
            if isinstance(official, dict)
            else "missing",
            "official_injuries_rows": official_rows_count,
            "secondary_injuries": str(secondary.get("status", "missing"))
            if isinstance(secondary, dict)
            else "missing",
            "roster": str(roster.get("status", "missing"))
            if isinstance(roster, dict)
            else "missing",
            "injuries_stale": _bool(injuries_stale),
            "roster_stale": _bool(roster_stale),
        },
        "excluded_games": missing_event_mappings,
        "identity_map_entries": (
            len(player_identity_map.get("players", {}))
            if isinstance(player_identity_map, dict)
            and isinstance(player_identity_map.get("players", {}), dict)
            else 0
        ),
    }
    quality_scores_all = [
        score
        for score in (_safe_float(item.get("quality_score")) for item in candidates)
        if score is not None
    ]
    quality_scores_eligible = [
        score
        for score in (_safe_float(item.get("quality_score")) for item in eligible_rows)
        if score is not None
    ]
    ev_low_eligible = [
        score
        for score in (_safe_float(item.get("ev_low")) for item in eligible_rows)
        if score is not None
    ]
    avg_quality_all = _mean(quality_scores_all)
    avg_quality_eligible = _mean(quality_scores_eligible)
    avg_ev_low = _mean(ev_low_eligible)
    actionability_rate = round((eligible_count / len(candidates)), 6) if candidates else 0.0
    rolling_priors_window_days = (
        int(rolling_priors.get("window_days", 0)) if isinstance(rolling_priors, dict) else 0
    )
    rolling_priors_rows_used = (
        int(rolling_priors.get("rows_used", 0)) if isinstance(rolling_priors, dict) else 0
    )
    rolling_priors_as_of_day = (
        str(rolling_priors.get("as_of_day", "")) if isinstance(rolling_priors, dict) else ""
    )
    generated_at_utc = _now_utc()
    exclusion_reason_counts = Counter(
        str(row.get("portfolio_reason", "")).strip()
        for row in portfolio_exclusions
        if str(row.get("portfolio_reason", "")).strip()
    )
    execution_plan = {
        "schema_version": EXECUTION_PLAN_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "strategy_id": "",
        "generated_at_utc": generated_at_utc,
        "constraints": {
            "max_picks": resolved_max_picks,
            "max_per_player": PORTFOLIO_MAX_PER_PLAYER,
            "max_per_game": PORTFOLIO_MAX_PER_GAME,
        },
        "counts": {
            "candidate_lines": len(candidates),
            "eligible_lines": eligible_count,
            "selected_lines": len(ranked),
            "excluded_lines": len(portfolio_exclusions),
        },
        "selected": [
            _execution_plan_row(row) for row in sorted(ranked, key=_execution_plan_sort_key)
        ],
        "excluded": [
            _execution_plan_row(row)
            for row in sorted(portfolio_exclusions, key=_execution_plan_sort_key)
        ],
        "exclusion_reason_counts": dict(sorted(exclusion_reason_counts.items())),
    }
    assert_execution_plan(execution_plan)

    return {
        "generated_at_utc": generated_at_utc,
        "modeled_date_et": _et_date_label(event_context),
        "timezone": "ET",
        "strategy_status": "modeled_with_gates",
        "strategy_mode": strategy_mode,
        "state_key": strategy_report_state_key(),
        "snapshot_id": snapshot_id,
        "health_report": health_report,
        "slate_snapshot": slate_snapshot,
        "availability": availability,
        "roster_status_warnings": roster_warnings,
        "unresolved_players": unresolved_players[:50],
        "verified_players": verified_players,
        "top_ev_plays": top_ev_plays,
        "one_source_edges": one_source_edges,
        "under_sweep": under_sweep,
        "execution_plan": execution_plan,
        "price_dependent_watchlist": price_dependent_watchlist,
        "kelly_summary": kelly_summary,
        "sgp_candidates": sgp_candidates,
        "gaps": gaps,
        "summary": {
            "events": len({item["event_id"] for item in candidates}),
            "candidate_lines": len(candidates),
            "tier_a_lines": tier_a_count,
            "tier_b_lines": tier_b_count,
            "eligible_lines": eligible_count,
            "ranked_lines": len(ranked),
            "max_picks": resolved_max_picks,
            "portfolio_excluded_lines": len(portfolio_exclusions),
            "strategy_mode": strategy_mode,
            "watchlist_only": _bool(strategy_mode == "watchlist_only"),
            "health_gate_count": len(health_gates),
            "eligible_tier_a": len([item for item in eligible_rows if item.get("tier") == "A"]),
            "eligible_tier_b": len([item for item in eligible_rows if item.get("tier") == "B"]),
            "eligible_pre_bet_ready": len(
                [item for item in eligible_rows if bool(item.get("pre_bet_ready"))]
            ),
            "qualified_unders": len(qualified_unders),
            "request_counts": request_counts,
            "quota": manifest.get("quota", {}),
            "injury_source_official": _bool(official_ready),
            "injury_source_secondary": _bool(
                isinstance(secondary, dict) and secondary.get("status") == "ok"
            ),
            "roster_source": _bool(roster_ok and roster_count > 0),
            "roster_team_rows": roster_count,
            "under_sweep_status": under_sweep.get("status", ""),
            "sgp_candidates": len(sgp_candidates),
            "actionability_rate": actionability_rate,
            "probabilistic_profile": probabilistic_profile,
            "probabilistic_rows_used": probabilistic_rows_used,
            "probabilistic_rows_missing": len(
                [
                    item
                    for item in candidates
                    if str(item.get("prob_source", "")).startswith("minutes_v1")
                    and str(item.get("prob_source", "")) != "minutes_v1_model"
                ]
            ),
            "avg_quality_score_all": round(avg_quality_all, 6)
            if avg_quality_all is not None
            else None,
            "avg_quality_score_eligible": (
                round(avg_quality_eligible, 6) if avg_quality_eligible is not None else None
            ),
            "avg_ev_low_eligible": round(avg_ev_low, 6) if avg_ev_low is not None else None,
            "rolling_priors_window_days": rolling_priors_window_days,
            "rolling_priors_rows_used": rolling_priors_rows_used,
        },
        "candidates": candidates,
        "ranked_plays": ranked,
        "watchlist": watchlist,
        "portfolio_watchlist": portfolio_watchlist,
        "audit": {
            "manifest_created_at_utc": manifest.get("created_at_utc", ""),
            "manifest_schema_version": manifest.get("schema_version", ""),
            "report_schema_version": 5,
            "min_ev": min_ev,
            "max_picks": resolved_max_picks,
            "portfolio_max_per_player": PORTFOLIO_MAX_PER_PLAYER,
            "portfolio_max_per_game": PORTFOLIO_MAX_PER_GAME,
            "tier_a_min_ev": tier_a_min_ev,
            "tier_b_min_ev": tier_b_min_ev,
            "allow_tier_b": allow_tier_b,
            "portfolio_ranking": portfolio_ranking,
            "market_baseline_method": baseline_method,
            "market_baseline_fallback": baseline_fallback,
            "exclude_selected_book_from_baseline": exclude_selected_book_from_baseline,
            "tier_b_min_other_books_for_baseline": tier_b_min_other_books_for_baseline,
            "min_book_pairs": min_book_pairs,
            "hold_cap": hold_cap,
            "p_over_iqr_cap": p_over_iqr_cap,
            "min_quality_score": min_quality_score,
            "min_ev_low": min_ev_low,
            "max_uncertainty_band": max_uncertainty_band,
            "probabilistic_profile": probabilistic_profile,
            "min_prob_confidence": min_prob_confidence,
            "max_minutes_band": max_minutes_band,
            "probabilistic_rows_used": probabilistic_rows_used,
            "rolling_priors_window_days": rolling_priors_window_days,
            "rolling_priors_rows_used": rolling_priors_rows_used,
            "rolling_priors_as_of_day": rolling_priors_as_of_day,
            "timezone": "ET",
            "audit_trail": audit_trail,
        },
    }


_strategy_output_impl._short_game_label = _short_game_label
_strategy_output_impl._prop_label = _prop_label
_strategy_output_impl._safe_float = _safe_float
_strategy_output_impl._fmt_american = _fmt_american

render_strategy_markdown = _strategy_output_impl.render_strategy_markdown
write_strategy_reports = _strategy_output_impl.write_strategy_reports
write_execution_plan = _strategy_output_impl.write_execution_plan
write_tagged_strategy_reports = _strategy_output_impl.write_tagged_strategy_reports
