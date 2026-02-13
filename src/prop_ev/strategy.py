"""Deterministic strategy report generation from snapshot-derived odds rows."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

from prop_ev.brief_builder import TEAM_ABBREVIATIONS
from prop_ev.context_health import official_rows_count
from prop_ev.context_sources import canonical_team_name, normalize_person_name
from prop_ev.identity_map import name_aliases
from prop_ev.models.core_minutes_usage import market_side_adjustment_core, minutes_usage_core
from prop_ev.odds_math import (
    american_to_decimal,
    decimal_to_american,
    ev_from_prob_and_price,
    implied_prob_from_american,
    normalize_prob_pair,
)
from prop_ev.state_keys import strategy_report_state_key
from prop_ev.time_utils import parse_iso_z, utc_now_str

ET_ZONE = ZoneInfo("America/New_York")
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


def _to_price(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.startswith("+"):
            raw = raw[1:]
        try:
            return int(raw)
        except ValueError:
            return None
    return None


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


def _quantile(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    if q <= 0:
        return sorted_values[0]
    if q >= 1:
        return sorted_values[-1]
    pos = q * (len(sorted_values) - 1)
    lower = int(pos)
    upper = min(len(sorted_values) - 1, lower + 1)
    if lower == upper:
        return sorted_values[lower]
    frac = pos - lower
    return (sorted_values[lower] * (1.0 - frac)) + (sorted_values[upper] * frac)


def _iqr(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    q1 = _quantile(ordered, 0.25)
    q3 = _quantile(ordered, 0.75)
    if q1 is None or q3 is None:
        return None
    return q3 - q1


def _per_book_prob_pairs(group_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return per-book no-vig probability pairs for a single (event, player, market, point) line."""
    book_sides: dict[str, dict[str, list[int]]] = {}
    for row in group_rows:
        if not isinstance(row, dict):
            continue
        book = str(row.get("book", "")).strip()
        if not book:
            continue
        side_raw = str(row.get("side", "")).strip().lower()
        if side_raw in {"over", "o"}:
            side = "over"
        elif side_raw in {"under", "u"}:
            side = "under"
        else:
            continue
        price = _to_price(row.get("price"))
        if price is None:
            continue
        entry = book_sides.setdefault(book, {"over": [], "under": []})
        entry[side].append(price)

    pairs: list[dict[str, Any]] = []
    for book, sides in book_sides.items():
        if not sides["over"] or not sides["under"]:
            continue
        over_price = max(sides["over"])
        under_price = max(sides["under"])
        over_prob_imp = _implied_prob_from_american(over_price)
        under_prob_imp = _implied_prob_from_american(under_price)
        if over_prob_imp is None or under_prob_imp is None:
            continue
        p_over_fair, p_under_fair = _normalize_prob_pair(over_prob_imp, under_prob_imp)
        pairs.append(
            {
                "book": book,
                "over_price": over_price,
                "under_price": under_price,
                "p_over_fair": p_over_fair,
                "p_under_fair": p_under_fair,
                "hold": (over_prob_imp + under_prob_imp) - 1.0,
            }
        )
    pairs.sort(key=lambda row: str(row.get("book", "")))
    return pairs


def _line_key(row: dict[str, Any]) -> tuple[str, str, str, float]:
    event_id = str(row.get("event_id", ""))
    market = str(row.get("market", ""))
    player = str(row.get("player", ""))
    point = _safe_float(row.get("point")) or 0.0
    return event_id, market, player, point


def _best_side(rows: list[dict[str, Any]]) -> dict[str, Any]:
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


def _odds_health(candidates: list[dict[str, Any]], stale_after_min: int) -> dict[str, Any]:
    timestamps: list[datetime] = []
    for row in candidates:
        if not isinstance(row, dict):
            continue
        raw = str(row.get("selected_last_update", "")).strip()
        parsed = _parse_quote_time(raw)
        if parsed is not None:
            timestamps.append(parsed)

    now = datetime.now(UTC)
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
    age_latest = (now - newest).total_seconds() / 60.0
    age_oldest = (now - oldest).total_seconds() / 60.0
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
    injuries: dict[str, Any] | None = None,
    roster: dict[str, Any] | None = None,
    event_context: dict[str, dict[str, str]] | None = None,
    slate_rows: list[dict[str, Any]] | None = None,
    player_identity_map: dict[str, Any] | None = None,
    min_ev: float = 0.01,
    allow_tier_b: bool = False,
    require_official_injuries: bool = True,
    stale_quote_minutes: int = 20,
    require_fresh_context: bool = True,
    market_baseline_method: str = "best_sides",
    market_baseline_fallback: str = "best_sides",
    min_book_pairs: int = 0,
    hold_cap: float | None = None,
    p_over_iqr_cap: float | None = None,
) -> dict[str, Any]:
    """Create an audit-ready, deterministic NBA prop strategy report."""
    baseline_method = market_baseline_method.strip().lower()
    if baseline_method not in {"best_sides", "median_book"}:
        raise ValueError(f"invalid market_baseline_method: {market_baseline_method}")
    baseline_fallback = market_baseline_fallback.strip().lower()
    if baseline_fallback not in {"best_sides", "none"}:
        raise ValueError(f"invalid market_baseline_fallback: {market_baseline_fallback}")
    min_book_pairs = max(0, int(min_book_pairs))
    if hold_cap is not None and hold_cap < 0:
        raise ValueError("hold_cap must be >= 0")
    if p_over_iqr_cap is not None and p_over_iqr_cap < 0:
        raise ValueError("p_over_iqr_cap must be >= 0")

    grouped: dict[tuple[str, str, str, float], list[dict[str, Any]]] = {}
    for row in rows:
        key = _line_key(row)
        grouped.setdefault(key, []).append(row)

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

        book_pairs = _per_book_prob_pairs(group_rows)
        book_pair_count = len(book_pairs)
        p_over_book = [
            pair["p_over_fair"] for pair in book_pairs if isinstance(pair.get("p_over_fair"), float)
        ]
        hold_book = [pair["hold"] for pair in book_pairs if isinstance(pair.get("hold"), float)]
        p_over_book_median = _median(p_over_book)
        hold_book_median = _median(hold_book)
        p_over_book_iqr = _iqr(p_over_book)
        p_over_book_range: float | None = None
        if p_over_book:
            p_over_book_range = max(p_over_book) - min(p_over_book)
        p_over_fair: float | None = None
        p_under_fair: float | None = None
        hold: float | None = None
        baseline_used = "best_sides"
        p_over_fair, p_under_fair, hold = p_over_fair_best, p_under_fair_best, hold_best
        if baseline_method == "median_book":
            if p_over_book_median is not None and hold_book_median is not None:
                p_over_fair = p_over_book_median
                p_under_fair = 1.0 - p_over_fair
                hold = hold_book_median
                baseline_used = "median_book"
            elif baseline_fallback == "best_sides":
                baseline_used = "best_sides_fallback"
            else:
                p_over_fair, p_under_fair, hold = None, None, None
                baseline_used = "missing"

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

        if eligible and baseline_used == "missing":
            eligible = False
            reason = "baseline_missing"
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
        if p_over_fair is not None and p_under_fair is not None:
            p_over_model = min(0.99, max(0.01, p_over_fair + adjustment + market_delta))
            p_under_model = 1.0 - p_over_model

        ev_over, kelly_over = _ev_and_kelly(p_over_model, _to_price(over["price"]))
        ev_under, kelly_under = _ev_and_kelly(p_under_model, _to_price(under["price"]))
        side = "none"
        best_ev: float | None = None
        best_kelly: float | None = None
        selected_price: int | None = None
        selected_book = ""
        selected_link = ""
        selected_last_update = ""
        model_p_hit: float | None = None
        fair_p_hit: float | None = None
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

        min_ev_for_line = tier_a_min_ev if tier == "A" else tier_b_min_ev
        if best_ev is None or best_ev < min_ev_for_line:
            if eligible:
                reason = "ev_below_threshold"
            eligible = False

        if eligible:
            eligible_count += 1

        target_roi = 0.03 if tier == "A" else 0.05
        play_to_decimal, play_to_american = _play_to(model_p_hit, target_roi)
        breakeven_decimal, breakeven_american = _play_to(model_p_hit, 0.0)

        fair_decimal = round((1.0 / model_p_hit), 6) if model_p_hit else None
        fair_american = _decimal_to_american(fair_decimal)

        hold_penalty = 20.0 if hold is None else hold * 100.0
        shop_value = int(over["shop_delta"]) + int(under["shop_delta"])
        score = (
            ((best_ev or -0.5) * 1000.0) + (book_count * 5.0) + (shop_value / 10.0) - hold_penalty
        )

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
                "book_pair_count": book_pair_count,
                "p_over_book_median": p_over_book_median,
                "hold_book_median": hold_book_median,
                "p_over_book_iqr": p_over_book_iqr,
                "p_over_book_range": p_over_book_range,
                "p_over_model": p_over_model,
                "p_under_model": p_under_model,
                "ev_over": ev_over,
                "ev_under": ev_under,
                "kelly_over": kelly_over,
                "kelly_under": kelly_under,
                "recommended_side": side,
                "selected_price": selected_price,
                "selected_book": selected_book,
                "selected_link": selected_link,
                "selected_last_update": selected_last_update,
                "model_p_hit": model_p_hit,
                "fair_p_hit": fair_p_hit,
                "fair_decimal": fair_decimal,
                "fair_american": fair_american,
                "edge_pct": round((best_ev or 0.0) * 100.0, 3) if best_ev is not None else None,
                "ev_per_100": round((best_ev or 0.0) * 100.0, 3) if best_ev is not None else None,
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
                "score": round(score, 6),
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

    odds_health = _odds_health(candidates, stale_quote_minutes)
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
    ranked = eligible_rows[:top_n]
    watchlist = [item for item in candidates if not item.get("eligible")][:top_n]
    top_ev_plays = [item for item in eligible_rows if item.get("tier") == "A"][:top_n]
    one_source_edges = [item for item in eligible_rows if item.get("tier") == "B"][:top_n]
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
        for item in eligible_rows[: max(top_n, 10)]
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

    return {
        "generated_at_utc": _now_utc(),
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
        },
        "candidates": candidates,
        "ranked_plays": ranked,
        "watchlist": watchlist,
        "audit": {
            "manifest_created_at_utc": manifest.get("created_at_utc", ""),
            "manifest_schema_version": manifest.get("schema_version", ""),
            "report_schema_version": 3,
            "min_ev": min_ev,
            "tier_a_min_ev": tier_a_min_ev,
            "tier_b_min_ev": tier_b_min_ev,
            "allow_tier_b": allow_tier_b,
            "market_baseline_method": baseline_method,
            "market_baseline_fallback": baseline_fallback,
            "min_book_pairs": min_book_pairs,
            "hold_cap": hold_cap,
            "p_over_iqr_cap": p_over_iqr_cap,
            "timezone": "ET",
            "audit_trail": audit_trail,
        },
    }


def render_strategy_markdown(report: dict[str, Any], top_n: int) -> str:
    """Render strategy report as an audit-ready markdown card."""
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    health = (
        report.get("health_report", {}) if isinstance(report.get("health_report"), dict) else {}
    )
    slate = (
        report.get("slate_snapshot", []) if isinstance(report.get("slate_snapshot"), list) else []
    )
    availability = (
        report.get("availability", {}) if isinstance(report.get("availability"), dict) else {}
    )
    warnings = (
        report.get("roster_status_warnings", [])
        if isinstance(report.get("roster_status_warnings"), list)
        else []
    )
    verified_players = (
        report.get("verified_players", [])
        if isinstance(report.get("verified_players"), list)
        else []
    )
    unresolved_players = (
        report.get("unresolved_players", [])
        if isinstance(report.get("unresolved_players"), list)
        else []
    )
    top_plays = (
        report.get("top_ev_plays", []) if isinstance(report.get("top_ev_plays"), list) else []
    )
    one_source = (
        report.get("one_source_edges", [])
        if isinstance(report.get("one_source_edges"), list)
        else []
    )
    sgp_candidates = (
        report.get("sgp_candidates", []) if isinstance(report.get("sgp_candidates"), list) else []
    )
    watchlist = (
        report.get("price_dependent_watchlist", [])
        if isinstance(report.get("price_dependent_watchlist"), list)
        else []
    )
    under_sweep = (
        report.get("under_sweep", {}) if isinstance(report.get("under_sweep"), dict) else {}
    )
    kelly = report.get("kelly_summary", []) if isinstance(report.get("kelly_summary"), list) else []
    gaps = report.get("gaps", []) if isinstance(report.get("gaps"), list) else []
    audit = report.get("audit", {}) if isinstance(report.get("audit"), dict) else {}
    audit_rows = audit.get("audit_trail", []) if isinstance(audit.get("audit_trail"), list) else []

    lines: list[str] = []
    lines.append("# NBA Prop EV Card")
    lines.append("")
    lines.append(f"- Modeled date: `{report.get('modeled_date_et', '')}`")
    lines.append(f"- Snapshot ID: `{report.get('snapshot_id', '')}`")
    lines.append(f"- Generated: `{report.get('generated_at_utc', '')}`")
    lines.append(f"- Strategy mode: `{report.get('strategy_mode', '')}`")
    lines.append(
        "- Strategy type: `Player props (over/under)` with Tier A dual-source "
        "and Tier B one-source policy"
    )
    lines.append("")

    lines.append("## Health Report")
    lines.append("")
    lines.append(f"- strategy_mode: `{health.get('strategy_mode', '')}`")
    gates = health.get("health_gates", []) if isinstance(health.get("health_gates"), list) else []
    lines.append(f"- health_gates: `{', '.join(gates) if gates else 'none'}`")
    feeds = health.get("feeds", {}) if isinstance(health.get("feeds"), dict) else {}
    lines.append(
        (
            "- feeds: official_injuries=`{}` secondary_injuries=`{}` roster=`{}` "
            "injuries_stale=`{}` roster_stale=`{}`"
        ).format(
            feeds.get("official_injuries", ""),
            feeds.get("secondary_injuries", ""),
            feeds.get("roster", ""),
            feeds.get("injuries_stale", ""),
            feeds.get("roster_stale", ""),
        )
    )
    odds = health.get("odds", {}) if isinstance(health.get("odds"), dict) else {}
    lines.append(
        (
            "- odds freshness: status=`{}` latest_quote_utc=`{}` "
            "age_latest_min=`{}` stale_after_min=`{}`"
        ).format(
            odds.get("status", ""),
            odds.get("latest_quote_utc", ""),
            odds.get("age_latest_min", ""),
            odds.get("stale_after_min", ""),
        )
    )
    contracts = health.get("contracts", {}) if isinstance(health.get("contracts"), dict) else {}
    props_contract = (
        contracts.get("props_rows", {}) if isinstance(contracts.get("props_rows"), dict) else {}
    )
    lines.append(
        "- contracts: props_rows=`{}` invalid_rows=`{}`".format(
            props_contract.get("row_count", 0),
            props_contract.get("invalid_count", 0),
        )
    )
    lines.append(f"- identity_map_entries: `{health.get('identity_map_entries', 0)}`")
    excluded_games = (
        health.get("excluded_games", []) if isinstance(health.get("excluded_games"), list) else []
    )
    if excluded_games:
        lines.append(f"- excluded_games: `{', '.join(excluded_games)}`")
    lines.append("")

    lines.append("## SLATE SNAPSHOT")
    lines.append("")
    if not slate:
        lines.append("- missing slate rows")
    else:
        lines.append("| Tip (ET) | Away @ Home | Spread | Total |")
        lines.append("| --- | --- | --- | --- |")
        for item in slate:
            if not isinstance(item, dict):
                continue
            lines.append(
                "| {} | {} | {} | {} |".format(
                    item.get("tip_et", ""),
                    _short_game_label(str(item.get("away_home", ""))),
                    item.get("spread", ""),
                    item.get("total", ""),
                )
            )
    lines.append("")

    lines.append("## Availability & Roster Notes")
    lines.append("")
    official = (
        availability.get("official", {}) if isinstance(availability.get("official"), dict) else {}
    )
    secondary = (
        availability.get("secondary", {}) if isinstance(availability.get("secondary"), dict) else {}
    )
    roster = availability.get("roster", {}) if isinstance(availability.get("roster"), dict) else {}
    lines.append(
        "- Official injury source: status=`{}` fetched=`{}` links=`{}` parsed_rows=`{}`".format(
            official.get("status", ""),
            official.get("fetched_at_utc", ""),
            official.get("count", 0),
            official.get("rows_count", 0),
        )
    )
    lines.append(
        "- Official injury PDF cache: status=`{}` path=`{}` parse_status=`{}`".format(
            official.get("pdf_download_status", ""),
            official.get("pdf_cached_path", ""),
            official.get("parse_status", ""),
        )
    )
    lines.append(
        "- Secondary injury source: status=`{}` fetched=`{}` rows=`{}`".format(
            secondary.get("status", ""),
            secondary.get("fetched_at_utc", ""),
            secondary.get("count", 0),
        )
    )
    lines.append(
        "- Roster source: status=`{}` fetched=`{}` team_rows=`{}`".format(
            roster.get("status", ""),
            roster.get("fetched_at_utc", ""),
            roster.get("count_teams", 0),
        )
    )

    key_injuries = (
        availability.get("key_injuries", [])
        if isinstance(availability.get("key_injuries"), list)
        else []
    )
    if key_injuries:
        lines.append("")
        lines.append("- Key injury statuses (today):")
        for row in key_injuries[:12]:
            if not isinstance(row, dict):
                continue
            lines.append(
                "  - {} ({}) status=`{}` note=`{}` update=`{}`".format(
                    row.get("player", ""),
                    row.get("team", ""),
                    row.get("status", ""),
                    row.get("note", ""),
                    row.get("date_update", ""),
                )
            )

    if warnings:
        lines.append("")
        lines.append("- Roster/Status warnings:")
        for warning in warnings[:12]:
            lines.append(f"  - {warning}")
    if unresolved_players:
        lines.append("")
        lines.append("- Unresolved player mappings:")
        for row in unresolved_players[:12]:
            if not isinstance(row, dict):
                continue
            suggestion = row.get("mapping_suggestion", {})
            if isinstance(suggestion, dict):
                suggested_team = str(suggestion.get("suggested_team", ""))
            else:
                suggested_team = ""
            lines.append(
                "  - {} ({}) status=`{}` detail=`{}` suggested_team=`{}`".format(
                    row.get("player", ""),
                    row.get("event_id", ""),
                    row.get("roster_status", ""),
                    row.get("detail", ""),
                    suggested_team,
                )
            )
    lines.append("")

    lines.append("## VERIFIED PLAYERS (TEAM CHECK)")
    lines.append("")
    if not verified_players:
        lines.append("- none")
    else:
        for row in verified_players:
            if not isinstance(row, dict):
                continue
            lines.append(
                "- {} — team=`{}` roster_status=`{}` source=`{}` link={}".format(
                    row.get("player", ""),
                    row.get("team", ""),
                    row.get("roster_status", ""),
                    row.get("verification_source", ""),
                    row.get("verification_link", ""),
                )
            )
    lines.append("")

    lines.append("## TOP EV PLAYS (RANKED)")
    lines.append("")
    if not top_plays:
        lines.append("- none")
    else:
        table_header = (
            "| Game | Player & Prop | Book/Price | p(hit) | Fair | Edge% | EV/$100 | "
            "PLAY-TO | Rationale |"
        )
        lines.append(table_header)
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for row in top_plays[:top_n]:
            if not isinstance(row, dict):
                continue
            play_to_text = (
                f"{_fmt_american(row.get('play_to_american'))} "
                f"(ROI>={((row.get('target_roi') or 0.0) * 100):.1f}%)"
            )
            lines.append(
                "| {} | {} | {} | {:.1f}% | {} / {} | {:+.2f}% | {:+.2f} | {} | {} |".format(
                    _short_game_label(str(row.get("game", ""))),
                    row.get("prop_label", ""),
                    row.get("book_price", ""),
                    (row.get("model_p_hit", 0.0) or 0.0) * 100.0,
                    f"{(row.get('fair_decimal') or 0.0):.3f}" if row.get("fair_decimal") else "",
                    _fmt_american(row.get("fair_american")),
                    row.get("edge_pct", 0.0) or 0.0,
                    row.get("ev_per_100", 0.0) or 0.0,
                    play_to_text,
                    str(row.get("rationale", "")).replace("|", "/"),
                )
            )
    lines.append("")

    lines.append("## ONE-SOURCE EDGES")
    lines.append("")
    if not one_source:
        lines.append("- none")
    else:
        table_header = (
            "| Game | Player & Prop | Book/Price | p(hit) | Fair | Edge% | EV/$100 | "
            "PLAY-TO | Rationale |"
        )
        lines.append(table_header)
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for row in one_source[:top_n]:
            if not isinstance(row, dict):
                continue
            play_to_text = (
                f"{_fmt_american(row.get('play_to_american'))} "
                f"(ROI>={((row.get('target_roi') or 0.0) * 100):.1f}%)"
            )
            lines.append(
                "| {} | {} | {} | {:.1f}% | {} / {} | {:+.2f}% | {:+.2f} | {} | {} |".format(
                    _short_game_label(str(row.get("game", ""))),
                    row.get("prop_label", ""),
                    row.get("book_price", ""),
                    (row.get("model_p_hit", 0.0) or 0.0) * 100.0,
                    f"{(row.get('fair_decimal') or 0.0):.3f}" if row.get("fair_decimal") else "",
                    _fmt_american(row.get("fair_american")),
                    row.get("edge_pct", 0.0) or 0.0,
                    row.get("ev_per_100", 0.0) or 0.0,
                    play_to_text,
                    str(row.get("rationale", "")).replace("|", "/"),
                )
            )
    lines.append("")

    lines.append("## SGP/SGPx (Correlation Haircut)")
    lines.append("")
    if not sgp_candidates:
        lines.append("- none")
    else:
        sgp_header = (
            "| Game | Legs | Independence p | Haircut | Adjusted p | Decimal | EV/$100 | "
            "1/8 Kelly |"
        )
        lines.append(sgp_header)
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        for row in sgp_candidates[:5]:
            if not isinstance(row, dict):
                continue
            legs = row.get("legs", [])
            leg_texts: list[str] = []
            if isinstance(legs, list):
                for leg in legs:
                    if not isinstance(leg, dict):
                        continue
                    leg_texts.append(
                        _prop_label(
                            str(leg.get("player", "")),
                            str(leg.get("side", "")),
                            _safe_float(leg.get("point")) or 0.0,
                            str(leg.get("market", "")),
                        )
                        + f" @{_fmt_american(leg.get('price'))}"
                    )
            lines.append(
                "| {} | {} | {:.2f}% | {:.1f}% | {:.2f}% | {} | {:+.2f} | {:.2f}% |".format(
                    _short_game_label(str(row.get("game", ""))),
                    " + ".join(leg_texts),
                    (row.get("independence_joint_p", 0.0) or 0.0) * 100.0,
                    (row.get("haircut", 0.0) or 0.0) * 100.0,
                    (row.get("adjusted_joint_p", 0.0) or 0.0) * 100.0,
                    row.get("unboosted_decimal", ""),
                    row.get("ev_per_100", 0.0) or 0.0,
                    (row.get("recommended_fractional_kelly", 0.0) or 0.0) * 100.0,
                )
            )
    lines.append("")

    lines.append("## UNDER SWEEP")
    lines.append("")
    lines.append(f"- status: `{under_sweep.get('status', '')}`")
    lines.append(f"- note: {under_sweep.get('note', '')}")
    qualified = (
        under_sweep.get("qualified", []) if isinstance(under_sweep.get("qualified"), list) else []
    )
    if qualified:
        lines.append("- Qualified unders:")
        for row in qualified[:5]:
            if not isinstance(row, dict):
                continue
            lines.append(
                "  - {} | {} | edge={:+.2f}% | play_to={}".format(
                    _short_game_label(str(row.get("game", ""))),
                    row.get("prop_label", ""),
                    row.get("edge_pct", 0.0) or 0.0,
                    _fmt_american(row.get("play_to_american")),
                )
            )
    misses = (
        under_sweep.get("closest_misses", [])
        if isinstance(under_sweep.get("closest_misses"), list)
        else []
    )
    if misses and under_sweep.get("status") != "ok":
        lines.append("- Closest under misses:")
        for row in misses[:5]:
            if not isinstance(row, dict):
                continue
            lines.append(
                "  - {} | {} | edge={:+.2f}% | play_to={} | reason={}".format(
                    _short_game_label(str(row.get("game", ""))),
                    row.get("prop_label", ""),
                    row.get("edge_pct", 0.0) or 0.0,
                    _fmt_american(row.get("play_to_american")),
                    row.get("reason", ""),
                )
            )
    lines.append("")

    lines.append("## PRICE-DEPENDENT WATCHLIST")
    lines.append("")
    if not watchlist:
        lines.append("- none")
    else:
        lines.append("| Game | Player & Prop | Current | PLAY-TO | Reason |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in watchlist[:top_n]:
            if not isinstance(row, dict):
                continue
            prop = _prop_label(
                str(row.get("player", "")),
                str(row.get("side", "")),
                _safe_float(row.get("point")) or 0.0,
                str(row.get("market", "")),
            )
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _short_game_label(str(row.get("game", ""))),
                    prop,
                    _fmt_american(row.get("current_price")),
                    _fmt_american(row.get("play_to_american")),
                    row.get("reason", ""),
                )
            )
    lines.append("")

    lines.append("## KELLY SIZING SUMMARY")
    lines.append("")
    if not kelly:
        lines.append("- none")
    else:
        lines.append("| Game | Prop | Book/Price | Full Kelly | 1/4 Kelly |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in kelly[: max(top_n, 10)]:
            if not isinstance(row, dict):
                continue
            prop = _prop_label(
                str(row.get("player", "")),
                str(row.get("side", "")),
                _safe_float(row.get("point")) or 0.0,
                str(row.get("market", "")),
            )
            full = (row.get("full_kelly", 0.0) or 0.0) * 100.0
            quarter = (row.get("quarter_kelly", 0.0) or 0.0) * 100.0
            lines.append(
                "| {} | {} | {} {} | {:.2f}% | {:.2f}% |".format(
                    _short_game_label(str(row.get("game", ""))),
                    prop,
                    row.get("book", ""),
                    _fmt_american(row.get("price")),
                    full,
                    quarter,
                )
            )
    lines.append("")

    lines.append("## AUDIT TRAIL")
    lines.append("")
    if not audit_rows:
        lines.append("- none")
    else:
        for row in audit_rows:
            if not isinstance(row, dict):
                continue
            lines.append(
                "- [{}] {} | {} | {} | {}".format(
                    row.get("category", ""),
                    row.get("label", ""),
                    row.get("url", ""),
                    row.get("timestamp_utc", ""),
                    row.get("note", ""),
                )
            )
    lines.append("")

    lines.append("## GAPS")
    lines.append("")
    if gaps:
        for gap in gaps:
            lines.append(f"- {gap}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## SUMMARY")
    lines.append("")
    lines.append(f"- events: `{summary.get('events', 0)}`")
    lines.append(f"- candidate_lines: `{summary.get('candidate_lines', 0)}`")
    lines.append(f"- tier_a_lines: `{summary.get('tier_a_lines', 0)}`")
    lines.append(f"- tier_b_lines: `{summary.get('tier_b_lines', 0)}`")
    lines.append(f"- eligible_lines: `{summary.get('eligible_lines', 0)}`")
    lines.append(f"- qualified_unders: `{summary.get('qualified_unders', 0)}`")
    lines.append(f"- under_sweep_status: `{summary.get('under_sweep_status', '')}`")
    lines.append("")
    return "\n".join(lines)


def write_strategy_reports(
    *,
    snapshot_dir: Path,
    report: dict[str, Any],
    top_n: int,
    strategy_id: str | None = None,
    write_canonical: bool = True,
) -> tuple[Path, Path]:
    """Write json and markdown strategy reports."""
    from prop_ev.strategies.base import normalize_strategy_id

    reports_dir = snapshot_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    markdown = render_strategy_markdown(report, top_n=top_n)

    canonical_json = reports_dir / "strategy-report.json"
    canonical_md = reports_dir / "strategy-report.md"
    canonical_card = reports_dir / "strategy-card.md"

    normalized = normalize_strategy_id(strategy_id) if strategy_id else ""

    def _suffix(path: Path) -> Path:
        return path.with_name(f"{path.stem}.{normalized}{path.suffix}")

    primary_json = canonical_json
    primary_md = canonical_md
    if not write_canonical:
        if not normalized:
            raise ValueError("strategy_id is required when write_canonical=false")
        primary_json = _suffix(canonical_json)
        primary_md = _suffix(canonical_md)

    def _write(json_path: Path, md_path: Path, card_path: Path) -> None:
        json_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        md_path.write_text(markdown, encoding="utf-8")
        card_path.write_text(markdown, encoding="utf-8")

    if write_canonical:
        _write(canonical_json, canonical_md, canonical_card)
    if normalized and not write_canonical:
        _write(_suffix(canonical_json), _suffix(canonical_md), _suffix(canonical_card))

    return primary_json, primary_md
