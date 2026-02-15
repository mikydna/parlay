"""Build compact brief inputs and fallback markdown for playbook outputs."""

from __future__ import annotations

import json
import re
from typing import Any
from zoneinfo import ZoneInfo

from prop_ev.nba_data.normalize import canonical_team_name
from prop_ev.time_utils import parse_iso_z

REQUIRED_PASS1_KEYS = {
    "slate_summary",
    "top_plays_explained",
    "watchouts",
    "data_quality_flags",
    "confidence_notes",
}

REQUIRED_PASS2_HEADINGS = [
    "## Snapshot",
    "## What The Bet Is",
    "## Executive Summary",
    "## Analyst Take",
    "## Action Plan (GO / LEAN / NO-GO)",
    "## Data Quality",
    "## Confidence",
]

P_HIT_NOTES_HEADING = "### Interpreting p(hit)"

ET_ZONE = ZoneInfo("America/New_York")

MARKET_LABELS = {
    "player_points": "points",
    "player_rebounds": "rebounds",
    "player_assists": "assists",
    "player_threes": "three-pointers",
    "player_points_rebounds_assists": "points+rebounds+assists",
}

TEAM_ABBREVIATIONS = {
    "atlanta hawks": "ATL",
    "boston celtics": "BOS",
    "brooklyn nets": "BKN",
    "charlotte hornets": "CHA",
    "chicago bulls": "CHI",
    "cleveland cavaliers": "CLE",
    "dallas mavericks": "DAL",
    "denver nuggets": "DEN",
    "detroit pistons": "DET",
    "golden state warriors": "GSW",
    "houston rockets": "HOU",
    "indiana pacers": "IND",
    "los angeles clippers": "LAC",
    "los angeles lakers": "LAL",
    "memphis grizzlies": "MEM",
    "miami heat": "MIA",
    "milwaukee bucks": "MIL",
    "minnesota timberwolves": "MIN",
    "new orleans pelicans": "NOP",
    "new york knicks": "NYK",
    "oklahoma city thunder": "OKC",
    "orlando magic": "ORL",
    "philadelphia 76ers": "PHI",
    "phoenix suns": "PHX",
    "portland trail blazers": "POR",
    "sacramento kings": "SAC",
    "san antonio spurs": "SAS",
    "toronto raptors": "TOR",
    "utah jazz": "UTA",
    "washington wizards": "WAS",
}


def _to_float(value: Any) -> float | None:
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


def _format_timestamp_et(value: Any) -> str:
    raw = _to_str(value)
    parsed = parse_iso_z(raw)
    if parsed is None:
        return ""
    local = parsed.astimezone(ET_ZONE)
    return local.strftime("%Y-%m-%d %I:%M:%S %p %Z")


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _to_optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _execution_books(report: dict[str, Any]) -> list[str]:
    projection = report.get("execution_projection", {})
    if not isinstance(projection, dict):
        return []
    raw_books = projection.get("bookmakers", [])
    if not isinstance(raw_books, list):
        return []
    books: list[str] = []
    seen: set[str] = set()
    for raw in raw_books:
        name = _to_str(raw).strip().lower()
        if not name or name in seen:
            continue
        seen.add(name)
        books.append(name)
    return books


def _pick_side_fields(item: dict[str, Any]) -> dict[str, Any]:
    side = _to_str(item.get("recommended_side", "")).lower()
    if side == "under":
        return {
            "side": "under",
            "best_price": item.get("under_best_price"),
            "best_book": _to_str(item.get("under_best_book", "")),
            "best_link": _to_str(item.get("under_link", "")),
            "model_prob": _to_float(item.get("p_under_model")),
            "fair_prob": _to_float(item.get("p_under_fair")),
        }
    return {
        "side": "over",
        "best_price": item.get("over_best_price"),
        "best_book": _to_str(item.get("over_best_book", "")),
        "best_link": _to_str(item.get("over_link", "")),
        "model_prob": _to_float(item.get("p_over_model")),
        "fair_prob": _to_float(item.get("p_over_fair")),
    }


def _format_number(value: Any) -> str:
    maybe = _to_float(value)
    if maybe is None:
        return ""
    if maybe.is_integer():
        return str(int(maybe))
    return f"{maybe:.1f}"


def _format_price(value: Any) -> str:
    if isinstance(value, bool):
        return ""
    if isinstance(value, int):
        if value > 0:
            return f"+{value}"
        return str(value)
    return _to_str(value)


def _format_prob(value: Any) -> str:
    prob = _to_float(value)
    if prob is None:
        return ""
    return f"{prob * 100.0:.1f}%"


def _format_prob_with_calibration(
    *,
    conservative_probability: Any,
    calibrated_probability: Any,
    fallback_probability: Any,
) -> str:
    conservative = _to_float(conservative_probability)
    if conservative is None:
        conservative = _to_float(fallback_probability)
    calibrated = _to_float(calibrated_probability)
    conservative_text = _format_prob(conservative)
    calibrated_text = _format_prob(calibrated)
    if calibrated_text and conservative_text and calibrated_text != conservative_text:
        return f"{conservative_text} → {calibrated_text}"
    return conservative_text or calibrated_text


def _p_hit_notes_block() -> list[str]:
    return [
        P_HIT_NOTES_HEADING,
        "",
        "- `p(hit)` = estimated chance the recommended side wins at that line.",
        (
            "- When shown as `X% → Y%`, it is conservative `p(hit)` "
            "mapped through calibration history."
        ),
        "- Built from no-vig odds + small injury/roster/spread adjustments (clamped 1%-99%).",
        "- Use it to rank EV, not as a guarantee; judge it by calibration over many bets.",
        "- Can be wrong when odds are stale, coverage is thin, or minutes/role are uncertain.",
    ]


def _market_label(market: str) -> str:
    return MARKET_LABELS.get(market, market.replace("_", " "))


def _md_cell(value: Any) -> str:
    text = _to_str(value).replace("\n", " ").strip()
    return text.replace("|", "\\|")


def _split_markdown_row(line: str) -> list[str] | None:
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|")):
        return None
    placeholder = "__PIPE_PLACEHOLDER__"
    normalized = stripped.replace("\\|", placeholder)
    cells = [cell.strip().replace(placeholder, "|") for cell in normalized[1:-1].split("|")]
    if not cells:
        return None
    return cells


def _make_ticket(
    *,
    player: str,
    side: str,
    point: Any,
    market: str,
    price: Any,
    book: str,
) -> str:
    point_text = _format_number(point)
    market_text = _market_label(market)
    price_text = _format_price(price)
    side_text = side.upper() if side else "TBD"
    book_text = book or "unknown_book"
    return f"{player} {side_text} {point_text} {market_text} @ {price_text} ({book_text})"


def _action_decision(
    *,
    best_ev: Any,
    injury_status: str,
    roster_status: str,
    reason: str,
    watchlist: bool,
) -> str:
    if watchlist:
        return "NO-GO"
    if reason in {"injury_gate", "roster_gate"}:
        return "NO-GO"
    if injury_status not in {"available", "available_unlisted"}:
        return "NO-GO"
    if roster_status not in {"active", "rostered"}:
        return "NO-GO"

    ev = _to_float(best_ev)
    if ev is None:
        return "NO-GO"

    if injury_status in {"questionable", "day_to_day", "unknown"}:
        if ev >= 0.06:
            return "LEAN"
        if ev >= 0.03:
            return "LEAN"
        return "NO-GO"
    if roster_status.startswith("unknown"):
        return "LEAN" if ev >= 0.05 else "NO-GO"
    if ev >= 0.05:
        return "GO"
    if ev >= 0.02:
        return "LEAN"
    return "NO-GO"


def _is_pre_bet_ready(row: dict[str, Any]) -> bool:
    raw = row.get("pre_bet_ready")
    if isinstance(raw, bool):
        return raw
    injury_status = _to_str(row.get("injury_status", ""))
    roster_status = _to_str(row.get("roster_status", ""))
    return injury_status in {"available", "available_unlisted"} and roster_status in {
        "active",
        "rostered",
    }


def _action_rank(action: str) -> int:
    ranking = {"GO": 0, "LEAN": 1, "NO-GO": 2}
    return ranking.get(action, 3)


def _play_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _action_rank(_to_str(row.get("action_default", ""))),
        -(_to_float(row.get("best_ev")) or -999.0),
        _to_str(row.get("game", "")),
        _to_str(row.get("player", "")),
    )


def _render_action_plan_table_rows(top_plays: list[dict[str, Any]]) -> list[str]:
    if not top_plays:
        return ["- none"]

    sorted_rows = sorted(
        [row for row in top_plays if isinstance(row, dict)],
        key=_play_rank_key,
    )
    lines = [
        "| Action | Game | Tier | Ticket | p(hit) | Edge Note | Why |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in sorted_rows:
        action = _md_cell(row.get("action_default", "NO-GO"))
        game = _md_cell(row.get("game", ""))
        ticket = _md_cell(row.get("ticket", ""))
        p_hit = _md_cell(
            _format_prob_with_calibration(
                conservative_probability=row.get("p_conservative"),
                calibrated_probability=row.get("p_calibrated"),
                fallback_probability=row.get("model_prob"),
            )
            or "n/a"
        )
        edge_note = _md_cell(row.get("edge_note", "n/a"))
        confidence_tier = _to_str(row.get("confidence_tier", "")).strip().lower()
        quality_score = _format_prob(row.get("quality_score"))
        uncertainty = _format_prob(row.get("uncertainty_band"))
        confidence_tokens: list[str] = []
        if confidence_tier:
            confidence_tokens.append(f"confidence={confidence_tier}")
        if quality_score:
            confidence_tokens.append(f"quality={quality_score}")
        if uncertainty:
            confidence_tokens.append(f"uncertainty={uncertainty}")
        confidence_note = f"; {'; '.join(confidence_tokens)}" if confidence_tokens else ""
        why = _md_cell(row.get("plain_reason", ""))
        context = (
            f"{why} "
            f"(tier={_to_str(row.get('tier', ''))}; "
            f"injury={_to_str(row.get('injury_status', ''))}; "
            f"roster={_to_str(row.get('roster_status', ''))}{confidence_note})"
        )
        lines.append(
            f"| {action} | {game} | {_md_cell(row.get('tier', ''))} | {ticket} | {p_hit} | "
            f"{edge_note} | {_md_cell(context)} |"
        )
    return lines


def _is_game_card_candidate(row: dict[str, Any], *, min_ev: float) -> bool:
    action = _to_str(row.get("action_default", "NO-GO"))
    if action not in {"GO", "LEAN"}:
        return False
    ev = _to_float(row.get("best_ev"))
    if ev is None:
        return False
    return ev >= min_ev


def _edge_note(best_ev: Any, best_kelly: Any) -> str:
    ev = _to_float(best_ev)
    kelly = _to_float(best_kelly)
    if ev is None and kelly is None:
        return "n/a"

    ev_pct = 0.0 if ev is None else ev * 100.0

    if ev_pct >= 8.0:
        strength = "Strong edge"
    elif ev_pct >= 4.0:
        strength = "Moderate edge"
    elif ev_pct > 0.0:
        strength = "Thin edge"
    else:
        strength = "No edge"

    if kelly is None or kelly <= 0:
        stake = "No stake"
    elif kelly >= 0.10:
        stake = "Higher stake"
    elif kelly >= 0.05:
        stake = "Medium stake"
    else:
        stake = "Small stake"

    return f"{strength} ({ev_pct:+.1f}% est.), {stake}"


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


def _game_label(away_team: str, home_team: str) -> str:
    away = _team_abbrev(away_team)
    home = _team_abbrev(home_team)
    if away and home:
        return f"{away} @ {home}"
    return away or home or "unknown_game"


def _plain_reason(
    *,
    action: str,
    injury_status: str,
    roster_status: str,
    tier: str,
    best_ev: Any,
    reason_code: str,
) -> str:
    ev = _to_float(best_ev)
    ev_text = f"{ev:.3f}" if ev is not None else "n/a"

    if action == "NO-GO":
        if reason_code == "injury_gate" or injury_status in {"out", "out_for_season", "doubtful"}:
            return "No-go because player availability risk is too high."
        if reason_code == "roster_gate" or roster_status in {"inactive", "not_on_roster"}:
            return "No-go because roster check does not confirm active status."
        return f"No-go due to weak edge or missing confidence signals (EV {ev_text})."

    if action == "LEAN":
        if roster_status.startswith("unknown"):
            return (
                f"Lean only: pricing edge exists (EV {ev_text}) but roster verification "
                "is incomplete."
            )
        if injury_status in {"questionable", "day_to_day", "unknown"}:
            return f"Lean only: edge exists (EV {ev_text}) with non-clean injury status."
        return f"Lean: positive edge (EV {ev_text}) but not strong enough for full GO."

    tier_text = tier or "A"
    return (
        f"Go: verified availability with tier {tier_text} market depth and positive edge "
        f"(EV {ev_text})."
    )


def build_brief_input(
    report: dict[str, Any], *, top_n: int, per_game_top_n: int = 5, game_card_min_ev: float = 0.01
) -> dict[str, Any]:
    """Create compact deterministic input for LLM summarization."""
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    audit = report.get("audit", {}) if isinstance(report.get("audit"), dict) else {}
    ranked_raw = report.get("ranked_plays", [])
    ranked = ranked_raw if isinstance(ranked_raw, list) else []
    watchlist_raw = report.get("watchlist", [])
    watchlist = watchlist_raw if isinstance(watchlist_raw, list) else []
    gaps_raw = report.get("gaps", [])
    gaps = [str(item) for item in gaps_raw if isinstance(item, str)]
    tier_b_spotlight: list[dict[str, Any]] = []

    all_top_plays: list[dict[str, Any]] = []
    for item in ranked:
        if not isinstance(item, dict):
            continue
        side_fields = _pick_side_fields(item)
        player = _to_str(item.get("player", ""))
        market = _to_str(item.get("market", ""))
        reason = _to_str(item.get("reason", ""))
        injury_status = _to_str(item.get("injury_status", ""))
        roster_status = _to_str(item.get("roster_status", ""))
        ticket = _make_ticket(
            player=player,
            side=side_fields["side"],
            point=item.get("point"),
            market=market,
            price=side_fields["best_price"],
            book=side_fields["best_book"],
        )
        action_default = _action_decision(
            best_ev=item.get("best_ev"),
            injury_status=injury_status,
            roster_status=roster_status,
            reason=reason,
            watchlist=False,
        )
        home_team = _to_str(item.get("home_team", ""))
        away_team = _to_str(item.get("away_team", ""))
        all_top_plays.append(
            {
                "event_id": _to_str(item.get("event_id", "")),
                "home_team": home_team,
                "away_team": away_team,
                "game": _game_label(away_team, home_team),
                "tip_et": _to_str(item.get("tip_et", "")),
                "market": market,
                "player": player,
                "point": _to_float(item.get("point")),
                "tier": _to_str(item.get("tier", "")),
                "recommended_side": side_fields["side"],
                "best_price": side_fields["best_price"],
                "best_book": side_fields["best_book"],
                "best_link": side_fields["best_link"],
                "best_ev": _to_float(item.get("best_ev")),
                "best_kelly": _to_float(item.get("best_kelly")),
                "model_prob": side_fields["model_prob"],
                "p_conservative": (
                    _to_float(item.get("p_conservative"))
                    or _to_float(item.get("p_hit_low"))
                    or side_fields["model_prob"]
                ),
                "p_calibrated": (
                    _to_float(item.get("p_calibrated"))
                    or _to_float(item.get("p_hit_low_calibrated"))
                    or _to_float(item.get("p_hit_calibrated"))
                ),
                "fair_prob": side_fields["fair_prob"],
                "hold": _to_float(item.get("hold")),
                "quality_score": _to_float(item.get("quality_score")),
                "uncertainty_band": _to_float(item.get("uncertainty_band")),
                "confidence_tier": _to_str(item.get("confidence_tier", "")),
                "injury_status": injury_status,
                "roster_status": roster_status,
                "pre_bet_ready": _to_optional_bool(item.get("pre_bet_ready")),
                "pre_bet_reason": _to_str(item.get("pre_bet_reason", "")),
                "reason": reason,
                "bet_type": "player_prop",
                "ticket": ticket,
                "action_default": action_default,
                "edge_note": _edge_note(item.get("best_ev"), item.get("best_kelly")),
                "plain_reason": _plain_reason(
                    action=action_default,
                    injury_status=injury_status,
                    roster_status=roster_status,
                    tier=_to_str(item.get("tier", "")),
                    best_ev=item.get("best_ev"),
                    reason_code=reason,
                ),
            }
        )
        if _to_str(item.get("tier", "")) == "B":
            tier_b_spotlight.append(
                {
                    "action": action_default,
                    "game": _game_label(away_team, home_team),
                    "ticket": ticket,
                    "edge_note": _edge_note(item.get("best_ev"), item.get("best_kelly")),
                    "reason": _plain_reason(
                        action=action_default,
                        injury_status=injury_status,
                        roster_status=roster_status,
                        tier=_to_str(item.get("tier", "")),
                        best_ev=item.get("best_ev"),
                        reason_code=reason,
                    ),
                    "tier": "B",
                    "best_ev": _to_float(item.get("best_ev")),
                }
            )

    all_top_plays.sort(key=_play_rank_key)
    top_plays = all_top_plays[: max(top_n, 0)]

    watch_outs: list[dict[str, Any]] = []
    for item in watchlist:
        if not isinstance(item, dict):
            continue
        player = _to_str(item.get("player", ""))
        market = _to_str(item.get("market", ""))
        side = _to_str(item.get("recommended_side", ""))
        point = _to_float(item.get("point"))
        price = item.get("over_best_price") if side == "over" else item.get("under_best_price")
        book = (
            _to_str(item.get("over_best_book", ""))
            if side == "over"
            else _to_str(item.get("under_best_book", ""))
        )
        ticket = _make_ticket(
            player=player,
            side=side,
            point=point,
            market=market,
            price=price,
            book=book,
        )
        injury_status = _to_str(item.get("injury_status", ""))
        roster_status = _to_str(item.get("roster_status", ""))
        reason = _to_str(item.get("reason", ""))
        action_default = _action_decision(
            best_ev=item.get("best_ev"),
            injury_status=injury_status,
            roster_status=roster_status,
            reason=reason,
            watchlist=True,
        )
        home_team = _to_str(item.get("home_team", ""))
        away_team = _to_str(item.get("away_team", ""))
        watch_outs.append(
            {
                "event_id": _to_str(item.get("event_id", "")),
                "home_team": home_team,
                "away_team": away_team,
                "game": _game_label(away_team, home_team),
                "player": player,
                "market": market,
                "point": point,
                "tier": _to_str(item.get("tier", "")),
                "reason": reason,
                "best_ev": _to_float(item.get("best_ev")),
                "model_prob": (
                    _to_float(item.get("model_p_hit"))
                    or _to_float(item.get("p_conservative"))
                    or _to_float(item.get("p_hit_low"))
                ),
                "p_conservative": (
                    _to_float(item.get("p_conservative"))
                    or _to_float(item.get("p_hit_low"))
                    or _to_float(item.get("model_p_hit"))
                ),
                "p_calibrated": (
                    _to_float(item.get("p_calibrated"))
                    or _to_float(item.get("p_hit_low_calibrated"))
                    or _to_float(item.get("p_hit_calibrated"))
                ),
                "quality_score": _to_float(item.get("quality_score")),
                "uncertainty_band": _to_float(item.get("uncertainty_band")),
                "confidence_tier": _to_str(item.get("confidence_tier", "")),
                "injury_status": injury_status,
                "roster_status": roster_status,
                "pre_bet_ready": _to_optional_bool(item.get("pre_bet_ready")),
                "pre_bet_reason": _to_str(item.get("pre_bet_reason", "")),
                "bet_type": "player_prop",
                "ticket": ticket,
                "action_default": action_default,
                "edge_note": _edge_note(item.get("best_ev"), item.get("best_kelly")),
                "plain_reason": _plain_reason(
                    action=action_default,
                    injury_status=injury_status,
                    roster_status=roster_status,
                    tier=_to_str(item.get("tier", "")),
                    best_ev=item.get("best_ev"),
                    reason_code=reason,
                ),
            }
        )
        if _to_str(item.get("tier", "")) == "B":
            tier_b_spotlight.append(
                {
                    "action": action_default,
                    "game": _game_label(away_team, home_team),
                    "ticket": ticket,
                    "edge_note": _edge_note(item.get("best_ev"), item.get("best_kelly")),
                    "reason": _plain_reason(
                        action=action_default,
                        injury_status=injury_status,
                        roster_status=roster_status,
                        tier=_to_str(item.get("tier", "")),
                        best_ev=item.get("best_ev"),
                        reason_code=reason,
                    ),
                    "tier": "B",
                    "best_ev": _to_float(item.get("best_ev")),
                }
            )

    watch_outs.sort(key=_play_rank_key)
    watch_outs = watch_outs[: max(top_n, 0)]

    tier_b_spotlight.sort(
        key=lambda row: (
            _action_rank(_to_str(row.get("action", ""))),
            -(_to_float(row.get("best_ev")) or -999.0),
            row.get("game", ""),
            row.get("ticket", ""),
        )
    )
    tier_b_spotlight = tier_b_spotlight[:5]

    game_index: dict[str, dict[str, Any]] = {}
    for row in all_top_plays:
        game = _to_str(row.get("game", "unknown_game"))
        event_id = _to_str(row.get("event_id", ""))
        key = event_id or game
        if key not in game_index:
            game_index[key] = {
                "event_id": event_id,
                "game": game,
                "tip_et": _to_str(row.get("tip_et", "")),
                "top_plays": [],
            }
        game_index[key]["top_plays"].append(row)

    game_cards: list[dict[str, Any]] = []
    for page in game_index.values():
        raw_rows = [row for row in page["top_plays"] if isinstance(row, dict)]
        selected = [
            row
            for row in raw_rows
            if _is_game_card_candidate(row, min_ev=max(0.0, game_card_min_ev))
        ]
        if not selected:
            selected = [
                row for row in raw_rows if _to_str(row.get("action_default", "")) != "NO-GO"
            ]
        if not selected:
            selected = raw_rows
        selected.sort(key=_play_rank_key)
        clipped = selected[: max(per_game_top_n, 0)]
        if not clipped:
            continue
        game_cards.append(
            {
                "event_id": _to_str(page.get("event_id", "")),
                "game": _to_str(page.get("game", "unknown_game")),
                "tip_et": _to_str(page.get("tip_et", "")),
                "qualified_count": len(clipped),
                "top_plays": clipped,
            }
        )
    game_cards.sort(
        key=lambda item: (
            _to_str(item.get("tip_et", "9999-99-99 99:99 ET")),
            _to_str(item.get("game", "")),
        )
    )

    generated_at_utc = _to_str(report.get("generated_at_utc", ""))
    execution_books = _execution_books(report)

    return {
        "snapshot_id": _to_str(report.get("snapshot_id", "")),
        "strategy_status": _to_str(report.get("strategy_status", "")),
        "generated_at_utc": generated_at_utc,
        "generated_at_et": _format_timestamp_et(generated_at_utc),
        "modeled_date_et": _to_str(report.get("modeled_date_et", "")),
        "execution_books": execution_books,
        "summary": {
            "events": summary.get("events", 0),
            "candidate_lines": summary.get("candidate_lines", 0),
            "tier_a_lines": summary.get("tier_a_lines", 0),
            "tier_b_lines": summary.get("tier_b_lines", 0),
            "allow_tier_b": bool(audit.get("allow_tier_b", False)),
            "eligible_lines": summary.get("eligible_lines", 0),
            "injury_source_official": _to_str(summary.get("injury_source_official", "no")),
            "injury_source_secondary": _to_str(summary.get("injury_source_secondary", "no")),
            "roster_source": _to_str(summary.get("roster_source", "no")),
            "roster_team_rows": summary.get("roster_team_rows", 0),
            "quota": summary.get("quota", {}),
        },
        "gaps": gaps,
        "top_plays": top_plays,
        "all_top_plays": all_top_plays,
        "watchlist": watch_outs,
        "tier_b_spotlight": tier_b_spotlight,
        "game_cards": game_cards,
    }


def default_pass1(brief_input: dict[str, Any]) -> dict[str, Any]:
    """Build deterministic pass-1 analysis when LLM is unavailable."""
    summary = brief_input.get("summary", {}) if isinstance(brief_input.get("summary"), dict) else {}
    top_plays = (
        brief_input.get("top_plays", []) if isinstance(brief_input.get("top_plays"), list) else []
    )
    watchlist = (
        brief_input.get("watchlist", []) if isinstance(brief_input.get("watchlist"), list) else []
    )
    gaps = brief_input.get("gaps", []) if isinstance(brief_input.get("gaps"), list) else []

    slate_summary = ("{} games, {} candidate lines, {} eligible plays after gates.").format(
        summary.get("events", 0),
        summary.get("candidate_lines", 0),
        summary.get("eligible_lines", 0),
    )

    explained: list[dict[str, Any]] = []
    for item in top_plays:
        if not isinstance(item, dict):
            continue
        explained.append(
            {
                "game": _to_str(item.get("game", "")),
                "player": _to_str(item.get("player", "")),
                "market": _to_str(item.get("market", "")),
                "point": item.get("point"),
                "side": _to_str(item.get("recommended_side", "")),
                "best_price": item.get("best_price"),
                "best_book": _to_str(item.get("best_book", "")),
                "ev": item.get("best_ev"),
                "kelly": item.get("best_kelly"),
                "ticket": _to_str(item.get("ticket", "")),
                "action": _to_str(item.get("action_default", "NO-GO")),
                "edge_note": _to_str(item.get("edge_note", "")),
                "why": _to_str(item.get("plain_reason", "")),
            }
        )

    watchouts: list[str] = []
    for item in watchlist:
        if not isinstance(item, dict):
            continue
        watchouts.append(
            "{} {} {} ({})".format(
                _to_str(item.get("player", "")),
                _to_str(item.get("market", "")),
                _to_str(item.get("point", "")),
                _to_str(item.get("reason", "")),
            ).strip()
        )

    confidence_notes = [
        "Tier A lines are preferred over Tier B by default.",
        "Model probabilities are market-implied with injury/roster adjustments.",
    ]
    if _to_str(summary.get("roster_source", "no")) != "yes":
        confidence_notes.append("Roster verification is incomplete for this snapshot.")

    return {
        "slate_summary": slate_summary,
        "top_plays_explained": explained,
        "watchouts": watchouts,
        "data_quality_flags": [str(item) for item in gaps],
        "confidence_notes": confidence_notes,
    }


def build_pass1_prompt(brief_input: dict[str, Any]) -> str:
    """Prompt for structured analyst extraction."""
    return (
        "You are writing a strict JSON analysis for a sports betting brief. "
        "Do not fabricate books, lines, prices, injuries, or confidence statements. "
        "Only use the provided JSON payload. "
        "Return only JSON with keys: slate_summary, top_plays_explained, watchouts, "
        "data_quality_flags, confidence_notes.\n\n"
        "Rules:\n"
        "1) top_plays_explained must reference exact game, player, market, point, side, "
        "best_price.\n"
        "2) Each top play must include action as one of GO, LEAN, NO-GO.\n"
        "3) Each top play must include a plain-English why sentence for non-technical readers.\n"
        "4) data_quality_flags must include any gaps that impact trust.\n"
        "5) Keep slate_summary concise (1-2 sentences).\n"
        "6) No markdown and no prose outside JSON.\n\n"
        f"INPUT_JSON:\n{json.dumps(brief_input, sort_keys=True)}"
    )


def build_pass2_prompt(brief_input: dict[str, Any], pass1: dict[str, Any]) -> str:
    """Prompt for non-technical markdown synthesis."""
    return (
        "You are writing a one-page betting strategy brief for a non-technical reader. "
        "Use plain language and keep it concise. "
        "Do not invent any numbers or sources.\n\n"
        "Return markdown with these exact headings in order:\n"
        "## Snapshot\n"
        "## What The Bet Is\n"
        "## Executive Summary\n"
        "## Analyst Take\n"
        "## Action Plan (GO / LEAN / NO-GO)\n"
        "## Data Quality\n"
        "## Confidence\n\n"
        "Within Snapshot, show dates/times in ET (America/New_York); do not use UTC/Z.\n"
        "Within Snapshot, include execution_books when present.\n"
        "Within Action Plan, use a markdown table with columns: Action, Game, Tier, Ticket, "
        "p(hit), Edge Note, Why.\n"
        "p(hit) must come from the provided model_prob for that ticket.\n"
        "Order Action Plan by decision quality: GO first, then LEAN, then NO-GO.\n"
        "State clearly that this is player-prop over/under betting, not game winner bets.\n"
        "If data is missing or weak, state that plainly.\n\n"
        f"BRIEF_INPUT_JSON:\n{json.dumps(brief_input, sort_keys=True)}\n\n"
        f"PASS1_JSON:\n{json.dumps(pass1, sort_keys=True)}"
    )


def extract_json_object(raw_text: str) -> dict[str, Any]:
    """Parse a JSON object from model output, tolerating fenced wrappers."""
    text = raw_text.strip()
    if not text:
        return {}

    fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
    if fence_match:
        text = fence_match.group(1).strip()

    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidate = text[start : end + 1]
        try:
            payload = json.loads(candidate)
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            return {}
    return {}


def sanitize_pass1(payload: dict[str, Any], brief_input: dict[str, Any]) -> dict[str, Any]:
    """Ensure pass1 has required shape; fallback to deterministic default if needed."""
    if not isinstance(payload, dict):
        return default_pass1(brief_input)
    if not REQUIRED_PASS1_KEYS.issubset(payload.keys()):
        fallback = default_pass1(brief_input)
        for key in REQUIRED_PASS1_KEYS:
            fallback[key] = payload.get(key, fallback[key])
        return fallback
    return payload


def render_fallback_markdown(
    *,
    brief_input: dict[str, Any],
    pass1: dict[str, Any],
    source_label: str,
) -> str:
    """Render deterministic markdown when pass2 output is unavailable."""
    lines: list[str] = []
    summary = brief_input.get("summary", {}) if isinstance(brief_input.get("summary"), dict) else {}
    top_plays = (
        brief_input.get("top_plays", []) if isinstance(brief_input.get("top_plays"), list) else []
    )
    watchlist = (
        brief_input.get("watchlist", []) if isinstance(brief_input.get("watchlist"), list) else []
    )
    watchlist_count = len(watchlist)
    gaps = brief_input.get("gaps", []) if isinstance(brief_input.get("gaps"), list) else []

    lines.append("# Strategy Brief")
    lines.append("")
    lines.append("## Snapshot")
    lines.append("")
    lines.append(f"- snapshot_id: `{brief_input.get('snapshot_id', '')}`")
    modeled_date_et = _to_str(brief_input.get("modeled_date_et", "")).strip()
    if modeled_date_et:
        lines.append(f"- modeled_date_et: `{modeled_date_et}`")
    generated_at_et = _to_str(brief_input.get("generated_at_et", "")).strip()
    if generated_at_et:
        lines.append(f"- generated_at_et: `{generated_at_et}`")
    else:
        lines.append(f"- generated_at_utc: `{brief_input.get('generated_at_utc', '')}`")
    execution_books = (
        brief_input.get("execution_books", [])
        if isinstance(brief_input.get("execution_books"), list)
        else []
    )
    if execution_books:
        lines.append(f"- execution_books: `{', '.join(_to_str(book) for book in execution_books)}`")
    lines.append(f"- source: `{source_label}`")
    lines.append("")
    lines.append("## What The Bet Is")
    lines.append("")
    lines.append("- Bet type: single-player props (over/under stat lines).")
    lines.append("- Not a game-winner (moneyline) strategy.")
    lines.append("- Injury and roster checks are used as action gates.")
    lines.append("- Tier A = 2+ book quotes for a line; Tier B = a single-book quote.")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"- {pass1.get('slate_summary', '')}")
    lines.append(
        "- Eligible plays: `{}` out of `{}` candidate lines.".format(
            summary.get("eligible_lines", 0), summary.get("candidate_lines", 0)
        )
    )
    lines.append(
        "- Tier-B mode: `{}`. Candidate mix: Tier A `{}`, Tier B `{}`.".format(
            "enabled" if bool(summary.get("allow_tier_b", False)) else "disabled",
            summary.get("tier_a_lines", 0),
            summary.get("tier_b_lines", 0),
        )
    )
    lines.append(f"- Gated-out lines hidden from this brief: `{watchlist_count}`.")
    lines.append(
        f"- First-page table below shows the top `{len(top_plays)}` plays across all games."
    )
    lines.append("")
    lines.append("## Action Plan (GO / LEAN / NO-GO)")
    lines.append("")
    lines.append(f"### Top {len(top_plays)} Across All Games")
    lines.append("")
    lines.extend(_render_action_plan_table_rows(top_plays))
    lines.append("")
    lines.append("## Data Quality")
    lines.append("")
    if not gaps:
        lines.append("- no material gaps reported")
    else:
        for gap in gaps:
            lines.append(f"- {gap}")
    lines.append(
        "- `unknown_roster` means the roster feed did not return a trusted active/inactive record "
        "for that player in that game snapshot."
    )
    lines.append(
        "- `unknown_event` means event-to-team mapping was missing, so player-to-game "
        "roster checks could not be resolved."
    )
    lines.append("")
    lines.append("## Confidence")
    lines.append("")
    notes = (
        pass1.get("confidence_notes", []) if isinstance(pass1.get("confidence_notes"), list) else []
    )
    if not notes:
        lines.append("- Confidence follows deterministic gates and EV thresholds.")
    else:
        for note in notes:
            lines.append(f"- {note}")
    lines.append("")
    lines.extend(_p_hit_notes_block())
    lines.append("")
    base_markdown = "\n".join(lines)
    return append_game_cards_section(base_markdown, brief_input=brief_input)


def _render_game_cards_section(
    *, brief_input: dict[str, Any], include_pagebreak_markers: bool = True
) -> str:
    game_cards = (
        brief_input.get("game_cards", []) if isinstance(brief_input.get("game_cards"), list) else []
    )
    if not game_cards:
        return ""

    lines: list[str] = []
    if include_pagebreak_markers:
        lines.append("<!-- pagebreak -->")
        lines.append("")
    lines.append("## Game Cards by Matchup")
    lines.append("")
    for card in game_cards:
        if not isinstance(card, dict):
            continue
        game_label = _to_str(card.get("game", "unknown_game"))
        tip_et = _to_str(card.get("tip_et", ""))
        qualified_count = int(_to_float(card.get("qualified_count")) or 0)
        lines.append(f"### {game_label}")
        if tip_et:
            lines.append(f"- Tip (ET): `{tip_et}`")
        lines.append(f"- Listed plays: `{qualified_count}`")
        lines.append("")
        rows = card.get("top_plays", []) if isinstance(card.get("top_plays"), list) else []
        if not rows:
            lines.append("- none")
            lines.append("")
            continue
        lines.append("| Action | Tier | Ticket | p(hit) | Edge Note | Why |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for row in rows:
            if not isinstance(row, dict):
                continue
            lines.append(
                "| {} | {} | {} | {} | {} | {} |".format(
                    _md_cell(row.get("action_default", "NO-GO")),
                    _md_cell(row.get("tier", "")),
                    _md_cell(row.get("ticket", "")),
                    _md_cell(
                        _format_prob_with_calibration(
                            conservative_probability=row.get("p_conservative"),
                            calibrated_probability=row.get("p_calibrated"),
                            fallback_probability=row.get("model_prob"),
                        )
                        or "n/a"
                    ),
                    _md_cell(row.get("edge_note", "")),
                    _md_cell(row.get("plain_reason", "")),
                )
            )
        lines.append("")
        if include_pagebreak_markers:
            lines.append("<!-- pagebreak -->")
            lines.append("")
    return "\n".join(lines).strip()


def append_game_cards_section(markdown: str, *, brief_input: dict[str, Any]) -> str:
    """Append deterministic per-game cards (one game per page in PDF)."""
    base = markdown.rstrip()
    section_heading = "## Game Cards by Matchup"
    if section_heading in base:
        return base + "\n"

    game_cards_section = _render_game_cards_section(
        brief_input=brief_input,
        include_pagebreak_markers=True,
    )
    if not game_cards_section:
        return base + "\n"
    return base + "\n\n" + game_cards_section + "\n"


def enforce_readability_labels(markdown: str, *, top_n: int) -> str:
    """Ensure stable reader-facing labels are present in brief markdown."""
    lines = markdown.splitlines()

    def _insert_after_heading(heading: str, required_line: str) -> None:
        for idx, line in enumerate(lines):
            if line.strip() != heading:
                continue
            probe = idx + 1
            while probe < len(lines) and not lines[probe].strip():
                probe += 1
            if probe < len(lines) and lines[probe].strip() == required_line:
                return
            lines.insert(idx + 1, "")
            lines.insert(idx + 2, required_line)
            lines.insert(idx + 3, "")
            return

    _insert_after_heading(
        "## Action Plan (GO / LEAN / NO-GO)",
        f"### Top {max(0, top_n)} Across All Games",
    )

    return "\n".join(lines).rstrip() + "\n"


def _best_available_row(brief_input: dict[str, Any]) -> dict[str, Any] | None:
    rows = (
        brief_input.get("all_top_plays", [])
        if isinstance(brief_input.get("all_top_plays"), list)
        else []
    )
    if not rows:
        rows = (
            brief_input.get("top_plays", [])
            if isinstance(brief_input.get("top_plays"), list)
            else []
        )

    actionable: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        action = _to_str(row.get("action_default", "")).upper()
        if action not in {"GO", "LEAN"}:
            continue
        if not _is_pre_bet_ready(row):
            continue
        actionable.append(row)
    if not actionable:
        return None
    actionable.sort(key=_play_rank_key)
    return actionable[0]


def render_best_available_section(brief_input: dict[str, Any]) -> str:
    """Render a concise best-available recommendation block."""
    best = _best_available_row(brief_input)
    lines: list[str] = []
    lines.append("## Best Available Bet Right Now")
    lines.append("")
    if best is None:
        lines.append("- status: no actionable GO/LEAN play passed pre-bet availability checks.")
        lines.append("- action: wait for injury/roster updates, then rerun.")
        return "\n".join(lines).strip()

    action = _to_str(best.get("action_default", "LEAN")).upper()
    game = _to_str(best.get("game", ""))
    ticket = _to_str(best.get("ticket", ""))
    book = _to_str(best.get("best_book", ""))
    price = _format_price(best.get("best_price"))
    side = _to_str(best.get("recommended_side", "")).upper()
    market = _market_label(_to_str(best.get("market", "")))
    point = _format_number(best.get("point"))
    ev = _to_float(best.get("best_ev"))
    ev_text = f"{ev * 100.0:+.1f}% est." if ev is not None else "n/a"
    why = _to_str(best.get("plain_reason", "")).strip()
    caveat = (
        "no GO passed clean gates in this snapshot; this is the highest-ranked LEAN."
        if action == "LEAN"
        else "meets clean GO gates in this snapshot."
    )
    lines.append(f"- **Decision:** **{action}** ({caveat})")
    if ticket:
        lines.append(f"- **Bet:** **{ticket}**")
    lookup_parts = []
    if game:
        lookup_parts.append(f"`{game}`")
    if market and point and side:
        lookup_parts.append(f"`{side} {point} {market}`")
    elif market and point:
        lookup_parts.append(f"`{point} {market}`")
    if book or price:
        lookup_parts.append(f"`{book} {price}`".strip())
    if lookup_parts:
        lines.append("- **Lookup Line:** " + " | ".join(lookup_parts))
    lines.append(f"- **Model Edge:** `{ev_text}`")
    if why:
        lines.append(f"- why: {why}")
    return "\n".join(lines).strip()


def upsert_best_available_section(markdown: str, *, brief_input: dict[str, Any]) -> str:
    """Insert/replace Best Available section before Action Plan."""
    lines = markdown.splitlines()
    lines, _ = _extract_top_level_section(lines, "## Best Available Bet Right Now")

    action_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.strip() == "## Action Plan (GO / LEAN / NO-GO)":
            action_idx = idx
            break
    if action_idx is None:
        return markdown.rstrip() + "\n"

    block = render_best_available_section(brief_input).splitlines()
    merged = lines[:action_idx] + block + [""] + lines[action_idx:]
    return "\n".join(merged).rstrip() + "\n"


def strip_empty_go_placeholder_rows(markdown: str) -> str:
    """Remove synthetic GO placeholder rows from Action Plan tables."""
    lines = markdown.splitlines()
    action_heading = "## Action Plan (GO / LEAN / NO-GO)"
    start_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.strip() == action_heading:
            start_idx = idx
            break
    if start_idx is None:
        return markdown.rstrip() + "\n"

    end_idx = len(lines)
    for idx in range(start_idx + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            end_idx = idx
            break

    section = lines[start_idx + 1 : end_idx]
    filtered: list[str] = []
    changed = False
    for line in section:
        stripped = line.strip()
        cells = _split_markdown_row(stripped)
        if cells is not None:
            action = _to_str(cells[0]).strip().upper()
            joined = " ".join(cells).lower()
            if action == "GO":
                dash_like = {"", "-", "--", "—"}
                middle = [_to_str(cell).strip() for cell in cells[1:-1]]
                why = _to_str(cells[-1]).strip().lower() if len(cells) > 1 else ""
                placeholder = (
                    all(cell in dash_like for cell in middle)
                    and ("no plays" in why)
                    and ("go" in why)
                )
                legacy_placeholder = "no plays meet a clean go threshold" in joined
                if placeholder or legacy_placeholder:
                    changed = True
                    continue
        if stripped.startswith("(") and (
            "no go entries available from the input" in stripped.lower()
        ):
            changed = True
            continue
        if (
            stripped.lower().startswith("note:")
            and "go" in stripped.lower()
            and "none" in stripped.lower()
        ):
            changed = True
            continue
        filtered.append(line)

    if not changed:
        return markdown.rstrip() + "\n"

    normalized: list[str] = []
    for line in filtered:
        if not line.strip() and normalized and not normalized[-1].strip():
            continue
        normalized.append(line)

    merged = lines[: start_idx + 1] + normalized + lines[end_idx:]
    return "\n".join(merged).rstrip() + "\n"


def build_analyst_web_prompt(brief_input: dict[str, Any], pass1: dict[str, Any]) -> str:
    """Prompt for external evidence synthesis with web search enabled."""
    summary = brief_input.get("summary", {}) if isinstance(brief_input.get("summary"), dict) else {}
    top_rows = (
        brief_input.get("top_plays", []) if isinstance(brief_input.get("top_plays"), list) else []
    )
    compact_rows: list[dict[str, Any]] = []
    for row in top_rows[:8]:
        if not isinstance(row, dict):
            continue
        compact_rows.append(
            {
                "game": _to_str(row.get("game", "")),
                "player": _to_str(row.get("player", "")),
                "market": _to_str(row.get("market", "")),
                "point": row.get("point"),
                "side": _to_str(row.get("recommended_side", "")),
                "price": row.get("best_price"),
                "book": _to_str(row.get("best_book", "")),
                "best_ev": row.get("best_ev"),
                "action": _to_str(row.get("action_default", "")),
                "ticket": _to_str(row.get("ticket", "")),
            }
        )
    compact_payload = {
        "snapshot_id": _to_str(brief_input.get("snapshot_id", "")),
        "summary": {
            "events": summary.get("events", 0),
            "candidate_lines": summary.get("candidate_lines", 0),
            "eligible_lines": summary.get("eligible_lines", 0),
            "tier_a_lines": summary.get("tier_a_lines", 0),
            "tier_b_lines": summary.get("tier_b_lines", 0),
        },
        "gaps": brief_input.get("gaps", []) if isinstance(brief_input.get("gaps"), list) else [],
        "top_plays": compact_rows,
        "slate_summary": _to_str(pass1.get("slate_summary", "")),
    }
    return (
        "You are the analyst of record for an NBA player-prop betting brief.\n"
        "Use web search to find current evidence that supports or refutes the top model edges.\n"
        "Do not fabricate sources, injuries, or news.\n"
        "Return ONLY JSON with this schema:\n"
        "{\n"
        '  "analysis_summary": "string",\n'
        '  "supporting_facts": [{"fact":"string","source_title":"string","source_url":"string"}],\n'
        '  "refuting_facts": [{"fact":"string","source_title":"string","source_url":"string"}],\n'
        '  "bottom_line": "string"\n'
        "}\n"
        "Rules:\n"
        "1) Keep facts concrete and tied to specific players/games from input.\n"
        "2) Include at least 2 supporting and 2 refuting facts when possible.\n"
        "3) If evidence is weak/missing, say that explicitly in bottom_line.\n"
        "4) Source URLs must be absolute URLs.\n\n"
        f"COMPACT_INPUT_JSON:\n{json.dumps(compact_payload, sort_keys=True)}"
    )


def build_analyst_synthesis_prompt(
    brief_input: dict[str, Any],
    pass1: dict[str, Any],
    web_sources: list[dict[str, str]],
) -> str:
    """Prompt to synthesize analyst take from deterministic data + web source list."""
    summary = brief_input.get("summary", {}) if isinstance(brief_input.get("summary"), dict) else {}
    top_rows = (
        brief_input.get("top_plays", []) if isinstance(brief_input.get("top_plays"), list) else []
    )
    compact_rows: list[dict[str, Any]] = []
    for row in top_rows[:8]:
        if not isinstance(row, dict):
            continue
        compact_rows.append(
            {
                "game": _to_str(row.get("game", "")),
                "player": _to_str(row.get("player", "")),
                "ticket": _to_str(row.get("ticket", "")),
                "best_ev": row.get("best_ev"),
                "action": _to_str(row.get("action_default", "")),
            }
        )
    compact_payload = {
        "snapshot_id": _to_str(brief_input.get("snapshot_id", "")),
        "summary": {
            "events": summary.get("events", 0),
            "eligible_lines": summary.get("eligible_lines", 0),
            "tier_a_lines": summary.get("tier_a_lines", 0),
            "tier_b_lines": summary.get("tier_b_lines", 0),
        },
        "slate_summary": _to_str(pass1.get("slate_summary", "")),
        "top_plays": compact_rows,
        "gaps": brief_input.get("gaps", []) if isinstance(brief_input.get("gaps"), list) else [],
    }
    trimmed_sources = [row for row in web_sources if isinstance(row, dict)][:6]
    return (
        "You are writing an analyst summary for non-technical readers.\n"
        "Use ONLY the provided deterministic data and web source list.\n"
        "Do not invent source URLs or claims.\n"
        "Return ONLY JSON with this schema:\n"
        "{\n"
        '  "analysis_summary":"string",\n'
        '  "supporting_facts":[{"fact":"string","source_title":"string","source_url":"string"}],\n'
        '  "refuting_facts":[{"fact":"string","source_title":"string","source_url":"string"}],\n'
        '  "bottom_line":"string"\n'
        "}\n"
        "Rules:\n"
        "1) Supporting/refuting facts must each cite a source URL from WEB_SOURCES_JSON.\n"
        "2) Prioritize web news evidence over deterministic model restatements.\n"
        "3) Return at most 3 supporting and 3 refuting facts.\n"
        "4) Avoid using deterministic snapshot ids as source_url.\n"
        "5) If evidence quality is weak, say so in bottom_line.\n\n"
        f"DETERMINISTIC_JSON:\n{json.dumps(compact_payload, sort_keys=True)}\n\n"
        f"WEB_SOURCES_JSON:\n{json.dumps(trimmed_sources, sort_keys=True)}"
    )


def default_analyst_take(brief_input: dict[str, Any], pass1: dict[str, Any]) -> dict[str, Any]:
    """Deterministic analyst fallback when web-search pass is unavailable."""
    summary = brief_input.get("summary", {}) if isinstance(brief_input.get("summary"), dict) else {}
    top_plays = (
        brief_input.get("top_plays", []) if isinstance(brief_input.get("top_plays"), list) else []
    )
    gaps = brief_input.get("gaps", []) if isinstance(brief_input.get("gaps"), list) else []
    top_games = ", ".join(
        sorted(
            {
                _to_str(item.get("game", ""))
                for item in top_plays
                if isinstance(item, dict) and _to_str(item.get("game", ""))
            }
        )[:3]
    )
    summary_line = _to_str(pass1.get("slate_summary", ""))
    if not summary_line:
        events = summary.get("events", 0)
        eligible_lines = summary.get("eligible_lines", 0)
        summary_line = f"{events} games and {eligible_lines} eligible lines."
    return {
        "analysis_summary": (
            f"Deterministic analyst view: {summary_line} Top concentration: {top_games or 'n/a'}."
        ),
        "supporting_facts": [],
        "refuting_facts": [],
        "bottom_line": (
            "Web evidence was not available in this run. Treat this as model-only guidance and "
            "re-check injuries/rosters before placing bets."
            if gaps
            else "Web evidence was not available in this run. Treat this as model-only guidance."
        ),
    }


def _sanitize_analyst_fact_rows(rows: Any) -> list[dict[str, str]]:
    clean: list[dict[str, str]] = []
    if not isinstance(rows, list):
        return clean
    for row in rows:
        if not isinstance(row, dict):
            continue
        fact = _to_str(row.get("fact", "")).strip()
        title = _to_str(row.get("source_title", "")).strip()
        url = _to_str(row.get("source_url", "")).strip()
        if not fact:
            continue
        clean.append({"fact": fact, "source_title": title, "source_url": url})
    return clean


def sanitize_analyst_take(
    payload: dict[str, Any], *, brief_input: dict[str, Any], pass1: dict[str, Any]
) -> dict[str, Any]:
    """Sanitize web-analyst payload to a stable schema."""
    fallback = default_analyst_take(brief_input, pass1)
    if not isinstance(payload, dict):
        return fallback
    analysis_summary = _to_str(payload.get("analysis_summary", "")).strip()
    bottom_line = _to_str(payload.get("bottom_line", "")).strip()
    supporting = _sanitize_analyst_fact_rows(payload.get("supporting_facts"))
    refuting = _sanitize_analyst_fact_rows(payload.get("refuting_facts"))
    if not analysis_summary:
        analysis_summary = _to_str(fallback.get("analysis_summary", ""))
    if not bottom_line:
        bottom_line = _to_str(fallback.get("bottom_line", ""))
    return {
        "analysis_summary": analysis_summary,
        "supporting_facts": supporting,
        "refuting_facts": refuting,
        "bottom_line": bottom_line,
    }


def merge_analyst_take_sources(
    analyst_take: dict[str, Any], web_sources: list[dict[str, str]]
) -> dict[str, Any]:
    """Backfill source title/url from web tool sources when model omits them."""
    if not web_sources:
        return analyst_take
    merged = dict(analyst_take)
    by_url: dict[str, dict[str, str]] = {}
    for source in web_sources:
        if not isinstance(source, dict):
            continue
        url = _to_str(source.get("url", "")).strip()
        if not url:
            continue
        by_url[url] = {
            "source_title": _to_str(source.get("title", "")).strip(),
            "source_url": url,
        }

    def _fill(rows: Any) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            url = _to_str(row.get("source_url", "")).strip()
            title = _to_str(row.get("source_title", "")).strip()
            if url and not title and url in by_url:
                title = by_url[url]["source_title"]
            out.append(
                {
                    "fact": _to_str(row.get("fact", "")).strip(),
                    "source_title": title,
                    "source_url": url,
                }
            )
        return out

    merged["supporting_facts"] = _fill(merged.get("supporting_facts"))
    merged["refuting_facts"] = _fill(merged.get("refuting_facts"))
    return merged


def render_analyst_take_section(
    analyst_take: dict[str, Any], *, mode: str, brief_input: dict[str, Any] | None = None
) -> str:
    """Render Analyst Take section as markdown."""

    def _trim_sentences(text: str, *, max_sentences: int, max_chars: int) -> str:
        raw = text.strip()
        if not raw:
            return raw
        pieces = re.split(r"(?<=[.!?])\s+", raw)
        selected = [piece for piece in pieces if piece.strip()][:max_sentences]
        joined = " ".join(selected).strip()
        if len(joined) > max_chars:
            return joined[: max_chars - 3].rstrip() + "..."
        return joined

    def _rows(value: Any) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        if not isinstance(value, list):
            return out
        for row in value:
            if isinstance(row, dict):
                out.append(
                    {
                        "fact": _to_str(row.get("fact", "")).strip(),
                        "source_title": _to_str(row.get("source_title", "")).strip(),
                        "source_url": _to_str(row.get("source_url", "")).strip(),
                        "domain": _to_str(row.get("domain", "")).strip(),
                    }
                )
        return out

    def _source_label(row: dict[str, str]) -> str:
        title = row.get("source_title", "").strip()
        domain = row.get("domain", "").strip()
        url = row.get("source_url", "").strip()
        if not domain and url.startswith("http"):
            domain = (
                url.replace("https://", "").replace("http://", "").split("/", 1)[0].strip().lower()
            )
        if title and domain:
            if domain in title.lower():
                return title
            return f"{title} ({domain})"
        if title:
            return title
        if domain:
            return domain
        return "source"

    supporting = _rows(analyst_take.get("supporting_facts"))
    refuting = _rows(analyst_take.get("refuting_facts"))

    def _is_external_url(url: str) -> bool:
        lowered = url.strip().lower()
        return lowered.startswith("https://") or lowered.startswith("http://")

    external_supporting = [row for row in supporting if _is_external_url(row.get("source_url", ""))]
    external_refuting = [row for row in refuting if _is_external_url(row.get("source_url", ""))]
    internal_supporting = [
        row for row in supporting if not _is_external_url(row.get("source_url", ""))
    ]
    internal_refuting = [row for row in refuting if not _is_external_url(row.get("source_url", ""))]

    source_ids: dict[tuple[str, str], int] = {}
    source_rows: list[dict[str, str]] = []

    def _source_id(row: dict[str, str]) -> int:
        url = row.get("source_url", "").strip()
        label = _source_label(row)
        key = (url, label)
        if key not in source_ids:
            source_ids[key] = len(source_rows) + 1
            source_rows.append({"label": label, "url": url})
        return source_ids[key]

    lines: list[str] = []
    lines.append("## Analyst Take")
    lines.append("")
    lines.append(f"- mode: `{mode}`")
    lines.append("")
    lines.append("### Read This First")
    lines.append("")
    lines.append(
        _trim_sentences(
            _to_str(analyst_take.get("analysis_summary", "")),
            max_sentences=2,
            max_chars=420,
        )
    )
    lines.append("")
    lines.append("### News Signals")
    lines.append("")
    if not external_supporting and not external_refuting:
        lines.append("- No new external news signal was captured for this run.")
        lines.append("- Treat this as model-only and verify injury/roster updates before betting.")
    else:
        if external_supporting:
            lines.append("- Supports:")
            for row in external_supporting:
                fact = row.get("fact", "").strip()
                if not fact:
                    continue
                sid = _source_id(row)
                lines.append(f"  - [S{sid}] {fact}")
        if external_refuting:
            lines.append("- Refutes/Risks:")
            for row in external_refuting:
                fact = row.get("fact", "").strip()
                if not fact:
                    continue
                sid = _source_id(row)
                lines.append(f"  - [S{sid}] {fact}")

    lines.append("")
    lines.append("### Bottom Line")
    lines.append("")
    best_row = _best_available_row(brief_input or {})
    if best_row is not None:
        ticket = _to_str(best_row.get("ticket", "")).strip()
        side = _to_str(best_row.get("recommended_side", "")).upper()
        point = _format_number(best_row.get("point"))
        market = _market_label(_to_str(best_row.get("market", "")))
        game = _to_str(best_row.get("game", "")).strip()
        book = _to_str(best_row.get("best_book", "")).strip()
        price = _format_price(best_row.get("best_price"))
        if ticket:
            lines.append(f"- **Best Bet:** **{ticket}**")
        lookup: list[str] = []
        if game:
            lookup.append(f"`{game}`")
        if market and point and side:
            lookup.append(f"`{side} {point} {market}`")
        elif market and point:
            lookup.append(f"`{point} {market}`")
        if book or price:
            lookup.append(f"`{book} {price}`".strip())
        if lookup:
            lines.append("- **Lookup Line:** " + " | ".join(lookup))
    lines.append(
        _trim_sentences(
            _to_str(analyst_take.get("bottom_line", "")),
            max_sentences=2,
            max_chars=420,
        )
    )
    lines.append("")
    lines.append("### Source Index")
    lines.append("")
    if not source_rows:
        lines.append("- none")
    else:
        for idx, row in enumerate(source_rows, start=1):
            label = _to_str(row.get("label", "")).strip() or "source"
            lines.append(f"- [S{idx}] {label}")
    if internal_supporting or internal_refuting:
        lines.append("- Deterministic model details are summarized in Snapshot/Action Plan.")
    lines.append("- Full source URLs are stored in `brief-analyst.json` for audit/debug use.")
    return "\n".join(lines).strip()


def upsert_analyst_take_section(markdown: str, analyst_section_markdown: str) -> str:
    """Insert/replace Analyst Take section before Action Plan with page breaks."""
    lines = markdown.splitlines()
    lines, _ = _extract_top_level_section(lines, "## Pre-Bet Checklist")
    lines, _ = _extract_top_level_section(lines, "## Analyst Take")

    action_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.strip() == "## Action Plan (GO / LEAN / NO-GO)":
            action_idx = idx
            break
    if action_idx is None:
        return markdown.rstrip() + "\n"

    block = ["<!-- pagebreak -->", ""]
    block.extend(analyst_section_markdown.splitlines())
    block.extend(["", "<!-- pagebreak -->", ""])
    merged = lines[:action_idx] + block + lines[action_idx:]
    return "\n".join(merged).rstrip() + "\n"


def strip_risks_and_watchouts_section(markdown: str) -> str:
    """Remove legacy risks/watchouts section from markdown output."""
    lines = markdown.splitlines()
    while True:
        start_idx: int | None = None
        for idx, line in enumerate(lines):
            if line.strip() == "## Risks and Watchouts":
                start_idx = idx
                break
        if start_idx is None:
            break

        end_idx = len(lines)
        for idx in range(start_idx + 1, len(lines)):
            if lines[idx].strip().startswith("## "):
                end_idx = idx
                break
        del lines[start_idx:end_idx]

    return "\n".join(lines).rstrip() + "\n"


def strip_tier_b_view_section(markdown: str) -> str:
    """Remove legacy Tier B table section from markdown output."""
    lines = markdown.splitlines()
    while True:
        start_idx: int | None = None
        for idx, line in enumerate(lines):
            if line.strip() == "## Tier B View (Single-Book Lines)":
                start_idx = idx
                break
        if start_idx is None:
            break

        end_idx = len(lines)
        for idx in range(start_idx + 1, len(lines)):
            if lines[idx].strip().startswith("## "):
                end_idx = idx
                break
        del lines[start_idx:end_idx]

    return "\n".join(lines).rstrip() + "\n"


def _extract_top_level_section(lines: list[str], heading: str) -> tuple[list[str], list[str]]:
    start_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.strip() == heading:
            start_idx = idx
            break
    if start_idx is None:
        return lines, []

    end_idx = len(lines)
    for idx in range(start_idx + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            end_idx = idx
            break

    trim_idx = end_idx
    while trim_idx > start_idx and not lines[trim_idx - 1].strip():
        trim_idx -= 1
    if trim_idx > start_idx and lines[trim_idx - 1].strip() == "<!-- pagebreak -->":
        trim_idx -= 1
        while trim_idx > start_idx and not lines[trim_idx - 1].strip():
            trim_idx -= 1

    section = lines[start_idx:trim_idx]
    remaining = lines[:start_idx] + lines[trim_idx:]
    return remaining, section


def _ensure_pagebreak_before_heading(lines: list[str], heading: str) -> list[str]:
    target_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.strip() == heading:
            target_idx = idx
            break
    if target_idx is None:
        return lines

    probe = target_idx - 1
    while probe >= 0 and not lines[probe].strip():
        probe -= 1
    if probe >= 0 and lines[probe].strip() == "<!-- pagebreak -->":
        return lines

    return lines[:target_idx] + ["<!-- pagebreak -->", ""] + lines[target_idx:]


def ensure_pagebreak_before_action_plan(markdown: str) -> str:
    """Guarantee a page break immediately before the Action Plan heading."""
    lines = markdown.splitlines()
    lines = _ensure_pagebreak_before_heading(lines, "## Action Plan (GO / LEAN / NO-GO)")
    return "\n".join(lines).rstrip() + "\n"


def move_disclosures_to_end(markdown: str) -> str:
    """Move Data Quality/Confidence to the end with a page break before disclosures."""
    lines = markdown.splitlines()
    lines, data_quality = _extract_top_level_section(lines, "## Data Quality")
    lines, confidence = _extract_top_level_section(lines, "## Confidence")
    lines = _ensure_pagebreak_before_heading(lines, "## Game Cards by Matchup")
    if not data_quality and not confidence:
        return markdown.rstrip() + "\n"

    out: list[str] = list(lines)
    while out and not out[-1].strip():
        out.pop()
    last_nonempty = out[-1].strip() if out else ""
    if last_nonempty != "<!-- pagebreak -->":
        out.append("")
        out.append("<!-- pagebreak -->")
        out.append("")
    else:
        out.append("")

    if data_quality:
        out.extend(data_quality)
        if confidence:
            out.append("")
    if confidence:
        out.extend(confidence)

    return "\n".join(out).rstrip() + "\n"


def upsert_action_plan_table(markdown: str, *, brief_input: dict[str, Any], top_n: int) -> str:
    """Replace the Action Plan table with a deterministic, sorted table."""
    lines = markdown.splitlines()
    action_heading = "## Action Plan (GO / LEAN / NO-GO)"

    action_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.strip() == action_heading:
            action_idx = idx
            break
    if action_idx is None:
        return markdown.rstrip() + "\n"

    section_end = len(lines)
    for idx in range(action_idx + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            section_end = idx
            break

    # Find the Top-N label if present; otherwise insert immediately after the Action Plan heading.
    top_label = f"### Top {max(0, top_n)} Across All Games"
    top_idx: int | None = None
    for idx in range(action_idx + 1, section_end):
        if lines[idx].strip() == top_label:
            top_idx = idx
            break
    if top_idx is None:
        for idx in range(action_idx + 1, section_end):
            if lines[idx].strip().startswith("### Top ") and " Across All Games" in lines[idx]:
                top_idx = idx
                break
    if top_idx is None:
        top_idx = action_idx

    search_start = top_idx + 1
    search_end = section_end
    for idx in range(search_start, section_end):
        stripped = lines[idx].strip()
        if stripped.startswith("## ") or stripped.startswith("### "):
            search_end = idx
            break

    table_start: int | None = None
    for idx in range(search_start, search_end):
        if _split_markdown_row(lines[idx]) is not None:
            table_start = idx
            break

    if table_start is None:
        replace_start = search_end
        replace_end = search_end
    else:
        replace_start = table_start
        table_end = table_start
        while table_end < search_end and _split_markdown_row(lines[table_end]) is not None:
            table_end += 1
        replace_end = table_end

    top_plays = (
        brief_input.get("top_plays", []) if isinstance(brief_input.get("top_plays"), list) else []
    )
    clipped = top_plays[: max(0, int(top_n))]
    table_lines = _render_action_plan_table_rows([row for row in clipped if isinstance(row, dict)])

    block: list[str] = []
    if replace_start <= len(lines) and (replace_start == 0 or lines[replace_start - 1].strip()):
        block.append("")
    block.extend(table_lines)
    if replace_end >= len(lines) or lines[replace_end].strip():
        block.append("")

    merged = lines[:replace_start] + block + lines[replace_end:]
    return "\n".join(merged).rstrip() + "\n"


def enforce_p_hit_notes(markdown: str) -> str:
    """Ensure Confidence includes a short explainer for p(hit)."""
    lines = markdown.splitlines()
    confidence_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.strip() == "## Confidence":
            confidence_idx = idx
            break
    if confidence_idx is None:
        return markdown.rstrip() + "\n"

    section_end = len(lines)
    for idx in range(confidence_idx + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            section_end = idx
            break

    section = lines[confidence_idx + 1 : section_end]
    if any(row.strip() == P_HIT_NOTES_HEADING for row in section):
        return markdown.rstrip() + "\n"

    insert_at = section_end
    while insert_at > confidence_idx + 1 and not lines[insert_at - 1].strip():
        insert_at -= 1

    block: list[str] = []
    if insert_at > confidence_idx + 1 and lines[insert_at - 1].strip():
        block.append("")
    block.extend(_p_hit_notes_block())
    block.append("")
    merged = lines[:insert_at] + block + lines[insert_at:]
    return "\n".join(merged).rstrip() + "\n"


def enforce_snapshot_mode_labels(
    markdown: str, *, llm_pass1_status: str, llm_pass2_status: str
) -> str:
    """Clarify source/scoring/narrative modes inside Snapshot section."""
    lines = markdown.splitlines()
    snapshot_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.strip() == "## Snapshot":
            snapshot_idx = idx
            break
    if snapshot_idx is None:
        return markdown.rstrip() + "\n"

    section_end = len(lines)
    for idx in range(snapshot_idx + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            section_end = idx
            break

    section = lines[snapshot_idx + 1 : section_end]
    filtered: list[str] = []
    for row in section:
        stripped = row.strip()
        if stripped.startswith("- source:"):
            continue
        if stripped.startswith("- scoring:"):
            continue
        if stripped.startswith("- narrative:"):
            continue
        filtered.append(row)

    narrative_mode = "llm" if llm_pass2_status == "ok" else "deterministic_fallback"
    source_lines = [
        "- source_data: `snapshot_inputs`",
        "- scoring: `deterministic`",
        f"- narrative: `{narrative_mode}`",
    ]
    if llm_pass1_status and llm_pass1_status != "ok":
        source_lines.append(f"- llm_pass1_status: `{llm_pass1_status}`")
    if llm_pass2_status and llm_pass2_status != "ok":
        source_lines.append(f"- llm_pass2_status: `{llm_pass2_status}`")

    insert_at = 0
    for idx, row in enumerate(filtered):
        stripped = row.strip()
        if stripped.startswith("- snapshot_id:") or stripped.startswith("- generated_at_utc:"):
            insert_at = idx + 1
    new_section = filtered[:insert_at] + source_lines + filtered[insert_at:]
    while len(new_section) >= 2 and not new_section[-1].strip() and not new_section[-2].strip():
        new_section.pop()

    merged = lines[: snapshot_idx + 1] + new_section + lines[section_end:]
    return "\n".join(merged).rstrip() + "\n"


def enforce_snapshot_dates_et(markdown: str, *, brief_input: dict[str, Any]) -> str:
    """Ensure Snapshot uses ET date/time labels and removes UTC/Z narrative lines."""
    lines = markdown.splitlines()
    snapshot_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.strip() == "## Snapshot":
            snapshot_idx = idx
            break
    if snapshot_idx is None:
        return markdown.rstrip() + "\n"

    section_end = len(lines)
    for idx in range(snapshot_idx + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            section_end = idx
            break

    section = lines[snapshot_idx + 1 : section_end]
    filtered: list[str] = []
    for row in section:
        stripped = row.strip()
        if stripped.startswith(
            ("- snapshot_id:", "- modeled_date_et:", "- generated_at_et:", "- execution_books:")
        ):
            continue
        if stripped.startswith("- generated_at_utc:"):
            continue
        if not stripped.startswith("-"):
            lower = stripped.lower()
            if lower.startswith("date:"):
                continue
            if "generated" in lower and ("utc" in lower or "z" in stripped):
                continue
        filtered.append(row)

    snapshot_id = _to_str(brief_input.get("snapshot_id", "")).strip()
    modeled_date_et = _to_str(brief_input.get("modeled_date_et", "")).strip()
    generated_at_et = _to_str(brief_input.get("generated_at_et", "")).strip()
    generated_at_utc = _to_str(brief_input.get("generated_at_utc", "")).strip()
    execution_books = (
        brief_input.get("execution_books", [])
        if isinstance(brief_input.get("execution_books"), list)
        else []
    )

    meta: list[str] = []
    if snapshot_id:
        meta.append(f"- snapshot_id: `{snapshot_id}`")
    if modeled_date_et:
        meta.append(f"- modeled_date_et: `{modeled_date_et}`")
    if generated_at_et:
        meta.append(f"- generated_at_et: `{generated_at_et}`")
    elif generated_at_utc:
        meta.append(f"- generated_at_utc: `{generated_at_utc}`")
    if execution_books:
        meta.append(f"- execution_books: `{', '.join(_to_str(book) for book in execution_books)}`")

    if not meta:
        return markdown.rstrip() + "\n"

    while filtered and not filtered[0].strip():
        filtered.pop(0)

    new_section = meta[:]
    new_section.extend(filtered)
    merged = lines[: snapshot_idx + 1] + new_section + lines[section_end:]
    return "\n".join(merged).rstrip() + "\n"


def normalize_pass2_markdown(pass2_text: str, fallback_markdown: str) -> str:
    """Accept pass2 markdown only if required headings exist."""
    text = pass2_text.strip()
    if not text:
        return fallback_markdown
    if all(heading in text for heading in REQUIRED_PASS2_HEADINGS):
        return text if text.endswith("\n") else text + "\n"
    return fallback_markdown
