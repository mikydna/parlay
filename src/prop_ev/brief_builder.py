"""Build compact brief inputs and fallback markdown for playbook outputs."""

from __future__ import annotations

import json
import re
from typing import Any

from prop_ev.context_sources import canonical_team_name

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
    "## Action Plan (GO / LEAN / NO-GO)",
    "## Data Quality",
    "## Confidence",
]

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


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


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


def _market_label(market: str) -> str:
    return MARKET_LABELS.get(market, market.replace("_", " "))


def _md_cell(value: Any) -> str:
    text = _to_str(value).replace("\n", " ").strip()
    return text.replace("|", "\\|")


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
    if injury_status in {"out", "out_for_season", "doubtful"}:
        return "NO-GO"
    if roster_status in {"inactive", "not_on_roster"}:
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
                "fair_prob": side_fields["fair_prob"],
                "hold": _to_float(item.get("hold")),
                "injury_status": injury_status,
                "roster_status": roster_status,
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
                "injury_status": injury_status,
                "roster_status": roster_status,
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

    return {
        "snapshot_id": _to_str(report.get("snapshot_id", "")),
        "strategy_status": _to_str(report.get("strategy_status", "")),
        "generated_at_utc": _to_str(report.get("generated_at_utc", "")),
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
        "## Action Plan (GO / LEAN / NO-GO)\n"
        "## Data Quality\n"
        "## Confidence\n\n"
        "Within Action Plan, use a markdown table with columns: Action, Game, Tier, Ticket, "
        "Edge Note, Why.\n"
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
    lines.append(f"- generated_at_utc: `{brief_input.get('generated_at_utc', '')}`")
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
    if not top_plays:
        lines.append("- none")
    else:
        lines.append("| Action | Game | Tier | Ticket | Edge Note | Why |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for row in top_plays:
            if not isinstance(row, dict):
                continue
            action = _md_cell(row.get("action_default", "NO-GO"))
            game = _md_cell(row.get("game", ""))
            ticket = _md_cell(row.get("ticket", ""))
            edge_note = _md_cell(row.get("edge_note", "n/a"))
            why = _md_cell(row.get("plain_reason", ""))
            context = (
                f"{why} "
                f"(tier={_to_str(row.get('tier', ''))}; "
                f"injury={_to_str(row.get('injury_status', ''))}; "
                f"roster={_to_str(row.get('roster_status', ''))})"
            )
            lines.append(
                f"| {action} | {game} | {_md_cell(row.get('tier', ''))} | {ticket} | {edge_note} | "
                f"{_md_cell(context)} |"
            )
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
        lines.append("| Action | Tier | Ticket | Edge Note | Why |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in rows:
            if not isinstance(row, dict):
                continue
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _md_cell(row.get("action_default", "NO-GO")),
                    _md_cell(row.get("tier", "")),
                    _md_cell(row.get("ticket", "")),
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


def normalize_pass2_markdown(pass2_text: str, fallback_markdown: str) -> str:
    """Accept pass2 markdown only if required headings exist."""
    text = pass2_text.strip()
    if not text:
        return fallback_markdown
    if all(heading in text for heading in REQUIRED_PASS2_HEADINGS):
        return text if text.endswith("\n") else text + "\n"
    return fallback_markdown
