"""Project strategy outputs into execution-bookmaker views."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from prop_ev.nba_data.normalize import normalize_person_name
from prop_ev.odds_math import american_to_decimal, ev_from_prob_and_price
from prop_ev.time_utils import utc_now_str
from prop_ev.util.parsing import safe_float as _to_float
from prop_ev.util.parsing import to_price as _to_price


@dataclass(frozen=True)
class ExecutionProjectionConfig:
    """Execution-specific filtering and threshold controls."""

    bookmakers: tuple[str, ...]
    top_n: int
    requires_pre_bet_ready: bool
    requires_meets_play_to: bool
    tier_a_min_ev: float
    tier_b_min_ev: float


def _normalize_side(raw: Any) -> str:
    side = str(raw).strip().lower()
    if side in {"over", "o"}:
        return "over"
    if side in {"under", "u"}:
        return "under"
    return ""


def _normalize_point(value: Any) -> float | None:
    parsed = _to_float(value)
    if parsed is None:
        return None
    return round(parsed, 6)


def _fmt_american(value: Any) -> str:
    parsed = _to_price(value)
    if parsed is None:
        return ""
    return f"+{parsed}" if parsed > 0 else str(parsed)


def _ev_and_kelly(
    probability: float | None,
    american_price: int | None,
) -> tuple[float | None, float | None]:
    ev = ev_from_prob_and_price(probability, american_price)
    if ev is None:
        return None, None
    decimal_odds = american_to_decimal(american_price)
    if decimal_odds is None:
        return None, None
    profit_if_win = decimal_odds - 1.0
    if profit_if_win <= 0:
        return None, None
    kelly = ev / profit_if_win
    return round(ev, 6), round(kelly, 6)


def _bookkeepers(raw: tuple[str, ...]) -> tuple[str, ...]:
    books: list[str] = []
    seen: set[str] = set()
    for book in raw:
        cleaned = str(book).strip().lower()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        books.append(cleaned)
    return tuple(books)


def _quote_rank(quote: dict[str, Any], *, priority: dict[str, int]) -> tuple[int, int, str, str]:
    price = _to_price(quote.get("price")) or -10_000
    book = str(quote.get("book", "")).strip().lower()
    book_priority = -priority.get(book, 1_000_000)
    last_update = str(quote.get("last_update", "")).strip()
    return (price, book_priority, last_update, book)


def _candidate_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        not bool(row.get("eligible")),
        -(_to_float(row.get("best_ev")) or -999.0),
        -(_to_float(row.get("score")) or -9999.0),
        str(row.get("event_id", "")),
        str(row.get("player", "")),
        _to_float(row.get("point")) or 0.0,
    )


def _projection_metadata(
    config: ExecutionProjectionConfig,
    *,
    candidate_count: int,
    eligible_count: int,
) -> dict[str, Any]:
    return {
        "generated_at_utc": utc_now_str(),
        "bookmakers": list(config.bookmakers),
        "top_n": int(config.top_n),
        "requires_pre_bet_ready": bool(config.requires_pre_bet_ready),
        "requires_meets_play_to": bool(config.requires_meets_play_to),
        "tier_a_min_ev": float(config.tier_a_min_ev),
        "tier_b_min_ev": float(config.tier_b_min_ev),
        "candidate_count": candidate_count,
        "eligible_count": eligible_count,
    }


def project_execution_report(
    report: dict[str, Any],
    event_prop_rows: list[dict[str, Any]],
    config: ExecutionProjectionConfig,
) -> dict[str, Any]:
    """Project one modeled strategy report into execution-bookmaker outputs."""
    projected = deepcopy(report)
    candidates_raw = projected.get("candidates", [])
    candidates = [dict(row) for row in candidates_raw if isinstance(row, dict)]

    books = _bookkeepers(config.bookmakers)
    book_priority = {book: idx for idx, book in enumerate(books)}
    quotes: dict[tuple[str, str, str, float, str], dict[str, Any]] = {}
    allowed = set(books)
    for row in event_prop_rows:
        if not isinstance(row, dict):
            continue
        book = str(row.get("book", "")).strip().lower()
        if not book or book not in allowed:
            continue
        event_id = str(row.get("event_id", "")).strip()
        player_norm = normalize_person_name(str(row.get("player", "")))
        market = str(row.get("market", "")).strip()
        point = _normalize_point(row.get("point"))
        side = _normalize_side(row.get("side"))
        price = _to_price(row.get("price"))
        if (
            not event_id
            or not player_norm
            or not market
            or point is None
            or not side
            or price is None
        ):
            continue
        key = (event_id, player_norm, market, point, side)
        candidate_quote = {
            "book": str(row.get("book", "")).strip(),
            "price": price,
            "link": str(row.get("link", "")).strip(),
            "last_update": str(row.get("last_update", "")).strip(),
        }
        current = quotes.get(key)
        if current is None or _quote_rank(candidate_quote, priority=book_priority) > _quote_rank(
            current,
            priority=book_priority,
        ):
            quotes[key] = candidate_quote

    for row in candidates:
        event_id = str(row.get("event_id", "")).strip()
        player_norm = normalize_person_name(str(row.get("player", "")))
        market = str(row.get("market", "")).strip()
        point = _normalize_point(row.get("point"))
        side = _normalize_side(row.get("recommended_side"))
        execution_quote = None
        if event_id and player_norm and market and point is not None and side:
            quote_key = (event_id, player_norm, market, point, side)
            execution_quote = quotes.get(quote_key)
        selected_book = str(execution_quote.get("book", "")).strip() if execution_quote else ""
        selected_price = _to_price(execution_quote.get("price")) if execution_quote else None
        selected_link = str(execution_quote.get("link", "")).strip() if execution_quote else ""
        selected_last_update = (
            str(execution_quote.get("last_update", "")).strip() if execution_quote else ""
        )

        row["selected_book"] = selected_book
        row["selected_price"] = selected_price
        row["selected_link"] = selected_link
        row["selected_last_update"] = selected_last_update

        if side == "over":
            row["over_best_book"] = selected_book
            row["over_best_price"] = selected_price
            row["over_link"] = selected_link
            row["over_last_update"] = selected_last_update
        elif side == "under":
            row["under_best_book"] = selected_book
            row["under_best_price"] = selected_price
            row["under_link"] = selected_link
            row["under_last_update"] = selected_last_update

        probability = _to_float(row.get("model_p_hit"))
        best_ev, best_kelly = _ev_and_kelly(probability, selected_price)
        row["best_ev"] = best_ev
        row["best_kelly"] = best_kelly
        row["full_kelly"] = best_kelly
        row["quarter_kelly"] = round(best_kelly / 4.0, 6) if best_kelly is not None else None
        row["edge_pct"] = round((best_ev or 0.0) * 100.0, 3) if best_ev is not None else None
        row["ev_per_100"] = round((best_ev or 0.0) * 100.0, 3) if best_ev is not None else None
        row["book_price"] = f"{selected_book} {_fmt_american(selected_price)}".strip()

        hold = _to_float(row.get("hold"))
        hold_penalty = 20.0 if hold is None else hold * 100.0
        shop_value = int(_to_float(row.get("over_shop_delta")) or 0.0) + int(
            _to_float(row.get("under_shop_delta")) or 0.0
        )
        book_count = int(_to_float(row.get("book_count")) or 0.0)
        score = (
            ((best_ev or -0.5) * 1000.0) + (book_count * 5.0) + (shop_value / 10.0) - hold_penalty
        )
        row["score"] = round(score, 6)

        tier = str(row.get("tier", "")).strip().upper()
        tier_min_ev = config.tier_a_min_ev if tier == "A" else config.tier_b_min_ev
        meets_pre_bet = bool(row.get("pre_bet_ready"))
        play_to_american = _to_price(row.get("play_to_american"))

        eligible = True
        reason = ""
        if selected_price is None:
            eligible = False
            reason = "execution_price_missing"
        elif probability is None:
            eligible = False
            reason = "execution_probability_missing"
        elif config.requires_pre_bet_ready and not meets_pre_bet:
            eligible = False
            reason = "execution_pre_bet_not_ready"
        elif config.requires_meets_play_to and (
            play_to_american is None or selected_price < play_to_american
        ):
            eligible = False
            reason = "execution_play_to_miss"
        elif best_ev is None or best_ev < tier_min_ev:
            eligible = False
            reason = "execution_ev_below_threshold"

        row["eligible"] = eligible
        row["reason"] = reason

    candidates.sort(key=_candidate_sort_key)
    projected["candidates"] = candidates

    top_n = max(0, int(config.top_n))
    eligible_rows = [row for row in candidates if bool(row.get("eligible"))]
    watchlist_rows = [row for row in candidates if not bool(row.get("eligible"))]
    ranked_plays = eligible_rows[:top_n]
    top_ev_plays = [row for row in eligible_rows if str(row.get("tier", "")) == "A"][:top_n]
    one_source_edges = [row for row in eligible_rows if str(row.get("tier", "")) == "B"][:top_n]
    watchlist = watchlist_rows[:top_n]

    qualified_unders = [
        row for row in eligible_rows if _normalize_side(row.get("recommended_side")) == "under"
    ]
    closest_under_misses = [
        row for row in watchlist_rows if _normalize_side(row.get("recommended_side")) == "under"
    ]
    closest_under_misses.sort(key=lambda row: -(_to_float(row.get("best_ev")) or -999.0))
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

    price_dependent_watchlist: list[dict[str, Any]] = []
    for row in watchlist_rows:
        if str(row.get("reason", "")) != "execution_ev_below_threshold":
            continue
        play_to = row.get("play_to_american")
        if play_to is None:
            continue
        price_dependent_watchlist.append(
            {
                "event_id": row.get("event_id", ""),
                "game": row.get("game", ""),
                "player": row.get("player", ""),
                "market": row.get("market", ""),
                "point": row.get("point", 0.0),
                "side": row.get("recommended_side", ""),
                "current_price": row.get("selected_price"),
                "play_to_american": play_to,
                "play_to_decimal": row.get("play_to_decimal"),
                "target_roi": row.get("target_roi"),
                "best_ev": row.get("best_ev"),
                "reason": row.get("reason", ""),
                "tier": row.get("tier", ""),
            }
        )

    kelly_summary = [
        {
            "event_id": row.get("event_id", ""),
            "game": row.get("game", ""),
            "player": row.get("player", ""),
            "market": row.get("market", ""),
            "point": row.get("point", 0.0),
            "side": row.get("recommended_side", ""),
            "book": row.get("selected_book", ""),
            "price": row.get("selected_price"),
            "full_kelly": row.get("full_kelly"),
            "quarter_kelly": row.get("quarter_kelly"),
        }
        for row in eligible_rows[: max(top_n, 10)]
    ]

    projected["ranked_plays"] = ranked_plays
    projected["top_ev_plays"] = top_ev_plays
    projected["one_source_edges"] = one_source_edges
    projected["watchlist"] = watchlist
    projected["under_sweep"] = under_sweep
    projected["price_dependent_watchlist"] = price_dependent_watchlist
    projected["kelly_summary"] = kelly_summary

    strategy_mode = "full_board" if eligible_rows else "watchlist_only"
    projected["strategy_mode"] = strategy_mode

    summary = projected.get("summary")
    if not isinstance(summary, dict):
        summary = {}
    summary.update(
        {
            "events": len(
                {str(row.get("event_id", "")) for row in candidates if str(row.get("event_id", ""))}
            ),
            "candidate_lines": len(candidates),
            "tier_a_lines": len([row for row in candidates if str(row.get("tier", "")) == "A"]),
            "tier_b_lines": len([row for row in candidates if str(row.get("tier", "")) == "B"]),
            "eligible_lines": len(eligible_rows),
            "strategy_mode": strategy_mode,
            "watchlist_only": "yes" if strategy_mode == "watchlist_only" else "no",
            "eligible_tier_a": len(
                [row for row in eligible_rows if str(row.get("tier", "")) == "A"]
            ),
            "eligible_tier_b": len(
                [row for row in eligible_rows if str(row.get("tier", "")) == "B"]
            ),
            "eligible_pre_bet_ready": len(
                [row for row in eligible_rows if bool(row.get("pre_bet_ready"))]
            ),
            "qualified_unders": len(qualified_unders),
            "under_sweep_status": under_sweep.get("status", ""),
        }
    )
    projected["summary"] = summary

    health_report = projected.get("health_report")
    if isinstance(health_report, dict):
        health_report["strategy_mode"] = strategy_mode
        projected["health_report"] = health_report

    metadata = _projection_metadata(
        config,
        candidate_count=len(candidates),
        eligible_count=len(eligible_rows),
    )
    projected["execution_projection"] = metadata
    audit = projected.get("audit")
    if not isinstance(audit, dict):
        audit = {}
    audit["execution_projection"] = metadata
    projected["audit"] = audit

    return projected
