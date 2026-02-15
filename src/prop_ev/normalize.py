"""Normalization helpers for Odds API snapshots."""

from __future__ import annotations

from typing import Any

from prop_ev.quote_table import (
    QUOTE_TABLE_SCHEMA_VERSION,
    canonical_event_props_row,
    canonical_featured_odds_row,
    canonicalize_event_props_rows,
    canonicalize_featured_odds_rows,
    validate_event_props_rows,
    validate_featured_odds_rows,
)

DERIVED_SCHEMA_VERSION = QUOTE_TABLE_SCHEMA_VERSION


def _expect_dict(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be an object")
    return value


def _expect_list(value: Any, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{context} must be a list")
    return value


def normalize_featured_odds(
    payload: Any, *, snapshot_id: str, provider: str
) -> list[dict[str, Any]]:
    """Normalize featured odds endpoint response into stable rows."""
    events = _expect_list(payload, "featured_payload")
    rows: list[dict[str, Any]] = []
    for event in events:
        event_dict = _expect_dict(event, "featured_event")
        event_id = str(event_dict.get("id", ""))
        bookmakers = _expect_list(event_dict.get("bookmakers", []), "featured_event.bookmakers")
        for bookmaker in bookmakers:
            book_dict = _expect_dict(bookmaker, "featured_bookmaker")
            book_key = str(book_dict.get("key", ""))
            markets = _expect_list(book_dict.get("markets", []), "featured_bookmaker.markets")
            for market in markets:
                market_dict = _expect_dict(market, "featured_market")
                market_key = str(market_dict.get("key", ""))
                last_update = str(
                    market_dict.get("last_update") or book_dict.get("last_update") or ""
                )
                outcomes = _expect_list(market_dict.get("outcomes", []), "featured_market.outcomes")
                for outcome in outcomes:
                    outcome_dict = _expect_dict(outcome, "featured_outcome")
                    rows.append(
                        canonical_featured_odds_row(
                            provider=provider,
                            snapshot_id=snapshot_id,
                            schema_version=DERIVED_SCHEMA_VERSION,
                            game_id=event_id,
                            market=market_key,
                            book=book_key,
                            price=outcome_dict.get("price"),
                            point=outcome_dict.get("point"),
                            side=outcome_dict.get("name", ""),
                            last_update=last_update,
                        )
                    )
    canonical_rows = canonicalize_featured_odds_rows(rows)
    validate_featured_odds_rows(canonical_rows)
    return canonical_rows


def normalize_event_odds(payload: Any, *, snapshot_id: str, provider: str) -> list[dict[str, Any]]:
    """Normalize per-event odds response into stable rows."""
    event = _expect_dict(payload, "event_payload")
    event_id = str(event.get("id", ""))
    bookmakers = _expect_list(event.get("bookmakers", []), "event_payload.bookmakers")
    rows: list[dict[str, Any]] = []
    for bookmaker in bookmakers:
        book_dict = _expect_dict(bookmaker, "event_bookmaker")
        book_key = str(book_dict.get("key", ""))
        markets = _expect_list(book_dict.get("markets", []), "event_bookmaker.markets")
        for market in markets:
            market_dict = _expect_dict(market, "event_market")
            market_key = str(market_dict.get("key", ""))
            last_update = str(market_dict.get("last_update") or book_dict.get("last_update") or "")
            outcomes = _expect_list(market_dict.get("outcomes", []), "event_market.outcomes")
            for outcome in outcomes:
                outcome_dict = _expect_dict(outcome, "event_outcome")
                rows.append(
                    canonical_event_props_row(
                        provider=provider,
                        snapshot_id=snapshot_id,
                        schema_version=DERIVED_SCHEMA_VERSION,
                        event_id=event_id,
                        market=market_key,
                        player=outcome_dict.get("description", ""),
                        side=outcome_dict.get("name", ""),
                        price=outcome_dict.get("price"),
                        point=outcome_dict.get("point"),
                        book=book_key,
                        last_update=last_update,
                        link=outcome_dict.get("link", ""),
                    )
                )
    canonical_rows = canonicalize_event_props_rows(rows)
    validate_event_props_rows(canonical_rows)
    return canonical_rows
