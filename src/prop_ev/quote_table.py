"""Canonical quote-table contracts for derived odds artifacts."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

QUOTE_TABLE_SCHEMA_VERSION = 1

EVENT_PROPS_TABLE = "event_props"
FEATURED_ODDS_TABLE = "featured_odds"

EVENT_PROPS_COLUMNS: tuple[str, ...] = (
    "provider",
    "snapshot_id",
    "schema_version",
    "event_id",
    "market",
    "player",
    "side",
    "price",
    "point",
    "book",
    "last_update",
    "link",
)
FEATURED_ODDS_COLUMNS: tuple[str, ...] = (
    "provider",
    "snapshot_id",
    "schema_version",
    "game_id",
    "market",
    "book",
    "price",
    "point",
    "side",
    "last_update",
)

EVENT_PROPS_IDENTITY_COLUMNS: tuple[str, ...] = (
    "event_id",
    "player",
    "market",
    "point",
    "side",
    "book",
)
FEATURED_ODDS_IDENTITY_COLUMNS: tuple[str, ...] = ("game_id", "market", "point", "side", "book")

EVENT_PROPS_SORT_COLUMNS: tuple[str, ...] = (
    *EVENT_PROPS_IDENTITY_COLUMNS,
    "price",
    "last_update",
)
FEATURED_ODDS_SORT_COLUMNS: tuple[str, ...] = (
    *FEATURED_ODDS_IDENTITY_COLUMNS,
    "price",
    "last_update",
)


class QuoteTableContractError(ValueError):
    """Raised when rows fail canonical quote-table validation."""


def _text(value: Any) -> str:
    return str(value).strip()


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    else:
        raw = _text(value)
        if not raw:
            return None
        try:
            parsed = float(raw)
        except ValueError:
            return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _float_sort_token(value: Any) -> str:
    parsed = _float_or_none(value)
    return "" if parsed is None else f"{parsed:.12g}"


def _schema_version_value(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return QUOTE_TABLE_SCHEMA_VERSION
    return parsed if parsed > 0 else QUOTE_TABLE_SCHEMA_VERSION


def _event_props_sort_key(row: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        _text(row.get("event_id", "")),
        _text(row.get("player", "")),
        _text(row.get("market", "")),
        _float_sort_token(row.get("point")),
        _text(row.get("side", "")),
        _text(row.get("book", "")),
        _float_sort_token(row.get("price")),
        _text(row.get("last_update", "")),
    )


def _featured_odds_sort_key(row: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        _text(row.get("game_id", "")),
        _text(row.get("market", "")),
        _float_sort_token(row.get("point")),
        _text(row.get("side", "")),
        _text(row.get("book", "")),
        _float_sort_token(row.get("price")),
        _text(row.get("last_update", "")),
    )


def canonical_event_props_row(
    *,
    provider: Any,
    snapshot_id: Any,
    event_id: Any,
    market: Any,
    player: Any,
    side: Any,
    price: Any,
    point: Any,
    book: Any,
    last_update: Any,
    link: Any,
    schema_version: int = QUOTE_TABLE_SCHEMA_VERSION,
) -> dict[str, Any]:
    return {
        "provider": _text(provider),
        "snapshot_id": _text(snapshot_id),
        "schema_version": int(schema_version),
        "event_id": _text(event_id),
        "market": _text(market),
        "player": _text(player),
        "side": _text(side),
        "price": _float_or_none(price),
        "point": _float_or_none(point),
        "book": _text(book),
        "last_update": _text(last_update),
        "link": _text(link),
    }


def canonical_featured_odds_row(
    *,
    provider: Any,
    snapshot_id: Any,
    game_id: Any,
    market: Any,
    book: Any,
    price: Any,
    point: Any,
    side: Any,
    last_update: Any,
    schema_version: int = QUOTE_TABLE_SCHEMA_VERSION,
) -> dict[str, Any]:
    return {
        "provider": _text(provider),
        "snapshot_id": _text(snapshot_id),
        "schema_version": int(schema_version),
        "game_id": _text(game_id),
        "market": _text(market),
        "book": _text(book),
        "price": _float_or_none(price),
        "point": _float_or_none(point),
        "side": _text(side),
        "last_update": _text(last_update),
    }


def canonicalize_event_props_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical = [
        canonical_event_props_row(
            provider=row.get("provider", ""),
            snapshot_id=row.get("snapshot_id", ""),
            schema_version=_schema_version_value(row.get("schema_version")),
            event_id=row.get("event_id", ""),
            market=row.get("market", ""),
            player=row.get("player", ""),
            side=row.get("side", ""),
            price=row.get("price"),
            point=row.get("point"),
            book=row.get("book", ""),
            last_update=row.get("last_update", ""),
            link=row.get("link", ""),
        )
        for row in rows
    ]
    canonical.sort(key=_event_props_sort_key)
    return canonical


def canonicalize_featured_odds_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical = [
        canonical_featured_odds_row(
            provider=row.get("provider", ""),
            snapshot_id=row.get("snapshot_id", ""),
            schema_version=_schema_version_value(row.get("schema_version")),
            game_id=row.get("game_id", ""),
            market=row.get("market", ""),
            book=row.get("book", ""),
            price=row.get("price"),
            point=row.get("point"),
            side=row.get("side", ""),
            last_update=row.get("last_update", ""),
        )
        for row in rows
    ]
    canonical.sort(key=_featured_odds_sort_key)
    return canonical


def _require_columns(
    *, row: Mapping[str, Any], row_index: int, columns: tuple[str, ...], table_name: str
) -> None:
    missing = [column for column in columns if column not in row]
    if missing:
        joined = ", ".join(sorted(missing))
        raise QuoteTableContractError(
            f"{table_name} row index={row_index} missing required columns: {joined}"
        )


def validate_event_props_rows(rows: list[dict[str, Any]]) -> None:
    for row_index, row in enumerate(rows):
        _require_columns(
            row=row,
            row_index=row_index,
            columns=EVENT_PROPS_COLUMNS,
            table_name=EVENT_PROPS_TABLE,
        )


def validate_featured_odds_rows(rows: list[dict[str, Any]]) -> None:
    for row_index, row in enumerate(rows):
        _require_columns(
            row=row,
            row_index=row_index,
            columns=FEATURED_ODDS_COLUMNS,
            table_name=FEATURED_ODDS_TABLE,
        )
