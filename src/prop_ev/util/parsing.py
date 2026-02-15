"""Shared parsing helpers for tolerant numeric/string coercion."""

from __future__ import annotations

from typing import Any


def safe_float(value: Any) -> float | None:
    """Parse number-like input into float, returning None when invalid."""
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


def safe_int(value: Any) -> int | None:
    """Parse number-like input into int, returning None when invalid."""
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


def to_price(value: Any) -> int | None:
    """Parse American-odds integer price."""
    return safe_int(value)
