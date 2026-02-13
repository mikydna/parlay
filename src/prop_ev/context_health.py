"""Shared context source health helpers."""

from __future__ import annotations

from typing import Any


def official_rows_count(official: dict[str, Any] | None) -> int:
    """Return normalized official injury row count."""
    if not isinstance(official, dict):
        return 0
    rows = official.get("rows", [])
    rows_count = len(rows) if isinstance(rows, list) else 0
    raw_count = official.get("rows_count", rows_count)
    if isinstance(raw_count, bool):
        return rows_count
    if isinstance(raw_count, (int, float)):
        return max(0, int(raw_count))
    if isinstance(raw_count, str):
        try:
            return max(0, int(raw_count.strip()))
        except ValueError:
            return rows_count
    return rows_count


def official_source_ready(official: dict[str, Any] | None) -> bool:
    """Return whether official injury source is usable."""
    if not isinstance(official, dict):
        return False
    if str(official.get("status", "")) != "ok":
        return False
    if official_rows_count(official) <= 0:
        return False
    parse_status = str(official.get("parse_status", ""))
    return parse_status in {"", "ok"}


def secondary_source_ready(secondary: dict[str, Any] | None) -> bool:
    """Return whether secondary injury source is usable."""
    if not isinstance(secondary, dict):
        return False
    if str(secondary.get("status", "")) != "ok":
        return False
    rows = secondary.get("rows", [])
    row_count = len(rows) if isinstance(rows, list) else 0
    raw_count = secondary.get("count", row_count)
    if isinstance(raw_count, bool):
        count = row_count
    elif isinstance(raw_count, (int, float)):
        count = int(raw_count)
    elif isinstance(raw_count, str):
        try:
            count = int(raw_count.strip())
        except ValueError:
            count = row_count
    else:
        count = row_count
    return count > 0
