"""Snapshot date resolution for NBA source policy decisions."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime

_DAY_SUFFIX_RE = re.compile(r"(?P<day>\d{4}-\d{2}-\d{2})$")
_DAILY_RE = re.compile(r"^daily-(\d{4})-?(\d{2})-?(\d{2})")


def resolve_snapshot_date(snapshot_id: str) -> date:
    """Resolve modeled snapshot date from snapshot id.

    Supported ids:
    - ISO-like prefix: 2026-02-12T...
    - daily ids: daily-20260212T...
    - day index ids: day-<dataset>-2026-02-12
    """
    raw = snapshot_id.strip()
    if len(raw) >= 10:
        prefix = raw[:10]
        try:
            return date.fromisoformat(prefix)
        except ValueError:
            pass

    match_daily = _DAILY_RE.match(raw)
    if match_daily:
        year, month, day = match_daily.groups()
        return date.fromisoformat(f"{year}-{month}-{day}")

    match_day = _DAY_SUFFIX_RE.search(raw)
    if match_day:
        return date.fromisoformat(match_day.group("day"))

    return datetime.now(UTC).date()


def resolve_snapshot_date_str(snapshot_id: str) -> str:
    """Return YYYY-MM-DD snapshot date string."""
    return resolve_snapshot_date(snapshot_id).isoformat()
