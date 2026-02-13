"""Timezone-aware day window utilities for backfill."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from prop_ev.time_utils import iso_z


def day_window(day_yyyy_mm_dd: str, tz_name: str) -> tuple[str, str]:
    """Return UTC ISO-Z bounds for one local day [start, end)."""
    try:
        parsed_day = date.fromisoformat(day_yyyy_mm_dd)
    except ValueError as exc:
        raise ValueError(f"invalid day value: {day_yyyy_mm_dd}") from exc
    tz = ZoneInfo(tz_name)
    start_local = datetime.combine(parsed_day, time.min, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return iso_z(start_local.astimezone(UTC)), iso_z(end_local.astimezone(UTC))
