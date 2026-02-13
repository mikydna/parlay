"""Shared UTC timestamp helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

ET_ZONE = ZoneInfo("America/New_York")


def utc_now() -> datetime:
    """Return current UTC time with second precision."""
    return datetime.now(UTC).replace(microsecond=0)


def iso_z(value: datetime) -> str:
    """Format a datetime as ISO-8601 with trailing Z in UTC."""
    normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def utc_now_str() -> str:
    """Return current UTC timestamp in ISO-Z format."""
    return iso_z(utc_now())


def et_snapshot_id_now() -> str:
    """Return a filesystem-safe snapshot id timestamp in ET."""
    return utc_now().astimezone(ET_ZONE).strftime("%Y-%m-%dT%H-%M-%S-ET")


def parse_iso_z(value: str) -> datetime | None:
    """Parse an ISO timestamp and normalize to UTC."""
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
