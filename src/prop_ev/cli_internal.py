"""Low-level pure helpers for CLI orchestration."""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

from prop_ev.time_utils import iso_z, utc_now


def default_window() -> tuple[str, str]:
    """Return default event lookup window in UTC."""
    start = utc_now().replace(hour=0, minute=0, second=0)
    end = start + timedelta(hours=32)
    return iso_z(start), iso_z(end)


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def teams_in_scope_from_events(events: list[dict[str, Any]]) -> set[str]:
    teams: set[str] = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        home = str(event.get("home_team", "")).strip()
        away = str(event.get("away_team", "")).strip()
        if home:
            teams.add(home)
        if away:
            teams.add(away)
    return teams
