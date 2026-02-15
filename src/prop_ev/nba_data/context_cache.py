"""Context cache helpers for NBA repository consumers."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from prop_ev.time_utils import parse_iso_z, utc_now_str


def now_utc() -> str:
    """Return an ISO UTC timestamp."""
    return utc_now_str()


def _apply_stale_flag(payload: dict[str, Any], stale_after_hours: float | None) -> dict[str, Any]:
    if stale_after_hours is None:
        return payload
    copy = dict(payload)
    fetched_at = parse_iso_z(str(copy.get("fetched_at_utc", "")))
    if fetched_at is None:
        copy["stale"] = True
        copy["stale_reason"] = "missing_fetched_at_utc"
        return copy
    age = datetime.now(UTC) - fetched_at
    stale = age > timedelta(hours=max(0.0, stale_after_hours))
    copy["stale"] = stale
    copy["stale_age_hours"] = round(age.total_seconds() / 3600.0, 2)
    return copy


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(value, dict):
        return value
    return {
        "status": "error",
        "error": f"invalid json object in {path}",
        "fetched_at_utc": now_utc(),
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def load_or_fetch_context(
    *,
    cache_path: Path,
    offline: bool,
    refresh: bool,
    fetcher: Callable[[], dict[str, Any]],
    fallback_paths: list[Path] | None = None,
    write_through_paths: list[Path] | None = None,
    stale_after_hours: float | None = None,
) -> dict[str, Any]:
    """Load cached context or fetch and cache a fresh copy."""
    fallback_paths = fallback_paths or []
    write_through_paths = write_through_paths or []

    if cache_path.exists() and not refresh:
        return _apply_stale_flag(_load_json(cache_path), stale_after_hours)

    for path in fallback_paths:
        if not path.exists() or refresh:
            continue
        value = _load_json(path)
        _write_json(cache_path, value)
        return _apply_stale_flag(value, stale_after_hours)

    if offline:
        return {
            "status": "missing",
            "offline": True,
            "error": f"missing context cache: {cache_path}",
            "fetched_at_utc": now_utc(),
            "stale": True,
        }

    value = fetcher()
    _write_json(cache_path, value)
    for path in write_through_paths:
        _write_json(path, value)
    return _apply_stale_flag(value, stale_after_hours)
