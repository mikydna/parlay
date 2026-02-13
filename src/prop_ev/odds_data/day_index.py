"""Day-level completeness index for cached odds datasets."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import suppress
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.spec import DatasetSpec, canonical_dict, dataset_id
from prop_ev.odds_data.window import day_window
from prop_ev.storage import SnapshotStore, request_hash
from prop_ev.time_utils import utc_now_str


def _atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".tmp-{path.name}-{uuid.uuid4().hex}")
    try:
        payload = json.dumps(value, sort_keys=True, ensure_ascii=True, indent=2) + "\n"
        tmp_path.write_text(payload, encoding="utf-8")
        os.replace(tmp_path, path)
    except OSError:
        with suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(raw_value: str) -> datetime | None:
    value = raw_value.strip()
    if not value:
        return None
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _historical_events_timestamp(day: str, tz_name: str, anchor_hour_local: int) -> str:
    parsed_day = date.fromisoformat(day)
    tz = ZoneInfo(tz_name)
    safe_hour = max(0, min(int(anchor_hour_local), 23))
    local_dt = datetime.combine(parsed_day, time(hour=safe_hour), tzinfo=tz)
    return _iso_z(local_dt)


def _historical_event_odds_timestamp(
    *,
    event_row: dict[str, Any],
    fallback_timestamp: str,
    pre_tip_minutes: int,
) -> str:
    commence = _parse_iso_utc(str(event_row.get("commence_time", "")))
    if commence is None:
        return fallback_timestamp
    safe_minutes = max(0, int(pre_tip_minutes))
    return _iso_z(commence - timedelta(minutes=safe_minutes))


def dataset_spec_path(data_root: Path | str, spec: DatasetSpec) -> Path:
    return Path(data_root) / "datasets" / dataset_id(spec) / "spec.json"


def dataset_days_dir(data_root: Path | str, spec: DatasetSpec) -> Path:
    return Path(data_root) / "datasets" / dataset_id(spec) / "days"


def _day_status_path(data_root: Path | str, spec: DatasetSpec, day: str) -> Path:
    return dataset_days_dir(data_root, spec) / f"{day}.json"


def snapshot_id_for_day(spec: DatasetSpec, day: str) -> str:
    return f"day-{dataset_id(spec)[:8]}-{day}"


def save_dataset_spec(data_root: Path | str, spec: DatasetSpec) -> Path:
    path = dataset_spec_path(data_root, spec)
    payload = canonical_dict(spec) | {"dataset_id": dataset_id(spec)}
    _atomic_write_json(path, payload)
    return path


def load_day_status(data_root: Path | str, spec: DatasetSpec, day: str) -> dict[str, Any] | None:
    path = _day_status_path(data_root, spec, day)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def save_day_status(
    data_root: Path | str, spec: DatasetSpec, day: str, status: dict[str, Any]
) -> Path:
    path = _day_status_path(data_root, spec, day)
    _atomic_write_json(path, status)
    return path


def _events_request(
    spec: DatasetSpec,
    *,
    day: str,
    tz_name: str,
    commence_from: str,
    commence_to: str,
) -> tuple[str, dict[str, Any], str]:
    if spec.historical:
        events_timestamp = _historical_events_timestamp(
            day,
            tz_name,
            spec.historical_anchor_hour_local,
        )
        return (
            f"/historical/sports/{spec.sport_key}/events",
            {"dateFormat": spec.date_format, "date": events_timestamp},
            events_timestamp,
        )
    path = f"/sports/{spec.sport_key}/events"
    params = {
        "dateFormat": spec.date_format,
        "commenceTimeFrom": commence_from,
        "commenceTimeTo": commence_to,
    }
    return path, params, ""


def _event_odds_request(
    spec: DatasetSpec,
    event_id: str,
    *,
    historical_date: str | None = None,
) -> tuple[str, dict[str, Any]]:
    if historical_date:
        path = f"/historical/sports/{spec.sport_key}/events/{event_id}/odds"
    else:
        path = f"/sports/{spec.sport_key}/events/{event_id}/odds"
    params: dict[str, Any] = {
        "markets": ",".join(sorted(set(spec.markets))),
        "oddsFormat": spec.odds_format,
        "dateFormat": spec.date_format,
    }
    if spec.bookmakers:
        params["bookmakers"] = spec.bookmakers
    elif spec.regions:
        params["regions"] = spec.regions
    if spec.include_links:
        params["includeLinks"] = "true"
    if spec.include_sids:
        params["includeSids"] = "true"
    if historical_date:
        params["date"] = historical_date
    return path, params


def compute_day_status_from_cache(
    *,
    data_root: Path | str,
    store: SnapshotStore,
    cache: GlobalCacheStore,
    spec: DatasetSpec,
    day: str,
    tz_name: str,
) -> dict[str, Any]:
    commence_from, commence_to = day_window(day, tz_name)
    snapshot_id = snapshot_id_for_day(spec, day)
    events_path, events_params, events_timestamp = _events_request(
        spec,
        day=day,
        tz_name=tz_name,
        commence_from=commence_from,
        commence_to=commence_to,
    )
    events_key = request_hash("GET", events_path, events_params)

    events_payload: Any | None = None
    if store.has_response(snapshot_id, events_key):
        events_payload = store.load_response(snapshot_id, events_key)
    elif cache.has_response(events_key):
        events_payload = cache.load_response(events_key)

    event_rows: list[dict[str, Any]] = []
    if isinstance(events_payload, dict):
        events_payload = events_payload.get("data")
    if isinstance(events_payload, list):
        event_rows = [item for item in events_payload if isinstance(item, dict)]

    event_ids: list[str] = []
    for event_row in event_rows:
        event_id = str(event_row.get("id", "")).strip()
        if event_id:
            event_ids.append(event_id)

    expected_event_odds: dict[str, str] = {}
    event_odds_dates: dict[str, str] = {}
    missing_event_ids: list[str] = []
    present_event_odds = 0
    for event_row in event_rows:
        event_id = str(event_row.get("id", "")).strip()
        if not event_id:
            continue
        historical_date = None
        if spec.historical:
            historical_date = _historical_event_odds_timestamp(
                event_row=event_row,
                fallback_timestamp=events_timestamp,
                pre_tip_minutes=spec.historical_pre_tip_minutes,
            )
            event_odds_dates[event_id] = historical_date
        request_path, request_params = _event_odds_request(
            spec,
            event_id,
            historical_date=historical_date,
        )
        key = request_hash("GET", request_path, request_params)
        expected_event_odds[event_id] = key
        if store.has_response(snapshot_id, key) or cache.has_response(key):
            present_event_odds += 1
        else:
            missing_event_ids.append(event_id)

    note = ""
    if events_payload is None:
        note = "missing events list response"
    elif not isinstance(events_payload, list):
        note = "invalid events list payload"

    return {
        "dataset_id": dataset_id(spec),
        "historical": bool(spec.historical),
        "day": day,
        "tz_name": tz_name,
        "commence_from": commence_from,
        "commence_to": commence_to,
        "events_timestamp": events_timestamp,
        "snapshot_id_for_day": snapshot_id,
        "events_key": events_key,
        "event_ids": event_ids,
        "expected_event_odds": expected_event_odds,
        "event_odds_dates": event_odds_dates,
        "present_event_odds": present_event_odds,
        "missing_event_ids": missing_event_ids,
        "complete": bool(events_payload is not None and not missing_event_ids),
        "total_events": len(event_ids),
        "missing_count": len(missing_event_ids),
        "updated_at_utc": utc_now_str(),
        "note": note,
    }
