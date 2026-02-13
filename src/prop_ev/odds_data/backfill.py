"""Backfill service for day-indexed odds datasets."""

from __future__ import annotations

import re
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from prop_ev.normalize import normalize_event_odds
from prop_ev.odds_client import (
    OddsAPIClient,
    OddsAPIError,
    estimate_event_credits,
    regions_equivalent,
)
from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.day_index import (
    compute_day_status_from_cache,
    save_dataset_spec,
    save_day_status,
    snapshot_id_for_day,
    with_day_error,
)
from prop_ev.odds_data.errors import CreditBudgetExceeded, OfflineCacheMiss, SpendBlockedError
from prop_ev.odds_data.policy import SpendPolicy, effective_max_credits
from prop_ev.odds_data.repo import OddsRepository
from prop_ev.odds_data.request import OddsRequest
from prop_ev.odds_data.spec import DatasetSpec, dataset_id
from prop_ev.odds_data.window import day_window
from prop_ev.settings import Settings
from prop_ev.storage import SnapshotStore, request_hash

HISTORICAL_ODDS_CREDIT_MULTIPLIER = 10


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sanitize_error_message(raw_message: str) -> str:
    message = str(raw_message)
    return re.sub(r"([?&](?:apiKey|api_key)=)[^&\s'\"]+", r"\1REDACTED", message)


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


def _parse_event_rows(events_payload: Any) -> list[dict[str, Any]]:
    payload = events_payload
    if isinstance(payload, dict):
        payload = payload.get("data")
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _parse_event_ids(events_payload: Any) -> list[str]:
    rows = _parse_event_rows(events_payload)
    if not rows:
        return []
    out: list[str] = []
    for row in rows:
        event_id = str(row.get("id", "")).strip()
        if event_id:
            out.append(event_id)
    return out


def _historical_events_timestamp(day: str, tz_name: str, anchor_hour_local: int) -> str:
    parsed_day = datetime.fromisoformat(day).date()
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


def _events_params(
    *,
    commence_from: str,
    commence_to: str,
    date_format: str,
    historical_date: str | None = None,
) -> dict[str, Any]:
    if historical_date:
        return {"dateFormat": date_format, "date": historical_date}
    return {
        "dateFormat": date_format,
        "commenceTimeFrom": commence_from,
        "commenceTimeTo": commence_to,
    }


def _event_odds_params(spec: DatasetSpec, *, historical_date: str | None = None) -> dict[str, Any]:
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
    return params


def backfill_days(
    *,
    data_root: Path,
    spec: DatasetSpec,
    days: list[str],
    tz_name: str,
    policy: SpendPolicy,
    dry_run: bool,
) -> list[dict[str, Any]]:
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    repo = OddsRepository(store=store, cache=cache)
    save_dataset_spec(data_root, spec)

    summaries: list[dict[str, Any]] = []
    remaining_credits = effective_max_credits(policy)
    client: OddsAPIClient | None = None

    def _client() -> OddsAPIClient:
        nonlocal client
        if client is None:
            client = OddsAPIClient(Settings.from_env())
        return client

    try:
        for day in days:
            commence_from, commence_to = day_window(day, tz_name)
            snapshot_id = snapshot_id_for_day(spec, day)
            events_historical_date = (
                _historical_events_timestamp(day, tz_name, spec.historical_anchor_hour_local)
                if spec.historical
                else None
            )
            run_config = {
                "mode": "data_backfill_day",
                "dataset_id": dataset_id(spec),
                "day": day,
                "tz_name": tz_name,
                "sport_key": spec.sport_key,
                "historical": bool(spec.historical),
                "historical_anchor_hour_local": int(spec.historical_anchor_hour_local),
                "historical_pre_tip_minutes": int(spec.historical_pre_tip_minutes),
                "historical_events_date": events_historical_date or "",
                "markets": sorted(set(spec.markets)),
                "regions": spec.regions or "",
                "bookmakers": spec.bookmakers or "",
                "include_links": bool(spec.include_links),
                "include_sids": bool(spec.include_sids),
                "commence_from": commence_from,
                "commence_to": commence_to,
            }

            day_error = ""
            estimated_paid_credits = 0
            actual_paid_credits = 0
            with store.lock_snapshot(snapshot_id):
                store.ensure_snapshot(snapshot_id, run_config=run_config)
                events_path = (
                    f"/historical/sports/{spec.sport_key}/events"
                    if spec.historical
                    else f"/sports/{spec.sport_key}/events"
                )
                events_params = _events_params(
                    commence_from=commence_from,
                    commence_to=commence_to,
                    date_format=spec.date_format,
                    historical_date=events_historical_date,
                )
                events_req = OddsRequest(
                    method="GET",
                    path=events_path,
                    params=events_params,
                    label="events_list",
                    is_paid=False,
                )

                def _fetch_events(
                    commence_from: str = commence_from,
                    commence_to: str = commence_to,
                    historical_date: str | None = events_historical_date,
                ) -> Any:
                    return _client().list_events(
                        sport_key=spec.sport_key,
                        commence_from=commence_from,
                        commence_to=commence_to,
                        date_format=spec.date_format,
                        historical_date=historical_date,
                    )

                try:
                    events_result = repo.get_or_fetch(
                        snapshot_id=snapshot_id,
                        req=events_req,
                        fetcher=_fetch_events,
                        policy=policy,
                    )
                except (OfflineCacheMiss, SpendBlockedError, OddsAPIError, ValueError) as exc:
                    day_error = _sanitize_error_message(str(exc))
                    status = compute_day_status_from_cache(
                        data_root=data_root,
                        store=store,
                        cache=cache,
                        spec=spec,
                        day=day,
                        tz_name=tz_name,
                    )
                    save_day_status(data_root, spec, day, with_day_error(status, error=day_error))
                    summaries.append(
                        {
                            "day": day,
                            "snapshot_id": snapshot_id,
                            "complete": False,
                            "missing": int(status.get("missing_count", 0)),
                            "events": int(status.get("total_events", 0)),
                            "estimated_paid_credits": estimated_paid_credits,
                            "actual_paid_credits": actual_paid_credits,
                            "remaining_credits": remaining_credits,
                            "error": day_error,
                        }
                    )
                    continue

                event_rows = _parse_event_rows(events_result.data)
                event_ids = _parse_event_ids(events_result.data)
                event_historical_dates: dict[str, str] = {}
                if spec.historical:
                    fallback = events_historical_date or commence_from
                    for row in event_rows:
                        event_id = str(row.get("id", "")).strip()
                        if not event_id:
                            continue
                        event_historical_dates[event_id] = _historical_event_odds_timestamp(
                            event_row=row,
                            fallback_timestamp=fallback,
                            pre_tip_minutes=spec.historical_pre_tip_minutes,
                        )
                missing_event_ids: list[str] = []
                for event_id in event_ids:
                    historical_date = event_historical_dates.get(event_id)
                    request_path = (
                        f"/historical/sports/{spec.sport_key}/events/{event_id}/odds"
                        if historical_date
                        else f"/sports/{spec.sport_key}/events/{event_id}/odds"
                    )
                    request_params = _event_odds_params(
                        spec,
                        historical_date=historical_date,
                    )
                    request_key = request_hash("GET", request_path, request_params)
                    if policy.refresh:
                        missing_event_ids.append(event_id)
                        continue
                    if store.has_response(snapshot_id, request_key) or cache.has_response(
                        request_key
                    ):
                        continue
                    missing_event_ids.append(event_id)

                regions_factor = regions_equivalent(spec.regions, spec.bookmakers)
                estimated_paid_credits = estimate_event_credits(
                    sorted(set(spec.markets)),
                    regions_factor,
                    len(missing_event_ids),
                )
                if spec.historical and estimated_paid_credits > 0:
                    estimated_paid_credits *= HISTORICAL_ODDS_CREDIT_MULTIPLIER

                try:
                    if missing_event_ids and (
                        policy.block_paid or effective_max_credits(policy) == 0
                    ):
                        raise SpendBlockedError(
                            "paid cache miss blocked for "
                            f"day={day} missing={len(missing_event_ids)}"
                        )
                    if (
                        estimated_paid_credits > remaining_credits
                        and missing_event_ids
                        and not policy.force
                    ):
                        raise CreditBudgetExceeded(
                            f"estimated credits {estimated_paid_credits} exceed "
                            f"remaining budget {remaining_credits} for day {day}"
                        )
                except (SpendBlockedError, CreditBudgetExceeded) as exc:
                    day_error = _sanitize_error_message(str(exc))
                    status = compute_day_status_from_cache(
                        data_root=data_root,
                        store=store,
                        cache=cache,
                        spec=spec,
                        day=day,
                        tz_name=tz_name,
                    )
                    save_day_status(data_root, spec, day, with_day_error(status, error=day_error))
                    summaries.append(
                        {
                            "day": day,
                            "snapshot_id": snapshot_id,
                            "complete": False,
                            "missing": int(status.get("missing_count", 0)),
                            "events": int(status.get("total_events", 0)),
                            "estimated_paid_credits": estimated_paid_credits,
                            "actual_paid_credits": actual_paid_credits,
                            "remaining_credits": remaining_credits,
                            "error": day_error,
                        }
                    )
                    continue

                if not dry_run:
                    for event_id in missing_event_ids:
                        historical_date = event_historical_dates.get(event_id)
                        request_path = (
                            f"/historical/sports/{spec.sport_key}/events/{event_id}/odds"
                            if historical_date
                            else f"/sports/{spec.sport_key}/events/{event_id}/odds"
                        )
                        request_params = _event_odds_params(
                            spec,
                            historical_date=historical_date,
                        )
                        req = OddsRequest(
                            method="GET",
                            path=request_path,
                            params=request_params,
                            label=f"event_odds:{event_id}",
                            is_paid=True,
                        )

                        def _fetch_event_odds(
                            event_id: str = event_id,
                            historical_date: str | None = historical_date,
                        ) -> Any:
                            return _client().get_event_odds(
                                sport_key=spec.sport_key,
                                event_id=event_id,
                                markets=spec.markets,
                                regions=spec.regions,
                                bookmakers=spec.bookmakers,
                                include_links=spec.include_links,
                                include_sids=spec.include_sids,
                                odds_format=spec.odds_format,
                                date_format=spec.date_format,
                                historical_date=historical_date,
                            )

                        try:
                            result = repo.get_or_fetch(
                                snapshot_id=snapshot_id,
                                req=req,
                                fetcher=_fetch_event_odds,
                                policy=policy,
                            )
                            try:
                                actual_paid_credits += int(
                                    result.headers.get("x-requests-last", "0")
                                )
                            except ValueError:
                                actual_paid_credits += 0
                        except (
                            OfflineCacheMiss,
                            SpendBlockedError,
                            OddsAPIError,
                            ValueError,
                        ) as exc:
                            day_error = _sanitize_error_message(str(exc))

                    rows: list[dict[str, Any]] = []
                    for event_id in event_ids:
                        historical_date = event_historical_dates.get(event_id)
                        request_path = (
                            f"/historical/sports/{spec.sport_key}/events/{event_id}/odds"
                            if historical_date
                            else f"/sports/{spec.sport_key}/events/{event_id}/odds"
                        )
                        request_params = _event_odds_params(
                            spec,
                            historical_date=historical_date,
                        )
                        request_key = request_hash("GET", request_path, request_params)
                        payload: Any | None = None
                        if store.has_response(snapshot_id, request_key):
                            payload = store.load_response(snapshot_id, request_key)
                        elif cache.has_response(request_key):
                            cache.materialize_into_snapshot(store, snapshot_id, request_key)
                            payload = cache.load_response(request_key)
                        if isinstance(payload, dict):
                            rows.extend(
                                normalize_event_odds(
                                    payload,
                                    snapshot_id=snapshot_id,
                                    provider="odds_api",
                                )
                            )
                    store.write_jsonl(store.derived_path(snapshot_id, "event_props.jsonl"), rows)

                    if actual_paid_credits > 0:
                        remaining_credits = max(0, remaining_credits - actual_paid_credits)
                    elif estimated_paid_credits > 0:
                        remaining_credits = max(0, remaining_credits - estimated_paid_credits)

                status = compute_day_status_from_cache(
                    data_root=data_root,
                    store=store,
                    cache=cache,
                    spec=spec,
                    day=day,
                    tz_name=tz_name,
                )
                if day_error:
                    status = with_day_error(status, error=day_error)
                save_day_status(data_root, spec, day, status)

                summaries.append(
                    {
                        "day": day,
                        "snapshot_id": snapshot_id,
                        "complete": bool(status.get("complete", False)),
                        "missing": int(status.get("missing_count", 0)),
                        "events": int(status.get("total_events", 0)),
                        "estimated_paid_credits": estimated_paid_credits,
                        "actual_paid_credits": actual_paid_credits,
                        "remaining_credits": remaining_credits,
                        "error": day_error,
                    }
                )
        return summaries
    finally:
        if client is not None:
            client.close()
