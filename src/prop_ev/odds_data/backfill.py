"""Backfill service for day-indexed odds datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

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
)
from prop_ev.odds_data.errors import CreditBudgetExceeded, OfflineCacheMiss, SpendBlockedError
from prop_ev.odds_data.policy import SpendPolicy, effective_max_credits
from prop_ev.odds_data.repo import OddsRepository
from prop_ev.odds_data.request import OddsRequest
from prop_ev.odds_data.spec import DatasetSpec, dataset_id
from prop_ev.odds_data.window import day_window
from prop_ev.settings import Settings
from prop_ev.storage import SnapshotStore, request_hash


def _parse_event_ids(events_payload: Any) -> list[str]:
    if not isinstance(events_payload, list):
        return []
    out: list[str] = []
    for row in events_payload:
        if not isinstance(row, dict):
            continue
        event_id = str(row.get("id", "")).strip()
        if event_id:
            out.append(event_id)
    return out


def _events_params(*, commence_from: str, commence_to: str, date_format: str) -> dict[str, Any]:
    return {
        "dateFormat": date_format,
        "commenceTimeFrom": commence_from,
        "commenceTimeTo": commence_to,
    }


def _event_odds_params(spec: DatasetSpec) -> dict[str, Any]:
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
            run_config = {
                "mode": "data_backfill_day",
                "dataset_id": dataset_id(spec),
                "day": day,
                "tz_name": tz_name,
                "sport_key": spec.sport_key,
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
            with store.lock_snapshot(snapshot_id):
                store.ensure_snapshot(snapshot_id, run_config=run_config)
                events_path = f"/sports/{spec.sport_key}/events"
                events_params = _events_params(
                    commence_from=commence_from,
                    commence_to=commence_to,
                    date_format=spec.date_format,
                )
                events_req = OddsRequest(
                    method="GET",
                    path=events_path,
                    params=events_params,
                    label="events_list",
                    is_paid=False,
                )
                try:
                    events_result = repo.get_or_fetch(
                        snapshot_id=snapshot_id,
                        req=events_req,
                        fetcher=lambda commence_from=commence_from, commence_to=commence_to: (
                            _client().list_events(
                                sport_key=spec.sport_key,
                                commence_from=commence_from,
                                commence_to=commence_to,
                                date_format=spec.date_format,
                            )
                        ),
                        policy=policy,
                    )
                except (OfflineCacheMiss, SpendBlockedError, OddsAPIError, ValueError) as exc:
                    day_error = str(exc)
                    status = compute_day_status_from_cache(
                        data_root=data_root,
                        store=store,
                        cache=cache,
                        spec=spec,
                        day=day,
                        tz_name=tz_name,
                    )
                    status["error"] = day_error
                    status["complete"] = False
                    save_day_status(data_root, spec, day, status)
                    summaries.append(
                        {
                            "day": day,
                            "snapshot_id": snapshot_id,
                            "complete": False,
                            "missing": int(status.get("missing_count", 0)),
                            "events": int(status.get("total_events", 0)),
                            "estimated_paid_credits": estimated_paid_credits,
                            "remaining_credits": remaining_credits,
                            "error": day_error,
                        }
                    )
                    continue

                event_ids = _parse_event_ids(events_result.data)
                event_odds_params = _event_odds_params(spec)
                missing_event_ids: list[str] = []
                for event_id in event_ids:
                    request_path = f"/sports/{spec.sport_key}/events/{event_id}/odds"
                    request_key = request_hash("GET", request_path, event_odds_params)
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
                    day_error = str(exc)
                    status = compute_day_status_from_cache(
                        data_root=data_root,
                        store=store,
                        cache=cache,
                        spec=spec,
                        day=day,
                        tz_name=tz_name,
                    )
                    status["error"] = day_error
                    status["complete"] = False
                    save_day_status(data_root, spec, day, status)
                    summaries.append(
                        {
                            "day": day,
                            "snapshot_id": snapshot_id,
                            "complete": False,
                            "missing": int(status.get("missing_count", 0)),
                            "events": int(status.get("total_events", 0)),
                            "estimated_paid_credits": estimated_paid_credits,
                            "remaining_credits": remaining_credits,
                            "error": day_error,
                        }
                    )
                    continue

                if not dry_run:
                    for event_id in missing_event_ids:
                        request_path = f"/sports/{spec.sport_key}/events/{event_id}/odds"
                        req = OddsRequest(
                            method="GET",
                            path=request_path,
                            params=event_odds_params,
                            label=f"event_odds:{event_id}",
                            is_paid=True,
                        )
                        try:
                            repo.get_or_fetch(
                                snapshot_id=snapshot_id,
                                req=req,
                                fetcher=lambda event_id=event_id: _client().get_event_odds(
                                    sport_key=spec.sport_key,
                                    event_id=event_id,
                                    markets=spec.markets,
                                    regions=spec.regions,
                                    bookmakers=spec.bookmakers,
                                    include_links=spec.include_links,
                                    include_sids=spec.include_sids,
                                    odds_format=spec.odds_format,
                                    date_format=spec.date_format,
                                ),
                                policy=policy,
                            )
                        except (
                            OfflineCacheMiss,
                            SpendBlockedError,
                            OddsAPIError,
                            ValueError,
                        ) as exc:
                            day_error = str(exc)

                    rows: list[dict[str, Any]] = []
                    for event_id in event_ids:
                        request_path = f"/sports/{spec.sport_key}/events/{event_id}/odds"
                        request_key = request_hash("GET", request_path, event_odds_params)
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

                    if estimated_paid_credits > 0:
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
                    status["error"] = day_error
                    status["complete"] = False
                save_day_status(data_root, spec, day, status)

                summaries.append(
                    {
                        "day": day,
                        "snapshot_id": snapshot_id,
                        "complete": bool(status.get("complete", False)),
                        "missing": int(status.get("missing_count", 0)),
                        "events": int(status.get("total_events", 0)),
                        "estimated_paid_credits": estimated_paid_credits,
                        "remaining_credits": remaining_credits,
                        "error": day_error,
                    }
                )
        return summaries
    finally:
        if client is not None:
            client.close()
