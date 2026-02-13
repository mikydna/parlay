from __future__ import annotations

from pathlib import Path

import pytest

from prop_ev.odds_client import OddsResponse
from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.errors import SpendBlockedError
from prop_ev.odds_data.policy import SpendPolicy
from prop_ev.odds_data.repo import OddsRepository
from prop_ev.odds_data.request import OddsRequest
from prop_ev.storage import SnapshotStore, request_hash


def test_global_cache_hit_materializes_into_snapshot(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    repo = OddsRepository(store=store, cache=cache)
    snapshot_id = "snap-1"
    store.ensure_snapshot(snapshot_id)

    path = "/sports/basketball_nba/events/event-1/odds"
    params = {
        "markets": "player_points",
        "regions": "us",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    key = request_hash("GET", path, params)
    cache.write_request(key, {"method": "GET", "path": path, "params": params})
    cache.write_response(key, {"id": "event-1"})
    cache.write_meta(
        key,
        {
            "endpoint": path,
            "status_code": 200,
            "duration_ms": 11,
            "retry_count": 0,
            "headers": {
                "x-requests-last": "1",
                "x-requests-used": "10",
                "x-requests-remaining": "90",
            },
            "fetched_at_utc": "2026-02-13T00:00:00Z",
        },
    )

    called = {"count": 0}

    def _fetcher() -> OddsResponse:
        called["count"] += 1
        return OddsResponse(
            data={"id": "event-1"},
            status_code=200,
            headers={},
            duration_ms=1,
            retry_count=0,
        )

    result = repo.get_or_fetch(
        snapshot_id=snapshot_id,
        req=OddsRequest(
            method="GET",
            path=path,
            params=params,
            label="event_odds:event-1",
            is_paid=True,
        ),
        fetcher=_fetcher,
        policy=SpendPolicy(),
    )

    assert called["count"] == 0
    assert result.status == "cached"
    assert result.cache_level == "global"
    assert store.has_response(snapshot_id, key)
    assert (store.snapshot_dir(snapshot_id) / "requests" / f"{key}.json").exists()


def test_paid_miss_no_spend_raises(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    repo = OddsRepository(store=store, cache=cache)
    snapshot_id = "snap-2"
    store.ensure_snapshot(snapshot_id)

    path = "/sports/basketball_nba/events/event-2/odds"
    params = {
        "markets": "player_points",
        "regions": "us",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }

    with pytest.raises(SpendBlockedError):
        repo.get_or_fetch(
            snapshot_id=snapshot_id,
            req=OddsRequest(
                method="GET",
                path=path,
                params=params,
                label="event_odds:event-2",
                is_paid=True,
            ),
            fetcher=lambda: OddsResponse(
                data={"id": "event-2"},
                status_code=200,
                headers={},
                duration_ms=1,
                retry_count=0,
            ),
            policy=SpendPolicy(max_credits=0, no_spend=True),
        )
