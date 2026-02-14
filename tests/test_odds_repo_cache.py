from __future__ import annotations

from pathlib import Path

import pytest

from prop_ev.odds_client import OddsResponse
from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.errors import OfflineCacheMiss, SpendBlockedError
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


def test_global_cache_ignores_legacy_cache_tree(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "odds_api"
    cache = GlobalCacheStore(data_root)
    key = "legacy-only-key"

    legacy_cache_dir = data_root / "cache"
    (legacy_cache_dir / "responses").mkdir(parents=True, exist_ok=True)
    (legacy_cache_dir / "meta").mkdir(parents=True, exist_ok=True)
    (legacy_cache_dir / "responses" / f"{key}.json").write_text(
        '{"id":"legacy"}\n', encoding="utf-8"
    )
    (legacy_cache_dir / "meta" / f"{key}.json").write_text(
        '{"status":"legacy"}\n', encoding="utf-8"
    )

    assert cache.has_response(key) is False
    assert cache.load_response(key) is None
    assert cache.load_meta(key) is None


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


def test_snapshot_hit_returns_cached_without_fetch(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    repo = OddsRepository(store=store, cache=cache)
    snapshot_id = "snap-3"
    store.ensure_snapshot(snapshot_id)

    path = "/sports/basketball_nba/events/event-3/odds"
    params = {
        "markets": "player_points",
        "regions": "us",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    key = request_hash("GET", path, params)
    store.write_response(snapshot_id, key, {"id": "event-3", "from": "snapshot"})
    store.write_meta(snapshot_id, key, {"headers": {"x-requests-last": "0"}})

    called = {"count": 0}

    def _fetcher() -> OddsResponse:
        called["count"] += 1
        return OddsResponse(
            data={"id": "event-3", "from": "network"},
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
            label="event_odds:event-3",
            is_paid=True,
        ),
        fetcher=_fetcher,
        policy=SpendPolicy(),
    )

    assert called["count"] == 0
    assert result.status == "cached"
    assert result.cache_level == "snapshot"
    assert result.data == {"id": "event-3", "from": "snapshot"}


def test_snapshot_ok_status_returns_skipped(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    repo = OddsRepository(store=store, cache=cache)
    snapshot_id = "snap-4"
    store.ensure_snapshot(snapshot_id)

    path = "/sports/basketball_nba/events/event-4/odds"
    params = {
        "markets": "player_points",
        "regions": "us",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    key = request_hash("GET", path, params)
    store.write_response(snapshot_id, key, {"id": "event-4"})
    store.write_meta(snapshot_id, key, {"headers": {"x-requests-last": "0"}})
    store.mark_request(
        snapshot_id,
        key,
        label="event_odds:event-4",
        path=path,
        params=params,
        status="ok",
    )

    called = {"count": 0}

    def _fetcher() -> OddsResponse:
        called["count"] += 1
        return OddsResponse(
            data={"id": "event-4"},
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
            label="event_odds:event-4",
            is_paid=True,
        ),
        fetcher=_fetcher,
        policy=SpendPolicy(),
    )

    assert called["count"] == 0
    assert result.status == "skipped"
    assert result.cache_level == "snapshot"


def test_offline_miss_raises(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    repo = OddsRepository(store=store, cache=cache)
    snapshot_id = "snap-5"
    store.ensure_snapshot(snapshot_id)

    path = "/sports/basketball_nba/events/event-5/odds"
    params = {
        "markets": "player_points",
        "regions": "us",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }

    with pytest.raises(OfflineCacheMiss):
        repo.get_or_fetch(
            snapshot_id=snapshot_id,
            req=OddsRequest(
                method="GET",
                path=path,
                params=params,
                label="event_odds:event-5",
                is_paid=True,
            ),
            fetcher=lambda: OddsResponse(
                data={"id": "event-5"},
                status_code=200,
                headers={},
                duration_ms=1,
                retry_count=0,
            ),
            policy=SpendPolicy(offline=True),
        )


def test_refresh_forces_network_and_writes_through_cache(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    repo = OddsRepository(store=store, cache=cache)
    snapshot_id = "snap-6"
    store.ensure_snapshot(snapshot_id)

    path = "/sports/basketball_nba/events/event-6/odds"
    params = {
        "markets": "player_points",
        "regions": "us",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    key = request_hash("GET", path, params)
    cache.write_request(key, {"method": "GET", "path": path, "params": params})
    cache.write_response(key, {"id": "event-6", "from": "global-cache"})
    cache.write_meta(key, {"headers": {"x-requests-last": "0"}})

    called = {"count": 0}

    def _fetcher() -> OddsResponse:
        called["count"] += 1
        return OddsResponse(
            data={"id": "event-6", "from": "network"},
            status_code=200,
            headers={
                "x-requests-last": "1",
                "x-requests-used": "42",
                "x-requests-remaining": "8",
            },
            duration_ms=9,
            retry_count=0,
        )

    result = repo.get_or_fetch(
        snapshot_id=snapshot_id,
        req=OddsRequest(
            method="GET",
            path=path,
            params=params,
            label="event_odds:event-6",
            is_paid=True,
        ),
        fetcher=_fetcher,
        policy=SpendPolicy(refresh=True),
    )

    assert called["count"] == 1
    assert result.status == "ok"
    assert result.cache_level == "network"
    assert cache.load_response(key) == {"id": "event-6", "from": "network"}
    usage_files = list(store.usage_dir.glob("usage-*.jsonl"))
    assert usage_files
    usage_payload = usage_files[0].read_text(encoding="utf-8")
    assert key in usage_payload
