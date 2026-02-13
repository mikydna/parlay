from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.day_index import compute_day_status_from_cache, with_day_error
from prop_ev.odds_data.spec import DatasetSpec, dataset_id
from prop_ev.odds_data.window import day_window
from prop_ev.storage import SnapshotStore, request_hash


def test_dataset_id_is_stable_for_equivalent_specs() -> None:
    first = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_rebounds", "player_points"],
        regions="us",
        bookmakers=None,
        include_links=False,
        include_sids=False,
    )
    second = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points", "player_rebounds", "player_points"],
        regions="us",
        bookmakers=None,
        include_links=False,
        include_sids=False,
    )
    assert dataset_id(first) == dataset_id(second)


def test_day_window_handles_dst_transition() -> None:
    commence_from, commence_to = day_window("2026-03-08", "America/New_York")
    assert commence_from == "2026-03-08T05:00:00Z"
    assert commence_to == "2026-03-09T04:00:00Z"


def test_compute_day_status_marks_missing_event_odds(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers=None,
        include_links=False,
        include_sids=False,
    )
    day = "2026-02-11"
    snapshot_id = "day-" + dataset_id(spec)[:8] + "-" + day
    store.ensure_snapshot(snapshot_id)

    commence_from, commence_to = day_window(day, "America/New_York")
    events_path = f"/sports/{spec.sport_key}/events"
    events_params = {
        "dateFormat": "iso",
        "commenceTimeFrom": commence_from,
        "commenceTimeTo": commence_to,
    }
    events_key = request_hash("GET", events_path, events_params)
    cache.write_request(events_key, {"method": "GET", "path": events_path, "params": events_params})
    cache.write_response(events_key, [{"id": "event-1"}, {"id": "event-2"}])
    cache.write_meta(events_key, {"headers": {}})

    odds_params = {
        "markets": "player_points",
        "oddsFormat": "american",
        "dateFormat": "iso",
        "regions": "us",
    }
    odds_path = f"/sports/{spec.sport_key}/events/event-1/odds"
    odds_key = request_hash("GET", odds_path, odds_params)
    cache.write_request(odds_key, {"method": "GET", "path": odds_path, "params": odds_params})
    cache.write_response(odds_key, {"id": "event-1", "bookmakers": []})
    cache.write_meta(odds_key, {"headers": {}})

    status = compute_day_status_from_cache(
        data_root=data_root,
        store=store,
        cache=cache,
        spec=spec,
        day=day,
        tz_name="America/New_York",
    )

    assert status["total_events"] == 2
    assert status["present_event_odds"] == 1
    assert status["missing_count"] == 1
    assert status["missing_event_ids"] == ["event-2"]
    assert status["complete"] is False
    assert status["status_code"] == "incomplete_missing_event_odds"
    assert status["reason_codes"] == ["missing_event_odds"]
    assert status["odds_coverage_ratio"] == 0.5


def test_compute_day_status_historical_uses_event_dates(tmp_path: Path) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions=None,
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
        historical=True,
        historical_anchor_hour_local=12,
        historical_pre_tip_minutes=60,
    )
    day = "2026-02-11"
    snapshot_id = "day-" + dataset_id(spec)[:8] + "-" + day
    store.ensure_snapshot(snapshot_id)

    events_timestamp = "2026-02-11T17:00:00Z"
    events_path = f"/historical/sports/{spec.sport_key}/events"
    events_params = {"dateFormat": "iso", "date": events_timestamp}
    events_key = request_hash("GET", events_path, events_params)
    cache.write_request(events_key, {"method": "GET", "path": events_path, "params": events_params})
    cache.write_response(
        events_key,
        [
            {
                "id": "event-1",
                "commence_time": "2026-02-11T20:30:00Z",
            }
        ],
    )
    cache.write_meta(events_key, {"headers": {}})

    commence = datetime.fromisoformat("2026-02-11T20:30:00+00:00").astimezone(UTC)
    odds_date = (commence - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    odds_params = {
        "markets": "player_points",
        "oddsFormat": "american",
        "dateFormat": "iso",
        "bookmakers": "draftkings",
        "date": odds_date,
    }
    odds_path = f"/historical/sports/{spec.sport_key}/events/event-1/odds"
    odds_key = request_hash("GET", odds_path, odds_params)
    cache.write_request(odds_key, {"method": "GET", "path": odds_path, "params": odds_params})
    cache.write_response(odds_key, {"id": "event-1", "bookmakers": []})
    cache.write_meta(odds_key, {"headers": {}})

    status = compute_day_status_from_cache(
        data_root=data_root,
        store=store,
        cache=cache,
        spec=spec,
        day=day,
        tz_name="America/New_York",
    )

    assert status["historical"] is True
    assert status["events_timestamp"] == events_timestamp
    assert status["event_odds_dates"]["event-1"] == odds_date
    assert status["missing_count"] == 0
    assert status["complete"] is True
    assert status["status_code"] == "complete"
    assert status["reason_codes"] == ["complete"]
    assert status["odds_coverage_ratio"] == 1.0


def test_with_day_error_sets_reason_code() -> None:
    base_status = {
        "day": "2026-02-11",
        "complete": True,
        "missing_count": 0,
        "total_events": 5,
        "present_event_odds": 5,
        "note": "",
        "error": "",
    }
    updated = with_day_error(
        base_status,
        error="estimated credits 100 exceed remaining budget 70 for day 2026-02-11",
    )

    assert updated["complete"] is False
    assert updated["error_code"] == "budget_exceeded"
    assert updated["status_code"] == "incomplete_budget_exceeded"
    assert updated["reason_codes"] == ["budget_exceeded"]
