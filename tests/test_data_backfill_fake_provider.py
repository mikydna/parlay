from __future__ import annotations

import json
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.odds_client import OddsResponse
from prop_ev.odds_data.day_index import load_day_status, snapshot_id_for_day
from prop_ev.odds_data.spec import DatasetSpec
from prop_ev.storage import SnapshotStore


def test_data_backfill_writes_day_snapshot_and_reuses_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_root))
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")

    counts = {"events": 0, "event_odds": 0}

    class FakeOddsClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def close(self) -> None:
            return None

        def list_events(self, **kwargs) -> OddsResponse:
            counts["events"] += 1
            return OddsResponse(
                data=[{"id": "event-1"}],
                status_code=200,
                headers={
                    "x-requests-last": "0",
                    "x-requests-used": "0",
                    "x-requests-remaining": "999",
                },
                duration_ms=3,
                retry_count=0,
            )

        def get_event_odds(self, **kwargs) -> OddsResponse:
            counts["event_odds"] += 1
            return OddsResponse(
                data={
                    "id": "event-1",
                    "bookmakers": [
                        {
                            "key": "draftkings",
                            "markets": [
                                {
                                    "key": "player_points",
                                    "last_update": "2026-02-11T20:00:00Z",
                                    "outcomes": [
                                        {
                                            "description": "Player A",
                                            "name": "Over",
                                            "price": -105,
                                            "point": 20.5,
                                            "link": "",
                                        },
                                        {
                                            "description": "Player A",
                                            "name": "Under",
                                            "price": -115,
                                            "point": 20.5,
                                            "link": "",
                                        },
                                    ],
                                }
                            ],
                        }
                    ],
                },
                status_code=200,
                headers={
                    "x-requests-last": "1",
                    "x-requests-used": "1",
                    "x-requests-remaining": "998",
                },
                duration_ms=8,
                retry_count=0,
            )

    monkeypatch.setattr("prop_ev.odds_data.backfill.OddsAPIClient", FakeOddsClient)

    day = "2026-02-11"
    args = [
        "data",
        "backfill",
        "--sport-key",
        "basketball_nba",
        "--markets",
        "player_points",
        "--bookmakers",
        "draftkings",
        "--from",
        day,
        "--to",
        day,
        "--max-credits",
        "5",
    ]

    assert main(args) == 0
    assert counts["event_odds"] == 1

    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
    )
    snapshot_id = snapshot_id_for_day(spec, day)
    store = SnapshotStore(data_root)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    derived_path = snapshot_dir / "derived" / "event_props.jsonl"
    assert derived_path.exists()
    assert derived_path.read_text(encoding="utf-8").strip()

    status = load_day_status(data_root, spec, day)
    assert isinstance(status, dict)
    assert status["complete"] is True

    code = main(
        [
            "data",
            "backfill",
            "--sport-key",
            "basketball_nba",
            "--markets",
            "player_points",
            "--bookmakers",
            "draftkings",
            "--from",
            day,
            "--to",
            day,
            "--no-spend",
        ]
    )
    assert code == 0
    assert counts["event_odds"] == 1

    status_after = load_day_status(data_root, spec, day)
    assert isinstance(status_after, dict)
    assert status_after["complete"] is True


def test_data_backfill_historical_uses_pre_tip_dates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_root))
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")

    captured: dict[str, str] = {}

    class FakeOddsClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def close(self) -> None:
            return None

        def list_events(self, **kwargs) -> OddsResponse:
            captured["events_historical_date"] = str(kwargs.get("historical_date", ""))
            return OddsResponse(
                data=[{"id": "event-1", "commence_time": "2026-02-11T20:30:00Z"}],
                status_code=200,
                headers={
                    "x-requests-last": "1",
                    "x-requests-used": "1",
                    "x-requests-remaining": "999",
                },
                duration_ms=3,
                retry_count=0,
            )

        def get_event_odds(self, **kwargs) -> OddsResponse:
            captured["odds_historical_date"] = str(kwargs.get("historical_date", ""))
            return OddsResponse(
                data={
                    "id": "event-1",
                    "bookmakers": [
                        {
                            "key": "draftkings",
                            "markets": [
                                {
                                    "key": "player_points",
                                    "last_update": "2026-02-11T19:30:00Z",
                                    "outcomes": [
                                        {
                                            "description": "Player A",
                                            "name": "Over",
                                            "price": -105,
                                            "point": 20.5,
                                            "link": "",
                                        },
                                        {
                                            "description": "Player A",
                                            "name": "Under",
                                            "price": -115,
                                            "point": 20.5,
                                            "link": "",
                                        },
                                    ],
                                }
                            ],
                        }
                    ],
                },
                status_code=200,
                headers={
                    "x-requests-last": "10",
                    "x-requests-used": "11",
                    "x-requests-remaining": "989",
                },
                duration_ms=8,
                retry_count=0,
            )

    monkeypatch.setattr("prop_ev.odds_data.backfill.OddsAPIClient", FakeOddsClient)

    day = "2026-02-11"
    code = main(
        [
            "data",
            "backfill",
            "--sport-key",
            "basketball_nba",
            "--markets",
            "player_points",
            "--bookmakers",
            "draftkings",
            "--from",
            day,
            "--to",
            day,
            "--historical",
            "--historical-anchor-hour-local",
            "12",
            "--historical-pre-tip-minutes",
            "60",
            "--max-credits",
            "20",
        ]
    )
    assert code == 0
    assert captured["events_historical_date"] == "2026-02-11T17:00:00Z"
    assert captured["odds_historical_date"] == "2026-02-11T19:30:00Z"

    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
        historical=True,
        historical_anchor_hour_local=12,
        historical_pre_tip_minutes=60,
    )
    snapshot_id = snapshot_id_for_day(spec, day)
    store = SnapshotStore(data_root)
    manifest = json.loads((store.snapshot_dir(snapshot_id) / "manifest.json").read_text("utf-8"))
    request_paths = {
        str(item.get("path", ""))
        for item in manifest.get("requests", {}).values()
        if isinstance(item, dict)
    }
    assert "/historical/sports/basketball_nba/events" in request_paths
    assert "/historical/sports/basketball_nba/events/event-1/odds" in request_paths
