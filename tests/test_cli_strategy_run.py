import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.storage import SnapshotStore, request_hash


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _seed_strategy_snapshot(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    injuries_payload: dict[str, object],
) -> None:
    now_utc = _iso(datetime.now(UTC))
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    store.write_jsonl(
        snapshot_dir / "derived" / "event_props.jsonl",
        [
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 22.5,
                "side": "Over",
                "price": -105,
                "book": "book_a",
                "link": "",
                "last_update": now_utc,
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 22.5,
                "side": "Under",
                "price": -115,
                "book": "book_b",
                "link": "",
                "last_update": now_utc,
            },
        ],
    )

    path = "/sports/basketball_nba/events"
    params = {"dateFormat": "iso"}
    key = request_hash("GET", path, params)
    store.write_request(snapshot_id, key, {"method": "GET", "path": path, "params": params})
    store.write_response(
        snapshot_id,
        key,
        [
            {
                "id": "event-1",
                "home_team": "Boston Celtics",
                "away_team": "Miami Heat",
                "commence_time": now_utc,
            }
        ],
    )
    store.write_meta(snapshot_id, key, {"headers": {}, "fetched_at_utc": now_utc})
    store.mark_request(
        snapshot_id,
        key,
        label="events_list",
        path=path,
        params=params,
        status="ok",
        quota={"remaining": "", "used": "", "last": ""},
    )

    context_dir = snapshot_dir / "context"
    context_dir.mkdir(parents=True, exist_ok=True)
    (context_dir / "injuries.json").write_text(
        json.dumps(injuries_payload, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    (context_dir / "roster.json").write_text(
        json.dumps(
            {
                "source": "nba_live_scoreboard",
                "url": "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
                "status": "ok",
                "fetched_at_utc": now_utc,
                "count_teams": 2,
                "missing_roster_teams": [],
                "teams": {
                    "boston celtics": {
                        "active": ["playera"],
                        "inactive": [],
                        "all": ["playera"],
                    },
                    "miami heat": {"active": [], "inactive": [], "all": []},
                },
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


@pytest.fixture
def local_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    return data_dir


def test_strategy_run_hard_fails_when_official_missing(
    local_data_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T11-00-00Z"
    now_utc = _iso(datetime.now(UTC))
    _seed_strategy_snapshot(
        store=store,
        snapshot_id=snapshot_id,
        injuries_payload={
            "fetched_at_utc": now_utc,
            "official": {"status": "error", "rows": [], "rows_count": 0},
            "secondary": {
                "status": "ok",
                "rows": [
                    {
                        "player": "Player A",
                        "player_norm": "playera",
                        "team": "Boston Celtics",
                        "team_norm": "boston celtics",
                        "status": "questionable",
                        "note": "Test note",
                    }
                ],
                "count": 1,
            },
        },
    )

    code = main(["strategy", "run", "--snapshot-id", snapshot_id, "--offline"])
    err = capsys.readouterr().err

    assert code == 2
    assert "official injury report unavailable" in err


def test_strategy_run_allows_secondary_with_explicit_flag(
    local_data_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T11-05-00Z"
    now_utc = _iso(datetime.now(UTC))
    _seed_strategy_snapshot(
        store=store,
        snapshot_id=snapshot_id,
        injuries_payload={
            "fetched_at_utc": now_utc,
            "official": {"status": "error", "rows": [], "rows_count": 0},
            "secondary": {
                "status": "ok",
                "rows": [
                    {
                        "player": "Player A",
                        "player_norm": "playera",
                        "team": "Boston Celtics",
                        "team_norm": "boston celtics",
                        "status": "questionable",
                        "note": "Test note",
                    }
                ],
                "count": 1,
            },
        },
    )

    code = main(
        [
            "strategy",
            "run",
            "--snapshot-id",
            snapshot_id,
            "--offline",
            "--allow-secondary-injuries",
        ]
    )
    out = capsys.readouterr().out

    assert code == 0
    assert "note=official_injury_missing_using_secondary_override" in out
