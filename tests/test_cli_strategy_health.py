import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.storage import SnapshotStore, request_hash


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _seed_snapshot(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    with_event_context: bool,
    injuries_fetched_at: str,
    roster_fetched_at: str,
    missing_roster_teams: list[str] | None = None,
    roster_fallback: dict[str, object] | None = None,
) -> Path:
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    now_utc = _iso(datetime.now(UTC))
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

    if with_event_context:
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

    injuries_payload = {
        "fetched_at_utc": injuries_fetched_at,
        "official": {
            "source": "official_nba",
            "url": "https://official.nba.com/nba-injury-report-2025-26-season/",
            "status": "ok",
            "count": 1,
            "fetched_at_utc": injuries_fetched_at,
            "pdf_links": ["https://example.com/injury.pdf"],
            "pdf_download_status": "ok",
            "selected_pdf_url": "https://example.com/injury.pdf",
            "parse_status": "ok",
            "parse_coverage": 1.0,
            "rows_count": 1,
            "rows": [
                {
                    "player": "Someone Else",
                    "player_norm": "someoneelse",
                    "team": "Boston Celtics",
                    "team_norm": "boston celtics",
                    "status": "out",
                    "note": "Injury/Illness - Knee",
                    "source": "official_nba_pdf",
                }
            ],
        },
        "secondary": {"status": "ok", "rows": [], "count": 0},
    }
    roster_payload = {
        "source": "nba_live_scoreboard",
        "url": "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
        "status": "ok",
        "fetched_at_utc": roster_fetched_at,
        "count_teams": 2,
        "missing_roster_teams": missing_roster_teams or [],
        "teams": {
            "boston celtics": {"active": ["playera"], "inactive": [], "all": ["playera"]},
            "miami heat": {"active": [], "inactive": [], "all": []},
        },
    }
    if roster_fallback:
        roster_payload["fallback"] = roster_fallback
    (context_dir / "injuries.json").write_text(
        json.dumps(injuries_payload, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    (context_dir / "roster.json").write_text(
        json.dumps(roster_payload, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return snapshot_dir


@pytest.fixture
def local_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    return data_dir


def test_strategy_health_healthy_with_missing_injury_informational(
    local_data_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T10-20-00Z"
    now_utc = _iso(datetime.now(UTC))
    _seed_snapshot(
        store=store,
        snapshot_id=snapshot_id,
        with_event_context=True,
        injuries_fetched_at=now_utc,
        roster_fetched_at=now_utc,
    )

    code = main(["strategy", "health", "--snapshot-id", snapshot_id, "--offline"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "healthy"
    assert payload["exit_code"] == 0
    assert payload["counts"]["missing_injury"] > 0
    assert payload["checks"]["injuries"]["pass"] is True
    assert payload["checks"]["roster"]["pass"] is True
    assert payload["checks"]["event_mapping"]["pass"] is True


def test_strategy_health_broken_when_event_mapping_missing(
    local_data_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T10-30-00Z"
    now_utc = _iso(datetime.now(UTC))
    _seed_snapshot(
        store=store,
        snapshot_id=snapshot_id,
        with_event_context=False,
        injuries_fetched_at=now_utc,
        roster_fetched_at=now_utc,
    )

    code = main(["strategy", "health", "--snapshot-id", snapshot_id, "--offline"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["status"] == "broken"
    assert payload["exit_code"] == 2
    assert payload["counts"]["unknown_event"] > 0
    assert "event_mapping_failed" in payload["gates"]


def test_strategy_health_degraded_when_context_is_stale(
    local_data_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T10-40-00Z"
    stale = _iso(datetime.now(UTC) - timedelta(hours=48))
    _seed_snapshot(
        store=store,
        snapshot_id=snapshot_id,
        with_event_context=True,
        injuries_fetched_at=stale,
        roster_fetched_at=stale,
    )

    code = main(["strategy", "health", "--snapshot-id", snapshot_id, "--offline"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "degraded"
    assert payload["exit_code"] == 1
    assert payload["counts"]["stale_inputs"] >= 2
    assert "stale_inputs" in payload["gates"]


def test_strategy_health_degraded_when_roster_fallback_covers_scope(
    local_data_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T10-50-00Z"
    now_utc = _iso(datetime.now(UTC))
    _seed_snapshot(
        store=store,
        snapshot_id=snapshot_id,
        with_event_context=True,
        injuries_fetched_at=now_utc,
        roster_fetched_at=now_utc,
        missing_roster_teams=["boston celtics", "miami heat"],
        roster_fallback={
            "source": "espn_team_rosters",
            "status": "ok",
            "count_teams": 2,
            "fetched_at_utc": now_utc,
        },
    )

    code = main(["strategy", "health", "--snapshot-id", snapshot_id, "--offline"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "degraded"
    assert payload["checks"]["roster"]["pass"] is True
    assert payload["checks"]["roster"]["fallback_covers_missing"] is True
    assert "roster_source_failed" not in payload["gates"]
    assert "roster_fallback_used" in payload["gates"]
