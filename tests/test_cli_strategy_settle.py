import json
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.storage import SnapshotStore


@pytest.fixture
def local_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    return data_dir


def _write_seed(path: Path, *, player: str, side: str, point: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "ticket_key": "ticket-1",
        "snapshot_id": "snap-1",
        "event_id": "event-1",
        "game": "Away Team @ Home Team",
        "home_team": "Home Team",
        "away_team": "Away Team",
        "player": player,
        "market": "player_points",
        "recommended_side": side,
        "point": point,
    }
    path.write_text(json.dumps(row, sort_keys=True) + "\n", encoding="utf-8")


def _results_payload(*, status: str, points: int) -> dict:
    return {
        "status": "ok",
        "source": "nba_live_scoreboard_boxscore",
        "fetched_at_utc": "2026-02-12T01:00:00Z",
        "games": [
            {
                "game_id": "g-1",
                "home_team": "home team",
                "away_team": "away team",
                "game_status": status,
                "game_status_text": "Final" if status == "final" else "Q3 4:21",
                "players": {
                    "playerone": {
                        "name": "Player One",
                        "statistics": {
                            "points": points,
                            "reboundsTotal": 8,
                            "assists": 4,
                            "threePointersMade": 2,
                        },
                    }
                },
            }
        ],
        "errors": [],
    }


def test_strategy_settle_returns_pending_exit_code(
    local_data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "snap-1"
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    seed_path = snapshot_dir / "reports" / "backtest-seed.jsonl"
    _write_seed(seed_path, player="Player One", side="over", point=20.5)

    monkeypatch.setattr(
        "prop_ev.settlement.fetch_nba_live_results",
        lambda *, teams_in_scope: _results_payload(status="in_progress", points=12),
    )

    code = main(
        [
            "strategy",
            "settle",
            "--snapshot-id",
            snapshot_id,
            "--refresh-results",
            "--write-csv",
            "--no-json",
        ]
    )
    out = capsys.readouterr().out
    assert code == 1
    assert "settlement_json=" in out
    assert "pending=1" in out
    assert (snapshot_dir / "reports" / "backtest-settlement.json").exists()
    assert (snapshot_dir / "reports" / "backtest-settlement.md").exists()
    assert (snapshot_dir / "reports" / "backtest-settlement.tex").exists()


def test_strategy_settle_returns_complete_exit_code(
    local_data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "snap-1"
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    seed_path = snapshot_dir / "reports" / "backtest-seed.jsonl"
    _write_seed(seed_path, player="Player One", side="over", point=20.5)

    monkeypatch.setattr(
        "prop_ev.settlement.fetch_nba_live_results",
        lambda *, teams_in_scope: _results_payload(status="final", points=25),
    )

    code = main(["strategy", "settle", "--snapshot-id", snapshot_id, "--refresh-results"])
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert code == 0
    assert payload["status"] == "complete"
    assert payload["counts"]["win"] == 1
    assert payload["exit_code"] == 0
