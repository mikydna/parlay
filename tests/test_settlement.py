import json
from pathlib import Path

import pytest

from prop_ev.settlement import grade_seed_rows, settle_snapshot


def _seed_row(
    *,
    ticket_key: str,
    player: str,
    market: str,
    side: str,
    point: float,
    game: str = "Away Team @ Home Team",
    home_team: str = "Home Team",
    away_team: str = "Away Team",
) -> dict:
    return {
        "ticket_key": ticket_key,
        "snapshot_id": "snap-1",
        "event_id": "event-1",
        "game": game,
        "home_team": home_team,
        "away_team": away_team,
        "player": player,
        "market": market,
        "recommended_side": side,
        "point": point,
    }


def _results_payload() -> dict:
    return {
        "status": "ok",
        "source": "nba_live_scoreboard_boxscore",
        "fetched_at_utc": "2026-02-12T01:00:00Z",
        "games": [
            {
                "game_id": "g-final",
                "home_team": "home team",
                "away_team": "away team",
                "game_status": "final",
                "game_status_text": "Final",
                "players": {
                    "playerone": {
                        "name": "Player One",
                        "statistics": {
                            "points": 25,
                            "reboundsTotal": 10,
                            "assists": 8,
                            "threePointersMade": 4,
                        },
                    },
                    "playertwo": {
                        "name": "Player Two",
                        "statistics": {
                            "points": 12,
                            "reboundsTotal": 10,
                            "assists": 4,
                            "threePointersMade": 1,
                        },
                    },
                },
            },
            {
                "game_id": "g-live",
                "home_team": "live home",
                "away_team": "live away",
                "game_status": "in_progress",
                "game_status_text": "Q3 5:00",
                "players": {
                    "playerthree": {
                        "name": "Player Three",
                        "statistics": {
                            "points": 14,
                            "reboundsTotal": 3,
                            "assists": 2,
                            "threePointersMade": 2,
                        },
                    }
                },
            },
        ],
        "errors": [],
    }


def test_grade_seed_rows_final_push_pending() -> None:
    seed_rows = [
        _seed_row(
            ticket_key="t1",
            player="Player One",
            market="player_points",
            side="over",
            point=20.5,
        ),
        _seed_row(
            ticket_key="t2",
            player="Player Two",
            market="player_rebounds",
            side="under",
            point=10.0,
        ),
        _seed_row(
            ticket_key="t3",
            player="Player Three",
            market="player_assists",
            side="over",
            point=6.5,
            game="Live Away @ Live Home",
            home_team="Live Home",
            away_team="Live Away",
        ),
        _seed_row(
            ticket_key="t4",
            player="Player One",
            market="player_blocks",
            side="over",
            point=1.5,
        ),
    ]

    rows = grade_seed_rows(seed_rows=seed_rows, results_payload=_results_payload())
    by_key = {str(row["ticket_key"]): row for row in rows}

    assert by_key["t1"]["result"] == "win"
    assert by_key["t1"]["result_reason"] == "final_settled"
    assert by_key["t2"]["result"] == "push"
    assert by_key["t3"]["result"] == "pending"
    assert by_key["t3"]["result_reason"] == "in_progress_pending"
    assert by_key["t4"]["result"] == "unresolved"
    assert by_key["t4"]["result_reason"] == "unsupported_market"


def test_settle_snapshot_writes_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot_dir = tmp_path / "data" / "odds_api" / "snapshots" / "snap-1"
    reports_dir = snapshot_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    seed_path = reports_dir / "backtest-seed.jsonl"
    seed_rows = [
        _seed_row(
            ticket_key="t1",
            player="Player One",
            market="player_points",
            side="over",
            point=20.5,
        ),
        _seed_row(
            ticket_key="t3",
            player="Player Three",
            market="player_assists",
            side="over",
            point=6.5,
            game="Live Away @ Live Home",
            home_team="Live Home",
            away_team="Live Away",
        ),
    ]
    seed_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in seed_rows),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "prop_ev.settlement.fetch_nba_live_results",
        lambda *, teams_in_scope: _results_payload(),
    )

    report = settle_snapshot(
        snapshot_dir=snapshot_dir,
        snapshot_id="snap-1",
        seed_path=seed_path,
        offline=False,
        refresh_results=True,
        write_csv=True,
    )

    assert report["status"] == "partial"
    assert report["exit_code"] == 1
    artifacts = report["artifacts"]
    assert Path(str(artifacts["json"])).exists()
    assert Path(str(artifacts["md"])).exists()
    assert Path(str(artifacts["tex"])).exists()
    assert Path(str(artifacts["meta"])).exists()
    assert Path(str(artifacts["csv"])).exists()
    assert report["pdf_status"] in {"ok", "missing_tool", "failed"}
