import csv
import json
from pathlib import Path

import pytest

from prop_ev.report_paths import snapshot_reports_dir
from prop_ev.settlement import grade_seed_rows, render_settlement_markdown, settle_snapshot
from prop_ev.storage import SnapshotStore


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

    rows = grade_seed_rows(
        seed_rows=seed_rows,
        results_payload=_results_payload(),
        source="nba_live_scoreboard_boxscore",
    )
    by_key = {str(row["ticket_key"]): row for row in rows}

    assert by_key["t1"]["result"] == "win"
    assert by_key["t1"]["result_reason"] == "final_settled"
    assert by_key["t2"]["result"] == "push"
    assert by_key["t3"]["result"] == "pending"
    assert by_key["t3"]["result_reason"] == "in_progress_pending"
    assert by_key["t4"]["result"] == "unresolved"
    assert by_key["t4"]["result_reason"] == "unsupported_market"


def test_grade_seed_rows_preserves_pricing_quality_fields() -> None:
    seed_row = _seed_row(
        ticket_key="t1",
        player="Player One",
        market="player_points",
        side="over",
        point=20.5,
    )
    seed_row.update(
        {
            "selected_price_american": "-110.0",
            "model_p_hit": 0.62,
            "p_hit_low": 0.58,
            "p_hit_high": 0.66,
            "fair_p_hit": 0.57,
            "best_ev": 0.08,
            "ev_low": 0.03,
            "ev_high": 0.12,
            "quality_score": 0.71,
            "depth_score": 0.65,
            "hold_score": 0.74,
            "dispersion_score": 0.68,
            "freshness_score": 0.92,
            "uncertainty_band": 0.06,
            "summary_candidate_lines": 190,
            "summary_eligible_lines": 104,
        }
    )
    rows = grade_seed_rows(
        seed_rows=[seed_row],
        results_payload=_results_payload(),
        source="nba_live_scoreboard_boxscore",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["selected_price_american"] == -110
    assert row["p_hit_low"] == 0.58
    assert row["p_hit_high"] == 0.66
    assert row["best_ev"] == 0.08
    assert row["ev_low"] == 0.03
    assert row["quality_score"] == 0.71
    assert row["uncertainty_band"] == 0.06
    assert row["summary_candidate_lines"] == 190
    assert row["summary_eligible_lines"] == 104


def test_grade_seed_rows_resolves_player_aliases() -> None:
    seed_rows = [
        _seed_row(
            ticket_key="suffix",
            player="Robert Williams",
            market="player_points",
            side="over",
            point=2.5,
        ),
        _seed_row(
            ticket_key="nickname",
            player="Carlton Carrington",
            market="player_points",
            side="over",
            point=2.5,
        ),
    ]
    payload = {
        "status": "ok",
        "source": "nba_data_schedule_plus_boxscore",
        "fetched_at_utc": "2026-02-12T01:00:00Z",
        "games": [
            {
                "game_id": "g-final",
                "home_team": "home team",
                "away_team": "away team",
                "game_status": "final",
                "game_status_text": "Final",
                "players": {
                    "robertwilliamsiii": {
                        "name": "Robert Williams III",
                        "statistics": {"points": 5},
                    },
                    "bubcarrington": {
                        "name": "Bub Carrington",
                        "statistics": {"points": 6},
                    },
                },
            }
        ],
        "errors": [],
    }
    rows = grade_seed_rows(
        seed_rows=seed_rows,
        results_payload=payload,
        source="nba_data_schedule_plus_boxscore",
    )
    by_key = {str(row["ticket_key"]): row for row in rows}
    assert by_key["suffix"]["result"] == "win"
    assert by_key["suffix"]["result_reason"] == "final_settled"
    assert by_key["nickname"]["result"] == "win"
    assert by_key["nickname"]["result_reason"] == "final_settled"


def test_render_settlement_markdown_uses_compact_labels() -> None:
    report = {
        "snapshot_id": "snap-1",
        "generated_at_utc": "2026-02-12T01:00:00Z",
        "status": "complete",
        "counts": {
            "total": 1,
            "win": 1,
            "loss": 0,
            "push": 0,
            "pending": 0,
            "unresolved": 0,
            "final_games": 1,
            "in_progress_games": 0,
            "scheduled_games": 0,
        },
        "source_details": {"source": "nba_live_scoreboard_boxscore"},
        "rows": [
            {
                "player": "Player One",
                "strategy_id": "balanced_combo",
                "away_team": "Indiana Pacers",
                "home_team": "Brooklyn Nets",
                "game": "Indiana Pacers @ Brooklyn Nets",
                "market": "player_points_rebounds_assists",
                "recommended_side": "over",
                "selected_book": "draftkings",
                "selected_price_american": 125,
                "model_p_hit": 0.618,
                "edge_pct": 14.42,
                "ev_per_100": 14.42,
                "point": 28.5,
                "actual_stat_value": 31.0,
                "result": "win",
                "result_reason": "final_settled",
                "game_status": "final",
                "game_status_text": "Final",
            }
        ],
    }

    markdown = render_settlement_markdown(report)

    assert "| Strategy | Tickets | Avg pHit | Avg Edge% | Avg EV/100 |" in markdown
    assert "| balanced_combo | 1 | 61.8% | +14.42% | 14.42 |" in markdown
    assert "| Player | Game | Mkt | Side | Line | Book/Price | pHit | Edge% | EV/100 |" in markdown
    assert (
        "| Player One | IND @ BKN | PRA | O | 28.50 | draftkings +125 | 61.8% | +14.42% | "
        "14.42 | 31 | W | settled | Final |"
    ) in markdown
    assert "Legend: `Mkt` uses short labels" in markdown


def test_settle_snapshot_writes_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot_dir = tmp_path / "data" / "odds_api" / "snapshots" / "snap-1"
    store = SnapshotStore(tmp_path / "data" / "odds_api")
    store.ensure_snapshot("snap-1")
    reports_dir = snapshot_reports_dir(store, "snap-1")
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
        "prop_ev.settlement.NBARepository.load_results_for_settlement",
        lambda self, *, seed_rows, offline, refresh, mode: (
            _results_payload(),
            self.snapshot_dir / "context" / "results.json",
        ),
    )

    report = settle_snapshot(
        snapshot_dir=snapshot_dir,
        reports_dir=reports_dir,
        snapshot_id="snap-1",
        seed_path=seed_path,
        offline=False,
        refresh_results=True,
        write_csv=True,
        results_source="live",
    )

    assert report["status"] == "partial"
    assert report["exit_code"] == 1
    artifacts = report["artifacts"]
    assert Path(str(artifacts["json"])).exists()
    assert artifacts["md"] == ""
    assert artifacts["tex"] == ""
    assert Path(str(artifacts["meta"])).exists()
    assert Path(str(artifacts["csv"])).exists()
    assert report["source_details"]["results_source_mode"] == "live"
    assert report["pdf_status"] in {"ok", "missing_tool", "failed"}
    with Path(str(artifacts["csv"])).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
    assert "p_hit_low" in columns
    assert "ev_low" in columns
    assert "quality_score" in columns
    assert "summary_candidate_lines" in columns


def test_settle_snapshot_writes_optional_markdown_and_tex(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot_dir = tmp_path / "data" / "odds_api" / "snapshots" / "snap-1"
    store = SnapshotStore(tmp_path / "data" / "odds_api")
    store.ensure_snapshot("snap-1")
    reports_dir = snapshot_reports_dir(store, "snap-1")
    reports_dir.mkdir(parents=True, exist_ok=True)
    seed_path = reports_dir / "backtest-seed.jsonl"
    seed_path.write_text(
        json.dumps(
            _seed_row(
                ticket_key="t1",
                player="Player One",
                market="player_points",
                side="over",
                point=20.5,
            ),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "prop_ev.settlement.NBARepository.load_results_for_settlement",
        lambda self, *, seed_rows, offline, refresh, mode: (
            _results_payload(),
            self.snapshot_dir / "context" / "results.json",
        ),
    )

    report = settle_snapshot(
        snapshot_dir=snapshot_dir,
        reports_dir=reports_dir,
        snapshot_id="snap-1",
        seed_path=seed_path,
        offline=False,
        refresh_results=True,
        write_csv=False,
        results_source="live",
        write_markdown=True,
        keep_tex=True,
    )

    artifacts = report["artifacts"]
    assert Path(str(artifacts["md"])).exists()
    assert Path(str(artifacts["tex"])).exists()


def test_settle_snapshot_default_schema_includes_auto_results_source_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot_dir = tmp_path / "data" / "odds_api" / "snapshots" / "snap-1"
    store = SnapshotStore(tmp_path / "data" / "odds_api")
    store.ensure_snapshot("snap-1")
    reports_dir = snapshot_reports_dir(store, "snap-1")
    reports_dir.mkdir(parents=True, exist_ok=True)
    seed_path = reports_dir / "backtest-seed.jsonl"
    seed_path.write_text(
        json.dumps(
            _seed_row(
                ticket_key="t1",
                player="Player One",
                market="player_points",
                side="over",
                point=20.5,
            ),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "prop_ev.settlement.NBARepository.load_results_for_settlement",
        lambda self, *, seed_rows, offline, refresh, mode: (
            _results_payload(),
            self.snapshot_dir / "context" / "results.json",
        ),
    )

    report = settle_snapshot(
        snapshot_dir=snapshot_dir,
        reports_dir=reports_dir,
        snapshot_id="snap-1",
        seed_path=seed_path,
        offline=False,
        refresh_results=False,
        write_csv=False,
    )

    source_details = report.get("source_details", {})
    assert isinstance(source_details, dict)
    assert source_details.get("results_source_mode") == "auto"


def test_settle_snapshot_offline_forces_cache_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot_dir = tmp_path / "data" / "odds_api" / "snapshots" / "snap-1"
    store = SnapshotStore(tmp_path / "data" / "odds_api")
    store.ensure_snapshot("snap-1")
    reports_dir = snapshot_reports_dir(store, "snap-1")
    reports_dir.mkdir(parents=True, exist_ok=True)
    seed_path = reports_dir / "backtest-seed.jsonl"
    seed_path.write_text(
        json.dumps(
            _seed_row(
                ticket_key="t1",
                player="Player One",
                market="player_points",
                side="over",
                point=20.5,
            ),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def _fake_load_results(
        self, *, seed_rows, offline: bool, refresh: bool, mode: str
    ) -> tuple[dict, Path]:
        captured["offline"] = offline
        captured["refresh"] = refresh
        captured["mode"] = mode
        return _results_payload(), self.snapshot_dir / "context" / "results.json"

    monkeypatch.setattr(
        "prop_ev.settlement.NBARepository.load_results_for_settlement",
        _fake_load_results,
    )

    report = settle_snapshot(
        snapshot_dir=snapshot_dir,
        reports_dir=reports_dir,
        snapshot_id="snap-1",
        seed_path=seed_path,
        offline=True,
        refresh_results=True,
        write_csv=False,
        results_source="live",
    )

    assert captured == {"offline": True, "refresh": False, "mode": "cache_only"}
    source_details = report.get("source_details", {})
    assert isinstance(source_details, dict)
    assert source_details.get("results_source_mode") == "cache_only"
