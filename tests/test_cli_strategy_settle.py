import json
from pathlib import Path

import pytest

from prop_ev import runtime_config
from prop_ev.cli import main
from prop_ev.report_paths import snapshot_reports_dir
from prop_ev.storage import SnapshotStore


@pytest.fixture
def local_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data" / "odds_api"
    config_path = tmp_path / "runtime.toml"
    config_path.write_text(
        "\n".join(
            [
                "[paths]",
                f'odds_data_dir = "{data_dir}"',
                f'nba_data_dir = "{tmp_path / "data" / "nba_data"}"',
                f'reports_dir = "{tmp_path / "data" / "reports" / "odds"}"',
                f'runtime_dir = "{tmp_path / "data" / "runtime"}"',
                'bookmakers_config_path = "config/bookmakers.json"',
                "",
                "[odds_api]",
                'key_files = ["ODDS_API_KEY.ignore"]',
                "",
                "[openai]",
                'key_files = ["OPENAI_KEY.ignore"]',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(runtime_config, "DEFAULT_CONFIG_PATH", config_path)
    monkeypatch.setattr(
        runtime_config,
        "DEFAULT_LOCAL_OVERRIDE_PATH",
        tmp_path / "runtime.local.toml",
    )
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
    store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    seed_path = reports_dir / "backtest-seed.jsonl"
    _write_seed(seed_path, player="Player One", side="over", point=20.5)

    monkeypatch.setattr(
        "prop_ev.settlement.NBARepository.load_results_for_settlement",
        lambda self, *, seed_rows, offline, refresh, mode: (
            _results_payload(status="in_progress", points=12),
            self.snapshot_dir / "context" / "results.json",
        ),
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
    assert (reports_dir / "settlement.json").exists()
    assert not (reports_dir / "settlement.md").exists()
    assert not (reports_dir / "settlement.tex").exists()


def test_strategy_settle_returns_complete_exit_code(
    local_data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "snap-1"
    store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    seed_path = reports_dir / "backtest-seed.jsonl"
    _write_seed(seed_path, player="Player One", side="over", point=20.5)

    monkeypatch.setattr(
        "prop_ev.settlement.NBARepository.load_results_for_settlement",
        lambda self, *, seed_rows, offline, refresh, mode: (
            _results_payload(status="final", points=25),
            self.snapshot_dir / "context" / "results.json",
        ),
    )

    code = main(["strategy", "settle", "--snapshot-id", snapshot_id, "--refresh-results"])
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert code == 0
    assert payload["status"] == "complete"
    assert payload["counts"]["win"] == 1
    assert payload["exit_code"] == 0


def test_strategy_settle_offline_forces_cache_only(
    local_data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "snap-1"
    store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    seed_path = reports_dir / "backtest-seed.jsonl"
    _write_seed(seed_path, player="Player One", side="over", point=20.5)
    captured: dict[str, object] = {}

    def _fake_load_results(
        self, *, seed_rows, offline: bool, refresh: bool, mode: str
    ) -> tuple[dict, Path]:
        captured["offline"] = offline
        captured["refresh"] = refresh
        captured["mode"] = mode
        return _results_payload(
            status="final", points=25
        ), self.snapshot_dir / "context" / "results.json"

    monkeypatch.setattr(
        "prop_ev.settlement.NBARepository.load_results_for_settlement",
        _fake_load_results,
    )

    code = main(
        [
            "strategy",
            "settle",
            "--snapshot-id",
            snapshot_id,
            "--offline",
            "--refresh-results",
            "--results-source",
            "live",
        ]
    )
    _ = capsys.readouterr()
    assert code == 0
    assert captured == {"offline": True, "refresh": False, "mode": "cache_only"}


def test_strategy_settle_falls_back_to_strategy_report_seed(
    local_data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "snap-1"
    store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_payload = {
        "snapshot_id": snapshot_id,
        "strategy_id": "s001",
        "generated_at_utc": "2026-02-13T00:00:00Z",
        "modeled_date_et": "2026-02-12",
        "strategy_mode": "replay",
        "strategy_status": "modeled_with_gates",
        "summary": {"events": 1, "candidate_lines": 1, "eligible_lines": 1},
        "candidates": [
            {
                "eligible": True,
                "event_id": "event-1",
                "game": "Away Team @ Home Team",
                "tip_et": "7:00 PM ET",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "player": "Player One",
                "market": "player_points",
                "recommended_side": "over",
                "point": 20.5,
                "tier": "A",
                "selected_book": "draftkings",
                "selected_price": -110,
                "play_to_american": -115,
                "play_to_decimal": 1.91,
                "model_p_hit": 0.62,
                "fair_p_hit": 0.57,
                "best_ev": 0.08,
                "edge_pct": 8.0,
                "ev_per_100": 8.0,
                "full_kelly": 0.1,
                "quarter_kelly": 0.025,
                "injury_status": "available",
                "roster_status": "active",
                "selected_last_update": "2026-02-13T00:00:00Z",
                "selected_link": "",
                "reason": "eligible",
            }
        ],
    }
    (reports_dir / "strategy-report.json").write_text(
        json.dumps(report_payload, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "prop_ev.settlement.NBARepository.load_results_for_settlement",
        lambda self, *, seed_rows, offline, refresh, mode: (
            _results_payload(status="final", points=25),
            self.snapshot_dir / "context" / "results.json",
        ),
    )

    code = main(["strategy", "settle", "--snapshot-id", snapshot_id, "--no-json"])
    out = capsys.readouterr().out
    assert code == 0
    assert "status=complete" in out
    assert (reports_dir / "settlement.json").exists()
    assert not (reports_dir / "backtest-seed.jsonl").exists()


def test_strategy_settle_prefers_brief_strategy_report_path(
    local_data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "snap-1"
    store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    reports_dir.mkdir(parents=True, exist_ok=True)

    (reports_dir / "backtest-seed.jsonl").write_text(
        json.dumps(
            {
                "ticket_key": "seed-ticket",
                "snapshot_id": snapshot_id,
                "event_id": "event-1",
                "game": "Away Team @ Home Team",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "player": "Seed Player",
                "market": "player_points",
                "recommended_side": "over",
                "point": 10.5,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    execution_report_path = reports_dir / "strategy-report.execution-draftkings.json"
    execution_report_path.write_text(
        json.dumps(
            {
                "snapshot_id": snapshot_id,
                "strategy_id": "s001",
                "generated_at_utc": "2026-02-13T00:00:00Z",
                "modeled_date_et": "2026-02-12",
                "strategy_mode": "replay",
                "strategy_status": "modeled_with_gates",
                "summary": {"events": 1, "candidate_lines": 1, "eligible_lines": 1},
                "candidates": [
                    {
                        "eligible": True,
                        "event_id": "event-1",
                        "game": "Away Team @ Home Team",
                        "tip_et": "7:00 PM ET",
                        "home_team": "Home Team",
                        "away_team": "Away Team",
                        "player": "Meta Player",
                        "market": "player_points",
                        "recommended_side": "over",
                        "point": 20.5,
                        "tier": "A",
                        "selected_book": "draftkings",
                        "selected_price": -110,
                        "play_to_american": -115,
                        "play_to_decimal": 1.91,
                        "model_p_hit": 0.62,
                        "fair_p_hit": 0.57,
                        "best_ev": 0.08,
                        "edge_pct": 8.0,
                        "ev_per_100": 8.0,
                        "full_kelly": 0.1,
                        "quarter_kelly": 0.025,
                        "injury_status": "available",
                        "roster_status": "active",
                        "selected_last_update": "2026-02-13T00:00:00Z",
                        "selected_link": "",
                        "reason": "eligible",
                    }
                ],
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (reports_dir / "strategy-brief.meta.json").write_text(
        json.dumps({"strategy_report_path": str(execution_report_path)}, sort_keys=True, indent=2)
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "prop_ev.settlement.NBARepository.load_results_for_settlement",
        lambda self, *, seed_rows, offline, refresh, mode: (
            _results_payload(status="final", points=25),
            self.snapshot_dir / "context" / "results.json",
        ),
    )

    code = main(["strategy", "settle", "--snapshot-id", snapshot_id])
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert code == 1
    assert payload["status"] == "partial"
    assert payload["source_details"]["seed_source"] == "override"
    assert payload["source_details"]["strategy_report_path"] == str(execution_report_path)
    players = [str(row.get("player", "")) for row in payload.get("rows", [])]
    assert players == ["Meta Player"]
