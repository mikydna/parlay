from __future__ import annotations

import json
from pathlib import Path

import pytest

from prop_ev.nba_data.repo import NBARepository


def _make_repo(tmp_path: Path, *, snapshot_id: str) -> NBARepository:
    odds_root = tmp_path / "odds_api"
    snapshot_dir = odds_root / "snapshots" / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    nba_root = tmp_path / "nba_data"
    nba_root.mkdir(parents=True, exist_ok=True)
    return NBARepository(
        odds_data_root=odds_root,
        snapshot_id=snapshot_id,
        snapshot_dir=snapshot_dir,
        nba_data_root=nba_root,
    )


def test_fetch_results_auto_prefers_historical_for_old_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _make_repo(tmp_path, snapshot_id="daily-20000101T000000Z")

    monkeypatch.setattr(
        repo,
        "_fetch_historical_results",
        lambda **kwargs: {
            "source": "hist",
            "fetched_at_utc": "2000-01-01T00:00:00Z",
            "status": "ok",
            "games": [{"game_id": "g1"}],
            "errors": [],
            "count_games": 1,
            "count_errors": 0,
        },
    )
    monkeypatch.setattr(
        repo,
        "_fetch_live_results",
        lambda **kwargs: {
            "source": "live",
            "fetched_at_utc": "2000-01-01T00:00:00Z",
            "status": "ok",
            "games": [{"game_id": "g2"}],
            "errors": [],
            "count_games": 1,
            "count_errors": 0,
        },
    )

    payload = repo._fetch_results(
        mode="auto",
        teams_in_scope=set(),
        snapshot_day="2000-01-01",
        refresh=True,
    )
    assert payload["source"] == "hist"


def test_fetch_results_auto_prefers_live_for_future_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _make_repo(tmp_path, snapshot_id="daily-29990101T000000Z")

    monkeypatch.setattr(
        repo,
        "_fetch_historical_results",
        lambda **kwargs: {
            "source": "hist",
            "fetched_at_utc": "2999-01-01T00:00:00Z",
            "status": "ok",
            "games": [{"game_id": "g1"}],
            "errors": [],
            "count_games": 1,
            "count_errors": 0,
        },
    )
    monkeypatch.setattr(
        repo,
        "_fetch_live_results",
        lambda **kwargs: {
            "source": "live",
            "fetched_at_utc": "2999-01-01T00:00:00Z",
            "status": "ok",
            "games": [{"game_id": "g2"}],
            "errors": [],
            "count_games": 1,
            "count_errors": 0,
        },
    )

    payload = repo._fetch_results(
        mode="auto",
        teams_in_scope=set(),
        snapshot_day="2999-01-01",
        refresh=True,
    )
    assert payload["source"] == "live"


def test_historical_game_ids_for_day_reads_schedule_files(tmp_path: Path) -> None:
    repo = _make_repo(tmp_path, snapshot_id="daily-20260212T000000Z")
    schedule_path = (
        repo.nba_data_root
        / "raw"
        / "schedule"
        / "season=2025-26"
        / "season_type=regular_season"
        / "schedule.json"
    )
    schedule_path.parent.mkdir(parents=True, exist_ok=True)
    schedule_path.write_text(
        json.dumps(
            {
                "games": [
                    {"game_id": "g1", "date": "2026-02-12"},
                    {"game_id": "g2", "date": "2026-02-13"},
                ]
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    assert repo._historical_game_ids_for_day("2026-02-12") == ["g1"]


def test_load_strategy_context_uses_repository_fetchers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _make_repo(tmp_path, snapshot_id="daily-20260212T000000Z")

    monkeypatch.setattr(
        "prop_ev.nba_data.repo.fetch_official_injury_links",
        lambda pdf_cache_dir=None: {"status": "ok", "rows": [], "source": "official_nba"},
    )
    monkeypatch.setattr(
        "prop_ev.nba_data.repo.fetch_secondary_injuries",
        lambda: {"status": "ok", "rows": [], "source": "secondary"},
    )
    monkeypatch.setattr(
        "prop_ev.nba_data.repo.fetch_roster_context",
        lambda teams_in_scope=None: {
            "status": "ok",
            "source": "roster_source",
            "teams": {
                "oklahoma city thunder": {"all": ["shai"], "active": ["shai"], "inactive": []}
            },
            "games": [],
        },
    )

    injuries, roster, injuries_path, roster_path = repo.load_strategy_context(
        teams_in_scope=["oklahoma city thunder"],
        offline=False,
        refresh=True,
        injuries_stale_hours=6.0,
        roster_stale_hours=24.0,
    )
    assert injuries_path.exists()
    assert roster_path.exists()
    assert repo.nba_data_root in injuries_path.parents
    assert repo.nba_data_root in roster_path.parents
    assert injuries.get("official", {}).get("status") == "ok"
    assert roster.get("status") == "ok"
    context_ref = repo.snapshot_dir / "context_ref.json"
    assert context_ref.exists()
    context_payload = json.loads(context_ref.read_text(encoding="utf-8"))
    assert "injuries" in context_payload.get("context", {})
    assert "roster" in context_payload.get("context", {})


def test_context_paths_use_canonical_nba_root_only(tmp_path: Path) -> None:
    repo = _make_repo(tmp_path, snapshot_id="daily-20260212T000000Z")
    legacy_context = repo.snapshot_dir / "context"
    legacy_context.mkdir(parents=True, exist_ok=True)
    (legacy_context / "injuries.json").write_text('{"status":"legacy"}\n', encoding="utf-8")
    (legacy_context / "roster.json").write_text('{"status":"legacy"}\n', encoding="utf-8")
    (legacy_context / "results.json").write_text('{"status":"legacy"}\n', encoding="utf-8")

    injuries_path, roster_path, results_path = repo.context_paths()
    assert injuries_path == repo.context_dir / "injuries.json"
    assert roster_path == repo.context_dir / "roster.json"
    assert results_path == repo.context_dir / "results.json"


def test_repo_uses_configured_nba_data_dir_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    odds_root = tmp_path / "odds_api"
    snapshot_id = "daily-20260212T000000Z"
    snapshot_dir = odds_root / "snapshots" / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    configured_root = tmp_path / "configured_nba_data"
    monkeypatch.setenv("PROP_EV_NBA_DATA_DIR", str(configured_root))

    repo = NBARepository(
        odds_data_root=odds_root,
        snapshot_id=snapshot_id,
        snapshot_dir=snapshot_dir,
    )

    assert repo.nba_data_root == configured_root.resolve()


def test_repo_prefers_sibling_nba_data_when_default_config(tmp_path: Path) -> None:
    odds_root = tmp_path / "odds_api"
    snapshot_id = "daily-20260212T000000Z"
    snapshot_dir = odds_root / "snapshots" / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    sibling_nba = tmp_path / "nba_data"
    sibling_nba.mkdir(parents=True, exist_ok=True)

    repo = NBARepository(
        odds_data_root=odds_root,
        snapshot_id=snapshot_id,
        snapshot_dir=snapshot_dir,
    )

    assert repo.nba_data_root == sibling_nba.resolve()
