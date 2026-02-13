from __future__ import annotations

import json
from pathlib import Path

from prop_ev.nba_data.cli import main
from prop_ev.nba_data.ingest.discover import discover_games
from prop_ev.nba_data.store.layout import build_layout
from prop_ev.nba_data.store.manifest import load_manifest


def test_discover_writes_schedule_and_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data" / "nba_data"

    def _fake_discover_games(**kwargs):
        return [
            {
                "game_id": "g2",
                "date": "2026-01-02",
                "home_team_id": "10",
                "away_team_id": "20",
            },
            {
                "game_id": "g1",
                "date": "2026-01-01",
                "home_team_id": "30",
                "away_team_id": "40",
            },
        ]

    monkeypatch.setattr("prop_ev.nba_data.cli.discover_games", _fake_discover_games)
    code = main(
        [
            "discover",
            "--data-dir",
            str(data_dir),
            "--seasons",
            "2025-26",
            "--season-type",
            "Regular Season",
        ]
    )
    assert code == 0

    layout = build_layout(data_dir)
    schedule_path = layout.schedule_path(season="2025-26", season_type="Regular Season")
    payload = json.loads(schedule_path.read_text(encoding="utf-8"))
    assert [item["game_id"] for item in payload["games"]] == ["g2", "g1"]

    manifest_path = layout.manifest_path(season="2025-26", season_type="Regular Season")
    rows = load_manifest(manifest_path)
    assert ("2025-26", "regular_season", "g1") in rows
    assert ("2025-26", "regular_season", "g2") in rows


def test_discover_games_extracts_visitor_team_id(tmp_path: Path, monkeypatch) -> None:
    layout = build_layout(tmp_path / "nba_data")

    def _fake_build_client(**kwargs):
        return object()

    def _fake_discover_final_games(client, *, season: str, season_type: str):
        return [
            {
                "game_id": "g1",
                "date": "2026-01-01",
                "home_team_id": 1610612760,
                "visitor_team_id": 1610612745,
                "status": "Final",
            }
        ]

    monkeypatch.setattr("prop_ev.nba_data.ingest.discover.build_client", _fake_build_client)
    monkeypatch.setattr(
        "prop_ev.nba_data.ingest.discover.discover_final_games",
        _fake_discover_final_games,
    )

    games = discover_games(
        layout=layout,
        season="2025-26",
        season_type="Regular Season",
        provider_games="stats_nba",
    )
    assert games == [
        {
            "game_id": "g1",
            "date": "2026-01-01",
            "home_team_id": "1610612760",
            "away_team_id": "1610612745",
        }
    ]
