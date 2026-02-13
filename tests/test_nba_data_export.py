from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from prop_ev.nba_data.clean.build import build_clean
from prop_ev.nba_data.cli import main
from prop_ev.nba_data.io_utils import atomic_write_json, atomic_write_jsonl
from prop_ev.nba_data.store.layout import build_layout
from prop_ev.nba_data.store.manifest import (
    ensure_row,
    set_resource_ok,
    write_manifest_deterministic,
)
from prop_ev.nba_data.verify.checks import run_verify


def _seed_raw_game(data_dir: Path, season: str, season_type: str, game_id: str) -> None:
    layout = build_layout(data_dir)
    schedule_path = layout.schedule_path(season=season, season_type=season_type)
    atomic_write_json(
        schedule_path,
        {
            "games": [
                {
                    "game_id": game_id,
                    "date": "2026-01-01",
                    "home_team_id": "1",
                    "away_team_id": "2",
                }
            ]
        },
    )

    manifest_path = layout.manifest_path(season=season, season_type=season_type)
    rows = {}
    row = ensure_row(rows, season=season, season_type=season_type, game_id=game_id)

    box_path = layout.raw_resource_path(
        resource="boxscore",
        season=season,
        season_type="regular_season",
        game_id=game_id,
        ext="json",
    )
    atomic_write_json(
        box_path,
        {
            "players": [
                {
                    "team_id": "1",
                    "player_id": "11",
                    "minutes": 240.0,
                    "points": 100,
                    "rebounds": 50,
                    "assists": 40,
                },
                {
                    "team_id": "2",
                    "player_id": "22",
                    "minutes": 240.0,
                    "points": 98,
                    "rebounds": 48,
                    "assists": 39,
                },
            ]
        },
    )
    pbp_path = layout.raw_resource_path(
        resource="enhanced_pbp",
        season=season,
        season_type="regular_season",
        game_id=game_id,
        ext="jsonl",
    )
    atomic_write_jsonl(
        pbp_path,
        [
            {"event_num": 1, "clock": "12:00", "event_type": "jump", "team_id": "1"},
            {"event_num": 2, "clock": "11:45", "event_type": "shot", "team_id": "2"},
        ],
    )
    poss_path = layout.raw_resource_path(
        resource="possessions",
        season=season,
        season_type="regular_season",
        game_id=game_id,
        ext="jsonl",
    )
    atomic_write_jsonl(
        poss_path,
        [
            {"possession_id": 1, "start_event_num": 1, "end_event_num": 2, "offense_team_id": "1"},
            {"possession_id": 2, "start_event_num": 3, "end_event_num": 4, "offense_team_id": "2"},
        ],
    )

    set_resource_ok(
        root=layout.root, row=row, resource="boxscore", provider="data_nba", path=box_path
    )
    set_resource_ok(
        root=layout.root,
        row=row,
        resource="enhanced_pbp",
        provider="data_nba",
        path=pbp_path,
    )
    set_resource_ok(
        root=layout.root,
        row=row,
        resource="possessions",
        provider="data_nba",
        path=poss_path,
    )
    write_manifest_deterministic(manifest_path, rows)


def test_export_clean_copies_nba_artifacts(tmp_path: Path) -> None:
    source_dir = tmp_path / "source_nba_data"
    destination_dir = tmp_path / "destination_nba_data"
    _seed_raw_game(source_dir, "2025-26", "Regular Season", "g1")

    source_layout = build_layout(source_dir)
    build_clean(
        layout=source_layout,
        seasons=["2025-26"],
        season_type="Regular Season",
        overwrite=True,
        schema_version=1,
    )
    run_verify(
        layout=source_layout,
        seasons=["2025-26"],
        season_type="Regular Season",
        schema_version=1,
        fail_on_warn=False,
    )

    code = main(
        [
            "export",
            "clean",
            "--data-dir",
            str(source_dir),
            "--dst-data-dir",
            str(destination_dir),
            "--seasons",
            "2025-26",
            "--season-type",
            "Regular Season",
            "--schema-version",
            "1",
        ]
    )
    assert code == 0

    destination_layout = build_layout(destination_dir)
    assert (
        destination_layout.clean_schema_dir(1)
        / "games"
        / "season=2025-26"
        / "season_type=regular_season"
        / "part-00000.parquet"
    ).exists()
    assert destination_layout.manifest_path(season="2025-26", season_type="Regular Season").exists()
    assert destination_layout.schedule_path(season="2025-26", season_type="Regular Season").exists()
    assert destination_layout.verify_report_path(
        season="2025-26", season_type="Regular Season", schema_version=1
    ).exists()


@pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd binary required")
def test_export_raw_archive_writes_archive_and_manifest(tmp_path: Path) -> None:
    source_dir = tmp_path / "source_nba_data"
    destination_dir = tmp_path / "destination_nba_data"
    _seed_raw_game(source_dir, "2025-26", "Regular Season", "g1")

    code = main(
        [
            "export",
            "raw-archive",
            "--data-dir",
            str(source_dir),
            "--dst-data-dir",
            str(destination_dir),
            "--seasons",
            "2025-26",
            "--season-type",
            "Regular Season",
        ]
    )
    assert code == 0

    archive_path = (
        destination_dir
        / "raw_archives"
        / "season=2025-26"
        / "season_type=regular_season"
        / "raw.tar.zst"
    )
    manifest_path = destination_dir / "raw_archives" / "manifest.json"
    assert archive_path.exists()
    assert manifest_path.exists()

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    archives = payload.get("archives", [])
    assert isinstance(archives, list)
    assert len(archives) == 1
    row = archives[0]
    assert (
        row["archive_path"] == "raw_archives/season=2025-26/season_type=regular_season/raw.tar.zst"
    )
    assert row["sha256"]
    assert row["bytes"] > 0
    assert row["created_at_utc"]
