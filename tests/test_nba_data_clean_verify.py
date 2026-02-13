from __future__ import annotations

from pathlib import Path

import polars as pl

from prop_ev.nba_data.clean.build import build_clean
from prop_ev.nba_data.io_utils import atomic_write_json, atomic_write_jsonl
from prop_ev.nba_data.store.layout import build_layout
from prop_ev.nba_data.store.manifest import (
    ensure_row,
    set_resource_ok,
    write_manifest_deterministic,
)
from prop_ev.nba_data.verify.checks import run_verify


def _seed_one_game(layout, season: str, season_type: str, game_id: str) -> None:
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
                    "minutes": 30,
                    "points": 20,
                    "rebounds": 5,
                    "assists": 6,
                },
                {
                    "team_id": "2",
                    "player_id": "22",
                    "minutes": 31,
                    "points": 18,
                    "rebounds": 7,
                    "assists": 4,
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


def test_clean_build_and_verify(tmp_path: Path) -> None:
    layout = build_layout(tmp_path / "nba_data")
    _seed_one_game(layout, "2025-26", "Regular Season", "g1")

    counts = build_clean(
        layout=layout,
        seasons=["2025-26"],
        season_type="Regular Season",
        overwrite=True,
        schema_version=1,
    )
    assert counts["games"] == 1
    assert counts["boxscore_players"] == 2
    assert counts["pbp_events"] == 2
    assert counts["possessions"] == 2

    games_df = pl.scan_parquet(str(layout.clean_schema_dir(1) / "games" / "**/*.parquet")).collect()
    assert games_df.height == 1
    assert games_df.columns == [
        "season",
        "season_type",
        "game_id",
        "date",
        "home_team_id",
        "away_team_id",
    ]

    counts_again = build_clean(
        layout=layout,
        seasons=["2025-26"],
        season_type="Regular Season",
        overwrite=True,
        schema_version=1,
    )
    assert counts_again == counts

    code, report = run_verify(
        layout=layout,
        seasons=["2025-26"],
        season_type="Regular Season",
        schema_version=1,
        fail_on_warn=False,
    )
    assert code == 1
    assert report["counts"]["games"] == 1
    assert report["warnings"]


def test_verify_filters_by_season(tmp_path: Path) -> None:
    layout = build_layout(tmp_path / "nba_data")
    _seed_one_game(layout, "2024-25", "Regular Season", "g1")
    _seed_one_game(layout, "2025-26", "Regular Season", "g2")

    counts = build_clean(
        layout=layout,
        seasons=["2024-25", "2025-26"],
        season_type="Regular Season",
        overwrite=True,
        schema_version=1,
    )
    assert counts["games"] == 2

    code, report = run_verify(
        layout=layout,
        seasons=["2025-26"],
        season_type="Regular Season",
        schema_version=1,
        fail_on_warn=False,
    )
    assert code == 1
    assert report["counts"]["games"] == 1
    assert report["counts"]["boxscore_players"] == 2
