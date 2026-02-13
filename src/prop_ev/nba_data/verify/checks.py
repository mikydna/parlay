"""Integrity and sanity checks for clean NBA datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from prop_ev.nba_data.io_utils import atomic_write_json
from prop_ev.nba_data.schema_version import SCHEMA_VERSION
from prop_ev.nba_data.store.layout import NBADataLayout, slugify_season_type


def _scan_dataset(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    return pl.scan_parquet(str(path / "**/*.parquet")).collect()


def _scan_table(*, base: Path, table: str, seasons: list[str], season_slug: str) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for season in seasons:
        partition = base / table / f"season={season}" / f"season_type={season_slug}"
        if not partition.exists():
            continue
        frames.append(_scan_dataset(partition))
    if not frames:
        return pl.DataFrame()
    if len(frames) == 1:
        return frames[0]
    return pl.concat(frames, how="vertical")


def run_verify(
    *,
    layout: NBADataLayout,
    seasons: list[str],
    season_type: str,
    schema_version: int,
    fail_on_warn: bool,
) -> tuple[int, dict[str, Any]]:
    schema = SCHEMA_VERSION if schema_version <= 0 else schema_version
    base = layout.clean_schema_dir(schema)
    season_slug = slugify_season_type(season_type)

    games = _scan_table(base=base, table="games", seasons=seasons, season_slug=season_slug)
    boxscore = _scan_table(
        base=base,
        table="boxscore_players",
        seasons=seasons,
        season_slug=season_slug,
    )
    pbp = _scan_table(base=base, table="pbp_events", seasons=seasons, season_slug=season_slug)
    possessions = _scan_table(
        base=base,
        table="possessions",
        seasons=seasons,
        season_slug=season_slug,
    )

    failures: list[str] = []
    warnings: list[str] = []

    game_ids = set(games.get_column("game_id").to_list()) if "game_id" in games.columns else set()

    for table_name, frame in (
        ("boxscore_players", boxscore),
        ("pbp_events", pbp),
        ("possessions", possessions),
    ):
        if "game_id" not in frame.columns:
            continue
        missing = set(frame.get_column("game_id").to_list()) - game_ids
        if missing:
            failures.append(f"{table_name}: missing game_id references={len(missing)}")

    if {"game_id", "event_num"} <= set(pbp.columns):
        dup = pbp.group_by(["game_id", "event_num"]).len().filter(pl.col("len") > 1)
        if dup.height > 0:
            failures.append(f"pbp_events duplicate (game_id,event_num) rows={dup.height}")

    if {"home_team_id", "away_team_id"} <= set(games.columns):
        missing_home = games.filter(pl.col("home_team_id").fill_null("").str.strip_chars() == "")
        missing_away = games.filter(pl.col("away_team_id").fill_null("").str.strip_chars() == "")
        if missing_home.height > 0:
            failures.append(f"games missing home_team_id rows={missing_home.height}")
        if missing_away.height > 0:
            failures.append(f"games missing away_team_id rows={missing_away.height}")

    if {"game_id", "possession_id"} <= set(possessions.columns):
        dup = possessions.group_by(["game_id", "possession_id"]).len().filter(pl.col("len") > 1)
        if dup.height > 0:
            failures.append(f"possessions duplicate (game_id,possession_id) rows={dup.height}")

    if {"game_id", "team_id", "minutes"} <= set(boxscore.columns):
        minute_rows = (
            boxscore.with_columns(pl.col("minutes").fill_null(0.0).cast(pl.Float64, strict=False))
            .group_by(["game_id", "team_id"])
            .agg(pl.sum("minutes").alias("team_minutes"))
        )
        low_warn = minute_rows.filter(pl.col("team_minutes") < 240.0)
        low_fail = minute_rows.filter(pl.col("team_minutes") < 200.0)
        high_warn = minute_rows.filter(pl.col("team_minutes") > 400.0)
        if low_warn.height > 0:
            warnings.append(f"team minutes <240 rows={low_warn.height}")
        if high_warn.height > 0:
            warnings.append(f"team minutes >400 rows={high_warn.height}")
        if low_fail.height > 0:
            failures.append(f"team minutes <200 rows={low_fail.height}")

    report = {
        "schema_version": schema,
        "seasons": seasons,
        "season_type": season_type,
        "counts": {
            "games": games.height,
            "boxscore_players": boxscore.height,
            "pbp_events": pbp.height,
            "possessions": possessions.height,
        },
        "failures": failures,
        "warnings": warnings,
    }
    out_path = layout.verify_report_path(
        season="multi" if len(seasons) != 1 else seasons[0],
        season_type=season_type,
        schema_version=schema,
    )
    atomic_write_json(out_path, report)

    if failures:
        return 1, report
    if fail_on_warn and warnings:
        return 1, report
    return 0, report
