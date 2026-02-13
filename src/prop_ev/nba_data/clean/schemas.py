"""Column and dtype schemas for clean NBA datasets."""

from __future__ import annotations

from typing import Any

import polars as pl

TABLE_SCHEMAS: dict[str, list[tuple[str, Any]]] = {
    "games": [
        ("season", pl.Utf8),
        ("season_type", pl.Utf8),
        ("game_id", pl.Utf8),
        ("date", pl.Utf8),
        ("home_team_id", pl.Utf8),
        ("away_team_id", pl.Utf8),
    ],
    "boxscore_players": [
        ("season", pl.Utf8),
        ("season_type", pl.Utf8),
        ("game_id", pl.Utf8),
        ("team_id", pl.Utf8),
        ("player_id", pl.Utf8),
        ("minutes", pl.Float64),
        ("points", pl.Float64),
        ("rebounds", pl.Float64),
        ("assists", pl.Float64),
    ],
    "pbp_events": [
        ("season", pl.Utf8),
        ("season_type", pl.Utf8),
        ("game_id", pl.Utf8),
        ("event_num", pl.Int64),
        ("clock", pl.Utf8),
        ("event_type", pl.Utf8),
        ("team_id", pl.Utf8),
        ("player_id", pl.Utf8),
        ("description", pl.Utf8),
    ],
    "possessions": [
        ("season", pl.Utf8),
        ("season_type", pl.Utf8),
        ("game_id", pl.Utf8),
        ("possession_id", pl.Int64),
        ("start_event_num", pl.Int64),
        ("end_event_num", pl.Int64),
        ("offense_team_id", pl.Utf8),
        ("defense_team_id", pl.Utf8),
    ],
}

TABLE_SORT_KEYS: dict[str, list[str]] = {
    "games": ["game_id"],
    "boxscore_players": ["game_id", "team_id", "player_id"],
    "pbp_events": ["game_id", "event_num"],
    "possessions": ["game_id", "possession_id"],
}


def enforce_schema(table: str, frame: pl.DataFrame) -> pl.DataFrame:
    """Apply schema with deterministic column ordering and dtypes."""
    schema = TABLE_SCHEMAS[table]
    columns = [name for name, _ in schema]
    working = frame
    for name, dtype in schema:
        if name not in working.columns:
            working = working.with_columns(pl.lit(None).cast(dtype).alias(name))
        else:
            working = working.with_columns(pl.col(name).cast(dtype, strict=False))
    return working.select(columns)
