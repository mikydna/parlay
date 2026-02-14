"""Feature extraction for probabilistic minutes modeling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl

from prop_ev.nba_data.store.layout import NBADataLayout, slugify_season_type

FEATURE_COLUMNS: tuple[str, ...] = (
    "prev_minutes_mean",
    "prev_minutes_std",
    "prev_minutes_short",
    "prev_minutes_trend",
    "prev_active_rate",
    "games_played",
    "games_on_team",
    "days_on_team",
    "new_team_phase_ord",
    "team_prev_minutes_mean",
    "team_prev_active_count",
)

REQUIRED_COLUMNS: tuple[str, ...] = (
    "season",
    "season_type",
    "game_id",
    "game_date",
    "player_id",
    "team_id",
    "actual_minutes",
    "active_target",
    "new_team_phase",
    *FEATURE_COLUMNS,
)


@dataclass(frozen=True)
class MinutesProbFeatureConfig:
    seasons: list[str]
    season_type: str
    history_games: int
    min_history_games: int
    eval_days: int
    schema_version: int


def _clean_table_glob(layout: NBADataLayout, *, table: str, schema_version: int) -> str:
    return str(layout.clean_schema_dir(schema_version=schema_version) / table / "**" / "*.parquet")


def _read_clean_table(
    layout: NBADataLayout,
    *,
    table: str,
    seasons: list[str],
    season_type: str,
    schema_version: int,
) -> pl.DataFrame:
    glob_path = _clean_table_glob(layout, table=table, schema_version=schema_version)
    season_type_slug = slugify_season_type(season_type)
    season_type_slug_expr = (
        pl.col("season_type")
        .cast(pl.Utf8)
        .str.to_lowercase()
        .str.replace_all(r"[^a-z0-9]+", "_")
        .str.strip_chars("_")
    )
    frame = pl.scan_parquet(glob_path).filter(
        pl.col("season").is_in(seasons) & (season_type_slug_expr == season_type_slug)
    )
    return frame.collect()


def _days_on_team_expr() -> pl.Expr:
    return (
        (pl.col("game_date").cast(pl.Int64) - pl.col("team_start_days").cast(pl.Int64))
        .clip(lower_bound=0)
        .cast(pl.Int64)
    )


def _new_team_phase_expr() -> pl.Expr:
    return (
        pl.when(pl.col("games_on_team") < 5)
        .then(pl.lit("lt_5"))
        .when(pl.col("games_on_team") <= 10)
        .then(pl.lit("g5_10"))
        .otherwise(pl.lit("gt_10"))
    )


def build_minutes_prob_feature_frame(
    *,
    layout: NBADataLayout,
    config: MinutesProbFeatureConfig,
) -> pl.DataFrame:
    """Build per-player per-game probabilistic features from nba-data clean parquet."""
    games = _read_clean_table(
        layout,
        table="games",
        seasons=config.seasons,
        season_type=config.season_type,
        schema_version=config.schema_version,
    ).select(["season", "season_type", "game_id", "date"])
    boxscore = _read_clean_table(
        layout,
        table="boxscore_players",
        seasons=config.seasons,
        season_type=config.season_type,
        schema_version=config.schema_version,
    ).select(["season", "season_type", "game_id", "team_id", "player_id", "minutes"])

    if games.is_empty() or boxscore.is_empty():
        return pl.DataFrame(
            schema={
                "season": pl.Utf8,
                "season_type": pl.Utf8,
                "game_id": pl.Utf8,
                "game_date": pl.Date,
                "player_id": pl.Utf8,
                "team_id": pl.Utf8,
                "actual_minutes": pl.Float64,
                "active_target": pl.Int64,
                "games_played": pl.Int64,
                "games_on_team": pl.Int64,
                "days_on_team": pl.Int64,
                "new_team_phase": pl.Utf8,
                "new_team_phase_ord": pl.Int64,
                "prev_minutes_mean": pl.Float64,
                "prev_minutes_std": pl.Float64,
                "prev_minutes_short": pl.Float64,
                "prev_minutes_trend": pl.Float64,
                "prev_active_rate": pl.Float64,
                "team_prev_minutes_mean": pl.Float64,
                "team_prev_active_count": pl.Float64,
            }
        )

    history_games = max(2, int(config.history_games))
    min_history_games = max(1, int(config.min_history_games))
    short_window = min(3, history_games)

    base = (
        boxscore.join(games, on=["season", "season_type", "game_id"], how="inner")
        .with_columns(
            pl.col("date").str.strptime(pl.Date, strict=False).alias("game_date"),
            pl.col("minutes").fill_null(0.0).cast(pl.Float64).alias("actual_minutes"),
            pl.col("player_id").cast(pl.Utf8),
            pl.col("team_id").cast(pl.Utf8),
        )
        .filter(
            (pl.col("player_id").str.len_chars() > 0)
            & (pl.col("team_id").str.len_chars() > 0)
            & pl.col("game_date").is_not_null()
        )
        .sort(["player_id", "game_date", "game_id"])
    )

    team_game = (
        base.group_by(["team_id", "game_id", "game_date"])
        .agg(
            pl.sum("actual_minutes").alias("team_minutes_total"),
            (pl.col("actual_minutes") > 0.0).cast(pl.Int64).sum().alias("team_active_count"),
        )
        .sort(["team_id", "game_date", "game_id"])
        .with_columns(
            pl.col("team_minutes_total")
            .shift(1)
            .rolling_mean(window_size=5, min_samples=2)
            .over("team_id")
            .alias("team_prev_minutes_mean"),
            pl.col("team_active_count")
            .shift(1)
            .rolling_mean(window_size=5, min_samples=2)
            .over("team_id")
            .alias("team_prev_active_count"),
        )
        .select(
            [
                "team_id",
                "game_id",
                "team_prev_minutes_mean",
                "team_prev_active_count",
            ]
        )
    )

    frame = (
        base.join(team_game, on=["team_id", "game_id"], how="left")
        .with_columns(
            (pl.col("actual_minutes") > 0.0).cast(pl.Int64).alias("active_target"),
            pl.col("game_date").cast(pl.Int64).alias("game_days"),
            pl.col("game_date")
            .cast(pl.Int64)
            .min()
            .over(["player_id", "team_id"])
            .alias("team_start_days"),
        )
        .with_columns(
            pl.col("actual_minutes")
            .shift(1)
            .rolling_mean(window_size=history_games, min_samples=min_history_games)
            .over("player_id")
            .alias("prev_minutes_mean"),
            pl.col("actual_minutes")
            .shift(1)
            .rolling_std(window_size=history_games, min_samples=min_history_games)
            .over("player_id")
            .alias("prev_minutes_std"),
            pl.col("actual_minutes")
            .shift(1)
            .rolling_mean(window_size=short_window, min_samples=1)
            .over("player_id")
            .alias("prev_minutes_short"),
            pl.col("active_target")
            .shift(1)
            .rolling_mean(window_size=history_games, min_samples=min_history_games)
            .over("player_id")
            .alias("prev_active_rate"),
        )
        .with_columns(
            pl.col("game_id")
            .cum_count()
            .over("player_id")
            .sub(1)
            .clip(lower_bound=0)
            .cast(pl.Int64)
            .alias("games_played")
        )
        .with_columns(
            pl.col("game_id")
            .cum_count()
            .over(["player_id", "team_id"])
            .sub(1)
            .clip(lower_bound=0)
            .cast(pl.Int64)
            .alias("games_on_team")
        )
        .with_columns(
            _days_on_team_expr().alias("days_on_team"),
            (pl.col("prev_minutes_short") - pl.col("prev_minutes_mean"))
            .fill_null(0.0)
            .alias("prev_minutes_trend"),
            _new_team_phase_expr().alias("new_team_phase"),
            pl.when(pl.col("games_on_team") < 5)
            .then(pl.lit(0))
            .when(pl.col("games_on_team") <= 10)
            .then(pl.lit(1))
            .otherwise(pl.lit(2))
            .cast(pl.Int64)
            .alias("new_team_phase_ord"),
        )
        .with_columns(
            pl.col("prev_minutes_std").fill_null(0.0),
            pl.col("team_prev_minutes_mean").fill_null(240.0),
            pl.col("team_prev_active_count").fill_null(8.0),
            pl.col("prev_active_rate").fill_null(0.95),
            pl.col("prev_minutes_short").fill_null(pl.col("prev_minutes_mean")).fill_null(24.0),
            pl.col("prev_minutes_mean").fill_null(24.0),
        )
        .select(list(REQUIRED_COLUMNS))
    )
    return frame


def apply_walk_forward_split(frame: pl.DataFrame, *, eval_days: int) -> pl.DataFrame:
    """Mark rows for walk-forward evaluation window."""
    if frame.is_empty():
        return frame.with_columns(pl.lit(False).alias("is_eval"))
    max_game_date = frame.select(pl.max("game_date").alias("max_date")).item()
    if not isinstance(max_game_date, date):
        return frame.with_columns(pl.lit(False).alias("is_eval"))
    eval_span = max(1, int(eval_days))
    eval_start = max_game_date - timedelta(days=eval_span - 1)
    return frame.with_columns((pl.col("game_date") >= pl.lit(eval_start)).alias("is_eval"))
