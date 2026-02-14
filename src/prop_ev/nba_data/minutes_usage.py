"""Deterministic minutes/usage baseline artifact builder."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from prop_ev.nba_data.schema_version import SCHEMA_VERSION
from prop_ev.nba_data.store.layout import NBADataLayout, slugify_season_type


@dataclass(frozen=True)
class MinutesUsageBuildConfig:
    seasons: list[str]
    season_type: str
    history_games: int
    eval_days: int
    min_history_games: int
    schema_version: int = SCHEMA_VERSION


def _iso_z_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_table_glob(
    layout: NBADataLayout,
    *,
    table: str,
    schema_version: int,
) -> str:
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
    frame = pl.scan_parquet(glob_path).filter(
        pl.col("season").is_in(seasons) & (pl.col("season_type") == season_type)
    )
    return frame.collect()


def _run_id(config: MinutesUsageBuildConfig) -> str:
    season_slug = "all" if not config.seasons else f"{config.seasons[0]}-{config.seasons[-1]}"
    season_type_slug = slugify_season_type(config.season_type)
    return (
        f"minutes-usage-v1-{season_slug}-{season_type_slug}-"
        f"h{int(config.history_games)}-e{int(config.eval_days)}-m{int(config.min_history_games)}"
    )


def _build_prediction_frame(
    *,
    layout: NBADataLayout,
    config: MinutesUsageBuildConfig,
) -> pl.DataFrame:
    games = _read_clean_table(
        layout,
        table="games",
        seasons=config.seasons,
        season_type=config.season_type,
        schema_version=config.schema_version,
    ).select(["game_id", "date"])
    boxscore = _read_clean_table(
        layout,
        table="boxscore_players",
        seasons=config.seasons,
        season_type=config.season_type,
        schema_version=config.schema_version,
    )

    if games.is_empty() or boxscore.is_empty():
        return pl.DataFrame(
            {
                "season": [],
                "season_type": [],
                "game_id": [],
                "game_date": [],
                "player_id": [],
                "team_id": [],
                "actual_minutes": [],
                "pred_minutes": [],
                "history_games_used": [],
                "abs_error": [],
                "sq_error": [],
                "is_eval": [],
            },
            schema={
                "season": pl.Utf8,
                "season_type": pl.Utf8,
                "game_id": pl.Utf8,
                "game_date": pl.Date,
                "player_id": pl.Utf8,
                "team_id": pl.Utf8,
                "actual_minutes": pl.Float64,
                "pred_minutes": pl.Float64,
                "history_games_used": pl.Int64,
                "abs_error": pl.Float64,
                "sq_error": pl.Float64,
                "is_eval": pl.Boolean,
            },
        )

    base = (
        boxscore.join(games, on="game_id", how="left")
        .with_columns(
            pl.col("date").str.strptime(pl.Date, strict=False).alias("game_date"),
            pl.col("minutes").fill_null(0.0).cast(pl.Float64).alias("actual_minutes"),
            pl.col("player_id").cast(pl.Utf8),
            pl.col("team_id").cast(pl.Utf8),
        )
        .filter((pl.col("player_id").str.len_chars() > 0) & pl.col("game_date").is_not_null())
        .sort(["player_id", "game_date", "game_id"])
    )

    history_games = max(1, int(config.history_games))
    min_history_games = max(1, int(config.min_history_games))

    with_history = base.with_columns(
        pl.col("actual_minutes")
        .shift(1)
        .rolling_mean(window_size=history_games, min_samples=min_history_games)
        .over("player_id")
        .alias("pred_minutes"),
        pl.col("actual_minutes")
        .shift(1)
        .is_not_null()
        .cast(pl.Int64)
        .rolling_sum(window_size=history_games, min_samples=1)
        .over("player_id")
        .fill_null(0)
        .cast(pl.Int64)
        .alias("history_games_used"),
    )

    max_game_date = with_history.select(pl.max("game_date").alias("max_date")).item()
    if isinstance(max_game_date, datetime):
        max_date = max_game_date.date()
    elif isinstance(max_game_date, date):
        max_date = max_game_date
    else:
        max_date = date.today()

    eval_days = max(1, int(config.eval_days))
    eval_start = max_date - timedelta(days=eval_days - 1)

    return with_history.with_columns(
        (pl.col("game_date") >= pl.lit(eval_start)).alias("is_eval"),
        (pl.col("actual_minutes") - pl.col("pred_minutes")).abs().alias("abs_error"),
        (pl.col("actual_minutes") - pl.col("pred_minutes")).pow(2).alias("sq_error"),
    ).select(
        [
            "season",
            "season_type",
            "game_id",
            "game_date",
            "player_id",
            "team_id",
            "actual_minutes",
            "pred_minutes",
            "history_games_used",
            "abs_error",
            "sq_error",
            "is_eval",
        ]
    )


def build_minutes_usage_baseline_artifact(
    *,
    layout: NBADataLayout,
    config: MinutesUsageBuildConfig,
    out_dir: Path,
) -> dict[str, object]:
    prediction_frame = _build_prediction_frame(layout=layout, config=config)
    run_id = _run_id(config)
    run_dir = out_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    prediction_path = run_dir / "predictions.parquet"
    prediction_frame.write_parquet(prediction_path)

    scored = prediction_frame.filter(pl.col("pred_minutes").is_not_null())
    eval_scored = scored.filter(pl.col("is_eval"))

    mae = eval_scored.select(pl.mean("abs_error")).item() if not eval_scored.is_empty() else None
    mse = eval_scored.select(pl.mean("sq_error")).item() if not eval_scored.is_empty() else None
    bias = (
        eval_scored.select((pl.col("actual_minutes") - pl.col("pred_minutes")).mean()).item()
        if not eval_scored.is_empty()
        else None
    )
    rmse = None if mse is None else float(mse) ** 0.5

    if prediction_frame.is_empty():
        game_dates = (None, None)
    else:
        game_dates = prediction_frame.select(
            pl.min("game_date").alias("min_date"),
            pl.max("game_date").alias("max_date"),
        ).row(0)
    if eval_scored.is_empty():
        eval_dates = (None, None)
    else:
        eval_dates = eval_scored.select(
            pl.min("game_date").alias("min_date"),
            pl.max("game_date").alias("max_date"),
        ).row(0)

    summary: dict[str, object] = {
        "schema_version": 1,
        "model_version": "minutes_usage_baseline_v1",
        "run_id": run_id,
        "generated_at_utc": _iso_z_now(),
        "seasons": list(config.seasons),
        "season_type": config.season_type,
        "history_games": int(config.history_games),
        "min_history_games": int(config.min_history_games),
        "eval_days": int(config.eval_days),
        "rows_total": int(prediction_frame.height),
        "rows_scored": int(scored.height),
        "rows_eval_scored": int(eval_scored.height),
        "train_window": {
            "date_start": game_dates[0].isoformat() if game_dates[0] is not None else "",
            "date_end": game_dates[1].isoformat() if game_dates[1] is not None else "",
        },
        "eval_window": {
            "date_start": eval_dates[0].isoformat() if eval_dates[0] is not None else "",
            "date_end": eval_dates[1].isoformat() if eval_dates[1] is not None else "",
        },
        "metrics": {
            "mae": None if mae is None else round(float(mae), 6),
            "rmse": None if rmse is None else round(float(rmse), 6),
            "bias": None if bias is None else round(float(bias), 6),
        },
        "artifacts": {
            "run_dir": str(run_dir),
            "predictions": str(prediction_path),
            "summary": str(run_dir / "summary.json"),
        },
    }

    summary_path = run_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return summary
