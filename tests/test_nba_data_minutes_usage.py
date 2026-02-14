from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from prop_ev.nba_data.cli import main
from prop_ev.nba_data.minutes_usage import (
    MinutesUsageBuildConfig,
    build_minutes_usage_baseline_artifact,
)
from prop_ev.nba_data.store.layout import build_layout, slugify_season_type


def _write_table_partition(
    *,
    layout_root: Path,
    table: str,
    season: str,
    season_type: str,
    frame: pl.DataFrame,
) -> None:
    out_dir = (
        layout_root
        / "clean"
        / "schema_v1"
        / table
        / f"season={season}"
        / f"season_type={slugify_season_type(season_type)}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(out_dir / "part-00000.parquet")


def _seed_clean_minutes_data(root: Path) -> None:
    build_layout(root)
    season = "2025-26"
    season_type = "regular_season"

    games = pl.DataFrame(
        {
            "season": [season] * 6,
            "season_type": [season_type] * 6,
            "game_id": [f"g{i}" for i in range(1, 7)],
            "date": [
                "2026-01-01",
                "2026-01-03",
                "2026-01-05",
                "2026-01-07",
                "2026-01-09",
                "2026-01-11",
            ],
            "home_team_id": ["1"] * 6,
            "away_team_id": ["2"] * 6,
        }
    )
    _write_table_partition(
        layout_root=root,
        table="games",
        season=season,
        season_type=season_type,
        frame=games,
    )

    boxscore = pl.DataFrame(
        {
            "season": [season] * 6,
            "season_type": [season_type] * 6,
            "game_id": [f"g{i}" for i in range(1, 7)],
            "team_id": ["1"] * 6,
            "player_id": ["p1"] * 6,
            "minutes": [30.0, 32.0, 34.0, 36.0, 38.0, 40.0],
            "points": [20.0] * 6,
            "rebounds": [8.0] * 6,
            "assists": [5.0] * 6,
        }
    )
    _write_table_partition(
        layout_root=root,
        table="boxscore_players",
        season=season,
        season_type=season_type,
        frame=boxscore,
    )


def test_build_minutes_usage_baseline_artifact(tmp_path: Path) -> None:
    data_root = tmp_path / "nba_data"
    _seed_clean_minutes_data(data_root)
    layout = build_layout(data_root)

    summary = build_minutes_usage_baseline_artifact(
        layout=layout,
        config=MinutesUsageBuildConfig(
            seasons=["2025-26"],
            season_type="Regular Season",
            history_games=3,
            eval_days=3,
            min_history_games=2,
            schema_version=1,
        ),
        out_dir=tmp_path / "analysis",
    )

    artifacts = summary["artifacts"]
    assert isinstance(artifacts, dict)
    summary_path = Path(str(artifacts["summary"]))
    predictions_path = Path(str(artifacts["predictions"]))
    assert summary_path.exists()
    assert predictions_path.exists()

    metrics = summary["metrics"]
    assert isinstance(metrics, dict)
    assert summary["rows_eval_scored"] > 0
    assert metrics["mae"] is not None


def test_build_minutes_usage_accepts_slug_or_label_season_type(tmp_path: Path) -> None:
    data_root = tmp_path / "nba_data"
    _seed_clean_minutes_data(data_root)
    layout = build_layout(data_root)

    summary_label = build_minutes_usage_baseline_artifact(
        layout=layout,
        config=MinutesUsageBuildConfig(
            seasons=["2025-26"],
            season_type="Regular Season",
            history_games=3,
            eval_days=3,
            min_history_games=2,
            schema_version=1,
        ),
        out_dir=tmp_path / "analysis_label",
    )
    summary_slug = build_minutes_usage_baseline_artifact(
        layout=layout,
        config=MinutesUsageBuildConfig(
            seasons=["2025-26"],
            season_type="regular_season",
            history_games=3,
            eval_days=3,
            min_history_games=2,
            schema_version=1,
        ),
        out_dir=tmp_path / "analysis_slug",
    )

    assert summary_label["rows_eval_scored"] == summary_slug["rows_eval_scored"]
    assert summary_label["rows_scored"] == summary_slug["rows_scored"]
    assert summary_label["rows_total"] == summary_slug["rows_total"]


def test_minutes_usage_artifact_is_deterministic_except_timestamp(tmp_path: Path) -> None:
    data_root = tmp_path / "nba_data"
    out_root = tmp_path / "analysis"
    _seed_clean_minutes_data(data_root)
    layout = build_layout(data_root)

    config = MinutesUsageBuildConfig(
        seasons=["2025-26"],
        season_type="Regular Season",
        history_games=3,
        eval_days=3,
        min_history_games=2,
        schema_version=1,
    )
    summary_a = build_minutes_usage_baseline_artifact(
        layout=layout, config=config, out_dir=out_root
    )
    summary_b = build_minutes_usage_baseline_artifact(
        layout=layout, config=config, out_dir=out_root
    )

    normalized_a = dict(summary_a)
    normalized_b = dict(summary_b)
    normalized_a.pop("generated_at_utc", None)
    normalized_b.pop("generated_at_utc", None)
    assert normalized_a == normalized_b


def test_cli_minutes_usage_command(tmp_path: Path) -> None:
    data_root = tmp_path / "nba_data"
    out_root = tmp_path / "output"
    _seed_clean_minutes_data(data_root)

    code = main(
        [
            "minutes-usage",
            "--data-dir",
            str(data_root),
            "--out-dir",
            str(out_root),
            "--seasons",
            "2025-26",
            "--season-type",
            "Regular Season",
            "--history-games",
            "3",
            "--min-history-games",
            "2",
            "--eval-days",
            "3",
            "--json",
        ]
    )
    assert code == 0

    summaries = list(out_root.glob("**/summary.json"))
    assert summaries
    payload = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert payload["model_version"] == "minutes_usage_baseline_v1"
    assert payload["rows_eval_scored"] > 0
