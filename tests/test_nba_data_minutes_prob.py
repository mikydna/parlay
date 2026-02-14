from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from prop_ev.nba_data.cli import main
from prop_ev.nba_data.minutes_prob.features import (
    MinutesProbFeatureConfig,
    build_minutes_prob_feature_frame,
)
from prop_ev.nba_data.minutes_prob.model import (
    MinutesProbTrainConfig,
    evaluate_minutes_prob_predictions_file,
    predict_minutes_probabilities,
    train_minutes_prob_model,
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


def _seed_clean_minutes_prob_data(root: Path) -> None:
    build_layout(root)
    season = "2025-26"
    season_type = "regular_season"

    game_ids = [f"g{i}" for i in range(1, 9)]
    game_dates = [
        "2026-01-01",
        "2026-01-03",
        "2026-01-05",
        "2026-01-07",
        "2026-01-09",
        "2026-01-11",
        "2026-01-13",
        "2026-01-15",
    ]
    games = pl.DataFrame(
        {
            "season": [season] * len(game_ids),
            "season_type": [season_type] * len(game_ids),
            "game_id": game_ids,
            "date": game_dates,
            "home_team_id": ["1", "1", "1", "1", "2", "2", "2", "2"],
            "away_team_id": ["2", "2", "2", "2", "1", "1", "1", "1"],
        }
    )
    _write_table_partition(
        layout_root=root,
        table="games",
        season=season,
        season_type=season_type,
        frame=games,
    )

    rows: list[dict[str, object]] = []
    p1_minutes = [30.0, 32.0, 34.0, 35.0, 24.0, 26.0, 28.0, 30.0]
    p1_teams = ["1", "1", "1", "1", "2", "2", "2", "2"]
    p2_minutes = [28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0]
    p3_minutes = [20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0]
    for index, game_id in enumerate(game_ids):
        rows.append(
            {
                "season": season,
                "season_type": season_type,
                "game_id": game_id,
                "team_id": p1_teams[index],
                "player_id": "p1",
                "minutes": p1_minutes[index],
                "points": 15.0 + index,
                "rebounds": 5.0 + (index % 3),
                "assists": 4.0 + (index % 2),
            }
        )
        rows.append(
            {
                "season": season,
                "season_type": season_type,
                "game_id": game_id,
                "team_id": "1",
                "player_id": "p2",
                "minutes": p2_minutes[index],
                "points": 16.0 + index,
                "rebounds": 6.0 + (index % 2),
                "assists": 3.0 + (index % 3),
            }
        )
        rows.append(
            {
                "season": season,
                "season_type": season_type,
                "game_id": game_id,
                "team_id": "2",
                "player_id": "p3",
                "minutes": p3_minutes[index],
                "points": 12.0 + index,
                "rebounds": 7.0 + (index % 2),
                "assists": 2.0 + (index % 3),
            }
        )
    boxscore = pl.DataFrame(rows)
    _write_table_partition(
        layout_root=root,
        table="boxscore_players",
        season=season,
        season_type=season_type,
        frame=boxscore,
    )


def test_minutes_prob_feature_frame_has_tenure_columns(tmp_path: Path) -> None:
    data_root = tmp_path / "nba_data"
    _seed_clean_minutes_prob_data(data_root)
    layout = build_layout(data_root)

    frame = build_minutes_prob_feature_frame(
        layout=layout,
        config=MinutesProbFeatureConfig(
            seasons=["2025-26"],
            season_type="Regular Season",
            history_games=4,
            min_history_games=2,
            eval_days=2,
            schema_version=1,
        ),
    )

    assert {
        "games_on_team",
        "days_on_team",
        "new_team_phase",
        "new_team_phase_ord",
    }.issubset(set(frame.columns))
    p1_rows = frame.filter(pl.col("player_id") == pl.lit("p1")).sort("game_date")
    trade_row = p1_rows.filter(pl.col("game_id") == pl.lit("g5")).row(0, named=True)
    assert int(trade_row["games_on_team"]) == 0
    assert int(trade_row["days_on_team"]) == 0
    assert str(trade_row["new_team_phase"]) == "lt_5"


def test_train_predict_evaluate_minutes_prob(tmp_path: Path) -> None:
    data_root = tmp_path / "nba_data"
    _seed_clean_minutes_prob_data(data_root)
    layout = build_layout(data_root)
    out_root = tmp_path / "analysis"

    summary = train_minutes_prob_model(
        layout=layout,
        config=MinutesProbTrainConfig(
            seasons=["2025-26"],
            season_type="Regular Season",
            history_games=4,
            min_history_games=2,
            eval_days=3,
            schema_version=1,
            random_seed=7,
            model_version="minutes_prob_test_v1",
        ),
        out_dir=out_root,
    )
    artifacts = summary["artifacts"]
    assert isinstance(artifacts, dict)
    model_dir = Path(str(artifacts["model_dir"]))
    assert (model_dir / "model.pkl").exists()

    out_path = out_root / "predictions" / "snapshot_date=2026-01-15" / "predictions.parquet"
    predict_minutes_probabilities(
        layout=layout,
        model_dir=model_dir,
        as_of_date="2026-01-15",
        out_path=out_path,
        snapshot_id="snap-2026-01-15",
        markets=("player_points", "player_rebounds"),
    )

    predictions = pl.read_parquet(out_path)
    assert predictions.height > 0
    assert {
        "minutes_p10",
        "minutes_p50",
        "minutes_p90",
        "minutes_mu",
        "minutes_sigma_proxy",
        "p_active",
        "confidence_score",
        "data_quality_flags",
    }.issubset(set(predictions.columns))
    monotone = predictions.select(
        (
            (pl.col("minutes_p10") <= pl.col("minutes_p50"))
            & (pl.col("minutes_p50") <= pl.col("minutes_p90"))
        ).all()
    ).item()
    assert bool(monotone)

    evaluation_path = out_root / "evaluation.json"
    evaluation = evaluate_minutes_prob_predictions_file(
        predictions_path=out_path,
        out_path=evaluation_path,
    )
    assert evaluation_path.exists()
    assert int(evaluation["rows_scored"]) > 0
    metrics = evaluation["metrics"]
    assert isinstance(metrics, dict)
    assert metrics["mae"] is not None


def test_minutes_prob_prediction_is_deterministic(tmp_path: Path) -> None:
    data_root = tmp_path / "nba_data"
    _seed_clean_minutes_prob_data(data_root)
    layout = build_layout(data_root)
    out_root = tmp_path / "analysis"

    summary = train_minutes_prob_model(
        layout=layout,
        config=MinutesProbTrainConfig(
            seasons=["2025-26"],
            season_type="Regular Season",
            history_games=4,
            min_history_games=2,
            eval_days=3,
            schema_version=1,
            random_seed=11,
            model_version="minutes_prob_test_v1",
        ),
        out_dir=out_root,
    )
    artifacts = summary["artifacts"]
    assert isinstance(artifacts, dict)
    model_dir = Path(str(artifacts["model_dir"]))

    out_a = out_root / "pred-a.parquet"
    out_b = out_root / "pred-b.parquet"
    predict_minutes_probabilities(
        layout=layout,
        model_dir=model_dir,
        as_of_date="2026-01-15",
        out_path=out_a,
        snapshot_id="snap-a",
        markets=("player_points",),
    )
    predict_minutes_probabilities(
        layout=layout,
        model_dir=model_dir,
        as_of_date="2026-01-15",
        out_path=out_b,
        snapshot_id="snap-a",
        markets=("player_points",),
    )
    frame_a = pl.read_parquet(out_a).sort(["event_id", "player_id", "market"])
    frame_b = pl.read_parquet(out_b).sort(["event_id", "player_id", "market"])
    assert frame_a.equals(frame_b)


def test_cli_minutes_prob_train_predict_evaluate(tmp_path: Path) -> None:
    data_root = tmp_path / "nba_data"
    out_root = tmp_path / "analysis"
    _seed_clean_minutes_prob_data(data_root)

    train_code = main(
        [
            "minutes-prob",
            "train",
            "--data-dir",
            str(data_root),
            "--out-dir",
            str(out_root),
            "--seasons",
            "2025-26",
            "--season-type",
            "Regular Season",
            "--history-games",
            "4",
            "--min-history-games",
            "2",
            "--eval-days",
            "3",
            "--model-version",
            "minutes_prob_cli_v1",
            "--json",
        ]
    )
    assert train_code == 0

    model_dirs = [path.parent for path in out_root.glob("**/model.pkl")]
    assert model_dirs
    model_dir = sorted(model_dirs)[-1]
    predictions_path = out_root / "predictions" / "snapshot_date=2026-01-15" / "predictions.parquet"
    predict_code = main(
        [
            "minutes-prob",
            "predict",
            "--data-dir",
            str(data_root),
            "--model-dir",
            str(model_dir),
            "--as-of-date",
            "2026-01-15",
            "--snapshot-id",
            "cli-snapshot",
            "--markets",
            "player_points",
            "--out",
            str(predictions_path),
            "--json",
        ]
    )
    assert predict_code == 0
    assert predictions_path.exists()

    evaluate_path = out_root / "cli-evaluation.json"
    evaluate_code = main(
        [
            "minutes-prob",
            "evaluate",
            "--model-dir",
            str(model_dir),
            "--predictions",
            str(predictions_path),
            "--out",
            str(evaluate_path),
            "--json",
        ]
    )
    assert evaluate_code == 0
    payload = json.loads(evaluate_path.read_text(encoding="utf-8"))
    assert int(payload.get("rows_scored", 0)) > 0
