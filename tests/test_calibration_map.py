from __future__ import annotations

from prop_ev.calibration_map import (
    annotate_rows_with_calibration_map,
    annotate_strategy_report_with_calibration_map,
    build_calibration_map,
)


def test_build_calibration_map_walk_forward_uses_only_past_days() -> None:
    calibration_map = build_calibration_map(
        rows_by_strategy={
            "s010": [
                {
                    "snapshot_id": "day-test-2026-02-01",
                    "model_p_hit": 0.61,
                    "result": "win",
                },
                {
                    "snapshot_id": "day-test-2026-02-02",
                    "model_p_hit": 0.62,
                    "result": "loss",
                },
                {
                    "snapshot_id": "day-test-2026-02-03",
                    "model_p_hit": 0.63,
                    "result": "loss",
                },
            ]
        },
        bin_size=0.1,
        mode="walk_forward",
    )
    strategy = calibration_map["strategies"]["s010"]
    assert strategy["rows_scored"] == 3
    assert strategy["by_day"]["2026-02-01"]["rows_scored"] == 0
    assert strategy["by_day"]["2026-02-02"]["rows_scored"] == 1
    assert strategy["by_day"]["2026-02-02"]["bins"][0]["hit_rate"] == 1.0
    assert strategy["by_day"]["2026-02-03"]["rows_scored"] == 2
    assert strategy["by_day"]["2026-02-03"]["bins"][0]["hit_rate"] == 0.5


def test_annotate_rows_with_calibration_map_applies_bin_hit_rate() -> None:
    calibration_map = {
        "schema_version": 1,
        "mode": "in_sample",
        "bin_size": 0.1,
        "strategies": {
            "s001": {
                "rows_scored": 20,
                "bins": [{"low": 0.5, "high": 0.6, "count": 20, "avg_p": 0.54, "hit_rate": 0.58}],
            }
        },
    }
    annotated = annotate_rows_with_calibration_map(
        rows=[
            {
                "player": "Player A",
                "model_p_hit": 0.54,
                "p_hit_low": 0.53,
                "quality_score": 0.7,
                "uncertainty_band": 0.05,
            }
        ],
        calibration_map=calibration_map,
        strategy_id="s001",
        modeled_day="2026-02-10",
    )
    assert len(annotated) == 1
    row = annotated[0]
    assert row["p_conservative"] == 0.53
    assert row["p_calibrated"] == 0.58
    assert row["calibration_bin"] == {"low": 0.5, "high": 0.6, "count": 20}
    assert row["confidence_tier"] == "high"


def test_annotate_strategy_report_with_calibration_map_applies_to_ranked_plays() -> None:
    report = {
        "snapshot_id": "day-test-2026-02-05",
        "strategy_id": "s001",
        "ranked_plays": [{"player": "Player A", "model_p_hit": 0.55, "p_hit_low": 0.54}],
        "watchlist": [{"player": "Player B", "model_p_hit": 0.52, "p_hit_low": 0.51}],
    }
    calibration_map = {
        "schema_version": 1,
        "mode": "walk_forward",
        "bin_size": 0.1,
        "strategies": {
            "s001": {
                "rows_scored": 10,
                "bins": [],
                "by_day": {
                    "2026-02-05": {
                        "rows_scored": 10,
                        "bins": [
                            {"low": 0.5, "high": 0.6, "count": 10, "avg_p": 0.54, "hit_rate": 0.56}
                        ],
                    }
                },
            }
        },
    }
    annotated_report = annotate_strategy_report_with_calibration_map(
        report=report,
        calibration_map=calibration_map,
    )
    assert annotated_report["ranked_plays"][0]["p_calibrated"] == 0.56
    assert annotated_report["watchlist"][0]["p_calibrated"] == 0.56
    assert annotated_report["audit"]["calibration_map_mode"] == "walk_forward"
