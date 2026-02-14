import csv
from pathlib import Path

import pytest

from prop_ev.rolling_priors import build_rolling_priors, calibration_feedback


def _write_backtest_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_build_rolling_priors_excludes_lookahead_and_outside_window(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports" / "odds"
    by_snapshot = reports_root / "by-snapshot"

    _write_backtest_csv(
        by_snapshot / "a" / "backtest-results-template.s008.csv",
        [
            {
                "snapshot_id": "daily-20260205T120000Z",
                "modeled_date_et": "Wednesday, Feb 05, 2026 (ET)",
                "market": "player_points",
                "recommended_side": "over",
                "result": "win",
            }
        ],
    )
    _write_backtest_csv(
        by_snapshot / "b" / "backtest-results-template.s008.csv",
        [
            {
                "snapshot_id": "daily-20260208T120000Z",
                "modeled_date_et": "Saturday, Feb 08, 2026 (ET)",
                "market": "player_points",
                "recommended_side": "over",
                "result": "loss",
            }
        ],
    )
    _write_backtest_csv(
        by_snapshot / "c" / "backtest-results-template.s008.csv",
        [
            {
                "snapshot_id": "daily-20260210T120000Z",
                "modeled_date_et": "Monday, Feb 10, 2026 (ET)",
                "market": "player_points",
                "recommended_side": "over",
                "result": "win",
            }
        ],
    )
    _write_backtest_csv(
        by_snapshot / "d" / "backtest-results-template.s008.csv",
        [
            {
                "snapshot_id": "daily-20260110T120000Z",
                "modeled_date_et": "Saturday, Jan 10, 2026 (ET)",
                "market": "player_points",
                "recommended_side": "over",
                "result": "win",
            }
        ],
    )

    priors = build_rolling_priors(
        reports_root=reports_root,
        strategy_id="s008",
        as_of_day="2026-02-10",
        window_days=21,
        min_samples=1,
    )

    key = "player_points::over"
    assert priors["rows_used"] == 2
    assert key in priors["adjustments"]
    assert priors["adjustments"][key]["sample_size"] == 2
    assert priors["adjustments"][key]["wins"] == 1
    assert priors["adjustments"][key]["losses"] == 1
    assert priors["adjustments"][key]["delta"] == 0.0
    assert priors["calibration"]["rows_scored"] == 0


def test_build_rolling_priors_supports_snapshot_day_fallback_and_delta_cap(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports" / "odds"
    _write_backtest_csv(
        reports_root / "by-snapshot" / "x" / "backtest-results-template.s008.csv",
        [
            {
                "snapshot_id": "daily-20260209T120000Z",
                "modeled_date_et": "",
                "market": "player_rebounds",
                "recommended_side": "under",
                "result": "win",
            }
        ],
    )

    priors = build_rolling_priors(
        reports_root=reports_root,
        strategy_id="s008",
        as_of_day="2026-02-10",
        window_days=10,
        min_samples=1,
        max_abs_delta=0.02,
    )

    key = "player_rebounds::under"
    assert priors["rows_used"] == 1
    assert priors["adjustments"][key]["sample_size"] == 1
    assert priors["adjustments"][key]["delta"] == 0.02


def test_build_rolling_priors_supports_day_index_snapshot_ids(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports" / "odds"
    _write_backtest_csv(
        reports_root / "by-snapshot" / "x" / "backtest-results-template.s009.csv",
        [
            {
                "snapshot_id": "day-bdfa890a-2026-02-12",
                "modeled_date_et": "",
                "market": "player_assists",
                "recommended_side": "over",
                "result": "win",
            }
        ],
    )

    priors = build_rolling_priors(
        reports_root=reports_root,
        strategy_id="s009",
        as_of_day="2026-02-13",
        window_days=7,
        min_samples=1,
    )

    key = "player_assists::over"
    assert priors["rows_used"] == 1
    assert priors["adjustments"][key]["sample_size"] == 1


def test_calibration_feedback_prefers_market_side_then_global(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports" / "odds"
    _write_backtest_csv(
        reports_root / "by-snapshot" / "x" / "backtest-results-template.s010.csv",
        [
            {
                "snapshot_id": "daily-20260205T120000Z",
                "modeled_date_et": "Thursday, Feb 05, 2026 (ET)",
                "market": "player_points",
                "recommended_side": "over",
                "model_p_hit": "0.61",
                "result": "loss",
            },
            {
                "snapshot_id": "daily-20260206T120000Z",
                "modeled_date_et": "Friday, Feb 06, 2026 (ET)",
                "market": "player_points",
                "recommended_side": "over",
                "model_p_hit": "0.62",
                "result": "loss",
            },
            {
                "snapshot_id": "daily-20260207T120000Z",
                "modeled_date_et": "Saturday, Feb 07, 2026 (ET)",
                "market": "player_rebounds",
                "recommended_side": "under",
                "model_p_hit": "0.71",
                "result": "win",
            },
            {
                "snapshot_id": "daily-20260208T120000Z",
                "modeled_date_et": "Sunday, Feb 08, 2026 (ET)",
                "market": "player_rebounds",
                "recommended_side": "under",
                "model_p_hit": "0.73",
                "result": "win",
            },
        ],
    )
    priors = build_rolling_priors(
        reports_root=reports_root,
        strategy_id="s010",
        as_of_day="2026-02-10",
        window_days=21,
        min_samples=1,
        calibration_bin_size=0.1,
        calibration_min_bin_samples=2,
    )
    market_hit = calibration_feedback(
        rolling_priors=priors,
        market="player_points",
        side="over",
        model_probability=0.61,
    )
    assert market_hit["source"] == "market_side"
    assert market_hit["sample_size"] == 2
    assert market_hit["delta"] == pytest.approx(-0.003892, abs=1e-6)
    assert market_hit["p_calibrated"] == pytest.approx(0.606108, abs=1e-6)
    assert market_hit["confidence"] == 1.0

    global_hit = calibration_feedback(
        rolling_priors=priors,
        market="player_assists",
        side="over",
        model_probability=0.72,
    )
    assert global_hit["source"] == "global"
    assert global_hit["sample_size"] == 2
    assert global_hit["delta"] == pytest.approx(0.0014, abs=1e-6)
    assert global_hit["p_calibrated"] == pytest.approx(0.7214, abs=1e-6)
    assert global_hit["confidence"] == pytest.approx(0.85, abs=1e-6)
