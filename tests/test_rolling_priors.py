import csv
from pathlib import Path

from prop_ev.rolling_priors import build_rolling_priors


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
