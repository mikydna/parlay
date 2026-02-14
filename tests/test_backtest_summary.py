import csv
from pathlib import Path

from prop_ev.backtest_summary import pick_winner, summarize_backtest_rows


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_summarize_backtest_rows_roi_and_brier(tmp_path: Path) -> None:
    csv_path = tmp_path / "backtest.csv"
    _write_csv(
        csv_path,
        [
            {
                "strategy_id": "s001",
                "selected_price_american": 100,
                "stake_units": 1,
                "model_p_hit": 0.6,
                "p_hit_low": 0.55,
                "best_ev": 0.05,
                "ev_low": 0.03,
                "quality_score": 0.7,
                "summary_candidate_lines": 2,
                "summary_eligible_lines": 1,
                "result": "win",
            },
            {
                "strategy_id": "s001",
                "selected_price_american": -110,
                "stake_units": 1,
                "model_p_hit": 0.55,
                "p_hit_low": 0.5,
                "best_ev": 0.02,
                "ev_low": 0.01,
                "quality_score": 0.6,
                "summary_candidate_lines": 2,
                "summary_eligible_lines": 1,
                "result": "loss",
            },
            {
                "strategy_id": "s001",
                "selected_price_american": -110,
                "stake_units": 1,
                "model_p_hit": 0.5,
                "p_hit_low": 0.45,
                "best_ev": 0.0,
                "ev_low": 0.0,
                "quality_score": 0.5,
                "summary_candidate_lines": 2,
                "summary_eligible_lines": 1,
                "result": "push",
            },
        ],
    )
    rows = csv_path.read_text(encoding="utf-8").splitlines()
    assert rows[0]

    summary = summarize_backtest_rows(
        [
            {
                "strategy_id": "s001",
                "selected_price_american": "100.0",
                "stake_units": "1",
                "model_p_hit": "0.6",
                "p_hit_low": "0.55",
                "best_ev": "0.05",
                "ev_low": "0.03",
                "quality_score": "0.7",
                "summary_candidate_lines": "2",
                "summary_eligible_lines": "1",
                "result": "win",
            },
            {
                "strategy_id": "s001",
                "selected_price_american": "-110.0",
                "stake_units": "1",
                "model_p_hit": "0.55",
                "p_hit_low": "0.5",
                "best_ev": "0.02",
                "ev_low": "0.01",
                "quality_score": "0.6",
                "summary_candidate_lines": "2",
                "summary_eligible_lines": "1",
                "result": "loss",
            },
            {
                "strategy_id": "s001",
                "selected_price_american": "-110.0",
                "stake_units": "1",
                "model_p_hit": "0.5",
                "p_hit_low": "0.45",
                "best_ev": "0.0",
                "ev_low": "0.0",
                "quality_score": "0.5",
                "summary_candidate_lines": "2",
                "summary_eligible_lines": "1",
                "result": "push",
            },
        ],
        strategy_id="s001",
        bin_size=0.1,
    )
    assert summary.rows_graded == 3
    assert summary.rows_scored == 2
    assert summary.wins == 1
    assert summary.losses == 1
    assert summary.pushes == 1
    assert summary.total_pnl_units == 0.0
    assert summary.total_stake_units == 3.0
    assert summary.roi == 0.0
    assert summary.brier == 0.23125
    assert summary.brier_low == 0.22625
    assert summary.log_loss == 0.654667
    assert summary.ece == 0.075
    assert summary.mce == 0.075
    assert summary.avg_ev_low == 0.013333
    assert summary.avg_quality_score == 0.6
    assert summary.avg_p_hit_low == 0.525
    assert summary.actionability_rate == 0.5


def test_summarize_backtest_rows_accepts_decimal_price_strings() -> None:
    summary = summarize_backtest_rows(
        [
            {
                "selected_price_american": "-106.0",
                "stake_units": "1",
                "model_p_hit": "0.6",
                "result": "win",
            },
            {
                "selected_price_american": "112.0",
                "stake_units": "1",
                "model_p_hit": "0.5",
                "result": "loss",
            },
        ],
        strategy_id="s001",
        bin_size=0.1,
    )
    assert summary.rows_graded == 2
    assert summary.rows_scored == 2
    assert summary.wins == 1
    assert summary.losses == 1
    assert summary.total_stake_units == 2.0
    assert summary.brier is not None
    assert summary.log_loss is not None


def test_summarize_backtest_rows_log_loss_clamps_edges() -> None:
    summary = summarize_backtest_rows(
        [
            {
                "selected_price_american": "-110",
                "stake_units": "1",
                "model_p_hit": "0.0",
                "result": "win",
            },
            {
                "selected_price_american": "-110",
                "stake_units": "1",
                "model_p_hit": "1.0",
                "result": "loss",
            },
        ],
        strategy_id="s001",
        bin_size=0.5,
    )
    assert summary.rows_scored == 2
    assert summary.brier == 1.0
    assert summary.log_loss == 13.815511
    assert summary.ece == 1.0
    assert summary.mce == 1.0


def test_summarize_backtest_rows_rows_scored_ignores_invalid_probabilities() -> None:
    summary = summarize_backtest_rows(
        [
            {
                "selected_price_american": "-110",
                "stake_units": "1",
                "model_p_hit": "0.4",
                "result": "win",
            },
            {
                "selected_price_american": "-110",
                "stake_units": "1",
                "model_p_hit": "",
                "result": "loss",
            },
        ],
        strategy_id="s001",
        bin_size=0.1,
    )
    assert summary.wins == 1
    assert summary.losses == 1
    assert summary.rows_graded == 2
    assert summary.rows_scored == 1
    assert summary.ece == 0.6
    assert summary.mce == 0.6


def test_pick_winner_min_graded(tmp_path: Path) -> None:
    a = summarize_backtest_rows(
        [
            {
                "result": "win",
                "selected_price_american": "100",
                "stake_units": "1",
                "model_p_hit": "0.5",
            }
        ],
        strategy_id="a",
    )
    b = summarize_backtest_rows(
        [
            {
                "result": "win",
                "selected_price_american": "100",
                "stake_units": "1",
                "model_p_hit": "0.5",
            }
        ],
        strategy_id="b",
    )
    assert pick_winner([a, b], min_graded=2) is None
    winner = pick_winner([a, b], min_graded=1)
    assert winner is not None
    assert winner.strategy_id in {"a", "b"}
