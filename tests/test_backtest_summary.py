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
                "best_ev": 0.05,
                "result": "win",
            },
            {
                "strategy_id": "s001",
                "selected_price_american": -110,
                "stake_units": 1,
                "model_p_hit": 0.55,
                "best_ev": 0.02,
                "result": "loss",
            },
            {
                "strategy_id": "s001",
                "selected_price_american": -110,
                "stake_units": 1,
                "model_p_hit": 0.5,
                "best_ev": 0.0,
                "result": "push",
            },
        ],
    )
    rows = csv_path.read_text(encoding="utf-8").splitlines()
    assert rows[0]

    # Skip CSV parsing here: summarize_backtest_rows operates on DictReader rows in production,
    # but the math is what matters for this unit test.
    summary = summarize_backtest_rows(
        [
            {
                "strategy_id": "s001",
                "selected_price_american": "100",
                "stake_units": "1",
                "model_p_hit": "0.6",
                "best_ev": "0.05",
                "result": "win",
            },
            {
                "strategy_id": "s001",
                "selected_price_american": "-110",
                "stake_units": "1",
                "model_p_hit": "0.55",
                "best_ev": "0.02",
                "result": "loss",
            },
            {
                "strategy_id": "s001",
                "selected_price_american": "-110",
                "stake_units": "1",
                "model_p_hit": "0.5",
                "best_ev": "0.0",
                "result": "push",
            },
        ],
        strategy_id="s001",
        bin_size=0.1,
    )
    assert summary.rows_graded == 3
    assert summary.wins == 1
    assert summary.losses == 1
    assert summary.pushes == 1
    # +1 (win at +100) -1 (loss) +0 (push) => 0 pnl on 3 units stake.
    assert summary.total_pnl_units == 0.0
    assert summary.total_stake_units == 3.0
    assert summary.roi == 0.0
    # Brier excludes pushes: mean((0.6-1)^2, (0.55-0)^2) = (0.16 + 0.3025) / 2
    assert summary.brier == 0.23125


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
