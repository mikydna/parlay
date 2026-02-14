from prop_ev.backtest_summary import summarize_backtest_rows
from prop_ev.eval_scoreboard import (
    PromotionThresholds,
    build_promotion_gate,
    pick_promotion_winner,
    resolve_baseline_strategy_id,
)


def _summary(
    strategy_id: str,
    rows: list[dict[str, str]],
):
    return summarize_backtest_rows(rows, strategy_id=strategy_id, bin_size=0.1)


def test_resolve_baseline_strategy_id_prefers_requested_or_s007() -> None:
    assert (
        resolve_baseline_strategy_id(
            requested="",
            available_strategy_ids=["s003", "s007", "s001"],
        )
        == "s007"
    )
    assert (
        resolve_baseline_strategy_id(
            requested="s014",
            available_strategy_ids=["s003", "s007", "s001"],
        )
        == "s014"
    )
    assert (
        resolve_baseline_strategy_id(
            requested="",
            available_strategy_ids=["s003", "s001"],
        )
        == "s001"
    )


def test_build_promotion_gate_handles_missing_baseline_and_insufficient_rows() -> None:
    summary = _summary(
        "s001",
        [
            {
                "selected_price_american": "100",
                "stake_units": "1",
                "model_p_hit": "0.6",
                "result": "win",
            }
        ],
    )
    gate = build_promotion_gate(
        summary=summary,
        baseline_summary=None,
        baseline_required=True,
        thresholds=PromotionThresholds(
            min_graded=2,
            min_scored_fraction=0.9,
            ece_slack=0.01,
            brier_slack=0.01,
        ),
    )
    assert gate["status"] == "fail"
    assert "missing_baseline" in gate["reasons"]
    assert "insufficient_graded" in gate["reasons"]


def test_build_promotion_gate_flags_scored_fraction_and_regression() -> None:
    baseline = _summary(
        "s007",
        [
            {
                "selected_price_american": "100",
                "stake_units": "1",
                "model_p_hit": "0.9",
                "result": "win",
            }
        ],
    )
    summary = _summary(
        "s014",
        [
            {
                "selected_price_american": "100",
                "stake_units": "1",
                "model_p_hit": "0.2",
                "result": "win",
            },
            {
                "selected_price_american": "-110",
                "stake_units": "1",
                "model_p_hit": "",
                "result": "loss",
            },
        ],
    )
    gate = build_promotion_gate(
        summary=summary,
        baseline_summary=baseline,
        baseline_required=True,
        thresholds=PromotionThresholds(
            min_graded=1,
            min_scored_fraction=0.9,
            ece_slack=0.0,
            brier_slack=0.0,
        ),
    )
    assert gate["status"] == "fail"
    assert "insufficient_scored_rows" in gate["reasons"]
    assert "calibration_regressed" in gate["reasons"]
    assert "brier_regressed" in gate["reasons"]


def test_pick_promotion_winner_sorts_deterministically() -> None:
    winner = pick_promotion_winner(
        [
            {
                "strategy_id": "s010",
                "roi": 0.2,
                "rows_graded": 40,
                "ece": 0.2,
                "brier": 0.2,
                "promotion_gate": {"status": "pass"},
            },
            {
                "strategy_id": "s011",
                "roi": 0.2,
                "rows_graded": 50,
                "ece": 0.3,
                "brier": 0.3,
                "promotion_gate": {"status": "pass"},
            },
            {
                "strategy_id": "s012",
                "roi": 0.3,
                "rows_graded": 10,
                "ece": 0.9,
                "brier": 0.9,
                "promotion_gate": {"status": "fail"},
            },
        ]
    )
    assert winner is not None
    assert winner["strategy_id"] == "s011"
