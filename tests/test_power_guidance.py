from prop_ev.power_guidance import PowerGuidanceAssumptions, build_power_guidance


def test_build_power_guidance_reports_required_days() -> None:
    guidance = build_power_guidance(
        daily_pnl_by_strategy={
            "s007": {
                "2026-02-01": 0.0,
                "2026-02-02": 0.0,
                "2026-02-03": 0.0,
                "2026-02-04": 0.0,
            },
            "s010": {
                "2026-02-01": 0.1,
                "2026-02-02": 0.2,
                "2026-02-03": 0.0,
                "2026-02-04": 0.1,
            },
        },
        baseline_strategy_id="s007",
        assumptions=PowerGuidanceAssumptions(
            alpha=0.05,
            power=0.8,
            picks_per_day=5,
            target_roi_uplifts_per_bet=(0.02,),
        ),
    )

    assert guidance["baseline_strategy_id"] == "s007"
    strategy_row = guidance["strategies"][0]
    assert strategy_row["strategy_id"] == "s010"
    assert strategy_row["overlap_days"] == 4
    assert strategy_row["insufficient_overlap"] is False
    target_row = strategy_row["required_days_by_target"][0]
    assert target_row["target_roi_uplift_per_bet"] == 0.02
    assert target_row["required_days"] == 6
    assert target_row["required_graded_rows"] == 30


def test_build_power_guidance_marks_insufficient_overlap() -> None:
    guidance = build_power_guidance(
        daily_pnl_by_strategy={
            "s007": {
                "2026-02-01": 0.0,
                "2026-02-02": 0.0,
            },
            "s010": {
                "2026-02-01": 0.1,
            },
        },
        baseline_strategy_id="s007",
    )

    assert guidance["baseline_strategy_id"] == "s007"
    strategy_row = guidance["strategies"][0]
    assert strategy_row["strategy_id"] == "s010"
    assert strategy_row["insufficient_overlap"] is True
    assert strategy_row["required_days_by_target"] == []
