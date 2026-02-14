from __future__ import annotations

from pathlib import Path

import pytest

from prop_ev.cli import (
    _ablation_prune_cap_root,
    _ablation_strategy_cache_valid,
    _ablation_write_state,
    _build_ablation_analysis_run_id,
    _build_ablation_input_hash,
    _parse_positive_int_csv,
    main,
)


def test_parse_positive_int_csv_defaults_and_dedup() -> None:
    parsed = _parse_positive_int_csv("", default=[1, 2, 5], flag_name="--caps")
    assert parsed == [1, 2, 5]
    parsed = _parse_positive_int_csv("5,2,2,1", default=[1], flag_name="--caps")
    assert parsed == [5, 2, 1]


def test_parse_positive_int_csv_rejects_invalid() -> None:
    with pytest.raises(RuntimeError):
        _parse_positive_int_csv("1,0", default=[1], flag_name="--caps")
    with pytest.raises(RuntimeError):
        _parse_positive_int_csv("1,foo", default=[1], flag_name="--caps")


def test_strategy_cache_valid_allows_zero_seed_without_settlement(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    strategy_id = "s018"
    (reports_dir / f"strategy-report.{strategy_id}.json").write_text("{}", encoding="utf-8")
    (reports_dir / f"backtest-seed.{strategy_id}.jsonl").write_text("", encoding="utf-8")
    (reports_dir / f"backtest-results-template.{strategy_id}.csv").write_text(
        "market,side\n",
        encoding="utf-8",
    )

    expected_hash = _build_ablation_input_hash(
        payload={"snapshot_id": "day-1", "strategy_id": strategy_id}
    )
    state_path = tmp_path / "state.json"
    _ablation_write_state(
        state_path,
        {
            "input_hash": expected_hash,
            "strategy_id": strategy_id,
            "seed_rows": 0,
        },
    )
    assert _ablation_strategy_cache_valid(
        reports_dir=reports_dir,
        state_path=state_path,
        expected_hash=expected_hash,
        strategy_id=strategy_id,
    )


def test_strategy_cache_requires_settlement_for_nonzero_seed(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    strategy_id = "s020"
    (reports_dir / f"strategy-report.{strategy_id}.json").write_text("{}", encoding="utf-8")
    (reports_dir / f"backtest-seed.{strategy_id}.jsonl").write_text(
        '{"strategy_id":"s020"}\n',
        encoding="utf-8",
    )
    (reports_dir / f"backtest-results-template.{strategy_id}.csv").write_text(
        "market,side\n",
        encoding="utf-8",
    )

    expected_hash = _build_ablation_input_hash(
        payload={"snapshot_id": "day-1", "strategy_id": strategy_id}
    )
    state_path = tmp_path / "state.json"
    _ablation_write_state(
        state_path,
        {
            "input_hash": expected_hash,
            "strategy_id": strategy_id,
            "seed_rows": 1,
        },
    )
    assert not _ablation_strategy_cache_valid(
        reports_dir=reports_dir,
        state_path=state_path,
        expected_hash=expected_hash,
        strategy_id=strategy_id,
    )

    (reports_dir / f"settlement.{strategy_id}.csv").write_text(
        "strategy_id,result\ns020,win\n",
        encoding="utf-8",
    )
    assert _ablation_strategy_cache_valid(
        reports_dir=reports_dir,
        state_path=state_path,
        expected_hash=expected_hash,
        strategy_id=strategy_id,
    )


def test_ablation_prune_cap_root_removes_intermediate_dirs(tmp_path: Path) -> None:
    cap_root = tmp_path / "cap-max1"
    (cap_root / "analysis" / "run1").mkdir(parents=True, exist_ok=True)
    (cap_root / "analysis" / "run1" / "aggregate-scoreboard.json").write_text(
        "{}\n",
        encoding="utf-8",
    )
    (cap_root / "by-snapshot" / "day-1").mkdir(parents=True, exist_ok=True)
    (cap_root / "_ablation_state").mkdir(parents=True, exist_ok=True)
    (cap_root / "by-snapshot" / "day-1" / "strategy-report.s001.json").write_text(
        "{}\n",
        encoding="utf-8",
    )
    (cap_root / "_ablation_state" / "day-1.s001.json").write_text(
        "{}\n",
        encoding="utf-8",
    )

    stats = _ablation_prune_cap_root(cap_root)

    assert stats["removed_dirs"] == 2
    assert stats["removed_files"] == 2
    assert not (cap_root / "by-snapshot").exists()
    assert not (cap_root / "_ablation_state").exists()
    assert (cap_root / "analysis" / "run1" / "aggregate-scoreboard.json").exists()


def test_build_ablation_analysis_run_id_avoids_prefix_duplication() -> None:
    value = _build_ablation_analysis_run_id(
        analysis_prefix="ablation-s007-s020-smoke",
        run_id="ablation-s007-s020-smoke-20260214-r1",
        cap=5,
    )
    assert value == "ablation-s007-s020-smoke-20260214-r1-max5"

    fallback = _build_ablation_analysis_run_id(
        analysis_prefix="ablation",
        run_id="latest",
        cap=1,
    )
    assert fallback == "ablation-latest-max1"


def test_strategy_unknown_command_is_rejected(
    capsys: pytest.CaptureFixture[str],
) -> None:
    bad_command = "ablate"
    with pytest.raises(SystemExit) as exc_info:
        main(["strategy", bad_command])
    err = capsys.readouterr().err
    assert exc_info.value.code == 2
    assert "invalid choice" in err
    assert bad_command in err
