from __future__ import annotations

from pathlib import Path

import pytest

from prop_ev.cli import (
    _abalation_build_input_hash,
    _abalation_strategy_cache_valid,
    _abalation_write_state,
    _parse_positive_int_csv,
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

    expected_hash = _abalation_build_input_hash(
        payload={"snapshot_id": "day-1", "strategy_id": strategy_id}
    )
    state_path = tmp_path / "state.json"
    _abalation_write_state(
        state_path,
        {
            "input_hash": expected_hash,
            "strategy_id": strategy_id,
            "seed_rows": 0,
        },
    )
    assert _abalation_strategy_cache_valid(
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

    expected_hash = _abalation_build_input_hash(
        payload={"snapshot_id": "day-1", "strategy_id": strategy_id}
    )
    state_path = tmp_path / "state.json"
    _abalation_write_state(
        state_path,
        {
            "input_hash": expected_hash,
            "strategy_id": strategy_id,
            "seed_rows": 1,
        },
    )
    assert not _abalation_strategy_cache_valid(
        reports_dir=reports_dir,
        state_path=state_path,
        expected_hash=expected_hash,
        strategy_id=strategy_id,
    )

    (reports_dir / f"settlement.{strategy_id}.csv").write_text(
        "strategy_id,result\ns020,win\n",
        encoding="utf-8",
    )
    assert _abalation_strategy_cache_valid(
        reports_dir=reports_dir,
        state_path=state_path,
        expected_hash=expected_hash,
        strategy_id=strategy_id,
    )
