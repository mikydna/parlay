"""Compatibility wrapper for modular strategy CLI implementation."""

from __future__ import annotations

from typing import Any

import prop_ev.cli_strategy as _impl

_cmd_strategy_health = _impl._cmd_strategy_health
_cmd_strategy_run = _impl._cmd_strategy_run
_cmd_strategy_ls = _impl._cmd_strategy_ls
_cmd_strategy_compare = _impl._cmd_strategy_compare
_cmd_strategy_ablation = _impl._cmd_strategy_ablation
_cmd_strategy_backtest_summarize = _impl._cmd_strategy_backtest_summarize
_cmd_strategy_backtest_prep = _impl._cmd_strategy_backtest_prep
_cmd_strategy_settle = _impl._cmd_strategy_settle

_parse_strategy_ids = _impl._parse_strategy_ids
_parse_positive_int_csv = _impl._parse_positive_int_csv

_latest_snapshot_id = _impl._latest_snapshot_id
_teams_in_scope = _impl._teams_in_scope
_teams_in_scope_from_events = _impl._teams_in_scope_from_events

_resolve_complete_day_dataset_id = _impl._resolve_complete_day_dataset_id
_complete_day_snapshots = _impl._complete_day_snapshots
_run_cli_subcommand = _impl._run_cli_subcommand

_resolve_input_probabilistic_profile = _impl._resolve_input_probabilistic_profile
_allow_secondary_injuries_override = _impl._allow_secondary_injuries_override
_preflight_context_for_snapshot = _impl._preflight_context_for_snapshot
_official_injury_hard_fail_message = _impl._official_injury_hard_fail_message

_build_discovery_execution_report = _impl._build_discovery_execution_report
_write_discovery_execution_reports = _impl._write_discovery_execution_reports
_snapshot_date = _impl._snapshot_date


def __getattr__(name: str) -> Any:
    return getattr(_impl, name)
