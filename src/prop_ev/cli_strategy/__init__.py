"""Modular strategy CLI implementation package."""

from __future__ import annotations

from .ablation import (
    _cmd_strategy_ablation,
    _complete_day_snapshots,
    _parse_positive_int_csv,
    _resolve_complete_day_dataset_id,
    _run_cli_subcommand,
)
from .backtest import _cmd_strategy_backtest_prep, _cmd_strategy_backtest_summarize
from .compare import (
    _cmd_strategy_compare,
    _cmd_strategy_ls,
    _parse_strategy_ids,
)
from .discovery import _build_discovery_execution_report, _write_discovery_execution_reports
from .health import _cmd_strategy_health
from .run import _cmd_strategy_run
from .settle import _cmd_strategy_settle
from .shared import (
    _allow_secondary_injuries_override,
    _latest_snapshot_id,
    _official_injury_hard_fail_message,
    _preflight_context_for_snapshot,
    _resolve_input_probabilistic_profile,
    _snapshot_date,
    _teams_in_scope,
    _teams_in_scope_from_events,
)

__all__ = [
    "_allow_secondary_injuries_override",
    "_build_discovery_execution_report",
    "_cmd_strategy_ablation",
    "_cmd_strategy_backtest_prep",
    "_cmd_strategy_backtest_summarize",
    "_cmd_strategy_compare",
    "_cmd_strategy_health",
    "_cmd_strategy_ls",
    "_cmd_strategy_run",
    "_cmd_strategy_settle",
    "_complete_day_snapshots",
    "_latest_snapshot_id",
    "_official_injury_hard_fail_message",
    "_parse_positive_int_csv",
    "_parse_strategy_ids",
    "_preflight_context_for_snapshot",
    "_resolve_complete_day_dataset_id",
    "_resolve_input_probabilistic_profile",
    "_run_cli_subcommand",
    "_snapshot_date",
    "_teams_in_scope",
    "_teams_in_scope_from_events",
    "_write_discovery_execution_reports",
]
