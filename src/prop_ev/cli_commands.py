"""CLI entrypoint for prop-ev."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from prop_ev import cli_ablation_helpers as _ablation_helpers
from prop_ev import cli_data_impl as _data_impl
from prop_ev import cli_playbook_impl as _playbook_impl
from prop_ev import cli_snapshot_impl as _snapshot_impl
from prop_ev import cli_strategy_impl as _strategy_impl
from prop_ev.backtest import ROW_SELECTIONS
from prop_ev.cli_global_overrides import (
    extract_global_overrides as _extract_global_overrides_impl,
)
from prop_ev.cli_parser import build_parser as _build_parser_impl
from prop_ev.cli_shared import (
    CLIError,
    _runtime_odds_api_default_max_credits,
)
from prop_ev.cli_shared import (
    _default_window as _default_window_impl,
)
from prop_ev.cli_shared import (
    _resolve_bookmakers as _resolve_bookmakers_impl,
)
from prop_ev.odds_client import OddsAPIError
from prop_ev.odds_data.errors import CreditBudgetExceeded, OfflineCacheMiss, SpendBlockedError
from prop_ev.playbook import generate_brief_for_snapshot  # noqa: F401
from prop_ev.runtime_config import load_runtime_config, set_current_runtime_config

# Parser-dispatched command handlers (kept as module attributes for compatibility/monkeypatch).
_cmd_snapshot_slate = _snapshot_impl._cmd_snapshot_slate
_cmd_snapshot_props = _snapshot_impl._cmd_snapshot_props
_cmd_snapshot_ls = _snapshot_impl._cmd_snapshot_ls
_cmd_snapshot_show = _snapshot_impl._cmd_snapshot_show
_cmd_snapshot_diff = _snapshot_impl._cmd_snapshot_diff
_cmd_snapshot_verify = _snapshot_impl._cmd_snapshot_verify
_cmd_snapshot_lake = _snapshot_impl._cmd_snapshot_lake
_cmd_snapshot_pack = _snapshot_impl._cmd_snapshot_pack
_cmd_snapshot_unpack = _snapshot_impl._cmd_snapshot_unpack
_cmd_credits_report = _snapshot_impl._cmd_credits_report
_cmd_credits_budget = _snapshot_impl._cmd_credits_budget

_cmd_data_datasets_ls = _data_impl._cmd_data_datasets_ls
_cmd_data_datasets_show = _data_impl._cmd_data_datasets_show
_cmd_data_status = _data_impl._cmd_data_status
_cmd_data_done_days = _data_impl._cmd_data_done_days
_cmd_data_export_denorm = _data_impl._cmd_data_export_denorm
_cmd_data_backfill = _data_impl._cmd_data_backfill
_cmd_data_verify = _data_impl._cmd_data_verify
_cmd_data_repair_derived = _data_impl._cmd_data_repair_derived
_cmd_data_guardrails = _data_impl._cmd_data_guardrails
_cmd_data_migrate_layout = _data_impl._cmd_data_migrate_layout

_cmd_strategy_health = _strategy_impl._cmd_strategy_health
_cmd_strategy_run = _strategy_impl._cmd_strategy_run
_cmd_strategy_ls = _strategy_impl._cmd_strategy_ls
_cmd_strategy_compare = _strategy_impl._cmd_strategy_compare
_cmd_strategy_ablation = _strategy_impl._cmd_strategy_ablation
_cmd_strategy_backtest_summarize = _strategy_impl._cmd_strategy_backtest_summarize
_cmd_strategy_backtest_prep = _strategy_impl._cmd_strategy_backtest_prep
_cmd_strategy_settle = _strategy_impl._cmd_strategy_settle
_parse_positive_int_csv = _strategy_impl._parse_positive_int_csv
_preflight_context_for_snapshot = _strategy_impl._preflight_context_for_snapshot
_resolve_input_probabilistic_profile = _strategy_impl._resolve_input_probabilistic_profile
_build_discovery_execution_report = _strategy_impl._build_discovery_execution_report
_parse_strategy_ids = _strategy_impl._parse_strategy_ids

_ablation_prune_cap_root = _ablation_helpers.ablation_prune_cap_root
_ablation_strategy_cache_valid = _ablation_helpers.ablation_strategy_cache_valid
_ablation_write_state = _ablation_helpers.ablation_write_state
_build_ablation_analysis_run_id = _ablation_helpers.build_ablation_analysis_run_id
_build_ablation_input_hash = _ablation_helpers.build_ablation_input_hash

_run_strategy_for_playbook = _playbook_impl._run_strategy_for_playbook
_run_snapshot_bundle_for_playbook = _playbook_impl._run_snapshot_bundle_for_playbook
_cmd_playbook_run = _playbook_impl._cmd_playbook_run
_cmd_playbook_render = _playbook_impl._cmd_playbook_render
_cmd_playbook_publish = _playbook_impl._cmd_playbook_publish
_cmd_playbook_budget = _playbook_impl._cmd_playbook_budget
_cmd_playbook_discover_execute = _playbook_impl._cmd_playbook_discover_execute
OddsAPIClient = _playbook_impl.OddsAPIClient
_default_window = _default_window_impl
_resolve_bookmakers = _resolve_bookmakers_impl


def _extract_global_overrides(argv: list[str]) -> tuple[list[str], str, str, str, str, str]:
    try:
        return _extract_global_overrides_impl(argv)
    except RuntimeError as exc:
        raise CLIError(str(exc)) from exc


def _build_parser() -> argparse.ArgumentParser:
    return _build_parser_impl(
        handlers=sys.modules[__name__],
        odds_api_default_max_credits=_runtime_odds_api_default_max_credits(),
        row_selections=ROW_SELECTIONS,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""
    raw_argv = list(argv) if isinstance(argv, list) else sys.argv[1:]
    (
        parsed_argv,
        config_override,
        data_dir_override,
        reports_dir_override,
        nba_data_dir_override,
        runtime_dir_override,
    ) = _extract_global_overrides(raw_argv)
    config_path = Path(config_override).expanduser() if config_override else None
    try:
        runtime_config = load_runtime_config(config_path)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    runtime_config = runtime_config.with_path_overrides(
        odds_data_dir=Path(data_dir_override).expanduser().resolve() if data_dir_override else None,
        nba_data_dir=(
            Path(nba_data_dir_override).expanduser().resolve() if nba_data_dir_override else None
        ),
        reports_dir=(
            Path(reports_dir_override).expanduser().resolve() if reports_dir_override else None
        ),
        runtime_dir=Path(runtime_dir_override).expanduser().resolve()
        if runtime_dir_override
        else None,
    )
    try:
        set_current_runtime_config(runtime_config)

        parser = _build_parser()
        args = parser.parse_args(parsed_argv)
        func = getattr(args, "func", None)
        if func is None:
            parser.print_help()
            return 0
        try:
            return int(func(args))
        except (
            CLIError,
            OddsAPIError,
            CreditBudgetExceeded,
            OfflineCacheMiss,
            SpendBlockedError,
            FileNotFoundError,
            ValueError,
        ) as exc:
            print(str(exc), file=sys.stderr)
            return 2
    finally:
        set_current_runtime_config(None)


if __name__ == "__main__":
    raise SystemExit(main())
