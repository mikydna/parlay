"""Parser construction for prop-ev CLI."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from typing import Any


def build_parser(
    *,
    handlers: Any,
    odds_api_default_max_credits: int,
    row_selections: Iterable[str],
) -> argparse.ArgumentParser:
    _cmd_credits_budget = handlers._cmd_credits_budget
    _cmd_credits_report = handlers._cmd_credits_report
    _cmd_data_backfill = handlers._cmd_data_backfill
    _cmd_data_datasets_ls = handlers._cmd_data_datasets_ls
    _cmd_data_datasets_show = handlers._cmd_data_datasets_show
    _cmd_data_done_days = handlers._cmd_data_done_days
    _cmd_data_guardrails = handlers._cmd_data_guardrails
    _cmd_data_migrate_layout = handlers._cmd_data_migrate_layout
    _cmd_data_repair_derived = handlers._cmd_data_repair_derived
    _cmd_data_status = handlers._cmd_data_status
    _cmd_data_verify = handlers._cmd_data_verify
    _cmd_playbook_budget = handlers._cmd_playbook_budget
    _cmd_playbook_discover_execute = handlers._cmd_playbook_discover_execute
    _cmd_playbook_publish = handlers._cmd_playbook_publish
    _cmd_playbook_render = handlers._cmd_playbook_render
    _cmd_playbook_run = handlers._cmd_playbook_run
    _cmd_snapshot_diff = handlers._cmd_snapshot_diff
    _cmd_snapshot_lake = handlers._cmd_snapshot_lake
    _cmd_snapshot_ls = handlers._cmd_snapshot_ls
    _cmd_snapshot_pack = handlers._cmd_snapshot_pack
    _cmd_snapshot_props = handlers._cmd_snapshot_props
    _cmd_snapshot_show = handlers._cmd_snapshot_show
    _cmd_snapshot_slate = handlers._cmd_snapshot_slate
    _cmd_snapshot_unpack = handlers._cmd_snapshot_unpack
    _cmd_snapshot_verify = handlers._cmd_snapshot_verify
    _cmd_strategy_ablation = handlers._cmd_strategy_ablation
    _cmd_strategy_backtest_prep = handlers._cmd_strategy_backtest_prep
    _cmd_strategy_backtest_summarize = handlers._cmd_strategy_backtest_summarize
    _cmd_strategy_compare = handlers._cmd_strategy_compare
    _cmd_strategy_health = handlers._cmd_strategy_health
    _cmd_strategy_ls = handlers._cmd_strategy_ls
    _cmd_strategy_run = handlers._cmd_strategy_run
    _cmd_strategy_settle = handlers._cmd_strategy_settle
    parser = argparse.ArgumentParser(prog="prop-ev")
    parser.add_argument(
        "--config",
        default="",
        help="Path to runtime config TOML (default: config/runtime.toml).",
    )
    parser.add_argument(
        "--data-dir",
        default="",
        help="Override odds data dir for this command invocation.",
    )
    parser.add_argument(
        "--reports-dir",
        default="",
        help=(
            "Override reports output dir for this command invocation. "
            "Default comes from runtime config."
        ),
    )
    parser.add_argument(
        "--nba-data-dir",
        default="",
        help="Override NBA data dir for this command invocation.",
    )
    parser.add_argument(
        "--runtime-dir",
        default="",
        help="Override runtime/cache dir for this command invocation.",
    )
    subparsers = parser.add_subparsers(dest="command")

    snapshot = subparsers.add_parser("snapshot", help="Create and inspect snapshots")
    snapshot_subparsers = snapshot.add_subparsers(dest="snapshot_command")

    snapshot_slate = snapshot_subparsers.add_parser("slate", help="Fetch slate featured odds")
    snapshot_slate.set_defaults(func=_cmd_snapshot_slate)
    snapshot_slate.add_argument("--sport-key", default="basketball_nba")
    snapshot_slate.add_argument("--markets", default="spreads,totals")
    snapshot_slate.add_argument("--regions", default="us")
    snapshot_slate.add_argument("--bookmakers", default="")
    snapshot_slate.add_argument("--snapshot-id", default="")
    snapshot_slate.add_argument("--commence-from", default="")
    snapshot_slate.add_argument("--commence-to", default="")
    snapshot_slate.add_argument("--max-credits", type=int, default=20)
    snapshot_slate.add_argument("--force", action="store_true")
    snapshot_slate.add_argument("--refresh", action="store_true")
    snapshot_slate.add_argument("--resume", action="store_true")
    snapshot_slate.add_argument("--offline", action="store_true")
    snapshot_slate.add_argument("--block-paid", action="store_true")
    snapshot_slate.add_argument("--dry-run", action="store_true")

    snapshot_props = snapshot_subparsers.add_parser("props", help="Fetch per-event prop odds")
    snapshot_props.set_defaults(func=_cmd_snapshot_props)
    snapshot_props.add_argument("--sport-key", default="basketball_nba")
    snapshot_props.add_argument("--markets", default="player_points")
    snapshot_props.add_argument("--regions", default="us")
    snapshot_props.add_argument("--bookmakers", default="")
    snapshot_props.add_argument("--snapshot-id", default="")
    snapshot_props.add_argument("--commence-from", default="")
    snapshot_props.add_argument("--commence-to", default="")
    snapshot_props.add_argument("--include-links", action="store_true")
    snapshot_props.add_argument("--include-sids", action="store_true")
    snapshot_props.add_argument("--max-events", type=int, default=0)
    snapshot_props.add_argument("--max-credits", type=int, default=20)
    snapshot_props.add_argument("--force", action="store_true")
    snapshot_props.add_argument("--refresh", action="store_true")
    snapshot_props.add_argument("--resume", action="store_true")
    snapshot_props.add_argument("--offline", action="store_true")
    snapshot_props.add_argument("--block-paid", action="store_true")
    snapshot_props.add_argument("--dry-run", action="store_true")

    snapshot_ls = snapshot_subparsers.add_parser("ls", help="List snapshots")
    snapshot_ls.set_defaults(func=_cmd_snapshot_ls)

    snapshot_show = snapshot_subparsers.add_parser("show", help="Show snapshot summary")
    snapshot_show.set_defaults(func=_cmd_snapshot_show)
    snapshot_show.add_argument("--snapshot-id", required=True)

    snapshot_diff = snapshot_subparsers.add_parser("diff", help="Diff derived snapshot outputs")
    snapshot_diff.set_defaults(func=_cmd_snapshot_diff)
    snapshot_diff.add_argument("--a", required=True)
    snapshot_diff.add_argument("--b", required=True)

    snapshot_verify = snapshot_subparsers.add_parser("verify", help="Verify snapshot artifacts")
    snapshot_verify.set_defaults(func=_cmd_snapshot_verify)
    snapshot_verify.add_argument("--snapshot-id", required=True)
    snapshot_verify.add_argument(
        "--check-derived",
        action="store_true",
        help="Also verify derived quote-table contracts and parity",
    )
    snapshot_verify.add_argument(
        "--require-parquet",
        action="store_true",
        help="Fail if known derived tables are missing parquet mirrors",
    )
    snapshot_verify.add_argument(
        "--require-table",
        action="append",
        default=[],
        help="Require one derived JSONL table (repeatable, e.g. event_props)",
    )

    snapshot_lake = snapshot_subparsers.add_parser(
        "lake", help="Convert derived JSONL artifacts to Parquet lake format"
    )
    snapshot_lake.set_defaults(func=_cmd_snapshot_lake)
    snapshot_lake.add_argument("--snapshot-id", required=True)

    snapshot_pack = snapshot_subparsers.add_parser(
        "pack", help="Pack one snapshot into a compressed tar bundle"
    )
    snapshot_pack.set_defaults(func=_cmd_snapshot_pack)
    snapshot_pack.add_argument("--snapshot-id", required=True)
    snapshot_pack.add_argument("--out", default="")

    snapshot_unpack = snapshot_subparsers.add_parser(
        "unpack", help="Unpack a snapshot bundle into the data directory"
    )
    snapshot_unpack.set_defaults(func=_cmd_snapshot_unpack)
    snapshot_unpack.add_argument("--bundle", required=True)

    data_cmd = subparsers.add_parser("data", help="Dataset status and backfill tools")
    data_subparsers = data_cmd.add_subparsers(dest="data_command")

    data_status = data_subparsers.add_parser("status", help="Show day completeness status")
    data_status.set_defaults(func=_cmd_data_status)
    data_status.add_argument(
        "--dataset-id",
        default="",
        help="Use an existing dataset id and ignore spec flags",
    )
    data_status.add_argument("--sport-key", default="basketball_nba")
    data_status.add_argument("--markets", default="player_points")
    data_status.add_argument("--regions", default="us")
    data_status.add_argument("--bookmakers", default="")
    data_status.add_argument("--include-links", action="store_true")
    data_status.add_argument("--include-sids", action="store_true")
    data_status.add_argument("--days", type=int, default=10)
    data_status.add_argument("--from", dest="from_day", default="")
    data_status.add_argument("--to", dest="to_day", default="")
    data_status.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_status.add_argument("--historical", action="store_true")
    data_status.add_argument("--historical-anchor-hour-local", type=int, default=12)
    data_status.add_argument("--historical-pre-tip-minutes", type=int, default=60)
    data_status.add_argument("--refresh", action="store_true")
    data_status.add_argument(
        "--json-summary",
        action="store_true",
        help="Emit machine-readable day summary JSON",
    )

    data_done_days = data_subparsers.add_parser(
        "done-days",
        help="Show complete vs incomplete days from stored day-index rows",
    )
    data_done_days.set_defaults(func=_cmd_data_done_days)
    data_done_days.add_argument("--dataset-id", required=True)
    data_done_days.add_argument("--from", dest="from_day", default="")
    data_done_days.add_argument("--to", dest="to_day", default="")
    data_done_days.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_done_days.add_argument(
        "--allow-incomplete-day",
        action="append",
        default=[],
        help="Allow one incomplete day (repeatable, YYYY-MM-DD).",
    )
    data_done_days.add_argument(
        "--allow-incomplete-reason",
        action="append",
        default=[],
        help="Allow incomplete reason code (repeatable, supports comma-separated values).",
    )
    data_done_days.add_argument(
        "--require-complete",
        action="store_true",
        help="Exit 2 when any selected day is incomplete after allowlist filtering.",
    )
    data_done_days.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    data_datasets = data_subparsers.add_parser(
        "datasets", help="Inspect stored dataset specs and day indexes"
    )
    data_datasets_subparsers = data_datasets.add_subparsers(dest="datasets_command")

    data_datasets_ls = data_datasets_subparsers.add_parser("ls", help="List known datasets")
    data_datasets_ls.set_defaults(func=_cmd_data_datasets_ls)
    data_datasets_ls.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    data_datasets_show = data_datasets_subparsers.add_parser(
        "show", help="Show one dataset spec and indexed day rows"
    )
    data_datasets_show.set_defaults(func=_cmd_data_datasets_show)
    data_datasets_show.add_argument("--dataset-id", required=True)
    data_datasets_show.add_argument("--from", dest="from_day", default="")
    data_datasets_show.add_argument("--to", dest="to_day", default="")
    data_datasets_show.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_datasets_show.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    data_verify = data_subparsers.add_parser(
        "verify",
        help="Verify dataset day-index and derived quote-table contracts",
    )
    data_verify.set_defaults(func=_cmd_data_verify)
    data_verify.add_argument("--dataset-id", required=True)
    data_verify.add_argument("--from", dest="from_day", default="")
    data_verify.add_argument("--to", dest="to_day", default="")
    data_verify.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_verify.add_argument(
        "--require-complete",
        action="store_true",
        help="Fail when selected days are incomplete",
    )
    data_verify.add_argument(
        "--require-parquet",
        action="store_true",
        help="Fail when complete-day snapshots are missing required parquet mirrors",
    )
    data_verify.add_argument(
        "--require-canonical-jsonl",
        action="store_true",
        help="Fail when complete-day snapshots have non-canonical derived JSONL rows",
    )
    data_verify.add_argument(
        "--allow-incomplete-day",
        action="append",
        default=[],
        help="Allow one incomplete day (repeatable, YYYY-MM-DD)",
    )
    data_verify.add_argument(
        "--allow-incomplete-reason",
        action="append",
        default=[],
        help="Allow incomplete reason code (repeatable, supports comma-separated values)",
    )
    data_verify.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    data_repair = data_subparsers.add_parser(
        "repair-derived",
        help="Canonicalize derived JSONL and rebuild parquet for complete dataset days",
    )
    data_repair.set_defaults(func=_cmd_data_repair_derived)
    data_repair.add_argument("--dataset-id", required=True)
    data_repair.add_argument("--from", dest="from_day", default="")
    data_repair.add_argument("--to", dest="to_day", default="")
    data_repair.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_repair.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    data_backfill = data_subparsers.add_parser("backfill", help="Backfill day snapshots")
    data_backfill.set_defaults(func=_cmd_data_backfill)
    data_backfill.add_argument("--sport-key", default="basketball_nba")
    data_backfill.add_argument("--markets", default="player_points")
    data_backfill.add_argument("--regions", default="us")
    data_backfill.add_argument("--bookmakers", default="")
    data_backfill.add_argument("--include-links", action="store_true")
    data_backfill.add_argument("--include-sids", action="store_true")
    data_backfill.add_argument("--days", type=int, default=10)
    data_backfill.add_argument("--from", dest="from_day", default="")
    data_backfill.add_argument("--to", dest="to_day", default="")
    data_backfill.add_argument("--tz", dest="tz_name", default="America/New_York")
    data_backfill.add_argument("--historical", action="store_true")
    data_backfill.add_argument("--historical-anchor-hour-local", type=int, default=12)
    data_backfill.add_argument("--historical-pre-tip-minutes", type=int, default=60)
    data_backfill.add_argument(
        "--max-credits",
        type=int,
        default=odds_api_default_max_credits,
    )
    data_backfill.add_argument("--no-spend", action="store_true")
    data_backfill.add_argument("--offline", action="store_true")
    data_backfill.add_argument("--refresh", action="store_true")
    data_backfill.add_argument("--resume", action="store_true", default=True)
    data_backfill.add_argument("--block-paid", action="store_true")
    data_backfill.add_argument("--force", action="store_true")
    data_backfill.add_argument("--dry-run", action="store_true")

    data_guardrails = data_subparsers.add_parser(
        "guardrails",
        help="Check lake/report/runtime boundary guardrails",
    )
    data_guardrails.set_defaults(func=_cmd_data_guardrails)
    data_guardrails.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    data_migrate_layout = data_subparsers.add_parser(
        "migrate-layout",
        help="Migrate legacy layout into P0 lake/report/runtime contract",
    )
    data_migrate_layout.set_defaults(func=_cmd_data_migrate_layout)
    data_migrate_layout.add_argument(
        "--snapshot-id",
        action="append",
        default=[],
        help="Restrict migration to one snapshot id (repeatable). Defaults to all snapshots.",
    )
    data_migrate_layout.add_argument(
        "--apply",
        action="store_true",
        help="Apply filesystem mutations (default is dry-run planning only).",
    )
    data_migrate_layout.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit machine-readable JSON payload",
    )

    credits = subparsers.add_parser("credits", help="Credit tooling")
    credits_subparsers = credits.add_subparsers(dest="credits_command")

    credits_report = credits_subparsers.add_parser("report", help="Report usage ledger")
    credits_report.set_defaults(func=_cmd_credits_report)
    credits_report.add_argument("--month", default="")

    credits_budget = credits_subparsers.add_parser("budget", help="Estimate budget")
    credits_budget.set_defaults(func=_cmd_credits_budget)
    credits_budget.add_argument("--events", type=int, default=0)
    credits_budget.add_argument("--markets", default="player_points")
    credits_budget.add_argument("--regions", default="us")
    credits_budget.add_argument("--bookmakers", default="")

    strategy = subparsers.add_parser("strategy", help="Run offline strategy reports")
    strategy_subparsers = strategy.add_subparsers(dest="strategy_command")

    strategy_ls = strategy_subparsers.add_parser("ls", help="List available strategy plugins")
    strategy_ls.set_defaults(func=_cmd_strategy_ls)

    strategy_run = strategy_subparsers.add_parser("run", help="Generate strategy report")
    strategy_run.set_defaults(func=_cmd_strategy_run)
    strategy_run.add_argument("--snapshot-id", default="")
    strategy_run.add_argument("--strategy", default="s001")
    strategy_run.add_argument("--top-n", type=int, default=25)
    strategy_run.add_argument(
        "--max-picks",
        type=int,
        default=0,
        help=(
            "Daily ranked pick cap (<=top-n). "
            "When omitted, uses runtime config strategy.max_picks_default."
        ),
    )
    strategy_run.add_argument("--min-ev", type=float, default=0.01)
    strategy_run.add_argument(
        "--mode",
        choices=("auto", "live", "replay"),
        default="auto",
        help="Strategy runtime mode (replay relaxes freshness gates for historical reruns).",
    )
    strategy_run.add_argument(
        "--probabilistic-profile",
        choices=("off", "minutes_v1"),
        default="",
        help=(
            "Optional probabilistic profile override. "
            "When omitted, uses runtime config strategy.probabilistic_profile."
        ),
    )
    strategy_run.add_argument("--allow-tier-b", action="store_true")
    strategy_run.add_argument(
        "--write-backtest-artifacts",
        action="store_true",
        help="Write seed/template/readiness backtest artifacts (disabled by default).",
    )
    strategy_run.add_argument(
        "--write-markdown",
        action="store_true",
        help="Write strategy markdown artifacts (disabled by default).",
    )
    strategy_run.add_argument("--offline", action="store_true")
    strategy_run.add_argument("--block-paid", action="store_true")
    strategy_run.add_argument("--refresh-context", action="store_true")
    strategy_run.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Allow secondary injury source when official report is unavailable.",
    )
    strategy_run.add_argument(
        "--execution-bookmakers",
        default="",
        help="Comma-separated execution books for projected strategy outputs.",
    )
    strategy_run.add_argument(
        "--execution-top-n",
        type=int,
        default=0,
        help="Projected execution report top-N (defaults to --top-n when omitted).",
    )
    strategy_run.add_argument(
        "--execution-requires-pre-bet-ready",
        action="store_true",
        help="Require pre_bet_ready=true for execution eligibility.",
    )
    strategy_run.add_argument(
        "--execution-requires-meets-play-to",
        action="store_true",
        help="Require selected execution price to meet play_to_american.",
    )
    strategy_run.add_argument("--execution-tier-a-min-ev", type=float, default=0.03)
    strategy_run.add_argument("--execution-tier-b-min-ev", type=float, default=0.05)

    strategy_health = strategy_subparsers.add_parser(
        "health", help="Report injury/roster/mapping health for a snapshot"
    )
    strategy_health.set_defaults(func=_cmd_strategy_health)
    strategy_health.add_argument("--snapshot-id", default="")
    strategy_health.add_argument("--offline", action="store_true")
    strategy_health.add_argument("--refresh-context", action="store_true")
    strategy_health.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Treat secondary injuries as explicit override when official report is unavailable.",
    )
    strategy_health.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Emit JSON output (default)",
    )
    strategy_health.add_argument(
        "--no-json",
        dest="json_output",
        action="store_false",
        help="Emit compact text output",
    )

    strategy_compare = strategy_subparsers.add_parser(
        "compare", help="Run multiple strategies for the same snapshot"
    )
    strategy_compare.set_defaults(func=_cmd_strategy_compare)
    strategy_compare.add_argument("--snapshot-id", default="")
    strategy_compare.add_argument("--strategies", required=True)
    strategy_compare.add_argument("--top-n", type=int, default=25)
    strategy_compare.add_argument(
        "--max-picks",
        type=int,
        default=0,
        help=(
            "Daily ranked pick cap per strategy (<=top-n). "
            "When omitted, uses runtime config strategy.max_picks_default."
        ),
    )
    strategy_compare.add_argument("--min-ev", type=float, default=0.01)
    strategy_compare.add_argument(
        "--mode",
        choices=("auto", "live", "replay"),
        default="auto",
        help="Strategy runtime mode (replay relaxes freshness gates for historical reruns).",
    )
    strategy_compare.add_argument(
        "--probabilistic-profile",
        choices=("off", "minutes_v1"),
        default="",
        help=(
            "Optional probabilistic profile override. "
            "When omitted, uses runtime config strategy.probabilistic_profile."
        ),
    )
    strategy_compare.add_argument("--allow-tier-b", action="store_true")
    strategy_compare.add_argument(
        "--write-markdown",
        action="store_true",
        help="Write suffixed strategy markdown artifacts (disabled by default).",
    )
    strategy_compare.add_argument("--offline", action="store_true")
    strategy_compare.add_argument("--block-paid", action="store_true")
    strategy_compare.add_argument("--refresh-context", action="store_true")

    strategy_ablation = strategy_subparsers.add_parser(
        "ablation",
        help="Run multi-cap strategy ablation with per-cap report roots and cache reuse",
    )
    strategy_ablation.set_defaults(func=_cmd_strategy_ablation)
    strategy_ablation.add_argument("--snapshot-id", default="")
    strategy_ablation.add_argument("--dataset-id", default="")
    strategy_ablation.add_argument("--strategies", required=True)
    strategy_ablation.add_argument(
        "--caps",
        default="1,2,5",
        help="Comma-separated max-picks caps to evaluate (default: 1,2,5).",
    )
    strategy_ablation.add_argument("--top-n", type=int, default=10)
    strategy_ablation.add_argument("--min-ev", type=float, default=0.01)
    strategy_ablation.add_argument(
        "--mode",
        choices=("auto", "live", "replay"),
        default="replay",
    )
    strategy_ablation.add_argument("--allow-tier-b", action="store_true")
    strategy_ablation.add_argument(
        "--probabilistic-profile",
        choices=("off", "minutes_v1"),
        default="",
    )
    strategy_ablation.add_argument(
        "--offline",
        dest="offline",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use offline mode for compare/settle sub-steps (default: true).",
    )
    strategy_ablation.add_argument(
        "--block-paid",
        dest="block_paid",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Block paid cache misses in compare steps (default: true).",
    )
    strategy_ablation.add_argument("--refresh-context", action="store_true")
    strategy_ablation.add_argument(
        "--results-source",
        choices=("auto", "historical", "live", "cache_only"),
        default="historical",
    )
    strategy_ablation.add_argument(
        "--prebuild-minutes-cache",
        dest="prebuild_minutes_cache",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Prebuild per-day minutes cache before compare loops (default: true).",
    )
    strategy_ablation.add_argument("--reports-root", default="")
    strategy_ablation.add_argument(
        "--run-id",
        default="",
        help="Run folder id under reports/odds/ablation (default: latest).",
    )
    strategy_ablation.add_argument(
        "--prune-intermediate",
        dest="prune_intermediate",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Delete per-snapshot by-snapshot/state artifacts after summarize "
            "and keep only run summary + analysis outputs (default: true)."
        ),
    )
    strategy_ablation.add_argument(
        "--write-scoreboard-pdf",
        dest="write_scoreboard_pdf",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write per-cap `aggregate-scoreboard.pdf` artifacts (default: true).",
    )
    strategy_ablation.add_argument(
        "--keep-scoreboard-tex",
        dest="keep_scoreboard_tex",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Keep per-cap `aggregate-scoreboard.tex` source files (default: false).",
    )
    strategy_ablation.add_argument("--analysis-run-prefix", default="ablation")
    strategy_ablation.add_argument(
        "--reuse-existing",
        dest="reuse_existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reuse unchanged compare/settle outputs when cache signatures match (default: true).",
    )
    strategy_ablation.add_argument(
        "--force",
        action="store_true",
        help="Force recompute for all days/strategies/caps.",
    )
    strategy_ablation.add_argument(
        "--force-days",
        default="",
        help="Comma-separated day values (YYYY-MM-DD) and/or snapshot ids to recompute.",
    )
    strategy_ablation.add_argument(
        "--force-strategies",
        default="",
        help="Comma-separated strategy ids to recompute.",
    )
    strategy_ablation.add_argument("--max-workers", type=int, default=6)
    strategy_ablation.add_argument("--cap-workers", type=int, default=3)
    strategy_ablation.add_argument("--min-graded", type=int, default=0)
    strategy_ablation.add_argument("--bin-size", type=float, default=0.1)
    strategy_ablation.add_argument("--require-scored-fraction", type=float, default=0.9)
    strategy_ablation.add_argument("--ece-slack", type=float, default=0.01)
    strategy_ablation.add_argument("--brier-slack", type=float, default=0.01)
    strategy_ablation.add_argument("--power-alpha", type=float, default=0.05)
    strategy_ablation.add_argument("--power-level", type=float, default=0.8)
    strategy_ablation.add_argument(
        "--power-target-uplifts",
        default="0.01,0.02,0.03,0.05",
    )
    strategy_ablation.add_argument(
        "--power-target-uplift-gate",
        type=float,
        default=0.02,
        help="Target ROI uplift per bet used for per-strategy power gate status.",
    )
    strategy_ablation.add_argument(
        "--require-power-gate",
        action="store_true",
        help="Fail promotion gate when the strategy is underpowered at the selected uplift.",
    )
    strategy_ablation.add_argument(
        "--calibration-map-mode",
        choices=("walk_forward", "in_sample"),
        default="walk_forward",
    )

    strategy_backtest_prep = strategy_subparsers.add_parser(
        "backtest-prep", help="Write backtest seed/readiness artifacts for a snapshot"
    )
    strategy_backtest_prep.set_defaults(func=_cmd_strategy_backtest_prep)
    strategy_backtest_prep.add_argument("--snapshot-id", default="")
    strategy_backtest_prep.add_argument("--strategy", default="")
    strategy_backtest_prep.add_argument(
        "--selection", choices=sorted(row_selections), default="eligible"
    )
    strategy_backtest_prep.add_argument("--top-n", type=int, default=0)

    strategy_settle = strategy_subparsers.add_parser(
        "settle", help="Grade backtest seed tickets using live NBA boxscore results"
    )
    strategy_settle.set_defaults(func=_cmd_strategy_settle)
    strategy_settle.add_argument("--snapshot-id", default="")
    strategy_settle.add_argument(
        "--seed-path",
        default="",
        help="Optional override path to backtest seed jsonl",
    )
    strategy_settle.add_argument(
        "--strategy-report-file",
        default="",
        help=(
            "Optional strategy report file (relative to snapshot reports/ or absolute). "
            "When omitted, settle prefers strategy-brief.meta.json strategy_report_path."
        ),
    )
    strategy_settle.add_argument("--offline", action="store_true")
    strategy_settle.add_argument("--refresh-results", action="store_true")
    strategy_settle.add_argument(
        "--results-source",
        choices=["auto", "historical", "live", "cache_only"],
        default="auto",
        help="Unified NBA source policy for settlement.",
    )
    strategy_settle.add_argument("--write-csv", action="store_true")
    strategy_settle.add_argument(
        "--write-markdown",
        action="store_true",
        help="Write settlement markdown artifact (disabled by default).",
    )
    strategy_settle.add_argument(
        "--no-pdf",
        action="store_true",
        help="Skip settlement PDF generation (useful for bulk backtests).",
    )
    strategy_settle.add_argument(
        "--keep-tex",
        action="store_true",
        help="Keep settlement .tex artifact after PDF generation (disabled by default).",
    )
    strategy_settle.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Emit JSON output (default)",
    )
    strategy_settle.add_argument(
        "--no-json",
        dest="json_output",
        action="store_false",
        help="Emit compact text output",
    )
    strategy_backtest_summarize = strategy_subparsers.add_parser(
        "backtest-summarize", help="Summarize graded backtest CSVs for one snapshot"
    )
    strategy_backtest_summarize.set_defaults(func=_cmd_strategy_backtest_summarize)
    strategy_backtest_summarize.add_argument("--snapshot-id", default="")
    strategy_backtest_summarize.add_argument("--strategies", default="")
    strategy_backtest_summarize.add_argument("--results", action="append", default=[])
    strategy_backtest_summarize.add_argument(
        "--all-complete-days",
        action="store_true",
        help="Aggregate each strategy across complete dataset days and summarize once.",
    )
    strategy_backtest_summarize.add_argument(
        "--dataset-id",
        default="",
        help="Dataset id used with --all-complete-days (required when multiple datasets exist).",
    )
    strategy_backtest_summarize.add_argument(
        "--baseline-strategy",
        default="",
        help=(
            "Baseline strategy for calibration-regression promotion checks "
            "(default: s007 if present)."
        ),
    )
    strategy_backtest_summarize.add_argument(
        "--require-scored-fraction",
        type=float,
        default=0.9,
        help="Minimum fraction of win/loss rows with valid model probabilities.",
    )
    strategy_backtest_summarize.add_argument(
        "--ece-slack",
        type=float,
        default=0.01,
        help="Allowed ECE regression vs baseline before failing promotion gate.",
    )
    strategy_backtest_summarize.add_argument(
        "--brier-slack",
        type=float,
        default=0.01,
        help="Allowed Brier regression vs baseline before failing promotion gate.",
    )
    strategy_backtest_summarize.add_argument(
        "--power-alpha",
        type=float,
        default=0.05,
        help="Type-I error rate used for promotion-floor power guidance.",
    )
    strategy_backtest_summarize.add_argument(
        "--power-level",
        type=float,
        default=0.8,
        help="Target statistical power used for promotion-floor guidance.",
    )
    strategy_backtest_summarize.add_argument(
        "--power-picks-per-day",
        type=int,
        default=5,
        help="Assumed executable picks per day when converting required days to graded rows.",
    )
    strategy_backtest_summarize.add_argument(
        "--power-target-uplifts",
        default="0.01,0.02,0.03,0.05",
        help="Comma-separated ROI uplift targets per bet used in power guidance.",
    )
    strategy_backtest_summarize.add_argument(
        "--power-target-uplift-gate",
        type=float,
        default=0.02,
        help=(
            "Power gate target ROI uplift per bet used for per-strategy power status "
            "(requires --all-complete-days)."
        ),
    )
    strategy_backtest_summarize.add_argument(
        "--require-power-gate",
        action="store_true",
        help=(
            "Fail promotion gate when power gate is underpowered for --power-target-uplift-gate."
        ),
    )
    strategy_backtest_summarize.add_argument("--min-graded", type=int, default=0)
    strategy_backtest_summarize.add_argument("--bin-size", type=float, default=0.05)
    strategy_backtest_summarize.add_argument(
        "--write-calibration-map",
        action="store_true",
        help="Write `backtest-calibration-map.json` for optional pick-level confidence annotation.",
    )
    strategy_backtest_summarize.add_argument(
        "--calibration-map-mode",
        choices=("walk_forward", "in_sample"),
        default="walk_forward",
        help="Calibration map build mode (walk_forward avoids same-day leakage).",
    )
    strategy_backtest_summarize.add_argument(
        "--write-markdown",
        action="store_true",
        help="Write backtest summary markdown artifact (disabled by default).",
    )
    strategy_backtest_summarize.add_argument(
        "--write-analysis-scoreboard",
        action="store_true",
        help=(
            "Write aggregate scoreboard artifact under "
            "`reports/odds/analysis/<run_id>/aggregate-scoreboard.json`."
        ),
    )
    strategy_backtest_summarize.add_argument(
        "--analysis-run-id",
        default="",
        help=(
            "Optional analysis run id used with --write-analysis-scoreboard. "
            "When omitted, a deterministic id is derived from dataset/snapshot."
        ),
    )
    strategy_backtest_summarize.add_argument(
        "--write-analysis-pdf",
        action="store_true",
        help=("Write direct LaTeX/PDF aggregate scoreboard artifacts in the analysis directory."),
    )
    strategy_backtest_summarize.add_argument(
        "--keep-analysis-tex",
        action="store_true",
        help="Keep generated aggregate-scoreboard.tex (otherwise removed on successful compile).",
    )

    playbook = subparsers.add_parser("playbook", help="Run playbook briefs")
    playbook_subparsers = playbook.add_subparsers(dest="playbook_command")

    playbook_run = playbook_subparsers.add_parser("run", help="Run live/offline playbook flow")
    playbook_run.set_defaults(func=_cmd_playbook_run)
    playbook_run.add_argument("--snapshot-id", default="")
    playbook_run.add_argument("--sport-key", default="basketball_nba")
    playbook_run.add_argument("--markets", default="player_points")
    playbook_run.add_argument("--regions", default="us")
    playbook_run.add_argument("--bookmakers", default="")
    playbook_run.add_argument("--commence-from", default="")
    playbook_run.add_argument("--commence-to", default="")
    playbook_run.add_argument(
        "--include-links", dest="include_links", action="store_true", default=True
    )
    playbook_run.add_argument("--no-include-links", dest="include_links", action="store_false")
    playbook_run.add_argument(
        "--include-sids", dest="include_sids", action="store_true", default=True
    )
    playbook_run.add_argument("--no-include-sids", dest="include_sids", action="store_false")
    playbook_run.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum events to fetch for props snapshot (0 means no cap).",
    )
    playbook_run.add_argument("--max-credits", type=int, default=20)
    playbook_run.add_argument("--force", action="store_true")
    playbook_run.add_argument("--refresh", action="store_true")
    playbook_run.add_argument("--resume", action="store_true")
    playbook_run.add_argument("--offline", action="store_true")
    playbook_run.add_argument("--block-paid", action="store_true")
    playbook_run.add_argument("--refresh-context", action="store_true")
    playbook_run.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Allow run when official injuries are unavailable and secondary source is healthy.",
    )
    playbook_run.add_argument("--refresh-llm", action="store_true")
    playbook_run.add_argument("--strategy", default="")
    playbook_run.add_argument("--top-n", type=int, default=0)
    playbook_run.add_argument("--per-game-top-n", type=int, default=0)
    playbook_run.add_argument("--strategy-top-n", type=int, default=25)
    playbook_run.add_argument(
        "--calibration-map-file",
        default="",
        help=(
            "Optional calibration map JSON (absolute or relative to snapshot reports dir). "
            "When omitted, playbook auto-loads backtest-calibration-map.json if present."
        ),
    )
    playbook_run.add_argument("--min-ev", type=float, default=0.01)
    playbook_run.add_argument("--allow-tier-b", action="store_true")
    playbook_run.add_argument(
        "--write-markdown",
        action="store_true",
        help="Write `strategy-brief.md` artifact (disabled by default).",
    )
    playbook_run.add_argument(
        "--keep-tex",
        action="store_true",
        help="Keep `strategy-brief.tex` artifact after PDF generation (disabled by default).",
    )
    playbook_run.add_argument(
        "--strategy-mode",
        choices=("auto", "live", "replay"),
        default="auto",
        help="Strategy runtime mode used for playbook strategy execution.",
    )
    playbook_run.add_argument(
        "--exit-on-no-games",
        action="store_true",
        help="Exit 0 early when events lookup returns no games in the selected window",
    )
    playbook_run.add_argument("--month", default="")

    playbook_render = playbook_subparsers.add_parser(
        "render", help="Render playbook briefs for an existing snapshot"
    )
    playbook_render.set_defaults(func=_cmd_playbook_render)
    playbook_render.add_argument("--snapshot-id", required=True)
    playbook_render.add_argument("--offline", action="store_true")
    playbook_render.add_argument("--block-paid", action="store_true")
    playbook_render.add_argument("--refresh-context", action="store_true")
    playbook_render.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Allow run when official injuries are unavailable and secondary source is healthy.",
    )
    playbook_render.add_argument("--refresh-llm", action="store_true")
    playbook_render.add_argument("--strategy", default="")
    playbook_render.add_argument("--top-n", type=int, default=0)
    playbook_render.add_argument("--per-game-top-n", type=int, default=0)
    playbook_render.add_argument("--strategy-top-n", type=int, default=25)
    playbook_render.add_argument(
        "--calibration-map-file",
        default="",
        help=(
            "Optional calibration map JSON (absolute or relative to snapshot reports dir). "
            "When omitted, playbook auto-loads backtest-calibration-map.json if present."
        ),
    )
    playbook_render.add_argument("--min-ev", type=float, default=0.01)
    playbook_render.add_argument("--allow-tier-b", action="store_true")
    playbook_render.add_argument(
        "--strategy-report-file",
        default="strategy-report.json",
        help="Strategy report file name (relative to snapshot reports/) or absolute path.",
    )
    playbook_render.add_argument(
        "--write-markdown",
        action="store_true",
        help="Write `strategy-brief.md` artifact (disabled by default).",
    )
    playbook_render.add_argument(
        "--keep-tex",
        action="store_true",
        help="Keep `strategy-brief.tex` artifact after PDF generation (disabled by default).",
    )
    playbook_render.add_argument(
        "--strategy-mode",
        choices=("auto", "live", "replay"),
        default="auto",
        help="Strategy runtime mode if render triggers canonical strategy refresh.",
    )
    playbook_render.add_argument("--month", default="")

    playbook_publish = playbook_subparsers.add_parser(
        "publish", help="Publish compact reports for one snapshot"
    )
    playbook_publish.set_defaults(func=_cmd_playbook_publish)
    playbook_publish.add_argument("--snapshot-id", required=True)

    playbook_budget = playbook_subparsers.add_parser(
        "budget", help="Show odds + LLM monthly budget status"
    )
    playbook_budget.set_defaults(func=_cmd_playbook_budget)
    playbook_budget.add_argument("--month", default="")

    playbook_discover_execute = playbook_subparsers.add_parser(
        "discover-execute",
        help="Run all-books discovery + execution-book comparison in one flow",
    )
    playbook_discover_execute.set_defaults(func=_cmd_playbook_discover_execute)
    playbook_discover_execute.add_argument("--base-snapshot-id", default="")
    playbook_discover_execute.add_argument("--sport-key", default="basketball_nba")
    playbook_discover_execute.add_argument(
        "--markets",
        default="player_points,player_rebounds,player_assists,player_threes,player_points_rebounds_assists",
    )
    playbook_discover_execute.add_argument("--discovery-regions", default="us")
    playbook_discover_execute.add_argument("--execution-regions", default="us")
    playbook_discover_execute.add_argument("--execution-bookmakers", default="draftkings,fanduel")
    playbook_discover_execute.add_argument("--commence-from", default="")
    playbook_discover_execute.add_argument("--commence-to", default="")
    playbook_discover_execute.add_argument(
        "--include-links", dest="include_links", action="store_true", default=True
    )
    playbook_discover_execute.add_argument(
        "--no-include-links", dest="include_links", action="store_false"
    )
    playbook_discover_execute.add_argument(
        "--include-sids", dest="include_sids", action="store_true", default=True
    )
    playbook_discover_execute.add_argument(
        "--no-include-sids", dest="include_sids", action="store_false"
    )
    playbook_discover_execute.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum events to fetch for discovery/execution snapshots (0 means no cap).",
    )
    playbook_discover_execute.add_argument("--max-credits", type=int, default=40)
    playbook_discover_execute.add_argument("--force", action="store_true")
    playbook_discover_execute.add_argument("--refresh", action="store_true")
    playbook_discover_execute.add_argument("--resume", action="store_true")
    playbook_discover_execute.add_argument("--offline", action="store_true")
    playbook_discover_execute.add_argument("--refresh-context", action="store_true")
    playbook_discover_execute.add_argument(
        "--allow-secondary-injuries",
        action="store_true",
        help="Allow run when official injuries are unavailable and secondary source is healthy.",
    )
    playbook_discover_execute.add_argument("--refresh-llm", action="store_true")
    playbook_discover_execute.add_argument("--strategy", default="")
    playbook_discover_execute.add_argument("--top-n", type=int, default=25)
    playbook_discover_execute.add_argument("--per-game-top-n", type=int, default=5)
    playbook_discover_execute.add_argument("--strategy-top-n", type=int, default=50)
    playbook_discover_execute.add_argument(
        "--calibration-map-file",
        default="",
        help=(
            "Optional calibration map JSON (absolute or relative "
            "to execution snapshot reports dir)."
        ),
    )
    playbook_discover_execute.add_argument("--min-ev", type=float, default=0.01)
    playbook_discover_execute.add_argument("--allow-tier-b", action="store_true")
    playbook_discover_execute.add_argument(
        "--write-markdown",
        action="store_true",
        help="Write `strategy-brief.md` artifact (disabled by default).",
    )
    playbook_discover_execute.add_argument(
        "--keep-tex",
        action="store_true",
        help="Keep `strategy-brief.tex` artifact after PDF generation (disabled by default).",
    )
    playbook_discover_execute.add_argument(
        "--strategy-mode",
        choices=("auto", "live", "replay"),
        default="auto",
        help="Strategy runtime mode used by both discovery and execution strategy runs.",
    )
    playbook_discover_execute.add_argument("--month", default="")

    return parser
