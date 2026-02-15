"""Strategy run command implementation."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from prop_ev.backtest import write_backtest_artifacts
from prop_ev.cli_shared import (
    CLIError,
    _env_bool,
    _env_float,
    _env_int,
    _runtime_nba_data_dir,
    _runtime_odds_data_dir,
    _runtime_strategy_probabilistic_profile,
)
from prop_ev.cli_strategy.context import (
    _hydrate_slate_for_strategy,
    _load_event_context,
    _load_slate_rows,
)
from prop_ev.cli_strategy.shared import (
    _allow_secondary_injuries_override,
    _coerce_dict,
    _execution_projection_tag,
    _latest_snapshot_id,
    _load_rolling_priors_for_strategy,
    _load_strategy_context,
    _official_injury_hard_fail_message,
    _official_source_ready,
    _replay_quote_now_from_manifest,
    _resolve_input_probabilistic_profile,
    _resolve_strategy_runtime_policy,
    _secondary_source_ready,
    _snapshot_date,
    _teams_in_scope,
)
from prop_ev.execution_projection import ExecutionProjectionConfig, project_execution_report
from prop_ev.identity_map import load_identity_map, update_identity_map
from prop_ev.nba_data.minutes_prob import load_minutes_prob_index_for_snapshot
from prop_ev.nba_data.repo import NBARepository
from prop_ev.nba_data.store.layout import build_layout as build_nba_layout
from prop_ev.odds_client import (
    parse_csv,
)
from prop_ev.report_paths import (
    snapshot_reports_dir,
)
from prop_ev.state_keys import (
    strategy_title,
)
from prop_ev.storage import SnapshotStore
from prop_ev.strategies import get_strategy
from prop_ev.strategies.base import (
    StrategyInputs,
    StrategyRunConfig,
    decorate_report,
    normalize_strategy_id,
)
from prop_ev.strategy import (
    load_jsonl,
    write_execution_plan,
    write_strategy_reports,
    write_tagged_strategy_reports,
)


def _load_strategy_inputs(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    offline: bool,
    block_paid: bool,
    refresh_context: bool,
    probabilistic_profile: str,
) -> tuple[
    Path,
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, dict[str, str]],
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    snapshot_dir = store.snapshot_dir(snapshot_id)
    manifest = store.load_manifest(snapshot_id)
    derived_path = snapshot_dir / "derived" / "event_props.jsonl"
    if not derived_path.exists():
        raise CLIError(f"missing derived props file: {derived_path}")

    rows = load_jsonl(derived_path)
    event_context = _load_event_context(store, snapshot_id, manifest)
    slate_rows = _load_slate_rows(store, snapshot_id)
    if not slate_rows and not offline and not block_paid:
        _hydrate_slate_for_strategy(store, snapshot_id, manifest)
        manifest = store.load_manifest(snapshot_id)
        event_context = _load_event_context(store, snapshot_id, manifest)
        slate_rows = _load_slate_rows(store, snapshot_id)

    injuries_stale_hours = _env_float("PROP_EV_CONTEXT_INJURIES_STALE_HOURS", 6.0)
    roster_stale_hours = _env_float("PROP_EV_CONTEXT_ROSTER_STALE_HOURS", 24.0)
    context_repo = NBARepository.from_store(store=store, snapshot_id=snapshot_id)
    identity_map_path = context_repo.identity_map_path()
    teams_in_scope = sorted(_teams_in_scope(event_context))
    injuries, roster, _, _ = _load_strategy_context(
        store=store,
        snapshot_id=snapshot_id,
        teams_in_scope=teams_in_scope,
        offline=offline,
        refresh_context=refresh_context,
        injuries_stale_hours=injuries_stale_hours,
        roster_stale_hours=roster_stale_hours,
    )
    update_identity_map(
        path=identity_map_path,
        rows=rows,
        roster=roster if isinstance(roster, dict) else None,
        event_context=event_context,
    )
    player_identity_map = load_identity_map(identity_map_path)
    minutes_probabilities = load_minutes_prob_index_for_snapshot(
        layout=build_nba_layout(Path(_runtime_nba_data_dir()).expanduser().resolve()),
        snapshot_day=_snapshot_date(snapshot_id),
        probabilistic_profile=probabilistic_profile,
        auto_build=True,
    )
    return (
        snapshot_dir,
        manifest,
        rows,
        event_context,
        slate_rows,
        injuries,
        roster,
        player_identity_map,
        minutes_probabilities,
    )


def _cmd_strategy_run(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    strategy_requested = str(getattr(args, "strategy", "s001"))
    plugin = get_strategy(strategy_requested)
    strategy_id = normalize_strategy_id(plugin.info.id)
    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    allow_secondary_injuries = _allow_secondary_injuries_override(
        cli_flag=bool(getattr(args, "allow_secondary_injuries", False)),
        default=_env_bool("PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES", False),
    )
    stale_quote_minutes_env = _env_int("PROP_EV_STRATEGY_STALE_QUOTE_MINUTES", 20)
    require_fresh_context_env = _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True)
    probabilistic_profile = _resolve_input_probabilistic_profile(
        default_profile=str(_runtime_strategy_probabilistic_profile()),
        probabilistic_profile_arg=str(getattr(args, "probabilistic_profile", "")),
        strategy_ids=[strategy_id],
    )
    (
        strategy_run_mode,
        stale_quote_minutes,
        require_fresh_context,
    ) = _resolve_strategy_runtime_policy(
        mode=str(getattr(args, "mode", "auto")),
        stale_quote_minutes=stale_quote_minutes_env,
        require_fresh_context=require_fresh_context_env,
    )
    (
        snapshot_dir,
        manifest,
        rows,
        event_context,
        slate_rows,
        injuries,
        roster,
        player_identity_map,
        minutes_probabilities,
    ) = _load_strategy_inputs(
        store=store,
        snapshot_id=snapshot_id,
        offline=bool(args.offline),
        block_paid=bool(getattr(args, "block_paid", False)),
        refresh_context=bool(args.refresh_context),
        probabilistic_profile=probabilistic_profile,
    )
    official = _coerce_dict(injuries.get("official")) if isinstance(injuries, dict) else {}
    secondary = _coerce_dict(injuries.get("secondary")) if isinstance(injuries, dict) else {}
    official_ready = _official_source_ready(official)
    secondary_ready = _secondary_source_ready(secondary)
    secondary_override_active = allow_secondary_injuries and secondary_ready and not official_ready
    if require_official_injuries and not official_ready and not secondary_override_active:
        raise CLIError(_official_injury_hard_fail_message())
    effective_require_official = require_official_injuries and not secondary_override_active
    if secondary_override_active:
        print("note=official_injury_missing_using_secondary_override")

    max_picks_default = _env_int("PROP_EV_STRATEGY_MAX_PICKS_DEFAULT", 5)
    requested_max_picks = int(getattr(args, "max_picks", 0))
    resolved_max_picks = (
        requested_max_picks if requested_max_picks > 0 else max(0, int(max_picks_default))
    )
    rolling_priors: dict[str, Any] = {}
    strategy_recipe = getattr(plugin, "recipe", None)
    if bool(getattr(strategy_recipe, "use_rolling_priors", False)):
        rolling_source_strategy_id = str(
            getattr(strategy_recipe, "rolling_priors_source_strategy_id", "") or strategy_id
        )
        rolling_source_strategy_id = normalize_strategy_id(rolling_source_strategy_id)
        rolling_priors = _load_rolling_priors_for_strategy(
            store=store,
            strategy_id=rolling_source_strategy_id,
            snapshot_id=snapshot_id,
        )
    replay_quote_now_utc = (
        _replay_quote_now_from_manifest(manifest) if strategy_run_mode == "replay" else None
    )
    config = StrategyRunConfig(
        top_n=int(args.top_n),
        max_picks=resolved_max_picks,
        min_ev=float(args.min_ev),
        allow_tier_b=bool(args.allow_tier_b),
        require_official_injuries=bool(effective_require_official),
        stale_quote_minutes=int(stale_quote_minutes),
        require_fresh_context=bool(require_fresh_context),
        probabilistic_profile=probabilistic_profile,
        quote_now_utc=replay_quote_now_utc,
    )
    inputs = StrategyInputs(
        snapshot_id=snapshot_id,
        manifest=manifest,
        rows=rows,
        injuries=injuries if isinstance(injuries, dict) else None,
        roster=roster if isinstance(roster, dict) else None,
        event_context=event_context if isinstance(event_context, dict) else None,
        slate_rows=slate_rows,
        player_identity_map=player_identity_map if isinstance(player_identity_map, dict) else None,
        rolling_priors=rolling_priors,
        minutes_probabilities=(
            minutes_probabilities if isinstance(minutes_probabilities, dict) else None
        ),
    )
    result = plugin.run(inputs=inputs, config=config)
    report = decorate_report(result.report, strategy=plugin.info, config=result.config)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    write_markdown = bool(getattr(args, "write_markdown", False))
    write_canonical_raw = getattr(args, "write_canonical", None)
    if write_canonical_raw is None:
        write_canonical = bool(strategy_id == "s001")
    else:
        write_canonical = bool(write_canonical_raw)

    json_path, md_path = write_strategy_reports(
        reports_dir=reports_dir,
        report=report,
        top_n=args.top_n,
        strategy_id=strategy_id,
        write_canonical=write_canonical,
        write_markdown=write_markdown,
    )
    execution_plan_path = write_execution_plan(
        reports_dir=reports_dir,
        report=report,
        strategy_id=strategy_id,
        write_canonical=write_canonical,
    )
    write_backtest_artifacts_flag = bool(getattr(args, "write_backtest_artifacts", False))
    backtest: dict[str, Any] = {
        "seed_jsonl": "",
        "results_template_csv": "",
        "readiness_json": "",
        "ready_for_backtest_seed": False,
        "ready_for_settlement": False,
    }
    if write_backtest_artifacts_flag:
        backtest = write_backtest_artifacts(
            snapshot_dir=snapshot_dir,
            reports_dir=reports_dir,
            report=report,
            selection="ranked",
            top_n=0,
            strategy_id=strategy_id,
            write_canonical=write_canonical,
        )
    summary = report.get("summary", {})
    health = (
        report.get("health_report", {}) if isinstance(report.get("health_report"), dict) else {}
    )
    health_gates = (
        health.get("health_gates", []) if isinstance(health.get("health_gates"), list) else []
    )
    print(
        (
            "snapshot_id={} strategy_status={} strategy_mode={} events={} candidate_lines={} "
            "tier_a={} tier_b={} eligible={}"
        ).format(
            snapshot_id,
            report.get("strategy_status", ""),
            report.get("strategy_mode", ""),
            summary.get("events", 0),
            summary.get("candidate_lines", 0),
            summary.get("tier_a_lines", 0),
            summary.get("tier_b_lines", 0),
            summary.get("eligible_lines", 0),
        )
    )
    print(f"strategy_id={strategy_id}")
    title = strategy_title(strategy_id)
    if title:
        print(f"strategy_title={title}")
    print(f"strategy_run_mode={strategy_run_mode}")
    print(f"strategy_max_picks={resolved_max_picks}")
    print(
        "rolling_priors_window_days={} rolling_priors_rows_used={}".format(
            int(rolling_priors.get("window_days", 0)),
            int(rolling_priors.get("rows_used", 0)),
        )
    )
    print(f"probabilistic_profile={probabilistic_profile}")
    print(f"stale_quote_minutes={stale_quote_minutes}")
    print(f"require_fresh_context={str(bool(require_fresh_context)).lower()}")
    print(f"health_gates={','.join(health_gates) if health_gates else 'none'}")
    print(f"report_json={json_path}")
    print(f"execution_plan_json={execution_plan_path}")
    if write_markdown:
        print(f"report_md={md_path}")
    card = reports_dir / "strategy-card.md"
    if not write_canonical:
        card = card.with_name(f"{card.stem}.{strategy_id}{card.suffix}")
    if write_markdown:
        print(f"report_card={card}")
    if write_backtest_artifacts_flag:
        print(f"backtest_seed_jsonl={backtest['seed_jsonl']}")
        print(f"backtest_results_template_csv={backtest['results_template_csv']}")
        print(f"backtest_readiness_json={backtest['readiness_json']}")
    context_repo = NBARepository.from_store(store=store, snapshot_id=snapshot_id)
    identity_map_path = context_repo.identity_map_path()
    identity_map = load_identity_map(identity_map_path)
    entries = (
        len(identity_map.get("players", {})) if isinstance(identity_map.get("players"), dict) else 0
    )
    print(f"identity_map={identity_map_path} entries={entries}")
    injuries_context_path, roster_context_path, _results_context_path = context_repo.context_paths()
    print(f"injuries_context={injuries_context_path}")
    print(f"roster_context={roster_context_path}")

    execution_books = tuple(parse_csv(str(getattr(args, "execution_bookmakers", ""))))
    if execution_books:
        execution_top_n_raw = int(getattr(args, "execution_top_n", 0))
        execution_top_n = execution_top_n_raw if execution_top_n_raw > 0 else int(args.top_n)
        execution_config = ExecutionProjectionConfig(
            bookmakers=execution_books,
            top_n=max(0, execution_top_n),
            requires_pre_bet_ready=bool(getattr(args, "execution_requires_pre_bet_ready", False)),
            requires_meets_play_to=bool(getattr(args, "execution_requires_meets_play_to", False)),
            tier_a_min_ev=float(getattr(args, "execution_tier_a_min_ev", 0.03)),
            tier_b_min_ev=float(getattr(args, "execution_tier_b_min_ev", 0.05)),
        )
        projected_report = project_execution_report(
            report=report,
            event_prop_rows=rows,
            config=execution_config,
        )
        execution_tag = _execution_projection_tag(execution_books)
        execution_json, execution_md = write_tagged_strategy_reports(
            reports_dir=reports_dir,
            report=projected_report,
            top_n=max(0, execution_top_n),
            tag=execution_tag,
            write_markdown=write_markdown,
        )
        execution_summary = (
            projected_report.get("summary", {})
            if isinstance(projected_report.get("summary"), dict)
            else {}
        )
        print(f"execution_bookmakers={','.join(execution_books)}")
        print(f"execution_tag={execution_tag}")
        print(
            "execution_candidates={} execution_eligible={}".format(
                execution_summary.get("candidate_lines", 0),
                execution_summary.get("eligible_lines", 0),
            )
        )
        print(f"execution_report_json={execution_json}")
        if write_markdown:
            print(f"execution_report_md={execution_md}")
    return 0
