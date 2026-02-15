"""Strategy list/compare command implementations."""

from __future__ import annotations

import argparse
import json
from typing import Any

from prop_ev.backtest import write_backtest_artifacts
from prop_ev.cli_markdown import (
    render_strategy_compare_markdown as _render_strategy_compare_markdown,
)
from prop_ev.cli_shared import (
    CLIError,
    _env_bool,
    _env_int,
    _iso,
    _runtime_odds_data_dir,
    _runtime_strategy_probabilistic_profile,
    _utc_now,
)
from prop_ev.cli_strategy.run import _load_strategy_inputs
from prop_ev.cli_strategy.shared import (
    _latest_snapshot_id,
    _load_rolling_priors_for_strategy,
    _replay_quote_now_from_manifest,
    _resolve_input_probabilistic_profile,
    _resolve_strategy_runtime_policy,
)
from prop_ev.report_paths import (
    snapshot_reports_dir,
)
from prop_ev.storage import SnapshotStore
from prop_ev.strategies import get_strategy, list_strategies, resolve_strategy_id
from prop_ev.strategies.base import (
    StrategyInputs,
    StrategyRunConfig,
    decorate_report,
    normalize_strategy_id,
)
from prop_ev.strategy import (
    write_execution_plan,
    write_strategy_reports,
)


def _cmd_strategy_ls(args: argparse.Namespace) -> int:
    del args
    for plugin in list_strategies():
        strategy_id = normalize_strategy_id(plugin.info.id)
        print(f"{strategy_id}\t{plugin.info.name}\t{plugin.info.description}")
    return 0


def _parse_strategy_ids(raw: str) -> list[str]:
    values = [item.strip() for item in (raw or "").split(",") if item.strip()]
    seen: set[str] = set()
    parsed: list[str] = []
    for value in values:
        plugin = get_strategy(resolve_strategy_id(value))
        canonical_id = normalize_strategy_id(plugin.info.id)
        if canonical_id in seen:
            continue
        seen.add(canonical_id)
        parsed.append(canonical_id)
    return parsed


def _ranked_key(row: dict[str, Any]) -> tuple[str, str, str, float, str]:
    event_id = str(row.get("event_id", "")).strip()
    player = str(row.get("player", "")).strip()
    market = str(row.get("market", "")).strip()
    point_raw = row.get("point")
    point = float(point_raw) if isinstance(point_raw, (int, float)) else 0.0
    side = str(row.get("recommended_side", "")).strip().lower()
    return (event_id, player, market, point, side)


def _cmd_strategy_compare(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    write_markdown = bool(getattr(args, "write_markdown", False))

    strategy_ids = _parse_strategy_ids(getattr(args, "strategies", ""))
    if len(strategy_ids) < 2:
        raise CLIError("compare requires --strategies with at least 2 unique ids")

    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    stale_quote_minutes_env = _env_int("PROP_EV_STRATEGY_STALE_QUOTE_MINUTES", 20)
    require_fresh_context_env = _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True)
    probabilistic_profile = _resolve_input_probabilistic_profile(
        default_profile=str(_runtime_strategy_probabilistic_profile()),
        probabilistic_profile_arg=str(getattr(args, "probabilistic_profile", "")),
        strategy_ids=strategy_ids,
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
    replay_quote_now_utc = (
        _replay_quote_now_from_manifest(manifest) if strategy_run_mode == "replay" else None
    )

    base_config = StrategyRunConfig(
        top_n=int(args.top_n),
        max_picks=(
            int(args.max_picks)
            if int(args.max_picks) > 0
            else _env_int("PROP_EV_STRATEGY_MAX_PICKS_DEFAULT", 5)
        ),
        min_ev=float(args.min_ev),
        allow_tier_b=bool(args.allow_tier_b),
        require_official_injuries=bool(require_official_injuries),
        stale_quote_minutes=int(stale_quote_minutes),
        require_fresh_context=bool(require_fresh_context),
        probabilistic_profile=probabilistic_profile,
        quote_now_utc=replay_quote_now_utc,
    )
    compare_rows: list[dict[str, Any]] = []
    ranked_sets: dict[str, set[tuple[str, str, str, float, str]]] = {}
    for requested in strategy_ids:
        plugin = get_strategy(requested)
        strategy_id = normalize_strategy_id(plugin.info.id)
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
        inputs = StrategyInputs(
            snapshot_id=snapshot_id,
            manifest=manifest,
            rows=rows,
            injuries=injuries if isinstance(injuries, dict) else None,
            roster=roster if isinstance(roster, dict) else None,
            event_context=event_context if isinstance(event_context, dict) else None,
            slate_rows=slate_rows,
            player_identity_map=(
                player_identity_map if isinstance(player_identity_map, dict) else None
            ),
            rolling_priors=rolling_priors,
            minutes_probabilities=(
                minutes_probabilities if isinstance(minutes_probabilities, dict) else None
            ),
        )
        result = plugin.run(inputs=inputs, config=base_config)
        report = decorate_report(result.report, strategy=plugin.info, config=result.config)
        strategy_id = normalize_strategy_id(report.get("strategy_id", strategy_id))

        write_strategy_reports(
            reports_dir=reports_dir,
            report=report,
            top_n=int(args.top_n),
            strategy_id=strategy_id,
            write_canonical=False,
            write_markdown=write_markdown,
        )
        write_execution_plan(
            reports_dir=reports_dir,
            report=report,
            strategy_id=strategy_id,
            write_canonical=False,
        )
        write_backtest_artifacts(
            snapshot_dir=snapshot_dir,
            reports_dir=reports_dir,
            report=report,
            selection="ranked",
            top_n=0,
            strategy_id=strategy_id,
            write_canonical=False,
        )

        summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
        compare_rows.append(
            {
                "strategy_id": strategy_id,
                "strategy_mode": str(report.get("strategy_mode", "")),
                "candidate_lines": int(summary.get("candidate_lines", 0)),
                "eligible_lines": int(summary.get("eligible_lines", 0)),
                "tier_a_lines": int(summary.get("tier_a_lines", 0)),
                "tier_b_lines": int(summary.get("tier_b_lines", 0)),
                "health_gate_count": int(summary.get("health_gate_count", 0)),
                "rolling_priors_rows_used": int(summary.get("rolling_priors_rows_used", 0)),
            }
        )

        ranked = (
            report.get("ranked_plays", []) if isinstance(report.get("ranked_plays"), list) else []
        )
        ranked_sets[strategy_id] = {
            _ranked_key(row)
            for row in ranked
            if isinstance(row, dict) and str(row.get("event_id", "")).strip()
        }

    intersection: set[tuple[str, str, str, float, str]] | None = None
    union: set[tuple[str, str, str, float, str]] = set()
    for keys in ranked_sets.values():
        union |= keys
        intersection = keys if intersection is None else (intersection & keys)
    intersection_count = len(intersection or set())

    compare_report = {
        "generated_at_utc": _iso(_utc_now()),
        "summary": {
            "snapshot_id": snapshot_id,
            "strategy_count": len(strategy_ids),
            "top_n": int(args.top_n),
            "max_picks": int(base_config.max_picks),
        },
        "strategies": sorted(compare_rows, key=lambda row: row.get("strategy_id", "")),
        "ranked_overlap": {
            "intersection_all": intersection_count,
            "union_all": len(union),
        },
    }
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "strategy-compare.json"
    md_path = reports_dir / "strategy-compare.md"
    json_path.write_text(
        json.dumps(compare_report, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    md_path.write_text(_render_strategy_compare_markdown(compare_report), encoding="utf-8")

    print(f"snapshot_id={snapshot_id}")
    print(f"strategies={','.join(strategy_ids)}")
    print(f"strategy_max_picks={int(base_config.max_picks)}")
    print(f"probabilistic_profile={probabilistic_profile}")
    print(f"compare_json={json_path}")
    print(f"compare_md={md_path}")
    return 0
