from __future__ import annotations

from dataclasses import replace

from prop_ev.strategies.base import (
    StrategyInfo,
    StrategyInputs,
    StrategyPlugin,
    StrategyResult,
    StrategyRunConfig,
)
from prop_ev.strategy import build_strategy_report


class V0TierBStrategy:
    info = StrategyInfo(
        id="v0_tier_b",
        name="v0_tier_b",
        description=(
            "v0 strategy but forces allow_tier_b=true (single-book lines included with "
            "higher thresholds)."
        ),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        forced = replace(config, allow_tier_b=True)
        report = build_strategy_report(
            snapshot_id=inputs.snapshot_id,
            manifest=inputs.manifest,
            rows=inputs.rows,
            top_n=forced.top_n,
            injuries=inputs.injuries,
            roster=inputs.roster,
            event_context=inputs.event_context,
            slate_rows=inputs.slate_rows,
            player_identity_map=inputs.player_identity_map,
            min_ev=forced.min_ev,
            allow_tier_b=forced.allow_tier_b,
            require_official_injuries=forced.require_official_injuries,
            stale_quote_minutes=forced.stale_quote_minutes,
            require_fresh_context=forced.require_fresh_context,
        )
        return StrategyResult(report=report, config=forced)


def plugin() -> StrategyPlugin:
    return V0TierBStrategy()
