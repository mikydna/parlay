from __future__ import annotations

from prop_ev.strategies.base import (
    StrategyInfo,
    StrategyInputs,
    StrategyPlugin,
    StrategyResult,
    StrategyRunConfig,
)
from prop_ev.strategy import build_strategy_report


class GateBookPairsMin2Strategy:
    info = StrategyInfo(
        id="gate_book_pairs_min2",
        name="gate_book_pairs_min2",
        description="Skips lines with fewer than 2 books offering both over+under for the point.",
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        report = build_strategy_report(
            snapshot_id=inputs.snapshot_id,
            manifest=inputs.manifest,
            rows=inputs.rows,
            top_n=config.top_n,
            injuries=inputs.injuries,
            roster=inputs.roster,
            event_context=inputs.event_context,
            slate_rows=inputs.slate_rows,
            player_identity_map=inputs.player_identity_map,
            min_ev=config.min_ev,
            allow_tier_b=config.allow_tier_b,
            require_official_injuries=config.require_official_injuries,
            stale_quote_minutes=config.stale_quote_minutes,
            require_fresh_context=config.require_fresh_context,
            min_book_pairs=2,
        )
        return StrategyResult(report=report, config=config)


def plugin() -> StrategyPlugin:
    return GateBookPairsMin2Strategy()
