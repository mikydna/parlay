from __future__ import annotations

from prop_ev.strategies.base import (
    StrategyInfo,
    StrategyInputs,
    StrategyPlugin,
    StrategyResult,
    StrategyRunConfig,
)
from prop_ev.strategy import build_strategy_report


class BaselineMedianNoVigStrategy:
    info = StrategyInfo(
        id="baseline_median_novig",
        name="baseline_median_novig",
        description=(
            "Uses median per-book no-vig baseline when available; "
            "falls back to best-sides baseline."
        ),
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
            market_baseline_method="median_book",
            market_baseline_fallback="best_sides",
        )
        return StrategyResult(report=report, config=config)


def plugin() -> StrategyPlugin:
    return BaselineMedianNoVigStrategy()
