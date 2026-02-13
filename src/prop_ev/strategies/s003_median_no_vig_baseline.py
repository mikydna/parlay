from __future__ import annotations

from prop_ev.strategies.base import (
    StrategyInfo,
    StrategyInputs,
    StrategyPlugin,
    StrategyRecipe,
    StrategyResult,
    StrategyRunConfig,
    compose_strategy_recipes,
    run_strategy_recipe,
)


class S003:
    info = StrategyInfo(
        id="s003",
        name="Median No-Vig Baseline",
        description=("Uses median per-book no-vig baseline, with fallback to best-sides baseline."),
    )
    recipe = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(
            market_baseline_method="median_book",
            market_baseline_fallback="best_sides",
        ),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S003()
