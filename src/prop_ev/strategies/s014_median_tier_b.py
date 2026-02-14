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


class S014:
    info = StrategyInfo(
        id="s014",
        name="Median No-Vig + Tier B",
        description=(
            "Extends s003 median no-vig baseline to include tier-B single-book edges "
            "with the same stricter EV floor."
        ),
    )
    recipe = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(
            market_baseline_method="median_book",
            market_baseline_fallback="best_sides",
        ),
        StrategyRecipe(force_allow_tier_b=True),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S014()
