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


class S006:
    info = StrategyInfo(
        id="s006",
        name="Dispersion-IQR Gate",
        description="Skips lines when per-book no-vig probability IQR exceeds the configured cap.",
    )
    recipe = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(p_over_iqr_cap=0.08),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S006()
