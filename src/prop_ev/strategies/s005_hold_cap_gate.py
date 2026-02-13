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


class S005:
    info = StrategyInfo(
        id="s005",
        name="Hold-Cap Gate",
        description="Skips lines when median per-book hold exceeds the configured cap.",
    )
    recipe = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(hold_cap=0.08),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S005()
