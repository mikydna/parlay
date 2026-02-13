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


class S004Min2BookPairGateStrategy:
    info = StrategyInfo(
        id="s004",
        name="Min-2 Book-Pair Gate",
        description=(
            "Skips lines unless at least 2 books post both over and under at the same point."
        ),
    )
    recipe = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(min_book_pairs=2),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S004Min2BookPairGateStrategy()
