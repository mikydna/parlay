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


class S002BaselineCoreTierBStrategy:
    info = StrategyInfo(
        id="s002",
        name="Baseline Core + Tier B",
        description=(
            "Same core model as s001, but includes tier-B single-book edges with stricter EV floor."
        ),
    )
    recipe = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(force_allow_tier_b=True),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S002BaselineCoreTierBStrategy()
