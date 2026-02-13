from __future__ import annotations

from prop_ev.strategies.base import (
    StrategyInfo,
    StrategyInputs,
    StrategyPlugin,
    StrategyRecipe,
    StrategyResult,
    StrategyRunConfig,
    run_strategy_recipe,
)


class S001BaselineCoreStrategy:
    info = StrategyInfo(
        id="s001",
        name="Baseline Core",
        description=(
            "Best-over/best-under no-vig baseline with deterministic minutes/usage and context "
            "gates."
        ),
    )
    recipe = StrategyRecipe()

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S001BaselineCoreStrategy()
