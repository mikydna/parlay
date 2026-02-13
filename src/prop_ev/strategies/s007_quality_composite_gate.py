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


class S007QualityCompositeGateStrategy:
    info = StrategyInfo(
        id="s007",
        name="Quality Composite Gate",
        description=(
            "Composes median no-vig baseline with min-2 book-pair and hold-cap gates "
            "(s003 + s004 + s005)."
        ),
    )
    recipe = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(
            market_baseline_method="median_book",
            market_baseline_fallback="best_sides",
        ),
        StrategyRecipe(min_book_pairs=2),
        StrategyRecipe(hold_cap=0.08),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S007QualityCompositeGateStrategy()
