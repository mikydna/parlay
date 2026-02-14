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


class S013:
    info = StrategyInfo(
        id="s013",
        name="Tier B + Quality-Weighted EV Low",
        description=(
            "Extends s002 but ranks portfolio candidates by EV-low weighted by quote quality, "
            "favoring deeper/cleaner markets when edges are similar."
        ),
    )
    recipe = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(force_allow_tier_b=True),
        StrategyRecipe(portfolio_ranking="ev_low_quality_weighted"),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S013()
