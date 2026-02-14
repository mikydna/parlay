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


class S010:
    info = StrategyInfo(
        id="s010",
        name="Tier B + Quality Floor",
        description=(
            "Extends baseline with tier-B single-book edges while enforcing conservative "
            "quality/uncertainty gates (best-sides baseline)."
        ),
    )
    recipe = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(force_allow_tier_b=True),
        StrategyRecipe(hold_cap=0.08),
        StrategyRecipe(p_over_iqr_cap=0.08),
        StrategyRecipe(min_quality_score=0.55),
        StrategyRecipe(min_ev_low=0.01),
        StrategyRecipe(max_uncertainty_band=0.08),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S010()
