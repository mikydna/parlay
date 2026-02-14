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


class S020:
    info = StrategyInfo(
        id="s020",
        name="Probabilistic Minutes v1",
        description=(
            "Tier-B enabled strategy using minutes-probability profile with confidence/band "
            "gates and conservative quality floors."
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
        StrategyRecipe(
            probabilistic_profile="minutes_v1",
            min_prob_confidence=0.5,
            max_minutes_band=22.0,
        ),
        StrategyRecipe(portfolio_ranking="ev_low_quality_weighted"),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S020()
