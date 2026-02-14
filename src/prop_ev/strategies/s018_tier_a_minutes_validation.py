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


class S018:
    info = StrategyInfo(
        id="s018",
        name="Tier A Minutes Validation",
        description=(
            "Tier-A conservative stack with minutes probabilistic profile enabled "
            "for clean overlay validation."
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
        StrategyRecipe(p_over_iqr_cap=0.08),
        StrategyRecipe(min_quality_score=0.55),
        StrategyRecipe(min_ev_low=0.01),
        StrategyRecipe(max_uncertainty_band=0.08),
        StrategyRecipe(
            probabilistic_profile="minutes_v1",
            min_prob_confidence=0.5,
            max_minutes_band=22.0,
        ),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S018()
