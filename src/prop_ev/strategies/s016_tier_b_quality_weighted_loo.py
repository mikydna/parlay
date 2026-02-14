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


class S016:
    info = StrategyInfo(
        id="s016",
        name="Tier B + LOO Quality Weighted",
        description=(
            "Tier-B conservative stack with leave-one-out baseline exclusion and "
            "EV-low quality-weighted ranking."
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
        StrategyRecipe(exclude_selected_book_from_baseline=True),
        StrategyRecipe(tier_b_min_other_books_for_baseline=2),
        StrategyRecipe(portfolio_ranking="ev_low_quality_weighted"),
    )

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        return run_strategy_recipe(
            inputs=inputs,
            config=config,
            recipe=self.recipe,
        )


def plugin() -> StrategyPlugin:
    return S016()
