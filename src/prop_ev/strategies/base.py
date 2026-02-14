from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Protocol

from prop_ev.portfolio import PortfolioRanking
from prop_ev.state_keys import (
    attach_strategy_description_key,
    attach_strategy_title_key,
    strategy_meta,
)


def normalize_strategy_id(value: str) -> str:
    raw = value.strip().lower().replace("-", "_")
    if not raw:
        raise ValueError("strategy id is required")
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789_")
    if any(ch not in allowed for ch in raw):
        raise ValueError(f"invalid strategy id: {value}")
    return raw


@dataclass(frozen=True)
class StrategyInfo:
    id: str
    name: str
    description: str


@dataclass(frozen=True)
class StrategyInputs:
    snapshot_id: str
    manifest: dict[str, Any]
    rows: list[dict[str, Any]]
    injuries: dict[str, Any] | None
    roster: dict[str, Any] | None
    event_context: dict[str, dict[str, str]] | None
    slate_rows: list[dict[str, Any]] | None
    player_identity_map: dict[str, Any] | None
    rolling_priors: dict[str, Any] | None = None


@dataclass(frozen=True)
class StrategyRunConfig:
    top_n: int
    max_picks: int
    min_ev: float
    allow_tier_b: bool
    require_official_injuries: bool
    stale_quote_minutes: int
    require_fresh_context: bool


@dataclass(frozen=True)
class StrategyResult:
    report: dict[str, Any]
    config: StrategyRunConfig


@dataclass(frozen=True)
class StrategyRecipe:
    force_allow_tier_b: bool = False
    use_rolling_priors: bool = False
    portfolio_ranking: PortfolioRanking | None = None
    market_baseline_method: str | None = None
    market_baseline_fallback: str | None = None
    min_book_pairs: int | None = None
    hold_cap: float | None = None
    p_over_iqr_cap: float | None = None
    min_quality_score: float | None = None
    min_ev_low: float | None = None
    max_uncertainty_band: float | None = None


class StrategyPlugin(Protocol):
    info: StrategyInfo

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        raise NotImplementedError


def compose_strategy_recipes(*recipes: StrategyRecipe) -> StrategyRecipe:
    merged = StrategyRecipe()
    for recipe in recipes:
        merged = StrategyRecipe(
            force_allow_tier_b=merged.force_allow_tier_b or bool(recipe.force_allow_tier_b),
            use_rolling_priors=merged.use_rolling_priors or bool(recipe.use_rolling_priors),
            portfolio_ranking=(
                recipe.portfolio_ranking
                if recipe.portfolio_ranking is not None
                else merged.portfolio_ranking
            ),
            market_baseline_method=(
                recipe.market_baseline_method
                if recipe.market_baseline_method is not None
                else merged.market_baseline_method
            ),
            market_baseline_fallback=(
                recipe.market_baseline_fallback
                if recipe.market_baseline_fallback is not None
                else merged.market_baseline_fallback
            ),
            min_book_pairs=(
                recipe.min_book_pairs
                if recipe.min_book_pairs is not None
                else merged.min_book_pairs
            ),
            hold_cap=recipe.hold_cap if recipe.hold_cap is not None else merged.hold_cap,
            p_over_iqr_cap=(
                recipe.p_over_iqr_cap
                if recipe.p_over_iqr_cap is not None
                else merged.p_over_iqr_cap
            ),
            min_quality_score=(
                recipe.min_quality_score
                if recipe.min_quality_score is not None
                else merged.min_quality_score
            ),
            min_ev_low=recipe.min_ev_low if recipe.min_ev_low is not None else merged.min_ev_low,
            max_uncertainty_band=(
                recipe.max_uncertainty_band
                if recipe.max_uncertainty_band is not None
                else merged.max_uncertainty_band
            ),
        )
    return merged


def run_strategy_recipe(
    *, inputs: StrategyInputs, config: StrategyRunConfig, recipe: StrategyRecipe
) -> StrategyResult:
    """Run one strategy as baseline + composable recipe adjustments."""
    from prop_ev.strategy import build_strategy_report

    effective_config = (
        replace(config, allow_tier_b=True)
        if recipe.force_allow_tier_b and not config.allow_tier_b
        else config
    )
    report = build_strategy_report(
        snapshot_id=inputs.snapshot_id,
        manifest=inputs.manifest,
        rows=inputs.rows,
        top_n=effective_config.top_n,
        max_picks=effective_config.max_picks,
        injuries=inputs.injuries,
        roster=inputs.roster,
        event_context=inputs.event_context,
        slate_rows=inputs.slate_rows,
        player_identity_map=inputs.player_identity_map,
        rolling_priors=inputs.rolling_priors if recipe.use_rolling_priors else None,
        min_ev=effective_config.min_ev,
        allow_tier_b=effective_config.allow_tier_b,
        require_official_injuries=effective_config.require_official_injuries,
        stale_quote_minutes=effective_config.stale_quote_minutes,
        require_fresh_context=effective_config.require_fresh_context,
        portfolio_ranking=recipe.portfolio_ranking
        if recipe.portfolio_ranking is not None
        else "default",
        market_baseline_method=recipe.market_baseline_method or "best_sides",
        market_baseline_fallback=recipe.market_baseline_fallback or "best_sides",
        min_book_pairs=recipe.min_book_pairs if recipe.min_book_pairs is not None else 0,
        hold_cap=recipe.hold_cap,
        p_over_iqr_cap=recipe.p_over_iqr_cap,
        min_quality_score=recipe.min_quality_score,
        min_ev_low=recipe.min_ev_low,
        max_uncertainty_band=recipe.max_uncertainty_band,
    )
    return StrategyResult(report=report, config=effective_config)


def decorate_report(
    report: dict[str, Any], *, strategy: StrategyInfo, config: StrategyRunConfig
) -> dict[str, Any]:
    """Attach plugin identity and config to a report without changing its meaning."""
    strategy_id = normalize_strategy_id(strategy.id)
    strategy_payload = strategy_meta(
        strategy_id=strategy_id,
        strategy_name=strategy.name,
        strategy_description=strategy.description,
    )
    report["strategy_id"] = strategy_id
    report["strategy"] = strategy_payload
    report["state_key"] = attach_strategy_title_key(
        report.get("state_key"),
        strategy_id=strategy_id,
        strategy_title=strategy.name,
    )
    report["state_key"] = attach_strategy_description_key(
        report.get("state_key"),
        strategy_id=strategy_id,
        strategy_description=strategy.description,
    )

    audit = report.get("audit", {})
    if not isinstance(audit, dict):
        audit = {}
        report["audit"] = audit

    audit["strategy_id"] = strategy_id
    audit["strategy_name"] = strategy.name
    audit["strategy_description"] = strategy.description
    audit["strategy_config"] = asdict(config)
    execution_plan = report.get("execution_plan")
    if isinstance(execution_plan, dict):
        execution_plan["strategy_id"] = strategy_id

    summary = report.get("summary", {})
    if isinstance(summary, dict):
        summary["strategy_id"] = strategy_id

    return report
