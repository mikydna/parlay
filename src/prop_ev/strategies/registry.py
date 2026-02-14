from __future__ import annotations

from collections.abc import Iterable

from prop_ev.strategies.base import StrategyPlugin, normalize_strategy_id
from prop_ev.strategies.s001_baseline_core import S001
from prop_ev.strategies.s002_baseline_core_tier_b import S002
from prop_ev.strategies.s003_median_no_vig_baseline import S003
from prop_ev.strategies.s004_min2_book_pair_gate import S004
from prop_ev.strategies.s005_hold_cap_gate import S005
from prop_ev.strategies.s006_dispersion_iqr_gate import S006
from prop_ev.strategies.s007_quality_composite_gate import S007
from prop_ev.strategies.s008_conservative_quality_floor import S008
from prop_ev.strategies.s009_conservative_quality_floor_rolling import S009
from prop_ev.strategies.s010_tier_b_quality_floor import S010
from prop_ev.strategies.s011_tier_b_quality_floor_rolling import S011
from prop_ev.strategies.s012_tier_b_aggressive_best_ev import S012
from prop_ev.strategies.s013_tier_b_quality_weighted import S013
from prop_ev.strategies.s014_median_tier_b import S014
from prop_ev.strategies.s015_tier_b_calibrated_ev_low import S015
from prop_ev.strategies.s020_prob_minutes import S020


def _registry() -> dict[str, StrategyPlugin]:
    plugins: Iterable[StrategyPlugin] = [
        S001(),
        S002(),
        S003(),
        S004(),
        S005(),
        S006(),
        S007(),
        S008(),
        S009(),
        S010(),
        S011(),
        S012(),
        S013(),
        S014(),
        S015(),
        S020(),
    ]
    out: dict[str, StrategyPlugin] = {}
    for plugin in plugins:
        strategy_id = normalize_strategy_id(plugin.info.id)
        if strategy_id in out:
            raise ValueError(f"duplicate strategy id: {strategy_id}")
        out[strategy_id] = plugin
    return out


def strategy_aliases() -> dict[str, str]:
    return {}


def resolve_strategy_id(strategy_id: str) -> str:
    return normalize_strategy_id(strategy_id)


def list_strategies() -> list[StrategyPlugin]:
    strategies = list(_registry().values())
    strategies.sort(key=lambda plugin: normalize_strategy_id(plugin.info.id))
    return strategies


def get_strategy(strategy_id: str) -> StrategyPlugin:
    normalized = resolve_strategy_id(strategy_id)
    registry = _registry()
    plugin = registry.get(normalized)
    if plugin is None:
        options = ",".join(sorted(registry.keys()))
        raise ValueError(f"unknown strategy id: {strategy_id} (options: {options})")
    return plugin
