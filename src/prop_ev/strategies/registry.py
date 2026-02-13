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


def _registry() -> dict[str, StrategyPlugin]:
    plugins: Iterable[StrategyPlugin] = [
        S001(),
        S002(),
        S003(),
        S004(),
        S005(),
        S006(),
        S007(),
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
