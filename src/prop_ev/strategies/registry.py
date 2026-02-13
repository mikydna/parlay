from __future__ import annotations

from collections.abc import Iterable

from prop_ev.strategies.base import StrategyPlugin, normalize_strategy_id
from prop_ev.strategies.baseline_median_novig import BaselineMedianNoVigStrategy
from prop_ev.strategies.gate_book_pairs_min2 import GateBookPairsMin2Strategy
from prop_ev.strategies.gate_dispersion_iqr import GateDispersionIQRStrategy
from prop_ev.strategies.gate_hold_cap import GateHoldCapStrategy
from prop_ev.strategies.v0 import V0Strategy
from prop_ev.strategies.v0_tier_b import V0TierBStrategy


def _registry() -> dict[str, StrategyPlugin]:
    plugins: Iterable[StrategyPlugin] = [
        V0Strategy(),
        V0TierBStrategy(),
        BaselineMedianNoVigStrategy(),
        GateBookPairsMin2Strategy(),
        GateHoldCapStrategy(),
        GateDispersionIQRStrategy(),
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
