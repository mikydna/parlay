"""Strategy plugins for offline report generation.

Plugins are intentionally deterministic and operate only on snapshot-derived inputs.
"""

from prop_ev.strategies.registry import (
    get_strategy,
    list_strategies,
    resolve_strategy_id,
    strategy_aliases,
)

__all__ = ["get_strategy", "list_strategies", "resolve_strategy_id", "strategy_aliases"]
