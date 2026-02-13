"""Spend-policy helpers for odds-data repository calls."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SpendPolicy:
    """Policy controls for network fetch behavior."""

    offline: bool = False
    max_credits: int = 20
    no_spend: bool = False
    refresh: bool = False
    resume: bool = True
    block_paid: bool = False
    force: bool = False


def effective_max_credits(policy: SpendPolicy) -> int:
    """Return normalized max credits with no-spend override."""
    if policy.no_spend:
        return 0
    return max(0, int(policy.max_credits))
