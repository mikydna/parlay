"""Shared odds conversion and EV math helpers."""

from __future__ import annotations


def implied_prob_from_american(price: int | None) -> float | None:
    """Convert American odds to implied probability."""
    if price is None:
        return None
    if price > 0:
        return 100.0 / (price + 100.0)
    if price < 0:
        value = -price
        return value / (value + 100.0)
    return None


def american_to_decimal(price: int | None) -> float | None:
    """Convert American odds to decimal odds."""
    if price is None:
        return None
    if price > 0:
        return 1.0 + (price / 100.0)
    if price < 0:
        return 1.0 + (100.0 / abs(price))
    return None


def decimal_to_american(decimal_odds: float | None) -> int | None:
    """Convert decimal odds to American odds."""
    if decimal_odds is None or decimal_odds <= 1.0:
        return None
    if decimal_odds >= 2.0:
        return int(round((decimal_odds - 1.0) * 100.0))
    return int(round(-100.0 / (decimal_odds - 1.0)))


def normalize_prob_pair(over_prob: float, under_prob: float) -> tuple[float, float]:
    """Normalize an over/under implied-probability pair to no-vig."""
    total = over_prob + under_prob
    if total <= 0:
        return 0.5, 0.5
    return over_prob / total, under_prob / total


def ev_from_prob_and_price(probability: float | None, price: int | None) -> float | None:
    """Compute 1-unit expected value from hit probability and American price."""
    if probability is None or probability <= 0 or probability >= 1:
        return None
    decimal_odds = american_to_decimal(price)
    if decimal_odds is None or decimal_odds <= 1.0:
        return None
    return (probability * (decimal_odds - 1.0)) - (1.0 - probability)
