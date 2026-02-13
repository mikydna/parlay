from __future__ import annotations

import pytest

from prop_ev.odds_math import (
    american_to_decimal,
    decimal_to_american,
    ev_from_prob_and_price,
    implied_prob_from_american,
    normalize_prob_pair,
)


@pytest.mark.parametrize(
    ("price", "expected"),
    [
        (+100, 0.5),
        (+150, 0.4),
        (-150, 0.6),
        (None, None),
        (0, None),
    ],
)
def test_implied_prob_from_american(price: int | None, expected: float | None) -> None:
    assert implied_prob_from_american(price) == expected


@pytest.mark.parametrize(
    ("price", "expected"),
    [
        (+100, 2.0),
        (+150, 2.5),
        (-200, 1.5),
        (None, None),
        (0, None),
    ],
)
def test_american_to_decimal(price: int | None, expected: float | None) -> None:
    assert american_to_decimal(price) == expected


@pytest.mark.parametrize(
    ("decimal_odds", "expected"),
    [
        (2.5, 150),
        (1.5, -200),
        (None, None),
        (1.0, None),
    ],
)
def test_decimal_to_american(decimal_odds: float | None, expected: int | None) -> None:
    assert decimal_to_american(decimal_odds) == expected


def test_normalize_prob_pair_handles_zero_total() -> None:
    assert normalize_prob_pair(0.0, 0.0) == (0.5, 0.5)


def test_normalize_prob_pair_normalizes_values() -> None:
    over, under = normalize_prob_pair(0.55, 0.6)

    assert round(over + under, 6) == 1.0
    assert over == pytest.approx(0.4782608695652174)
    assert under == pytest.approx(0.5217391304347826)


def test_ev_from_prob_and_price() -> None:
    assert ev_from_prob_and_price(0.55, -110) == pytest.approx(0.05, abs=1e-6)


@pytest.mark.parametrize("probability,price", [(None, -110), (0.0, -110), (1.0, -110), (0.55, 0)])
def test_ev_from_prob_and_price_invalid_inputs(
    probability: float | None, price: int | None
) -> None:
    assert ev_from_prob_and_price(probability, price) is None
