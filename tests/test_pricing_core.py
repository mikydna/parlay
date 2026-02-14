from __future__ import annotations

from datetime import UTC, datetime

import pytest

from prop_ev.pricing_core import (
    extract_book_fair_pairs,
    resolve_baseline_selection,
    summarize_line_pricing,
)
from prop_ev.pricing_reference import ReferenceEstimate


def test_extract_book_fair_pairs_picks_best_prices_per_side() -> None:
    rows = [
        {"book": "book_b", "side": "Over", "price": -120},
        {"book": "book_b", "side": "Under", "price": 100},
        {"book": "book_a", "side": "Over", "price": -120},
        {"book": "book_a", "side": "Over", "price": -110},
        {"book": "book_a", "side": "Under", "price": 100},
        {"book": "book_a", "side": "Under", "price": 105},
        {"book": "book_c", "side": "Over", "price": -105},
    ]

    pairs = extract_book_fair_pairs(rows)

    assert [pair.book for pair in pairs] == ["book_a", "book_b"]
    assert pairs[0].over_price == -110
    assert pairs[0].under_price == 105
    assert pairs[0].p_over_fair == pytest.approx(0.517796, abs=1e-6)
    assert pairs[0].p_under_fair == pytest.approx(0.482204, abs=1e-6)
    assert pairs[0].hold == pytest.approx(0.011614, abs=1e-6)


def test_extract_book_fair_pairs_respects_excluded_books() -> None:
    rows = [
        {"book": "book_a", "side": "Over", "price": -110},
        {"book": "book_a", "side": "Under", "price": -110},
        {"book": "book_b", "side": "Over", "price": -105},
        {"book": "book_b", "side": "Under", "price": -115},
    ]

    pairs = extract_book_fair_pairs(rows, exclude_book_keys=frozenset({"book_a"}))

    assert [pair.book for pair in pairs] == ["book_b"]


def test_summarize_line_pricing_uses_hold_fallback_and_quote_age() -> None:
    now_utc = datetime(2026, 2, 14, 12, 0, tzinfo=UTC)
    rows = [
        {
            "book": "book_a",
            "side": "Over",
            "price": -110,
            "last_update": "2026-02-14T11:50:00Z",
        },
        {
            "book": "book_b",
            "side": "Over",
            "price": -108,
            "last_update": "2026-02-14T11:40:00Z",
        },
    ]

    summary = summarize_line_pricing(
        group_rows=rows,
        now_utc=now_utc,
        stale_quote_minutes=20,
        hold_fallback=0.06,
    )

    assert summary.book_pair_count == 0
    assert summary.books_used == ()
    assert summary.freshest_quote_utc == "2026-02-14T11:50:00Z"
    assert summary.quote_age_minutes == 10.0
    assert summary.depth_score == 0.0
    assert summary.hold_score == 0.5
    assert summary.dispersion_score == 0.0
    assert summary.freshness_score == 0.75
    assert summary.quality_score == 0.275
    assert summary.uncertainty_band == 0.1275


def test_summarize_line_pricing_respects_excluded_books() -> None:
    now_utc = datetime(2026, 2, 14, 12, 0, tzinfo=UTC)
    rows = [
        {
            "book": "book_a",
            "side": "Over",
            "price": -110,
            "last_update": "2026-02-14T11:50:00Z",
        },
        {
            "book": "book_a",
            "side": "Under",
            "price": -110,
            "last_update": "2026-02-14T11:50:00Z",
        },
        {
            "book": "book_b",
            "side": "Over",
            "price": -105,
            "last_update": "2026-02-14T11:49:00Z",
        },
        {
            "book": "book_b",
            "side": "Under",
            "price": -115,
            "last_update": "2026-02-14T11:49:00Z",
        },
    ]

    summary = summarize_line_pricing(
        group_rows=rows,
        now_utc=now_utc,
        stale_quote_minutes=20,
        hold_fallback=None,
        exclude_book_keys=frozenset({"book_b"}),
    )

    assert summary.books_used == ("book_a",)
    assert summary.book_pair_count == 1


def test_resolve_baseline_selection_provenance() -> None:
    reference_estimate = ReferenceEstimate(
        p_over=0.53,
        hold=0.04,
        method="interpolated",
        points_used=3,
    )

    exact = resolve_baseline_selection(
        baseline_method="median_book",
        baseline_fallback="best_sides",
        p_over_fair_best=0.5,
        p_under_fair_best=0.5,
        hold_best=0.05,
        p_over_book_median=0.54,
        hold_book_median=0.03,
        reference_estimate=reference_estimate,
    )
    assert exact.baseline_used == "median_book"
    assert exact.reference_line_method == "exact"
    assert exact.line_source == "exact_point_pairs"
    assert exact.p_over_fair == 0.54
    assert exact.p_under_fair == pytest.approx(0.46, abs=1e-9)
    assert exact.hold == 0.03

    interpolated = resolve_baseline_selection(
        baseline_method="median_book",
        baseline_fallback="best_sides",
        p_over_fair_best=0.5,
        p_under_fair_best=0.5,
        hold_best=0.05,
        p_over_book_median=None,
        hold_book_median=None,
        reference_estimate=reference_estimate,
    )
    assert interpolated.baseline_used == "median_book_interpolated"
    assert interpolated.reference_line_method == "interpolated"
    assert interpolated.line_source == "reference_curve"
    assert interpolated.p_over_fair == 0.53
    assert interpolated.p_under_fair == pytest.approx(0.47, abs=1e-9)

    missing = resolve_baseline_selection(
        baseline_method="median_book",
        baseline_fallback="none",
        p_over_fair_best=0.5,
        p_under_fair_best=0.5,
        hold_best=0.05,
        p_over_book_median=None,
        hold_book_median=None,
        reference_estimate=ReferenceEstimate(
            p_over=None,
            hold=None,
            method="missing",
            points_used=0,
        ),
    )
    assert missing.baseline_used == "missing"
    assert missing.line_source == "missing"
    assert missing.p_over_fair is None
    assert missing.p_under_fair is None
    assert missing.hold is None
