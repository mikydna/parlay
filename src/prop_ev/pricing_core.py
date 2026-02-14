"""Deterministic pricing primitives for de-vig, quality, and baseline selection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from prop_ev.odds_math import implied_prob_from_american, normalize_prob_pair
from prop_ev.pricing_reference import ReferenceEstimate
from prop_ev.time_utils import parse_iso_z


@dataclass(frozen=True)
class BookFairPair:
    """Per-book paired over/under line with implied and no-vig probabilities."""

    book: str
    over_price: int
    under_price: int
    p_over_implied: float
    p_under_implied: float
    p_over_fair: float
    p_under_fair: float
    hold: float


@dataclass(frozen=True)
class LinePricingQuality:
    """Deterministic quality and uncertainty summary for one candidate line."""

    book_pairs: tuple[BookFairPair, ...]
    books_used: tuple[str, ...]
    book_pair_count: int
    p_over_median: float | None
    hold_median: float | None
    p_over_iqr: float | None
    p_over_range: float | None
    freshest_quote_utc: str
    quote_age_minutes: float | None
    depth_score: float
    hold_score: float
    dispersion_score: float
    freshness_score: float
    quality_score: float
    uncertainty_band: float


@dataclass(frozen=True)
class BaselineSelection:
    """Resolved fair-probability baseline selection for strategy scoring."""

    p_over_fair: float | None
    p_under_fair: float | None
    hold: float | None
    baseline_used: str
    reference_line_method: str
    line_source: str


def _to_price(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.startswith("+"):
            raw = raw[1:]
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _quantile(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    if q <= 0:
        return sorted_values[0]
    if q >= 1:
        return sorted_values[-1]
    pos = q * (len(sorted_values) - 1)
    lower = int(pos)
    upper = min(len(sorted_values) - 1, lower + 1)
    if lower == upper:
        return sorted_values[lower]
    frac = pos - lower
    return (sorted_values[lower] * (1.0 - frac)) + (sorted_values[upper] * frac)


def _iqr(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    q1 = _quantile(ordered, 0.25)
    q3 = _quantile(ordered, 0.75)
    if q1 is None or q3 is None:
        return None
    return q3 - q1


def _parse_side(value: Any) -> str | None:
    side_raw = str(value).strip().lower()
    if side_raw in {"over", "o"}:
        return "over"
    if side_raw in {"under", "u"}:
        return "under"
    return None


def extract_book_fair_pairs(
    group_rows: list[dict[str, Any]],
    *,
    exclude_book_keys: frozenset[str] | None = None,
) -> list[BookFairPair]:
    """Extract deterministic per-book paired prices with no-vig probabilities."""
    excluded = exclude_book_keys or frozenset()
    book_sides: dict[str, dict[str, list[int]]] = {}
    for row in group_rows:
        if not isinstance(row, dict):
            continue
        book = str(row.get("book", "")).strip()
        if not book:
            continue
        if book in excluded:
            continue
        side = _parse_side(row.get("side", ""))
        if side is None:
            continue
        price = _to_price(row.get("price"))
        if price is None:
            continue
        entry = book_sides.setdefault(book, {"over": [], "under": []})
        entry[side].append(price)

    pairs: list[BookFairPair] = []
    for book in sorted(book_sides):
        sides = book_sides[book]
        if not sides["over"] or not sides["under"]:
            continue
        over_price = max(sides["over"])
        under_price = max(sides["under"])
        p_over_implied = implied_prob_from_american(over_price)
        p_under_implied = implied_prob_from_american(under_price)
        if p_over_implied is None or p_under_implied is None:
            continue
        p_over_fair, p_under_fair = normalize_prob_pair(p_over_implied, p_under_implied)
        pairs.append(
            BookFairPair(
                book=book,
                over_price=over_price,
                under_price=under_price,
                p_over_implied=p_over_implied,
                p_under_implied=p_under_implied,
                p_over_fair=p_over_fair,
                p_under_fair=p_under_fair,
                hold=(p_over_implied + p_under_implied) - 1.0,
            )
        )
    return pairs


def summarize_line_pricing(
    *,
    group_rows: list[dict[str, Any]],
    now_utc: datetime,
    stale_quote_minutes: int,
    hold_fallback: float | None,
    exclude_book_keys: frozenset[str] | None = None,
) -> LinePricingQuality:
    """Compute deterministic pricing quality and uncertainty fields for one line."""
    book_pairs = tuple(extract_book_fair_pairs(group_rows, exclude_book_keys=exclude_book_keys))
    p_over_values = [pair.p_over_fair for pair in book_pairs]
    hold_values = [pair.hold for pair in book_pairs]
    p_over_median = _median(p_over_values)
    hold_median = _median(hold_values)
    p_over_iqr = _iqr(p_over_values)
    p_over_range = (max(p_over_values) - min(p_over_values)) if p_over_values else None

    freshest_quote = max(
        (
            parsed
            for row in group_rows
            if isinstance(row, dict)
            for parsed in [parse_iso_z(str(row.get("last_update", "")))]
            if parsed is not None
        ),
        default=None,
    )
    freshest_quote_utc = ""
    quote_age_minutes: float | None = None
    if freshest_quote is not None:
        freshest_quote_utc = freshest_quote.isoformat().replace("+00:00", "Z")
        age = (now_utc - freshest_quote).total_seconds() / 60.0
        quote_age_minutes = round(max(0.0, age), 6)

    book_pair_count = len(book_pairs)
    depth_score = _clamp(book_pair_count / 4.0, 0.0, 1.0)
    hold_for_quality = hold_median if hold_median is not None else hold_fallback
    hold_score = (
        _clamp(1.0 - (_clamp(hold_for_quality, 0.0, 1.0) / 0.12), 0.0, 1.0)
        if hold_for_quality is not None
        else 0.0
    )
    dispersion_source = p_over_iqr if p_over_iqr is not None else p_over_range
    dispersion_score = (
        _clamp(1.0 - (_clamp(dispersion_source, 0.0, 1.0) / 0.15), 0.0, 1.0)
        if dispersion_source is not None
        else 0.0
    )
    freshness_horizon_minutes = max(5.0, float(max(stale_quote_minutes, 1) * 2))
    freshness_score = (
        _clamp(1.0 - (quote_age_minutes / freshness_horizon_minutes), 0.0, 1.0)
        if quote_age_minutes is not None
        else 0.0
    )
    quality_score = round(
        (depth_score * 0.30)
        + (hold_score * 0.25)
        + (dispersion_score * 0.25)
        + (freshness_score * 0.20),
        6,
    )
    uncertainty_band = (
        0.01
        + ((1.0 - depth_score) * 0.05)
        + ((1.0 - hold_score) * 0.02)
        + ((1.0 - dispersion_score) * 0.05)
        + ((1.0 - freshness_score) * 0.03)
    )
    if p_over_iqr is not None:
        uncertainty_band = max(uncertainty_band, p_over_iqr / 2.0)
    uncertainty_band = round(_clamp(uncertainty_band, 0.01, 0.2), 6)

    return LinePricingQuality(
        book_pairs=book_pairs,
        books_used=tuple(pair.book for pair in book_pairs),
        book_pair_count=book_pair_count,
        p_over_median=p_over_median,
        hold_median=hold_median,
        p_over_iqr=p_over_iqr,
        p_over_range=p_over_range,
        freshest_quote_utc=freshest_quote_utc,
        quote_age_minutes=quote_age_minutes,
        depth_score=depth_score,
        hold_score=hold_score,
        dispersion_score=dispersion_score,
        freshness_score=freshness_score,
        quality_score=quality_score,
        uncertainty_band=uncertainty_band,
    )


def line_source_for_baseline(baseline_used: str) -> str:
    """Map baseline policy outcomes to stable line-source provenance labels."""
    mapping = {
        "median_book": "exact_point_pairs",
        "median_book_interpolated": "reference_curve",
        "best_sides": "best_sides",
        "best_sides_fallback": "best_sides",
        "missing": "missing",
    }
    return mapping.get(baseline_used, "unknown")


def resolve_baseline_selection(
    *,
    baseline_method: str,
    baseline_fallback: str,
    p_over_fair_best: float | None,
    p_under_fair_best: float | None,
    hold_best: float | None,
    p_over_book_median: float | None,
    hold_book_median: float | None,
    reference_estimate: ReferenceEstimate,
) -> BaselineSelection:
    """Resolve fair-probability baseline and provenance for one candidate line."""
    p_over_fair = p_over_fair_best
    p_under_fair = p_under_fair_best
    hold = hold_best
    baseline_used = "best_sides"
    reference_line_method = reference_estimate.method

    if baseline_method == "median_book":
        if p_over_book_median is not None and hold_book_median is not None:
            p_over_fair = p_over_book_median
            p_under_fair = 1.0 - p_over_fair
            hold = hold_book_median
            baseline_used = "median_book"
            reference_line_method = "exact"
        elif reference_estimate.p_over is not None:
            p_over_fair = reference_estimate.p_over
            p_under_fair = 1.0 - p_over_fair
            hold = reference_estimate.hold
            baseline_used = "median_book_interpolated"
        elif baseline_fallback == "best_sides":
            baseline_used = "best_sides_fallback"
        else:
            p_over_fair = None
            p_under_fair = None
            hold = None
            baseline_used = "missing"

    return BaselineSelection(
        p_over_fair=p_over_fair,
        p_under_fair=p_under_fair,
        hold=hold,
        baseline_used=baseline_used,
        reference_line_method=reference_line_method,
        line_source=line_source_for_baseline(baseline_used),
    )
