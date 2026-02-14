"""Deterministic reference-probability helpers for pricing stages."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median


@dataclass(frozen=True)
class ReferencePoint:
    point: float
    p_over: float
    hold: float | None = None
    weight: float = 1.0


@dataclass(frozen=True)
class ReferenceEstimate:
    p_over: float | None
    hold: float | None
    method: str
    points_used: int


def _clamp_probability(value: float) -> float:
    return max(0.01, min(0.99, float(value)))


def _pav_nonincreasing(values: list[float], weights: list[float]) -> list[float]:
    if not values:
        return []

    blocks: list[dict[str, float | int]] = [
        {
            "start": int(index),
            "end": int(index),
            "weight": max(float(weights[index]), 1e-9),
            "value": float(values[index]),
        }
        for index in range(len(values))
    ]

    index = 0
    while index < len(blocks) - 1:
        left = blocks[index]
        right = blocks[index + 1]
        if float(left["value"]) < float(right["value"]):
            merged_weight = float(left["weight"]) + float(right["weight"])
            merged_value = (
                (float(left["value"]) * float(left["weight"]))
                + (float(right["value"]) * float(right["weight"]))
            ) / merged_weight
            blocks[index] = {
                "start": int(left["start"]),
                "end": int(right["end"]),
                "weight": merged_weight,
                "value": merged_value,
            }
            del blocks[index + 1]
            if index > 0:
                index -= 1
            continue
        index += 1

    adjusted = [0.5] * len(values)
    for block in blocks:
        start = int(block["start"])
        end = int(block["end"])
        value = _clamp_probability(float(block["value"]))
        for position in range(start, end + 1):
            adjusted[position] = value
    return adjusted


def build_reference_curve(points: list[ReferencePoint]) -> list[ReferencePoint]:
    if not points:
        return []

    grouped: dict[float, dict[str, float]] = {}
    for row in points:
        point = float(row.point)
        weight = max(float(row.weight), 1e-9)
        entry = grouped.setdefault(
            point,
            {
                "p_weighted_sum": 0.0,
                "hold_weighted_sum": 0.0,
                "hold_weight": 0.0,
                "weight": 0.0,
            },
        )
        entry["p_weighted_sum"] += _clamp_probability(row.p_over) * weight
        entry["weight"] += weight
        if row.hold is not None:
            entry["hold_weighted_sum"] += float(row.hold) * weight
            entry["hold_weight"] += weight

    sorted_points = sorted(grouped)
    raw_probs = [
        grouped[point]["p_weighted_sum"] / grouped[point]["weight"] for point in sorted_points
    ]
    raw_weights = [grouped[point]["weight"] for point in sorted_points]
    adjusted_probs = _pav_nonincreasing(raw_probs, raw_weights)

    curve: list[ReferencePoint] = []
    for index, point in enumerate(sorted_points):
        entry = grouped[point]
        hold: float | None = None
        if entry["hold_weight"] > 0:
            hold = entry["hold_weighted_sum"] / entry["hold_weight"]
        curve.append(
            ReferencePoint(
                point=point,
                p_over=adjusted_probs[index],
                hold=hold,
                weight=entry["weight"],
            )
        )
    return curve


def _interpolate(x0: float, x1: float, y0: float, y1: float, target: float) -> float:
    if x1 == x0:
        return y0
    ratio = (target - x0) / (x1 - x0)
    return y0 + ((y1 - y0) * ratio)


def estimate_reference_probability(
    points: list[ReferencePoint],
    *,
    target_point: float,
) -> ReferenceEstimate:
    curve = build_reference_curve(points)
    if not curve:
        return ReferenceEstimate(p_over=None, hold=None, method="missing", points_used=0)

    holds = [row.hold for row in curve if row.hold is not None]
    hold_median = median(holds) if holds else None
    target = float(target_point)

    for row in curve:
        if row.point == target:
            return ReferenceEstimate(
                p_over=_clamp_probability(row.p_over),
                hold=row.hold if row.hold is not None else hold_median,
                method="exact",
                points_used=len(curve),
            )

    if target <= curve[0].point:
        return ReferenceEstimate(
            p_over=_clamp_probability(curve[0].p_over),
            hold=curve[0].hold if curve[0].hold is not None else hold_median,
            method="clamped_low",
            points_used=len(curve),
        )

    if target >= curve[-1].point:
        return ReferenceEstimate(
            p_over=_clamp_probability(curve[-1].p_over),
            hold=curve[-1].hold if curve[-1].hold is not None else hold_median,
            method="clamped_high",
            points_used=len(curve),
        )

    for left, right in zip(curve, curve[1:], strict=False):
        if left.point <= target <= right.point:
            interpolated = _interpolate(left.point, right.point, left.p_over, right.p_over, target)
            if left.hold is not None and right.hold is not None:
                interpolated_hold = _interpolate(
                    left.point,
                    right.point,
                    left.hold,
                    right.hold,
                    target,
                )
            else:
                interpolated_hold = hold_median
            return ReferenceEstimate(
                p_over=_clamp_probability(interpolated),
                hold=interpolated_hold,
                method="interpolated",
                points_used=len(curve),
            )

    return ReferenceEstimate(
        p_over=_clamp_probability(curve[-1].p_over),
        hold=curve[-1].hold if curve[-1].hold is not None else hold_median,
        method="fallback_last",
        points_used=len(curve),
    )
