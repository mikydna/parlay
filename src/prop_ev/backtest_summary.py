from __future__ import annotations

import csv
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _safe_int(value: Any) -> int | None:
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
            try:
                parsed = float(raw)
            except ValueError:
                return None
            rounded = round(parsed)
            if abs(parsed - rounded) > 1e-6:
                return None
            return int(rounded)
    return None


def _normalize_result(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"w", "win"}:
        return "win"
    if raw in {"l", "loss"}:
        return "loss"
    if raw in {"p", "push"}:
        return "push"
    return ""


def _pnl_units(*, result: str, american_price: int, stake_units: float) -> float:
    if stake_units <= 0:
        return 0.0
    if result == "push":
        return 0.0
    if result == "loss":
        return -stake_units
    if result != "win":
        return 0.0
    if american_price > 0:
        return stake_units * (american_price / 100.0)
    if american_price < 0:
        return stake_units * (100.0 / abs(american_price))
    return 0.0


def _mean(values: Iterable[float]) -> float | None:
    items = list(values)
    if not items:
        return None
    return sum(items) / len(items)


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


def _brier(p: float, y: int) -> float:
    return (p - float(y)) ** 2


def _clamp_probability(probability: float, *, eps: float = 1e-6) -> float:
    return max(eps, min(1.0 - eps, probability))


def _log_loss(probability: float, outcome: int) -> float:
    clamped = _clamp_probability(probability)
    if outcome == 1:
        return -math.log(clamped)
    return -math.log(1.0 - clamped)


@dataclass(frozen=True)
class CalibrationBucket:
    bucket_low: float
    bucket_high: float
    count: int
    avg_p: float | None
    hit_rate: float | None
    brier: float | None


@dataclass(frozen=True)
class BacktestSummary:
    strategy_id: str
    rows_total: int
    rows_graded: int
    rows_scored: int
    wins: int
    losses: int
    pushes: int
    total_stake_units: float
    total_pnl_units: float
    roi: float | None
    avg_best_ev: float | None
    avg_ev_low: float | None
    avg_quality_score: float | None
    avg_p_hit_low: float | None
    brier: float | None
    brier_low: float | None
    log_loss: float | None
    ece: float | None
    mce: float | None
    actionability_rate: float | None
    calibration: list[CalibrationBucket]

    def to_dict(self) -> dict[str, Any]:
        return json.loads(json.dumps(self, default=lambda o: o.__dict__, sort_keys=True))


def load_backtest_csv(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not isinstance(row, dict):
                continue
            rows.append({str(k): (str(v) if v is not None else "") for k, v in row.items()})
    return rows


def summarize_backtest_rows(
    rows: list[dict[str, str]],
    *,
    strategy_id: str,
    bin_size: float = 0.05,
) -> BacktestSummary:
    if bin_size <= 0 or bin_size > 0.5:
        raise ValueError("bin_size must be in (0, 0.5]")

    wins = 0
    losses = 0
    pushes = 0
    total_stake = 0.0
    total_pnl = 0.0
    best_evs: list[float] = []
    ev_lows: list[float] = []
    quality_scores: list[float] = []

    brier_terms: list[float] = []
    brier_low_terms: list[float] = []
    log_loss_terms: list[float] = []
    cal_points: list[tuple[float, int]] = []
    p_hit_low_points: list[float] = []
    actionability_samples: list[float] = []

    for row in rows:
        result = _normalize_result(row.get("result", ""))
        if not result:
            continue

        stake = _safe_float(row.get("stake_units"))
        stake_units = 1.0 if stake is None else max(0.0, stake)
        price = _safe_int(row.get("graded_price_american"))
        if price is None:
            price = _safe_int(row.get("selected_price_american"))
        if price is None:
            continue

        total_stake += stake_units
        pnl = _safe_float(row.get("pnl_units"))
        if pnl is None:
            pnl = _pnl_units(result=result, american_price=price, stake_units=stake_units)
        total_pnl += pnl

        if result == "win":
            wins += 1
        elif result == "loss":
            losses += 1
        elif result == "push":
            pushes += 1

        best_ev = _safe_float(row.get("best_ev"))
        if best_ev is not None:
            best_evs.append(best_ev)
        ev_low = _safe_float(row.get("ev_low"))
        if ev_low is not None:
            ev_lows.append(ev_low)
        quality_score = _safe_float(row.get("quality_score"))
        if quality_score is not None:
            quality_scores.append(quality_score)
        eligible_lines = _safe_float(row.get("summary_eligible_lines"))
        candidate_lines = _safe_float(row.get("summary_candidate_lines"))
        if (
            eligible_lines is not None
            and candidate_lines is not None
            and candidate_lines > 0
            and eligible_lines >= 0
        ):
            ratio = eligible_lines / candidate_lines
            actionability_samples.append(max(0.0, min(1.0, ratio)))

        if result in {"win", "loss"}:
            p = _safe_float(row.get("model_p_hit"))
            if p is not None and 0.0 <= p <= 1.0:
                y = 1 if result == "win" else 0
                brier_terms.append(_brier(p, y))
                log_loss_terms.append(_log_loss(p, y))
                cal_points.append((p, y))
            p_low = _safe_float(row.get("p_hit_low"))
            if p_low is not None and 0.0 <= p_low <= 1.0:
                y = 1 if result == "win" else 0
                brier_low_terms.append(_brier(p_low, y))
                p_hit_low_points.append(p_low)

    graded = wins + losses + pushes
    roi = (total_pnl / total_stake) if total_stake > 0 else None
    avg_best_ev = _mean(best_evs)
    avg_ev_low = _mean(ev_lows)
    avg_quality_score = _mean(quality_scores)
    avg_p_hit_low = _mean(p_hit_low_points)
    brier_score = _mean(brier_terms)
    brier_low_score = _mean(brier_low_terms)
    actionability_rate = _mean(actionability_samples)
    log_loss_score = _mean(log_loss_terms)

    calibration: list[CalibrationBucket] = []
    weighted_calibration_error = 0.0
    weighted_calibration_count = 0
    max_calibration_error: float | None = None
    if cal_points:
        buckets: dict[int, list[tuple[float, int]]] = {}
        for p, y in cal_points:
            idx = int(p / bin_size)
            if idx < 0:
                idx = 0
            max_idx = int(1.0 / bin_size)
            if idx > max_idx:
                idx = max_idx
            buckets.setdefault(idx, []).append((p, y))
        for idx in sorted(buckets.keys()):
            items = buckets[idx]
            ps = [p for p, _ in items]
            ys = [y for _, y in items]
            low = idx * bin_size
            high = min(1.0, low + bin_size)
            avg_p = _mean(ps)
            hit_rate = _mean(ys)
            brier_bucket = _mean([_brier(p, y) for p, y in items])
            if avg_p is not None and hit_rate is not None:
                calibration_error = abs(avg_p - hit_rate)
                weighted_calibration_error += calibration_error * len(items)
                weighted_calibration_count += len(items)
                if max_calibration_error is None:
                    max_calibration_error = calibration_error
                else:
                    max_calibration_error = max(max_calibration_error, calibration_error)
            calibration.append(
                CalibrationBucket(
                    bucket_low=round(low, 6),
                    bucket_high=round(high, 6),
                    count=len(items),
                    avg_p=None if avg_p is None else round(avg_p, 6),
                    hit_rate=None if hit_rate is None else round(hit_rate, 6),
                    brier=None if brier_bucket is None else round(brier_bucket, 6),
                )
            )

    ece_score: float | None = None
    if weighted_calibration_count > 0:
        ece_score = weighted_calibration_error / weighted_calibration_count

    return BacktestSummary(
        strategy_id=strategy_id,
        rows_total=len(rows),
        rows_graded=graded,
        rows_scored=len(cal_points),
        wins=wins,
        losses=losses,
        pushes=pushes,
        total_stake_units=round(total_stake, 6),
        total_pnl_units=round(total_pnl, 6),
        roi=None if roi is None else round(roi, 6),
        avg_best_ev=None if avg_best_ev is None else round(avg_best_ev, 6),
        avg_ev_low=None if avg_ev_low is None else round(avg_ev_low, 6),
        avg_quality_score=(None if avg_quality_score is None else round(avg_quality_score, 6)),
        avg_p_hit_low=None if avg_p_hit_low is None else round(avg_p_hit_low, 6),
        brier=None if brier_score is None else round(brier_score, 6),
        brier_low=None if brier_low_score is None else round(brier_low_score, 6),
        log_loss=None if log_loss_score is None else round(log_loss_score, 6),
        ece=None if ece_score is None else round(ece_score, 6),
        mce=(None if max_calibration_error is None else round(max_calibration_error, 6)),
        actionability_rate=(None if actionability_rate is None else round(actionability_rate, 6)),
        calibration=calibration,
    )


def pick_winner(summaries: list[BacktestSummary], *, min_graded: int) -> BacktestSummary | None:
    eligible = [item for item in summaries if item.rows_graded >= max(0, int(min_graded))]
    if not eligible:
        return None
    eligible.sort(
        key=lambda item: (
            -(item.roi if item.roi is not None else -9999.0),
            -item.rows_graded,
            item.strategy_id,
        )
    )
    return eligible[0]
