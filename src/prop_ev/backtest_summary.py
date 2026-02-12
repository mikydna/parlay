from __future__ import annotations

import csv
import json
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
            return None
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
    wins: int
    losses: int
    pushes: int
    total_stake_units: float
    total_pnl_units: float
    roi: float | None
    avg_best_ev: float | None
    brier: float | None
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

    brier_terms: list[float] = []
    cal_points: list[tuple[float, int]] = []

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

        if result in {"win", "loss"}:
            p = _safe_float(row.get("model_p_hit"))
            if p is not None and 0.0 <= p <= 1.0:
                y = 1 if result == "win" else 0
                brier_terms.append(_brier(p, y))
                cal_points.append((p, y))

    graded = wins + losses + pushes
    roi = (total_pnl / total_stake) if total_stake > 0 else None
    avg_best_ev = _mean(best_evs)
    brier_score = _mean(brier_terms)

    calibration: list[CalibrationBucket] = []
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

    return BacktestSummary(
        strategy_id=strategy_id,
        rows_total=len(rows),
        rows_graded=graded,
        wins=wins,
        losses=losses,
        pushes=pushes,
        total_stake_units=round(total_stake, 6),
        total_pnl_units=round(total_pnl, 6),
        roi=None if roi is None else round(roi, 6),
        avg_best_ev=None if avg_best_ev is None else round(avg_best_ev, 6),
        brier=None if brier_score is None else round(brier_score, 6),
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
