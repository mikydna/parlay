"""Rolling settled-outcome priors for conservative strategy ranking adjustments."""

from __future__ import annotations

import csv
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from prop_ev.util.parsing import safe_float as _safe_float

_DAY_SUFFIX_RE = re.compile(r"(?P<day>\d{4}-\d{2}-\d{2})$")


def _clamp_probability(value: float, *, eps: float = 0.01) -> float:
    return max(eps, min(1.0 - eps, value))


def _effective_bin_size(raw: float) -> float:
    candidate = float(raw)
    if candidate <= 0:
        return 0.1
    return max(0.02, min(0.5, candidate))


def _bin_index(probability: float, *, bin_size: float) -> int:
    size = _effective_bin_size(bin_size)
    raw_index = int(probability / size)
    max_index = int(1.0 / size)
    if raw_index < 0:
        return 0
    if raw_index > max_index:
        return max_index
    return raw_index


def _bin_bounds(index: int, *, bin_size: float) -> tuple[float, float]:
    size = _effective_bin_size(bin_size)
    low = index * size
    high = min(1.0, low + size)
    return low, high


def _safe_day(value: str) -> date | None:
    raw = value.strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _day_from_snapshot_id(snapshot_id: str) -> date | None:
    raw = snapshot_id.strip()
    if len(raw) >= 10:
        prefix = _safe_day(raw[:10])
        if prefix is not None:
            return prefix
    match = re.match(r"^daily-(\d{4})-?(\d{2})-?(\d{2})", raw)
    if match:
        return _safe_day(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")
    match_day = _DAY_SUFFIX_RE.search(raw)
    if match_day:
        return _safe_day(match_day.group("day"))
    return None


def _day_from_modeled_date_et(modeled_date_et: str) -> date | None:
    raw = modeled_date_et.strip()
    if not raw:
        return None
    for fmt in ("%A, %b %d, %Y (ET)", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        return parsed.date()
    return None


def _row_day(row: dict[str, str]) -> date | None:
    modeled = _day_from_modeled_date_et(str(row.get("modeled_date_et", "")))
    if modeled is not None:
        return modeled
    snapshot_id = str(row.get("snapshot_id", ""))
    return _day_from_snapshot_id(snapshot_id)


def _normalize_result(value: str) -> str:
    raw = value.strip().lower()
    if raw in {"w", "win"}:
        return "win"
    if raw in {"l", "loss"}:
        return "loss"
    return ""


def _adjustment_key(*, market: str, side: str) -> str:
    return f"{market.strip().lower()}::{side.strip().lower()}"


def _append_bin_count(
    container: dict[int, dict[str, float]],
    *,
    bin_index: int,
    model_probability: float,
    result: str,
) -> None:
    bucket = container.setdefault(bin_index, {"count": 0.0, "wins": 0.0, "p_sum": 0.0})
    bucket["count"] += 1.0
    if result == "win":
        bucket["wins"] += 1.0
    bucket["p_sum"] += model_probability


def _serialize_calibration_bins(
    container: dict[int, dict[str, float]],
    *,
    bin_size: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index in sorted(container.keys()):
        bucket = container[index]
        count = int(bucket.get("count", 0.0))
        wins = int(bucket.get("wins", 0.0))
        p_sum = float(bucket.get("p_sum", 0.0))
        avg_p = (p_sum / count) if count > 0 else None
        hit_rate = (wins / count) if count > 0 else None
        delta = (hit_rate - avg_p) if (avg_p is not None and hit_rate is not None) else None
        low, high = _bin_bounds(index, bin_size=bin_size)
        rows.append(
            {
                "bucket_index": int(index),
                "bucket_low": round(low, 6),
                "bucket_high": round(high, 6),
                "count": count,
                "avg_model_p": None if avg_p is None else round(avg_p, 6),
                "hit_rate": None if hit_rate is None else round(hit_rate, 6),
                "delta": None if delta is None else round(delta, 6),
            }
        )
    return rows


def _safe_calibration(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _find_calibration_bucket(
    *, bins: list[dict[str, Any]], target_index: int, min_bin_samples: int
) -> dict[str, Any] | None:
    effective_min_samples = max(1, int(min_bin_samples))
    for row in bins:
        if not isinstance(row, dict):
            continue
        index = int(row.get("bucket_index", -1))
        count = int(row.get("count", 0))
        if index != target_index or count < effective_min_samples:
            continue
        return row
    return None


def calibration_feedback(
    *,
    rolling_priors: dict[str, Any] | None,
    market: str,
    side: str,
    model_probability: float | None,
) -> dict[str, Any]:
    base_prob = _safe_float(model_probability)
    if base_prob is None or not (0.0 <= base_prob <= 1.0):
        return {
            "p_calibrated": None,
            "delta": None,
            "source": "unavailable",
            "sample_size": 0,
            "confidence": 0.0,
            "bucket_index": None,
            "bucket_low": None,
            "bucket_high": None,
        }

    clamped = _clamp_probability(base_prob)
    priors = _safe_calibration(rolling_priors)
    calibration = _safe_calibration(priors.get("calibration"))
    bin_size = _effective_bin_size(_safe_float(calibration.get("bin_size")) or 0.1)
    min_bin_samples = max(1, int(calibration.get("min_bin_samples", 10) or 10))
    calibration_max_abs_delta = max(
        0.0,
        float(
            _safe_float(calibration.get("max_abs_delta"))
            or _safe_float(priors.get("max_abs_delta"))
            or 0.02
        ),
    )
    calibration_shrink_k = max(1, int(_safe_float(calibration.get("shrink_k")) or 100.0))
    calibration_bucket_weight = max(
        0.0,
        min(1.0, float(_safe_float(calibration.get("bucket_weight")) or 0.3)),
    )
    target_index = _bin_index(clamped, bin_size=bin_size)

    global_bins = calibration.get("global_bins", [])
    by_market_side = _safe_calibration(calibration.get("by_market_side"))
    key = _adjustment_key(market=market, side=side)
    market_bins = by_market_side.get(key, [])
    adjustment_payload = _safe_calibration(_safe_calibration(priors.get("adjustments")).get(key))
    adjustment_delta = _safe_float(adjustment_payload.get("delta")) or 0.0
    adjustment_sample_size = max(0, int(adjustment_payload.get("sample_size", 0) or 0))

    selected = None
    source = "none"
    if isinstance(market_bins, list):
        selected = _find_calibration_bucket(
            bins=market_bins,
            target_index=target_index,
            min_bin_samples=min_bin_samples,
        )
        if selected is not None:
            source = "market_side"
    if selected is None and isinstance(global_bins, list):
        selected = _find_calibration_bucket(
            bins=global_bins,
            target_index=target_index,
            min_bin_samples=min_bin_samples,
        )
        if selected is not None:
            source = "global"

    if selected is None and adjustment_sample_size <= 0:
        low, high = _bin_bounds(target_index, bin_size=bin_size)
        return {
            "p_calibrated": round(clamped, 6),
            "delta": 0.0,
            "source": source,
            "sample_size": 0,
            "confidence": 0.0,
            "bucket_index": int(target_index),
            "bucket_low": round(low, 6),
            "bucket_high": round(high, 6),
        }

    count = (
        max(0, int(selected.get("count", 0)))
        if isinstance(selected, dict)
        else adjustment_sample_size
    )
    bucket_delta = _safe_float(selected.get("delta")) if isinstance(selected, dict) else None
    source_weight = 1.0 if source == "market_side" else (0.85 if source == "global" else 0.7)
    coverage = min(1.0, count / float(min_bin_samples))
    shrink = count / float(count + calibration_shrink_k)
    if bucket_delta is None:
        raw_delta = adjustment_delta
        if source == "none":
            source = "adjustment"
    else:
        raw_delta = ((1.0 - calibration_bucket_weight) * adjustment_delta) + (
            calibration_bucket_weight * bucket_delta
        )
    delta = raw_delta * shrink * source_weight
    if delta > calibration_max_abs_delta:
        delta = calibration_max_abs_delta
    elif delta < -calibration_max_abs_delta:
        delta = -calibration_max_abs_delta
    calibrated = _clamp_probability(clamped + delta)
    return {
        "p_calibrated": round(calibrated, 6),
        "delta": round(delta, 6),
        "source": source,
        "sample_size": count,
        "confidence": round(coverage * source_weight, 6),
        "bucket_index": (
            int(selected.get("bucket_index", target_index))
            if isinstance(selected, dict)
            else int(target_index)
        ),
        "bucket_low": _safe_float(selected.get("bucket_low"))
        if isinstance(selected, dict)
        else None,
        "bucket_high": (
            _safe_float(selected.get("bucket_high")) if isinstance(selected, dict) else None
        ),
    }


def _iter_backtest_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not isinstance(row, dict):
                continue
            rows.append({str(key): str(value) for key, value in row.items()})
    return rows


def build_rolling_priors(
    *,
    reports_root: Path,
    strategy_id: str,
    as_of_day: str,
    window_days: int = 21,
    min_samples: int = 25,
    max_abs_delta: float = 0.02,
    calibration_bin_size: float = 0.1,
    calibration_min_bin_samples: int = 10,
    calibration_max_abs_delta: float = 0.02,
    calibration_shrink_k: int = 100,
    calibration_bucket_weight: float = 0.3,
) -> dict[str, Any]:
    """Build market+side prior deltas using only settled history before `as_of_day`."""
    as_of = _safe_day(as_of_day)
    if as_of is None:
        return {
            "as_of_day": as_of_day,
            "window_days": window_days,
            "rows_used": 0,
            "adjustments": {},
        }

    effective_window_days = max(1, int(window_days))
    effective_min_samples = max(1, int(min_samples))
    effective_max_abs_delta = max(0.0, float(max_abs_delta))
    effective_calibration_bin_size = _effective_bin_size(float(calibration_bin_size))
    effective_calibration_min_bin_samples = max(1, int(calibration_min_bin_samples))
    effective_calibration_max_abs_delta = max(
        0.0,
        float(calibration_max_abs_delta),
    )
    effective_calibration_shrink_k = max(1, int(calibration_shrink_k))
    effective_calibration_bucket_weight = max(0.0, min(1.0, float(calibration_bucket_weight)))
    start_day = as_of - timedelta(days=effective_window_days)
    by_snapshot = reports_root / "by-snapshot"
    if not by_snapshot.exists():
        return {
            "as_of_day": as_of.isoformat(),
            "window_days": effective_window_days,
            "rows_used": 0,
            "adjustments": {},
            "calibration": {
                "bin_size": effective_calibration_bin_size,
                "min_bin_samples": effective_calibration_min_bin_samples,
                "max_abs_delta": effective_calibration_max_abs_delta,
                "shrink_k": effective_calibration_shrink_k,
                "bucket_weight": effective_calibration_bucket_weight,
                "rows_scored": 0,
                "global_bins": [],
                "by_market_side": {},
            },
        }

    key_counts: dict[str, dict[str, int]] = {}
    calibration_global_counts: dict[int, dict[str, float]] = {}
    calibration_key_counts: dict[str, dict[int, dict[str, float]]] = {}
    scored_rows = 0
    rows_used = 0
    for snapshot_dir in sorted(path for path in by_snapshot.iterdir() if path.is_dir()):
        csv_path = snapshot_dir / f"settlement.{strategy_id}.csv"
        if strategy_id == "s001" and not csv_path.exists():
            csv_path = snapshot_dir / "settlement.csv"
        if not csv_path.exists():
            csv_path = snapshot_dir / f"backtest-results-template.{strategy_id}.csv"
            if strategy_id == "s001" and not csv_path.exists():
                csv_path = snapshot_dir / "backtest-results-template.csv"
        if not csv_path.exists():
            continue

        for row in _iter_backtest_rows(csv_path):
            row_day = _row_day(row)
            if row_day is None:
                continue
            if row_day < start_day or row_day >= as_of:
                continue
            result = _normalize_result(str(row.get("result", "")))
            if result not in {"win", "loss"}:
                continue
            market = str(row.get("market", "")).strip().lower()
            side = str(row.get("recommended_side", "")).strip().lower()
            if not market or side not in {"over", "under"}:
                continue
            key = _adjustment_key(market=market, side=side)
            bucket = key_counts.setdefault(key, {"wins": 0, "losses": 0})
            if result == "win":
                bucket["wins"] += 1
            else:
                bucket["losses"] += 1
            model_probability = _safe_float(row.get("model_p_hit"))
            if model_probability is not None and 0.0 <= model_probability <= 1.0:
                clamped_probability = _clamp_probability(model_probability)
                bucket_index = _bin_index(
                    clamped_probability,
                    bin_size=effective_calibration_bin_size,
                )
                _append_bin_count(
                    calibration_global_counts,
                    bin_index=bucket_index,
                    model_probability=clamped_probability,
                    result=result,
                )
                keyed_counts = calibration_key_counts.setdefault(key, {})
                _append_bin_count(
                    keyed_counts,
                    bin_index=bucket_index,
                    model_probability=clamped_probability,
                    result=result,
                )
                scored_rows += 1
            rows_used += 1

    adjustments: dict[str, dict[str, Any]] = {}
    for key, bucket in sorted(key_counts.items()):
        wins = int(bucket.get("wins", 0))
        losses = int(bucket.get("losses", 0))
        sample_size = wins + losses
        if sample_size <= 0:
            continue
        hit_rate = wins / float(sample_size)
        posterior_hit_rate = (wins + 1.0) / float(sample_size + 2)
        coverage = min(1.0, sample_size / float(effective_min_samples))
        raw_delta = (posterior_hit_rate - 0.5) * coverage
        delta = max(-effective_max_abs_delta, min(effective_max_abs_delta, raw_delta))
        adjustments[key] = {
            "sample_size": sample_size,
            "wins": wins,
            "losses": losses,
            "hit_rate": round(hit_rate, 6),
            "posterior_hit_rate": round(posterior_hit_rate, 6),
            "delta": round(delta, 6),
        }

    calibration_by_market_side: dict[str, list[dict[str, Any]]] = {}
    for key in sorted(calibration_key_counts.keys()):
        calibration_by_market_side[key] = _serialize_calibration_bins(
            calibration_key_counts[key],
            bin_size=effective_calibration_bin_size,
        )

    return {
        "as_of_day": as_of.isoformat(),
        "window_days": effective_window_days,
        "min_samples": effective_min_samples,
        "max_abs_delta": effective_max_abs_delta,
        "rows_used": rows_used,
        "adjustments": adjustments,
        "calibration": {
            "bin_size": effective_calibration_bin_size,
            "min_bin_samples": effective_calibration_min_bin_samples,
            "max_abs_delta": effective_calibration_max_abs_delta,
            "shrink_k": effective_calibration_shrink_k,
            "bucket_weight": effective_calibration_bucket_weight,
            "rows_scored": scored_rows,
            "global_bins": _serialize_calibration_bins(
                calibration_global_counts,
                bin_size=effective_calibration_bin_size,
            ),
            "by_market_side": calibration_by_market_side,
        },
    }
