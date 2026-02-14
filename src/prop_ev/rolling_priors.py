"""Rolling settled-outcome priors for conservative strategy ranking adjustments."""

from __future__ import annotations

import csv
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

_DAY_SUFFIX_RE = re.compile(r"(?P<day>\d{4}-\d{2}-\d{2})$")


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
    start_day = as_of - timedelta(days=effective_window_days)
    by_snapshot = reports_root / "by-snapshot"
    if not by_snapshot.exists():
        return {
            "as_of_day": as_of.isoformat(),
            "window_days": effective_window_days,
            "rows_used": 0,
            "adjustments": {},
        }

    key_counts: dict[str, dict[str, int]] = {}
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

    return {
        "as_of_day": as_of.isoformat(),
        "window_days": effective_window_days,
        "min_samples": effective_min_samples,
        "max_abs_delta": effective_max_abs_delta,
        "rows_used": rows_used,
        "adjustments": adjustments,
    }
