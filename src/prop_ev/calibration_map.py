from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

CALIBRATION_MAP_SCHEMA_VERSION = 1

_MODELED_DATE_FORMATS = ("%A, %b %d, %Y (ET)", "%Y-%m-%d")

CalibrationMode = Literal["in_sample", "walk_forward"]


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


def _normalize_result(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"w", "win"}:
        return "win"
    if raw in {"l", "loss"}:
        return "loss"
    return ""


def _day_from_snapshot_id(snapshot_id: str) -> date | None:
    raw = snapshot_id.strip()
    if len(raw) >= 10:
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            pass
    if raw.startswith("day-") and len(raw) >= 10:
        suffix = raw[-10:]
        try:
            return date.fromisoformat(suffix)
        except ValueError:
            return None
    return None


def resolve_modeled_day(*, modeled_date_et: str, snapshot_id: str) -> str:
    raw_modeled = modeled_date_et.strip()
    if raw_modeled:
        for fmt in _MODELED_DATE_FORMATS:
            try:
                return datetime.strptime(raw_modeled, fmt).date().isoformat()
            except ValueError:
                continue
    parsed_from_snapshot = _day_from_snapshot_id(snapshot_id)
    return parsed_from_snapshot.isoformat() if parsed_from_snapshot is not None else ""


def _normalized_rows(rows: list[dict[str, Any]]) -> list[tuple[str, float, int]]:
    normalized: list[tuple[str, float, int]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        result = _normalize_result(row.get("result"))
        if result not in {"win", "loss"}:
            continue
        model_probability = _safe_float(row.get("model_p_hit"))
        if model_probability is None or not (0.0 <= model_probability <= 1.0):
            continue
        snapshot_id = str(row.get("snapshot_id", "")).strip()
        modeled_date_et = str(row.get("modeled_date_et", "")).strip()
        modeled_day = resolve_modeled_day(modeled_date_et=modeled_date_et, snapshot_id=snapshot_id)
        if not modeled_day:
            continue
        outcome = 1 if result == "win" else 0
        normalized.append((modeled_day, model_probability, outcome))
    normalized.sort(key=lambda item: (item[0], item[1], item[2]))
    return normalized


def _build_bins(points: list[tuple[float, int]], *, bin_size: float) -> list[dict[str, Any]]:
    size = float(bin_size)
    if size <= 0.0 or size > 0.5:
        raise ValueError("bin_size must be in (0, 0.5]")
    buckets: dict[int, list[tuple[float, int]]] = {}
    for probability, outcome in points:
        index = int(probability / size)
        if index < 0:
            index = 0
        max_index = int(1.0 / size)
        if index > max_index:
            index = max_index
        buckets.setdefault(index, []).append((probability, outcome))
    rows: list[dict[str, Any]] = []
    for index in sorted(buckets.keys()):
        items = buckets[index]
        count = len(items)
        average_probability = sum(probability for probability, _ in items) / count
        hit_rate = sum(outcome for _, outcome in items) / count
        low = round(index * size, 6)
        high = round(min(1.0, low + size), 6)
        rows.append(
            {
                "low": low,
                "high": high,
                "count": count,
                "avg_p": round(average_probability, 6),
                "hit_rate": round(hit_rate, 6),
            }
        )
    return rows


def build_calibration_map(
    *,
    rows_by_strategy: dict[str, list[dict[str, Any]]],
    bin_size: float = 0.05,
    mode: CalibrationMode = "walk_forward",
    dataset_id: str = "",
) -> dict[str, Any]:
    strategies: dict[str, Any] = {}
    for strategy_id in sorted(rows_by_strategy.keys()):
        rows = rows_by_strategy.get(strategy_id, [])
        normalized = _normalized_rows(rows)
        points = [(probability, outcome) for _, probability, outcome in normalized]
        strategy_payload: dict[str, Any] = {
            "rows_scored": len(points),
            "bins": _build_bins(points, bin_size=bin_size),
        }
        if mode == "walk_forward":
            by_day: dict[str, Any] = {}
            all_days = sorted({modeled_day for modeled_day, _, _ in normalized})
            for modeled_day in all_days:
                history_points = [
                    (probability, outcome)
                    for day_key, probability, outcome in normalized
                    if day_key < modeled_day
                ]
                by_day[modeled_day] = {
                    "rows_scored": len(history_points),
                    "bins": _build_bins(history_points, bin_size=bin_size),
                }
            strategy_payload["by_day"] = by_day
        strategies[strategy_id] = strategy_payload
    return {
        "schema_version": CALIBRATION_MAP_SCHEMA_VERSION,
        "dataset_id": dataset_id,
        "mode": mode,
        "bin_size": float(bin_size),
        "strategies": strategies,
    }


def _confidence_tier(
    *,
    calibrated_probability: float | None,
    quality_score: float | None,
    uncertainty_band: float | None,
) -> str:
    if calibrated_probability is None:
        return "unrated"
    quality = 0.0 if quality_score is None else max(0.0, min(1.0, quality_score))
    uncertainty = 1.0 if uncertainty_band is None else max(0.0, uncertainty_band)
    if calibrated_probability >= 0.57 and quality >= 0.7 and uncertainty <= 0.05:
        return "high"
    if calibrated_probability >= 0.53 and quality >= 0.55 and uncertainty <= 0.08:
        return "medium"
    return "low"


def _resolve_bins_for_report(
    *,
    calibration_map: dict[str, Any],
    strategy_id: str,
    modeled_day: str,
) -> list[dict[str, Any]]:
    strategies = calibration_map.get("strategies", {})
    if not isinstance(strategies, dict):
        return []
    strategy_payload = strategies.get(strategy_id, {})
    if not isinstance(strategy_payload, dict):
        return []
    mode = str(calibration_map.get("mode", "in_sample")).strip().lower()
    if mode == "walk_forward" and modeled_day:
        by_day = strategy_payload.get("by_day", {})
        if isinstance(by_day, dict):
            day_payload = by_day.get(modeled_day, {})
            if isinstance(day_payload, dict):
                bins = day_payload.get("bins", [])
                if isinstance(bins, list):
                    return [row for row in bins if isinstance(row, dict)]
    bins = strategy_payload.get("bins", [])
    if isinstance(bins, list):
        return [row for row in bins if isinstance(row, dict)]
    return []


def _calibrated_probability(
    *, bins: list[dict[str, Any]], conservative_probability: float
) -> tuple[float | None, dict[str, Any] | None]:
    for row in bins:
        low = _safe_float(row.get("low"))
        high = _safe_float(row.get("high"))
        hit_rate = _safe_float(row.get("hit_rate"))
        if low is None or high is None:
            continue
        in_bucket = low <= conservative_probability < high
        is_last_bucket = high >= 1.0 and conservative_probability <= high
        if not in_bucket and not is_last_bucket:
            continue
        return hit_rate, row
    return None, None


def annotate_rows_with_calibration_map(
    *,
    rows: list[dict[str, Any]],
    calibration_map: dict[str, Any],
    strategy_id: str,
    modeled_day: str,
) -> list[dict[str, Any]]:
    bins = _resolve_bins_for_report(
        calibration_map=calibration_map,
        strategy_id=strategy_id,
        modeled_day=modeled_day,
    )
    mode = str(calibration_map.get("mode", "in_sample")).strip().lower() or "in_sample"
    annotated: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        output = dict(row)
        conservative_probability = _safe_float(row.get("p_hit_low"))
        if conservative_probability is None:
            side = str(row.get("recommended_side", "")).strip().lower()
            if side == "over":
                conservative_probability = _safe_float(row.get("p_over_low"))
                if conservative_probability is None:
                    conservative_probability = _safe_float(row.get("p_over_model"))
            elif side == "under":
                conservative_probability = _safe_float(row.get("p_under_low"))
                if conservative_probability is None:
                    conservative_probability = _safe_float(row.get("p_under_model"))
        if conservative_probability is None:
            conservative_probability = _safe_float(row.get("model_p_hit"))
        if conservative_probability is not None and not (0.0 <= conservative_probability <= 1.0):
            conservative_probability = None

        calibrated_probability: float | None = None
        bucket_payload: dict[str, Any] | None = None
        if conservative_probability is not None and bins:
            calibrated_probability, bucket_payload = _calibrated_probability(
                bins=bins,
                conservative_probability=conservative_probability,
            )

        output["p_conservative"] = (
            None if conservative_probability is None else round(conservative_probability, 6)
        )
        output["p_calibrated"] = (
            None
            if calibrated_probability is None
            else round(max(0.0, min(1.0, calibrated_probability)), 6)
        )
        output["calibration_bin"] = (
            {
                "low": _safe_float(bucket_payload.get("low")),
                "high": _safe_float(bucket_payload.get("high")),
                "count": int(bucket_payload.get("count", 0) or 0),
            }
            if isinstance(bucket_payload, dict)
            else {}
        )
        output["calibration_map_mode"] = mode
        output["confidence_tier"] = _confidence_tier(
            calibrated_probability=_safe_float(output.get("p_calibrated")),
            quality_score=_safe_float(row.get("quality_score")),
            uncertainty_band=_safe_float(row.get("uncertainty_band")),
        )
        annotated.append(output)
    return annotated


def annotate_strategy_report_with_calibration_map(
    *,
    report: dict[str, Any],
    calibration_map: dict[str, Any],
) -> dict[str, Any]:
    output = dict(report)
    strategy_id = str(report.get("strategy_id", "s001")).strip().lower() or "s001"
    snapshot_id = str(report.get("snapshot_id", "")).strip()
    modeled_day = resolve_modeled_day(
        modeled_date_et=str(report.get("modeled_date_et", "")).strip(),
        snapshot_id=snapshot_id,
    )
    for field in ("ranked_plays", "watchlist", "top_ev_plays", "portfolio_watchlist", "candidates"):
        rows = output.get(field, [])
        if not isinstance(rows, list):
            continue
        output[field] = annotate_rows_with_calibration_map(
            rows=[row for row in rows if isinstance(row, dict)],
            calibration_map=calibration_map,
            strategy_id=strategy_id,
            modeled_day=modeled_day,
        )
    audit = output.get("audit", {})
    if isinstance(audit, dict):
        audit_payload = dict(audit)
        audit_payload["calibration_map_mode"] = str(calibration_map.get("mode", ""))
        audit_payload["calibration_map_modeled_day"] = modeled_day
        output["audit"] = audit_payload
    return output
