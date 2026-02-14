from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from prop_ev.backtest_summary import BacktestSummary

DEFAULT_BASELINE_STRATEGY_ID = "s007"

PROMOTION_STATUS_PASS = "pass"
PROMOTION_STATUS_FAIL = "fail"


@dataclass(frozen=True)
class PromotionThresholds:
    min_graded: int
    min_scored_fraction: float
    ece_slack: float
    brier_slack: float

    def to_dict(self) -> dict[str, float | int]:
        return {
            "min_graded": int(self.min_graded),
            "min_scored_fraction": float(self.min_scored_fraction),
            "ece_slack": float(self.ece_slack),
            "brier_slack": float(self.brier_slack),
        }


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


def resolve_baseline_strategy_id(*, requested: str, available_strategy_ids: list[str]) -> str:
    available = sorted(
        {str(value).strip() for value in available_strategy_ids if str(value).strip()}
    )
    if not available:
        return ""
    requested_value = requested.strip()
    if requested_value:
        return requested_value
    if DEFAULT_BASELINE_STRATEGY_ID in available:
        return DEFAULT_BASELINE_STRATEGY_ID
    return available[0]


def build_promotion_gate(
    *,
    summary: BacktestSummary,
    baseline_summary: BacktestSummary | None,
    baseline_required: bool,
    thresholds: PromotionThresholds,
) -> dict[str, Any]:
    reasons: list[str] = []
    rows_graded = int(summary.rows_graded)
    rows_win_loss = int(summary.wins + summary.losses)
    rows_scored = int(summary.rows_scored)
    scored_fraction = (rows_scored / rows_win_loss) if rows_win_loss > 0 else 0.0

    if baseline_required and baseline_summary is None:
        reasons.append("missing_baseline")

    if rows_graded < max(0, int(thresholds.min_graded)):
        reasons.append("insufficient_graded")

    min_scored_fraction = min(1.0, max(0.0, float(thresholds.min_scored_fraction)))
    if rows_win_loss <= 0 or scored_fraction < min_scored_fraction:
        reasons.append("insufficient_scored_rows")

    if summary.ece is None:
        reasons.append("missing_calibration")

    if baseline_summary is not None and summary.strategy_id != baseline_summary.strategy_id:
        if (
            summary.ece is not None
            and baseline_summary.ece is not None
            and summary.ece > baseline_summary.ece + max(0.0, float(thresholds.ece_slack))
        ):
            reasons.append("calibration_regressed")
        if (
            summary.brier is not None
            and baseline_summary.brier is not None
            and summary.brier > baseline_summary.brier + max(0.0, float(thresholds.brier_slack))
        ):
            reasons.append("brier_regressed")

    unique_reasons = sorted(set(reasons))
    status = PROMOTION_STATUS_PASS if not unique_reasons else PROMOTION_STATUS_FAIL
    return {
        "status": status,
        "reasons": unique_reasons,
        "rows_win_loss": rows_win_loss,
        "rows_scored": rows_scored,
        "scored_fraction": round(scored_fraction, 6),
        "thresholds": thresholds.to_dict(),
    }


def pick_promotion_winner(strategy_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    eligible: list[dict[str, Any]] = []
    for row in strategy_rows:
        gate = row.get("promotion_gate")
        if not isinstance(gate, dict):
            continue
        if str(gate.get("status", "")).strip().lower() != PROMOTION_STATUS_PASS:
            continue
        eligible.append(row)

    if not eligible:
        return None

    def _key(row: dict[str, Any]) -> tuple[float, int, float, float, str]:
        roi_value = _safe_float(row.get("roi"))
        rows_graded = int(_safe_float(row.get("rows_graded")) or 0)
        ece_value = _safe_float(row.get("ece"))
        brier_value = _safe_float(row.get("brier"))
        strategy_id = str(row.get("strategy_id", ""))
        return (
            -(roi_value if roi_value is not None else -9999.0),
            -rows_graded,
            ece_value if ece_value is not None else 9999.0,
            brier_value if brier_value is not None else 9999.0,
            strategy_id,
        )

    winner = sorted(eligible, key=_key)[0]
    return {
        "strategy_id": str(winner.get("strategy_id", "")),
        "roi": winner.get("roi"),
        "rows_graded": winner.get("rows_graded"),
        "ece": winner.get("ece"),
        "brier": winner.get("brier"),
        "decision": "selected_by=roi_then_rows_graded_then_ece_then_brier_then_strategy_id",
    }
