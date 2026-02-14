from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from statistics import NormalDist, fmean, stdev
from typing import Any

DEFAULT_TARGET_ROI_UPLIFTS_PER_BET: tuple[float, ...] = (0.01, 0.02, 0.03, 0.05)


@dataclass(frozen=True)
class PowerGuidanceAssumptions:
    alpha: float = 0.05
    power: float = 0.8
    picks_per_day: int = 5
    target_roi_uplifts_per_bet: tuple[float, ...] = DEFAULT_TARGET_ROI_UPLIFTS_PER_BET

    def normalized(self) -> PowerGuidanceAssumptions:
        alpha_value = float(self.alpha)
        if alpha_value <= 0.0 or alpha_value >= 1.0:
            raise ValueError("alpha must be in (0, 1)")

        power_value = float(self.power)
        if power_value <= 0.0 or power_value >= 1.0:
            raise ValueError("power must be in (0, 1)")

        picks_per_day_value = max(1, int(self.picks_per_day))
        target_uplifts = tuple(
            sorted(
                {
                    float(value)
                    for value in self.target_roi_uplifts_per_bet
                    if float(value) > 0.0 and math.isfinite(float(value))
                }
            )
        )
        if not target_uplifts:
            target_uplifts = DEFAULT_TARGET_ROI_UPLIFTS_PER_BET

        return PowerGuidanceAssumptions(
            alpha=alpha_value,
            power=power_value,
            picks_per_day=picks_per_day_value,
            target_roi_uplifts_per_bet=target_uplifts,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": "normal_approximation_paired_daily_pnl_diff",
            "alpha": float(self.alpha),
            "power": float(self.power),
            "picks_per_day": int(self.picks_per_day),
            "target_roi_uplifts_per_bet": [
                float(value) for value in self.target_roi_uplifts_per_bet
            ],
        }


def _safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (float, int)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = float(raw)
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def _required_days_for_effect(
    *,
    std_daily_pnl_diff: float,
    effect_daily_pnl: float,
    alpha: float,
    power: float,
) -> int | None:
    if effect_daily_pnl <= 0.0:
        return None
    if std_daily_pnl_diff < 0.0 or not math.isfinite(std_daily_pnl_diff):
        return None
    if std_daily_pnl_diff == 0.0:
        return 1

    normal = NormalDist()
    z_alpha = normal.inv_cdf(1.0 - (alpha / 2.0))
    z_power = normal.inv_cdf(power)
    ratio = ((z_alpha + z_power) * std_daily_pnl_diff) / effect_daily_pnl
    if not math.isfinite(ratio):
        return None
    return max(1, int(math.ceil(ratio**2)))


def build_power_guidance(
    *,
    daily_pnl_by_strategy: Mapping[str, Mapping[str, float | int | str]],
    baseline_strategy_id: str,
    assumptions: PowerGuidanceAssumptions | None = None,
) -> dict[str, Any]:
    baseline = baseline_strategy_id.strip()
    if not baseline:
        return {}

    assumption_values = (
        assumptions.normalized()
        if assumptions is not None
        else PowerGuidanceAssumptions().normalized()
    )
    baseline_daily = daily_pnl_by_strategy.get(baseline, {})
    if not baseline_daily:
        return {}

    strategy_rows: list[dict[str, Any]] = []
    for strategy_id in sorted(daily_pnl_by_strategy.keys()):
        if strategy_id == baseline:
            continue
        candidate_daily = daily_pnl_by_strategy.get(strategy_id, {})
        overlap_days = sorted(set(candidate_daily.keys()) & set(baseline_daily.keys()))
        daily_diffs: list[float] = []
        for day in overlap_days:
            candidate_value = _safe_float(candidate_daily.get(day))
            baseline_value = _safe_float(baseline_daily.get(day))
            if candidate_value is None or baseline_value is None:
                continue
            daily_diffs.append(candidate_value - baseline_value)

        if len(daily_diffs) < 2:
            strategy_rows.append(
                {
                    "strategy_id": strategy_id,
                    "overlap_days": len(daily_diffs),
                    "mean_daily_pnl_diff": round(fmean(daily_diffs), 6) if daily_diffs else None,
                    "std_daily_pnl_diff": None,
                    "insufficient_overlap": True,
                    "required_days_by_target": [],
                }
            )
            continue

        mean_daily_pnl_diff = fmean(daily_diffs)
        std_daily_pnl_diff = stdev(daily_diffs)
        required_days_by_target: list[dict[str, Any]] = []
        for target in assumption_values.target_roi_uplifts_per_bet:
            effect_daily_pnl = target * assumption_values.picks_per_day
            required_days = _required_days_for_effect(
                std_daily_pnl_diff=std_daily_pnl_diff,
                effect_daily_pnl=effect_daily_pnl,
                alpha=assumption_values.alpha,
                power=assumption_values.power,
            )
            required_days_by_target.append(
                {
                    "target_roi_uplift_per_bet": round(target, 6),
                    "daily_effect_units": round(effect_daily_pnl, 6),
                    "required_days": required_days,
                    "required_graded_rows": (
                        int(required_days * assumption_values.picks_per_day)
                        if isinstance(required_days, int)
                        else None
                    ),
                    "meets_with_observed_days": (
                        bool(required_days is not None and len(daily_diffs) >= required_days)
                    ),
                }
            )

        strategy_rows.append(
            {
                "strategy_id": strategy_id,
                "overlap_days": len(daily_diffs),
                "mean_daily_pnl_diff": round(mean_daily_pnl_diff, 6),
                "std_daily_pnl_diff": round(std_daily_pnl_diff, 6),
                "insufficient_overlap": False,
                "required_days_by_target": required_days_by_target,
            }
        )

    if not strategy_rows:
        return {}

    return {
        "baseline_strategy_id": baseline,
        "assumptions": assumption_values.to_dict(),
        "strategies": strategy_rows,
    }
