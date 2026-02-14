"""Conformal helpers for probabilistic minutes intervals."""

from __future__ import annotations

import numpy as np


def symmetric_halfwidth_from_residuals(
    *,
    actual: np.ndarray,
    median_prediction: np.ndarray,
    quantile: float = 0.9,
) -> float:
    """Estimate symmetric interval halfwidth using absolute residual quantile."""
    if actual.size == 0 or median_prediction.size == 0:
        return 4.0
    if actual.shape != median_prediction.shape:
        raise ValueError("actual and median_prediction must have matching shapes")
    q = max(0.5, min(0.995, float(quantile)))
    residuals = np.abs(actual - median_prediction)
    if residuals.size == 0:
        return 4.0
    offset = float(np.quantile(residuals, q))
    if not np.isfinite(offset):
        return 4.0
    return max(1.0, min(20.0, offset))
