"""Source policy helpers for unified NBA repository."""

from __future__ import annotations

from typing import Literal

ResultsSourceMode = Literal["auto", "historical", "live", "cache_only"]


_VALID_RESULTS_MODES: tuple[ResultsSourceMode, ...] = ("auto", "historical", "live", "cache_only")


def normalize_results_source_mode(value: str) -> ResultsSourceMode:
    """Normalize CLI/env value to a supported results source mode."""
    cleaned = value.strip().lower()
    if cleaned in _VALID_RESULTS_MODES:
        return cleaned
    raise ValueError(f"invalid results source mode: {value}")
