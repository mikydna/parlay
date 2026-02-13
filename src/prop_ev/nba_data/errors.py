"""Error types for nba-data flows."""

from __future__ import annotations


class NBADataError(RuntimeError):
    """Base error for nba-data operations."""


class CLIError(NBADataError):
    """User-facing CLI error for nba-data commands."""
