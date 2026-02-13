"""Errors raised by odds-data repository and backfill flows."""

from __future__ import annotations


class OddsDataError(Exception):
    """Base error for odds-data operations."""


class OfflineCacheMiss(OddsDataError):
    """Raised when offline mode hits a cache miss."""


class SpendBlockedError(OddsDataError):
    """Raised when paid fetches are blocked but a paid cache miss occurs."""


class CreditBudgetExceeded(OddsDataError):
    """Raised when estimated spend exceeds the configured credit budget."""
