"""Typed contracts for unified NBA repository payloads."""

from __future__ import annotations

from typing import Any, TypedDict

from prop_ev.nba_data.source_policy import ResultsSourceMode


class PlayerStats(TypedDict, total=False):
    """Normalized player stat map used by settlement grading."""

    points: float | None
    reboundsTotal: float | None
    assists: float | None
    threePointersMade: float | None


class PlayerRow(TypedDict, total=False):
    """Normalized player row keyed by normalized player name."""

    name: str
    status: str
    statistics: dict[str, Any]


class GameRow(TypedDict, total=False):
    """Normalized game row shared across historical and live sources."""

    game_id: str
    home_team: str
    away_team: str
    game_status: str
    game_status_text: str
    players: dict[str, PlayerRow]
    period: str
    game_clock: str


class ResultsPayload(TypedDict, total=False):
    """Repository payload for settlement-grade results."""

    source: str
    mode: ResultsSourceMode
    fetched_at_utc: str
    status: str
    errors: list[str]
    games: list[GameRow]
    count_games: int
    count_errors: int
    cache_level: str
    snapshot_date: str


class ContextEnvelope(TypedDict, total=False):
    """Repository payload for injuries/roster context."""

    fetched_at_utc: str
    official: dict[str, Any]
    secondary: dict[str, Any]
    stale: bool
    stale_age_hours: float


class RosterPayload(TypedDict, total=False):
    """Repository payload for roster availability context."""

    source: str
    fetched_at_utc: str
    status: str
    teams: dict[str, dict[str, Any]]
    games: list[dict[str, Any]]
    stale: bool
    stale_age_hours: float
