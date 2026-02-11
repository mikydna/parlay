"""Typed wrappers for The Odds API v4 client.

Notes:
- The Odds API v4 examples pass authentication using an `apiKey` query param.
- Player props are fetched per event, so the flow is list events first, then call
  `/events/{eventId}/odds` for each event.
"""

from dataclasses import dataclass
from typing import Any

from prop_ev.odds_client import OddsAPIClient


@dataclass(frozen=True)
class SportKey:
    """A sport key value used by The Odds API."""

    value: str


@dataclass(frozen=True)
class EventId:
    """An event identifier returned by The Odds API."""

    value: str


@dataclass(frozen=True)
class MarketKey:
    """A market key value used when requesting odds."""

    value: str


def list_events(*, sport_key: SportKey) -> list[dict[str, Any]]:
    """Backward-compatible placeholder function."""
    raise NotImplementedError("use list_events_with_client")


def list_events_with_client(
    *,
    client: OddsAPIClient,
    sport_key: SportKey,
    commence_from: str | None = None,
    commence_to: str | None = None,
) -> list[dict[str, Any]]:
    """List events for a sport."""
    response = client.list_events(
        sport_key=sport_key.value,
        commence_from=commence_from,
        commence_to=commence_to,
    )
    if not isinstance(response.data, list):
        return []
    events: list[dict[str, Any]] = []
    for row in response.data:
        if isinstance(row, dict):
            events.append(row)
    return events


def get_event_odds(
    *,
    sport_key: SportKey,
    event_id: EventId,
    market_key: MarketKey,
) -> dict[str, Any]:
    """Backward-compatible placeholder function."""
    raise NotImplementedError("use get_event_odds_with_client")


def get_event_odds_with_client(
    *,
    client: OddsAPIClient,
    sport_key: SportKey,
    event_id: EventId,
    market_key: MarketKey,
    regions: str = "us",
) -> dict[str, Any]:
    """Fetch odds for one event."""
    response = client.get_event_odds(
        sport_key=sport_key.value,
        event_id=event_id.value,
        markets=[market_key.value],
        regions=regions,
        bookmakers=None,
    )
    if isinstance(response.data, dict):
        return response.data
    return {}
