"""Type and interface placeholders for The Odds API v4.

Notes:
- The Odds API v4 examples pass authentication using an `apiKey` query param.
- Player props are fetched per event, so the flow is list events first, then call
  `/events/{eventId}/odds` for each event.
"""

from dataclasses import dataclass
from typing import Any


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
    """List events for a sport.

    This is intentionally unimplemented; network calls are added later.
    """

    raise NotImplementedError


def get_event_odds(
    *,
    sport_key: SportKey,
    event_id: EventId,
    market_key: MarketKey,
) -> dict[str, Any]:
    """Fetch odds for one event.

    This is intentionally unimplemented; network calls are added later.
    """

    raise NotImplementedError
