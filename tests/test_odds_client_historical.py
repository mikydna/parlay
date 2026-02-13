from __future__ import annotations

import pytest

from prop_ev.odds_client import OddsAPIClient, OddsAPIError, OddsResponse
from prop_ev.settings import Settings


def _settings() -> Settings:
    return Settings(ODDS_API_KEY="odds-test")


def test_list_events_historical_unwraps_data(monkeypatch: pytest.MonkeyPatch) -> None:
    client = OddsAPIClient(_settings())
    captured: dict[str, object] = {}

    def fake_request(*, path: str, params: dict[str, object]) -> OddsResponse:
        captured["path"] = path
        captured["params"] = dict(params)
        return OddsResponse(
            data={
                "timestamp": "2026-02-11T17:00:00Z",
                "data": [{"id": "event-1"}],
            },
            status_code=200,
            headers={},
            duration_ms=1,
            retry_count=0,
        )

    monkeypatch.setattr(client, "_request", fake_request)
    response = client.list_events(
        sport_key="basketball_nba",
        historical_date="2026-02-11T17:00:00Z",
    )
    client.close()

    assert captured["path"] == "/historical/sports/basketball_nba/events"
    assert isinstance(captured["params"], dict)
    assert captured["params"]["date"] == "2026-02-11T17:00:00Z"
    assert response.data == [{"id": "event-1"}]


def test_get_event_odds_historical_unwraps_data(monkeypatch: pytest.MonkeyPatch) -> None:
    client = OddsAPIClient(_settings())
    captured: dict[str, object] = {}

    def fake_request(*, path: str, params: dict[str, object]) -> OddsResponse:
        captured["path"] = path
        captured["params"] = dict(params)
        return OddsResponse(
            data={
                "timestamp": "2026-02-11T19:30:00Z",
                "data": {"id": "event-1", "bookmakers": []},
            },
            status_code=200,
            headers={},
            duration_ms=1,
            retry_count=0,
        )

    monkeypatch.setattr(client, "_request", fake_request)
    response = client.get_event_odds(
        sport_key="basketball_nba",
        event_id="event-1",
        markets=["player_points"],
        regions="us",
        bookmakers=None,
        historical_date="2026-02-11T19:30:00Z",
    )
    client.close()

    assert captured["path"] == "/historical/sports/basketball_nba/events/event-1/odds"
    assert isinstance(captured["params"], dict)
    assert captured["params"]["date"] == "2026-02-11T19:30:00Z"
    assert response.data == {"id": "event-1", "bookmakers": []}


def test_list_events_historical_rejects_non_list(monkeypatch: pytest.MonkeyPatch) -> None:
    client = OddsAPIClient(_settings())

    def fake_request(*, path: str, params: dict[str, object]) -> OddsResponse:
        del path, params
        return OddsResponse(
            data={"timestamp": "2026-02-11T17:00:00Z", "data": {}},
            status_code=200,
            headers={},
            duration_ms=1,
            retry_count=0,
        )

    monkeypatch.setattr(client, "_request", fake_request)
    with pytest.raises(OddsAPIError):
        client.list_events(
            sport_key="basketball_nba",
            historical_date="2026-02-11T17:00:00Z",
        )
    client.close()
