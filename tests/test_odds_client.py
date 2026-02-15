import pytest

from prop_ev.odds_client import (
    OddsAPIClient,
    OddsAPIError,
    estimate_event_credits,
    estimate_featured_credits,
    parse_csv,
    regions_equivalent,
)
from prop_ev.settings import Settings


def test_parse_csv() -> None:
    assert parse_csv("a,b, c") == ["a", "b", "c"]
    assert parse_csv("") == []


def test_regions_equivalent_prefers_bookmakers() -> None:
    assert regions_equivalent("us,eu", "book1,book2") == 1
    assert (
        regions_equivalent(
            "us", "book1,book2,book3,book4,book5,book6,book7,book8,book9,book10,book11"
        )
        == 2
    )


def test_credit_estimators() -> None:
    assert estimate_featured_credits(["spreads", "totals"], 1) == 2
    assert estimate_event_credits(["player_points"], 1, 10) == 10


def test_odds_client_raises_when_key_missing() -> None:
    settings = Settings(odds_api_key="")
    with (
        OddsAPIClient(settings) as client,
        pytest.raises(OddsAPIError, match="missing Odds API key"),
    ):
        client.list_sports()
