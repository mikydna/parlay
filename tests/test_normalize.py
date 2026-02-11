import json
from pathlib import Path

from prop_ev.normalize import normalize_event_odds, normalize_featured_odds


def test_normalize_featured_odds_fixture() -> None:
    fixture_path = Path("tests/fixtures/featured_sample.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    rows = normalize_featured_odds(payload, snapshot_id="snap-1", provider="odds_api")

    assert len(rows) == 2
    assert rows[0]["provider"] == "odds_api"
    assert rows[0]["snapshot_id"] == "snap-1"
    assert rows[0]["market"] == "spreads"


def test_normalize_event_odds_fixture() -> None:
    fixture_path = Path("tests/fixtures/event_sample.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    rows = normalize_event_odds(payload, snapshot_id="snap-1", provider="odds_api")

    assert len(rows) == 2
    assert rows[0]["event_id"] == "event-1"
    assert rows[0]["market"] == "player_points"
    assert rows[0]["provider"] == "odds_api"
