from pathlib import Path

from prop_ev.identity_map import name_aliases, update_identity_map


def test_name_aliases_handles_suffixes() -> None:
    aliases = name_aliases("Paul Reed Jr.")
    assert "paulreedjr" in aliases
    assert "paulreed" in aliases


def test_update_identity_map_writes_entries(tmp_path: Path) -> None:
    path = tmp_path / "reference" / "player_identity_map.json"
    summary = update_identity_map(
        path=path,
        rows=[
            {
                "event_id": "event-1",
                "player": "Player A",
            },
            {
                "event_id": "event-1",
                "player": "Paul Reed Jr.",
            },
        ],
        roster={
            "teams": {
                "boston celtics": {"all": ["playera"], "active": ["playera"], "inactive": []},
                "miami heat": {"all": ["paulreed"], "active": ["paulreed"], "inactive": []},
            }
        },
        event_context={
            "event-1": {
                "home_team": "Boston Celtics",
                "away_team": "Miami Heat",
            }
        },
    )

    assert path.exists()
    assert summary["player_entries"] >= 2
