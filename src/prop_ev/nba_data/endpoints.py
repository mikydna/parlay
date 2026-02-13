"""Canonical NBA endpoint constants for repository/fetchers."""

from __future__ import annotations

TODAYS_SCOREBOARD_URL = (
    "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
)
BOXSCORE_URL_TEMPLATE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
