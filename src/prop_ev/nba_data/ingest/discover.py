"""Season discovery for final NBA games."""

from __future__ import annotations

from typing import Any

from prop_ev.nba_data.ingest.pbp_adapter import build_client, discover_final_games
from prop_ev.nba_data.store.layout import NBADataLayout


def discover_games(
    *,
    layout: NBADataLayout,
    season: str,
    season_type: str,
    provider_games: str,
) -> list[dict[str, Any]]:
    """Fetch and normalize final game list for one season+type."""
    client = build_client(
        response_dir=layout.pbpstats_response_dir,
        resource_settings={"Games": {"data_provider": provider_games}},
        source="web",
    )
    games = discover_final_games(client, season=season, season_type=season_type)
    normalized: list[dict[str, Any]] = []
    for row in games:
        game_id = str(row.get("game_id", "") or row.get("id", "")).strip()
        if not game_id:
            continue
        normalized.append(
            {
                "game_id": game_id,
                "date": str(row.get("date", "")),
                "home_team_id": str(row.get("home_team_id", "") or row.get("home_team", "")),
                "away_team_id": str(row.get("away_team_id", "") or row.get("away_team", "")),
            }
        )
    normalized.sort(key=lambda row: row["game_id"])
    return normalized
