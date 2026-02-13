"""Season discovery for final NBA games."""

from __future__ import annotations

from typing import Any

from prop_ev.nba_data.ingest.pbp_adapter import build_client, discover_final_games
from prop_ev.nba_data.store.layout import NBADataLayout


def _extract_team_id(value: Any) -> str:
    if isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        return str(int(value))
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("team_id", "id", "teamId"):
            nested = value.get(key)
            parsed = _extract_team_id(nested)
            if parsed:
                return parsed
    return ""


def _team_id_from_row(row: dict[str, Any], *, side: str) -> str:
    prefixes = ("home", "away") if side == "home" else ("away", "visitor")
    keys: list[str] = []
    for prefix in prefixes:
        keys.extend(
            [
                f"{prefix}_team_id",
                f"{prefix}_team",
                f"{prefix}TeamId",
                f"{prefix}Team",
                f"{prefix}TeamID",
            ]
        )
    keys.extend(["home_team_id" if side == "home" else "away_team_id"])
    for key in keys:
        parsed = _extract_team_id(row.get(key))
        if parsed:
            return parsed
    return ""


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
                "home_team_id": _team_id_from_row(row, side="home"),
                "away_team_id": _team_id_from_row(row, side="away"),
            }
        )
    normalized.sort(key=lambda row: row["game_id"])
    return normalized
