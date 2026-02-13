"""pbpstats adapter seam for testability."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from prop_ev.nba_data.errors import NBADataError


def build_client(
    *,
    response_dir: Path,
    resource_settings: dict[str, dict[str, str]],
    source: str,
) -> Any:
    """Build a pbpstats client with unified settings."""
    try:
        from pbpstats.client import Client  # pyright: ignore[reportMissingImports]
    except Exception as exc:  # pragma: no cover - exercised in runtime environments
        raise NBADataError(
            "pbpstats is required for nba-data ingestion; install project dependencies"
        ) from exc

    settings: dict[str, dict[str, str]] = {}
    all_resources = ("Games", "EnhancedPbp", "Possessions", "Boxscore")
    for resource in all_resources:
        cfg = dict(resource_settings.get(resource, {}))
        cfg["source"] = source
        cfg["data_dir"] = str(response_dir)
        settings[resource] = cfg
    return Client(settings)


def _to_records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def discover_final_games(client: Any, *, season: str, season_type: str) -> list[dict[str, Any]]:
    """Return final games from a pbpstats client."""
    season_obj = client.Season("nba", season, season_type)
    games = getattr(season_obj, "games", None)
    final_games = getattr(games, "final_games", [])
    return _to_records(final_games)


def load_game_resource(client: Any, *, game_id: str, resource: str) -> Any:
    """Load one game resource from a pbpstats client."""
    if hasattr(client, "load_resource"):
        return client.load_resource(game_id=game_id, resource=resource)

    game_obj = None
    if hasattr(client, "Game"):
        game_obj = client.Game(game_id)
    elif hasattr(client, "game"):
        game_obj = client.game(game_id)
    if game_obj is None:
        raise NBADataError("pbpstats client missing Game accessor")

    attr_name = {
        "boxscore": "boxscore",
        "enhanced_pbp": "enhanced_pbp",
        "possessions": "possessions",
    }[resource]
    payload = getattr(game_obj, attr_name, None)
    if callable(payload):
        return payload()
    return payload
