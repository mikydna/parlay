"""Context and roster/injury helper functions for strategy generation."""

from __future__ import annotations

from typing import Any

from prop_ev.identity_map import name_aliases
from prop_ev.nba_data.normalize import canonical_team_name, normalize_person_name


def injury_source_rows(source: Any, *, default_source: str) -> list[dict[str, Any]]:
    if not isinstance(source, dict):
        return []
    rows = source.get("rows", [])
    if not isinstance(rows, list):
        return []
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        player = str(row.get("player", "")).strip()
        player_norm = str(row.get("player_norm", "")).strip() or normalize_person_name(player)
        if not player_norm:
            continue
        team_norm = canonical_team_name(str(row.get("team_norm", row.get("team", ""))))
        item = dict(row)
        item["player"] = player
        item["player_norm"] = player_norm
        item["team_norm"] = team_norm
        item["source"] = str(row.get("source", default_source))
        cleaned.append(item)
    return cleaned


def merged_injury_rows(injuries: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(injuries, dict):
        return []
    official_rows = injury_source_rows(
        injuries.get("official"),
        default_source="official_nba_pdf",
    )
    secondary_rows = injury_source_rows(
        injuries.get("secondary"),
        default_source="secondary_injuries",
    )
    merged: dict[str, dict[str, Any]] = {}
    for row in secondary_rows:
        merged[str(row.get("player_norm", ""))] = row
    for row in official_rows:
        key = str(row.get("player_norm", ""))
        previous = merged.get(key)
        if previous is not None:
            item = dict(previous)
            item.update(row)
            item["team"] = str(previous.get("team", row.get("team", "")))
            item["team_norm"] = canonical_team_name(
                str(previous.get("team_norm", previous.get("team", "")))
            )
            merged[key] = item
        else:
            merged[key] = row
    return list(merged.values())


def injury_index(injuries: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}
    rows = merged_injury_rows(injuries)
    if not rows:
        return index
    severity = {
        "unknown": 0,
        "available": 1,
        "day_to_day": 1,
        "probable": 2,
        "questionable": 3,
        "doubtful": 4,
        "out": 5,
        "out_for_season": 6,
    }
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("player_norm", "")) or normalize_person_name(str(row.get("player", "")))
        if not key:
            continue
        status = str(row.get("status", "unknown"))
        date_update = str(row.get("date_update", ""))
        team_norm = canonical_team_name(str(row.get("team_norm", row.get("team", ""))))
        current = index.get(key)
        candidate = {
            "status": status,
            "date_update": date_update,
            "source": str(row.get("source", "")),
            "note": str(row.get("note", "")),
            "team_norm": team_norm,
            "team": str(row.get("team", "")),
        }
        if current is None:
            index[key] = candidate
            continue
        if severity.get(status, 0) > severity.get(current.get("status", "unknown"), 0):
            index[key] = candidate
            continue
        if date_update > current.get("date_update", ""):
            index[key] = candidate
    return index


def injuries_by_team(injuries: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in merged_injury_rows(injuries):
        team_norm = canonical_team_name(str(row.get("team_norm", row.get("team", ""))))
        if not team_norm:
            continue
        grouped.setdefault(team_norm, []).append(row)
    return grouped


def roster_status(
    *,
    player_name: str,
    event_id: str,
    event_context: dict[str, dict[str, str]] | None,
    roster: dict[str, Any] | None,
    player_identity_map: dict[str, Any] | None = None,
) -> str:
    if not isinstance(event_context, dict):
        return "unknown_event"
    ctx = event_context.get(event_id)
    if not isinstance(ctx, dict):
        return "unknown_event"
    if not isinstance(roster, dict):
        return "unknown_roster"
    teams = roster.get("teams", {})
    if not isinstance(teams, dict):
        return "unknown_roster"

    home = canonical_team_name(str(ctx.get("home_team", "")))
    away = canonical_team_name(str(ctx.get("away_team", "")))
    if not home or not away:
        return "unknown_event"
    home_row = teams.get(home)
    away_row = teams.get(away)
    if not isinstance(home_row, dict) or not isinstance(away_row, dict):
        return "unknown_roster"

    player_norm = normalize_person_name(player_name)
    aliases = set(name_aliases(player_name)) | {player_norm}
    if isinstance(player_identity_map, dict):
        player_rows = player_identity_map.get("players", {})
        if isinstance(player_rows, dict):
            for alias in list(aliases):
                row = player_rows.get(alias)
                if isinstance(row, dict):
                    alias_rows = row.get("aliases", [])
                    if isinstance(alias_rows, list):
                        aliases.update(item for item in alias_rows if isinstance(item, str))
    home_active = set(home_row.get("active", []))
    away_active = set(away_row.get("active", []))
    home_inactive = set(home_row.get("inactive", []))
    away_inactive = set(away_row.get("inactive", []))
    home_all = set(home_row.get("all", []))
    away_all = set(away_row.get("all", []))

    if aliases & home_inactive or aliases & away_inactive:
        return "inactive"
    if aliases & home_active or aliases & away_active:
        return "active"
    if aliases & home_all or aliases & away_all:
        return "rostered"
    return "not_on_roster"


def resolve_player_team(
    *,
    player_name: str,
    event_id: str,
    event_context: dict[str, dict[str, str]] | None,
    roster: dict[str, Any] | None,
    injury_row: dict[str, Any],
    player_identity_map: dict[str, Any] | None = None,
) -> str:
    ctx = event_context.get(event_id, {}) if isinstance(event_context, dict) else {}
    home = canonical_team_name(str(ctx.get("home_team", "")))
    away = canonical_team_name(str(ctx.get("away_team", "")))

    if isinstance(roster, dict):
        teams = roster.get("teams", {})
        if isinstance(teams, dict) and home and away:
            home_row = teams.get(home, {})
            away_row = teams.get(away, {})
            if isinstance(home_row, dict) and isinstance(away_row, dict):
                player_norm = normalize_person_name(player_name)
                aliases = set(name_aliases(player_name)) | {player_norm}
                if isinstance(player_identity_map, dict):
                    player_rows = player_identity_map.get("players", {})
                    if isinstance(player_rows, dict):
                        for alias in list(aliases):
                            row = player_rows.get(alias)
                            if isinstance(row, dict):
                                alias_rows = row.get("aliases", [])
                                if isinstance(alias_rows, list):
                                    aliases.update(
                                        item for item in alias_rows if isinstance(item, str)
                                    )
                home_all = set(home_row.get("all", []))
                away_all = set(away_row.get("all", []))
                if aliases & home_all and not (aliases & away_all):
                    return home
                if aliases & away_all and not (aliases & home_all):
                    return away

    injury_team = canonical_team_name(str(injury_row.get("team_norm", "")))
    if injury_team and injury_team in {home, away}:
        return injury_team

    if isinstance(player_identity_map, dict):
        players = player_identity_map.get("players", {})
        if isinstance(players, dict):
            for alias in name_aliases(player_name):
                row = players.get(alias)
                if not isinstance(row, dict):
                    continue
                teams = row.get("teams", [])
                if not isinstance(teams, list):
                    continue
                for team in teams:
                    team_norm = canonical_team_name(str(team))
                    if team_norm in {home, away}:
                        return team_norm
    return ""


def count_team_status(rows: list[dict[str, Any]], exclude_player_norm: str) -> dict[str, int]:
    counts = {
        "out": 0,
        "out_for_season": 0,
        "doubtful": 0,
        "questionable": 0,
    }
    for row in rows:
        if not isinstance(row, dict):
            continue
        if normalize_person_name(str(row.get("player", ""))) == exclude_player_norm:
            continue
        status = str(row.get("status", "unknown"))
        if status in counts:
            counts[status] += 1
    return counts
