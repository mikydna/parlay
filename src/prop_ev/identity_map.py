"""Persistent player identity mapping for cross-source joins."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from prop_ev.nba_data.normalize import canonical_team_name, normalize_person_name
from prop_ev.time_utils import utc_now_str

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def _now_utc() -> str:
    return utc_now_str()


def name_aliases(name: str) -> list[str]:
    """Generate deterministic normalized aliases for player-name matching."""
    raw = name.strip()
    if not raw:
        return []

    aliases: set[str] = set()
    primary = normalize_person_name(raw)
    if primary:
        aliases.add(primary)

    words = re.sub(r"[^a-zA-Z0-9\s]", " ", raw).lower().split()
    if words and words[-1] in SUFFIXES:
        no_suffix = " ".join(words[:-1]).strip()
        if no_suffix:
            norm = normalize_person_name(no_suffix)
            if norm:
                aliases.add(norm)

    # Handle occasional initials or doubled spaces.
    collapsed = " ".join(words)
    if collapsed:
        norm = normalize_person_name(collapsed)
        if norm:
            aliases.add(norm)

    return sorted(aliases)


def _empty_map() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "updated_at_utc": "",
        "players": {},
    }


def load_identity_map(path: Path) -> dict[str, Any]:
    """Load identity map from disk or return empty structure."""
    if not path.exists():
        return _empty_map()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _empty_map()
    if not isinstance(payload, dict):
        return _empty_map()
    players = payload.get("players", {})
    if not isinstance(players, dict):
        return _empty_map()
    payload.setdefault("schema_version", 1)
    payload.setdefault("updated_at_utc", "")
    return payload


def _pick_player_team(
    *,
    player_aliases: list[str],
    event_id: str,
    roster: dict[str, Any],
    event_context: dict[str, dict[str, str]],
) -> str:
    ctx = event_context.get(event_id, {})
    if not isinstance(ctx, dict):
        return ""
    home = canonical_team_name(str(ctx.get("home_team", "")))
    away = canonical_team_name(str(ctx.get("away_team", "")))
    if not home or not away:
        return ""

    teams = roster.get("teams", {}) if isinstance(roster, dict) else {}
    home_row = teams.get(home, {}) if isinstance(teams, dict) else {}
    away_row = teams.get(away, {}) if isinstance(teams, dict) else {}
    home_all = set(home_row.get("all", [])) if isinstance(home_row, dict) else set()
    away_all = set(away_row.get("all", [])) if isinstance(away_row, dict) else set()

    for alias in player_aliases:
        if alias in home_all and alias not in away_all:
            return home
        if alias in away_all and alias not in home_all:
            return away
    return ""


def _ensure_player_entry(players: dict[str, Any], key: str, canonical_name: str) -> dict[str, Any]:
    current = players.get(key)
    if not isinstance(current, dict):
        current = {
            "canonical_name": canonical_name,
            "aliases": [key],
            "odds_api_names": [canonical_name],
            "teams": [],
            "espn_ids": [],
            "last_seen_event_ids": [],
            "last_seen_utc": _now_utc(),
        }
        players[key] = current
    return current


def _merge_sorted_unique(existing: list[str], values: list[str]) -> list[str]:
    merged = {item for item in existing if isinstance(item, str) and item}
    merged.update(item for item in values if isinstance(item, str) and item)
    return sorted(merged)


def update_identity_map(
    *,
    path: Path,
    rows: list[dict[str, Any]],
    roster: dict[str, Any] | None,
    event_context: dict[str, dict[str, str]] | None,
) -> dict[str, Any]:
    """Update persistent identity map from observed odds rows and roster context."""
    event_context = event_context or {}
    payload = load_identity_map(path)
    players = payload.get("players", {})
    if not isinstance(players, dict):
        players = {}
        payload["players"] = players

    observed = 0
    updated = 0
    unresolved = 0

    for row in rows:
        if not isinstance(row, dict):
            continue
        player_name = str(row.get("player", "")).strip()
        event_id = str(row.get("event_id", "")).strip()
        if not player_name:
            continue
        observed += 1
        aliases = name_aliases(player_name)
        if not aliases:
            unresolved += 1
            continue

        canonical_key = aliases[0]
        entry = _ensure_player_entry(players, canonical_key, player_name)
        old_fingerprint = json.dumps(entry, sort_keys=True)

        entry["canonical_name"] = str(entry.get("canonical_name", "") or player_name)
        entry["aliases"] = _merge_sorted_unique(list(entry.get("aliases", [])), aliases)
        entry["odds_api_names"] = _merge_sorted_unique(
            list(entry.get("odds_api_names", [])), [player_name]
        )
        if event_id:
            entry["last_seen_event_ids"] = _merge_sorted_unique(
                list(entry.get("last_seen_event_ids", [])), [event_id]
            )

        team = ""
        if isinstance(roster, dict) and isinstance(event_context, dict) and event_id:
            team = _pick_player_team(
                player_aliases=aliases,
                event_id=event_id,
                roster=roster,
                event_context=event_context,
            )
        if team:
            entry["teams"] = _merge_sorted_unique(list(entry.get("teams", [])), [team])

        entry["last_seen_utc"] = _now_utc()

        new_fingerprint = json.dumps(entry, sort_keys=True)
        if new_fingerprint != old_fingerprint:
            updated += 1
        if not team:
            unresolved += 1

    payload["updated_at_utc"] = _now_utc()
    payload["players"] = dict(sorted(players.items()))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")

    return {
        "path": str(path),
        "player_entries": len(payload["players"]),
        "observed_rows": observed,
        "updated_entries": updated,
        "unresolved_rows": unresolved,
        "updated_at_utc": payload["updated_at_utc"],
    }
