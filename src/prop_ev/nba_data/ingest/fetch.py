"""Per-game resource ingestion with resume + provider fallback."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import requests

from prop_ev.nba_data.errors import NBADataError
from prop_ev.nba_data.ingest.pbp_adapter import build_client, load_game_resource
from prop_ev.nba_data.ingest.rate_limit import RateLimiter
from prop_ev.nba_data.io_utils import atomic_write_json, atomic_write_jsonl
from prop_ev.nba_data.store.layout import NBADataLayout, slugify_season_type
from prop_ev.nba_data.store.lock import LockConfig, lock_root
from prop_ev.nba_data.store.manifest import (
    RESOURCE_NAMES,
    ManifestRow,
    ResourceName,
    reconcile_ok_statuses,
    set_resource_error,
    set_resource_ok,
)

_REQUEST_TIMEOUT_SECONDS = 30


def _resource_key(resource: ResourceName) -> str:
    return {
        "boxscore": "Boxscore",
        "enhanced_pbp": "EnhancedPbp",
        "possessions": "Possessions",
    }[resource]


def _resource_ext(resource: ResourceName) -> str:
    return "json" if resource == "boxscore" else "jsonl"


def _rows_from_payload(
    payload: Any, *, game_id: str, resource: ResourceName
) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
    elif isinstance(payload, dict):
        if resource == "enhanced_pbp" and isinstance(payload.get("events"), list):
            rows = [row for row in payload["events"] if isinstance(row, dict)]
        elif resource == "possessions" and isinstance(payload.get("possessions"), list):
            rows = [row for row in payload["possessions"] if isinstance(row, dict)]
        elif isinstance(payload.get("data"), list):
            rows = [row for row in payload["data"] if isinstance(row, dict)]
        else:
            rows = [payload]
    else:
        rows = []
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item.setdefault("game_id", game_id)
        out.append(item)
    return out


def _write_resource(path: Path, *, resource: ResourceName, payload: Any, game_id: str) -> None:
    if resource == "boxscore":
        data = (
            payload
            if isinstance(payload, dict)
            else {"rows": _rows_from_payload(payload, game_id=game_id, resource=resource)}
        )
        atomic_write_json(path, data)
    else:
        rows = _rows_from_payload(payload, game_id=game_id, resource=resource)
        atomic_write_jsonl(path, rows)


def _is_valid_mirror(path: Path, *, resource: ResourceName) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    try:
        if resource == "boxscore":
            payload = json.loads(path.read_text(encoding="utf-8"))
            return isinstance(payload, dict)
        lines = path.read_text(encoding="utf-8").splitlines()
        if not lines:
            return False
        for line in lines[:3]:
            row = json.loads(line)
            if not isinstance(row, dict):
                return False
        return True
    except (OSError, json.JSONDecodeError):
        return False


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        with_sign = raw[1:] if raw.startswith("-") else raw
        if with_sign.replace(".", "", 1).isdigit():
            return float(raw)
    return None


def _as_int(value: object, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if raw.startswith("-"):
            stripped = raw[1:]
            if stripped.isdigit():
                return int(raw)
        elif raw.isdigit():
            return int(raw)
    return default


def _parse_iso_duration_minutes(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if ":" in raw:
        parts = raw.split(":")
        if len(parts) == 2 and parts[0].isdigit():
            sec = _as_float(parts[1])
            if sec is None:
                return None
            return float(int(parts[0])) + sec / 60.0
        return None
    match = re.match(r"^PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$", raw)
    if not match:
        return _as_float(raw)
    minutes = float(match.group(1) or 0.0)
    seconds = float(match.group(2) or 0.0)
    return minutes + seconds / 60.0


def _request_json(*, url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
    response = requests.get(
        url,
        headers=headers,
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise NBADataError(f"unexpected non-object JSON payload from {url}")
    return payload


def _load_cdn_playbyplay(*, game_id: str) -> dict[str, Any]:
    return _request_json(
        url=f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
    )


def _load_cdn_boxscore(*, game_id: str) -> dict[str, Any]:
    return _request_json(
        url=f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    )


def _normalize_cdn_boxscore(payload: dict[str, Any]) -> dict[str, Any]:
    game = payload.get("game", {})
    if not isinstance(game, dict):
        return {"players": []}

    players_out: list[dict[str, Any]] = []
    for side in ("homeTeam", "awayTeam"):
        team_payload = game.get(side, {})
        if not isinstance(team_payload, dict):
            continue
        team_id = str(team_payload.get("teamId", "")).strip()
        team_players = team_payload.get("players", [])
        if not isinstance(team_players, list):
            continue
        for player in team_players:
            if not isinstance(player, dict):
                continue
            stats = player.get("statistics", {})
            if not isinstance(stats, dict):
                stats = {}
            minutes = _parse_iso_duration_minutes(
                stats.get("minutes", stats.get("minutesCalculated", ""))
            )
            players_out.append(
                {
                    "team_id": team_id,
                    "player_id": str(player.get("personId", "")).strip(),
                    "minutes": minutes,
                    "points": _as_float(stats.get("points", 0)),
                    "rebounds": _as_float(stats.get("reboundsTotal", stats.get("rebounds", 0))),
                    "assists": _as_float(stats.get("assists", 0)),
                }
            )
    return {"players": players_out}


def _extract_cdn_actions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    game = payload.get("game", {})
    if not isinstance(game, dict):
        return []
    actions = game.get("actions", [])
    if not isinstance(actions, list):
        return []
    return [action for action in actions if isinstance(action, dict)]


def _normalize_cdn_enhanced_pbp(*, actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, action in enumerate(actions, start=1):
        event_num = _as_int(action.get("actionNumber"), default=0)
        if event_num <= 0:
            event_num = _as_int(action.get("actionId"), default=index)
        team_id_int = _as_int(action.get("teamId"), default=0)
        person_id_int = _as_int(action.get("personId"), default=0)
        rows.append(
            {
                "event_num": event_num,
                "clock": str(action.get("clock", "")),
                "event_type": str(action.get("actionType", "")),
                "event_type_name": str(action.get("subType", "")),
                "team_id": str(team_id_int) if team_id_int > 0 else "",
                "player_id": str(person_id_int) if person_id_int > 0 else "",
                "description": str(action.get("description", "")),
            }
        )
    return rows


def _build_possessions_from_actions(*, actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    team_ids = {
        _as_int(action.get("teamId"), default=0)
        for action in actions
        if _as_int(action.get("teamId"), default=0) > 0
    }
    possession_rows: list[dict[str, Any]] = []
    current_team = 0
    current_start = 0
    current_end = 0

    for index, action in enumerate(actions, start=1):
        possession_team = _as_int(action.get("possession"), default=0)
        if possession_team <= 0:
            continue
        event_num = _as_int(action.get("actionNumber"), default=0)
        if event_num <= 0:
            event_num = _as_int(action.get("actionId"), default=index)
        if event_num <= 0:
            event_num = index
        if current_team == 0:
            current_team = possession_team
            current_start = event_num
            current_end = event_num
            continue
        if possession_team != current_team:
            other_team = ""
            if len(team_ids) == 2:
                other_team = str(next(team for team in team_ids if team != current_team))
            possession_rows.append(
                {
                    "possession_id": len(possession_rows) + 1,
                    "start_event_num": current_start,
                    "end_event_num": current_end,
                    "offense_team_id": str(current_team),
                    "defense_team_id": other_team,
                }
            )
            current_team = possession_team
            current_start = event_num
            current_end = event_num
        else:
            current_end = event_num

    if current_team > 0:
        other_team = ""
        if len(team_ids) == 2:
            other_team = str(next(team for team in team_ids if team != current_team))
        possession_rows.append(
            {
                "possession_id": len(possession_rows) + 1,
                "start_event_num": current_start,
                "end_event_num": current_end,
                "offense_team_id": str(current_team),
                "defense_team_id": other_team,
            }
        )
    return possession_rows


def _load_via_cdn_fallback(*, resource: ResourceName, game_id: str) -> Any:
    if resource == "boxscore":
        return _normalize_cdn_boxscore(_load_cdn_boxscore(game_id=game_id))
    payload = _load_cdn_playbyplay(game_id=game_id)
    actions = _extract_cdn_actions(payload)
    if resource == "enhanced_pbp":
        return _normalize_cdn_enhanced_pbp(actions=actions)
    if resource == "possessions":
        return _build_possessions_from_actions(actions=actions)
    raise NBADataError(f"unsupported resource for CDN fallback: {resource}")


def _load_via_source(
    *,
    layout: NBADataLayout,
    source: str,
    provider: str,
    resource: ResourceName,
    season: str,
    season_type: str,
    game_id: str,
) -> Any:
    provider_settings = {
        "Games": {"data_provider": provider},
        "Boxscore": {"data_provider": provider},
        "EnhancedPbp": {"data_provider": provider},
        "Possessions": {"data_provider": provider},
    }
    if source == "web":
        try:
            return _load_via_cdn_fallback(resource=resource, game_id=game_id)
        except Exception as cdn_exc:
            try:
                client = build_client(
                    response_dir=layout.pbpstats_response_dir,
                    source=source,
                    resource_settings=provider_settings,
                )
                return load_game_resource(client, game_id=game_id, resource=resource)
            except Exception as primary_exc:
                raise NBADataError(f"{cdn_exc}; pbpstats_web={primary_exc}") from primary_exc

    client = build_client(
        response_dir=layout.pbpstats_response_dir,
        source=source,
        resource_settings=provider_settings,
    )
    return load_game_resource(client, game_id=game_id, resource=resource)


def ingest_resources(
    *,
    layout: NBADataLayout,
    rows: dict[tuple[str, str, str], ManifestRow],
    season: str,
    season_type: str,
    resources: list[ResourceName],
    only_missing: bool,
    retry_errors: bool,
    max_games: int,
    rpm: int,
    providers: dict[ResourceName, list[str]],
    fail_fast: bool,
    lock_config: LockConfig,
) -> dict[str, int]:
    summary = {
        "ok": 0,
        "skipped": 0,
        "error": 0,
    }
    expected_season_type = slugify_season_type(season_type)
    limiter = RateLimiter(rpm=rpm, jitter_seconds=0.05)
    with lock_root(layout.root, config=lock_config):
        for row in rows.values():
            reconcile_ok_statuses(root=layout.root, row=row)
        targets = [
            rows[key]
            for key in sorted(rows)
            if rows[key]["season"] == season and rows[key]["season_type"] == expected_season_type
        ]
        if max_games > 0:
            targets = targets[:max_games]

        for row in targets:
            season_key = row["season"]
            season_type_key = row["season_type"]
            game_id = row["game_id"]
            for resource in resources:
                entry = row["resources"][resource]
                raw_path = layout.raw_resource_path(
                    resource=resource,
                    season=season_key,
                    season_type=season_type_key,
                    game_id=game_id,
                    ext=_resource_ext(resource),
                )
                if entry["status"] == "ok":
                    if _is_valid_mirror(raw_path, resource=resource):
                        summary["skipped"] += 1
                        continue
                    entry["status"] = "missing"
                    entry["error"] = "ok status reconciled to missing file"

                if entry["status"] == "error" and not retry_errors:
                    summary["skipped"] += 1
                    continue

                if (
                    only_missing
                    and entry["status"] != "missing"
                    and not (entry["status"] == "error" and retry_errors)
                ):
                    summary["skipped"] += 1
                    continue

                loaded = False
                last_error = ""
                providers_for_resource = providers.get(resource, ["data_nba", "stats_nba"])
                providers_for_resource = providers_for_resource or ["data_nba", "stats_nba"]

                for source in ("file", "web"):
                    for provider in providers_for_resource:
                        try:
                            if source == "web":
                                limiter.wait()
                            payload = _load_via_source(
                                layout=layout,
                                source=source,
                                provider=provider,
                                resource=resource,
                                season=season,
                                season_type=season_type,
                                game_id=game_id,
                            )
                            _write_resource(
                                raw_path, resource=resource, payload=payload, game_id=game_id
                            )
                            if not _is_valid_mirror(raw_path, resource=resource):
                                raise NBADataError("written mirror failed validation")
                            set_resource_ok(
                                root=layout.root,
                                row=row,
                                resource=resource,
                                provider=provider,
                                path=raw_path,
                            )
                            summary["ok"] += 1
                            loaded = True
                            break
                        except Exception as exc:
                            last_error = f"{source}:{provider}:{exc}"
                            continue
                    if loaded:
                        break

                if not loaded:
                    set_resource_error(
                        row=row,
                        resource=resource,
                        provider=",".join(providers_for_resource),
                        error=last_error or "unknown fetch error",
                    )
                    summary["error"] += 1
                    if fail_fast:
                        return summary
    return summary


def parse_resources(value: str) -> list[ResourceName]:
    raw = [item.strip() for item in value.split(",") if item.strip()]
    if not raw:
        return list(RESOURCE_NAMES)
    valid = set(RESOURCE_NAMES)
    out: list[ResourceName] = []
    for item in raw:
        if item not in valid:
            raise NBADataError(f"invalid resource: {item}")
        out.append(item)  # pyright: ignore[reportArgumentType]
    return out
