"""Per-game resource ingestion with resume + provider fallback."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

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
    client = build_client(
        response_dir=layout.pbpstats_response_dir,
        source=source,
        resource_settings={
            "Games": {"data_provider": provider},
            _resource_key(resource): {"data_provider": provider},
        },
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
