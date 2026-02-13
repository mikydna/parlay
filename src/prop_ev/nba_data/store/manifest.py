"""Resumable per-game manifest helpers for nba-data ingestion."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, TypedDict, cast

from prop_ev.nba_data.io_utils import atomic_write_jsonl, sha256_file
from prop_ev.nba_data.schema_version import SCHEMA_VERSION
from prop_ev.nba_data.store.layout import slugify_season_type

ResourceStatus = Literal["missing", "ok", "error"]
ResourceName = Literal["boxscore", "enhanced_pbp", "possessions"]
RESOURCE_NAMES: tuple[ResourceName, ...] = ("boxscore", "enhanced_pbp", "possessions")


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class ResourceRow(TypedDict):
    status: ResourceStatus
    provider: str
    path: str
    sha256: str
    bytes: int
    error: str
    updated_at: str


class ManifestRow(TypedDict):
    season: str
    season_type: str
    game_id: str
    discovered_at: str
    last_updated_at: str
    schedule_path: str
    resources: dict[ResourceName, ResourceRow]
    schema_version: int


def _resource_default() -> ResourceRow:
    return {
        "status": "missing",
        "provider": "",
        "path": "",
        "sha256": "",
        "bytes": 0,
        "error": "",
        "updated_at": "",
    }


def _key(row: ManifestRow) -> tuple[str, str, str]:
    return (row["season"], row["season_type"], row["game_id"])


def _normalize_row(payload: dict[str, Any]) -> ManifestRow:
    season = str(payload.get("season", ""))
    season_type = str(payload.get("season_type", ""))
    game_id = str(payload.get("game_id", ""))
    resources_payload = payload.get("resources", {})
    resources: dict[ResourceName, ResourceRow] = {}
    for name in RESOURCE_NAMES:
        value = resources_payload.get(name, {}) if isinstance(resources_payload, dict) else {}
        if not isinstance(value, dict):
            value = {}
        raw_status = str(value.get("status", "missing"))
        status = (
            cast(ResourceStatus, raw_status)
            if raw_status in {"missing", "ok", "error"}
            else "missing"
        )
        resources[name] = {
            "status": status,
            "provider": str(value.get("provider", "")),
            "path": str(value.get("path", "")),
            "sha256": str(value.get("sha256", "")),
            "bytes": int(value.get("bytes", 0) or 0),
            "error": str(value.get("error", "")),
            "updated_at": str(value.get("updated_at", "")),
        }
    discovered_at = str(payload.get("discovered_at", "")) or _now_utc()
    return {
        "season": season,
        "season_type": season_type,
        "game_id": game_id,
        "discovered_at": discovered_at,
        "last_updated_at": str(payload.get("last_updated_at", "")) or discovered_at,
        "schedule_path": str(payload.get("schedule_path", "")),
        "resources": resources,
        "schema_version": int(payload.get("schema_version", SCHEMA_VERSION)),
    }


def load_manifest(path: Path) -> dict[tuple[str, str, str], ManifestRow]:
    if not path.exists():
        return {}
    rows: dict[tuple[str, str, str], ManifestRow] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            continue
        row = _normalize_row(payload)
        rows[_key(row)] = row
    return rows


def write_manifest_deterministic(path: Path, rows: dict[tuple[str, str, str], ManifestRow]) -> None:
    ordered = [rows[key] for key in sorted(rows)]
    atomic_write_jsonl(path, ordered)


def ensure_row(
    rows: dict[tuple[str, str, str], ManifestRow],
    *,
    season: str,
    season_type: str,
    game_id: str,
) -> ManifestRow:
    normalized_type = slugify_season_type(season_type)
    key = (season, normalized_type, game_id)
    if key not in rows:
        now = _now_utc()
        rows[key] = {
            "season": season,
            "season_type": normalized_type,
            "game_id": game_id,
            "discovered_at": now,
            "last_updated_at": now,
            "schedule_path": "",
            "resources": {name: _resource_default() for name in RESOURCE_NAMES},
            "schema_version": SCHEMA_VERSION,
        }
    return rows[key]


def set_resource_ok(
    *,
    root: Path,
    row: ManifestRow,
    resource: ResourceName,
    provider: str,
    path: Path,
) -> None:
    relative_path = path.relative_to(root).as_posix()
    entry = row["resources"][resource]
    entry["status"] = "ok"
    entry["provider"] = provider
    entry["path"] = relative_path
    entry["sha256"] = sha256_file(path)
    entry["bytes"] = path.stat().st_size
    entry["error"] = ""
    entry["updated_at"] = _now_utc()
    row["last_updated_at"] = entry["updated_at"]


def set_resource_error(
    *,
    row: ManifestRow,
    resource: ResourceName,
    error: str,
    provider: str = "",
) -> None:
    entry = row["resources"][resource]
    entry["status"] = "error"
    entry["provider"] = provider
    entry["error"] = error[:500]
    entry["updated_at"] = _now_utc()
    row["last_updated_at"] = entry["updated_at"]


def set_schedule_path(*, root: Path, row: ManifestRow, schedule_path: Path) -> None:
    row["schedule_path"] = schedule_path.relative_to(root).as_posix()
    row["last_updated_at"] = _now_utc()


def reconcile_ok_statuses(*, root: Path, row: ManifestRow) -> None:
    for name in RESOURCE_NAMES:
        entry = row["resources"][name]
        if entry["status"] != "ok":
            continue
        path = entry.get("path", "")
        if not path:
            entry["status"] = "missing"
            entry["error"] = "missing file path"
            continue
        abs_path = root / path
        if not abs_path.exists() or abs_path.stat().st_size <= 0:
            entry["status"] = "missing"
            entry["error"] = "missing or empty file"


def pending_games(
    rows: dict[tuple[str, str, str], ManifestRow],
    *,
    resources: list[ResourceName],
    only_missing: bool,
    include_errors: bool,
) -> list[ManifestRow]:
    pending: list[ManifestRow] = []
    for key in sorted(rows):
        row = rows[key]
        needs_work = False
        for resource in resources:
            status = row["resources"][resource]["status"]
            if status == "missing":
                needs_work = True
                break
            if status == "error" and include_errors:
                needs_work = True
                break
            if not only_missing and status != "ok":
                needs_work = True
                break
        if needs_work:
            pending.append(row)
    return pending
