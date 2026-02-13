"""Build deterministic clean parquet datasets from raw mirrors."""

from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from typing import Any

import polars as pl

from prop_ev.nba_data.clean.schemas import TABLE_SORT_KEYS, enforce_schema
from prop_ev.nba_data.schema_version import SCHEMA_VERSION
from prop_ev.nba_data.store.layout import NBADataLayout
from prop_ev.nba_data.store.manifest import RESOURCE_NAMES, load_manifest


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _extract_boxscore_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = []
    for key in ("players", "player_stats", "rows"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates = [row for row in value if isinstance(row, dict)]
            if candidates:
                break
    if not candidates:
        candidates = [payload]
    return candidates


def _normalize_game_row(
    *,
    season: str,
    season_type: str,
    game_id: str,
    schedule_row: dict[str, Any],
) -> dict[str, Any]:
    return {
        "season": season,
        "season_type": season_type,
        "game_id": game_id,
        "date": str(schedule_row.get("date", "")),
        "home_team_id": str(schedule_row.get("home_team_id", "")),
        "away_team_id": str(schedule_row.get("away_team_id", "")),
    }


def _write_partitioned_table(
    base_dir: Path, table: str, frame: pl.DataFrame, overwrite: bool
) -> None:
    table_root = base_dir / table
    if table_root.exists() and not overwrite:
        return
    tmp_root = table_root.with_name(f"{table}.tmp-{uuid.uuid4().hex}")
    if tmp_root.exists():
        shutil.rmtree(tmp_root, ignore_errors=True)
    tmp_root.mkdir(parents=True, exist_ok=True)

    grouped = frame.partition_by(["season", "season_type"], as_dict=True, maintain_order=True)
    for key, part in grouped.items():
        if not isinstance(key, tuple) or len(key) != 2:
            continue
        season, season_type = key
        out_dir = tmp_root / f"season={season}" / f"season_type={season_type}"
        out_dir.mkdir(parents=True, exist_ok=True)
        part.write_parquet(out_dir / "part-00000.parquet")

    if table_root.exists():
        shutil.rmtree(table_root, ignore_errors=True)
    tmp_root.rename(table_root)


def build_clean(
    *,
    layout: NBADataLayout,
    seasons: list[str],
    season_type: str,
    overwrite: bool,
    schema_version: int,
) -> dict[str, int]:
    schema_version = int(schema_version)
    target_version = SCHEMA_VERSION if schema_version <= 0 else schema_version
    out_dir = layout.clean_schema_dir(schema_version=target_version)
    out_dir.mkdir(parents=True, exist_ok=True)

    games_rows: list[dict[str, Any]] = []
    boxscore_rows: list[dict[str, Any]] = []
    pbp_rows: list[dict[str, Any]] = []
    possessions_rows: list[dict[str, Any]] = []

    for season in seasons:
        manifest_path = layout.manifest_path(season=season, season_type=season_type)
        manifest = load_manifest(manifest_path)
        schedule_path = layout.schedule_path(season=season, season_type=season_type)
        schedule_payload = _read_json(schedule_path) if schedule_path.exists() else {"games": []}
        schedule_rows = (
            schedule_payload.get("games", [])
            if isinstance(schedule_payload.get("games"), list)
            else []
        )
        schedule_by_game_id = {
            str(item.get("game_id", "")): item for item in schedule_rows if isinstance(item, dict)
        }

        for row in manifest.values():
            game_id = row["game_id"]
            if any(row["resources"][name]["status"] != "ok" for name in RESOURCE_NAMES):
                continue
            games_rows.append(
                _normalize_game_row(
                    season=row["season"],
                    season_type=row["season_type"],
                    game_id=game_id,
                    schedule_row=schedule_by_game_id.get(game_id, {}),
                )
            )

            box_path = layout.root / row["resources"]["boxscore"]["path"]
            if box_path.exists():
                for record in _extract_boxscore_rows(_read_json(box_path)):
                    boxscore_rows.append(
                        {
                            "season": row["season"],
                            "season_type": row["season_type"],
                            "game_id": game_id,
                            "team_id": str(record.get("team_id", "")),
                            "player_id": str(
                                record.get("player_id", "") or record.get("person_id", "")
                            ),
                            "minutes": record.get("minutes"),
                            "points": record.get("points"),
                            "rebounds": record.get("rebounds"),
                            "assists": record.get("assists"),
                        }
                    )

            pbp_path = layout.root / row["resources"]["enhanced_pbp"]["path"]
            if pbp_path.exists():
                for record in _read_jsonl(pbp_path):
                    pbp_rows.append(
                        {
                            "season": row["season"],
                            "season_type": row["season_type"],
                            "game_id": game_id,
                            "event_num": record.get("event_num") or record.get("eventnum"),
                            "clock": str(record.get("clock", "") or record.get("game_clock", "")),
                            "event_type": str(
                                record.get("event_type", "") or record.get("event_type_name", "")
                            ),
                            "team_id": str(record.get("team_id", "")),
                            "player_id": str(record.get("player_id", "")),
                            "description": str(
                                record.get("description", "") or record.get("text", "")
                            ),
                        }
                    )

            possession_path = layout.root / row["resources"]["possessions"]["path"]
            if possession_path.exists():
                for record in _read_jsonl(possession_path):
                    possessions_rows.append(
                        {
                            "season": row["season"],
                            "season_type": row["season_type"],
                            "game_id": game_id,
                            "possession_id": record.get("possession_id") or record.get("id"),
                            "start_event_num": record.get("start_event_num"),
                            "end_event_num": record.get("end_event_num"),
                            "offense_team_id": str(record.get("offense_team_id", "")),
                            "defense_team_id": str(record.get("defense_team_id", "")),
                        }
                    )

    frames = {
        "games": pl.DataFrame(games_rows),
        "boxscore_players": pl.DataFrame(boxscore_rows),
        "pbp_events": pl.DataFrame(pbp_rows),
        "possessions": pl.DataFrame(possessions_rows),
    }
    out_counts: dict[str, int] = {}
    for table, frame in frames.items():
        normalized = enforce_schema(table, frame)
        sort_keys = TABLE_SORT_KEYS[table]
        normalized = normalized.sort(sort_keys)
        _write_partitioned_table(out_dir, table, normalized, overwrite=overwrite)
        out_counts[table] = normalized.height
    return out_counts
