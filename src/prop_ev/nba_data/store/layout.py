"""Filesystem layout helpers for nba-data module."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from prop_ev.nba_data.schema_version import SCHEMA_VERSION


def slugify_season_type(value: str) -> str:
    """Return stable filesystem-safe season type."""
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower())
    cleaned = cleaned.strip("_")
    return cleaned or "unknown"


@dataclass(frozen=True)
class NBADataLayout:
    """Resolved canonical paths for nba-data storage."""

    root: Path
    pbpstats_response_dir: Path
    raw_dir: Path
    clean_dir: Path
    manifests_dir: Path
    reports_dir: Path

    def manifest_path(self, *, season: str, season_type: str) -> Path:
        return (
            self.manifests_dir
            / f"season={season}"
            / f"season_type={slugify_season_type(season_type)}"
            / "manifest.jsonl"
        )

    def schedule_path(self, *, season: str, season_type: str) -> Path:
        return (
            self.raw_dir
            / "schedule"
            / f"season={season}"
            / f"season_type={slugify_season_type(season_type)}"
            / "schedule.json"
        )

    def raw_resource_path(
        self,
        *,
        resource: str,
        season: str,
        season_type: str,
        game_id: str,
        ext: str,
    ) -> Path:
        return (
            self.raw_dir
            / resource
            / f"season={season}"
            / f"season_type={slugify_season_type(season_type)}"
            / f"game_id={game_id}"
            / f"{resource}.{ext}"
        )

    def clean_schema_dir(self, schema_version: int = SCHEMA_VERSION) -> Path:
        return self.clean_dir / f"schema_v{int(schema_version)}"

    def verify_report_path(
        self,
        *,
        season: str,
        season_type: str,
        schema_version: int = SCHEMA_VERSION,
    ) -> Path:
        return (
            self.reports_dir
            / "verify"
            / f"schema_v{int(schema_version)}"
            / f"season={season}"
            / f"season_type={slugify_season_type(season_type)}"
            / "verify.json"
        )


def build_layout(root: Path) -> NBADataLayout:
    root = root.resolve()
    pbpstats_response_dir = root / "response_data"
    for subdir in ("game_details", "overrides", "pbp", "schedule"):
        (pbpstats_response_dir / subdir).mkdir(parents=True, exist_ok=True)
    raw_dir = root / "raw"
    clean_dir = root / "clean"
    manifests_dir = root / "manifests"
    reports_dir = root / "reports"
    for path in (raw_dir, clean_dir, manifests_dir, reports_dir):
        path.mkdir(parents=True, exist_ok=True)
    return NBADataLayout(
        root=root,
        pbpstats_response_dir=pbpstats_response_dir,
        raw_dir=raw_dir,
        clean_dir=clean_dir,
        manifests_dir=manifests_dir,
        reports_dir=reports_dir,
    )
