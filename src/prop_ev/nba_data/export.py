"""Export helpers for nba-data migration workflows."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from shutil import copy2
from tempfile import TemporaryDirectory
from typing import Any

from prop_ev.archive_utils import compress_tar_zst, sha256_file, write_tar
from prop_ev.nba_data.io_utils import atomic_write_json
from prop_ev.nba_data.store.layout import NBADataLayout, slugify_season_type

_CLEAN_TABLES: tuple[str, ...] = ("games", "boxscore_players", "pbp_events", "possessions")
_RAW_RESOURCES: tuple[str, ...] = ("boxscore", "enhanced_pbp", "possessions")


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iter_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*") if path.is_file())


def _copy_file(*, src: Path, dst: Path, overwrite: bool) -> bool:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and not overwrite:
        return False
    copy2(src, dst)
    return True


def _missing_error(label: str, missing: list[Path]) -> FileNotFoundError:
    sample = ", ".join(path.as_posix() for path in missing[:3])
    suffix = "" if len(missing) <= 3 else f" (+{len(missing) - 3} more)"
    return FileNotFoundError(f"{label}: missing required source paths: {sample}{suffix}")


def export_clean_artifacts(
    *,
    src_layout: NBADataLayout,
    dst_layout: NBADataLayout,
    seasons: list[str],
    season_type: str,
    schema_version: int,
    overwrite: bool,
) -> dict[str, Any]:
    season_slug = slugify_season_type(season_type)
    source_paths: list[Path] = []
    missing: list[Path] = []
    schema_dir = src_layout.clean_schema_dir(schema_version)

    for table_name in _CLEAN_TABLES:
        table_dir = schema_dir / table_name
        for season in seasons:
            partition_dir = table_dir / f"season={season}" / f"season_type={season_slug}"
            if not partition_dir.exists():
                missing.append(partition_dir)
                continue
            source_paths.extend(_iter_files(partition_dir))

    for season in seasons:
        manifest_path = src_layout.manifest_path(season=season, season_type=season_type)
        schedule_path = src_layout.schedule_path(season=season, season_type=season_type)
        for path in (manifest_path, schedule_path):
            if path.exists():
                source_paths.append(path)
            else:
                missing.append(path)

    verify_dir = src_layout.reports_dir / "verify" / f"schema_v{int(schema_version)}"
    if verify_dir.exists():
        source_paths.extend(_iter_files(verify_dir))
    else:
        missing.append(verify_dir)

    if missing:
        raise _missing_error("export clean", missing)

    copied = 0
    skipped = 0
    for src_path in sorted(dict.fromkeys(source_paths)):
        dst_path = dst_layout.root / src_path.relative_to(src_layout.root)
        if _copy_file(src=src_path, dst=dst_path, overwrite=overwrite):
            copied += 1
        else:
            skipped += 1

    return {
        "seasons": seasons,
        "season_type": season_slug,
        "schema_version": int(schema_version),
        "copied_files": copied,
        "skipped_files": skipped,
    }


def _archive_input_files(*, layout: NBADataLayout, season: str, season_type: str) -> list[Path]:
    season_slug = slugify_season_type(season_type)
    files: list[Path] = []
    missing: list[Path] = []
    for resource in _RAW_RESOURCES:
        resource_dir = layout.raw_dir / resource / f"season={season}" / f"season_type={season_slug}"
        if not resource_dir.exists():
            missing.append(resource_dir)
            continue
        files.extend(_iter_files(resource_dir))

    manifest_path = layout.manifest_path(season=season, season_type=season_type)
    schedule_path = layout.schedule_path(season=season, season_type=season_type)
    for path in (manifest_path, schedule_path):
        if path.exists():
            files.append(path)
        else:
            missing.append(path)

    if missing:
        raise _missing_error(f"export raw-archive season={season}", missing)
    return sorted(dict.fromkeys(files))


def export_raw_archives(
    *,
    src_layout: NBADataLayout,
    dst_layout: NBADataLayout,
    seasons: list[str],
    season_type: str,
    compression_level: int,
    overwrite: bool,
) -> dict[str, Any]:
    season_slug = slugify_season_type(season_type)
    archive_records: list[dict[str, Any]] = []
    for season in seasons:
        source_files = _archive_input_files(
            layout=src_layout, season=season, season_type=season_type
        )
        archive_path = (
            dst_layout.root
            / "raw_archives"
            / f"season={season}"
            / f"season_type={season_slug}"
            / "raw.tar.zst"
        )
        if archive_path.exists() and not overwrite:
            bytes_size = archive_path.stat().st_size
            archive_records.append(
                {
                    "season": season,
                    "season_type": season_slug,
                    "archive_path": archive_path.relative_to(dst_layout.root).as_posix(),
                    "sha256": sha256_file(archive_path),
                    "bytes": bytes_size,
                    "created_at_utc": _now_utc(),
                }
            )
            continue

        with TemporaryDirectory(prefix=f"raw-archive-{season}-") as tmp_dir:
            tar_path = Path(tmp_dir) / "raw.tar"
            write_tar(tar_path=tar_path, root=src_layout.root, files=source_files)
            compress_tar_zst(tar_path=tar_path, out_path=archive_path, level=compression_level)

        archive_records.append(
            {
                "season": season,
                "season_type": season_slug,
                "archive_path": archive_path.relative_to(dst_layout.root).as_posix(),
                "sha256": sha256_file(archive_path),
                "bytes": archive_path.stat().st_size,
                "created_at_utc": _now_utc(),
            }
        )

    manifest = {
        "schema_version": 1,
        "generated_at_utc": _now_utc(),
        "archives": sorted(archive_records, key=lambda row: str(row.get("archive_path", ""))),
    }
    manifest_path = dst_layout.root / "raw_archives" / "manifest.json"
    atomic_write_json(manifest_path, manifest)
    return {
        "seasons": seasons,
        "season_type": season_slug,
        "archives": len(archive_records),
        "manifest_path": manifest_path.as_posix(),
    }
