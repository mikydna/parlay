"""Snapshot artifact conversion and bundle helpers."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import polars as pl

from prop_ev.archive_utils import (
    compress_tar_zst,
    decompress_zst_to_tar,
    safe_extract_tar,
    sha256_file,
    write_tar,
)
from prop_ev.quote_table import (
    EVENT_PROPS_SORT_COLUMNS,
    EVENT_PROPS_TABLE,
    FEATURED_ODDS_SORT_COLUMNS,
    FEATURED_ODDS_TABLE,
    canonicalize_event_props_rows,
    canonicalize_featured_odds_rows,
    validate_event_props_rows,
    validate_featured_odds_rows,
)

_TABLE_SCHEMAS: dict[str, list[tuple[str, Any]]] = {
    EVENT_PROPS_TABLE: [
        ("provider", pl.Utf8),
        ("snapshot_id", pl.Utf8),
        ("schema_version", pl.Int64),
        ("event_id", pl.Utf8),
        ("market", pl.Utf8),
        ("player", pl.Utf8),
        ("side", pl.Utf8),
        ("price", pl.Float64),
        ("point", pl.Float64),
        ("book", pl.Utf8),
        ("last_update", pl.Utf8),
        ("link", pl.Utf8),
    ],
    FEATURED_ODDS_TABLE: [
        ("provider", pl.Utf8),
        ("snapshot_id", pl.Utf8),
        ("schema_version", pl.Int64),
        ("game_id", pl.Utf8),
        ("market", pl.Utf8),
        ("book", pl.Utf8),
        ("price", pl.Float64),
        ("point", pl.Float64),
        ("side", pl.Utf8),
        ("last_update", pl.Utf8),
    ],
}

_SORT_KEYS: dict[str, list[str]] = {
    EVENT_PROPS_TABLE: list(EVENT_PROPS_SORT_COLUMNS),
    FEATURED_ODDS_TABLE: list(FEATURED_ODDS_SORT_COLUMNS),
}


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        payload = json.loads(raw)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _enforce_schema(table_name: str, frame: pl.DataFrame) -> pl.DataFrame:
    schema = _TABLE_SCHEMAS[table_name]
    columns = [name for name, _ in schema]
    working = frame
    for name, dtype in schema:
        if name not in working.columns:
            working = working.with_columns(pl.lit(None).cast(dtype).alias(name))
        else:
            working = working.with_columns(pl.col(name).cast(dtype, strict=False))
    return working.select(columns)


def _generic_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    frame = pl.DataFrame(rows)
    if not frame.columns:
        return frame
    for column in frame.columns:
        if frame.schema[column] == pl.Null:
            frame = frame.with_columns(pl.col(column).cast(pl.Utf8))
    return frame.select(sorted(frame.columns))


def _canonical_rows_for_table(table_name: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if table_name == EVENT_PROPS_TABLE:
        canonical = canonicalize_event_props_rows(rows)
        validate_event_props_rows(canonical)
        return canonical
    if table_name == FEATURED_ODDS_TABLE:
        canonical = canonicalize_featured_odds_rows(rows)
        validate_featured_odds_rows(canonical)
        return canonical
    return rows


def _canonical_frame_for_table(table_name: str, rows: list[dict[str, Any]]) -> pl.DataFrame:
    frame = _enforce_schema(table_name, pl.DataFrame(rows))
    sort_keys = _SORT_KEYS.get(table_name, [])
    if sort_keys and frame.height > 0:
        frame = frame.sort(sort_keys)
    return frame


def verify_snapshot_derived_contracts(
    *,
    snapshot_dir: Path,
    require_parquet: bool = False,
    required_tables: tuple[str, ...] = (),
) -> list[dict[str, str]]:
    """Verify canonical derived-table contracts and jsonl/parquet parity."""
    derived_dir = snapshot_dir / "derived"
    if not derived_dir.exists():
        return [
            {
                "code": "missing_derived_dir",
                "detail": derived_dir.as_posix(),
            }
        ]

    issues: list[dict[str, str]] = []
    known_tables = sorted(_TABLE_SCHEMAS.keys())
    present_jsonl_tables = {
        path.stem for path in derived_dir.glob("*.jsonl") if path.stem in _TABLE_SCHEMAS
    }

    for table_name in required_tables:
        if table_name not in present_jsonl_tables:
            issues.append(
                {
                    "code": "missing_required_jsonl",
                    "table": table_name,
                    "detail": (derived_dir / f"{table_name}.jsonl").as_posix(),
                }
            )

    for table_name in known_tables:
        jsonl_path = derived_dir / f"{table_name}.jsonl"
        parquet_path = derived_dir / f"{table_name}.parquet"
        if not jsonl_path.exists():
            if require_parquet and parquet_path.exists():
                issues.append(
                    {
                        "code": "parquet_without_jsonl",
                        "table": table_name,
                        "detail": parquet_path.as_posix(),
                    }
                )
            continue

        try:
            rows = _load_jsonl(jsonl_path)
        except json.JSONDecodeError as exc:
            issues.append(
                {
                    "code": "invalid_jsonl",
                    "table": table_name,
                    "detail": f"{jsonl_path}: {exc}",
                }
            )
            continue

        try:
            canonical_rows = _canonical_rows_for_table(table_name, rows)
        except ValueError as exc:
            issues.append(
                {
                    "code": "contract_validation_failed",
                    "table": table_name,
                    "detail": str(exc),
                }
            )
            continue

        if rows != canonical_rows:
            issues.append(
                {
                    "code": "jsonl_noncanonical",
                    "table": table_name,
                    "detail": jsonl_path.as_posix(),
                }
            )

        if not parquet_path.exists():
            if require_parquet:
                issues.append(
                    {
                        "code": "missing_required_parquet",
                        "table": table_name,
                        "detail": parquet_path.as_posix(),
                    }
                )
            continue

        parquet_frame = pl.read_parquet(parquet_path)
        expected_frame = _canonical_frame_for_table(table_name, canonical_rows)
        actual_frame = _canonical_frame_for_table(table_name, parquet_frame.to_dicts())
        if expected_frame.to_dicts() != actual_frame.to_dicts():
            issues.append(
                {
                    "code": "parquet_parity_mismatch",
                    "table": table_name,
                    "detail": parquet_path.as_posix(),
                }
            )

    return issues


def lake_snapshot_derived(snapshot_dir: Path) -> list[Path]:
    """Convert all snapshot derived JSONL files into deterministic Parquet outputs."""
    derived_dir = snapshot_dir / "derived"
    if not derived_dir.exists():
        raise FileNotFoundError(f"missing derived directory: {derived_dir}")

    written: list[Path] = []
    for jsonl_path in sorted(derived_dir.glob("*.jsonl")):
        rows = _load_jsonl(jsonl_path)
        table_name = jsonl_path.stem
        if table_name in _TABLE_SCHEMAS:
            if table_name == EVENT_PROPS_TABLE:
                rows = canonicalize_event_props_rows(rows)
                validate_event_props_rows(rows)
            elif table_name == FEATURED_ODDS_TABLE:
                rows = canonicalize_featured_odds_rows(rows)
                validate_featured_odds_rows(rows)
            frame = _enforce_schema(table_name, pl.DataFrame(rows))
            sort_keys = _SORT_KEYS.get(table_name, [])
            if sort_keys and frame.height > 0:
                frame = frame.sort(sort_keys)
        else:
            frame = _generic_frame(rows)
            if frame.columns and frame.height > 0:
                frame = frame.sort(frame.columns)

        output_path = jsonl_path.with_suffix(".parquet")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(output_path, compression="zstd")
        written.append(output_path)
    return written


def _snapshot_bundle_default(data_root: Path, snapshot_id: str) -> Path:
    return data_root / "bundles" / "snapshots" / f"{snapshot_id}.tar.zst"


def _bundle_metadata_path(bundle_path: Path) -> Path:
    filename = bundle_path.name
    stem = filename[: -len(".tar.zst")] if filename.endswith(".tar.zst") else bundle_path.stem
    return bundle_path.with_name(f"{stem}.bundle.json")


def _snapshot_files(snapshot_dir: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(snapshot_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.name == ".lock":
            continue
        files.append(path)
    return files


def pack_snapshot(
    *, data_root: Path, snapshot_id: str, out_path: Path | None = None
) -> tuple[Path, Path]:
    """Pack one snapshot into `tar.zst` plus sidecar metadata."""
    snapshot_dir = data_root / "snapshots" / snapshot_id
    if not snapshot_dir.exists():
        raise FileNotFoundError(f"snapshot not found: {snapshot_dir}")
    files = _snapshot_files(snapshot_dir)
    if not files:
        raise ValueError(f"snapshot has no packable files: {snapshot_id}")

    bundle_path = (
        out_path if out_path is not None else _snapshot_bundle_default(data_root, snapshot_id)
    )
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path = _bundle_metadata_path(bundle_path)

    with TemporaryDirectory(prefix=f"{snapshot_id}-bundle-") as tmp_dir:
        tmp_tar = Path(tmp_dir) / f"{snapshot_id}.tar"
        write_tar(tar_path=tmp_tar, root=data_root, files=files)
        compress_tar_zst(tar_path=tmp_tar, out_path=bundle_path, level=19)

    file_list = [path.relative_to(data_root).as_posix() for path in files]
    metadata = {
        "snapshot_id": snapshot_id,
        "created_at_utc": _now_utc(),
        "bundle_path": bundle_path.as_posix(),
        "sha256": sha256_file(bundle_path),
        "bytes": bundle_path.stat().st_size,
        "file_count": len(file_list),
        "files": file_list,
    }
    sidecar_path.write_text(json.dumps(metadata, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return bundle_path, sidecar_path


def unpack_snapshot(*, data_root: Path, bundle_path: Path) -> dict[str, Any]:
    """Unpack a snapshot bundle into the target data root."""
    if not bundle_path.exists():
        raise FileNotFoundError(f"bundle not found: {bundle_path}")

    with TemporaryDirectory(prefix="snapshot-unpack-") as tmp_dir:
        tmp_tar = Path(tmp_dir) / "snapshot.tar"
        decompress_zst_to_tar(zst_path=bundle_path, tar_path=tmp_tar)
        extracted_names = safe_extract_tar(tar_path=tmp_tar, destination=data_root)

    snapshot_ids = sorted(
        {
            Path(name).parts[1]
            for name in extracted_names
            if len(Path(name).parts) >= 2 and Path(name).parts[0] == "snapshots"
        }
    )
    return {
        "bundle_path": bundle_path.as_posix(),
        "data_root": data_root.as_posix(),
        "files_extracted": len(extracted_names),
        "snapshot_ids": snapshot_ids,
    }
