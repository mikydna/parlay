"""Split-table denormalized Parquet exports for odds dataset day indexes."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from prop_ev.data_paths import data_home_from_odds_root
from prop_ev.time_utils import utc_now_str

TABLE_FACT_OUTCOMES = "fact_outcomes"
TABLE_DIM_REQUEST = "dim_request"
TABLE_DIM_DAY_STATUS = "dim_day_status"

_FACT_OUTCOMES_SCHEMA: list[tuple[str, Any]] = [
    ("dataset_id", pl.Utf8),
    ("day", pl.Utf8),
    ("snapshot_id", pl.Utf8),
    ("event_id", pl.Utf8),
    ("request_key", pl.Utf8),
    ("market", pl.Utf8),
    ("player", pl.Utf8),
    ("side", pl.Utf8),
    ("point", pl.Float64),
    ("book", pl.Utf8),
    ("price", pl.Float64),
    ("last_update", pl.Utf8),
    ("link", pl.Utf8),
]

_DIM_REQUEST_SCHEMA: list[tuple[str, Any]] = [
    ("day", pl.Utf8),
    ("snapshot_id", pl.Utf8),
    ("request_key", pl.Utf8),
    ("request_label", pl.Utf8),
    ("request_path", pl.Utf8),
    ("request_status", pl.Utf8),
    ("updated_at_utc", pl.Utf8),
    ("param_bookmakers", pl.Utf8),
    ("param_regions", pl.Utf8),
    ("param_markets", pl.Utf8),
    ("param_date", pl.Utf8),
    ("param_odds_format", pl.Utf8),
    ("param_date_format", pl.Utf8),
]

_DIM_DAY_STATUS_SCHEMA: list[tuple[str, Any]] = [
    ("dataset_id", pl.Utf8),
    ("day", pl.Utf8),
    ("snapshot_id", pl.Utf8),
    ("day_complete", pl.Boolean),
    ("day_odds_coverage_ratio", pl.Float64),
    ("day_missing_count", pl.Int64),
    ("day_total_events", pl.Int64),
    ("day_reason_code", pl.Utf8),
    ("events_timestamp", pl.Utf8),
    ("updated_at_utc", pl.Utf8),
]


@dataclass(frozen=True)
class _DayPlan:
    day: str
    snapshot_id: str
    status: dict[str, Any]
    snapshot_dir: Path
    event_props_path: Path
    manifest_path: Path


def default_export_root(data_root: Path | str) -> Path:
    odds_root = Path(data_root).expanduser().resolve()
    return data_home_from_odds_root(odds_root) / "exports" / "odds" / "export_denorm" / "v1"


def export_dataset_denorm(
    *,
    data_root: Path | str,
    dataset_id_value: str,
    days: list[str],
    out_root: Path | str | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    odds_root = Path(data_root).expanduser().resolve()
    target_root = (
        Path(out_root).expanduser().resolve()
        if out_root is not None and str(out_root).strip()
        else default_export_root(odds_root)
    )

    warnings: list[dict[str, str]] = []
    skipped_incomplete_days = 0
    skipped_missing_status_days = 0
    skipped_missing_snapshot_days = 0
    skipped_missing_event_props_days = 0

    day_plans: list[_DayPlan] = []
    for day in days:
        status_path = odds_root / "datasets" / dataset_id_value / "days" / f"{day}.json"
        if not status_path.exists():
            skipped_missing_status_days += 1
            warnings.append(
                _warning(
                    code="missing_day_status",
                    day=day,
                    detail=status_path.as_posix(),
                )
            )
            continue
        try:
            status = _load_json_dict(status_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            skipped_missing_status_days += 1
            warnings.append(
                _warning(
                    code="invalid_day_status",
                    day=day,
                    detail=f"{status_path.as_posix()}: {exc}",
                )
            )
            continue
        if not bool(status.get("complete", False)):
            skipped_incomplete_days += 1
            continue

        snapshot_id = _text(status.get("snapshot_id_for_day", ""))
        if not snapshot_id:
            skipped_missing_snapshot_days += 1
            warnings.append(
                _warning(
                    code="missing_snapshot_id",
                    day=day,
                    detail=status_path.as_posix(),
                )
            )
            continue
        snapshot_dir = odds_root / "snapshots" / snapshot_id
        if not snapshot_dir.exists():
            skipped_missing_snapshot_days += 1
            warnings.append(
                _warning(
                    code="missing_snapshot_dir",
                    day=day,
                    detail=snapshot_dir.as_posix(),
                )
            )
            continue
        event_props_path = snapshot_dir / "derived" / "event_props.jsonl"
        if not event_props_path.exists():
            skipped_missing_event_props_days += 1
            warnings.append(
                _warning(
                    code="missing_event_props_jsonl",
                    day=day,
                    detail=event_props_path.as_posix(),
                )
            )
            continue
        day_plans.append(
            _DayPlan(
                day=day,
                snapshot_id=snapshot_id,
                status=status,
                snapshot_dir=snapshot_dir,
                event_props_path=event_props_path,
                manifest_path=snapshot_dir / "manifest.json",
            )
        )

    fact_rows_written = 0
    dim_request_rows_written = 0
    dim_day_rows_written = 0
    fact_partitions_written = 0
    dim_request_partitions_written = 0
    dim_day_partitions_written = 0
    exported_days = 0

    for plan in day_plans:
        try:
            event_props_rows = _load_jsonl_rows(plan.event_props_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            skipped_missing_event_props_days += 1
            warnings.append(
                _warning(
                    code="invalid_event_props_jsonl",
                    day=plan.day,
                    detail=f"{plan.event_props_path.as_posix()}: {exc}",
                )
            )
            continue

        requests_by_key: dict[str, dict[str, Any]] = {}
        if plan.manifest_path.exists():
            try:
                manifest_payload = _load_json_dict(plan.manifest_path)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                warnings.append(
                    _warning(
                        code="invalid_manifest",
                        day=plan.day,
                        detail=f"{plan.manifest_path.as_posix()}: {exc}",
                    )
                )
                manifest_payload = {}
            requests_value = manifest_payload.get("requests", {})
            if isinstance(requests_value, dict):
                requests_by_key = {
                    str(key): row
                    for key, row in requests_value.items()
                    if isinstance(key, str) and isinstance(row, dict)
                }
            elif requests_value:
                warnings.append(
                    _warning(
                        code="invalid_manifest_requests",
                        day=plan.day,
                        detail=plan.manifest_path.as_posix(),
                    )
                )
        else:
            warnings.append(
                _warning(
                    code="missing_manifest",
                    day=plan.day,
                    detail=plan.manifest_path.as_posix(),
                )
            )

        event_request_keys = _event_request_key_by_event(requests_by_key)
        fact_rows = _build_fact_rows(
            dataset_id_value=dataset_id_value,
            day=plan.day,
            snapshot_id=plan.snapshot_id,
            event_props_rows=event_props_rows,
            event_request_keys=event_request_keys,
        )
        request_rows = _build_request_rows(
            day=plan.day,
            snapshot_id=plan.snapshot_id,
            requests_by_key=requests_by_key,
        )
        day_status_rows = [
            {
                "dataset_id": dataset_id_value,
                "day": plan.day,
                "snapshot_id": plan.snapshot_id,
                "day_complete": bool(plan.status.get("complete", False)),
                "day_odds_coverage_ratio": _float_or_none(plan.status.get("odds_coverage_ratio")),
                "day_missing_count": _int_or_zero(plan.status.get("missing_count")),
                "day_total_events": _int_or_zero(plan.status.get("total_events")),
                "day_reason_code": _day_reason_code(plan.status),
                "events_timestamp": _text(plan.status.get("events_timestamp", "")),
                "updated_at_utc": _text(plan.status.get("updated_at_utc", "")),
            }
        ]

        fact_frame = _frame_for_schema(fact_rows, _FACT_OUTCOMES_SCHEMA)
        request_frame = _frame_for_schema(request_rows, _DIM_REQUEST_SCHEMA)
        day_status_frame = _frame_for_schema(day_status_rows, _DIM_DAY_STATUS_SCHEMA)

        _check_day_output_conflicts(
            target_root=target_root,
            day=plan.day,
            overwrite=overwrite,
        )
        _write_partition(
            table_root=target_root / TABLE_FACT_OUTCOMES,
            day=plan.day,
            frame=fact_frame,
            overwrite=overwrite,
        )
        _write_partition(
            table_root=target_root / TABLE_DIM_REQUEST,
            day=plan.day,
            frame=request_frame,
            overwrite=overwrite,
        )
        _write_partition(
            table_root=target_root / TABLE_DIM_DAY_STATUS,
            day=plan.day,
            frame=day_status_frame,
            overwrite=overwrite,
        )

        exported_days += 1
        fact_partitions_written += 1
        dim_request_partitions_written += 1
        dim_day_partitions_written += 1
        fact_rows_written += fact_frame.height
        dim_request_rows_written += request_frame.height
        dim_day_rows_written += day_status_frame.height

    return {
        "dataset_id": dataset_id_value,
        "selected_days": len(days),
        "eligible_complete_days": len(day_plans),
        "exported_days": exported_days,
        "skipped_incomplete_days": skipped_incomplete_days,
        "skipped_missing_status_days": skipped_missing_status_days,
        "skipped_missing_snapshot_days": skipped_missing_snapshot_days,
        "skipped_missing_event_props_days": skipped_missing_event_props_days,
        "overwrite": bool(overwrite),
        "output_root": target_root.as_posix(),
        "tables": {
            TABLE_FACT_OUTCOMES: {
                "rows": fact_rows_written,
                "partitions_written": fact_partitions_written,
            },
            TABLE_DIM_REQUEST: {
                "rows": dim_request_rows_written,
                "partitions_written": dim_request_partitions_written,
            },
            TABLE_DIM_DAY_STATUS: {
                "rows": dim_day_rows_written,
                "partitions_written": dim_day_partitions_written,
            },
        },
        "warning_count": len(warnings),
        "warnings": warnings,
        "generated_at_utc": utc_now_str(),
    }


def _check_day_output_conflicts(*, target_root: Path, day: str, overwrite: bool) -> None:
    for table_name in (TABLE_FACT_OUTCOMES, TABLE_DIM_REQUEST, TABLE_DIM_DAY_STATUS):
        partition_dir = target_root / table_name / f"day={day}"
        if partition_dir.exists() and not overwrite:
            raise FileExistsError(
                f"output partition exists: {partition_dir.as_posix()} (pass --overwrite)"
            )


def _build_fact_rows(
    *,
    dataset_id_value: str,
    day: str,
    snapshot_id: str,
    event_props_rows: list[dict[str, Any]],
    event_request_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_row in event_props_rows:
        event_id = _text(source_row.get("event_id", ""))
        request_key = event_request_keys.get(event_id)
        rows.append(
            {
                "dataset_id": dataset_id_value,
                "day": day,
                "snapshot_id": snapshot_id,
                "event_id": event_id,
                "request_key": request_key if request_key else None,
                "market": _text(source_row.get("market", "")),
                "player": _text(source_row.get("player", "")),
                "side": _text(source_row.get("side", "")),
                "point": _float_or_none(source_row.get("point")),
                "book": _text(source_row.get("book", "")),
                "price": _float_or_none(source_row.get("price")),
                "last_update": _text(source_row.get("last_update", "")),
                "link": _text(source_row.get("link", "")),
            }
        )
    return rows


def _build_request_rows(
    *,
    day: str,
    snapshot_id: str,
    requests_by_key: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for request_key in sorted(requests_by_key.keys()):
        request_row = requests_by_key.get(request_key, {})
        params = request_row.get("params", {})
        if not isinstance(params, dict):
            params = {}
        rows.append(
            {
                "day": day,
                "snapshot_id": snapshot_id,
                "request_key": request_key,
                "request_label": _text(request_row.get("label", "")),
                "request_path": _text(request_row.get("path", "")),
                "request_status": _text(request_row.get("status", "")),
                "updated_at_utc": _text(request_row.get("updated_at_utc", "")),
                "param_bookmakers": _param_text(params.get("bookmakers")),
                "param_regions": _param_text(params.get("regions")),
                "param_markets": _param_text(params.get("markets")),
                "param_date": _param_text(params.get("date")),
                "param_odds_format": _param_text(params.get("oddsFormat")),
                "param_date_format": _param_text(params.get("dateFormat")),
            }
        )
    return rows


def _event_request_key_by_event(requests_by_key: dict[str, dict[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for request_key, request_row in sorted(requests_by_key.items()):
        label = _text(request_row.get("label", ""))
        if not label.startswith("event_odds:"):
            continue
        _, _, event_id = label.partition(":")
        event_id = event_id.strip()
        if not event_id or event_id in mapping:
            continue
        mapping[event_id] = request_key
    return mapping


def _load_json_dict(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("expected JSON object")
    return payload


def _load_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("expected JSON object in JSONL")
        rows.append(payload)
    return rows


def _frame_for_schema(rows: list[dict[str, Any]], schema: list[tuple[str, Any]]) -> pl.DataFrame:
    schema_map = {name: dtype for name, dtype in schema}
    columns = [name for name, _ in schema]
    frame = pl.DataFrame(rows) if rows else pl.DataFrame(schema=schema_map)
    for name, dtype in schema:
        if name not in frame.columns:
            frame = frame.with_columns(pl.lit(None).cast(dtype).alias(name))
        else:
            frame = frame.with_columns(pl.col(name).cast(dtype, strict=False))
    return frame.select(columns)


def _write_partition(*, table_root: Path, day: str, frame: pl.DataFrame, overwrite: bool) -> None:
    partition_dir = table_root / f"day={day}"
    if partition_dir.exists():
        if not overwrite:
            raise FileExistsError(
                f"output partition exists: {partition_dir.as_posix()} (pass --overwrite)"
            )
        shutil.rmtree(partition_dir, ignore_errors=True)
    partition_dir.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(partition_dir / "part-00000.parquet", compression="zstd")


def _warning(*, code: str, day: str, detail: str, hint: str = "") -> dict[str, str]:
    row = {
        "code": _text(code),
        "day": _text(day),
        "detail": _text(detail),
    }
    hint_value = _text(hint)
    if hint_value:
        row["hint"] = hint_value
    return row


def _text(value: Any) -> str:
    return str(value).strip()


def _param_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = [_text(item) for item in value if item is not None and _text(item)]
        return ",".join(parts)
    return _text(value)


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: Any) -> int:
    if value is None or isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _day_reason_code(status: dict[str, Any]) -> str:
    reason_codes = status.get("reason_codes", [])
    if isinstance(reason_codes, list):
        for item in reason_codes:
            code = _text(item)
            if code:
                return code
    status_code = _text(status.get("status_code", ""))
    if status_code:
        return status_code
    return "complete" if bool(status.get("complete", False)) else ""
