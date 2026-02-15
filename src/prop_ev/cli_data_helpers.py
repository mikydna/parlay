"""Data/index helper functions for CLI commands."""

from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from prop_ev.odds_client import parse_csv
from prop_ev.odds_data.day_index import canonicalize_day_status, primary_incomplete_reason_code
from prop_ev.odds_data.spec import DatasetSpec


def dataset_root(data_root: Path) -> Path:
    return data_root / "datasets"


def dataset_dir(data_root: Path, dataset_id_value: str) -> Path:
    return dataset_root(data_root) / dataset_id_value


def dataset_spec_path(data_root: Path, dataset_id_value: str) -> Path:
    return dataset_dir(data_root, dataset_id_value) / "spec.json"


def dataset_days_dir(data_root: Path, dataset_id_value: str) -> Path:
    return dataset_dir(data_root, dataset_id_value) / "days"


def load_json_object(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    return payload


def discover_dataset_ids(data_root: Path) -> list[str]:
    root = dataset_root(data_root)
    if not root.exists():
        return []
    ids: list[str] = []
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        if (entry / "spec.json").exists() or (entry / "days").exists():
            ids.append(entry.name)
    return sorted(set(ids))


def dataset_day_names(data_root: Path, dataset_id_value: str) -> list[str]:
    days_dir = dataset_days_dir(data_root, dataset_id_value)
    if not days_dir.exists():
        return []
    names: list[str] = []
    for path in days_dir.glob("*.json"):
        candidate = path.stem.strip()
        try:
            date.fromisoformat(candidate)
        except ValueError:
            continue
        names.append(candidate)
    return sorted(set(names))


def dataset_spec_from_payload(payload: dict[str, Any], *, source: str) -> DatasetSpec:
    sport_key = str(payload.get("sport_key", "")).strip() or "basketball_nba"
    markets_raw = payload.get("markets", [])
    markets: list[str]
    if isinstance(markets_raw, list):
        markets = [str(item).strip() for item in markets_raw if str(item).strip()]
    else:
        markets = parse_csv(str(markets_raw))
    if not markets:
        raise RuntimeError(f"invalid dataset spec at {source}: markets must be a non-empty list")
    regions = str(payload.get("regions", "")).strip() or None
    bookmakers = str(payload.get("bookmakers", "")).strip() or None
    return DatasetSpec(
        sport_key=sport_key,
        markets=markets,
        regions=regions,
        bookmakers=bookmakers,
        include_links=bool(payload.get("include_links", False)),
        include_sids=bool(payload.get("include_sids", False)),
        odds_format=str(payload.get("odds_format", "american")).strip() or "american",
        date_format=str(payload.get("date_format", "iso")).strip() or "iso",
        historical=bool(payload.get("historical", False)),
        historical_anchor_hour_local=int(payload.get("historical_anchor_hour_local", 12)),
        historical_pre_tip_minutes=int(payload.get("historical_pre_tip_minutes", 60)),
    )


def load_dataset_spec_or_error(data_root: Path, dataset_id_value: str) -> tuple[DatasetSpec, Path]:
    path = dataset_spec_path(data_root, dataset_id_value)
    payload = load_json_object(path)
    if payload is None:
        raise RuntimeError(f"missing dataset spec: {path}")
    return dataset_spec_from_payload(payload, source=str(path)), path


def load_day_status_for_dataset(
    data_root: Path,
    *,
    dataset_id_value: str,
    day: str,
) -> dict[str, Any] | None:
    return load_json_object(dataset_days_dir(data_root, dataset_id_value) / f"{day}.json")


def day_row_from_status(day: str, status: dict[str, Any]) -> dict[str, Any]:
    normalized = canonicalize_day_status(status, day=day)
    return {
        "day": day,
        "complete": bool(normalized.get("complete", False)),
        "missing_count": int(normalized.get("missing_count", 0)),
        "total_events": int(normalized.get("total_events", 0)),
        "snapshot_id": str(normalized.get("snapshot_id_for_day", "")),
        "note": str(normalized.get("note", "")),
        "error": str(normalized.get("error", "")),
        "error_code": str(normalized.get("error_code", "")),
        "status_code": str(normalized.get("status_code", "")),
        "reason_codes": [
            str(item)
            for item in normalized.get("reason_codes", [])
            if isinstance(item, str) and str(item).strip()
        ],
        "odds_coverage_ratio": float(normalized.get("odds_coverage_ratio", 0.0)),
        "updated_at_utc": str(normalized.get("updated_at_utc", "")),
    }


def incomplete_reason_code(row: dict[str, Any]) -> str:
    error_code = str(row.get("error_code", "")).strip()
    if error_code:
        return error_code
    reason_codes_raw = row.get("reason_codes", [])
    if isinstance(reason_codes_raw, list):
        reason_codes = [str(item).strip() for item in reason_codes_raw if str(item).strip()]
        if reason_codes:
            return primary_incomplete_reason_code(reason_codes)
    status_code = str(row.get("status_code", "")).strip()
    if status_code.startswith("incomplete_"):
        return status_code.removeprefix("incomplete_")
    error_text = str(row.get("error", "")).strip().lower()
    if error_text:
        if "404" in error_text:
            return "upstream_404"
        if "exceed remaining budget" in error_text:
            return "budget_exceeded"
        if "blocked" in error_text:
            return "spend_blocked"
        return "error"
    note_text = str(row.get("note", "")).strip().lower()
    if note_text == "missing events list response":
        return "missing_events_list"
    if note_text == "missing day status":
        return "missing_day_status"
    if note_text:
        return "note"
    if int(row.get("missing_count", 0)) > 0:
        return "missing_event_odds"
    return "incomplete_unknown"


def parse_allow_incomplete_days(raw_values: list[str]) -> set[str]:
    allowed_days: set[str] = set()
    for raw_value in raw_values:
        for value in parse_csv(str(raw_value)):
            try:
                date.fromisoformat(value)
            except ValueError as exc:
                raise RuntimeError(f"invalid --allow-incomplete-day value: {value}") from exc
            allowed_days.add(value)
    return allowed_days


def parse_allow_incomplete_reasons(raw_values: list[str]) -> set[str]:
    allowed_reasons: set[str] = set()
    for raw_value in raw_values:
        allowed_reasons.update(parse_csv(str(raw_value)))
    return allowed_reasons


def build_status_summary_payload(
    *,
    dataset_id_value: str,
    spec: DatasetSpec,
    rows: list[dict[str, Any]],
    from_day: str,
    to_day: str,
    tz_name: str,
    warnings: list[dict[str, str]],
    generated_at_utc: str,
) -> dict[str, Any]:
    complete_days = [row["day"] for row in rows if bool(row["complete"])]
    incomplete_days = [row["day"] for row in rows if not bool(row["complete"])]
    incomplete_reason_counts: Counter[str] = Counter(
        incomplete_reason_code(row) for row in rows if not bool(row["complete"])
    )
    incomplete_error_code_counts: Counter[str] = Counter(
        (str(row.get("error_code", "")).strip() or incomplete_reason_code(row))
        for row in rows
        if not bool(row["complete"])
    )
    coverage_values = [float(row.get("odds_coverage_ratio", 0.0)) for row in rows]
    payload: dict[str, Any] = {
        "dataset_id": dataset_id_value,
        "sport_key": spec.sport_key,
        "markets": sorted(set(spec.markets)),
        "regions": spec.regions,
        "bookmakers": spec.bookmakers,
        "historical": bool(spec.historical),
        "from_day": from_day,
        "to_day": to_day,
        "tz_name": tz_name,
        "total_days": len(rows),
        "complete_count": len(complete_days),
        "incomplete_count": len(incomplete_days),
        "missing_events_total": sum(int(row["missing_count"]) for row in rows),
        "avg_odds_coverage_ratio": (
            sum(coverage_values) / len(coverage_values) if coverage_values else 0.0
        ),
        "minimum_odds_coverage_ratio": min(coverage_values) if coverage_values else 0.0,
        "complete_days": complete_days,
        "incomplete_days": incomplete_days,
        "incomplete_reason_counts": dict(sorted(incomplete_reason_counts.items())),
        "incomplete_error_code_counts": dict(sorted(incomplete_error_code_counts.items())),
        "days": rows,
        "generated_at_utc": generated_at_utc,
    }
    if warnings:
        payload["warnings"] = warnings
    return payload


def print_day_rows(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        reason_code = (
            incomplete_reason_code(row) if not bool(row.get("complete", False)) else "complete"
        )
        error_code = str(row.get("error_code", "")).strip() or reason_code
        print(
            (
                "day={} complete={} reason={} error_code={} missing={} events={} coverage={} "
                "snapshot_id={} note={}"
            ).format(
                row["day"],
                str(row["complete"]).lower(),
                reason_code,
                error_code,
                row["missing_count"],
                row["total_events"],
                f"{float(row.get('odds_coverage_ratio', 0.0)):.3f}",
                row["snapshot_id"],
                row["note"],
            )
        )


def print_warnings(warnings: list[dict[str, str]]) -> None:
    for warning in warnings:
        code = str(warning.get("code", "")).strip()
        detail = str(warning.get("detail", "")).strip()
        hint = str(warning.get("hint", "")).strip()
        print(f"warning={code} detail={detail} hint={hint}")


def resolve_complete_day_dataset_id(data_root: Path, requested: str) -> str:
    dataset_id_value = requested.strip()
    if dataset_id_value:
        load_dataset_spec_or_error(data_root, dataset_id_value)
        return dataset_id_value

    discovered = discover_dataset_ids(data_root)
    if not discovered:
        raise RuntimeError("no datasets found under data root; run data backfill first")
    if len(discovered) == 1:
        return discovered[0]
    choices = ",".join(discovered[:6])
    raise RuntimeError(
        f"multiple datasets found; pass --dataset-id with --all-complete-days (examples: {choices})"
    )


def complete_day_snapshots(data_root: Path, dataset_id_value: str) -> list[tuple[str, str]]:
    complete_rows: list[tuple[str, str]] = []
    for day in dataset_day_names(data_root, dataset_id_value):
        status = load_day_status_for_dataset(data_root, dataset_id_value=dataset_id_value, day=day)
        if not isinstance(status, dict):
            continue
        row = day_row_from_status(day, status)
        if not bool(row.get("complete", False)):
            continue
        snapshot_id = str(row.get("snapshot_id", "")).strip()
        if not snapshot_id:
            continue
        complete_rows.append((day, snapshot_id))
    complete_rows.sort(key=lambda item: item[0])
    return complete_rows
