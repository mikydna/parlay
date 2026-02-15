"""Data/dataset CLI command implementations."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from prop_ev.cli_shared import (
    CLIError,
    _build_status_summary_payload,
    _dataset_day_names,
    _dataset_days_dir,
    _dataset_spec_from_args,
    _day_row_from_status,
    _discover_dataset_ids,
    _incomplete_reason_code,
    _load_dataset_spec_or_error,
    _load_day_status_for_dataset,
    _parse_allow_incomplete_days,
    _parse_allow_incomplete_reasons,
    _print_day_rows,
    _print_warnings,
    _resolve_days,
    _runtime_odds_data_dir,
    _spend_policy_from_args,
    _utc_now,
)
from prop_ev.lake_guardrails import build_guardrail_report
from prop_ev.lake_migration import migrate_layout
from prop_ev.odds_data.backfill import backfill_days
from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.day_index import compute_day_status_from_cache, load_day_status
from prop_ev.odds_data.spec import dataset_id
from prop_ev.quote_table import EVENT_PROPS_TABLE
from prop_ev.snapshot_artifacts import (
    repair_snapshot_derived_contracts,
    verify_snapshot_derived_contracts,
)
from prop_ev.storage import SnapshotStore
from prop_ev.time_utils import iso_z


def _cmd_data_datasets_ls(args: argparse.Namespace) -> int:
    data_root = Path(_runtime_odds_data_dir())
    dataset_ids = _discover_dataset_ids(data_root)
    summaries: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []

    for dataset_id_value in dataset_ids:
        day_names = _dataset_day_names(data_root, dataset_id_value)
        updated_at_values: list[str] = []
        try:
            spec, _ = _load_dataset_spec_or_error(data_root, dataset_id_value)
        except CLIError as exc:
            summaries.append(
                {
                    "dataset_id": dataset_id_value,
                    "day_count": len(day_names),
                    "complete_count": 0,
                    "incomplete_count": len(day_names),
                    "from_day": day_names[0] if day_names else "",
                    "to_day": day_names[-1] if day_names else "",
                    "error": str(exc),
                }
            )
            warnings.append(
                {
                    "code": "invalid_dataset_spec",
                    "detail": dataset_id_value,
                    "hint": "run `prop-ev data datasets show --dataset-id <id>`",
                }
            )
            continue

        rows: list[dict[str, Any]] = []
        for day in day_names:
            status = _load_day_status_for_dataset(
                data_root,
                dataset_id_value=dataset_id_value,
                day=day,
            )
            if not isinstance(status, dict):
                rows.append(
                    {
                        "day": day,
                        "complete": False,
                        "missing_count": 0,
                        "total_events": 0,
                        "snapshot_id": "",
                        "note": "missing day status",
                        "error": "",
                        "error_code": "missing_day_status",
                        "status_code": "incomplete_missing_day_status",
                        "reason_codes": ["missing_day_status"],
                        "odds_coverage_ratio": 0.0,
                        "updated_at_utc": "",
                    }
                )
                continue
            row = _day_row_from_status(day, status)
            rows.append(row)
            updated_at = str(row.get("updated_at_utc", "")).strip()
            if updated_at:
                updated_at_values.append(updated_at)

        complete_count = sum(1 for row in rows if bool(row.get("complete", False)))
        reason_counts: Counter[str] = Counter(
            _incomplete_reason_code(row) for row in rows if not bool(row.get("complete", False))
        )
        error_code_counts: Counter[str] = Counter(
            (str(row.get("error_code", "")).strip() or _incomplete_reason_code(row))
            for row in rows
            if not bool(row.get("complete", False))
        )
        coverage_values = [float(row.get("odds_coverage_ratio", 0.0)) for row in rows]
        summary = {
            "dataset_id": dataset_id_value,
            "sport_key": spec.sport_key,
            "markets": sorted(set(spec.markets)),
            "regions": spec.regions,
            "bookmakers": spec.bookmakers,
            "historical": bool(spec.historical),
            "day_count": len(day_names),
            "complete_count": complete_count,
            "incomplete_count": len(day_names) - complete_count,
            "missing_events_total": sum(int(row.get("missing_count", 0)) for row in rows),
            "avg_odds_coverage_ratio": (
                sum(coverage_values) / len(coverage_values) if coverage_values else 0.0
            ),
            "minimum_odds_coverage_ratio": min(coverage_values) if coverage_values else 0.0,
            "incomplete_reason_counts": dict(sorted(reason_counts.items())),
            "incomplete_error_code_counts": dict(sorted(error_code_counts.items())),
            "from_day": day_names[0] if day_names else "",
            "to_day": day_names[-1] if day_names else "",
            "updated_at_utc": max(updated_at_values) if updated_at_values else "",
        }
        summaries.append(summary)

    if bool(getattr(args, "json_output", False)):
        payload: dict[str, Any] = {
            "generated_at_utc": iso_z(_utc_now()),
            "dataset_count": len(summaries),
            "datasets": summaries,
        }
        if warnings:
            payload["warnings"] = warnings
        print(json.dumps(payload, sort_keys=True))
        return 0

    if warnings:
        _print_warnings(warnings)
    for row in summaries:
        markets = ",".join(row.get("markets", [])) if isinstance(row.get("markets"), list) else ""
        scope_label = "bookmakers" if str(row.get("bookmakers", "")).strip() else "regions"
        scope_value = str(row.get("bookmakers", "")).strip() or str(row.get("regions", "")).strip()
        print(
            (
                "dataset_id={} sport_key={} markets={} {}={} historical={} days={} "
                "complete={} incomplete={} from={} to={}"
            ).format(
                row.get("dataset_id", ""),
                row.get("sport_key", ""),
                markets,
                scope_label,
                scope_value,
                str(bool(row.get("historical", False))).lower(),
                int(row.get("day_count", 0)),
                int(row.get("complete_count", 0)),
                int(row.get("incomplete_count", 0)),
                row.get("from_day", ""),
                row.get("to_day", ""),
            )
        )
    return 0


def _cmd_data_datasets_show(args: argparse.Namespace) -> int:
    data_root = Path(_runtime_odds_data_dir())
    dataset_id_value = str(getattr(args, "dataset_id", "")).strip()
    if not dataset_id_value:
        raise CLIError("--dataset-id is required")

    spec, _ = _load_dataset_spec_or_error(data_root, dataset_id_value)
    available_days = _dataset_day_names(data_root, dataset_id_value)
    from_day = str(getattr(args, "from_day", "")).strip()
    to_day = str(getattr(args, "to_day", "")).strip()
    if (from_day and not to_day) or (to_day and not from_day):
        raise CLIError("--from and --to must be provided together")

    if from_day and to_day:
        selected_days = _resolve_days(
            days=1,
            from_day=from_day,
            to_day=to_day,
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    else:
        selected_days = available_days

    rows: list[dict[str, Any]] = []
    for day in selected_days:
        status = _load_day_status_for_dataset(
            data_root,
            dataset_id_value=dataset_id_value,
            day=day,
        )
        if not isinstance(status, dict):
            rows.append(
                {
                    "day": day,
                    "complete": False,
                    "missing_count": 0,
                    "total_events": 0,
                    "snapshot_id": "",
                    "note": "missing day status",
                    "error": "",
                    "error_code": "missing_day_status",
                    "status_code": "incomplete_missing_day_status",
                    "reason_codes": ["missing_day_status"],
                    "odds_coverage_ratio": 0.0,
                    "updated_at_utc": "",
                }
            )
            continue
        rows.append(_day_row_from_status(day, status))

    payload = _build_status_summary_payload(
        dataset_id_value=dataset_id_value,
        spec=spec,
        rows=rows,
        from_day=selected_days[0] if selected_days else "",
        to_day=selected_days[-1] if selected_days else "",
        tz_name=str(getattr(args, "tz_name", "America/New_York")),
        warnings=[],
    )
    payload["available_day_count"] = len(available_days)
    payload["available_from_day"] = available_days[0] if available_days else ""
    payload["available_to_day"] = available_days[-1] if available_days else ""

    if bool(getattr(args, "json_output", False)):
        print(json.dumps(payload, sort_keys=True))
        return 0

    markets = ",".join(sorted(set(spec.markets)))
    scope_label = "bookmakers" if spec.bookmakers else "regions"
    scope_value = spec.bookmakers or (spec.regions or "")
    print(
        f"dataset_id={dataset_id_value} sport_key={spec.sport_key} markets={markets} "
        f"{scope_label}={scope_value} historical={str(bool(spec.historical)).lower()} "
        f"available_days={len(available_days)}"
    )
    _print_day_rows(rows)
    return 0


def _cmd_data_status(args: argparse.Namespace) -> int:
    data_root = Path(_runtime_odds_data_dir())
    store = SnapshotStore(data_root)
    cache = GlobalCacheStore(data_root)
    warnings: list[dict[str, str]] = []
    dataset_id_override = str(getattr(args, "dataset_id", "")).strip()
    if dataset_id_override:
        spec, _ = _load_dataset_spec_or_error(data_root, dataset_id_override)
        warnings.append(
            {
                "code": "dataset_id_override",
                "detail": dataset_id_override,
                "hint": "status uses stored dataset spec; CLI spec args are ignored",
            }
        )
    else:
        spec = _dataset_spec_from_args(args)
    dataset_id_value = dataset_id_override or dataset_id(spec)
    try:
        days = _resolve_days(
            days=int(getattr(args, "days", 10)),
            from_day=str(getattr(args, "from_day", "")),
            to_day=str(getattr(args, "to_day", "")),
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    except (KeyError, ValueError) as exc:
        raise CLIError(str(exc)) from exc

    if (
        not dataset_id_override
        and not bool(getattr(args, "refresh", False))
        and not _dataset_day_names(data_root, dataset_id_value)
    ):
        discovered = [item for item in _discover_dataset_ids(data_root) if item != dataset_id_value]
        if discovered:
            warnings.append(
                {
                    "code": "dataset_not_found_for_spec",
                    "detail": dataset_id_value,
                    "hint": "run `prop-ev data datasets ls` and pick --dataset-id",
                }
            )

    rows: list[dict[str, Any]] = []
    for day in days:
        status: dict[str, Any] | None = None
        if not bool(getattr(args, "refresh", False)):
            if dataset_id_override:
                status = _load_day_status_for_dataset(
                    data_root,
                    dataset_id_value=dataset_id_value,
                    day=day,
                )
            else:
                status = load_day_status(data_root, spec, day)
        if not isinstance(status, dict):
            status = compute_day_status_from_cache(
                data_root=data_root,
                store=store,
                cache=cache,
                spec=spec,
                day=day,
                tz_name=str(getattr(args, "tz_name", "America/New_York")),
            )
        row = _day_row_from_status(day, status)
        rows.append(row)

    if bool(getattr(args, "json_summary", False)):
        payload = _build_status_summary_payload(
            dataset_id_value=dataset_id_value,
            spec=spec,
            rows=rows,
            from_day=days[0] if days else "",
            to_day=days[-1] if days else "",
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
            warnings=warnings,
        )
        print(json.dumps(payload, sort_keys=True))
    else:
        if warnings:
            _print_warnings(warnings)
        _print_day_rows(rows)
    return 0


def _cmd_data_done_days(args: argparse.Namespace) -> int:
    data_root = Path(_runtime_odds_data_dir())
    dataset_id_value = str(getattr(args, "dataset_id", "")).strip()
    if not dataset_id_value:
        raise CLIError("--dataset-id is required")

    spec, _ = _load_dataset_spec_or_error(data_root, dataset_id_value)
    available_days = _dataset_day_names(data_root, dataset_id_value)
    from_day = str(getattr(args, "from_day", "")).strip()
    to_day = str(getattr(args, "to_day", "")).strip()
    if (from_day and not to_day) or (to_day and not from_day):
        raise CLIError("--from and --to must be provided together")

    if from_day and to_day:
        selected_days = _resolve_days(
            days=1,
            from_day=from_day,
            to_day=to_day,
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    else:
        selected_days = available_days

    rows: list[dict[str, Any]] = []
    for day in selected_days:
        status = _load_day_status_for_dataset(
            data_root,
            dataset_id_value=dataset_id_value,
            day=day,
        )
        if not isinstance(status, dict):
            rows.append(
                {
                    "day": day,
                    "complete": False,
                    "missing_count": 0,
                    "total_events": 0,
                    "snapshot_id": "",
                    "note": "missing day status",
                    "error": "",
                    "error_code": "missing_day_status",
                    "status_code": "incomplete_missing_day_status",
                    "reason_codes": ["missing_day_status"],
                    "odds_coverage_ratio": 0.0,
                    "updated_at_utc": "",
                }
            )
            continue
        rows.append(_day_row_from_status(day, status))

    payload = _build_status_summary_payload(
        dataset_id_value=dataset_id_value,
        spec=spec,
        rows=rows,
        from_day=selected_days[0] if selected_days else "",
        to_day=selected_days[-1] if selected_days else "",
        tz_name=str(getattr(args, "tz_name", "America/New_York")),
        warnings=[],
    )
    payload["available_day_count"] = len(available_days)
    payload["available_from_day"] = available_days[0] if available_days else ""
    payload["available_to_day"] = available_days[-1] if available_days else ""
    allow_incomplete_days = _parse_allow_incomplete_days(
        [str(value) for value in getattr(args, "allow_incomplete_day", [])]
    )
    allow_incomplete_reasons = _parse_allow_incomplete_reasons(
        [str(value) for value in getattr(args, "allow_incomplete_reason", [])]
    )
    disallowed_days: list[str] = []
    disallowed_reason_counts: Counter[str] = Counter()
    allowed_incomplete_count = 0
    for row in rows:
        if bool(row.get("complete", False)):
            continue
        day = str(row.get("day", "")).strip()
        reason = _incomplete_reason_code(row)
        allowed = False
        if day and day in allow_incomplete_days:
            allowed = True
        if reason and reason in allow_incomplete_reasons:
            allowed = True
        if allowed:
            allowed_incomplete_count += 1
            continue
        if day:
            disallowed_days.append(day)
        disallowed_reason_counts[reason] += 1

    payload["allowed_incomplete_days"] = sorted(allow_incomplete_days)
    payload["allowed_incomplete_reasons"] = sorted(allow_incomplete_reasons)
    payload["allowed_incomplete_count"] = allowed_incomplete_count
    payload["disallowed_incomplete_days"] = sorted(disallowed_days)
    payload["disallowed_incomplete_count"] = len(disallowed_days)
    payload["disallowed_incomplete_reason_counts"] = dict(sorted(disallowed_reason_counts.items()))
    payload["preflight_pass"] = len(disallowed_days) == 0

    if bool(getattr(args, "json_output", False)):
        print(json.dumps(payload, sort_keys=True))
    else:
        print(
            ("dataset_id={} selected_days={} complete={} incomplete={} from={} to={}").format(
                dataset_id_value,
                payload.get("total_days", 0),
                payload.get("complete_count", 0),
                payload.get("incomplete_count", 0),
                payload.get("from_day", ""),
                payload.get("to_day", ""),
            )
        )
        complete_days = payload.get("complete_days", [])
        incomplete_days = payload.get("incomplete_days", [])
        if isinstance(complete_days, list):
            print(f"complete_days={','.join(str(day) for day in complete_days)}")
        if isinstance(incomplete_days, list):
            print(f"incomplete_days={','.join(str(day) for day in incomplete_days)}")
        reason_counts = payload.get("incomplete_reason_counts", {})
        if isinstance(reason_counts, dict):
            for reason, count in sorted(reason_counts.items()):
                print(f"incomplete_reason={reason} count={count}")
        disallowed_reason_counts_payload = payload.get("disallowed_incomplete_reason_counts", {})
        if isinstance(disallowed_reason_counts_payload, dict):
            for reason, count in sorted(disallowed_reason_counts_payload.items()):
                print(f"disallowed_incomplete_reason={reason} count={count}")
        print(
            "preflight_pass={} disallowed_incomplete_count={}".format(
                str(bool(payload.get("preflight_pass", False))).lower(),
                int(payload.get("disallowed_incomplete_count", 0)),
            )
        )

    if (
        bool(getattr(args, "require_complete", False))
        and int(payload.get("disallowed_incomplete_count", 0)) > 0
    ):
        return 2
    return 0


def _cmd_data_backfill(args: argparse.Namespace) -> int:
    data_root = Path(_runtime_odds_data_dir())
    spec = _dataset_spec_from_args(args)
    try:
        days = _resolve_days(
            days=int(getattr(args, "days", 10)),
            from_day=str(getattr(args, "from_day", "")),
            to_day=str(getattr(args, "to_day", "")),
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    except (KeyError, ValueError) as exc:
        raise CLIError(str(exc)) from exc

    policy = _spend_policy_from_args(args)
    summaries = backfill_days(
        data_root=data_root,
        spec=spec,
        days=days,
        tz_name=str(getattr(args, "tz_name", "America/New_York")),
        policy=policy,
        dry_run=bool(getattr(args, "dry_run", False)),
    )

    had_error = False
    for row in summaries:
        error = str(row.get("error", "")).strip()
        error_code = str(row.get("error_code", "")).strip()
        if error:
            had_error = True
        print(
            (
                "day={} snapshot_id={} complete={} missing={} events={} "
                "estimated_paid_credits={} actual_paid_credits={} remaining_credits={} "
                "error_code={} error={}"
            ).format(
                str(row.get("day", "")),
                str(row.get("snapshot_id", "")),
                str(bool(row.get("complete", False))).lower(),
                int(row.get("missing", 0)),
                int(row.get("events", 0)),
                int(row.get("estimated_paid_credits", 0)),
                int(row.get("actual_paid_credits", 0)),
                int(row.get("remaining_credits", 0)),
                error_code,
                error,
            )
        )
    return 2 if had_error else 0


def _cmd_data_verify(args: argparse.Namespace) -> int:
    data_root = Path(_runtime_odds_data_dir())
    dataset_id_value = str(getattr(args, "dataset_id", "")).strip()
    if not dataset_id_value:
        raise CLIError("--dataset-id is required")

    spec, _ = _load_dataset_spec_or_error(data_root, dataset_id_value)
    available_days = _dataset_day_names(data_root, dataset_id_value)
    from_day = str(getattr(args, "from_day", "")).strip()
    to_day = str(getattr(args, "to_day", "")).strip()
    if (from_day and not to_day) or (to_day and not from_day):
        raise CLIError("--from and --to must be provided together")

    if from_day and to_day:
        selected_days = _resolve_days(
            days=1,
            from_day=from_day,
            to_day=to_day,
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    else:
        selected_days = available_days

    allow_incomplete_days = _parse_allow_incomplete_days(
        [str(value) for value in getattr(args, "allow_incomplete_day", [])]
    )
    allow_incomplete_reasons = _parse_allow_incomplete_reasons(
        [str(value) for value in getattr(args, "allow_incomplete_reason", [])]
    )

    day_reports: list[dict[str, Any]] = []
    issue_count = 0
    checked_complete_days = 0
    for day in selected_days:
        row_issues: list[dict[str, str]] = []
        status = _load_day_status_for_dataset(
            data_root,
            dataset_id_value=dataset_id_value,
            day=day,
        )
        if not isinstance(status, dict):
            row = {
                "day": day,
                "complete": False,
                "missing_count": 0,
                "total_events": 0,
                "snapshot_id": "",
                "note": "missing day status",
                "error": "",
                "error_code": "missing_day_status",
                "status_code": "incomplete_missing_day_status",
                "reason_codes": ["missing_day_status"],
                "odds_coverage_ratio": 0.0,
                "updated_at_utc": "",
            }
            row_issues.append(
                {
                    "code": "missing_day_status",
                    "detail": (
                        _dataset_days_dir(data_root, dataset_id_value) / f"{day}.json"
                    ).as_posix(),
                }
            )
        else:
            row = _day_row_from_status(day, status)

        if bool(getattr(args, "require_complete", False)) and not bool(row.get("complete", False)):
            reason_code = _incomplete_reason_code(row)
            allowlisted = day in allow_incomplete_days or reason_code in allow_incomplete_reasons
            if not allowlisted:
                row_issues.append(
                    {
                        "code": "incomplete_day",
                        "detail": reason_code,
                    }
                )

        if bool(row.get("complete", False)):
            checked_complete_days += 1
            snapshot_id = str(row.get("snapshot_id", "")).strip()
            if not snapshot_id:
                row_issues.append({"code": "missing_snapshot_id", "detail": day})
            else:
                snapshot_dir = data_root / "snapshots" / snapshot_id
                if not snapshot_dir.exists():
                    row_issues.append(
                        {
                            "code": "missing_snapshot_dir",
                            "detail": snapshot_dir.as_posix(),
                        }
                    )
                else:
                    derived_issues = verify_snapshot_derived_contracts(
                        snapshot_dir=snapshot_dir,
                        require_parquet=bool(getattr(args, "require_parquet", False)),
                        require_canonical_jsonl=bool(
                            getattr(args, "require_canonical_jsonl", False)
                        ),
                        required_tables=(EVENT_PROPS_TABLE,),
                    )
                    row_issues.extend(derived_issues)

        issue_count += len(row_issues)
        day_reports.append(
            {
                **row,
                "issues": row_issues,
                "issue_count": len(row_issues),
            }
        )

    payload: dict[str, Any] = {
        "dataset_id": dataset_id_value,
        "sport_key": spec.sport_key,
        "markets": sorted(set(spec.markets)),
        "regions": spec.regions,
        "bookmakers": spec.bookmakers,
        "historical": bool(spec.historical),
        "available_day_count": len(available_days),
        "available_from_day": available_days[0] if available_days else "",
        "available_to_day": available_days[-1] if available_days else "",
        "checked_days": len(selected_days),
        "checked_complete_days": checked_complete_days,
        "issue_count": issue_count,
        "require_complete": bool(getattr(args, "require_complete", False)),
        "require_parquet": bool(getattr(args, "require_parquet", False)),
        "require_canonical_jsonl": bool(getattr(args, "require_canonical_jsonl", False)),
        "allow_incomplete_days": sorted(allow_incomplete_days),
        "allow_incomplete_reasons": sorted(allow_incomplete_reasons),
        "days": day_reports,
        "generated_at_utc": iso_z(_utc_now()),
    }

    if bool(getattr(args, "json_output", False)):
        print(json.dumps(payload, sort_keys=True))
    else:
        print(
            "dataset_id={} checked_days={} checked_complete_days={} issue_count={} "
            "require_complete={} require_parquet={} require_canonical_jsonl={}".format(
                dataset_id_value,
                len(selected_days),
                checked_complete_days,
                issue_count,
                str(bool(getattr(args, "require_complete", False))).lower(),
                str(bool(getattr(args, "require_parquet", False))).lower(),
                str(bool(getattr(args, "require_canonical_jsonl", False))).lower(),
            )
        )
        for row in day_reports:
            issue_codes = ",".join(
                str(item.get("code", ""))
                for item in row.get("issues", [])
                if isinstance(item, dict) and str(item.get("code", "")).strip()
            )
            print(
                "day={} complete={} reason={} coverage={} issues={} issue_codes={}".format(
                    str(row.get("day", "")),
                    str(bool(row.get("complete", False))).lower(),
                    _incomplete_reason_code(row)
                    if not bool(row.get("complete", False))
                    else "complete",
                    f"{float(row.get('odds_coverage_ratio', 0.0)):.3f}",
                    int(row.get("issue_count", 0)),
                    issue_codes,
                )
            )

    return 2 if issue_count else 0


def _cmd_data_repair_derived(args: argparse.Namespace) -> int:
    data_root = Path(_runtime_odds_data_dir())
    dataset_id_value = str(getattr(args, "dataset_id", "")).strip()
    if not dataset_id_value:
        raise CLIError("--dataset-id is required")

    _load_dataset_spec_or_error(data_root, dataset_id_value)
    available_days = _dataset_day_names(data_root, dataset_id_value)
    from_day = str(getattr(args, "from_day", "")).strip()
    to_day = str(getattr(args, "to_day", "")).strip()
    if (from_day and not to_day) or (to_day and not from_day):
        raise CLIError("--from and --to must be provided together")

    if from_day and to_day:
        selected_days = _resolve_days(
            days=1,
            from_day=from_day,
            to_day=to_day,
            tz_name=str(getattr(args, "tz_name", "America/New_York")),
        )
    else:
        selected_days = available_days

    day_reports: list[dict[str, Any]] = []
    issue_count = 0
    repaired_days = 0
    skipped_incomplete_days = 0
    for day in selected_days:
        status = _load_day_status_for_dataset(
            data_root,
            dataset_id_value=dataset_id_value,
            day=day,
        )
        if not isinstance(status, dict):
            issues = [
                {
                    "code": "missing_day_status",
                    "detail": (
                        _dataset_days_dir(data_root, dataset_id_value) / f"{day}.json"
                    ).as_posix(),
                }
            ]
            issue_count += len(issues)
            day_reports.append(
                {
                    "day": day,
                    "status": "error",
                    "snapshot_id": "",
                    "jsonl_rewritten": 0,
                    "parquet_written": 0,
                    "issue_count": len(issues),
                    "issues": issues,
                }
            )
            continue

        row = _day_row_from_status(day, status)
        if not bool(row.get("complete", False)):
            skipped_incomplete_days += 1
            day_reports.append(
                {
                    "day": day,
                    "status": "skipped_incomplete",
                    "snapshot_id": str(row.get("snapshot_id", "")),
                    "reason": _incomplete_reason_code(row),
                    "jsonl_rewritten": 0,
                    "parquet_written": 0,
                    "issue_count": 0,
                    "issues": [],
                }
            )
            continue

        snapshot_id = str(row.get("snapshot_id", "")).strip()
        if not snapshot_id:
            issues = [{"code": "missing_snapshot_id", "detail": day}]
            issue_count += len(issues)
            day_reports.append(
                {
                    "day": day,
                    "status": "error",
                    "snapshot_id": "",
                    "jsonl_rewritten": 0,
                    "parquet_written": 0,
                    "issue_count": len(issues),
                    "issues": issues,
                }
            )
            continue

        snapshot_dir = data_root / "snapshots" / snapshot_id
        if not snapshot_dir.exists():
            issues = [
                {
                    "code": "missing_snapshot_dir",
                    "detail": snapshot_dir.as_posix(),
                }
            ]
            issue_count += len(issues)
            day_reports.append(
                {
                    "day": day,
                    "status": "error",
                    "snapshot_id": snapshot_id,
                    "jsonl_rewritten": 0,
                    "parquet_written": 0,
                    "issue_count": len(issues),
                    "issues": issues,
                }
            )
            continue

        try:
            repair_report = repair_snapshot_derived_contracts(snapshot_dir)
        except (FileNotFoundError, ValueError) as exc:
            issues = [{"code": "repair_failed", "detail": str(exc)}]
            issue_count += len(issues)
            day_reports.append(
                {
                    "day": day,
                    "status": "error",
                    "snapshot_id": snapshot_id,
                    "jsonl_rewritten": 0,
                    "parquet_written": 0,
                    "issue_count": len(issues),
                    "issues": issues,
                }
            )
            continue

        issues = verify_snapshot_derived_contracts(
            snapshot_dir=snapshot_dir,
            require_parquet=True,
            require_canonical_jsonl=True,
            required_tables=(EVENT_PROPS_TABLE,),
        )
        issue_count += len(issues)
        repaired_days += 1
        day_reports.append(
            {
                "day": day,
                "status": "repaired" if not issues else "repaired_with_issues",
                "snapshot_id": snapshot_id,
                "jsonl_rewritten": len(repair_report.get("jsonl_rewritten", [])),
                "parquet_written": len(repair_report.get("parquet_written", [])),
                "issue_count": len(issues),
                "issues": issues,
            }
        )

    payload = {
        "dataset_id": dataset_id_value,
        "selected_days": len(selected_days),
        "repaired_days": repaired_days,
        "skipped_incomplete_days": skipped_incomplete_days,
        "issue_count": issue_count,
        "days": day_reports,
        "generated_at_utc": iso_z(_utc_now()),
    }

    if bool(getattr(args, "json_output", False)):
        print(json.dumps(payload, sort_keys=True))
    else:
        print(
            f"dataset_id={dataset_id_value} selected_days={len(selected_days)} "
            f"repaired_days={repaired_days} skipped_incomplete_days={skipped_incomplete_days} "
            f"issue_count={issue_count}"
        )
        for row in day_reports:
            issue_codes = ",".join(
                str(item.get("code", ""))
                for item in row.get("issues", [])
                if isinstance(item, dict) and str(item.get("code", "")).strip()
            )
            print(
                f"day={str(row.get('day', ''))} "
                f"status={str(row.get('status', ''))} "
                f"snapshot_id={str(row.get('snapshot_id', ''))} "
                f"jsonl_rewritten={int(row.get('jsonl_rewritten', 0))} "
                f"parquet_written={int(row.get('parquet_written', 0))} "
                f"issues={int(row.get('issue_count', 0))} issue_codes={issue_codes}"
            )

    return 2 if issue_count else 0


def _cmd_data_guardrails(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    report = build_guardrail_report(store.root)
    violations = report.get("violations", [])
    violation_count = int(report.get("violation_count", 0))
    if bool(getattr(args, "json_output", False)):
        print(json.dumps(report, sort_keys=True))
    else:
        print(f"odds_root={report.get('odds_root', '')}")
        print(f"status={report.get('status', '')} violation_count={violation_count}")
        for row in violations:
            if not isinstance(row, dict):
                continue
            print(
                "violation code={} path={} detail={}".format(
                    str(row.get("code", "")),
                    str(row.get("path", "")),
                    str(row.get("detail", "")),
                )
            )
    return 1 if violation_count > 0 else 0


def _cmd_data_migrate_layout(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_ids = [str(value) for value in getattr(args, "snapshot_id", []) if str(value).strip()]
    report = migrate_layout(
        odds_root=store.root,
        snapshot_ids=snapshot_ids or None,
        dry_run=not bool(getattr(args, "apply", False)),
    )
    if bool(getattr(args, "json_output", False)):
        print(json.dumps(report, sort_keys=True))
    else:
        action_counts = report.get("action_counts", {})
        print(f"odds_root={report.get('odds_root', '')}")
        print(f"reports_root={report.get('reports_root', '')}")
        print(f"runtime_root={report.get('runtime_root', '')}")
        print(f"dry_run={str(bool(report.get('dry_run', True))).lower()}")
        print(f"action_counts={json.dumps(action_counts, sort_keys=True)}")
        for row in report.get("actions", []):
            if not isinstance(row, dict):
                continue
            print(
                "action={} status={} source={} destination={} reason={}".format(
                    str(row.get("action", "")),
                    str(row.get("status", "")),
                    str(row.get("source", "")),
                    str(row.get("destination", "")),
                    str(row.get("reason", "")),
                )
            )
        guardrails = report.get("guardrails")
        if isinstance(guardrails, dict):
            print(f"guardrails_status={guardrails.get('status', '')}")
            print(f"guardrails_violation_count={guardrails.get('violation_count', 0)}")
    action_counts = report.get("action_counts", {})
    conflicts = int(action_counts.get("conflict", 0))
    if conflicts > 0:
        return 2
    if bool(getattr(args, "apply", False)):
        guardrails = report.get("guardrails", {})
        if isinstance(guardrails, dict) and int(guardrails.get("violation_count", 0)) > 0:
            return 1
    return 0
