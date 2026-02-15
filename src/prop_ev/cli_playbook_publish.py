"""Playbook publish/pointer artifact helpers."""

from __future__ import annotations

import json
from pathlib import Path
from shutil import copy2
from typing import TYPE_CHECKING

from prop_ev.nba_data.date_resolver import resolve_snapshot_date_str
from prop_ev.report_paths import (
    canonical_report_outputs_root,
    report_outputs_root,
    snapshot_reports_dir,
)

if TYPE_CHECKING:
    from prop_ev.storage import SnapshotStore


COMPACT_PLAYBOOK_REPORTS: tuple[str, ...] = (
    "strategy-report.json",
    "strategy-brief.meta.json",
    "strategy-brief.pdf",
)


def snapshot_date(snapshot_id: str) -> str:
    return resolve_snapshot_date_str(snapshot_id)


def publish_compact_playbook_outputs(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    now_utc_iso: str,
) -> tuple[list[str], Path, Path, Path]:
    reports_dir = snapshot_reports_dir(
        store,
        snapshot_id,
        reports_root=canonical_report_outputs_root(store),
    )
    if not reports_dir.exists():
        raise RuntimeError(f"missing reports directory: {reports_dir}")

    snapshot_day = snapshot_date(snapshot_id)
    reports_root = report_outputs_root(store)
    daily_dir = reports_root / "daily" / snapshot_day / f"snapshot={snapshot_id}"
    latest_dir = reports_root / "latest"
    daily_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)

    published: list[str] = []
    for filename in COMPACT_PLAYBOOK_REPORTS:
        src = reports_dir / filename
        if not src.exists():
            continue
        copy2(src, daily_dir / filename)
        copy2(src, latest_dir / filename)
        published.append(filename)

    if not published:
        raise RuntimeError(
            "no compact reports found; run `prop-ev playbook run` or "
            "`prop-ev playbook render` first"
        )

    pointer = {
        "snapshot_id": snapshot_id,
        "updated_at_utc": now_utc_iso,
        "files": published,
    }
    latest_json = latest_dir / "latest.json"
    latest_json.write_text(json.dumps(pointer, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    publish_json = daily_dir / "publish.json"
    publish_json.write_text(json.dumps(pointer, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return published, daily_dir, latest_dir, latest_json
