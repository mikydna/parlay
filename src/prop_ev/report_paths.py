"""Canonical report path helpers (separate from odds snapshot lake artifacts)."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from zoneinfo import ZoneInfo

from prop_ev.storage import SnapshotStore
from prop_ev.time_utils import parse_iso_z

ET_ZONE = ZoneInfo("America/New_York")


def canonical_report_outputs_root(store: SnapshotStore) -> Path:
    """Return report root derived only from data root (ignores env override)."""
    root = store.root.resolve()
    if root.name == "odds_api":
        return root.parent / "reports"
    return root / "reports"


def report_outputs_root(store: SnapshotStore) -> Path:
    """Return user-facing report root outside odds snapshot storage."""
    override = os.environ.get("PROP_EV_REPORTS_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return canonical_report_outputs_root(store)


def _sanitize_snapshot_label(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("._-")
    return cleaned or "snapshot"


def _manifest_created_at_utc(store: SnapshotStore, snapshot_id: str) -> str:
    manifest_path = store.snapshot_dir(snapshot_id) / "manifest.json"
    if not manifest_path.exists():
        return ""
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("created_at_utc", "")).strip()


def snapshot_report_label(store: SnapshotStore, snapshot_id: str) -> str:
    """Build readable ET snapshot label for report directories."""
    created_at_utc = _manifest_created_at_utc(store, snapshot_id)
    parsed = parse_iso_z(created_at_utc)
    if parsed is not None:
        return parsed.astimezone(ET_ZONE).strftime("%Y-%m-%dT%H-%M-%S-ET")
    return _sanitize_snapshot_label(snapshot_id)


def snapshot_reports_dir(
    store: SnapshotStore, snapshot_id: str, *, reports_root: Path | None = None
) -> Path:
    """Canonical report directory for one snapshot."""
    root = reports_root or report_outputs_root(store)
    return root / "snapshots" / snapshot_report_label(store, snapshot_id)


def latest_reports_dir(store: SnapshotStore) -> Path:
    """Canonical latest report directory."""
    return report_outputs_root(store) / "latest"


def maybe_relpath(path: Path, *, root: Path) -> str:
    """Return relative path to root when possible."""
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)
