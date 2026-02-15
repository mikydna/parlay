"""Canonical report path helpers (separate from odds snapshot lake artifacts)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from zoneinfo import ZoneInfo

from prop_ev.data_paths import (
    canonical_reports_root,
    data_home_from_odds_root,
    runtime_config_for_odds_root,
)
from prop_ev.runtime_config import current_runtime_config
from prop_ev.storage import SnapshotStore
from prop_ev.time_utils import parse_iso_z

ET_ZONE = ZoneInfo("America/New_York")


def canonical_report_outputs_root(store: SnapshotStore) -> Path:
    """Return canonical odds report root derived from odds data root."""
    return canonical_reports_root(store.root)


def legacy_report_outputs_root(store: SnapshotStore) -> Path:
    """Return legacy report root used before `reports/odds/**` split."""
    root = store.root.resolve()
    if root.name == "odds_api":
        return root.parent / "reports"
    if root.name == "odds" and root.parent.name == "lakes":
        return data_home_from_odds_root(root) / "reports"
    return root / "reports"


def report_outputs_root(store: SnapshotStore) -> Path:
    """Return user-facing report root from runtime config or canonical location."""
    if runtime_config_for_odds_root(store.root) is not None:
        return current_runtime_config().reports_dir.resolve()
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
    return root / "by-snapshot" / _sanitize_snapshot_label(snapshot_id)


def legacy_snapshot_reports_dir(store: SnapshotStore, snapshot_id: str) -> Path:
    """Legacy per-snapshot reports directory path."""
    return (
        legacy_report_outputs_root(store) / "snapshots" / snapshot_report_label(store, snapshot_id)
    )


def latest_reports_dir(store: SnapshotStore) -> Path:
    """Canonical latest report directory."""
    return report_outputs_root(store) / "latest"


def maybe_relpath(path: Path, *, root: Path) -> str:
    """Return relative path to root when possible."""
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)
