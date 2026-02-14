"""Migration helpers for P0 lake/report/runtime layout split."""

from __future__ import annotations

import filecmp
import shutil
from contextlib import suppress
from pathlib import Path
from typing import Any

from prop_ev.data_paths import resolve_runtime_root
from prop_ev.lake_guardrails import build_guardrail_report
from prop_ev.nba_data.repo import NBARepository
from prop_ev.report_paths import (
    canonical_report_outputs_root,
    legacy_snapshot_reports_dir,
    snapshot_reports_dir,
)
from prop_ev.storage import SnapshotStore


def _record_action(
    actions: list[dict[str, str]],
    *,
    action: str,
    source: Path,
    destination: Path,
    status: str,
    reason: str = "",
) -> None:
    actions.append(
        {
            "action": action,
            "source": str(source),
            "destination": str(destination),
            "status": status,
            "reason": reason,
        }
    )


def _prune_empty_dirs(path: Path) -> None:
    if not path.exists() or not path.is_dir():
        return
    for child in sorted(path.rglob("*"), reverse=True):
        if not child.is_dir():
            continue
        try:
            child.rmdir()
        except OSError:
            continue
    with suppress(OSError):
        path.rmdir()


def _move_file(
    *,
    source: Path,
    destination: Path,
    dry_run: bool,
    actions: list[dict[str, str]],
) -> None:
    if not source.exists() or not source.is_file():
        return
    if destination.exists():
        if destination.is_file() and filecmp.cmp(source, destination, shallow=False):
            _record_action(
                actions,
                action="move_file",
                source=source,
                destination=destination,
                status="deduped",
                reason="destination already has identical content",
            )
            if not dry_run:
                source.unlink(missing_ok=True)
            return
        _record_action(
            actions,
            action="move_file",
            source=source,
            destination=destination,
            status="conflict",
            reason="destination exists with different content",
        )
        return
    _record_action(
        actions,
        action="move_file",
        source=source,
        destination=destination,
        status="planned" if dry_run else "moved",
    )
    if dry_run:
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(destination))


def _move_tree_contents(
    *,
    source: Path,
    destination: Path,
    dry_run: bool,
    actions: list[dict[str, str]],
) -> None:
    if not source.exists() or not source.is_dir():
        return
    for file_path in sorted(path for path in source.rglob("*") if path.is_file()):
        rel = file_path.relative_to(source)
        _move_file(
            source=file_path,
            destination=destination / rel,
            dry_run=dry_run,
            actions=actions,
        )
    if dry_run:
        return
    _prune_empty_dirs(source)


def _iter_snapshot_ids(store: SnapshotStore, snapshot_ids: list[str] | None) -> list[str]:
    if snapshot_ids:
        return sorted({value.strip() for value in snapshot_ids if value.strip()})
    snapshots_root = store.root / "snapshots"
    if not snapshots_root.exists():
        return []
    return sorted(path.name for path in snapshots_root.iterdir() if path.is_dir())


def migrate_layout(
    *,
    odds_root: Path | str,
    snapshot_ids: list[str] | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Migrate legacy odds/nba/report/runtime layout into P0 contract layout."""
    store = SnapshotStore(odds_root)
    canonical_reports_root = canonical_report_outputs_root(store)
    runtime_root = resolve_runtime_root(store.root)
    actions: list[dict[str, str]] = []

    for snapshot_id in _iter_snapshot_ids(store, snapshot_ids):
        snapshot_dir = store.snapshot_dir(snapshot_id)
        repo = NBARepository(
            odds_data_root=store.root,
            snapshot_id=snapshot_id,
            snapshot_dir=snapshot_dir,
        )

        legacy_context = snapshot_dir / "context"
        if legacy_context.exists():
            _move_file(
                source=legacy_context / "injuries.json",
                destination=repo.context_dir / "injuries.json",
                dry_run=dry_run,
                actions=actions,
            )
            _move_file(
                source=legacy_context / "roster.json",
                destination=repo.context_dir / "roster.json",
                dry_run=dry_run,
                actions=actions,
            )
            _move_file(
                source=legacy_context / "results.json",
                destination=repo.context_dir / "results.json",
                dry_run=dry_run,
                actions=actions,
            )
            for file_path in sorted(legacy_context.glob("results-*.json")):
                _move_file(
                    source=file_path,
                    destination=repo.context_dir / file_path.name,
                    dry_run=dry_run,
                    actions=actions,
                )
            _move_tree_contents(
                source=legacy_context / "official_injury_pdf",
                destination=repo.official_injury_pdf_dir(),
                dry_run=dry_run,
                actions=actions,
            )

        canonical_snapshot_reports = snapshot_reports_dir(
            store,
            snapshot_id,
            reports_root=canonical_reports_root,
        )
        for legacy_reports in (
            snapshot_dir / "reports",
            legacy_snapshot_reports_dir(store, snapshot_id),
        ):
            _move_tree_contents(
                source=legacy_reports,
                destination=canonical_snapshot_reports,
                dry_run=dry_run,
                actions=actions,
            )

        if not dry_run:
            repo.refresh_context_ref()

    nba_repo = NBARepository(
        odds_data_root=store.root,
        snapshot_id="layout-migration",
        snapshot_dir=store.root / "snapshots" / "layout-migration",
    )
    _move_file(
        source=store.root / "reference" / "player_identity_map.json",
        destination=nba_repo.identity_map_path(),
        dry_run=dry_run,
        actions=actions,
    )
    _move_tree_contents(
        source=store.root / "reference" / "injuries",
        destination=nba_repo.reference_dir / "injuries",
        dry_run=dry_run,
        actions=actions,
    )
    _move_tree_contents(
        source=store.root / "reference" / "rosters",
        destination=nba_repo.reference_dir / "rosters",
        dry_run=dry_run,
        actions=actions,
    )

    _move_tree_contents(
        source=store.root / "cache",
        destination=runtime_root / "odds_cache",
        dry_run=dry_run,
        actions=actions,
    )
    _move_tree_contents(
        source=store.root / "nba_cache",
        destination=runtime_root / "nba_cache",
        dry_run=dry_run,
        actions=actions,
    )
    _move_tree_contents(
        source=store.root / "llm_cache",
        destination=runtime_root / "llm_cache",
        dry_run=dry_run,
        actions=actions,
    )
    _move_tree_contents(
        source=store.root / "llm_usage",
        destination=runtime_root / "llm_usage",
        dry_run=dry_run,
        actions=actions,
    )

    status_counts: dict[str, int] = {}
    for row in actions:
        status = row["status"]
        status_counts[status] = status_counts.get(status, 0) + 1

    report = {
        "odds_root": str(store.root),
        "reports_root": str(canonical_reports_root),
        "runtime_root": str(runtime_root),
        "dry_run": dry_run,
        "actions": actions,
        "action_counts": status_counts,
    }
    if not dry_run:
        report["guardrails"] = build_guardrail_report(store.root)
    return report
