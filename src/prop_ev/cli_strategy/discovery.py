"""Discovery/execution report helpers for strategy CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from prop_ev.discovery_execution import (
    build_discovery_execution_report,
    write_discovery_execution_reports,
)
from prop_ev.storage import SnapshotStore


def _build_discovery_execution_report(
    *,
    discovery_snapshot_id: str,
    execution_snapshot_id: str,
    discovery_report: dict[str, Any],
    execution_report: dict[str, Any],
    top_n: int,
) -> dict[str, Any]:
    return build_discovery_execution_report(
        discovery_snapshot_id=discovery_snapshot_id,
        execution_snapshot_id=execution_snapshot_id,
        discovery_report=discovery_report,
        execution_report=execution_report,
        top_n=top_n,
    )


def _write_discovery_execution_reports(
    *,
    store: SnapshotStore,
    execution_snapshot_id: str,
    report: dict[str, Any],
    write_markdown: bool = False,
) -> tuple[Path, Path | None]:
    return write_discovery_execution_reports(
        store=store,
        execution_snapshot_id=execution_snapshot_id,
        report=report,
        write_markdown=write_markdown,
    )
