"""Shared data-path resolution helpers for odds, reports, nba, and runtime roots."""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_ODDS_DATA_DIR = "data/odds_api"
DEFAULT_NBA_DATA_DIR = "data/nba_data"


def resolve_odds_data_root(value: str | Path | None = None) -> Path:
    """Resolve odds data root from explicit value, env, then default."""
    if isinstance(value, Path):
        return value.expanduser()
    if isinstance(value, str) and value.strip():
        return Path(value.strip()).expanduser()
    env_value = os.environ.get("PROP_EV_DATA_DIR", "").strip()
    if env_value:
        return Path(env_value).expanduser()
    return Path(DEFAULT_ODDS_DATA_DIR)


def data_home_from_odds_root(odds_root: Path | str) -> Path:
    """Resolve parlay-data home from an odds root path."""
    root = Path(odds_root).expanduser().resolve()
    if root.name == "odds_api":
        return root.parent
    if root.name == "odds" and root.parent.name == "lakes":
        return root.parent.parent
    return root


def canonical_reports_root(odds_root: Path | str) -> Path:
    """Return canonical reports namespace root for odds outputs."""
    return data_home_from_odds_root(odds_root) / "reports" / "odds"


def resolve_runtime_root(odds_root: Path | str) -> Path:
    """Resolve runtime root from env override or canonical sibling location."""
    override = os.environ.get("PROP_EV_RUNTIME_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return data_home_from_odds_root(odds_root) / "runtime"


def resolve_nba_data_root(
    odds_root: Path | str,
    *,
    configured: Path | str | None = None,
) -> Path:
    """Resolve NBA data root with explicit/env override and sibling discovery."""
    configured_path = Path(configured).expanduser().resolve() if configured is not None else None

    env_value = os.environ.get("PROP_EV_NBA_DATA_DIR", "").strip()
    if env_value:
        return Path(env_value).expanduser().resolve()

    if configured_path is not None and configured_path != Path(DEFAULT_NBA_DATA_DIR).resolve():
        return configured_path

    root = Path(odds_root).expanduser().resolve()
    data_home = data_home_from_odds_root(root)
    lake_candidate = data_home / "lakes" / "nba"
    if lake_candidate.exists():
        return lake_candidate.resolve()
    legacy_candidate = data_home / "nba_data"
    if legacy_candidate.exists():
        return legacy_candidate.resolve()
    if root.name == "odds" and root.parent.name == "lakes":
        return lake_candidate
    return legacy_candidate


def canonical_context_dir(nba_data_root: Path | str, snapshot_id: str) -> Path:
    """Return canonical NBA-owned context directory for one snapshot."""
    return Path(nba_data_root).resolve() / "context" / "snapshots" / snapshot_id
