"""Shared data-path resolution helpers for odds, reports, nba, and runtime roots."""

from __future__ import annotations

from pathlib import Path

from prop_ev.runtime_config import current_runtime_config

DEFAULT_ODDS_DATA_DIR = "data/odds_api"
DEFAULT_NBA_DATA_DIR = "data/nba_data"


def resolve_odds_data_root(value: str | Path | None = None) -> Path:
    """Resolve odds data root from explicit value, runtime config, then default."""
    if isinstance(value, Path):
        return value.expanduser()
    if isinstance(value, str) and value.strip():
        return Path(value.strip()).expanduser()
    try:
        return current_runtime_config().odds_data_dir.resolve()
    except RuntimeError:
        pass
    return Path(DEFAULT_ODDS_DATA_DIR)


def runtime_config_for_odds_root(odds_root: Path | str) -> Path | None:
    """Return runtime-configured odds root when it matches provided root scope."""
    try:
        configured = current_runtime_config().odds_data_dir.resolve()
    except RuntimeError:
        return None
    target = Path(odds_root).expanduser().resolve()
    if target == configured:
        return configured
    if data_home_from_odds_root(target) == data_home_from_odds_root(configured):
        return configured
    return None


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
    """Resolve runtime root from runtime config or canonical sibling location."""
    configured = runtime_config_for_odds_root(odds_root)
    if configured is not None:
        return current_runtime_config().runtime_dir.resolve()
    return data_home_from_odds_root(odds_root) / "runtime"


def resolve_nba_data_root(
    odds_root: Path | str,
    *,
    configured: Path | str | None = None,
) -> Path:
    """Resolve NBA data root with explicit/runtime override and sibling discovery."""
    configured_path = Path(configured).expanduser().resolve() if configured is not None else None

    if configured_path is not None and configured_path != Path(DEFAULT_NBA_DATA_DIR).resolve():
        return configured_path

    configured_odds_root = runtime_config_for_odds_root(odds_root)
    if configured_odds_root is not None:
        return current_runtime_config().nba_data_dir.resolve()

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
