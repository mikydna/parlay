"""Configuration helpers for nba-data CLI."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from prop_ev.runtime_config import current_runtime_config


@dataclass(frozen=True)
class NBADataConfig:
    """Resolved runtime configuration."""

    data_dir: Path
    stale_lock_minutes: int = 120


def resolve_data_dir(cli_value: str | None) -> Path:
    """Resolve nba-data root from CLI/runtime config/default."""
    if cli_value and cli_value.strip():
        return Path(cli_value.strip()).expanduser()
    try:
        return current_runtime_config().nba_data_dir.resolve()
    except RuntimeError:
        pass
    return Path("data/nba_data")


def load_config(*, data_dir: str | None) -> NBADataConfig:
    return NBADataConfig(data_dir=resolve_data_dir(data_dir))
