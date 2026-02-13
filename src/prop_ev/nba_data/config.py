"""Configuration helpers for nba-data CLI."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class NBADataConfig:
    """Resolved runtime configuration."""

    data_dir: Path
    stale_lock_minutes: int = 120


def resolve_data_dir(cli_value: str | None) -> Path:
    """Resolve nba-data root from CLI/env/default."""
    if cli_value and cli_value.strip():
        return Path(cli_value.strip()).expanduser()
    env_value = os.environ.get("PROP_EV_NBA_DATA_DIR", "").strip()
    if env_value:
        return Path(env_value).expanduser()
    return Path("data/nba_data")


def load_config(*, data_dir: str | None) -> NBADataConfig:
    return NBADataConfig(data_dir=resolve_data_dir(data_dir))
