"""Runtime-config-driven CLI helpers."""

from __future__ import annotations

import json
from pathlib import Path

from prop_ev.odds_client import parse_csv
from prop_ev.runtime_config import RuntimeConfig, current_runtime_config


def load_bookmaker_whitelist(path: Path) -> list[str]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return []
    enabled = payload.get("enabled", True)
    if isinstance(enabled, bool) and not enabled:
        return []
    raw_books = payload.get("bookmakers", [])
    if not isinstance(raw_books, list):
        return []
    books: list[str] = []
    for value in raw_books:
        if not isinstance(value, str):
            continue
        normalized = value.strip()
        if normalized:
            books.append(normalized)
    return list(dict.fromkeys(books))


def resolve_bookmakers(explicit: str, *, allow_config: bool = True) -> tuple[str, str]:
    explicit_books = parse_csv(explicit)
    if explicit_books:
        return ",".join(explicit_books), "cli"

    if not allow_config:
        return "", "none"

    config_path = current_runtime_config().bookmakers_config_path.resolve()
    books = load_bookmaker_whitelist(config_path)
    if books:
        return ",".join(books), f"config:{config_path}"
    return "", "none"


def runtime_config() -> RuntimeConfig:
    return current_runtime_config()


def runtime_odds_data_dir() -> str:
    return str(runtime_config().odds_data_dir)


def runtime_nba_data_dir() -> str:
    return str(runtime_config().nba_data_dir)


def runtime_runtime_dir() -> str:
    return str(runtime_config().runtime_dir)


def runtime_strategy_probabilistic_profile() -> str:
    return runtime_config().strategy_probabilistic_profile


def runtime_odds_api_default_max_credits() -> int:
    return int(runtime_config().odds_api_default_max_credits)


def runtime_override_value(name: str) -> bool | int | float | str | None:
    config = runtime_config()
    value_map: dict[str, bool | int | float | str] = {
        "PROP_EV_ODDS_API_DEFAULT_MAX_CREDITS": config.odds_api_default_max_credits,
        "PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES": config.strategy_require_official_injuries,
        "PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES": config.strategy_allow_secondary_injuries,
        "PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT": config.strategy_require_fresh_context,
        "PROP_EV_STRATEGY_STALE_QUOTE_MINUTES": config.strategy_stale_quote_minutes,
        "PROP_EV_STRATEGY_MAX_PICKS_DEFAULT": config.strategy_max_picks_default,
        "PROP_EV_STRATEGY_PROBABILISTIC_PROFILE": config.strategy_probabilistic_profile,
        "PROP_EV_STRATEGY_ROLLING_PRIOR_WINDOW_DAYS": config.strategy_rolling_prior_window_days,
        "PROP_EV_STRATEGY_ROLLING_PRIOR_MIN_SAMPLES": config.strategy_rolling_prior_min_samples,
        "PROP_EV_STRATEGY_ROLLING_PRIOR_MAX_DELTA": config.strategy_rolling_prior_max_delta,
        "PROP_EV_STRATEGY_CALIBRATION_BIN_SIZE": config.strategy_calibration_bin_size,
        "PROP_EV_STRATEGY_CALIBRATION_MIN_BIN_SAMPLES": config.strategy_calibration_min_bin_samples,
        "PROP_EV_STRATEGY_CALIBRATION_MAX_DELTA": config.strategy_calibration_max_delta,
        "PROP_EV_STRATEGY_CALIBRATION_SHRINK_K": config.strategy_calibration_shrink_k,
        "PROP_EV_STRATEGY_CALIBRATION_BUCKET_WEIGHT": config.strategy_calibration_bucket_weight,
        "PROP_EV_CONTEXT_INJURIES_STALE_HOURS": config.context_injuries_stale_hours,
        "PROP_EV_CONTEXT_ROSTER_STALE_HOURS": config.context_roster_stale_hours,
    }
    return value_map.get(name)


def env_bool_from_runtime(name: str, default: bool) -> bool:
    runtime_value = runtime_override_value(name)
    if isinstance(runtime_value, bool):
        return runtime_value
    if runtime_value is not None:
        return str(runtime_value).strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def env_int_from_runtime(name: str, default: int) -> int:
    runtime_value = runtime_override_value(name)
    if isinstance(runtime_value, bool):
        return default
    if isinstance(runtime_value, int):
        return runtime_value
    if runtime_value is not None:
        try:
            return int(str(runtime_value).strip())
        except ValueError:
            return default
    return default


def env_float_from_runtime(name: str, default: float) -> float:
    runtime_value = runtime_override_value(name)
    if isinstance(runtime_value, bool):
        return default
    if isinstance(runtime_value, (int, float)):
        return float(runtime_value)
    if runtime_value is not None:
        try:
            return float(str(runtime_value).strip())
        except ValueError:
            return default
    return default
