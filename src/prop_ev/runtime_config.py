"""Runtime configuration loader (config-first, flag-overrides)."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "runtime.toml"
DEFAULT_LOCAL_OVERRIDE_PATH = Path(__file__).resolve().parents[2] / "config" / "runtime.local.toml"

MANAGED_ENV_KEYS: tuple[str, ...] = (
    "PROP_EV_DATA_DIR",
    "PROP_EV_NBA_DATA_DIR",
    "PROP_EV_REPORTS_DIR",
    "PROP_EV_RUNTIME_DIR",
    "PROP_EV_BOOKMAKERS_CONFIG_PATH",
    "PROP_EV_ODDS_API_BASE_URL",
    "PROP_EV_ODDS_API_TIMEOUT_S",
    "PROP_EV_ODDS_API_SPORT_KEY",
    "PROP_EV_ODDS_API_DEFAULT_REGIONS",
    "PROP_EV_ODDS_API_DEFAULT_MAX_CREDITS",
    "PROP_EV_ODDS_API_KEY_FILE_CANDIDATES",
    "PROP_EV_ODDS_MONTHLY_CAP_CREDITS",
    "PROP_EV_OPENAI_MODEL",
    "PROP_EV_OPENAI_TIMEOUT_S",
    "PROP_EV_OPENAI_KEY_FILE_CANDIDATES",
    "PROP_EV_LLM_MONTHLY_CAP_USD",
    "PROP_EV_PLAYBOOK_LIVE_WINDOW_PRE_TIP_H",
    "PROP_EV_PLAYBOOK_LIVE_WINDOW_POST_TIP_H",
    "PROP_EV_PLAYBOOK_TOP_N",
    "PROP_EV_PLAYBOOK_PER_GAME_TOP_N",
    "PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES",
    "PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES",
    "PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT",
    "PROP_EV_STRATEGY_STALE_QUOTE_MINUTES",
    "PROP_EV_STRATEGY_DEFAULT_ID",
    "PROP_EV_CONTEXT_INJURIES_STALE_HOURS",
    "PROP_EV_CONTEXT_ROSTER_STALE_HOURS",
)


@dataclass(frozen=True)
class RuntimeConfig:
    """Materialized runtime configuration."""

    config_path: Path
    odds_data_dir: Path
    nba_data_dir: Path
    reports_dir: Path
    runtime_dir: Path
    bookmakers_config_path: Path
    odds_api_base_url: str
    odds_api_timeout_s: float
    odds_api_sport_key: str
    odds_api_default_regions: str
    odds_api_default_max_credits: int
    odds_api_key_files: tuple[str, ...]
    odds_monthly_cap_credits: int
    openai_model: str
    openai_timeout_s: float
    openai_key_files: tuple[str, ...]
    llm_monthly_cap_usd: float
    playbook_live_window_pre_tip_h: int
    playbook_live_window_post_tip_h: int
    playbook_top_n: int
    playbook_per_game_top_n: int
    strategy_default_id: str
    strategy_require_official_injuries: bool
    strategy_allow_secondary_injuries: bool
    strategy_require_fresh_context: bool
    strategy_stale_quote_minutes: int
    context_injuries_stale_hours: float
    context_roster_stale_hours: float

    def with_path_overrides(
        self,
        *,
        odds_data_dir: Path | None = None,
        nba_data_dir: Path | None = None,
        reports_dir: Path | None = None,
        runtime_dir: Path | None = None,
    ) -> RuntimeConfig:
        """Return copy with explicit CLI path overrides applied."""
        resolved_odds = odds_data_dir or self.odds_data_dir
        resolved_nba = nba_data_dir or self.nba_data_dir
        resolved_reports = reports_dir or self.reports_dir
        resolved_runtime = runtime_dir or self.runtime_dir

        if odds_data_dir is not None:
            data_home = _data_home_from_odds_root(resolved_odds)
            if nba_data_dir is None:
                resolved_nba = data_home / "nba_data"
            if reports_dir is None:
                resolved_reports = data_home / "reports" / "odds"
            if runtime_dir is None:
                resolved_runtime = data_home / "runtime"

        return replace(
            self,
            odds_data_dir=resolved_odds,
            nba_data_dir=resolved_nba,
            reports_dir=resolved_reports,
            runtime_dir=resolved_runtime,
        )


def _data_home_from_odds_root(odds_root: Path) -> Path:
    root = odds_root.resolve()
    if root.name == "odds_api":
        return root.parent
    if root.name == "odds" and root.parent.name == "lakes":
        return root.parent.parent
    return root


_CURRENT_RUNTIME_CONFIG: RuntimeConfig | None = None


def set_current_runtime_config(config: RuntimeConfig | None) -> None:
    global _CURRENT_RUNTIME_CONFIG
    _CURRENT_RUNTIME_CONFIG = config


def current_runtime_config() -> RuntimeConfig:
    config = _CURRENT_RUNTIME_CONFIG
    if config is not None:
        return config
    loaded = load_runtime_config()
    set_current_runtime_config(loaded)
    return loaded


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in overlay.items():
        existing = out.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            out[key] = _deep_merge(existing, value)
        else:
            out[key] = value
    return out


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RuntimeError(f"failed reading runtime config: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise RuntimeError(f"invalid runtime config TOML: {path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"runtime config root must be a table: {path}")
    return payload


def _as_table(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise RuntimeError(f"runtime config section [{key}] must be a table")
    return value


def _as_str(value: Any, *, default: str) -> str:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    return default


def _as_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    return default


def _as_int(value: Any, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return default
    return default


def _as_float(value: Any, *, default: float) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return default
    return default


def _as_csv_list(values: Any, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(values, list):
        cleaned = [str(value).strip() for value in values if str(value).strip()]
        return tuple(cleaned) if cleaned else default
    if isinstance(values, str):
        cleaned = [part.strip() for part in values.split(",") if part.strip()]
        return tuple(cleaned) if cleaned else default
    return default


def _resolve_path(raw: Any, *, default: str, base_dir: Path) -> Path:
    value = _as_str(raw, default=default)
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def load_runtime_config(config_path: Path | None = None) -> RuntimeConfig:
    """Load runtime config from `config/runtime.toml` plus optional local override."""
    source = (config_path or DEFAULT_CONFIG_PATH).expanduser().resolve()
    if not source.exists():
        raise RuntimeError(f"runtime config file not found: {source}")

    payload = _read_toml(source)
    if source == DEFAULT_CONFIG_PATH and DEFAULT_LOCAL_OVERRIDE_PATH.exists():
        payload = _deep_merge(payload, _read_toml(DEFAULT_LOCAL_OVERRIDE_PATH))

    paths = _as_table(payload, "paths")
    odds_api = _as_table(payload, "odds_api")
    openai = _as_table(payload, "openai")
    playbook = _as_table(payload, "playbook")
    strategy = _as_table(payload, "strategy")
    base_dir = source.parent

    return RuntimeConfig(
        config_path=source,
        odds_data_dir=_resolve_path(
            paths.get("odds_data_dir"),
            default="data/odds_api",
            base_dir=base_dir,
        ),
        nba_data_dir=_resolve_path(
            paths.get("nba_data_dir"),
            default="data/nba_data",
            base_dir=base_dir,
        ),
        reports_dir=_resolve_path(
            paths.get("reports_dir"),
            default="reports",
            base_dir=base_dir,
        ),
        runtime_dir=_resolve_path(
            paths.get("runtime_dir"),
            default="runtime",
            base_dir=base_dir,
        ),
        bookmakers_config_path=_resolve_path(
            paths.get("bookmakers_config_path"),
            default="config/bookmakers.json",
            base_dir=base_dir,
        ),
        odds_api_base_url=_as_str(
            odds_api.get("base_url"),
            default="https://api.the-odds-api.com/v4",
        ),
        odds_api_timeout_s=_as_float(odds_api.get("timeout_s"), default=10.0),
        odds_api_sport_key=_as_str(odds_api.get("sport_key"), default="basketball_nba"),
        odds_api_default_regions=_as_str(odds_api.get("default_regions"), default="us"),
        odds_api_default_max_credits=_as_int(odds_api.get("default_max_credits"), default=20),
        odds_api_key_files=_as_csv_list(
            odds_api.get("key_files"),
            default=("ODDS_API_KEY_PAID.ignore", "ODDS_API_KEY.ignore", "ODDS_API_KEY"),
        ),
        odds_monthly_cap_credits=_as_int(odds_api.get("monthly_cap_credits"), default=450),
        openai_model=_as_str(openai.get("model"), default="gpt-5-mini"),
        openai_timeout_s=_as_float(openai.get("timeout_s"), default=60.0),
        openai_key_files=_as_csv_list(
            openai.get("key_files"),
            default=("OPENAI_KEY.ignore", "OPENAI_KEY"),
        ),
        llm_monthly_cap_usd=_as_float(openai.get("monthly_cap_usd"), default=5.0),
        playbook_live_window_pre_tip_h=_as_int(playbook.get("live_window_pre_tip_h"), default=3),
        playbook_live_window_post_tip_h=_as_int(playbook.get("live_window_post_tip_h"), default=1),
        playbook_top_n=_as_int(playbook.get("top_n"), default=5),
        playbook_per_game_top_n=_as_int(playbook.get("per_game_top_n"), default=5),
        strategy_default_id=_as_str(strategy.get("default_id"), default="s001"),
        strategy_require_official_injuries=_as_bool(
            strategy.get("require_official_injuries"),
            default=True,
        ),
        strategy_allow_secondary_injuries=_as_bool(
            strategy.get("allow_secondary_injuries"),
            default=False,
        ),
        strategy_require_fresh_context=_as_bool(
            strategy.get("require_fresh_context"), default=True
        ),
        strategy_stale_quote_minutes=_as_int(strategy.get("stale_quote_minutes"), default=20),
        context_injuries_stale_hours=_as_float(
            strategy.get("context_injuries_stale_hours"),
            default=6.0,
        ),
        context_roster_stale_hours=_as_float(
            strategy.get("context_roster_stale_hours"),
            default=24.0,
        ),
    )


def runtime_env_overrides(config: RuntimeConfig) -> dict[str, str]:
    """Project runtime config onto managed process env keys used by existing modules."""
    return {
        "PROP_EV_DATA_DIR": str(config.odds_data_dir),
        "PROP_EV_NBA_DATA_DIR": str(config.nba_data_dir),
        "PROP_EV_REPORTS_DIR": str(config.reports_dir),
        "PROP_EV_RUNTIME_DIR": str(config.runtime_dir),
        "PROP_EV_BOOKMAKERS_CONFIG_PATH": str(config.bookmakers_config_path),
        "PROP_EV_ODDS_API_BASE_URL": config.odds_api_base_url,
        "PROP_EV_ODDS_API_TIMEOUT_S": str(config.odds_api_timeout_s),
        "PROP_EV_ODDS_API_SPORT_KEY": config.odds_api_sport_key,
        "PROP_EV_ODDS_API_DEFAULT_REGIONS": config.odds_api_default_regions,
        "PROP_EV_ODDS_API_DEFAULT_MAX_CREDITS": str(config.odds_api_default_max_credits),
        "PROP_EV_ODDS_API_KEY_FILE_CANDIDATES": ",".join(config.odds_api_key_files),
        "PROP_EV_ODDS_MONTHLY_CAP_CREDITS": str(config.odds_monthly_cap_credits),
        "PROP_EV_OPENAI_MODEL": config.openai_model,
        "PROP_EV_OPENAI_TIMEOUT_S": str(config.openai_timeout_s),
        "PROP_EV_OPENAI_KEY_FILE_CANDIDATES": ",".join(config.openai_key_files),
        "PROP_EV_LLM_MONTHLY_CAP_USD": str(config.llm_monthly_cap_usd),
        "PROP_EV_PLAYBOOK_LIVE_WINDOW_PRE_TIP_H": str(config.playbook_live_window_pre_tip_h),
        "PROP_EV_PLAYBOOK_LIVE_WINDOW_POST_TIP_H": str(config.playbook_live_window_post_tip_h),
        "PROP_EV_PLAYBOOK_TOP_N": str(config.playbook_top_n),
        "PROP_EV_PLAYBOOK_PER_GAME_TOP_N": str(config.playbook_per_game_top_n),
        "PROP_EV_STRATEGY_DEFAULT_ID": config.strategy_default_id,
        "PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES": str(
            config.strategy_require_official_injuries
        ).lower(),
        "PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES": str(
            config.strategy_allow_secondary_injuries
        ).lower(),
        "PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT": str(
            config.strategy_require_fresh_context
        ).lower(),
        "PROP_EV_STRATEGY_STALE_QUOTE_MINUTES": str(config.strategy_stale_quote_minutes),
        "PROP_EV_CONTEXT_INJURIES_STALE_HOURS": str(config.context_injuries_stale_hours),
        "PROP_EV_CONTEXT_ROSTER_STALE_HOURS": str(config.context_roster_stale_hours),
    }
