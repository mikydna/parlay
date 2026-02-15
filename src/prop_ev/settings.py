"""Application settings for prop-ev."""

import os
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from prop_ev.runtime_config import current_runtime_config


class Settings(BaseSettings):
    """Runtime settings for external service configuration."""

    model_config = SettingsConfigDict(
        env_prefix="PROP_EV_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    odds_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("ODDS_API_KEY", "PROP_EV_ODDS_API_KEY"),
    )
    odds_api_base_url: str = "https://api.the-odds-api.com/v4"
    odds_api_timeout_s: float = 10.0
    odds_api_sport_key: str = "basketball_nba"
    odds_api_default_regions: str = "us"
    odds_api_default_max_credits: int = 20
    odds_api_key_file_candidates: str = "ODDS_API_KEY,ODDS_API_KEY.ignore"
    odds_monthly_cap_credits: int = 450
    openai_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("OPENAI_API_KEY", "PROP_EV_OPENAI_API_KEY"),
    )
    openai_model: str = "gpt-5-mini"
    openai_timeout_s: float = 60.0
    llm_monthly_cap_usd: float = 5.0
    openai_key_file_candidates: str = "OPENAI_KEY,OPENAI_KEY.ignore"
    playbook_live_window_pre_tip_h: int = 3
    playbook_live_window_post_tip_h: int = 1
    playbook_top_n: int = 5
    playbook_per_game_top_n: int = 5
    strategy_require_official_injuries: bool = True
    strategy_allow_secondary_injuries: bool = False
    strategy_require_fresh_context: bool = True
    strategy_stale_quote_minutes: int = 20
    strategy_max_picks_default: int = 5
    strategy_default_id: str = "s001"
    strategy_probabilistic_profile: str = "off"
    context_injuries_stale_hours: float = 6.0
    context_roster_stale_hours: float = 24.0
    data_dir: str = "data/odds_api"

    @staticmethod
    def _parse_key_file(path: Path, *, allowed_names: set[str]) -> str:
        try:
            raw = path.read_text(encoding="utf-8").strip()
        except OSError:
            return ""
        if not raw:
            return ""
        first_line = raw.splitlines()[0].strip()
        if not first_line:
            return ""
        if "=" in first_line:
            key_name, value = first_line.split("=", 1)
            if key_name.strip().upper() not in allowed_names:
                return ""
            return value.strip().strip('"').strip("'")
        return first_line.strip().strip('"').strip("'")

    @classmethod
    def from_runtime(cls) -> "Settings":
        """Construct settings from runtime config + direct secret env/key-file fallback."""
        runtime = current_runtime_config()
        config_root = runtime.config_path.parent.resolve()

        direct_odds_key = (
            os.environ.get("ODDS_API_KEY", "").strip()
            or os.environ.get("PROP_EV_ODDS_API_KEY", "").strip()
        )
        resolved_odds_key = direct_odds_key
        if not resolved_odds_key:
            for candidate in runtime.odds_api_key_files:
                candidate_path = Path(candidate).expanduser()
                path = (
                    candidate_path
                    if candidate_path.is_absolute()
                    else (config_root / candidate_path).resolve()
                )
                if not path.exists() or not path.is_file():
                    continue
                parsed = cls._parse_key_file(
                    path,
                    allowed_names={"ODDS_API_KEY", "PROP_EV_ODDS_API_KEY"},
                )
                if parsed:
                    resolved_odds_key = parsed
                    break

        direct_openai_key = (
            os.environ.get("OPENAI_API_KEY", "").strip()
            or os.environ.get("PROP_EV_OPENAI_API_KEY", "").strip()
        )

        return cls(
            odds_api_key=resolved_odds_key,
            odds_api_base_url=runtime.odds_api_base_url,
            odds_api_timeout_s=runtime.odds_api_timeout_s,
            odds_api_sport_key=runtime.odds_api_sport_key,
            odds_api_default_regions=runtime.odds_api_default_regions,
            odds_api_default_max_credits=runtime.odds_api_default_max_credits,
            odds_api_key_file_candidates=",".join(runtime.odds_api_key_files),
            odds_monthly_cap_credits=runtime.odds_monthly_cap_credits,
            openai_api_key=direct_openai_key,
            openai_model=runtime.openai_model,
            openai_timeout_s=runtime.openai_timeout_s,
            llm_monthly_cap_usd=runtime.llm_monthly_cap_usd,
            openai_key_file_candidates=",".join(runtime.openai_key_files),
            playbook_live_window_pre_tip_h=runtime.playbook_live_window_pre_tip_h,
            playbook_live_window_post_tip_h=runtime.playbook_live_window_post_tip_h,
            playbook_top_n=runtime.playbook_top_n,
            playbook_per_game_top_n=runtime.playbook_per_game_top_n,
            strategy_default_id=runtime.strategy_default_id,
            strategy_require_official_injuries=runtime.strategy_require_official_injuries,
            strategy_allow_secondary_injuries=runtime.strategy_allow_secondary_injuries,
            strategy_require_fresh_context=runtime.strategy_require_fresh_context,
            strategy_stale_quote_minutes=runtime.strategy_stale_quote_minutes,
            strategy_max_picks_default=runtime.strategy_max_picks_default,
            strategy_probabilistic_profile=runtime.strategy_probabilistic_profile,
            context_injuries_stale_hours=runtime.context_injuries_stale_hours,
            context_roster_stale_hours=runtime.context_roster_stale_hours,
            data_dir=str(runtime.odds_data_dir),
        )
