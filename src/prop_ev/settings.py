"""Application settings for prop-ev."""

import os
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-driven settings for external service configuration."""

    model_config = SettingsConfigDict(
        env_prefix="PROP_EV_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    odds_api_key: str = Field(validation_alias=AliasChoices("ODDS_API_KEY", "PROP_EV_ODDS_API_KEY"))
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
    strategy_require_fresh_context: bool = True
    strategy_stale_quote_minutes: int = 20
    strategy_default_id: str = "s001"
    context_injuries_stale_hours: float = 6.0
    context_roster_stale_hours: float = 24.0
    data_dir: str = "data/odds_api"

    @classmethod
    def from_env(cls) -> "Settings":
        """Construct settings from environment/.env, with key-file fallback."""
        direct_key = (
            os.environ.get("ODDS_API_KEY", "").strip()
            or os.environ.get("PROP_EV_ODDS_API_KEY", "").strip()
        )
        if direct_key:
            return cls()  # pyright: ignore[reportCallIssue]

        candidates_raw = os.environ.get(
            "PROP_EV_ODDS_API_KEY_FILE_CANDIDATES",
            "ODDS_API_KEY,ODDS_API_KEY.ignore",
        )
        candidates = [item.strip() for item in candidates_raw.split(",") if item.strip()]
        cwd = Path.cwd()
        for candidate in candidates:
            path = cwd / candidate
            if not path.exists() or not path.is_file():
                continue
            try:
                first_line = path.read_text(encoding="utf-8").strip().splitlines()[0].strip()
            except (OSError, IndexError):
                continue
            if not first_line:
                continue
            if "=" in first_line:
                key_name, value = first_line.split("=", 1)
                if key_name.strip().upper() not in {"ODDS_API_KEY", "PROP_EV_ODDS_API_KEY"}:
                    continue
                parsed = value.strip().strip('"').strip("'")
            else:
                parsed = first_line.strip().strip('"').strip("'")
            if parsed:
                return cls(ODDS_API_KEY=parsed)  # pyright: ignore[reportCallIssue]

        return cls()  # pyright: ignore[reportCallIssue]
