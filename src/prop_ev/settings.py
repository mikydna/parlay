"""Application settings for prop-ev."""

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
