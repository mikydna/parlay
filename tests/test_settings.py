from pathlib import Path

import pytest
from pydantic import ValidationError

from prop_ev.settings import Settings


def test_settings_load_with_odds_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "test-key")

    settings = Settings(_env_file=None)

    assert settings.odds_api_key == "test-key"
    assert settings.odds_api_base_url == "https://api.the-odds-api.com/v4"
    assert settings.odds_api_timeout_s == 10.0
    assert settings.odds_api_sport_key == "basketball_nba"
    assert settings.openai_model == "gpt-5-mini"
    assert settings.openai_timeout_s == 60.0
    assert settings.llm_monthly_cap_usd == 5.0
    assert settings.odds_monthly_cap_credits == 450
    assert settings.strategy_require_official_injuries is True
    assert settings.strategy_require_fresh_context is True
    assert settings.strategy_stale_quote_minutes == 20
    assert settings.data_dir == "data/odds_api"


def test_settings_error_when_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_ODDS_API_KEY", raising=False)

    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    err = str(exc_info.value)
    assert "ODDS_API_KEY" in err or "odds_api_key" in err


def test_settings_from_env_uses_key_file_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_ODDS_API_KEY", raising=False)
    monkeypatch.setenv("PROP_EV_ODDS_API_KEY_FILE_CANDIDATES", "ODDS_API_KEY")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "ODDS_API_KEY").write_text("file-key\n", encoding="utf-8")

    settings = Settings.from_env()

    assert settings.odds_api_key == "file-key"
