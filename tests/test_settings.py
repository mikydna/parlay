import pytest
from pydantic import ValidationError

from prop_ev.settings import Settings


def test_settings_load_with_odds_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "test-key")

    settings = Settings(_env_file=None)

    assert settings.odds_api_key == "test-key"
    assert settings.odds_api_base_url == "https://api.the-odds-api.com/v4"
    assert settings.odds_api_timeout_s == 10.0


def test_settings_error_when_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_ODDS_API_KEY", raising=False)

    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    err = str(exc_info.value)
    assert "ODDS_API_KEY" in err or "odds_api_key" in err
