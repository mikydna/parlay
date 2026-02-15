from pathlib import Path

import pytest

from prop_ev.runtime_config import load_runtime_config, set_current_runtime_config
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
    assert settings.strategy_max_picks_default == 5
    assert settings.strategy_probabilistic_profile == "off"
    assert settings.data_dir == "data/odds_api"


def test_settings_allows_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_ODDS_API_KEY", raising=False)

    settings = Settings(_env_file=None)
    assert settings.odds_api_key == ""


def test_settings_from_runtime_uses_key_file_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_ODDS_API_KEY", raising=False)
    config_path = tmp_path / "runtime.toml"
    config_path.write_text(
        "\n".join(
            [
                "[paths]",
                'odds_data_dir = "odds_api"',
                'nba_data_dir = "nba_data"',
                'reports_dir = "reports/odds"',
                'runtime_dir = "runtime"',
                "",
                "[odds_api]",
                'key_files = ["ODDS_API_KEY"]',
                "",
                "[openai]",
                'key_files = ["OPENAI_KEY.ignore"]',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "ODDS_API_KEY").write_text("file-key\n", encoding="utf-8")

    runtime_config = load_runtime_config(config_path)
    set_current_runtime_config(runtime_config)
    try:
        settings = Settings.from_runtime()
    finally:
        set_current_runtime_config(None)

    assert settings.odds_api_key == "file-key"


def test_settings_from_runtime_resolves_key_relative_to_config_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_ODDS_API_KEY", raising=False)
    nested = tmp_path / "nested"
    nested.mkdir(parents=True, exist_ok=True)
    config_path = nested / "runtime.toml"
    config_path.write_text(
        "\n".join(
            [
                "[paths]",
                'odds_data_dir = "odds_api"',
                'nba_data_dir = "nba_data"',
                'reports_dir = "reports/odds"',
                'runtime_dir = "runtime"',
                "",
                "[odds_api]",
                'key_files = ["ODDS_API_KEY.ignore"]',
                "",
                "[openai]",
                'key_files = ["OPENAI_KEY.ignore"]',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (nested / "ODDS_API_KEY.ignore").write_text("relative-key\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    runtime_config = load_runtime_config(config_path)
    set_current_runtime_config(runtime_config)
    try:
        settings = Settings.from_runtime()
    finally:
        set_current_runtime_config(None)

    assert settings.odds_api_key == "relative-key"
