from pathlib import Path

import pytest

from prop_ev.llm_client import (
    LLMBudgetExceededError,
    LLMClient,
    MissingOpenAIKeyError,
    resolve_openai_api_key,
)
from prop_ev.runtime_config import load_runtime_config, set_current_runtime_config
from prop_ev.settings import Settings


def test_llm_budget_cap_blocks_live_calls(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test")
    monkeypatch.setenv("PROP_EV_LLM_MONTHLY_CAP_USD", "0")
    settings = Settings(_env_file=None)

    called = {"value": False}

    def fake_post(url: str, headers: dict[str, str], payload: dict, timeout: float) -> dict:
        called["value"] = True
        return {"output_text": "x", "usage": {}}

    client = LLMClient(settings=settings, data_root=tmp_path / "data", post_fn=fake_post)
    with pytest.raises(LLMBudgetExceededError):
        client.cached_completion(
            task="playbook_pass1",
            prompt_version="v1",
            prompt="hello",
            payload={"a": 1},
            snapshot_id="snap-1",
            model="gpt-5-mini",
            max_output_tokens=100,
            temperature=0.1,
            refresh=False,
            offline=False,
        )
    assert called["value"] is False


def test_openai_key_missing_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_OPENAI_API_KEY", raising=False)
    settings = Settings(_env_file=None)

    with pytest.raises(MissingOpenAIKeyError):
        resolve_openai_api_key(settings, root=tmp_path)


def test_openai_key_file_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_OPENAI_API_KEY", raising=False)
    key_file = tmp_path / "OPENAI_KEY.ignore"
    key_file.write_text("test-file-key\n", encoding="utf-8")
    settings = Settings(_env_file=None)

    resolved = resolve_openai_api_key(settings, root=tmp_path)
    assert resolved == "test-file-key"


def test_openai_key_file_resolution_is_relative_to_runtime_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_OPENAI_API_KEY", raising=False)
    config_dir = tmp_path / "config-root"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "runtime.toml"
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
    (config_dir / "OPENAI_KEY.ignore").write_text("OPENAI_API_KEY=config-key\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    runtime = load_runtime_config(config_path)
    set_current_runtime_config(runtime)
    try:
        monkeypatch.setenv("ODDS_API_KEY", "odds-test")
        settings = Settings.from_runtime()
        resolved = resolve_openai_api_key(settings)
    finally:
        set_current_runtime_config(None)

    assert resolved == "config-key"
