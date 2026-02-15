from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import prop_ev.cli as cli_module
from prop_ev.cli import main
from prop_ev.cli_config import env_bool_from_runtime, env_int_from_runtime
from prop_ev.runtime_config import load_runtime_config, set_current_runtime_config


def _write_runtime_config(
    path: Path,
    *,
    odds_dir: Path,
    nba_dir: Path,
    default_max_credits: int = 20,
    require_fresh_context: bool = True,
) -> None:
    fresh_context_value = "true" if require_fresh_context else "false"
    path.write_text(
        "\n".join(
            [
                "[paths]",
                f'odds_data_dir = "{odds_dir}"',
                f'nba_data_dir = "{nba_dir}"',
                f'reports_dir = "{odds_dir.parent / "reports" / "odds"}"',
                f'runtime_dir = "{odds_dir.parent / "runtime"}"',
                'bookmakers_config_path = "bookmakers.json"',
                "",
                "[odds_api]",
                'key_files = ["ODDS_API_KEY.ignore"]',
                f"default_max_credits = {default_max_credits}",
                "",
                "[openai]",
                'key_files = ["OPENAI_KEY.ignore"]',
                "",
                "[strategy]",
                f"require_fresh_context = {fresh_context_value}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_cli_reads_data_root_from_runtime_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    odds_dir = tmp_path / "parlay-data" / "odds_api"
    nba_dir = tmp_path / "parlay-data" / "nba_data"
    odds_dir.mkdir(parents=True)
    nba_dir.mkdir(parents=True)
    config_path = tmp_path / "runtime.toml"
    _write_runtime_config(config_path, odds_dir=odds_dir, nba_dir=nba_dir)
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(tmp_path / "wrong" / "odds_api"))

    code = main(["--config", str(config_path), "data", "guardrails", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["odds_root"] == str(odds_dir.resolve())


def test_data_dir_flag_overrides_runtime_config(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    odds_dir = tmp_path / "parlay-data" / "odds_api"
    nba_dir = tmp_path / "parlay-data" / "nba_data"
    override_odds_dir = tmp_path / "override" / "odds_api"
    odds_dir.mkdir(parents=True)
    nba_dir.mkdir(parents=True)
    override_odds_dir.mkdir(parents=True)
    config_path = tmp_path / "runtime.toml"
    _write_runtime_config(config_path, odds_dir=odds_dir, nba_dir=nba_dir)

    code = main(
        [
            "--config",
            str(config_path),
            "--data-dir",
            str(override_odds_dir),
            "data",
            "guardrails",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["odds_root"] == str(override_odds_dir.resolve())


def test_cli_keeps_env_api_keys_for_settings_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    odds_dir = tmp_path / "parlay-data" / "odds_api"
    nba_dir = tmp_path / "parlay-data" / "nba_data"
    odds_dir.mkdir(parents=True)
    nba_dir.mkdir(parents=True)
    config_path = tmp_path / "runtime.toml"
    _write_runtime_config(config_path, odds_dir=odds_dir, nba_dir=nba_dir)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace('key_files = ["ODDS_API_KEY.ignore"]', 'key_files = ["DO_NOT_EXIST"]')
        .replace('key_files = ["OPENAI_KEY.ignore"]', 'key_files = ["ALSO_MISSING"]'),
        encoding="utf-8",
    )
    monkeypatch.setenv("ODDS_API_KEY", "env-odds-key")
    monkeypatch.setenv("OPENAI_API_KEY", "env-openai-key")

    code = main(["--config", str(config_path), "playbook", "budget"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert "odds" in payload
    assert os.environ.get("ODDS_API_KEY") == "env-odds-key"
    assert os.environ.get("OPENAI_API_KEY") == "env-openai-key"


def test_cli_parser_uses_runtime_max_credits_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    odds_dir = tmp_path / "parlay-data" / "odds_api"
    nba_dir = tmp_path / "parlay-data" / "nba_data"
    odds_dir.mkdir(parents=True)
    nba_dir.mkdir(parents=True)
    config_path = tmp_path / "runtime.toml"
    _write_runtime_config(
        config_path,
        odds_dir=odds_dir,
        nba_dir=nba_dir,
        default_max_credits=77,
    )
    runtime_config = load_runtime_config(config_path)
    monkeypatch.setenv("PROP_EV_ODDS_API_DEFAULT_MAX_CREDITS", "5")
    set_current_runtime_config(runtime_config)
    try:
        parser = cli_module._build_parser()
        args = parser.parse_args(["data", "backfill"])
    finally:
        set_current_runtime_config(None)
    assert args.max_credits == 77


def test_cli_runtime_overrides_do_not_fallback_to_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    odds_dir = tmp_path / "parlay-data" / "odds_api"
    nba_dir = tmp_path / "parlay-data" / "nba_data"
    odds_dir.mkdir(parents=True)
    nba_dir.mkdir(parents=True)
    config_path = tmp_path / "runtime.toml"
    _write_runtime_config(
        config_path,
        odds_dir=odds_dir,
        nba_dir=nba_dir,
        require_fresh_context=False,
    )
    runtime_config = load_runtime_config(config_path)
    monkeypatch.setenv("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", "true")
    monkeypatch.setenv("TEST_INT_SHADOW", "99")
    set_current_runtime_config(runtime_config)
    try:
        resolved_fresh_context = env_bool_from_runtime(
            "PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT",
            default=True,
        )
        unmanaged_value = env_int_from_runtime("TEST_INT_SHADOW", default=11)
    finally:
        set_current_runtime_config(None)

    assert resolved_fresh_context is False
    assert unmanaged_value == 11
