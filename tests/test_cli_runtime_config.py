from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from prop_ev.cli import main


def _write_runtime_config(path: Path, *, odds_dir: Path, nba_dir: Path) -> None:
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
                "",
                "[openai]",
                'key_files = ["OPENAI_KEY.ignore"]',
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
