import json
from pathlib import Path

from prop_ev.cli import _resolve_bookmakers
from prop_ev.runtime_config import load_runtime_config, set_current_runtime_config


def _write_config(path: Path, *, enabled: bool, books: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "enabled": enabled,
        "bookmakers": books,
    }
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _set_runtime(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "runtime.toml"
    config_path.write_text(
        "\n".join(
            [
                "[paths]",
                'odds_data_dir = "odds_api"',
                'nba_data_dir = "nba_data"',
                'reports_dir = "reports/odds"',
                'runtime_dir = "runtime"',
                'bookmakers_config_path = "config/bookmakers.json"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    runtime = load_runtime_config(config_path)
    set_current_runtime_config(runtime)
    monkeypatch.chdir(tmp_path)


def test_resolve_bookmakers_uses_config_when_cli_empty(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config" / "bookmakers.json"
    _write_config(config_path, enabled=True, books=["draftkings", "fanduel"])
    _set_runtime(monkeypatch, tmp_path)

    try:
        value, source = _resolve_bookmakers("")
    finally:
        set_current_runtime_config(None)

    assert value == "draftkings,fanduel"
    assert source.startswith("config:")


def test_resolve_bookmakers_cli_overrides_config(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config" / "bookmakers.json"
    _write_config(config_path, enabled=True, books=["draftkings", "fanduel"])
    _set_runtime(monkeypatch, tmp_path)

    try:
        value, source = _resolve_bookmakers("betmgm")
    finally:
        set_current_runtime_config(None)

    assert value == "betmgm"
    assert source == "cli"


def test_resolve_bookmakers_config_disabled(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config" / "bookmakers.json"
    _write_config(config_path, enabled=False, books=["draftkings", "fanduel"])
    _set_runtime(monkeypatch, tmp_path)

    try:
        value, source = _resolve_bookmakers("")
    finally:
        set_current_runtime_config(None)

    assert value == ""
    assert source == "none"


def test_resolve_bookmakers_can_skip_config(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config" / "bookmakers.json"
    _write_config(config_path, enabled=True, books=["draftkings", "fanduel"])
    _set_runtime(monkeypatch, tmp_path)

    try:
        value, source = _resolve_bookmakers("", allow_config=False)
    finally:
        set_current_runtime_config(None)

    assert value == ""
    assert source == "none"
