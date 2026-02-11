import json
from pathlib import Path

from prop_ev.cli import _resolve_bookmakers


def _write_config(path: Path, *, enabled: bool, books: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "enabled": enabled,
        "bookmakers": books,
    }
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def test_resolve_bookmakers_uses_config_when_cli_empty(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config" / "bookmakers.json"
    _write_config(config_path, enabled=True, books=["draftkings", "fanduel"])
    monkeypatch.chdir(tmp_path)

    value, source = _resolve_bookmakers("")

    assert value == "draftkings,fanduel"
    assert source.startswith("config:")


def test_resolve_bookmakers_cli_overrides_config(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config" / "bookmakers.json"
    _write_config(config_path, enabled=True, books=["draftkings", "fanduel"])
    monkeypatch.chdir(tmp_path)

    value, source = _resolve_bookmakers("betmgm")

    assert value == "betmgm"
    assert source == "cli"


def test_resolve_bookmakers_config_disabled(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config" / "bookmakers.json"
    _write_config(config_path, enabled=False, books=["draftkings", "fanduel"])
    monkeypatch.chdir(tmp_path)

    value, source = _resolve_bookmakers("")

    assert value == ""
    assert source == "none"


def test_resolve_bookmakers_can_skip_config(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config" / "bookmakers.json"
    _write_config(config_path, enabled=True, books=["draftkings", "fanduel"])
    monkeypatch.chdir(tmp_path)

    value, source = _resolve_bookmakers("", allow_config=False)

    assert value == ""
    assert source == "none"
