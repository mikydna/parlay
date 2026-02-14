from __future__ import annotations

from pathlib import Path

from prop_ev.runtime_config import DEFAULT_CONFIG_PATH, load_runtime_config, runtime_env_overrides


def test_load_runtime_config_resolves_relative_paths(tmp_path: Path) -> None:
    config_path = tmp_path / "runtime.toml"
    config_path.write_text(
        "\n".join(
            [
                "[paths]",
                'odds_data_dir = "odds_api"',
                'nba_data_dir = "nba_data"',
                'reports_dir = "reports/odds"',
                'runtime_dir = "runtime"',
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

    config = load_runtime_config(config_path)

    assert config.odds_data_dir == (tmp_path / "odds_api").resolve()
    assert config.nba_data_dir == (tmp_path / "nba_data").resolve()
    assert config.reports_dir == (tmp_path / "reports" / "odds").resolve()
    assert config.runtime_dir == (tmp_path / "runtime").resolve()
    assert config.bookmakers_config_path == (tmp_path / "bookmakers.json").resolve()


def test_runtime_env_overrides_projects_key_fields(tmp_path: Path) -> None:
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
                'key_files = ["ODDS_API_KEY.ignore"]',
                "",
                "[openai]",
                'key_files = ["OPENAI_KEY.ignore"]',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config = load_runtime_config(config_path)
    projected = runtime_env_overrides(config)

    assert projected["PROP_EV_DATA_DIR"] == str((tmp_path / "odds_api").resolve())
    assert projected["PROP_EV_NBA_DATA_DIR"] == str((tmp_path / "nba_data").resolve())
    assert projected["PROP_EV_REPORTS_DIR"] == str((tmp_path / "reports" / "odds").resolve())
    assert projected["PROP_EV_RUNTIME_DIR"] == str((tmp_path / "runtime").resolve())
    assert projected["PROP_EV_ODDS_API_KEY_FILE_CANDIDATES"] == "ODDS_API_KEY.ignore"


def test_default_runtime_config_bookmakers_path_is_repo_config() -> None:
    config = load_runtime_config(DEFAULT_CONFIG_PATH)
    expected = (DEFAULT_CONFIG_PATH.parent / "bookmakers.json").resolve()
    assert config.bookmakers_config_path == expected
