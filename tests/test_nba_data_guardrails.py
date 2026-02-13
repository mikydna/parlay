from __future__ import annotations

import ast
from pathlib import Path


def _nba_data_python_files() -> list[Path]:
    root = Path(__file__).resolve().parents[1] / "src" / "prop_ev" / "nba_data"
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def _runtime_python_files() -> list[Path]:
    root = Path(__file__).resolve().parents[1] / "src" / "prop_ev"
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def _imports_httpx(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) and any(alias.name == "httpx" for alias in node.names):
            return True
        if isinstance(node, ast.ImportFrom) and node.module == "httpx":
            return True
    return False


def test_nba_data_httpx_imports_are_gateway_only() -> None:
    allowed = {"gateway.py"}
    violations: list[str] = []
    for path in _nba_data_python_files():
        if path.name in allowed:
            continue
        if _imports_httpx(path):
            violations.append(str(path))
    assert not violations, "httpx import outside nba_data gateway: " + ", ".join(violations)


def test_nba_data_modules_do_not_import_context_sources() -> None:
    violations: list[str] = []
    for path in _nba_data_python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "prop_ev.context_sources":
                violations.append(str(path))
                break
            if isinstance(node, ast.Import) and any(
                alias.name == "prop_ev.context_sources" for alias in node.names
            ):
                violations.append(str(path))
                break
    assert not violations, "nba_data imports context_sources: " + ", ".join(violations)


def test_runtime_does_not_import_restricted_context_fetch_primitives() -> None:
    restricted = {
        "BOXSCORE_URL_TEMPLATE",
        "TODAYS_SCOREBOARD_URL",
        "fetch_official_injury_links",
        "fetch_secondary_injuries",
        "fetch_roster_context",
        "load_or_fetch_context",
    }
    violations: list[str] = []
    for path in _runtime_python_files():
        if path.name == "context_sources.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.module != "prop_ev.context_sources":
                continue
            names = {alias.name for alias in node.names}
            if "*" in names or names & restricted:
                violations.append(str(path))
                break
    assert not violations, "restricted context_sources imports outside module: " + ", ".join(
        violations
    )
