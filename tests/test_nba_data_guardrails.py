from __future__ import annotations

import ast
from pathlib import Path


def _nba_data_python_files() -> list[Path]:
    root = Path(__file__).resolve().parents[1] / "src" / "prop_ev" / "nba_data"
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
