"""Global CLI flag extraction helpers."""

from __future__ import annotations


def extract_global_overrides(argv: list[str]) -> tuple[list[str], str, str, str, str, str]:
    cleaned: list[str] = []
    config_path = ""
    data_dir = ""
    reports_dir = ""
    nba_data_dir = ""
    runtime_dir = ""
    idx = 0
    while idx < len(argv):
        token = argv[idx]
        if token == "--config":
            if idx + 1 >= len(argv):
                raise RuntimeError("--config requires a value")
            config_path = str(argv[idx + 1]).strip()
            idx += 2
            continue
        if token.startswith("--config="):
            config_path = token.split("=", 1)[1].strip()
            idx += 1
            continue
        if token == "--data-dir":
            if idx + 1 >= len(argv):
                raise RuntimeError("--data-dir requires a value")
            data_dir = str(argv[idx + 1]).strip()
            idx += 2
            continue
        if token.startswith("--data-dir="):
            data_dir = token.split("=", 1)[1].strip()
            idx += 1
            continue
        if token == "--reports-dir":
            if idx + 1 >= len(argv):
                raise RuntimeError("--reports-dir requires a value")
            reports_dir = str(argv[idx + 1]).strip()
            idx += 2
            continue
        if token.startswith("--reports-dir="):
            reports_dir = token.split("=", 1)[1].strip()
            idx += 1
            continue
        if token == "--nba-data-dir":
            if idx + 1 >= len(argv):
                raise RuntimeError("--nba-data-dir requires a value")
            nba_data_dir = str(argv[idx + 1]).strip()
            idx += 2
            continue
        if token.startswith("--nba-data-dir="):
            nba_data_dir = token.split("=", 1)[1].strip()
            idx += 1
            continue
        if token == "--runtime-dir":
            if idx + 1 >= len(argv):
                raise RuntimeError("--runtime-dir requires a value")
            runtime_dir = str(argv[idx + 1]).strip()
            idx += 2
            continue
        if token.startswith("--runtime-dir="):
            runtime_dir = token.split("=", 1)[1].strip()
            idx += 1
            continue
        cleaned.append(token)
        idx += 1
    return cleaned, config_path, data_dir, reports_dir, nba_data_dir, runtime_dir
