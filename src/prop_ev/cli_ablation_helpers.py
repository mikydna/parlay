"""Helpers shared by strategy ablation command flow."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path
from shutil import rmtree
from typing import Any


def parse_positive_int_csv(value: str, *, default: list[int], flag_name: str) -> list[int]:
    raw = [item.strip() for item in value.split(",") if item.strip()]
    if not raw:
        return list(default)
    parsed: list[int] = []
    for item in raw:
        try:
            parsed_value = int(item)
        except ValueError as exc:
            raise RuntimeError(f"{flag_name} expects comma-separated integers") from exc
        if parsed_value <= 0:
            raise RuntimeError(f"{flag_name} values must be > 0")
        parsed.append(parsed_value)
    return list(dict.fromkeys(parsed))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_ablation_input_hash(*, payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return sha256_text(normalized)


def ablation_git_head() -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return "unknown"
    if proc.returncode != 0:
        return "unknown"
    head = proc.stdout.strip()
    return head or "unknown"


def ablation_state_dir(reports_root: Path) -> Path:
    return reports_root / "_ablation_state"


def ablation_load_state(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError:
        return None
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def ablation_write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def ablation_prune_cap_root(cap_root: Path) -> dict[str, int]:
    removed_dirs = 0
    removed_files = 0
    for relative in ("by-snapshot", "_ablation_state"):
        target = cap_root / relative
        if not target.exists():
            continue
        removed_files += sum(1 for item in target.rglob("*") if item.is_file())
        rmtree(target)
        removed_dirs += 1
    return {"removed_dirs": removed_dirs, "removed_files": removed_files}


def ablation_count_seed_rows(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def build_ablation_analysis_run_id(*, analysis_prefix: str, run_id: str, cap: int) -> str:
    run_id_clean = run_id.strip()
    prefix_clean = analysis_prefix.strip()
    if run_id_clean.startswith(f"{prefix_clean}-") or run_id_clean == prefix_clean:
        base = run_id_clean
    else:
        base = f"{prefix_clean}-{run_id_clean}"
    return f"{base}-max{cap}"


def ablation_strategy_artifacts_exist(
    *,
    reports_dir: Path,
    strategy_id: str,
    seed_rows: int,
) -> bool:
    required = [
        reports_dir / f"strategy-report.{strategy_id}.json",
        reports_dir / f"backtest-seed.{strategy_id}.jsonl",
        reports_dir / f"backtest-results-template.{strategy_id}.csv",
    ]
    if seed_rows > 0:
        required.append(reports_dir / f"settlement.{strategy_id}.csv")
    return all(path.exists() for path in required)


def ablation_strategy_cache_valid(
    *,
    reports_dir: Path,
    state_path: Path,
    expected_hash: str,
    strategy_id: str,
) -> bool:
    payload = ablation_load_state(state_path)
    if not isinstance(payload, dict):
        return False
    input_hash = str(payload.get("input_hash", "")).strip()
    if input_hash != expected_hash:
        return False
    seed_rows = int(payload.get("seed_rows", 1) or 0)
    return ablation_strategy_artifacts_exist(
        reports_dir=reports_dir,
        strategy_id=strategy_id,
        seed_rows=seed_rows,
    )


def ablation_compare_artifacts_exist(*, reports_dir: Path, strategy_ids: Sequence[str]) -> bool:
    required = [
        reports_dir / "strategy-compare.json",
        reports_dir / "strategy-compare.md",
    ]
    for strategy_id in strategy_ids:
        required.append(reports_dir / f"strategy-report.{strategy_id}.json")
        required.append(reports_dir / f"backtest-seed.{strategy_id}.jsonl")
    return all(path.exists() for path in required)


def ablation_compare_cache_valid(
    *,
    reports_dir: Path,
    state_path: Path,
    expected_hash: str,
    strategy_ids: Sequence[str],
) -> bool:
    payload = ablation_load_state(state_path)
    if not isinstance(payload, dict):
        return False
    input_hash = str(payload.get("input_hash", "")).strip()
    if input_hash != expected_hash:
        return False
    return ablation_compare_artifacts_exist(reports_dir=reports_dir, strategy_ids=strategy_ids)


def parse_cli_kv(stdout: str) -> dict[str, str]:
    payload: dict[str, str] = {}
    for line in stdout.splitlines():
        key, sep, value = line.partition("=")
        if not sep:
            continue
        normalized = key.strip()
        if not normalized:
            continue
        payload[normalized] = value.strip()
    return payload


def sanitize_analysis_run_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
