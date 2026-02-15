"""Strategy ablation command implementation."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from prop_ev.cli_ablation_helpers import (
    ablation_compare_cache_valid as _ablation_compare_cache_valid,
)
from prop_ev.cli_ablation_helpers import (
    ablation_count_seed_rows as _ablation_count_seed_rows,
)
from prop_ev.cli_ablation_helpers import (
    ablation_git_head as _ablation_git_head,
)
from prop_ev.cli_ablation_helpers import (
    ablation_prune_cap_root as _ablation_prune_cap_root,
)
from prop_ev.cli_ablation_helpers import (
    ablation_state_dir as _ablation_state_dir,
)
from prop_ev.cli_ablation_helpers import (
    ablation_strategy_cache_valid as _ablation_strategy_cache_valid,
)
from prop_ev.cli_ablation_helpers import (
    ablation_write_state as _ablation_write_state,
)
from prop_ev.cli_ablation_helpers import (
    build_ablation_analysis_run_id as _build_ablation_analysis_run_id,
)
from prop_ev.cli_ablation_helpers import (
    build_ablation_input_hash as _build_ablation_input_hash,
)
from prop_ev.cli_ablation_helpers import (
    parse_cli_kv as _parse_cli_kv,
)
from prop_ev.cli_ablation_helpers import (
    parse_positive_int_csv as _parse_positive_int_csv_impl,
)
from prop_ev.cli_ablation_helpers import (
    sha256_file as _sha256_file,
)
from prop_ev.cli_data_helpers import (
    complete_day_snapshots as _complete_day_snapshots_impl,
)
from prop_ev.cli_data_helpers import (
    resolve_complete_day_dataset_id as _resolve_complete_day_dataset_id_impl,
)
from prop_ev.cli_shared import (
    CLIError,
    _iso,
    _runtime_nba_data_dir,
    _runtime_odds_data_dir,
    _runtime_runtime_dir,
    _runtime_strategy_probabilistic_profile,
    _sanitize_analysis_run_id,
    _utc_now,
)
from prop_ev.cli_strategy.compare import _parse_strategy_ids
from prop_ev.cli_strategy.shared import _resolve_input_probabilistic_profile
from prop_ev.nba_data.minutes_prob import load_minutes_prob_index_for_snapshot
from prop_ev.nba_data.store.layout import build_layout as build_nba_layout
from prop_ev.odds_client import (
    parse_csv,
)
from prop_ev.report_paths import (
    report_outputs_root,
    snapshot_reports_dir,
)
from prop_ev.storage import SnapshotStore


def _resolve_complete_day_dataset_id(data_root: Path, requested: str) -> str:
    try:
        return _resolve_complete_day_dataset_id_impl(data_root, requested)
    except RuntimeError as exc:
        raise CLIError(str(exc)) from exc


def _complete_day_snapshots(data_root: Path, dataset_id_value: str) -> list[tuple[str, str]]:
    return _complete_day_snapshots_impl(data_root, dataset_id_value)


def _parse_positive_int_csv(value: str, *, default: list[int], flag_name: str) -> list[int]:
    try:
        return _parse_positive_int_csv_impl(value, default=default, flag_name=flag_name)
    except RuntimeError as exc:
        raise CLIError(str(exc)) from exc


def _run_cli_subcommand(
    *,
    args: list[str],
    env: dict[str, str] | None,
    cwd: Path,
    global_cli_args: Sequence[str] | None = None,
) -> str:
    cmd = [sys.executable, "-m", "prop_ev.cli"]
    if global_cli_args:
        cmd.extend(global_cli_args)
    cmd.extend(args)
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=cwd,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        stdout = proc.stdout.strip()
        details = stderr or stdout or f"exit={proc.returncode}"
        raise CLIError(f"subcommand failed ({' '.join(args)}): {details}")
    return proc.stdout


def _cmd_strategy_ablation(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    data_root = store.root
    dataset_id_value = _resolve_complete_day_dataset_id(
        data_root, str(getattr(args, "dataset_id", ""))
    )
    complete_rows = _complete_day_snapshots(data_root, dataset_id_value)
    if not complete_rows:
        raise CLIError(f"dataset has no complete indexed days: {dataset_id_value}")

    strategy_ids = _parse_strategy_ids(str(getattr(args, "strategies", "")))
    if not strategy_ids:
        raise CLIError("ablation requires --strategies")
    if len(strategy_ids) < 2:
        raise CLIError("ablation requires at least 2 strategies")
    caps = _parse_positive_int_csv(
        str(getattr(args, "caps", "")),
        default=[1, 2, 5],
        flag_name="--caps",
    )
    force_days = {item.strip() for item in parse_csv(str(getattr(args, "force_days", ""))) if item}
    force_strategies = set(_parse_strategy_ids(str(getattr(args, "force_strategies", ""))))
    force_all = bool(getattr(args, "force", False))
    reuse_existing = bool(getattr(args, "reuse_existing", True)) and not force_all

    default_profile = str(_runtime_strategy_probabilistic_profile())
    probabilistic_profile = _resolve_input_probabilistic_profile(
        default_profile=default_profile,
        probabilistic_profile_arg=str(getattr(args, "probabilistic_profile", "")),
        strategy_ids=list(strategy_ids),
    )

    reports_root_raw = str(getattr(args, "reports_root", "")).strip()
    base_reports_root = (
        Path(reports_root_raw).expanduser().resolve()
        if reports_root_raw
        else report_outputs_root(store)
    )
    run_id_raw = str(getattr(args, "run_id", "")).strip()
    if run_id_raw:
        run_id = _sanitize_analysis_run_id(run_id_raw)
        if not run_id:
            raise CLIError("--run-id must contain letters, numbers, '_' '-' or '.'")
    else:
        run_id = "latest"
    if not run_id:
        raise CLIError("failed to build run id")
    run_root = base_reports_root / "ablation" / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    cwd = Path.cwd()
    code_revision = _ablation_git_head()
    nba_data_dir = str(Path(_runtime_nba_data_dir()).expanduser().resolve())
    runtime_dir = str(Path(_runtime_runtime_dir()).expanduser().resolve())

    manifest_hashes = {
        snapshot_id: _sha256_file(store.snapshot_dir(snapshot_id) / "manifest.json")
        for _, snapshot_id in complete_rows
    }

    prebuild_minutes_cache = bool(getattr(args, "prebuild_minutes_cache", True))
    if prebuild_minutes_cache and probabilistic_profile == "minutes_v1":
        nba_dir = Path(_runtime_nba_data_dir()).expanduser().resolve()
        nba_layout = build_nba_layout(nba_dir)
        prebuild_workers = max(1, int(getattr(args, "max_workers", 6)))

        def _prebuild_one(day_value: str) -> tuple[str, int]:
            payload = load_minutes_prob_index_for_snapshot(
                layout=nba_layout,
                snapshot_day=day_value,
                probabilistic_profile="minutes_v1",
                auto_build=True,
            )
            meta = payload.get("meta", {}) if isinstance(payload.get("meta"), dict) else {}
            rows = int(meta.get("rows", 0) or 0)
            return day_value, rows

        with ThreadPoolExecutor(max_workers=prebuild_workers) as executor:
            futures = {executor.submit(_prebuild_one, day): day for day, _ in complete_rows}
            for future in as_completed(futures):
                day_value, rows = future.result()
                print(f"minutes_cache_day={day_value} rows={rows}")

    mode = str(getattr(args, "mode", "replay"))
    top_n = max(0, int(getattr(args, "top_n", 10)))
    min_ev = float(getattr(args, "min_ev", 0.01))
    allow_tier_b = bool(getattr(args, "allow_tier_b", False))
    offline = bool(getattr(args, "offline", True))
    block_paid = bool(getattr(args, "block_paid", True))
    refresh_context = bool(getattr(args, "refresh_context", False))
    results_source = str(getattr(args, "results_source", "historical")).strip() or "historical"
    prune_intermediate = bool(getattr(args, "prune_intermediate", True))
    write_scoreboard_pdf = bool(getattr(args, "write_scoreboard_pdf", True))
    keep_scoreboard_tex = bool(getattr(args, "keep_scoreboard_tex", False))
    max_workers = max(1, int(getattr(args, "max_workers", 6)))
    cap_workers = max(1, int(getattr(args, "cap_workers", 3)))
    cap_workers = min(cap_workers, len(caps))
    segment_by = str(getattr(args, "segment_by", "none")).strip().lower() or "none"
    if segment_by not in {"none", "market"}:
        raise CLIError("--segment-by must be one of: none,market")

    min_graded = max(0, int(getattr(args, "min_graded", 0)))
    bin_size = float(getattr(args, "bin_size", 0.1))
    require_scored_fraction = float(getattr(args, "require_scored_fraction", 0.9))
    ece_slack = float(getattr(args, "ece_slack", 0.01))
    brier_slack = float(getattr(args, "brier_slack", 0.01))
    power_alpha = float(getattr(args, "power_alpha", 0.05))
    power_level = float(getattr(args, "power_level", 0.8))
    power_target_uplifts = str(getattr(args, "power_target_uplifts", "0.01,0.02,0.03,0.05")).strip()
    power_target_uplift_gate = float(getattr(args, "power_target_uplift_gate", 0.02))
    if power_target_uplift_gate <= 0.0:
        raise CLIError("--power-target-uplift-gate must be > 0")
    require_power_gate = bool(getattr(args, "require_power_gate", False))
    calibration_map_mode = str(getattr(args, "calibration_map_mode", "walk_forward")).strip()
    analysis_prefix_raw = str(getattr(args, "analysis_run_prefix", "ablation")).strip()
    analysis_prefix = _sanitize_analysis_run_id(analysis_prefix_raw)
    if not analysis_prefix:
        raise CLIError("--analysis-run-prefix must contain letters, numbers, '_' '-' or '.'")
    snapshot_id_for_summary = str(getattr(args, "snapshot_id", "")).strip() or complete_rows[-1][1]

    cap_results: list[dict[str, Any]] = []

    def _cap_worker(cap: int) -> dict[str, Any]:
        cap_root = run_root / f"cap-max{cap}"
        cap_root.mkdir(parents=True, exist_ok=True)
        cap_global_cli_args = [
            "--data-dir",
            str(store.root),
            "--reports-dir",
            str(cap_root),
            "--nba-data-dir",
            nba_data_dir,
            "--runtime-dir",
            runtime_dir,
        ]
        state_dir = _ablation_state_dir(cap_root)
        state_dir.mkdir(parents=True, exist_ok=True)

        cap_summary: dict[str, Any] = {
            "cap": cap,
            "compare_ran": 0,
            "compare_skipped": 0,
            "settled": 0,
            "settle_skipped": 0,
            "no_seed_rows": 0,
            "analysis_scoreboard_pdf": "",
            "analysis_scoreboard_pdf_status": "",
            "pruned_dirs": 0,
            "pruned_files": 0,
        }

        def _snapshot_worker(day_snapshot: tuple[str, str]) -> dict[str, int]:
            day_value, snapshot_id = day_snapshot
            reports_dir = snapshot_reports_dir(store, snapshot_id, reports_root=cap_root)
            reports_dir.mkdir(parents=True, exist_ok=True)
            manifest_hash = manifest_hashes.get(snapshot_id, "")
            forced_day = force_all or day_value in force_days or snapshot_id in force_days

            compare_payload = {
                "kind": "compare",
                "snapshot_id": snapshot_id,
                "day": day_value,
                "strategies": list(strategy_ids),
                "cap": cap,
                "top_n": top_n,
                "min_ev": min_ev,
                "mode": mode,
                "allow_tier_b": allow_tier_b,
                "probabilistic_profile": probabilistic_profile,
                "manifest_hash": manifest_hash,
                "code_revision": code_revision,
            }
            compare_hash = _build_ablation_input_hash(payload=compare_payload)
            compare_state_path = state_dir / f"{snapshot_id}.compare.json"
            compare_cached = (
                reuse_existing
                and not forced_day
                and _ablation_compare_cache_valid(
                    reports_dir=reports_dir,
                    state_path=compare_state_path,
                    expected_hash=compare_hash,
                    strategy_ids=strategy_ids,
                )
            )

            strategy_hash_by_id: dict[str, str] = {}
            strategy_cached_by_id: dict[str, bool] = {}
            strategy_core_ready: dict[str, bool] = {}
            for strategy_id in strategy_ids:
                strategy_payload = {
                    "kind": "strategy",
                    "snapshot_id": snapshot_id,
                    "day": day_value,
                    "strategy_id": strategy_id,
                    "cap": cap,
                    "top_n": top_n,
                    "min_ev": min_ev,
                    "mode": mode,
                    "allow_tier_b": allow_tier_b,
                    "probabilistic_profile": probabilistic_profile,
                    "results_source": results_source,
                    "manifest_hash": manifest_hash,
                    "code_revision": code_revision,
                }
                strategy_hash = _build_ablation_input_hash(payload=strategy_payload)
                strategy_hash_by_id[strategy_id] = strategy_hash
                strategy_state_path = state_dir / f"{snapshot_id}.{strategy_id}.json"
                strategy_cached_by_id[strategy_id] = (
                    reuse_existing
                    and not forced_day
                    and strategy_id not in force_strategies
                    and _ablation_strategy_cache_valid(
                        reports_dir=reports_dir,
                        state_path=strategy_state_path,
                        expected_hash=strategy_hash,
                        strategy_id=strategy_id,
                    )
                )
                strategy_core_ready[strategy_id] = (
                    reports_dir / f"strategy-report.{strategy_id}.json"
                ).exists() and (reports_dir / f"backtest-seed.{strategy_id}.jsonl").exists()

            needs_compare = (
                not compare_cached
                or forced_day
                or any(strategy_id in force_strategies for strategy_id in strategy_ids)
                or any(
                    not strategy_core_ready.get(strategy_id, False) for strategy_id in strategy_ids
                )
            )
            local_summary: dict[str, int] = {
                "compare_ran": 0,
                "compare_skipped": 0,
                "settled": 0,
                "settle_skipped": 0,
                "no_seed_rows": 0,
            }
            if needs_compare:
                compare_cmd = [
                    "strategy",
                    "compare",
                    "--snapshot-id",
                    snapshot_id,
                    "--strategies",
                    ",".join(strategy_ids),
                    "--top-n",
                    str(top_n),
                    "--max-picks",
                    str(cap),
                    "--min-ev",
                    str(min_ev),
                    "--mode",
                    mode,
                    "--probabilistic-profile",
                    probabilistic_profile,
                ]
                if allow_tier_b:
                    compare_cmd.append("--allow-tier-b")
                if offline:
                    compare_cmd.append("--offline")
                if block_paid:
                    compare_cmd.append("--block-paid")
                if refresh_context:
                    compare_cmd.append("--refresh-context")
                _run_cli_subcommand(
                    args=compare_cmd,
                    env=None,
                    cwd=cwd,
                    global_cli_args=cap_global_cli_args,
                )
                _ablation_write_state(
                    compare_state_path,
                    {
                        "input_hash": compare_hash,
                        "snapshot_id": snapshot_id,
                        "day": day_value,
                        "cap": cap,
                        "strategies": list(strategy_ids),
                        "generated_at_utc": _iso(_utc_now()),
                    },
                )
                local_summary["compare_ran"] += 1
            else:
                local_summary["compare_skipped"] += 1

            for strategy_id in strategy_ids:
                strategy_state_path = state_dir / f"{snapshot_id}.{strategy_id}.json"
                strategy_hash = strategy_hash_by_id[strategy_id]
                forced_strategy = forced_day or strategy_id in force_strategies
                if (
                    reuse_existing
                    and not forced_strategy
                    and _ablation_strategy_cache_valid(
                        reports_dir=reports_dir,
                        state_path=strategy_state_path,
                        expected_hash=strategy_hash,
                        strategy_id=strategy_id,
                    )
                ):
                    local_summary["settle_skipped"] += 1
                    continue

                seed_path = reports_dir / f"backtest-seed.{strategy_id}.jsonl"
                seed_rows = _ablation_count_seed_rows(seed_path)
                if seed_rows == 0:
                    _ablation_write_state(
                        strategy_state_path,
                        {
                            "input_hash": strategy_hash,
                            "snapshot_id": snapshot_id,
                            "day": day_value,
                            "cap": cap,
                            "strategy_id": strategy_id,
                            "seed_rows": 0,
                            "generated_at_utc": _iso(_utc_now()),
                        },
                    )
                    local_summary["no_seed_rows"] += 1
                    continue

                settle_cmd = [
                    "strategy",
                    "settle",
                    "--snapshot-id",
                    snapshot_id,
                    "--strategy-report-file",
                    f"strategy-report.{strategy_id}.json",
                    "--results-source",
                    results_source,
                    "--write-csv",
                    "--no-pdf",
                    "--no-json",
                ]
                if offline:
                    settle_cmd.append("--offline")
                _run_cli_subcommand(
                    args=settle_cmd,
                    env=None,
                    cwd=cwd,
                    global_cli_args=cap_global_cli_args,
                )
                _ablation_write_state(
                    strategy_state_path,
                    {
                        "input_hash": strategy_hash,
                        "snapshot_id": snapshot_id,
                        "day": day_value,
                        "cap": cap,
                        "strategy_id": strategy_id,
                        "seed_rows": seed_rows,
                        "generated_at_utc": _iso(_utc_now()),
                    },
                )
                local_summary["settled"] += 1

            return local_summary

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_snapshot_worker, day_snapshot): day_snapshot
                for day_snapshot in complete_rows
            }
            aggregate_keys = (
                "compare_ran",
                "compare_skipped",
                "settled",
                "settle_skipped",
                "no_seed_rows",
                "pruned_dirs",
                "pruned_files",
            )
            for future in as_completed(futures):
                local = future.result()
                for key in aggregate_keys:
                    cap_summary[key] += int(local.get(key, 0))

        analysis_run_id = _sanitize_analysis_run_id(
            _build_ablation_analysis_run_id(
                analysis_prefix=analysis_prefix,
                run_id=run_id,
                cap=cap,
            )
        )
        if not analysis_run_id:
            raise CLIError("failed to build analysis run id")
        summarize_cmd = [
            "strategy",
            "backtest-summarize",
            "--snapshot-id",
            snapshot_id_for_summary,
            "--strategies",
            ",".join(strategy_ids),
            "--all-complete-days",
            "--dataset-id",
            dataset_id_value,
            "--min-graded",
            str(min_graded),
            "--bin-size",
            str(bin_size),
            "--require-scored-fraction",
            str(require_scored_fraction),
            "--ece-slack",
            str(ece_slack),
            "--brier-slack",
            str(brier_slack),
            "--power-alpha",
            str(power_alpha),
            "--power-level",
            str(power_level),
            "--power-picks-per-day",
            str(cap),
            "--power-target-uplifts",
            power_target_uplifts,
            "--power-target-uplift-gate",
            str(power_target_uplift_gate),
            "--write-analysis-scoreboard",
            "--analysis-run-id",
            analysis_run_id,
            "--write-calibration-map",
            "--calibration-map-mode",
            calibration_map_mode,
        ]
        if write_scoreboard_pdf:
            summarize_cmd.append("--write-analysis-pdf")
        if keep_scoreboard_tex:
            summarize_cmd.append("--keep-analysis-tex")
        summarize_cmd.extend(["--segment-by", segment_by])
        if require_power_gate:
            summarize_cmd.append("--require-power-gate")
        summarize_stdout = _run_cli_subcommand(
            args=summarize_cmd,
            env=None,
            cwd=cwd,
            global_cli_args=cap_global_cli_args,
        )
        kv = _parse_cli_kv(summarize_stdout)
        cap_summary["summary_json"] = kv.get("summary_json", "")
        cap_summary["analysis_scoreboard_json"] = kv.get("analysis_scoreboard_json", "")
        cap_summary["analysis_scoreboard_pdf"] = kv.get("analysis_scoreboard_pdf", "")
        cap_summary["analysis_scoreboard_pdf_status"] = kv.get("analysis_scoreboard_pdf_status", "")
        cap_summary["analysis_scoreboard_by_market_json"] = kv.get(
            "analysis_scoreboard_by_market_json", ""
        )
        cap_summary["calibration_map_json"] = kv.get("calibration_map_json", "")
        cap_summary["winner_strategy_id"] = kv.get("winner_strategy_id", "")
        cap_summary["reports_root"] = str(cap_root)
        if prune_intermediate:
            prune_stats = _ablation_prune_cap_root(cap_root)
            cap_summary["pruned_dirs"] = int(prune_stats.get("removed_dirs", 0))
            cap_summary["pruned_files"] = int(prune_stats.get("removed_files", 0))
        print(
            f"ablation_cap={cap} compare_ran={cap_summary['compare_ran']} "
            f"settle_ran={cap_summary['settled']} "
            f"settle_skipped={cap_summary['settle_skipped']} "
            f"no_seed_rows={cap_summary['no_seed_rows']} "
            f"pruned_files={cap_summary['pruned_files']}"
        )
        print(f"ablation_cap_reports_root={cap_root}")
        if cap_summary["analysis_scoreboard_json"]:
            print(f"ablation_cap_scoreboard_json={cap_summary['analysis_scoreboard_json']}")
        if cap_summary["analysis_scoreboard_pdf"]:
            print(f"ablation_cap_scoreboard_pdf={cap_summary['analysis_scoreboard_pdf']}")
        if cap_summary["analysis_scoreboard_by_market_json"]:
            print(
                "ablation_cap_scoreboard_by_market_json="
                f"{cap_summary['analysis_scoreboard_by_market_json']}"
            )
        if cap_summary["analysis_scoreboard_pdf_status"]:
            print(
                "ablation_cap_scoreboard_pdf_status="
                f"{cap_summary['analysis_scoreboard_pdf_status']}"
            )
        return cap_summary

    with ThreadPoolExecutor(max_workers=cap_workers) as executor:
        futures = {executor.submit(_cap_worker, cap): cap for cap in caps}
        for future in as_completed(futures):
            cap_results.append(future.result())

    cap_results.sort(key=lambda row: int(row.get("cap", 0)))
    run_summary = {
        "schema_version": 1,
        "report_kind": "ablation_run",
        "generated_at_utc": _iso(_utc_now()),
        "run_id": run_id,
        "segment_by": segment_by,
        "dataset_id": dataset_id_value,
        "snapshot_count": len(complete_rows),
        "strategies": list(strategy_ids),
        "caps": caps,
        "probabilistic_profile": probabilistic_profile,
        "results_source": results_source,
        "reuse_existing": reuse_existing,
        "prune_intermediate": prune_intermediate,
        "reports_root": str(run_root),
        "caps_summary": cap_results,
    }
    summary_path = run_root / "ablation-run.json"
    summary_path.write_text(
        json.dumps(run_summary, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    print(f"ablation_run_id={run_id}")
    print(f"ablation_summary_json={summary_path}")
    for row in cap_results:
        cap_value = int(row.get("cap", 0))
        winner = str(row.get("winner_strategy_id", ""))
        scoreboard = str(row.get("analysis_scoreboard_json", ""))
        print(
            f"ablation_cap_result cap={cap_value} "
            f"winner_strategy_id={winner} "
            f"scoreboard={scoreboard}"
        )
    return 0
