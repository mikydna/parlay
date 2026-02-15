"""Strategy backtest summarize/prep command implementations."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, cast

from prop_ev.backtest import write_backtest_artifacts
from prop_ev.cli_markdown import (
    render_backtest_summary_markdown as _render_backtest_summary_markdown,
)
from prop_ev.cli_shared import (
    CLIError,
    _iso,
    _parse_positive_float_csv,
    _runtime_odds_data_dir,
    _sanitize_analysis_run_id,
    _utc_now,
)
from prop_ev.cli_strategy.ablation import (
    _complete_day_snapshots,
    _resolve_complete_day_dataset_id,
)
from prop_ev.cli_strategy.compare import _parse_strategy_ids
from prop_ev.cli_strategy.shared import _latest_snapshot_id
from prop_ev.report_paths import (
    report_outputs_root,
    snapshot_reports_dir,
)
from prop_ev.storage import SnapshotStore
from prop_ev.strategies.base import (
    normalize_strategy_id,
)


def _cmd_strategy_backtest_summarize(args: argparse.Namespace) -> int:
    from prop_ev.backtest_summary import load_backtest_csv, summarize_backtest_rows
    from prop_ev.calibration_map import CalibrationMode, build_calibration_map
    from prop_ev.eval_scoreboard import (
        PromotionThresholds,
        build_power_gate,
        build_promotion_gate,
        pick_execution_winner,
        pick_promotion_winner,
        resolve_baseline_strategy_id,
    )
    from prop_ev.power_guidance import PowerGuidanceAssumptions, build_power_guidance

    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    reports_dir = snapshot_reports_dir(store, snapshot_id)

    def _resolve_results_csv(reports_dir: Path, strategy_id: str) -> Path:
        settlement_path = reports_dir / f"settlement.{strategy_id}.csv"
        if settlement_path.exists():
            return settlement_path
        if strategy_id == "s001":
            settlement_path = reports_dir / "settlement.csv"
            if settlement_path.exists():
                return settlement_path
        template_path = reports_dir / f"backtest-results-template.{strategy_id}.csv"
        if template_path.exists():
            return template_path
        if strategy_id == "s001":
            template_path = reports_dir / "backtest-results-template.csv"
        return template_path

    bin_size = float(getattr(args, "bin_size", 0.05))
    min_graded = max(0, int(getattr(args, "min_graded", 0)))
    require_scored_fraction = float(getattr(args, "require_scored_fraction", 0.9))
    if require_scored_fraction < 0.0 or require_scored_fraction > 1.0:
        raise CLIError("--require-scored-fraction must be between 0 and 1")
    ece_slack = max(0.0, float(getattr(args, "ece_slack", 0.01)))
    brier_slack = max(0.0, float(getattr(args, "brier_slack", 0.01)))
    power_alpha = float(getattr(args, "power_alpha", 0.05))
    power_level = float(getattr(args, "power_level", 0.8))
    if power_alpha <= 0.0 or power_alpha >= 1.0:
        raise CLIError("--power-alpha must be between 0 and 1 (exclusive)")
    if power_level <= 0.0 or power_level >= 1.0:
        raise CLIError("--power-level must be between 0 and 1 (exclusive)")
    power_picks_per_day = max(1, int(getattr(args, "power_picks_per_day", 5)))
    power_target_uplifts = _parse_positive_float_csv(
        str(getattr(args, "power_target_uplifts", "0.01,0.02,0.03,0.05")),
        default=[0.01, 0.02, 0.03, 0.05],
        flag_name="--power-target-uplifts",
    )
    power_target_uplift_gate = float(getattr(args, "power_target_uplift_gate", 0.02))
    if power_target_uplift_gate <= 0.0:
        raise CLIError("--power-target-uplift-gate must be > 0")
    require_power_gate = bool(getattr(args, "require_power_gate", False))
    write_analysis_scoreboard = bool(getattr(args, "write_analysis_scoreboard", False))
    write_analysis_pdf = bool(getattr(args, "write_analysis_pdf", False))
    keep_analysis_tex = bool(getattr(args, "keep_analysis_tex", False))
    analysis_run_id_raw = str(getattr(args, "analysis_run_id", "")).strip()
    all_complete_days = bool(getattr(args, "all_complete_days", False))
    write_calibration_map = bool(getattr(args, "write_calibration_map", False))
    calibration_map_mode = (
        str(getattr(args, "calibration_map_mode", "walk_forward")).strip().lower()
    )
    if calibration_map_mode not in {"walk_forward", "in_sample"}:
        raise CLIError("--calibration-map-mode must be one of: walk_forward,in_sample")
    segment_by = str(getattr(args, "segment_by", "none")).strip().lower() or "none"
    if segment_by not in {"none", "market"}:
        raise CLIError("--segment-by must be one of: none,market")
    explicit_results = getattr(args, "results", None)
    computed = []
    day_coverage: dict[str, Any] = {}
    rows_for_map: dict[str, list[dict[str, str]]] = {}
    daily_pnl_by_strategy: dict[str, dict[str, float]] = {}
    dataset_id_value = ""
    if all_complete_days:
        if isinstance(explicit_results, list) and explicit_results:
            raise CLIError("--all-complete-days cannot be combined with --results")
        strategy_ids = _parse_strategy_ids(getattr(args, "strategies", ""))
        if not strategy_ids:
            raise CLIError("--all-complete-days requires --strategies")

        data_root = store.root
        dataset_id_value = _resolve_complete_day_dataset_id(
            data_root,
            str(getattr(args, "dataset_id", "")),
        )
        complete_days = _complete_day_snapshots(data_root, dataset_id_value)
        if not complete_days:
            raise CLIError(f"dataset has no complete indexed days: {dataset_id_value}")

        rows_by_strategy: dict[str, list[dict[str, str]]] = {sid: [] for sid in strategy_ids}
        daily_pnl_by_strategy = {sid: {} for sid in strategy_ids}
        skipped_days: list[dict[str, str]] = []
        days_with_any_results: set[str] = set()
        for day, day_snapshot_id in complete_days:
            day_reports_dir = snapshot_reports_dir(store, day_snapshot_id)
            for strategy_id in strategy_ids:
                path = _resolve_results_csv(day_reports_dir, strategy_id)
                if not path.exists():
                    skipped_days.append(
                        {
                            "day": day,
                            "snapshot_id": day_snapshot_id,
                            "strategy_id": strategy_id,
                            "reason": "missing_backtest_csv",
                        }
                    )
                    continue
                rows = load_backtest_csv(path)
                if rows:
                    rows_by_strategy[strategy_id].extend(rows)
                    day_summary = summarize_backtest_rows(
                        rows,
                        strategy_id=strategy_id,
                        bin_size=bin_size,
                    )
                    if day_summary.rows_graded > 0:
                        daily_pnl_by_strategy[strategy_id][day] = float(day_summary.total_pnl_units)
                    days_with_any_results.add(day)

        for strategy_id in strategy_ids:
            summary = summarize_backtest_rows(
                rows_by_strategy[strategy_id],
                strategy_id=strategy_id,
                bin_size=bin_size,
            )
            computed.append(summary)

        if not any(item.rows_total > 0 for item in computed):
            raise CLIError(
                "no backtest rows found for selected complete days/strategies; "
                "run `prop-ev strategy backtest-prep` and "
                "`prop-ev strategy settle --write-csv` first"
            )
        rows_for_map = rows_by_strategy
        day_coverage = {
            "all_complete_days": True,
            "dataset_id": dataset_id_value,
            "complete_days": len(complete_days),
            "days_with_any_results": len(days_with_any_results),
            "skipped_rows": len(skipped_days),
            "skipped": skipped_days[:200],
        }
    else:
        paths: list[tuple[str, Path]] = []
        if isinstance(explicit_results, list) and explicit_results:
            for raw in explicit_results:
                path = Path(str(raw))
                rows = load_backtest_csv(path)
                strategy = ""
                for row in rows:
                    candidate = str(row.get("strategy_id", "")).strip()
                    if candidate:
                        strategy = normalize_strategy_id(candidate)
                        break
                if not strategy:
                    strategy = normalize_strategy_id(path.stem.replace(".", "_"))
                paths.append((strategy, path))
        else:
            strategy_ids = _parse_strategy_ids(getattr(args, "strategies", ""))
            if not strategy_ids:
                raise CLIError("backtest-summarize requires --strategies or --results")
            for strategy_id in strategy_ids:
                paths.append((strategy_id, _resolve_results_csv(reports_dir, strategy_id)))

        for strategy_id, path in paths:
            if not path.exists():
                raise CLIError(f"missing backtest CSV: {path}")
            rows = load_backtest_csv(path)
            rows_for_map[strategy_id] = rows
            summary = summarize_backtest_rows(rows, strategy_id=strategy_id, bin_size=bin_size)
            computed.append(summary)

    requested_baseline = str(getattr(args, "baseline_strategy", "")).strip()
    if requested_baseline:
        requested_baseline = normalize_strategy_id(requested_baseline)
    baseline_strategy_id = resolve_baseline_strategy_id(
        requested=requested_baseline,
        available_strategy_ids=[item.strategy_id for item in computed],
    )
    baseline_summary = next(
        (item for item in computed if item.strategy_id == baseline_strategy_id),
        None,
    )
    thresholds = PromotionThresholds(
        min_graded=min_graded,
        min_scored_fraction=require_scored_fraction,
        ece_slack=ece_slack,
        brier_slack=brier_slack,
    )
    power_guidance: dict[str, Any] = {}
    if all_complete_days and baseline_summary is not None and daily_pnl_by_strategy:
        power_guidance = build_power_guidance(
            daily_pnl_by_strategy=daily_pnl_by_strategy,
            baseline_strategy_id=baseline_strategy_id,
            assumptions=PowerGuidanceAssumptions(
                alpha=power_alpha,
                power=power_level,
                picks_per_day=power_picks_per_day,
                target_roi_uplifts_per_bet=tuple(power_target_uplifts),
            ),
        )

    strategy_rows: list[dict[str, Any]] = []
    for summary in computed:
        row = summary.to_dict()
        promotion_gate = build_promotion_gate(
            summary=summary,
            baseline_summary=baseline_summary,
            baseline_required=bool(baseline_strategy_id),
            thresholds=thresholds,
        )
        row["promotion_gate"] = promotion_gate
        if power_guidance:
            power_gate = build_power_gate(
                summary=summary,
                power_guidance=power_guidance,
                target_roi_uplift_per_bet=power_target_uplift_gate,
            )
            row["power_gate"] = power_gate
            if bool(require_power_gate) and power_gate.get("status") == "fail":
                reasons = promotion_gate.get("reasons", [])
                if not isinstance(reasons, list):
                    reasons = []
                if "underpowered_for_target_uplift" not in reasons:
                    reasons = [*reasons, "underpowered_for_target_uplift"]
                promotion_gate["status"] = "fail"
                promotion_gate["reasons"] = sorted({str(value) for value in reasons if value})
        strategy_rows.append(row)

    winner = pick_execution_winner(strategy_rows)
    promotion_winner = pick_promotion_winner(strategy_rows)
    segments: dict[str, Any] = {}
    if segment_by == "market" and rows_for_map:
        markets: set[str] = set()
        rows_by_market: dict[str, dict[str, list[dict[str, str]]]] = {}
        for strategy_id, rows in rows_for_map.items():
            for row in rows:
                market = str(row.get("market", "")).strip().lower() or "unknown"
                markets.add(market)
                rows_by_market.setdefault(market, {}).setdefault(strategy_id, []).append(row)

        market_segments: list[dict[str, Any]] = []
        for market in sorted(markets):
            segment_summaries = [
                summarize_backtest_rows(
                    rows_by_market.get(market, {}).get(strategy_id, []),
                    strategy_id=strategy_id,
                    bin_size=bin_size,
                )
                for strategy_id in sorted(rows_for_map.keys())
            ]
            segment_baseline_summary = next(
                (item for item in segment_summaries if item.strategy_id == baseline_strategy_id),
                None,
            )
            segment_rows: list[dict[str, Any]] = []
            for summary in segment_summaries:
                row = summary.to_dict()
                row["promotion_gate"] = build_promotion_gate(
                    summary=summary,
                    baseline_summary=segment_baseline_summary,
                    baseline_required=bool(baseline_strategy_id),
                    thresholds=thresholds,
                )
                segment_rows.append(row)
            segment_winner = pick_execution_winner(segment_rows)
            segment_promotion_winner = pick_promotion_winner(segment_rows)
            market_segments.append(
                {
                    "market": market,
                    "strategies": sorted(segment_rows, key=lambda row: row.get("strategy_id", "")),
                    "winner": segment_winner if segment_winner is not None else {},
                    "promotion_winner": (
                        segment_promotion_winner if segment_promotion_winner is not None else {}
                    ),
                }
            )
        segments["by_market"] = market_segments
    report = {
        "schema_version": 1,
        "report_kind": "backtest_summary",
        "generated_at_utc": _iso(_utc_now()),
        "summary": {
            "snapshot_id": snapshot_id,
            "strategy_count": len(strategy_rows),
            "segment_by": segment_by,
            "min_graded": min_graded,
            "bin_size": bin_size,
            "baseline_strategy_id": baseline_strategy_id,
            "baseline_found": baseline_summary is not None,
            "require_scored_fraction": require_scored_fraction,
            "ece_slack": ece_slack,
            "brier_slack": brier_slack,
            "power_target_uplift_gate": power_target_uplift_gate,
            "require_power_gate": require_power_gate,
            "power_picks_per_day": power_picks_per_day,
            **day_coverage,
        },
        "strategies": sorted(strategy_rows, key=lambda row: row.get("strategy_id", "")),
        "winner": winner if winner is not None else {},
        "promotion_winner": promotion_winner if promotion_winner is not None else {},
    }
    if segments:
        report["segments"] = segments
    if power_guidance:
        report["power_guidance"] = power_guidance
    calibration_map_payload: dict[str, Any] | None = None
    if write_calibration_map and rows_for_map:
        calibration_map_payload = build_calibration_map(
            rows_by_strategy=rows_for_map,
            bin_size=bin_size,
            mode=cast(CalibrationMode, calibration_map_mode),
            dataset_id=dataset_id_value,
        )
        report["calibration_map"] = {
            "mode": calibration_map_mode,
            "strategy_count": len(calibration_map_payload.get("strategies", {})),
        }

    reports_dir.mkdir(parents=True, exist_ok=True)
    write_markdown = bool(getattr(args, "write_markdown", False))
    json_path = reports_dir / "backtest-summary.json"
    md_path = reports_dir / "backtest-summary.md"
    calibration_map_path = reports_dir / "backtest-calibration-map.json"
    analysis_json_path: Path | None = None
    analysis_market_json_path: Path | None = None
    analysis_md_path: Path | None = None
    analysis_pdf_path: Path | None = None
    analysis_pdf_status = ""
    analysis_tex_path: Path | None = None
    analysis_pdf_message = ""
    json_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    if calibration_map_payload is not None:
        calibration_map_path.write_text(
            json.dumps(calibration_map_payload, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    elif calibration_map_path.exists():
        calibration_map_path.unlink()
    if write_markdown:
        md_path.write_text(_render_backtest_summary_markdown(report), encoding="utf-8")
    elif md_path.exists():
        md_path.unlink()

    if write_analysis_scoreboard:
        if analysis_run_id_raw:
            analysis_run_id = _sanitize_analysis_run_id(analysis_run_id_raw)
            if not analysis_run_id:
                raise CLIError("--analysis-run-id must contain letters, numbers, '_' '-' or '.'")
        elif all_complete_days and dataset_id_value:
            analysis_run_id = f"eval-scoreboard-dataset-{dataset_id_value[:8]}"
        else:
            analysis_run_id = f"eval-scoreboard-snapshot-{snapshot_id}"

        analysis_dir = report_outputs_root(store) / "analysis" / analysis_run_id
        analysis_dir.mkdir(parents=True, exist_ok=True)
        analysis_json_path = analysis_dir / "aggregate-scoreboard.json"
        analysis_payload = dict(report)
        analysis_payload["report_kind"] = "aggregate_scoreboard"
        analysis_payload["analysis_run_id"] = analysis_run_id
        analysis_json_path.write_text(
            json.dumps(analysis_payload, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        if segments.get("by_market"):
            analysis_market_json_path = analysis_dir / "aggregate-scoreboard.by-market.json"
            analysis_market_payload = {
                "schema_version": 1,
                "report_kind": "aggregate_scoreboard_by_market",
                "generated_at_utc": analysis_payload.get("generated_at_utc", ""),
                "analysis_run_id": analysis_run_id,
                "summary": analysis_payload.get("summary", {}),
                "segments": segments,
            }
            analysis_market_json_path.write_text(
                json.dumps(analysis_market_payload, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
        if write_markdown:
            analysis_md_path = analysis_dir / "aggregate-scoreboard.md"
            analysis_md_path.write_text(
                _render_backtest_summary_markdown(analysis_payload),
                encoding="utf-8",
            )
        else:
            stale_md_path = analysis_dir / "aggregate-scoreboard.md"
            if stale_md_path.exists():
                stale_md_path.unlink()
        if write_analysis_pdf:
            from prop_ev.scoreboard_pdf import render_aggregate_scoreboard_pdf

            analysis_pdf_path = analysis_dir / "aggregate-scoreboard.pdf"
            analysis_tex_source = analysis_dir / "aggregate-scoreboard.tex"
            pdf_result = render_aggregate_scoreboard_pdf(
                analysis_payload=analysis_payload,
                tex_path=analysis_tex_source,
                pdf_path=analysis_pdf_path,
                keep_tex=keep_analysis_tex,
            )
            analysis_pdf_status = str(pdf_result.get("status", "")).strip()
            analysis_pdf_message = str(pdf_result.get("message", "")).strip()
            tex_path_value = str(pdf_result.get("tex_path", "")).strip()
            if tex_path_value:
                analysis_tex_path = Path(tex_path_value)

    print(f"snapshot_id={snapshot_id}")
    print(f"summary_json={json_path}")
    if calibration_map_payload is not None:
        print(f"calibration_map_json={calibration_map_path}")
    if write_markdown:
        print(f"summary_md={md_path}")
    if analysis_json_path is not None:
        print(f"analysis_scoreboard_json={analysis_json_path}")
    if analysis_market_json_path is not None:
        print(f"analysis_scoreboard_by_market_json={analysis_market_json_path}")
    if analysis_md_path is not None:
        print(f"analysis_scoreboard_md={analysis_md_path}")
    if analysis_pdf_path is not None:
        print(f"analysis_scoreboard_pdf={analysis_pdf_path}")
        print(f"analysis_scoreboard_pdf_status={analysis_pdf_status}")
        if analysis_pdf_message:
            print(f"analysis_scoreboard_pdf_message={analysis_pdf_message}")
    if analysis_tex_path is not None:
        print(f"analysis_scoreboard_tex={analysis_tex_path}")
    if winner is not None:
        print(
            "winner_strategy_id={} roi={} graded={}".format(
                winner.get("strategy_id", ""),
                winner.get("roi", ""),
                winner.get("rows_graded", 0),
            )
        )
    if promotion_winner is not None:
        print(
            "promotion_winner_strategy_id={} roi={} graded={}".format(
                promotion_winner.get("strategy_id", ""),
                promotion_winner.get("roi", ""),
                promotion_winner.get("rows_graded", 0),
            )
        )
    return 0


def _cmd_strategy_backtest_prep(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    requested = str(getattr(args, "strategy", "") or "").strip()
    write_canonical = True
    strategy_id: str | None = None
    report_path = reports_dir / "strategy-report.json"
    if requested:
        strategy_id = normalize_strategy_id(requested)
        suffixed_path = reports_dir / f"strategy-report.{strategy_id}.json"
        if suffixed_path.exists():
            report_path = suffixed_path
            write_canonical = False
        elif report_path.exists():
            canonical_payload = json.loads(report_path.read_text(encoding="utf-8"))
            if not isinstance(canonical_payload, dict):
                raise CLIError(f"invalid strategy report payload: {report_path}")
            canonical_id = normalize_strategy_id(str(canonical_payload.get("strategy_id", "s001")))
            if canonical_id != strategy_id:
                raise CLIError(
                    f"missing strategy report: {suffixed_path} (canonical is {canonical_id})"
                )
        else:
            report_path = suffixed_path
            write_canonical = False
    if not report_path.exists():
        raise CLIError(f"missing strategy report: {report_path}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise CLIError(f"invalid strategy report payload: {report_path}")

    result = write_backtest_artifacts(
        snapshot_dir=snapshot_dir,
        reports_dir=reports_dir,
        report=report,
        selection=args.selection,
        top_n=max(0, int(args.top_n)),
        strategy_id=strategy_id,
        write_canonical=write_canonical,
    )
    print(f"snapshot_id={snapshot_id}")
    print(f"selection_mode={result['selection_mode']} top_n={result['top_n']}")
    print(f"seed_rows={result['seed_rows']}")
    print(f"backtest_seed_jsonl={result['seed_jsonl']}")
    print(f"backtest_results_template_csv={result['results_template_csv']}")
    print(f"backtest_readiness_json={result['readiness_json']}")
    print(
        "ready_for_backtest_seed={} ready_for_settlement={}".format(
            result["ready_for_backtest_seed"], result["ready_for_settlement"]
        )
    )
    return 0
