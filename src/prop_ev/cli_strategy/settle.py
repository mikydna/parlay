"""Strategy settle command implementation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from prop_ev.backtest import build_backtest_seed_rows
from prop_ev.cli_shared import (
    CLIError,
    _runtime_odds_data_dir,
)
from prop_ev.cli_strategy.shared import _latest_snapshot_id
from prop_ev.report_paths import (
    snapshot_reports_dir,
)
from prop_ev.settlement import settle_snapshot
from prop_ev.storage import SnapshotStore
from prop_ev.strategies.base import (
    normalize_strategy_id,
)
from prop_ev.strategy import (
    load_jsonl,
)


def _resolve_settlement_strategy_report_path(
    *, reports_dir: Path, strategy_report_file: str
) -> Path | None:
    requested = strategy_report_file.strip()
    if requested:
        candidate = Path(requested).expanduser()
        return candidate if candidate.is_absolute() else (reports_dir / candidate)

    brief_meta_path = reports_dir / "strategy-brief.meta.json"
    if brief_meta_path.exists():
        try:
            payload = json.loads(brief_meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        if isinstance(payload, dict):
            raw = str(payload.get("strategy_report_path", "")).strip()
            if raw:
                candidate = Path(raw).expanduser()
                if not candidate.is_absolute():
                    candidate = reports_dir / candidate
                if candidate.exists():
                    return candidate

    default_path = reports_dir / "strategy-report.json"
    if default_path.exists():
        return default_path
    return None


def _cmd_strategy_settle(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    seed_path = (
        Path(str(args.seed_path)).expanduser()
        if str(getattr(args, "seed_path", "")).strip()
        else reports_dir / "backtest-seed.jsonl"
    )
    seed_rows_override: list[dict[str, Any]] | None = None
    strategy_report_for_settlement = ""
    strategy_report_path = _resolve_settlement_strategy_report_path(
        reports_dir=reports_dir,
        strategy_report_file=str(getattr(args, "strategy_report_file", "")),
    )
    using_default_seed_path = not str(getattr(args, "seed_path", "")).strip()
    if using_default_seed_path and strategy_report_path is not None:
        try:
            payload = json.loads(strategy_report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CLIError(f"invalid strategy report: {strategy_report_path}") from exc
        if isinstance(payload, dict):
            selection = "eligible"
            ranked = payload.get("ranked_plays")
            if isinstance(ranked, list) and ranked:
                selection = "ranked"
            seed_rows_override = build_backtest_seed_rows(
                report=payload,
                selection=selection,
                top_n=0,
            )
            strategy_report_for_settlement = str(strategy_report_path)
    if using_default_seed_path and seed_rows_override is None and not seed_path.exists():
        if strategy_report_path is None:
            raise CLIError(f"missing backtest seed file: {seed_path}")
        raise CLIError(
            f"could not derive settlement rows from strategy report: {strategy_report_path}"
        )

    def _resolve_settlement_suffix(
        *,
        seed_rows: list[dict[str, Any]] | None,
        seed_path: Path,
        using_default_seed_path: bool,
        strategy_report_path: Path | None,
    ) -> str:
        if using_default_seed_path and (
            strategy_report_path is None or strategy_report_path.name == "strategy-report.json"
        ):
            return ""
        resolved_rows = seed_rows
        if resolved_rows is None and seed_path.exists():
            try:
                resolved_rows = load_jsonl(seed_path)
            except OSError:
                resolved_rows = None
        if resolved_rows:
            for row in resolved_rows:
                if not isinstance(row, dict):
                    continue
                candidate = str(row.get("strategy_id", "")).strip()
                if candidate:
                    return normalize_strategy_id(candidate)
        return ""

    output_suffix = _resolve_settlement_suffix(
        seed_rows=seed_rows_override,
        seed_path=seed_path,
        using_default_seed_path=using_default_seed_path,
        strategy_report_path=strategy_report_path,
    )

    report = settle_snapshot(
        snapshot_dir=snapshot_dir,
        reports_dir=reports_dir,
        snapshot_id=snapshot_id,
        seed_path=seed_path,
        offline=bool(args.offline),
        refresh_results=bool(args.refresh_results),
        write_csv=bool(args.write_csv),
        results_source=str(getattr(args, "results_source", "auto")),
        write_markdown=bool(getattr(args, "write_markdown", False)),
        keep_tex=bool(getattr(args, "keep_tex", False)),
        write_pdf=not bool(getattr(args, "no_pdf", False)),
        output_suffix=output_suffix,
        seed_rows_override=seed_rows_override,
        strategy_report_path=strategy_report_for_settlement,
    )

    if bool(getattr(args, "json_output", True)):
        print(json.dumps(report, sort_keys=True, indent=2))
    else:
        counts = report.get("counts", {}) if isinstance(report.get("counts"), dict) else {}
        artifacts = report.get("artifacts", {}) if isinstance(report.get("artifacts"), dict) else {}
        print(
            (
                "snapshot_id={} status={} exit_code={} total={} win={} loss={} push={} "
                "pending={} unresolved={} pdf_status={}"
            ).format(
                snapshot_id,
                report.get("status", ""),
                report.get("exit_code", 1),
                counts.get("total", 0),
                counts.get("win", 0),
                counts.get("loss", 0),
                counts.get("push", 0),
                counts.get("pending", 0),
                counts.get("unresolved", 0),
                report.get("pdf_status", ""),
            )
        )
        print(f"settlement_json={artifacts.get('json', '')}")
        settlement_md = str(artifacts.get("md", "")).strip()
        if settlement_md:
            print(f"settlement_md={settlement_md}")
        settlement_tex = str(artifacts.get("tex", "")).strip()
        if settlement_tex:
            print(f"settlement_tex={settlement_tex}")
        print(f"settlement_pdf={artifacts.get('pdf', '')}")
        print(f"settlement_meta={artifacts.get('meta', '')}")
        csv_artifact = str(artifacts.get("csv", "")).strip()
        if csv_artifact:
            print(f"settlement_csv={csv_artifact}")

    return int(report.get("exit_code", 1))
