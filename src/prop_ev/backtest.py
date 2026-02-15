"""Backtest preparation artifacts for strategy snapshots."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from prop_ev.nba_data.repo import NBARepository
from prop_ev.time_utils import utc_now_str
from prop_ev.util.parsing import safe_float as _safe_float

ROW_SELECTIONS = {"eligible", "ranked", "top_ev", "one_source", "all_candidates"}


def _now_utc() -> str:
    return utc_now_str()


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.startswith("+"):
            raw = raw[1:]
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _ticket_key(snapshot_id: str, row: dict[str, Any]) -> str:
    # Keep this identity stable across strategy variants so we can compare
    # the same underlying prop even when selected book/price changes.
    del snapshot_id  # retained in signature for backwards compatibility
    payload = {
        "version": 2,
        "event_id": str(row.get("event_id", "")).strip(),
        "player": str(row.get("player", "")).strip().lower(),
        "market": str(row.get("market", "")).strip().lower(),
        "point": _safe_float(row.get("point")),
        "side": str(row.get("recommended_side", "")).strip().lower(),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def _pick_rows(report: dict[str, Any], selection: str, top_n: int) -> list[dict[str, Any]]:
    selected = selection.strip().lower()
    if selected not in ROW_SELECTIONS:
        raise ValueError(f"invalid selection: {selection}")
    if selected == "eligible":
        rows = report.get("candidates", [])
        if not isinstance(rows, list):
            rows = []
        picked = [
            item
            for item in rows
            if isinstance(item, dict)
            and bool(item.get("eligible"))
            and str(item.get("event_id", ""))
        ]
    elif selected == "ranked":
        rows = report.get("ranked_plays", [])
        if not isinstance(rows, list):
            rows = []
        picked = [item for item in rows if isinstance(item, dict) and str(item.get("event_id", ""))]
    elif selected == "top_ev":
        rows = report.get("top_ev_plays", [])
        if not isinstance(rows, list):
            rows = []
        picked = [item for item in rows if isinstance(item, dict) and str(item.get("event_id", ""))]
    elif selected == "one_source":
        rows = report.get("one_source_edges", [])
        if not isinstance(rows, list):
            rows = []
        picked = [item for item in rows if isinstance(item, dict) and str(item.get("event_id", ""))]
    else:
        rows = report.get("candidates", [])
        if not isinstance(rows, list):
            rows = []
        picked = [item for item in rows if isinstance(item, dict) and str(item.get("event_id", ""))]

    if top_n > 0:
        return picked[:top_n]
    return picked


def build_backtest_seed_rows(
    *, report: dict[str, Any], selection: str, top_n: int
) -> list[dict[str, Any]]:
    snapshot_id = str(report.get("snapshot_id", ""))
    strategy_id = str(report.get("strategy_id", ""))
    generated_at_utc = str(report.get("generated_at_utc", ""))
    modeled_date_et = str(report.get("modeled_date_et", ""))
    strategy_mode = str(report.get("strategy_mode", ""))
    strategy_status = str(report.get("strategy_status", ""))
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    rows = _pick_rows(report, selection=selection, top_n=top_n)

    seed: list[dict[str, Any]] = []
    for row in rows:
        ticket_key = _ticket_key(snapshot_id, row)
        seed.append(
            {
                "ticket_key": ticket_key,
                "snapshot_id": snapshot_id,
                "strategy_id": strategy_id,
                "snapshot_generated_at_utc": generated_at_utc,
                "modeled_date_et": modeled_date_et,
                "strategy_mode": strategy_mode,
                "strategy_status": strategy_status,
                "selection_mode": selection,
                "event_id": str(row.get("event_id", "")),
                "game": str(row.get("game", "")),
                "tip_et": str(row.get("tip_et", "")),
                "home_team": str(row.get("home_team", "")),
                "away_team": str(row.get("away_team", "")),
                "player": str(row.get("player", "")),
                "market": str(row.get("market", "")),
                "recommended_side": str(row.get("recommended_side", "")),
                "point": _safe_float(row.get("point")),
                "tier": str(row.get("tier", "")),
                "selected_book": str(row.get("selected_book", "")),
                "selected_price_american": _safe_int(row.get("selected_price")),
                "play_to_american": _safe_int(row.get("play_to_american")),
                "play_to_decimal": _safe_float(row.get("play_to_decimal")),
                "model_p_hit": _safe_float(row.get("model_p_hit")),
                "p_hit_low": _safe_float(row.get("p_hit_low")),
                "p_hit_high": _safe_float(row.get("p_hit_high")),
                "fair_p_hit": _safe_float(row.get("fair_p_hit")),
                "best_ev": _safe_float(row.get("best_ev")),
                "ev_low": _safe_float(row.get("ev_low")),
                "ev_high": _safe_float(row.get("ev_high")),
                "edge_pct": _safe_float(row.get("edge_pct")),
                "ev_per_100": _safe_float(row.get("ev_per_100")),
                "uncertainty_band": _safe_float(row.get("uncertainty_band")),
                "quality_score": _safe_float(row.get("quality_score")),
                "depth_score": _safe_float(row.get("depth_score")),
                "hold_score": _safe_float(row.get("hold_score")),
                "dispersion_score": _safe_float(row.get("dispersion_score")),
                "freshness_score": _safe_float(row.get("freshness_score")),
                "full_kelly": _safe_float(row.get("full_kelly")),
                "quarter_kelly": _safe_float(row.get("quarter_kelly")),
                "injury_status": str(row.get("injury_status", "")),
                "roster_status": str(row.get("roster_status", "")),
                "quote_last_update_utc": str(row.get("selected_last_update", "")),
                "quote_link": str(row.get("selected_link", "")),
                "reason": str(row.get("reason", "")),
                "summary_candidate_lines": int(summary.get("candidate_lines", 0)),
                "summary_eligible_lines": int(summary.get("eligible_lines", 0)),
                "summary_events": int(summary.get("events", 0)),
                "actual_stat_value": None,
                "result": "",
                "graded_price_american": None,
                "stake_units": None,
                "pnl_units": None,
                "graded_at_utc": "",
                "grading_notes": "",
            }
        )
    return seed


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _write_template_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "ticket_key",
        "snapshot_id",
        "strategy_id",
        "event_id",
        "game",
        "tip_et",
        "player",
        "market",
        "recommended_side",
        "point",
        "tier",
        "selected_book",
        "selected_price_american",
        "play_to_american",
        "model_p_hit",
        "p_hit_low",
        "p_hit_high",
        "best_ev",
        "ev_low",
        "ev_high",
        "quality_score",
        "depth_score",
        "hold_score",
        "dispersion_score",
        "freshness_score",
        "uncertainty_band",
        "actual_stat_value",
        "result",
        "graded_price_american",
        "stake_units",
        "pnl_units",
        "graded_at_utc",
        "grading_notes",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in columns})


def _json_path_status(path: Path) -> dict[str, Any]:
    return {"path": str(path), "exists": path.exists()}


def _jsonl_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object JSON in {path}")
    return payload


def _context_status(path: Path) -> dict[str, Any]:
    base = {"path": str(path), "exists": path.exists(), "status": "missing", "stale": True}
    if not path.exists():
        return base
    payload = _load_json(path)
    status = str(payload.get("status", "ok"))
    stale = bool(payload.get("stale", False))
    if "official" in payload and isinstance(payload.get("official"), dict):
        status = str(payload["official"].get("status", status))
    return {
        "path": str(path),
        "exists": True,
        "status": status,
        "stale": stale,
    }


def build_backtest_readiness(
    *,
    snapshot_dir: Path,
    reports_dir: Path,
    report: dict[str, Any],
    seed_rows: list[dict[str, Any]],
    selection: str,
    top_n: int,
    strategy_report_path: Path | None = None,
) -> dict[str, Any]:
    health = (
        report.get("health_report", {}) if isinstance(report.get("health_report"), dict) else {}
    )
    health_gates = (
        health.get("health_gates", []) if isinstance(health.get("health_gates"), list) else []
    )
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    derived_dir = snapshot_dir / "derived"
    event_props_path = derived_dir / "event_props.jsonl"
    featured_path = derived_dir / "featured_odds.jsonl"
    strategy_path = strategy_report_path or (reports_dir / "strategy-report.json")
    odds_data_root = snapshot_dir.parent.parent
    context_repo = NBARepository(
        odds_data_root=odds_data_root,
        snapshot_id=str(report.get("snapshot_id", "")).strip() or snapshot_dir.name,
        snapshot_dir=snapshot_dir,
    )
    injuries_path, roster_path, _results_path = context_repo.context_paths()
    official_pdf_dir = context_repo.official_injury_pdf_dir()

    injuries = _context_status(injuries_path)
    roster = _context_status(roster_path)
    official_pdf_exists = official_pdf_dir.exists() and any(official_pdf_dir.glob("*.pdf"))

    ready_for_seed = strategy_path.exists() and event_props_path.exists() and len(seed_rows) > 0
    ready_for_settlement = False

    missing_for_settlement: list[str] = []
    missing_for_settlement.append("final player boxscore stats for each ticket")
    missing_for_settlement.append("graded result per ticket (win/loss/push)")
    missing_for_settlement.append("stake units (optional; needed for pnl in units)")

    return {
        "snapshot_id": str(report.get("snapshot_id", "")),
        "strategy_id": str(report.get("strategy_id", "")),
        "generated_at_utc": _now_utc(),
        "modeled_date_et": str(report.get("modeled_date_et", "")),
        "strategy_mode": str(report.get("strategy_mode", "")),
        "strategy_status": str(report.get("strategy_status", "")),
        "selection_mode": selection,
        "top_n": top_n,
        "counts": {
            "seed_rows": len(seed_rows),
            "summary_events": int(summary.get("events", 0)),
            "summary_candidate_lines": int(summary.get("candidate_lines", 0)),
            "summary_eligible_lines": int(summary.get("eligible_lines", 0)),
            "event_props_rows": _jsonl_row_count(event_props_path),
            "featured_odds_rows": _jsonl_row_count(featured_path),
        },
        "artifacts": {
            "strategy_report_json": _json_path_status(strategy_path),
            "event_props_jsonl": _json_path_status(event_props_path),
            "featured_odds_jsonl": _json_path_status(featured_path),
            "injuries_context_json": injuries,
            "roster_context_json": roster,
            "official_injury_pdf_cached": {
                "path": str(official_pdf_dir),
                "exists": official_pdf_exists,
            },
        },
        "health_gates": [str(item) for item in health_gates],
        "ready_for_backtest_seed": ready_for_seed,
        "ready_for_settlement": ready_for_settlement,
        "needs_outcome_fill": True,
        "missing_for_settlement": missing_for_settlement,
        "notes": [
            "Seed rows are deterministic snapshot records; do not rewrite entry prices tomorrow.",
            "Fill outcome fields in backtest-results-template.csv after games finish.",
        ],
    }


def write_backtest_artifacts(
    *,
    snapshot_dir: Path,
    reports_dir: Path,
    report: dict[str, Any],
    selection: str = "eligible",
    top_n: int = 0,
    strategy_id: str | None = None,
    write_canonical: bool = True,
) -> dict[str, Any]:
    from prop_ev.strategies.base import normalize_strategy_id

    reports_dir.mkdir(parents=True, exist_ok=True)
    seed_rows = build_backtest_seed_rows(report=report, selection=selection, top_n=top_n)

    canonical_seed_jsonl = reports_dir / "backtest-seed.jsonl"
    canonical_template_csv = reports_dir / "backtest-results-template.csv"
    canonical_readiness_json = reports_dir / "backtest-readiness.json"

    normalized = ""
    if strategy_id:
        normalized = normalize_strategy_id(strategy_id)
    elif str(report.get("strategy_id", "")).strip():
        normalized = normalize_strategy_id(str(report.get("strategy_id", "")).strip())

    def _suffix(path: Path) -> Path:
        return path.with_name(f"{path.stem}.{normalized}{path.suffix}")

    primary_seed = canonical_seed_jsonl
    primary_template = canonical_template_csv
    primary_readiness = canonical_readiness_json
    primary_strategy_report = reports_dir / "strategy-report.json"
    if not write_canonical:
        if not normalized:
            raise ValueError("strategy_id is required when write_canonical=false")
        primary_seed = _suffix(canonical_seed_jsonl)
        primary_template = _suffix(canonical_template_csv)
        primary_readiness = _suffix(canonical_readiness_json)
        primary_strategy_report = _suffix(primary_strategy_report)

    def _write(
        seed_path: Path, template_path: Path, readiness_path: Path, strategy_path: Path
    ) -> None:
        _write_jsonl(seed_path, seed_rows)
        _write_template_csv(template_path, seed_rows)
        readiness = build_backtest_readiness(
            snapshot_dir=snapshot_dir,
            reports_dir=reports_dir,
            report=report,
            seed_rows=seed_rows,
            selection=selection,
            top_n=top_n,
            strategy_report_path=strategy_path,
        )
        _write_json(readiness_path, readiness)

    _write(primary_seed, primary_template, primary_readiness, primary_strategy_report)

    readiness = build_backtest_readiness(
        snapshot_dir=snapshot_dir,
        reports_dir=reports_dir,
        report=report,
        seed_rows=seed_rows,
        selection=selection,
        top_n=top_n,
        strategy_report_path=primary_strategy_report,
    )

    return {
        "seed_jsonl": str(primary_seed),
        "results_template_csv": str(primary_template),
        "readiness_json": str(primary_readiness),
        "selection_mode": selection,
        "top_n": top_n,
        "seed_rows": len(seed_rows),
        "ready_for_backtest_seed": bool(readiness.get("ready_for_backtest_seed", False)),
        "ready_for_settlement": bool(readiness.get("ready_for_settlement", False)),
    }
