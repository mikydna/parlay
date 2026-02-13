"""Discovery-vs-execution comparison report helpers."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from prop_ev.odds_math import american_to_decimal, ev_from_prob_and_price
from prop_ev.report_paths import snapshot_reports_dir
from prop_ev.storage import SnapshotStore
from prop_ev.time_utils import iso_z, utc_now


def _utc_now() -> datetime:
    return utc_now()


def _iso(dt: datetime) -> str:
    return iso_z(dt)


def _to_price(value: Any) -> int | None:
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


def _american_to_decimal(price: int | None) -> float | None:
    return american_to_decimal(price)


def _ev_from_prob_and_price(probability: float | None, price: int | None) -> float | None:
    return ev_from_prob_and_price(probability, price)


def _format_price(price: int | None) -> str:
    if price is None:
        return ""
    if price > 0:
        return f"+{price}"
    return str(price)


def _price_meets_threshold(price: int | None, threshold: int | None) -> bool:
    if price is None or threshold is None:
        return False
    return price >= threshold


def _row_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("event_id", "")),
        str(row.get("player", "")),
        str(row.get("market", "")),
        str(row.get("point", "")),
    )


def _candidate_pre_bet_ready(row: dict[str, Any]) -> bool:
    raw = row.get("pre_bet_ready")
    if isinstance(raw, bool):
        return raw
    injury_status = str(row.get("injury_status", "")).strip().lower()
    roster_status = str(row.get("roster_status", "")).strip().lower()
    return injury_status in {"available", "available_unlisted"} and roster_status in {
        "active",
        "rostered",
    }


def _price_for_side(row: dict[str, Any], side: str) -> tuple[int | None, str]:
    normalized = side.lower().strip()
    if normalized == "under":
        return _to_price(row.get("under_best_price")), str(row.get("under_best_book", ""))
    return _to_price(row.get("over_best_price")), str(row.get("over_best_book", ""))


def build_discovery_execution_report(
    *,
    discovery_snapshot_id: str,
    execution_snapshot_id: str,
    discovery_report: dict[str, Any],
    execution_report: dict[str, Any],
    top_n: int,
) -> dict[str, Any]:
    """Build a deterministic comparison between discovery and execution books."""
    discovery_candidates = (
        discovery_report.get("candidates", [])
        if isinstance(discovery_report.get("candidates"), list)
        else []
    )
    execution_candidates = (
        execution_report.get("candidates", [])
        if isinstance(execution_report.get("candidates"), list)
        else []
    )
    discovery_rows = [
        row
        for row in discovery_candidates
        if isinstance(row, dict) and bool(row.get("eligible")) and _candidate_pre_bet_ready(row)
    ]
    execution_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in execution_candidates:
        if not isinstance(row, dict):
            continue
        execution_by_key[_row_key(row)] = row

    actionable: list[dict[str, Any]] = []
    misses: list[dict[str, Any]] = []
    unmatched = 0
    for row in discovery_rows:
        side = str(row.get("recommended_side", "")).lower().strip()
        if side not in {"over", "under"}:
            continue
        key = _row_key(row)
        execution_row = execution_by_key.get(key)
        if execution_row is None:
            unmatched += 1
            continue
        execution_price, execution_book = _price_for_side(execution_row, side)
        discovery_model_p = row.get("model_p_hit")
        if isinstance(discovery_model_p, bool):
            discovery_model_p = None
        if isinstance(discovery_model_p, int):
            discovery_model_p = float(discovery_model_p)
        if isinstance(discovery_model_p, str):
            try:
                discovery_model_p = float(discovery_model_p)
            except ValueError:
                discovery_model_p = None
        if not isinstance(discovery_model_p, float):
            discovery_model_p = None

        play_to = _to_price(row.get("play_to_american"))
        meets_play_to = _price_meets_threshold(execution_price, play_to)
        execution_ev_at_discovery_p = _ev_from_prob_and_price(discovery_model_p, execution_price)
        record = {
            "event_id": str(row.get("event_id", "")),
            "game": str(row.get("game", "")),
            "player": str(row.get("player", "")),
            "market": str(row.get("market", "")),
            "point": row.get("point"),
            "side": side,
            "ticket": (
                f"{row.get('player', '')} {side.upper()} {row.get('point', '')} "
                f"{row.get('market', '')}"
            ).strip(),
            "discovery_price": _to_price(row.get("selected_price")),
            "discovery_book": str(row.get("selected_book", "")),
            "discovery_best_ev": row.get("best_ev"),
            "discovery_play_to": play_to,
            "execution_price": execution_price,
            "execution_book": execution_book,
            "execution_native_best_ev": execution_row.get("best_ev"),
            "execution_ev_at_discovery_p": execution_ev_at_discovery_p,
            "meets_play_to": meets_play_to,
            "tier": str(execution_row.get("tier", "")),
        }
        if meets_play_to:
            actionable.append(record)
        else:
            misses.append(record)

    actionable.sort(
        key=lambda row: (
            -(
                float(row["execution_ev_at_discovery_p"])
                if row["execution_ev_at_discovery_p"] is not None
                else -999.0
            ),
            -(float(row["discovery_best_ev"]) if row["discovery_best_ev"] is not None else -999.0),
            row["game"],
            row["player"],
        )
    )
    misses.sort(
        key=lambda row: (
            -(float(row["discovery_best_ev"]) if row["discovery_best_ev"] is not None else -999.0),
            row["game"],
            row["player"],
        )
    )

    summary = {
        "discovery_snapshot_id": discovery_snapshot_id,
        "execution_snapshot_id": execution_snapshot_id,
        "discovery_eligible_rows": len(discovery_rows),
        "matched_execution_rows": len(actionable) + len(misses),
        "unmatched_rows": unmatched,
        "actionable_rows": len(actionable),
        "miss_rows": len(misses),
    }

    return {
        "generated_at_utc": _iso(_utc_now()),
        "summary": summary,
        "actionable": actionable[:top_n],
        "misses": misses[:top_n],
    }


def _render_discovery_execution_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    actionable = report.get("actionable", []) if isinstance(report.get("actionable"), list) else []
    misses = report.get("misses", []) if isinstance(report.get("misses"), list) else []

    lines: list[str] = []
    lines.append("# Discovery vs Execution Report")
    lines.append("")
    lines.append("- discovery_snapshot_id: `{}`".format(summary.get("discovery_snapshot_id", "")))
    lines.append("- execution_snapshot_id: `{}`".format(summary.get("execution_snapshot_id", "")))
    lines.append(
        "- discovery_eligible_rows: `{}`".format(summary.get("discovery_eligible_rows", 0))
    )
    lines.append("- matched_execution_rows: `{}`".format(summary.get("matched_execution_rows", 0)))
    lines.append("- actionable_rows: `{}`".format(summary.get("actionable_rows", 0)))
    lines.append("- misses: `{}`".format(summary.get("miss_rows", 0)))
    lines.append("")
    lines.append(
        "Actionable rows are discovery-signaled plays where execution books still meet "
        "discovery PLAY-TO."
    )
    lines.append("")
    lines.append("## Actionable")
    lines.append("")
    if not actionable:
        lines.append("- none")
    else:
        lines.append(
            "| Game | Ticket | Discovery (book/price) | Execution (book/price) | "
            "Discovery PLAY-TO | Exec EV@Discovery p |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for row in actionable:
            if not isinstance(row, dict):
                continue
            ev = row.get("execution_ev_at_discovery_p")
            ev_text = f"{(ev * 100.0):+.2f}%" if isinstance(ev, (int, float)) else ""
            lines.append(
                "| {} | {} | {} {} | {} {} | {} | {} |".format(
                    row.get("game", ""),
                    row.get("ticket", ""),
                    row.get("discovery_book", ""),
                    _format_price(_to_price(row.get("discovery_price"))),
                    row.get("execution_book", ""),
                    _format_price(_to_price(row.get("execution_price"))),
                    _format_price(_to_price(row.get("discovery_play_to"))),
                    ev_text,
                )
            )
    lines.append("")
    lines.append("## Near Misses")
    lines.append("")
    if not misses:
        lines.append("- none")
    else:
        lines.append(
            "| Game | Ticket | Discovery (book/price) | Execution (book/price) | "
            "Discovery PLAY-TO |"
        )
        lines.append("| --- | --- | --- | --- | --- |")
        for row in misses:
            if not isinstance(row, dict):
                continue
            lines.append(
                "| {} | {} | {} {} | {} {} | {} |".format(
                    row.get("game", ""),
                    row.get("ticket", ""),
                    row.get("discovery_book", ""),
                    _format_price(_to_price(row.get("discovery_price"))),
                    row.get("execution_book", ""),
                    _format_price(_to_price(row.get("execution_price"))),
                    _format_price(_to_price(row.get("discovery_play_to"))),
                )
            )
    lines.append("")
    return "\n".join(lines)


def write_discovery_execution_reports(
    *,
    store: SnapshotStore,
    execution_snapshot_id: str,
    report: dict[str, Any],
    write_markdown: bool = False,
) -> tuple[Path, Path | None]:
    """Write discovery-vs-execution JSON and optional markdown artifacts."""
    reports_dir = snapshot_reports_dir(store, execution_snapshot_id)
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "discovery-execution.json"
    md_path = reports_dir / "discovery-execution.md"
    json_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    if write_markdown:
        md_path.write_text(_render_discovery_execution_markdown(report), encoding="utf-8")
        return json_path, md_path
    if md_path.exists():
        md_path.unlink()
    return json_path, None
