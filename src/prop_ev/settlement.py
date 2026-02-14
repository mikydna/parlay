"""Ticket settlement for strategy backtest seeds."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from prop_ev.brief_builder import TEAM_ABBREVIATIONS
from prop_ev.latex_renderer import cleanup_latex_artifacts, render_pdf_from_markdown
from prop_ev.nba_data.context_cache import now_utc
from prop_ev.nba_data.normalize import canonical_team_name, normalize_person_name
from prop_ev.nba_data.repo import NBARepository
from prop_ev.nba_data.source_policy import ResultsSourceMode, normalize_results_source_mode

RESULTS_SOURCE = "nba_results"
MARKET_SHORT_LABELS = {
    "player_points": "P",
    "player_rebounds": "R",
    "player_assists": "A",
    "player_threes": "3PM",
    "player_points_rebounds_assists": "PRA",
}
RESULT_SHORT_LABELS = {
    "win": "W",
    "loss": "L",
    "push": "P",
    "pending": "PD",
    "unresolved": "U",
}
RESULT_REASON_LABELS = {
    "final_settled": "settled",
    "in_progress_pending": "in_progress",
    "scheduled_pending": "scheduled",
    "game_not_found": "game_missing",
    "game_status_unknown": "status_unknown",
    "player_not_found": "player_missing",
    "line_missing": "line_missing",
    "stat_unavailable": "stat_missing",
    "unsupported_market": "market_unsupported",
    "unsupported_side": "side_unsupported",
}


def _safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


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
            try:
                parsed = float(raw)
            except ValueError:
                return None
            rounded = round(parsed)
            if abs(parsed - rounded) > 1e-6:
                return None
            return int(rounded)
    return None


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"missing backtest seed file: {path}")
    rows: list[dict[str, Any]] = []
    for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        raw = line.strip()
        if not raw:
            continue
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError(f"invalid jsonl row in {path}:{idx}")
        rows.append(payload)
    return rows


def _row_teams(row: dict[str, Any]) -> tuple[str, str]:
    home = canonical_team_name(str(row.get("home_team", "")))
    away = canonical_team_name(str(row.get("away_team", "")))
    game = str(row.get("game", ""))
    if "@" in game:
        parts = game.split("@", 1)
        parsed_away = canonical_team_name(parts[0])
        parsed_home = canonical_team_name(parts[1])
        if not away and parsed_away:
            away = parsed_away
        if not home and parsed_home:
            home = parsed_home
    return home, away


def _build_game_index(results_payload: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows = results_payload.get("games", [])
    if not isinstance(rows, list):
        return {}
    indexed: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        home = canonical_team_name(str(row.get("home_team", "")))
        away = canonical_team_name(str(row.get("away_team", "")))
        if not home or not away:
            continue
        indexed[(home, away)] = row
    return indexed


def _ticket_label(row: dict[str, Any]) -> str:
    player = str(row.get("player", ""))
    side = str(row.get("recommended_side", "")).upper()
    point = row.get("point")
    market = str(row.get("market", ""))
    return f"{player} {side} {point} {market}".strip()


def _market_actual_value(market: str, statistics: dict[str, Any]) -> tuple[float | None, str]:
    normalized = market.strip().lower()
    points = _safe_float(statistics.get("points"))
    rebounds = _safe_float(statistics.get("reboundsTotal"))
    assists = _safe_float(statistics.get("assists"))
    threes = _safe_float(statistics.get("threePointersMade"))

    if normalized == "player_points":
        return points, "stat_unavailable" if points is None else ""
    if normalized == "player_rebounds":
        return rebounds, "stat_unavailable" if rebounds is None else ""
    if normalized == "player_assists":
        return assists, "stat_unavailable" if assists is None else ""
    if normalized == "player_threes":
        return threes, "stat_unavailable" if threes is None else ""
    if normalized == "player_points_rebounds_assists":
        if points is None or rebounds is None or assists is None:
            return None, "stat_unavailable"
        return points + rebounds + assists, ""
    return None, "unsupported_market"


def _grade_final(side: str, line: float, actual: float) -> str:
    normalized = side.strip().lower()
    if actual == line:
        return "push"
    if normalized == "over":
        return "win" if actual > line else "loss"
    if normalized == "under":
        return "win" if actual < line else "loss"
    return "unresolved"


def _settle_row(
    row: dict[str, Any],
    *,
    game_index: dict[tuple[str, str], dict[str, Any]],
    source: str,
) -> dict[str, Any]:
    ticket_key = str(row.get("ticket_key", ""))
    point_value = _safe_float(row.get("point"))
    home_team, away_team = _row_teams(row)
    game_row = None
    if home_team and away_team:
        game_row = game_index.get((home_team, away_team)) or game_index.get((away_team, home_team))

    base = {
        "ticket_key": ticket_key,
        "snapshot_id": str(row.get("snapshot_id", "")),
        "event_id": str(row.get("event_id", "")),
        "strategy_id": str(row.get("strategy_id", "")),
        "game": str(row.get("game", "")),
        "home_team": home_team,
        "away_team": away_team,
        "ticket": _ticket_label(row),
        "player": str(row.get("player", "")),
        "market": str(row.get("market", "")),
        "recommended_side": str(row.get("recommended_side", "")),
        "selected_book": str(row.get("selected_book", "")),
        "selected_price_american": _safe_int(row.get("selected_price_american")),
        "model_p_hit": _safe_float(row.get("model_p_hit")),
        "p_hit_low": _safe_float(row.get("p_hit_low")),
        "p_hit_high": _safe_float(row.get("p_hit_high")),
        "fair_p_hit": _safe_float(row.get("fair_p_hit")),
        "best_ev": _safe_float(row.get("best_ev")),
        "edge_pct": _safe_float(row.get("edge_pct")),
        "ev_per_100": _safe_float(row.get("ev_per_100")),
        "ev_low": _safe_float(row.get("ev_low")),
        "ev_high": _safe_float(row.get("ev_high")),
        "quality_score": _safe_float(row.get("quality_score")),
        "depth_score": _safe_float(row.get("depth_score")),
        "hold_score": _safe_float(row.get("hold_score")),
        "dispersion_score": _safe_float(row.get("dispersion_score")),
        "freshness_score": _safe_float(row.get("freshness_score")),
        "uncertainty_band": _safe_float(row.get("uncertainty_band")),
        "play_to_american": _safe_float(row.get("play_to_american")),
        "quarter_kelly": _safe_float(row.get("quarter_kelly")),
        "summary_candidate_lines": _safe_int(row.get("summary_candidate_lines")),
        "summary_eligible_lines": _safe_int(row.get("summary_eligible_lines")),
        "point": point_value,
        "game_status": "unknown",
        "game_status_text": "",
        "actual_stat_value": None,
        "result": "unresolved",
        "result_reason": "game_not_found",
        "resolved_at_utc": now_utc(),
        "source": source,
        "source_game_id": "",
    }
    if game_row is None:
        return base

    game_status = str(game_row.get("game_status", "unknown"))
    game_status_text = str(game_row.get("game_status_text", ""))
    source_game_id = str(game_row.get("game_id", ""))
    players = game_row.get("players", {})
    if not isinstance(players, dict):
        players = {}

    player_key = normalize_person_name(base["player"])
    player_row = players.get(player_key) if player_key else None
    stats = {}
    if isinstance(player_row, dict):
        raw_stats = player_row.get("statistics", {})
        if isinstance(raw_stats, dict):
            stats = raw_stats
    actual_value, actual_reason = (
        _market_actual_value(base["market"], stats) if stats else (None, "")
    )

    resolved = dict(base)
    resolved["game_status"] = game_status
    resolved["game_status_text"] = game_status_text
    resolved["source_game_id"] = source_game_id
    resolved["actual_stat_value"] = actual_value

    if game_status == "in_progress":
        resolved["result"] = "pending"
        resolved["result_reason"] = "in_progress_pending"
        return resolved
    if game_status == "scheduled":
        resolved["result"] = "pending"
        resolved["result_reason"] = "scheduled_pending"
        return resolved
    if game_status != "final":
        resolved["result"] = "unresolved"
        resolved["result_reason"] = "game_status_unknown"
        return resolved

    if not isinstance(player_row, dict):
        resolved["result"] = "unresolved"
        resolved["result_reason"] = "player_not_found"
        return resolved
    if point_value is None:
        resolved["result"] = "unresolved"
        resolved["result_reason"] = "line_missing"
        return resolved
    if actual_value is None:
        resolved["result"] = "unresolved"
        resolved["result_reason"] = actual_reason or "stat_unavailable"
        return resolved

    graded = _grade_final(str(row.get("recommended_side", "")), point_value, actual_value)
    if graded == "unresolved":
        resolved["result"] = "unresolved"
        resolved["result_reason"] = "unsupported_side"
        return resolved
    resolved["result"] = graded
    resolved["result_reason"] = "final_settled"
    return resolved


def grade_seed_rows(
    *, seed_rows: list[dict[str, Any]], results_payload: dict[str, Any], source: str
) -> list[dict[str, Any]]:
    """Grade seed rows using normalized results payload."""
    game_index = _build_game_index(results_payload)
    return [_settle_row(row, game_index=game_index, source=source) for row in seed_rows]


def _count_rows(rows: list[dict[str, Any]], key: str, value: str) -> int:
    return sum(1 for row in rows if str(row.get(key, "")).strip().lower() == value)


def _fmt_num(value: Any) -> str:
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}"
    return str(value) if value is not None else ""


def _fmt_pct(
    value: Any, *, assume_fraction: bool = False, signed: bool = False, decimals: int = 1
) -> str:
    parsed = _safe_float(value)
    if parsed is None:
        return ""
    if assume_fraction:
        parsed *= 100.0
    sign = "+" if signed else ""
    return f"{parsed:{sign}.{decimals}f}%"


def _fmt_american(value: Any) -> str:
    parsed = _safe_float(value)
    if parsed is None:
        return ""
    integer = int(round(parsed))
    return f"+{integer}" if integer > 0 else str(integer)


def _team_abbrev(team_name: str) -> str:
    raw = team_name.strip()
    if not raw:
        return ""
    canonical = canonical_team_name(raw)
    if canonical in TEAM_ABBREVIATIONS:
        return TEAM_ABBREVIATIONS[canonical]
    token = raw.replace(".", "").strip()
    if 2 <= len(token) <= 4 and token.isalpha():
        return token.upper()
    words = canonical.split()
    if words:
        return words[-1][:3].upper()
    return raw[:3].upper()


def _short_game_label(row: dict[str, Any]) -> str:
    away = _team_abbrev(str(row.get("away_team", "")))
    home = _team_abbrev(str(row.get("home_team", "")))
    if not away or not home:
        game = str(row.get("game", ""))
        if "@" in game:
            away_raw, home_raw = game.split("@", 1)
            away = away or _team_abbrev(away_raw)
            home = home or _team_abbrev(home_raw)
    if away and home:
        return f"{away} @ {home}"
    return str(row.get("game", "")).strip()


def _market_short_label(market: str) -> str:
    raw = market.strip().lower()
    return MARKET_SHORT_LABELS.get(raw, raw.replace("player_", "").upper())


def _side_short_label(side: str) -> str:
    raw = side.strip().lower()
    if raw == "over":
        return "O"
    if raw == "under":
        return "U"
    return raw.upper()


def _result_short_label(result: str) -> str:
    raw = result.strip().lower()
    return RESULT_SHORT_LABELS.get(raw, raw.upper())


def _reason_short_label(reason: str) -> str:
    raw = reason.strip().lower()
    return RESULT_REASON_LABELS.get(raw, raw)


def _book_price_label(book: str, price: Any) -> str:
    price_text = _fmt_american(price)
    book_text = book.strip().lower()
    if book_text and price_text:
        return f"{book_text} {price_text}"
    return price_text or book_text


def _strategy_seed_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, float]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        strategy = str(row.get("strategy_id", "")).strip() or "unknown"
        bucket = grouped.setdefault(
            strategy,
            {
                "rows": 0.0,
                "edge_total": 0.0,
                "edge_n": 0.0,
                "ev_total": 0.0,
                "ev_n": 0.0,
                "phit_total": 0.0,
                "phit_n": 0.0,
            },
        )
        bucket["rows"] += 1.0
        edge = _safe_float(row.get("edge_pct"))
        if edge is not None:
            bucket["edge_total"] += edge
            bucket["edge_n"] += 1.0
        ev = _safe_float(row.get("ev_per_100"))
        if ev is not None:
            bucket["ev_total"] += ev
            bucket["ev_n"] += 1.0
        phit = _safe_float(row.get("model_p_hit"))
        if phit is not None:
            bucket["phit_total"] += phit
            bucket["phit_n"] += 1.0

    summary_rows: list[dict[str, Any]] = []
    for strategy, bucket in sorted(grouped.items(), key=lambda item: item[0]):
        summary_rows.append(
            {
                "strategy_id": strategy,
                "rows": int(bucket["rows"]),
                "avg_edge_pct": (
                    bucket["edge_total"] / bucket["edge_n"] if bucket["edge_n"] else None
                ),
                "avg_ev_per_100": bucket["ev_total"] / bucket["ev_n"] if bucket["ev_n"] else None,
                "avg_model_p_hit": (
                    bucket["phit_total"] / bucket["phit_n"] if bucket["phit_n"] else None
                ),
            }
        )
    return summary_rows


def render_settlement_markdown(report: dict[str, Any]) -> str:
    """Render deterministic markdown for ticket settlement."""
    summary = report.get("counts", {}) if isinstance(report.get("counts"), dict) else {}
    rows = report.get("rows", []) if isinstance(report.get("rows"), list) else []
    source_details = (
        report.get("source_details", {}) if isinstance(report.get("source_details"), dict) else {}
    )

    lines: list[str] = []
    lines.append("# Settlement")
    lines.append("")
    lines.append(f"- snapshot_id: `{report.get('snapshot_id', '')}`")
    lines.append(f"- generated_at_utc: `{report.get('generated_at_utc', '')}`")
    lines.append(f"- status: `{report.get('status', '')}`")
    lines.append(f"- source: `{source_details.get('source', '')}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(
        "| Total | Win | Loss | Push | Pending | Unresolved | "
        "Final Games | In Progress Games | Scheduled Games |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    lines.append(
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            summary.get("total", 0),
            summary.get("win", 0),
            summary.get("loss", 0),
            summary.get("push", 0),
            summary.get("pending", 0),
            summary.get("unresolved", 0),
            summary.get("final_games", 0),
            summary.get("in_progress_games", 0),
            summary.get("scheduled_games", 0),
        )
    )
    strategy_rows = _strategy_seed_summary(rows)
    if strategy_rows:
        lines.append("## Strategy Inputs (Seed)")
        lines.append("")
        lines.append("| Strategy | Tickets | Avg pHit | Avg Edge% | Avg EV/100 |")
        lines.append("| --- | --- | --- | --- | --- |")
        for item in strategy_rows:
            if not isinstance(item, dict):
                continue
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    item.get("strategy_id", ""),
                    item.get("rows", 0),
                    _fmt_pct(item.get("avg_model_p_hit"), assume_fraction=True, decimals=1),
                    _fmt_pct(item.get("avg_edge_pct"), signed=True, decimals=2),
                    _fmt_num(item.get("avg_ev_per_100")),
                )
            )
        lines.append("")

    lines.append("## Tickets")
    lines.append("")
    if not rows:
        lines.append("- none")
        return "\n".join(lines) + "\n"

    lines.append(
        "| Player | Game | Mkt | Side | Line | Book/Price | pHit | Edge% | EV/100 | "
        "Actual | Result | Reason | Status |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        if not isinstance(row, dict):
            continue
        lines.append(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                row.get("player", ""),
                _short_game_label(row),
                _market_short_label(str(row.get("market", ""))),
                _side_short_label(str(row.get("recommended_side", ""))),
                _fmt_num(row.get("point")),
                _book_price_label(
                    str(row.get("selected_book", "")),
                    row.get("selected_price_american"),
                ),
                _fmt_pct(row.get("model_p_hit"), assume_fraction=True, decimals=1),
                _fmt_pct(row.get("edge_pct"), signed=True, decimals=2),
                _fmt_num(row.get("ev_per_100")),
                _fmt_num(row.get("actual_stat_value")),
                _result_short_label(str(row.get("result", ""))),
                _reason_short_label(str(row.get("result_reason", ""))),
                row.get("game_status_text", "") or row.get("game_status", ""),
            )
        )
    lines.append("")
    lines.append("Legend: `Mkt` uses short labels (P, R, A, 3PM, PRA). `Side`: O/U.")
    lines.append("")
    lines.append(
        "In-progress and scheduled games remain `pending` until final boxscores are available."
    )
    lines.append("")
    return "\n".join(lines)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "ticket_key",
        "snapshot_id",
        "event_id",
        "strategy_id",
        "game",
        "home_team",
        "away_team",
        "ticket",
        "player",
        "market",
        "recommended_side",
        "selected_book",
        "selected_price_american",
        "model_p_hit",
        "p_hit_low",
        "p_hit_high",
        "fair_p_hit",
        "best_ev",
        "edge_pct",
        "ev_per_100",
        "ev_low",
        "ev_high",
        "quality_score",
        "depth_score",
        "hold_score",
        "dispersion_score",
        "freshness_score",
        "uncertainty_band",
        "play_to_american",
        "quarter_kelly",
        "summary_candidate_lines",
        "summary_eligible_lines",
        "point",
        "game_status",
        "game_status_text",
        "actual_stat_value",
        "result",
        "result_reason",
        "resolved_at_utc",
        "source",
        "source_game_id",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in columns})


def _build_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    final_games = {
        str(row.get("source_game_id", ""))
        for row in rows
        if str(row.get("game_status", "")) == "final" and str(row.get("source_game_id", ""))
    }
    in_progress_games = {
        str(row.get("source_game_id", ""))
        for row in rows
        if str(row.get("game_status", "")) == "in_progress" and str(row.get("source_game_id", ""))
    }
    scheduled_games = {
        str(row.get("source_game_id", ""))
        for row in rows
        if str(row.get("game_status", "")) == "scheduled" and str(row.get("source_game_id", ""))
    }
    return {
        "total": len(rows),
        "win": _count_rows(rows, "result", "win"),
        "loss": _count_rows(rows, "result", "loss"),
        "push": _count_rows(rows, "result", "push"),
        "pending": _count_rows(rows, "result", "pending"),
        "unresolved": _count_rows(rows, "result", "unresolved"),
        "final_games": len(final_games),
        "in_progress_games": len(in_progress_games),
        "scheduled_games": len(scheduled_games),
    }


def settle_snapshot(
    *,
    snapshot_dir: Path,
    reports_dir: Path,
    snapshot_id: str,
    seed_path: Path,
    offline: bool,
    refresh_results: bool,
    write_csv: bool,
    results_source: str = "auto",
    write_markdown: bool = False,
    keep_tex: bool = False,
    seed_rows_override: list[dict[str, Any]] | None = None,
    strategy_report_path: str = "",
) -> dict[str, Any]:
    """Settle snapshot seed tickets and write report artifacts."""
    seed_rows = seed_rows_override if seed_rows_override is not None else _load_jsonl(seed_path)
    if not seed_rows:
        raise ValueError(f"no seed rows found in {seed_path}")

    requested_source = str(results_source).strip()
    normalized_source = normalize_results_source_mode(requested_source or "auto")
    effective_source: ResultsSourceMode = "cache_only" if offline else normalized_source
    effective_refresh = bool(refresh_results and not offline)

    odds_data_root = snapshot_dir.parent.parent
    repo = NBARepository(
        odds_data_root=odds_data_root,
        snapshot_id=snapshot_id,
        snapshot_dir=snapshot_dir,
    )
    results_payload, results_cache_path = repo.load_results_for_settlement(
        seed_rows=seed_rows,
        offline=offline,
        refresh=effective_refresh,
        mode=effective_source,
    )
    status = str(results_payload.get("status", ""))
    if status not in {"ok", "partial"}:
        error = str(results_payload.get("error", "")).strip()
        if error:
            raise ValueError(f"results fetch failed: {error}")
        raise ValueError("results fetch failed")

    resolved_source = str(results_payload.get("source", RESULTS_SOURCE)).strip() or RESULTS_SOURCE
    rows = grade_seed_rows(
        seed_rows=seed_rows,
        results_payload=results_payload,
        source=resolved_source,
    )
    counts = _build_counts(rows)
    overall = "complete" if counts["pending"] == 0 and counts["unresolved"] == 0 else "partial"
    exit_code = 0 if overall == "complete" else 1

    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "settlement.json"
    md_path = reports_dir / "settlement.md"
    tex_path = reports_dir / "settlement.tex"
    pdf_path = reports_dir / "settlement.pdf"
    csv_path = reports_dir / "settlement.csv"
    meta_path = reports_dir / "settlement.meta.json"

    source_details: dict[str, Any] = {
        "source": resolved_source,
        "results_source_mode": effective_source,
        "fetched_at_utc": str(results_payload.get("fetched_at_utc", "")),
        "status": status,
        "offline": offline,
        "refresh_results": effective_refresh,
        "seed_source": "override" if seed_rows_override is not None else "seed_file",
        "seed_path": str(seed_path),
        "strategy_report_path": strategy_report_path,
        "write_markdown": bool(write_markdown),
        "keep_tex": bool(keep_tex),
        "results_cache_path": str(results_cache_path),
        "results_errors": results_payload.get("errors", []),
    }

    report: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "generated_at_utc": now_utc(),
        "status": overall,
        "exit_code": exit_code,
        "counts": counts,
        "source_details": source_details,
        "rows": rows,
    }
    markdown = render_settlement_markdown(report)
    if write_markdown:
        md_path.write_text(markdown, encoding="utf-8")
    elif md_path.exists():
        md_path.unlink()
    pdf_result = render_pdf_from_markdown(
        markdown,
        tex_path=tex_path,
        pdf_path=pdf_path,
        title="Settlement",
        landscape=True,
    )
    cleanup_latex_artifacts(tex_path=tex_path, keep_tex=keep_tex)
    if write_csv:
        _write_csv(csv_path, rows)

    artifacts = {
        "json": str(json_path),
        "md": str(md_path) if write_markdown else "",
        "tex": str(tex_path) if keep_tex else "",
        "pdf": str(pdf_path),
        "csv": str(csv_path) if write_csv else "",
        "meta": str(meta_path),
    }
    report["artifacts"] = artifacts
    report["pdf_status"] = str(pdf_result.get("status", ""))
    report["pdf"] = pdf_result
    json_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")

    meta = {
        "snapshot_id": snapshot_id,
        "generated_at_utc": report["generated_at_utc"],
        "status": report["status"],
        "exit_code": report["exit_code"],
        "counts": counts,
        "artifacts": artifacts,
        "pdf": pdf_result,
    }
    meta_path.write_text(json.dumps(meta, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return report
