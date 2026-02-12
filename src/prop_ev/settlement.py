"""Ticket settlement for strategy backtest seeds."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import httpx

from prop_ev.context_sources import (
    BOXSCORE_URL_TEMPLATE,
    TODAYS_SCOREBOARD_URL,
    canonical_team_name,
    load_or_fetch_context,
    normalize_person_name,
    now_utc,
)
from prop_ev.latex_renderer import render_pdf_from_markdown

RESULTS_SOURCE = "nba_live_scoreboard_boxscore"


def _http_get(url: str, *, timeout_s: float = 12.0) -> httpx.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; prop-ev/0.1.0)",
        "Accept": "application/json;q=0.9,*/*;q=0.8",
    }
    response = httpx.get(url, headers=headers, timeout=timeout_s, follow_redirects=True)
    response.raise_for_status()
    return response


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


def _game_status(code: Any, text: str) -> str:
    if isinstance(code, str):
        parsed = _safe_float(code)
        code = int(parsed) if parsed is not None else None
    elif isinstance(code, float):
        code = int(code)
    elif not isinstance(code, int):
        code = None

    cleaned = text.strip().lower()
    if code == 3 or cleaned.startswith("final"):
        return "final"
    if code == 2:
        return "in_progress"
    if code == 1:
        return "scheduled"
    return "unknown"


def _extract_players(game_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    players: dict[str, dict[str, Any]] = {}
    for side in ("homeTeam", "awayTeam"):
        team_payload = game_payload.get(side, {})
        if not isinstance(team_payload, dict):
            continue
        team_players = team_payload.get("players", [])
        if not isinstance(team_players, list):
            continue
        for item in team_players:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            key = normalize_person_name(name)
            if not key:
                continue
            stats = item.get("statistics", {})
            players[key] = {
                "name": name,
                "statistics": stats if isinstance(stats, dict) else {},
                "status": str(item.get("status", "")),
            }
    return players


def fetch_nba_live_results(*, teams_in_scope: set[str]) -> dict[str, Any]:
    """Fetch live scoreboard + boxscore payloads and normalize for settlement."""
    payload: dict[str, Any] = {
        "source": RESULTS_SOURCE,
        "url": TODAYS_SCOREBOARD_URL,
        "fetched_at_utc": now_utc(),
        "status": "ok",
        "games": [],
        "errors": [],
    }
    scoreboard = _http_get(TODAYS_SCOREBOARD_URL).json()
    games = scoreboard.get("scoreboard", {}).get("games", [])
    if not isinstance(games, list):
        games = []

    normalized_games: list[dict[str, Any]] = []
    for game in games:
        if not isinstance(game, dict):
            continue
        game_id = str(game.get("gameId", ""))
        home = game.get("homeTeam", {})
        away = game.get("awayTeam", {})
        if not isinstance(home, dict) or not isinstance(away, dict):
            continue
        home_team = canonical_team_name(f"{home.get('teamCity', '')} {home.get('teamName', '')}")
        away_team = canonical_team_name(f"{away.get('teamCity', '')} {away.get('teamName', '')}")
        if teams_in_scope and home_team not in teams_in_scope and away_team not in teams_in_scope:
            continue

        game_status_text = str(game.get("gameStatusText", ""))
        status = _game_status(game.get("gameStatus"), game_status_text)
        game_row: dict[str, Any] = {
            "game_id": game_id,
            "home_team": home_team,
            "away_team": away_team,
            "game_status": status,
            "game_status_text": game_status_text,
            "players": {},
            "period": "",
            "game_clock": "",
        }

        if not game_id:
            payload["errors"].append("missing_game_id")
            normalized_games.append(game_row)
            continue

        boxscore_url = BOXSCORE_URL_TEMPLATE.format(game_id=game_id)
        try:
            boxscore = _http_get(boxscore_url).json()
        except Exception as exc:
            payload["errors"].append(f"{game_id}:{exc}")
            normalized_games.append(game_row)
            continue

        game_payload = boxscore.get("game", {})
        if not isinstance(game_payload, dict):
            normalized_games.append(game_row)
            continue
        game_row["players"] = _extract_players(game_payload)
        game_row["period"] = str(game_payload.get("period", ""))
        game_row["game_clock"] = str(game_payload.get("gameClock", ""))
        boxscore_status = str(game_payload.get("gameStatusText", ""))
        if boxscore_status:
            game_row["game_status_text"] = boxscore_status
            game_row["game_status"] = _game_status(game_payload.get("gameStatus"), boxscore_status)
        normalized_games.append(game_row)

    if payload["errors"] and not normalized_games:
        payload["status"] = "error"
    elif payload["errors"]:
        payload["status"] = "partial"
    payload["games"] = normalized_games
    payload["count_games"] = len(normalized_games)
    payload["count_errors"] = len(payload["errors"])
    return payload


def _seed_teams(seed_rows: list[dict[str, Any]]) -> set[str]:
    teams: set[str] = set()
    for row in seed_rows:
        home = canonical_team_name(str(row.get("home_team", "")))
        away = canonical_team_name(str(row.get("away_team", "")))
        if home:
            teams.add(home)
        if away:
            teams.add(away)
    return teams


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
        "game": str(row.get("game", "")),
        "home_team": home_team,
        "away_team": away_team,
        "ticket": _ticket_label(row),
        "player": str(row.get("player", "")),
        "market": str(row.get("market", "")),
        "recommended_side": str(row.get("recommended_side", "")),
        "point": point_value,
        "game_status": "unknown",
        "game_status_text": "",
        "actual_stat_value": None,
        "result": "unresolved",
        "result_reason": "game_not_found",
        "resolved_at_utc": now_utc(),
        "source": RESULTS_SOURCE,
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
    *, seed_rows: list[dict[str, Any]], results_payload: dict[str, Any]
) -> list[dict[str, Any]]:
    """Grade seed rows using normalized results payload."""
    game_index = _build_game_index(results_payload)
    return [_settle_row(row, game_index=game_index) for row in seed_rows]


def _count_rows(rows: list[dict[str, Any]], key: str, value: str) -> int:
    return sum(1 for row in rows if str(row.get(key, "")).strip().lower() == value)


def _fmt_num(value: Any) -> str:
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}"
    return str(value) if value is not None else ""


def render_settlement_markdown(report: dict[str, Any]) -> str:
    """Render deterministic markdown for ticket settlement."""
    summary = report.get("counts", {}) if isinstance(report.get("counts"), dict) else {}
    rows = report.get("rows", []) if isinstance(report.get("rows"), list) else []
    source_details = (
        report.get("source_details", {}) if isinstance(report.get("source_details"), dict) else {}
    )

    lines: list[str] = []
    lines.append("# Backtest Settlement")
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
    lines.append("")
    lines.append("## Tickets")
    lines.append("")
    if not rows:
        lines.append("- none")
        return "\n".join(lines) + "\n"

    lines.append(
        "| Ticket | Game | Market | Side | Line | Actual | Result | Reason | Game Status |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        if not isinstance(row, dict):
            continue
        lines.append(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                row.get("ticket", ""),
                row.get("game", ""),
                row.get("market", ""),
                str(row.get("recommended_side", "")).upper(),
                _fmt_num(row.get("point")),
                _fmt_num(row.get("actual_stat_value")),
                row.get("result", ""),
                row.get("result_reason", ""),
                row.get("game_status_text", "") or row.get("game_status", ""),
            )
        )
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
        "game",
        "home_team",
        "away_team",
        "ticket",
        "player",
        "market",
        "recommended_side",
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
    snapshot_id: str,
    seed_path: Path,
    offline: bool,
    refresh_results: bool,
    write_csv: bool,
) -> dict[str, Any]:
    """Settle snapshot seed tickets and write report artifacts."""
    seed_rows = _load_jsonl(seed_path)
    if not seed_rows:
        raise ValueError(f"no seed rows found in {seed_path}")

    teams_in_scope = _seed_teams(seed_rows)
    results_cache_path = snapshot_dir / "context" / "results-live.json"
    results_payload = load_or_fetch_context(
        cache_path=results_cache_path,
        offline=offline,
        refresh=refresh_results,
        fetcher=lambda: fetch_nba_live_results(teams_in_scope=teams_in_scope),
    )
    status = str(results_payload.get("status", ""))
    if status not in {"ok", "partial"}:
        error = str(results_payload.get("error", "")).strip()
        if error:
            raise ValueError(f"results fetch failed: {error}")
        raise ValueError("results fetch failed")

    rows = grade_seed_rows(seed_rows=seed_rows, results_payload=results_payload)
    counts = _build_counts(rows)
    overall = "complete" if counts["pending"] == 0 and counts["unresolved"] == 0 else "partial"
    exit_code = 0 if overall == "complete" else 1

    reports_dir = snapshot_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "backtest-settlement.json"
    md_path = reports_dir / "backtest-settlement.md"
    tex_path = reports_dir / "backtest-settlement.tex"
    pdf_path = reports_dir / "backtest-settlement.pdf"
    csv_path = reports_dir / "backtest-settlement.csv"
    meta_path = reports_dir / "backtest-settlement.meta.json"

    report: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "generated_at_utc": now_utc(),
        "status": overall,
        "exit_code": exit_code,
        "counts": counts,
        "source_details": {
            "source": str(results_payload.get("source", RESULTS_SOURCE)),
            "fetched_at_utc": str(results_payload.get("fetched_at_utc", "")),
            "status": status,
            "offline": offline,
            "refresh_results": refresh_results,
            "results_cache_path": str(results_cache_path),
            "results_errors": results_payload.get("errors", []),
        },
        "rows": rows,
    }
    markdown = render_settlement_markdown(report)
    md_path.write_text(markdown, encoding="utf-8")
    pdf_result = render_pdf_from_markdown(
        markdown,
        tex_path=tex_path,
        pdf_path=pdf_path,
        title="Backtest Settlement",
        landscape=True,
    )
    if write_csv:
        _write_csv(csv_path, rows)

    artifacts = {
        "json": str(json_path),
        "md": str(md_path),
        "tex": str(tex_path),
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
