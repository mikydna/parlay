"""Strategy output rendering and artifact writing helpers."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from prop_ev.execution_plan_contract import assert_execution_plan
from prop_ev.util.parsing import safe_float as _safe_float


def _short_game_label(value: str) -> str:
    return value


def _prop_label(player: str, side: str, point: float, market: str) -> str:
    return f"{player} {side} {point} {market}".strip()


def _fmt_american(value: Any) -> str:
    if value is None:
        return ""
    try:
        price = int(value)
    except (TypeError, ValueError):
        return str(value)
    return f"+{price}" if price > 0 else str(price)


def render_strategy_markdown(report: dict[str, Any], top_n: int) -> str:
    """Render strategy report as an audit-ready markdown card."""
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    health = (
        report.get("health_report", {}) if isinstance(report.get("health_report"), dict) else {}
    )
    slate = (
        report.get("slate_snapshot", []) if isinstance(report.get("slate_snapshot"), list) else []
    )
    availability = (
        report.get("availability", {}) if isinstance(report.get("availability"), dict) else {}
    )
    warnings = (
        report.get("roster_status_warnings", [])
        if isinstance(report.get("roster_status_warnings"), list)
        else []
    )
    verified_players = (
        report.get("verified_players", [])
        if isinstance(report.get("verified_players"), list)
        else []
    )
    unresolved_players = (
        report.get("unresolved_players", [])
        if isinstance(report.get("unresolved_players"), list)
        else []
    )
    top_plays = (
        report.get("top_ev_plays", []) if isinstance(report.get("top_ev_plays"), list) else []
    )
    one_source = (
        report.get("one_source_edges", [])
        if isinstance(report.get("one_source_edges"), list)
        else []
    )
    sgp_candidates = (
        report.get("sgp_candidates", []) if isinstance(report.get("sgp_candidates"), list) else []
    )
    watchlist = (
        report.get("price_dependent_watchlist", [])
        if isinstance(report.get("price_dependent_watchlist"), list)
        else []
    )
    under_sweep = (
        report.get("under_sweep", {}) if isinstance(report.get("under_sweep"), dict) else {}
    )
    kelly = report.get("kelly_summary", []) if isinstance(report.get("kelly_summary"), list) else []
    gaps = report.get("gaps", []) if isinstance(report.get("gaps"), list) else []
    audit = report.get("audit", {}) if isinstance(report.get("audit"), dict) else {}
    audit_rows = audit.get("audit_trail", []) if isinstance(audit.get("audit_trail"), list) else []

    lines: list[str] = []
    lines.append("# NBA Prop EV Card")
    lines.append("")
    lines.append(f"- Modeled date: `{report.get('modeled_date_et', '')}`")
    lines.append(f"- Snapshot ID: `{report.get('snapshot_id', '')}`")
    lines.append(f"- Generated: `{report.get('generated_at_utc', '')}`")
    lines.append(f"- Strategy mode: `{report.get('strategy_mode', '')}`")
    lines.append(
        "- Strategy type: `Player props (over/under)` with Tier A dual-source "
        "and Tier B one-source policy"
    )
    lines.append("")

    lines.append("## Health Report")
    lines.append("")
    lines.append(f"- strategy_mode: `{health.get('strategy_mode', '')}`")
    gates = health.get("health_gates", []) if isinstance(health.get("health_gates"), list) else []
    lines.append(f"- health_gates: `{', '.join(gates) if gates else 'none'}`")
    feeds = health.get("feeds", {}) if isinstance(health.get("feeds"), dict) else {}
    lines.append(
        (
            "- feeds: official_injuries=`{}` secondary_injuries=`{}` roster=`{}` "
            "injuries_stale=`{}` roster_stale=`{}`"
        ).format(
            feeds.get("official_injuries", ""),
            feeds.get("secondary_injuries", ""),
            feeds.get("roster", ""),
            feeds.get("injuries_stale", ""),
            feeds.get("roster_stale", ""),
        )
    )
    odds = health.get("odds", {}) if isinstance(health.get("odds"), dict) else {}
    lines.append(
        (
            "- odds freshness: status=`{}` latest_quote_utc=`{}` "
            "age_latest_min=`{}` stale_after_min=`{}`"
        ).format(
            odds.get("status", ""),
            odds.get("latest_quote_utc", ""),
            odds.get("age_latest_min", ""),
            odds.get("stale_after_min", ""),
        )
    )
    contracts = health.get("contracts", {}) if isinstance(health.get("contracts"), dict) else {}
    props_contract = (
        contracts.get("props_rows", {}) if isinstance(contracts.get("props_rows"), dict) else {}
    )
    lines.append(
        "- contracts: props_rows=`{}` invalid_rows=`{}`".format(
            props_contract.get("row_count", 0),
            props_contract.get("invalid_count", 0),
        )
    )
    lines.append(f"- identity_map_entries: `{health.get('identity_map_entries', 0)}`")
    excluded_games = (
        health.get("excluded_games", []) if isinstance(health.get("excluded_games"), list) else []
    )
    if excluded_games:
        lines.append(f"- excluded_games: `{', '.join(excluded_games)}`")
    lines.append("")

    lines.append("## SLATE SNAPSHOT")
    lines.append("")
    if not slate:
        lines.append("- missing slate rows")
    else:
        lines.append("| Tip (ET) | Away @ Home | Spread | Total |")
        lines.append("| --- | --- | --- | --- |")
        for item in slate:
            if not isinstance(item, dict):
                continue
            lines.append(
                "| {} | {} | {} | {} |".format(
                    item.get("tip_et", ""),
                    _short_game_label(str(item.get("away_home", ""))),
                    item.get("spread", ""),
                    item.get("total", ""),
                )
            )
    lines.append("")

    lines.append("## Availability & Roster Notes")
    lines.append("")
    official = (
        availability.get("official", {}) if isinstance(availability.get("official"), dict) else {}
    )
    secondary = (
        availability.get("secondary", {}) if isinstance(availability.get("secondary"), dict) else {}
    )
    roster = availability.get("roster", {}) if isinstance(availability.get("roster"), dict) else {}
    lines.append(
        "- Official injury source: status=`{}` fetched=`{}` links=`{}` parsed_rows=`{}`".format(
            official.get("status", ""),
            official.get("fetched_at_utc", ""),
            official.get("count", 0),
            official.get("rows_count", 0),
        )
    )
    lines.append(
        "- Official injury PDF cache: status=`{}` path=`{}` parse_status=`{}`".format(
            official.get("pdf_download_status", ""),
            official.get("pdf_cached_path", ""),
            official.get("parse_status", ""),
        )
    )
    lines.append(
        "- Secondary injury source: status=`{}` fetched=`{}` rows=`{}`".format(
            secondary.get("status", ""),
            secondary.get("fetched_at_utc", ""),
            secondary.get("count", 0),
        )
    )
    lines.append(
        "- Roster source: status=`{}` fetched=`{}` team_rows=`{}`".format(
            roster.get("status", ""),
            roster.get("fetched_at_utc", ""),
            roster.get("count_teams", 0),
        )
    )

    key_injuries = (
        availability.get("key_injuries", [])
        if isinstance(availability.get("key_injuries"), list)
        else []
    )
    if key_injuries:
        lines.append("")
        lines.append("- Key injury statuses (today):")
        for row in key_injuries[:12]:
            if not isinstance(row, dict):
                continue
            lines.append(
                "  - {} ({}) status=`{}` note=`{}` update=`{}`".format(
                    row.get("player", ""),
                    row.get("team", ""),
                    row.get("status", ""),
                    row.get("note", ""),
                    row.get("date_update", ""),
                )
            )

    if warnings:
        lines.append("")
        lines.append("- Roster/Status warnings:")
        for warning in warnings[:12]:
            lines.append(f"  - {warning}")
    if unresolved_players:
        lines.append("")
        lines.append("- Unresolved player mappings:")
        for row in unresolved_players[:12]:
            if not isinstance(row, dict):
                continue
            suggestion = row.get("mapping_suggestion", {})
            if isinstance(suggestion, dict):
                suggested_team = str(suggestion.get("suggested_team", ""))
            else:
                suggested_team = ""
            lines.append(
                "  - {} ({}) status=`{}` detail=`{}` suggested_team=`{}`".format(
                    row.get("player", ""),
                    row.get("event_id", ""),
                    row.get("roster_status", ""),
                    row.get("detail", ""),
                    suggested_team,
                )
            )
    lines.append("")

    lines.append("## VERIFIED PLAYERS (TEAM CHECK)")
    lines.append("")
    if not verified_players:
        lines.append("- none")
    else:
        for row in verified_players:
            if not isinstance(row, dict):
                continue
            lines.append(
                "- {} — team=`{}` roster_status=`{}` source=`{}` link={}".format(
                    row.get("player", ""),
                    row.get("team", ""),
                    row.get("roster_status", ""),
                    row.get("verification_source", ""),
                    row.get("verification_link", ""),
                )
            )
    lines.append("")

    lines.append("## TOP EV PLAYS (RANKED)")
    lines.append("")
    if not top_plays:
        lines.append("- none")
    else:
        table_header = (
            "| Game | Player & Prop | Book/Price | p(hit) | Fair | Edge% | EV/$100 | "
            "PLAY-TO | Rationale |"
        )
        lines.append(table_header)
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for row in top_plays[:top_n]:
            if not isinstance(row, dict):
                continue
            play_to_text = (
                f"{_fmt_american(row.get('play_to_american'))} "
                f"(ROI>={((row.get('target_roi') or 0.0) * 100):.1f}%)"
            )
            lines.append(
                "| {} | {} | {} | {:.1f}% | {} / {} | {:+.2f}% | {:+.2f} | {} | {} |".format(
                    _short_game_label(str(row.get("game", ""))),
                    row.get("prop_label", ""),
                    row.get("book_price", ""),
                    (row.get("model_p_hit", 0.0) or 0.0) * 100.0,
                    f"{(row.get('fair_decimal') or 0.0):.3f}" if row.get("fair_decimal") else "",
                    _fmt_american(row.get("fair_american")),
                    row.get("edge_pct", 0.0) or 0.0,
                    row.get("ev_per_100", 0.0) or 0.0,
                    play_to_text,
                    str(row.get("rationale", "")).replace("|", "/"),
                )
            )
    lines.append("")

    lines.append("## ONE-SOURCE EDGES")
    lines.append("")
    if not one_source:
        lines.append("- none")
    else:
        table_header = (
            "| Game | Player & Prop | Book/Price | p(hit) | Fair | Edge% | EV/$100 | "
            "PLAY-TO | Rationale |"
        )
        lines.append(table_header)
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for row in one_source[:top_n]:
            if not isinstance(row, dict):
                continue
            play_to_text = (
                f"{_fmt_american(row.get('play_to_american'))} "
                f"(ROI>={((row.get('target_roi') or 0.0) * 100):.1f}%)"
            )
            lines.append(
                "| {} | {} | {} | {:.1f}% | {} / {} | {:+.2f}% | {:+.2f} | {} | {} |".format(
                    _short_game_label(str(row.get("game", ""))),
                    row.get("prop_label", ""),
                    row.get("book_price", ""),
                    (row.get("model_p_hit", 0.0) or 0.0) * 100.0,
                    f"{(row.get('fair_decimal') or 0.0):.3f}" if row.get("fair_decimal") else "",
                    _fmt_american(row.get("fair_american")),
                    row.get("edge_pct", 0.0) or 0.0,
                    row.get("ev_per_100", 0.0) or 0.0,
                    play_to_text,
                    str(row.get("rationale", "")).replace("|", "/"),
                )
            )
    lines.append("")

    lines.append("## SGP/SGPx (Correlation Haircut)")
    lines.append("")
    if not sgp_candidates:
        lines.append("- none")
    else:
        sgp_header = (
            "| Game | Legs | Independence p | Haircut | Adjusted p | Decimal | EV/$100 | "
            "1/8 Kelly |"
        )
        lines.append(sgp_header)
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        for row in sgp_candidates[:5]:
            if not isinstance(row, dict):
                continue
            legs = row.get("legs", [])
            leg_texts: list[str] = []
            if isinstance(legs, list):
                for leg in legs:
                    if not isinstance(leg, dict):
                        continue
                    leg_texts.append(
                        _prop_label(
                            str(leg.get("player", "")),
                            str(leg.get("side", "")),
                            _safe_float(leg.get("point")) or 0.0,
                            str(leg.get("market", "")),
                        )
                        + f" @{_fmt_american(leg.get('price'))}"
                    )
            lines.append(
                "| {} | {} | {:.2f}% | {:.1f}% | {:.2f}% | {} | {:+.2f} | {:.2f}% |".format(
                    _short_game_label(str(row.get("game", ""))),
                    " + ".join(leg_texts),
                    (row.get("independence_joint_p", 0.0) or 0.0) * 100.0,
                    (row.get("haircut", 0.0) or 0.0) * 100.0,
                    (row.get("adjusted_joint_p", 0.0) or 0.0) * 100.0,
                    row.get("unboosted_decimal", ""),
                    row.get("ev_per_100", 0.0) or 0.0,
                    (row.get("recommended_fractional_kelly", 0.0) or 0.0) * 100.0,
                )
            )
    lines.append("")

    lines.append("## UNDER SWEEP")
    lines.append("")
    lines.append(f"- status: `{under_sweep.get('status', '')}`")
    lines.append(f"- note: {under_sweep.get('note', '')}")
    qualified = (
        under_sweep.get("qualified", []) if isinstance(under_sweep.get("qualified"), list) else []
    )
    if qualified:
        lines.append("- Qualified unders:")
        for row in qualified[:5]:
            if not isinstance(row, dict):
                continue
            lines.append(
                "  - {} | {} | edge={:+.2f}% | play_to={}".format(
                    _short_game_label(str(row.get("game", ""))),
                    row.get("prop_label", ""),
                    row.get("edge_pct", 0.0) or 0.0,
                    _fmt_american(row.get("play_to_american")),
                )
            )
    misses = (
        under_sweep.get("closest_misses", [])
        if isinstance(under_sweep.get("closest_misses"), list)
        else []
    )
    if misses and under_sweep.get("status") != "ok":
        lines.append("- Closest under misses:")
        for row in misses[:5]:
            if not isinstance(row, dict):
                continue
            lines.append(
                "  - {} | {} | edge={:+.2f}% | play_to={} | reason={}".format(
                    _short_game_label(str(row.get("game", ""))),
                    row.get("prop_label", ""),
                    row.get("edge_pct", 0.0) or 0.0,
                    _fmt_american(row.get("play_to_american")),
                    row.get("reason", ""),
                )
            )
    lines.append("")

    lines.append("## PRICE-DEPENDENT WATCHLIST")
    lines.append("")
    if not watchlist:
        lines.append("- none")
    else:
        lines.append("| Game | Player & Prop | Current | PLAY-TO | Reason |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in watchlist[:top_n]:
            if not isinstance(row, dict):
                continue
            prop = _prop_label(
                str(row.get("player", "")),
                str(row.get("side", "")),
                _safe_float(row.get("point")) or 0.0,
                str(row.get("market", "")),
            )
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _short_game_label(str(row.get("game", ""))),
                    prop,
                    _fmt_american(row.get("current_price")),
                    _fmt_american(row.get("play_to_american")),
                    row.get("reason", ""),
                )
            )
    lines.append("")

    lines.append("## KELLY SIZING SUMMARY")
    lines.append("")
    if not kelly:
        lines.append("- none")
    else:
        lines.append("| Game | Prop | Book/Price | Full Kelly | 1/4 Kelly |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in kelly[: max(top_n, 10)]:
            if not isinstance(row, dict):
                continue
            prop = _prop_label(
                str(row.get("player", "")),
                str(row.get("side", "")),
                _safe_float(row.get("point")) or 0.0,
                str(row.get("market", "")),
            )
            full = (row.get("full_kelly", 0.0) or 0.0) * 100.0
            quarter = (row.get("quarter_kelly", 0.0) or 0.0) * 100.0
            lines.append(
                "| {} | {} | {} {} | {:.2f}% | {:.2f}% |".format(
                    _short_game_label(str(row.get("game", ""))),
                    prop,
                    row.get("book", ""),
                    _fmt_american(row.get("price")),
                    full,
                    quarter,
                )
            )
    lines.append("")

    lines.append("## AUDIT TRAIL")
    lines.append("")
    if not audit_rows:
        lines.append("- none")
    else:
        for row in audit_rows:
            if not isinstance(row, dict):
                continue
            lines.append(
                "- [{}] {} | {} | {} | {}".format(
                    row.get("category", ""),
                    row.get("label", ""),
                    row.get("url", ""),
                    row.get("timestamp_utc", ""),
                    row.get("note", ""),
                )
            )
    lines.append("")

    lines.append("## GAPS")
    lines.append("")
    if gaps:
        for gap in gaps:
            lines.append(f"- {gap}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## SUMMARY")
    lines.append("")
    lines.append(f"- events: `{summary.get('events', 0)}`")
    lines.append(f"- candidate_lines: `{summary.get('candidate_lines', 0)}`")
    lines.append(f"- tier_a_lines: `{summary.get('tier_a_lines', 0)}`")
    lines.append(f"- tier_b_lines: `{summary.get('tier_b_lines', 0)}`")
    lines.append(f"- eligible_lines: `{summary.get('eligible_lines', 0)}`")
    lines.append(f"- ranked_lines: `{summary.get('ranked_lines', 0)}`")
    lines.append(f"- max_picks: `{summary.get('max_picks', 0)}`")
    lines.append(f"- portfolio_excluded_lines: `{summary.get('portfolio_excluded_lines', 0)}`")
    lines.append(f"- actionability_rate: `{summary.get('actionability_rate', '')}`")
    lines.append(f"- avg_quality_score_eligible: `{summary.get('avg_quality_score_eligible', '')}`")
    lines.append(f"- avg_ev_low_eligible: `{summary.get('avg_ev_low_eligible', '')}`")
    lines.append(f"- qualified_unders: `{summary.get('qualified_unders', 0)}`")
    lines.append(f"- under_sweep_status: `{summary.get('under_sweep_status', '')}`")
    lines.append("")
    return "\n".join(lines)


def write_strategy_reports(
    *,
    reports_dir: Path,
    report: dict[str, Any],
    top_n: int,
    strategy_id: str | None = None,
    write_canonical: bool = True,
    write_markdown: bool = False,
) -> tuple[Path, Path]:
    """Write JSON strategy report and optional markdown companions."""
    from prop_ev.strategies.base import normalize_strategy_id

    reports_dir.mkdir(parents=True, exist_ok=True)
    markdown = render_strategy_markdown(report, top_n=top_n)

    canonical_json = reports_dir / "strategy-report.json"
    canonical_md = reports_dir / "strategy-report.md"
    canonical_card = reports_dir / "strategy-card.md"

    normalized = normalize_strategy_id(strategy_id) if strategy_id else ""

    def _suffix(path: Path) -> Path:
        return path.with_name(f"{path.stem}.{normalized}{path.suffix}")

    primary_json = canonical_json
    primary_md = canonical_md
    if not write_canonical:
        if not normalized:
            raise ValueError("strategy_id is required when write_canonical=false")
        primary_json = _suffix(canonical_json)
        primary_md = _suffix(canonical_md)

    def _write(json_path: Path, md_path: Path, card_path: Path) -> None:
        json_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        if write_markdown:
            md_path.write_text(markdown, encoding="utf-8")
            card_path.write_text(markdown, encoding="utf-8")
        else:
            for path in (md_path, card_path):
                if path.exists():
                    path.unlink()

    if write_canonical:
        _write(canonical_json, canonical_md, canonical_card)
    if normalized and not write_canonical:
        _write(_suffix(canonical_json), _suffix(canonical_md), _suffix(canonical_card))

    return primary_json, primary_md


def write_execution_plan(
    *,
    reports_dir: Path,
    report: dict[str, Any],
    strategy_id: str | None = None,
    write_canonical: bool = True,
) -> Path:
    """Write execution-plan JSON artifact for one strategy report."""
    from prop_ev.strategies.base import normalize_strategy_id

    payload = report.get("execution_plan")
    if not isinstance(payload, dict):
        raise ValueError("strategy report missing execution_plan object")
    assert_execution_plan(payload)

    reports_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = reports_dir / "execution-plan.json"
    normalized = normalize_strategy_id(strategy_id) if strategy_id else ""

    if write_canonical:
        output_path = canonical_path
    else:
        if not normalized:
            raise ValueError("strategy_id is required when write_canonical=false")
        output_path = reports_dir / f"execution-plan.{normalized}.json"

    write_payload = dict(payload)
    if normalized:
        write_payload["strategy_id"] = normalized
    output_path.write_text(
        json.dumps(write_payload, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def _sanitize_artifact_tag(tag: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", tag.strip())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("._-")
    return cleaned


def write_tagged_strategy_reports(
    *,
    reports_dir: Path,
    report: dict[str, Any],
    top_n: int,
    tag: str,
    write_markdown: bool = False,
) -> tuple[Path, Path]:
    """Write one tagged strategy report bundle without touching canonical files."""
    safe_tag = _sanitize_artifact_tag(tag)
    if not safe_tag:
        raise ValueError("tag must contain at least one filename-safe character")

    reports_dir.mkdir(parents=True, exist_ok=True)
    markdown = render_strategy_markdown(report, top_n=top_n)

    json_path = reports_dir / f"strategy-report.{safe_tag}.json"
    md_path = reports_dir / f"strategy-report.{safe_tag}.md"
    card_path = reports_dir / f"strategy-card.{safe_tag}.md"

    json_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    if write_markdown:
        md_path.write_text(markdown, encoding="utf-8")
        card_path.write_text(markdown, encoding="utf-8")
    else:
        for path in (md_path, card_path):
            if path.exists():
                path.unlink()
    return json_path, md_path
