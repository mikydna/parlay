"""Direct LaTeX renderer for aggregate scoreboard artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from prop_ev.latex_renderer import cleanup_latex_artifacts, compile_pdf, escape_latex


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _format_number(value: object, *, digits: int) -> str:
    numeric = _as_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}f}"


def _sorted_strategy_rows(rows: Sequence[object]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = [row for row in rows if isinstance(row, dict)]
    normalized.sort(
        key=lambda row: (
            -(_as_float(row.get("total_pnl_units")) or float("-inf")),
            str(row.get("strategy_id", "")),
        )
    )
    return normalized


def _render_metadata(summary: Mapping[str, Any]) -> list[str]:
    fields = [
        ("snapshot_id", summary.get("snapshot_id", "")),
        ("dataset_id", summary.get("dataset_id", "")),
        ("complete_days", summary.get("complete_days", "")),
        ("days_with_any_results", summary.get("days_with_any_results", "")),
        ("baseline_strategy_id", summary.get("baseline_strategy_id", "")),
        ("min_graded", summary.get("min_graded", "")),
        ("bin_size", _format_number(summary.get("bin_size"), digits=3)),
        (
            "require_scored_fraction",
            _format_number(summary.get("require_scored_fraction"), digits=3),
        ),
        ("power_picks_per_day", summary.get("power_picks_per_day", "")),
        (
            "power_target_uplift_gate",
            _format_number(summary.get("power_target_uplift_gate"), digits=3),
        ),
    ]
    lines: list[str] = [r"\section*{Metadata}", r"\begin{itemize}"]
    for label, value in fields:
        raw = str(value).strip()
        if not raw:
            continue
        lines.append(rf"\item \textbf{{{escape_latex(label)}}}: \texttt{{{escape_latex(raw)}}}")
    lines.append(r"\end{itemize}")
    return lines


def _strategy_row_cells(row: Mapping[str, Any]) -> list[str]:
    gate = row.get("promotion_gate", {}) if isinstance(row.get("promotion_gate"), dict) else {}
    gate_status = str(gate.get("status", "")).strip()
    return [
        str(row.get("strategy_id", "")).strip(),
        str(int(row.get("rows_graded", 0) or 0)),
        str(int(row.get("rows_scored", 0) or 0)),
        _format_number(row.get("roi"), digits=3),
        _format_number(row.get("total_pnl_units"), digits=2),
        str(int(row.get("wins", 0) or 0)),
        str(int(row.get("losses", 0) or 0)),
        str(int(row.get("pushes", 0) or 0)),
        _format_number(row.get("brier"), digits=3),
        _format_number(row.get("log_loss"), digits=3),
        _format_number(row.get("ece"), digits=3),
        _format_number(row.get("avg_quality_score"), digits=3),
        _format_number(row.get("avg_ev_low"), digits=3),
        _format_number(row.get("actionability_rate"), digits=3),
        gate_status,
    ]


def _render_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    header_cells = [rf"\textbf{{{escape_latex(cell)}}}" for cell in headers]
    header_row = " & ".join(header_cells) + r" \\"
    lines: list[str] = [
        r"{\scriptsize",
        r"\setlength{\tabcolsep}{3pt}",
        r"\setlength{\LTleft}{0pt}",
        r"\setlength{\LTright}{0pt}",
        r"\begin{longtable}{lrrrrrrrrrrrrrl}",
        header_row,
        r"\hline",
        r"\endfirsthead",
        header_row,
        r"\hline",
        r"\endhead",
    ]
    for row in rows:
        lines.append(" & ".join(escape_latex(cell) for cell in row) + r" \\")
    lines.extend([r"\end{longtable}", r"}"])
    return lines


def _render_power_guidance(power_guidance: Mapping[str, Any]) -> list[str]:
    strategy_rows = (
        power_guidance.get("strategies", [])
        if isinstance(power_guidance.get("strategies"), list)
        else []
    )
    if not strategy_rows:
        return []
    rows: list[list[str]] = []
    for row in strategy_rows:
        if not isinstance(row, dict):
            continue
        required_rows = (
            row.get("required_days_by_target", [])
            if isinstance(row.get("required_days_by_target"), list)
            else []
        )
        if not required_rows:
            rows.append(
                [
                    str(row.get("strategy_id", "")),
                    str(int(row.get("overlap_days", 0) or 0)),
                    _format_number(row.get("mean_daily_pnl_diff"), digits=3),
                    _format_number(row.get("std_daily_pnl_diff"), digits=3),
                    "",
                    "",
                ]
            )
            continue
        first = True
        for required in required_rows:
            if not isinstance(required, dict):
                continue
            rows.append(
                [
                    str(row.get("strategy_id", "")) if first else "",
                    str(int(row.get("overlap_days", 0) or 0)) if first else "",
                    _format_number(row.get("mean_daily_pnl_diff"), digits=3) if first else "",
                    _format_number(row.get("std_daily_pnl_diff"), digits=3) if first else "",
                    _format_number(required.get("target_roi_uplift_per_bet"), digits=3),
                    str(int(required.get("required_days", 0) or 0)),
                ]
            )
            first = False

    if not rows:
        return []
    lines: list[str] = [r"\section*{Power Guidance}"]
    lines.extend(
        _render_table(
            [
                "Strategy",
                "OverlapDays",
                "MeanDailyDeltaPnL",
                "StdDailyDeltaPnL",
                "TargetRoiUplift",
                "RequiredDays",
            ],
            rows,
        )
    )
    return lines


def render_aggregate_scoreboard_latex(analysis_payload: Mapping[str, Any]) -> str:
    """Render aggregate-scoreboard payload to deterministic LaTeX."""
    summary = analysis_payload.get("summary", {})
    if not isinstance(summary, Mapping):
        summary = {}
    strategies = analysis_payload.get("strategies", [])
    if not isinstance(strategies, Sequence):
        strategies = []
    winner = analysis_payload.get("winner", {})
    if not isinstance(winner, Mapping):
        winner = {}
    power_guidance = analysis_payload.get("power_guidance", {})
    if not isinstance(power_guidance, Mapping):
        power_guidance = {}

    strategy_rows = [_strategy_row_cells(row) for row in _sorted_strategy_rows(strategies)]

    lines: list[str] = [
        r"\documentclass[10pt]{article}",
        r"\usepackage[margin=0.6in,landscape]{geometry}",
        r"\usepackage[T1]{fontenc}",
        r"\usepackage[utf8]{inputenc}",
        r"\usepackage{lmodern}",
        r"\usepackage{longtable}",
        r"\setlength{\parindent}{0pt}",
        r"\setlength{\parskip}{4pt}",
        r"\begin{document}",
        r"\begin{center}\LARGE\textbf{Aggregate Scoreboard}\end{center}",
    ]
    lines.extend(_render_metadata(summary))
    lines.append(r"\section*{Strategies}")
    lines.extend(
        _render_table(
            [
                "Strategy",
                "Graded",
                "Scored",
                "ROI",
                "TotalPnL",
                "W",
                "L",
                "P",
                "Brier",
                "LogLoss",
                "ECE",
                "AvgQ",
                "AvgEVLow",
                "Actionability",
                "Gate",
            ],
            strategy_rows,
        )
    )
    if winner:
        winner_strategy = escape_latex(str(winner.get("strategy_id", "")).strip())
        winner_roi = escape_latex(_format_number(winner.get("roi"), digits=3))
        winner_graded = escape_latex(str(int(winner.get("rows_graded", 0) or 0)))
        lines.append(r"\section*{Winner}")
        lines.append(rf"\textbf{{strategy\_id}}: \texttt{{{winner_strategy}}}")
        lines.append(rf"\textbf{{roi}}: \texttt{{{winner_roi}}}")
        lines.append(rf"\textbf{{graded}}: \texttt{{{winner_graded}}}")

    lines.extend(_render_power_guidance(power_guidance))
    lines.append(r"\end{document}")
    return "\n".join(lines) + "\n"


def render_aggregate_scoreboard_pdf(
    *,
    analysis_payload: Mapping[str, Any],
    tex_path: Path,
    pdf_path: Path,
    keep_tex: bool,
) -> dict[str, Any]:
    """Write scoreboard TeX and compile PDF with best-effort cleanup."""
    tex_path.parent.mkdir(parents=True, exist_ok=True)
    tex_path.write_text(render_aggregate_scoreboard_latex(analysis_payload), encoding="utf-8")

    compile_result = compile_pdf(tex_path=tex_path, pdf_path=pdf_path)
    status = str(compile_result.get("status", "")).strip() or "failed"
    retain_tex = bool(keep_tex) or status != "ok"
    cleanup_latex_artifacts(tex_path=tex_path, keep_tex=retain_tex)

    result = dict(compile_result)
    result["tex_path"] = str(tex_path) if retain_tex else ""
    result["tex_retained"] = retain_tex
    result["pdf_path"] = str(pdf_path)
    return result
