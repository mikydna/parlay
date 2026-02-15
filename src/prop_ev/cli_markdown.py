"""Markdown render helpers for CLI summary artifacts."""

from __future__ import annotations

from typing import Any


def render_strategy_compare_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    rows = report.get("strategies", []) if isinstance(report.get("strategies"), list) else []
    overlap = (
        report.get("ranked_overlap", {}) if isinstance(report.get("ranked_overlap"), dict) else {}
    )

    lines: list[str] = []
    lines.append("# Strategy Compare")
    lines.append("")
    lines.append(f"- snapshot_id: `{summary.get('snapshot_id', '')}`")
    lines.append(f"- strategies: `{summary.get('strategy_count', 0)}`")
    lines.append(f"- ranked_top_n: `{summary.get('top_n', 0)}`")
    lines.append(f"- max_picks: `{summary.get('max_picks', 0)}`")
    lines.append("")

    if rows:
        lines.append("| Strategy | Mode | Eligible | Candidate | TierA | TierB | Gates |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for row in rows:
            if not isinstance(row, dict):
                continue
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} |".format(
                    row.get("strategy_id", ""),
                    row.get("strategy_mode", ""),
                    row.get("eligible_lines", 0),
                    row.get("candidate_lines", 0),
                    row.get("tier_a_lines", 0),
                    row.get("tier_b_lines", 0),
                    row.get("health_gate_count", 0),
                )
            )
        lines.append("")

    lines.append("## Ranked Overlap")
    lines.append("")
    lines.append(f"- intersection_all: `{overlap.get('intersection_all', 0)}`")
    lines.append(f"- union_all: `{overlap.get('union_all', 0)}`")
    lines.append("")
    return "\n".join(lines)


def render_backtest_summary_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    rows = report.get("strategies", []) if isinstance(report.get("strategies"), list) else []
    winner = report.get("winner", {}) if isinstance(report.get("winner"), dict) else {}
    promotion_winner = (
        report.get("promotion_winner", {})
        if isinstance(report.get("promotion_winner"), dict)
        else {}
    )

    lines: list[str] = []
    lines.append("# Backtest Summary")
    lines.append("")
    lines.append(f"- snapshot_id: `{summary.get('snapshot_id', '')}`")
    lines.append(f"- strategies: `{summary.get('strategy_count', 0)}`")
    lines.append(f"- min_graded: `{summary.get('min_graded', 0)}`")
    lines.append(f"- bin_size: `{summary.get('bin_size', '')}`")
    lines.append(f"- baseline_strategy_id: `{summary.get('baseline_strategy_id', '')}`")
    lines.append(f"- baseline_found: `{summary.get('baseline_found', False)}`")
    lines.append(f"- require_scored_fraction: `{summary.get('require_scored_fraction', '')}`")
    lines.append(f"- power_target_uplift_gate: `{summary.get('power_target_uplift_gate', '')}`")
    lines.append(f"- require_power_gate: `{summary.get('require_power_gate', False)}`")
    if bool(summary.get("all_complete_days", False)):
        lines.append(f"- dataset_id: `{summary.get('dataset_id', '')}`")
        lines.append(f"- complete_days: `{summary.get('complete_days', 0)}`")
        lines.append(f"- days_with_any_results: `{summary.get('days_with_any_results', 0)}`")
    lines.append("")

    if rows:
        lines.append(
            "| Strategy | Graded | Scored | ROI | W | L | P | Brier | BrierLow | "
            "LogLoss | ECE | MCE | AvgQ | AvgEVLow | Actionability | Gate | PowerGate |"
        )
        lines.append(
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
            " --- | --- | --- | --- | --- |"
        )
        for row in rows:
            if not isinstance(row, dict):
                continue
            gate = row.get("promotion_gate", {})
            gate_status = gate.get("status", "") if isinstance(gate, dict) else ""
            power_gate = row.get("power_gate", {})
            power_gate_status = power_gate.get("status", "") if isinstance(power_gate, dict) else ""
            lines.append(
                (
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |"
                    " {} | {} |"
                ).format(
                    row.get("strategy_id", ""),
                    row.get("rows_graded", 0),
                    row.get("rows_scored", 0),
                    row.get("roi", ""),
                    row.get("wins", 0),
                    row.get("losses", 0),
                    row.get("pushes", 0),
                    row.get("brier", ""),
                    row.get("brier_low", ""),
                    row.get("log_loss", ""),
                    row.get("ece", ""),
                    row.get("mce", ""),
                    row.get("avg_quality_score", ""),
                    row.get("avg_ev_low", ""),
                    row.get("actionability_rate", ""),
                    gate_status,
                    power_gate_status,
                )
            )
        lines.append("")

    power_guidance = (
        report.get("power_guidance", {}) if isinstance(report.get("power_guidance"), dict) else {}
    )
    if power_guidance:
        assumptions = (
            power_guidance.get("assumptions", {})
            if isinstance(power_guidance.get("assumptions"), dict)
            else {}
        )
        lines.append("## Power Guidance")
        lines.append("")
        lines.append(f"- baseline_strategy_id: `{power_guidance.get('baseline_strategy_id', '')}`")
        lines.append(f"- alpha: `{assumptions.get('alpha', '')}`")
        lines.append(f"- power: `{assumptions.get('power', '')}`")
        lines.append(f"- picks_per_day: `{assumptions.get('picks_per_day', '')}`")
        lines.append("")
        lines.append(
            "| Strategy | Overlap Days | Mean Daily ΔPnL | "
            "Std Daily ΔPnL | Target | Required Days |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for row in power_guidance.get("strategies", []):
            if not isinstance(row, dict):
                continue
            required_rows = (
                row.get("required_days_by_target", [])
                if isinstance(row.get("required_days_by_target"), list)
                else []
            )
            if not required_rows:
                lines.append(
                    "| {} | {} | {} | {} | n/a | n/a |".format(
                        row.get("strategy_id", ""),
                        row.get("overlap_days", 0),
                        row.get("mean_daily_pnl_diff", ""),
                        row.get("std_daily_pnl_diff", ""),
                    )
                )
                continue
            first = True
            for target_row in required_rows:
                if not isinstance(target_row, dict):
                    continue
                lines.append(
                    "| {} | {} | {} | {} | {} | {} |".format(
                        row.get("strategy_id", "") if first else "",
                        row.get("overlap_days", 0) if first else "",
                        row.get("mean_daily_pnl_diff", "") if first else "",
                        row.get("std_daily_pnl_diff", "") if first else "",
                        target_row.get("target_roi_uplift_per_bet", ""),
                        target_row.get("required_days", ""),
                    )
                )
                first = False
        lines.append("")

    if winner:
        lines.append("## Winner")
        lines.append("")
        lines.append(f"- strategy_id: `{winner.get('strategy_id', '')}`")
        lines.append(f"- roi: `{winner.get('roi', '')}`")
        lines.append(f"- rows_graded: `{winner.get('rows_graded', 0)}`")
        lines.append(f"- promotion_gate_status: `{winner.get('promotion_gate_status', '')}`")
        lines.append("")

    if promotion_winner:
        lines.append("## Promotion Winner")
        lines.append("")
        lines.append(f"- strategy_id: `{promotion_winner.get('strategy_id', '')}`")
        lines.append(f"- roi: `{promotion_winner.get('roi', '')}`")
        lines.append(f"- rows_graded: `{promotion_winner.get('rows_graded', 0)}`")
        lines.append("")

    return "\n".join(lines)
