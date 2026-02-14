from __future__ import annotations

from pathlib import Path
from typing import Any

from prop_ev.scoreboard_pdf import (
    render_aggregate_scoreboard_latex,
    render_aggregate_scoreboard_pdf,
)


def _sample_payload() -> dict[str, Any]:
    return {
        "summary": {
            "snapshot_id": "day-abc-2026-02-12",
            "dataset_id": "basketball_nba_player_points",
            "complete_days": 3,
            "days_with_any_results": 3,
            "baseline_strategy_id": "s007",
            "min_graded": 20,
            "bin_size": 0.1,
            "power_picks_per_day": 5,
            "power_target_uplift_gate": 0.02,
        },
        "strategies": [
            {
                "strategy_id": "s010_alpha",
                "rows_graded": 42,
                "rows_scored": 38,
                "roi": 0.05712,
                "total_pnl_units": 2.67,
                "wins": 23,
                "losses": 19,
                "pushes": 0,
                "brier": 0.227,
                "log_loss": 0.621,
                "ece": 0.018,
                "avg_quality_score": 0.711,
                "avg_ev_low": 0.026,
                "actionability_rate": 0.475,
                "promotion_gate": {"status": "pass"},
            }
        ],
        "winner": {"strategy_id": "s010_alpha", "roi": 0.05712, "rows_graded": 42},
    }


def test_render_aggregate_scoreboard_latex_contains_expected_sections() -> None:
    latex = render_aggregate_scoreboard_latex(_sample_payload())
    assert "Aggregate Scoreboard" in latex
    assert r"\begin{longtable}" in latex
    assert r"s010\_alpha" in latex
    assert r"basketball\_nba\_player\_points" in latex


def test_render_aggregate_scoreboard_pdf_keeps_tex_when_tool_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def _missing(*, tex_path: Path, pdf_path: Path) -> dict[str, Any]:
        return {"status": "missing_tool", "message": "tectonic missing", "pdf_path": str(pdf_path)}

    monkeypatch.setattr("prop_ev.scoreboard_pdf.compile_pdf", _missing)
    tex_path = tmp_path / "aggregate-scoreboard.tex"
    pdf_path = tmp_path / "aggregate-scoreboard.pdf"
    result = render_aggregate_scoreboard_pdf(
        analysis_payload=_sample_payload(),
        tex_path=tex_path,
        pdf_path=pdf_path,
        keep_tex=False,
    )
    assert result["status"] == "missing_tool"
    assert result["tex_retained"] is True
    assert tex_path.exists()
    assert not pdf_path.exists()


def test_render_aggregate_scoreboard_pdf_removes_tex_on_success(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def _ok(*, tex_path: Path, pdf_path: Path) -> dict[str, Any]:
        pdf_path.write_bytes(b"%PDF-1.4\n")
        return {"status": "ok", "message": "pdf generated", "pdf_path": str(pdf_path)}

    monkeypatch.setattr("prop_ev.scoreboard_pdf.compile_pdf", _ok)
    tex_path = tmp_path / "aggregate-scoreboard.tex"
    pdf_path = tmp_path / "aggregate-scoreboard.pdf"
    result = render_aggregate_scoreboard_pdf(
        analysis_payload=_sample_payload(),
        tex_path=tex_path,
        pdf_path=pdf_path,
        keep_tex=False,
    )
    assert result["status"] == "ok"
    assert result["tex_retained"] is False
    assert not tex_path.exists()
    assert pdf_path.exists()
