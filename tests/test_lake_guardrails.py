from __future__ import annotations

from pathlib import Path

from prop_ev.lake_guardrails import build_guardrail_report


def test_guardrails_report_detects_p0_violations(tmp_path: Path) -> None:
    odds_root = tmp_path / "data" / "odds_api"
    (odds_root / "snapshots" / "snap-1" / "reports").mkdir(parents=True, exist_ok=True)
    (odds_root / "snapshots" / "snap-1" / "context").mkdir(parents=True, exist_ok=True)
    (odds_root / "snapshots" / "snap-1" / "context" / "injuries.json").write_text(
        "{}\n",
        encoding="utf-8",
    )
    (odds_root / "reference").mkdir(parents=True, exist_ok=True)
    (odds_root / "reference" / "player_identity_map.json").write_text("{}\n", encoding="utf-8")
    (odds_root / "llm_cache").mkdir(parents=True, exist_ok=True)

    report = build_guardrail_report(odds_root)
    assert report["status"] == "violations"
    assert int(report["violation_count"]) >= 4
    codes = {
        str(item["code"])
        for item in report["violations"]
        if isinstance(item, dict) and "code" in item
    }
    assert "embedded_snapshot_reports" in codes
    assert "legacy_nba_context" in codes
    assert "legacy_nba_reference" in codes
    assert "runtime_artifact_in_lake" in codes


def test_guardrails_report_clean_root_is_ok(tmp_path: Path) -> None:
    odds_root = tmp_path / "data" / "odds_api"
    (odds_root / "snapshots" / "snap-1" / "derived").mkdir(parents=True, exist_ok=True)

    report = build_guardrail_report(odds_root)
    assert report["status"] == "ok"
    assert report["violation_count"] == 0
