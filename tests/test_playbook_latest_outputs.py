import json
from pathlib import Path

import pytest

import prop_ev.playbook as playbook
from prop_ev.playbook import generate_brief_for_snapshot
from prop_ev.settings import Settings
from prop_ev.storage import SnapshotStore


def _sample_strategy_report() -> dict:
    return {
        "snapshot_id": "snap-1",
        "generated_at_utc": "2026-02-11T17:00:00Z",
        "strategy_status": "modeled_with_gates",
        "summary": {
            "events": 1,
            "candidate_lines": 2,
            "tier_a_lines": 1,
            "tier_b_lines": 1,
            "eligible_lines": 1,
            "injury_source_official": "yes",
            "injury_source_secondary": "yes",
            "roster_source": "no",
            "roster_team_rows": 0,
            "quota": {"remaining": "490", "used": "10", "last": "1"},
        },
        "gaps": ["Roster feed returned no active rows."],
        "ranked_plays": [
            {
                "event_id": "event-1",
                "home_team": "A",
                "away_team": "B",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "tier": "A",
                "recommended_side": "over",
                "over_best_price": 105,
                "over_best_book": "book_a",
                "over_link": "",
                "under_best_price": -120,
                "under_best_book": "book_b",
                "under_link": "",
                "best_ev": 0.04,
                "best_kelly": 0.03,
                "p_over_model": 0.52,
                "p_over_fair": 0.49,
                "hold": 0.02,
                "injury_status": "unknown",
                "roster_status": "active",
                "reason": "",
            }
        ],
        "watchlist": [],
        "audit": {"report_schema_version": 2},
    }


def test_generate_brief_writes_snapshot_and_latest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(tmp_path / "data" / "odds_api"))

    store = SnapshotStore(tmp_path / "data" / "odds_api")
    snapshot_id = "2026-02-11T17-00-00Z"
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "strategy-report.json").write_text(
        json.dumps(_sample_strategy_report(), sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    settings = Settings(_env_file=None)
    result = generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=snapshot_id,
        top_n=5,
        llm_refresh=False,
        llm_offline=True,
    )

    assert (reports_dir / "brief-input.json").exists()
    assert (reports_dir / "brief-pass1.json").exists()
    assert (reports_dir / "brief-analyst.json").exists()
    assert (reports_dir / "strategy-brief.md").exists()
    assert (reports_dir / "strategy-brief.tex").exists()
    assert (reports_dir / "strategy-brief.meta.json").exists()

    latest_dir = store.root / "reports" / "latest"
    assert (latest_dir / "strategy-brief.md").exists()
    assert (latest_dir / "strategy-brief.tex").exists()
    markdown = (reports_dir / "strategy-brief.md").read_text(encoding="utf-8")
    assert "## Analyst Take" in markdown
    assert markdown.index("## Analyst Take") < markdown.index("## Action Plan (GO / LEAN / NO-GO)")
    assert "## Best Available Bet Right Now" in markdown
    assert markdown.index("## Best Available Bet Right Now") < markdown.index(
        "## Action Plan (GO / LEAN / NO-GO)"
    )
    assert "<!-- pagebreak -->\n\n## Action Plan (GO / LEAN / NO-GO)" in markdown
    assert markdown.rfind("## Data Quality") > markdown.rfind("## Game Cards by Matchup")
    assert markdown.rfind("## Confidence") > markdown.rfind("## Data Quality")
    assert "<!-- pagebreak -->" in markdown
    latest_payload = json.loads((latest_dir / "latest.json").read_text(encoding="utf-8"))
    assert latest_payload["snapshot_id"] == snapshot_id
    assert result["snapshot_id"] == snapshot_id


def test_generate_brief_pass1_retries_then_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test")
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(tmp_path / "data" / "odds_api"))

    store = SnapshotStore(tmp_path / "data" / "odds_api")
    snapshot_id = "2026-02-11T18-00-00Z"
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "strategy-report.json").write_text(
        json.dumps(_sample_strategy_report(), sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    class FakeLLMClient:
        def __init__(self, **kwargs) -> None:
            del kwargs
            self.pass1_calls = 0

        def cached_completion(self, **kwargs):
            task = kwargs["task"]
            if task == "playbook_pass1":
                self.pass1_calls += 1
                if self.pass1_calls == 1:
                    return {"text": "", "cached": False, "cache_key": "a1", "usage": {}}
                return {
                    "text": (
                        '{"slate_summary":"ok","top_plays_explained":[],"watchouts":[],'
                        '"data_quality_flags":[],"confidence_notes":[]}'
                    ),
                    "cached": False,
                    "cache_key": "a2",
                    "usage": {},
                }
            return {
                "text": (
                    "## Snapshot\n\n## What The Bet Is\n\n## Executive Summary\n\n"
                    "## Analyst Take\n\n"
                    "## Action Plan (GO / LEAN / NO-GO)\n\n## Risks and Watchouts\n\n"
                    "## Tier B View (Single-Book Lines)\n\n## Data Quality\n\n## Confidence\n"
                ),
                "cached": False,
                "cache_key": "p2",
                "usage": {},
            }

    monkeypatch.setattr(playbook, "LLMClient", FakeLLMClient)

    settings = Settings(_env_file=None)
    result = generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=snapshot_id,
        top_n=5,
        llm_refresh=True,
        llm_offline=False,
    )
    meta = json.loads(Path(result["report_meta"]).read_text(encoding="utf-8"))
    markdown = Path(result["report_markdown"]).read_text(encoding="utf-8")
    assert "## Risks and Watchouts" not in markdown
    assert "## Tier B View (Single-Book Lines)" not in markdown
    assert "## Analyst Take" in markdown
    assert meta["llm"]["pass1"]["status"] == "ok"
    assert meta["llm"]["pass1"]["attempts"] == 2
