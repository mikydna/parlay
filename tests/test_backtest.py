import json
from pathlib import Path

from prop_ev.backtest import build_backtest_seed_rows, write_backtest_artifacts


def _sample_report() -> dict:
    return {
        "snapshot_id": "snap-1",
        "generated_at_utc": "2026-02-11T20:00:00Z",
        "modeled_date_et": "Wednesday, Feb 11, 2026 (ET)",
        "strategy_mode": "full_board",
        "strategy_status": "modeled_with_gates",
        "summary": {
            "events": 1,
            "candidate_lines": 2,
            "eligible_lines": 1,
        },
        "health_report": {"health_gates": []},
        "candidates": [
            {
                "event_id": "event-1",
                "game": "A @ B",
                "tip_et": "07:00 PM ET",
                "home_team": "B",
                "away_team": "A",
                "player": "Player One",
                "market": "player_points",
                "recommended_side": "over",
                "point": 20.5,
                "tier": "A",
                "selected_book": "fanduel",
                "selected_price": -110,
                "play_to_american": -115,
                "play_to_decimal": 1.87,
                "model_p_hit": 0.56,
                "fair_p_hit": 0.51,
                "best_ev": 0.04,
                "edge_pct": 4.0,
                "ev_per_100": 4.0,
                "full_kelly": 0.02,
                "quarter_kelly": 0.005,
                "injury_status": "unknown",
                "roster_status": "active",
                "selected_last_update": "2026-02-11T19:58:00Z",
                "selected_link": "https://example.com",
                "eligible": True,
                "reason": "",
            },
            {
                "event_id": "event-1",
                "game": "A @ B",
                "tip_et": "07:00 PM ET",
                "home_team": "B",
                "away_team": "A",
                "player": "Player Two",
                "market": "player_rebounds",
                "recommended_side": "under",
                "point": 7.5,
                "selected_book": "draftkings",
                "selected_price": -108,
                "eligible": False,
                "reason": "roster_gate",
            },
        ],
        "ranked_plays": [
            {
                "event_id": "event-1",
                "game": "A @ B",
                "tip_et": "07:00 PM ET",
                "home_team": "B",
                "away_team": "A",
                "player": "Player One",
                "market": "player_points",
                "recommended_side": "over",
                "point": 20.5,
                "selected_book": "fanduel",
                "selected_price": -110,
            }
        ],
        "top_ev_plays": [],
        "one_source_edges": [],
    }


def test_build_backtest_seed_rows_filters_by_selection() -> None:
    report = _sample_report()
    eligible_rows = build_backtest_seed_rows(report=report, selection="eligible", top_n=0)
    assert len(eligible_rows) == 1
    assert eligible_rows[0]["player"] == "Player One"
    assert eligible_rows[0]["ticket_key"]
    assert eligible_rows[0]["result"] == ""
    assert eligible_rows[0]["actual_stat_value"] is None

    ranked_rows = build_backtest_seed_rows(report=report, selection="ranked", top_n=0)
    assert len(ranked_rows) == 1
    assert ranked_rows[0]["selection_mode"] == "ranked"


def test_ticket_key_stable_across_price_and_book() -> None:
    report_a = _sample_report()
    report_b = _sample_report()
    report_b["candidates"][0]["selected_book"] = "draftkings"
    report_b["candidates"][0]["selected_price"] = -125

    rows_a = build_backtest_seed_rows(report=report_a, selection="eligible", top_n=0)
    rows_b = build_backtest_seed_rows(report=report_b, selection="eligible", top_n=0)

    assert len(rows_a) == 1
    assert len(rows_b) == 1
    assert rows_a[0]["ticket_key"] == rows_b[0]["ticket_key"]


def test_write_backtest_artifacts(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "data" / "odds_api" / "snapshots" / "snap-1"
    (snapshot_dir / "reports").mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "derived").mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "context").mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "derived" / "event_props.jsonl").write_text(
        json.dumps({"event_id": "event-1"}) + "\n",
        encoding="utf-8",
    )
    (snapshot_dir / "derived" / "featured_odds.jsonl").write_text(
        json.dumps({"event_id": "event-1"}) + "\n",
        encoding="utf-8",
    )
    (snapshot_dir / "context" / "injuries.json").write_text(
        json.dumps({"status": "ok", "official": {"status": "ok"}}) + "\n",
        encoding="utf-8",
    )
    (snapshot_dir / "context" / "roster.json").write_text(
        json.dumps({"status": "ok"}) + "\n",
        encoding="utf-8",
    )
    (snapshot_dir / "reports" / "strategy-report.json").write_text(
        json.dumps(_sample_report(), sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    result = write_backtest_artifacts(
        snapshot_dir=snapshot_dir,
        report=_sample_report(),
        selection="eligible",
        top_n=0,
    )
    seed_path = Path(result["seed_jsonl"])
    template_path = Path(result["results_template_csv"])
    readiness_path = Path(result["readiness_json"])
    assert seed_path.exists()
    assert template_path.exists()
    assert readiness_path.exists()
    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    assert readiness["ready_for_backtest_seed"] is True
    assert readiness["counts"]["seed_rows"] == 1
    assert readiness["counts"]["event_props_rows"] == 1
