from __future__ import annotations

from prop_ev.execution_projection import ExecutionProjectionConfig, project_execution_report


def _base_report(*, pre_bet_ready: bool = True) -> dict:
    candidate = {
        "event_id": "event-1",
        "game": "Away @ Home",
        "player": "Player A",
        "market": "player_points",
        "point": 20.5,
        "tier": "B",
        "book_count": 1,
        "over_shop_delta": 0,
        "under_shop_delta": 0,
        "hold": 0.02,
        "recommended_side": "over",
        "selected_price": -110,
        "selected_book": "old_book",
        "selected_link": "",
        "selected_last_update": "2026-02-13T00:00:00Z",
        "over_best_price": -110,
        "over_best_book": "old_book",
        "over_link": "",
        "over_last_update": "2026-02-13T00:00:00Z",
        "under_best_price": -120,
        "under_best_book": "old_book",
        "under_link": "",
        "under_last_update": "2026-02-13T00:00:00Z",
        "model_p_hit": 0.60,
        "play_to_american": -105,
        "pre_bet_ready": pre_bet_ready,
        "best_ev": 0.01,
        "best_kelly": 0.01,
        "full_kelly": 0.01,
        "quarter_kelly": 0.0025,
        "edge_pct": 1.0,
        "ev_per_100": 1.0,
        "book_price": "old_book -110",
        "eligible": True,
        "reason": "",
        "score": 0.0,
    }
    return {
        "snapshot_id": "snap-1",
        "strategy_mode": "full_board",
        "summary": {
            "events": 1,
            "candidate_lines": 1,
            "tier_a_lines": 0,
            "tier_b_lines": 1,
            "eligible_lines": 1,
            "strategy_mode": "full_board",
            "watchlist_only": "no",
        },
        "health_report": {"strategy_mode": "full_board", "health_gates": []},
        "candidates": [candidate],
        "ranked_plays": [candidate],
        "top_ev_plays": [],
        "one_source_edges": [candidate],
        "watchlist": [],
        "under_sweep": {},
        "price_dependent_watchlist": [],
        "kelly_summary": [],
        "audit": {},
    }


def test_project_execution_report_reprices_and_recomputes_ev() -> None:
    report = _base_report()
    event_props = [
        {
            "event_id": "event-1",
            "player": "Player A",
            "market": "player_points",
            "point": 20.5,
            "side": "over",
            "book": "draftkings",
            "price": 120,
            "link": "https://example.com/dk",
            "last_update": "2026-02-13T01:23:45Z",
        }
    ]
    config = ExecutionProjectionConfig(
        bookmakers=("draftkings",),
        top_n=10,
        requires_pre_bet_ready=False,
        requires_meets_play_to=False,
        tier_a_min_ev=0.03,
        tier_b_min_ev=0.05,
    )

    projected = project_execution_report(report, event_props, config)
    candidate = projected["candidates"][0]

    assert candidate["selected_book"] == "draftkings"
    assert candidate["selected_price"] == 120
    assert candidate["selected_link"] == "https://example.com/dk"
    assert candidate["selected_last_update"] == "2026-02-13T01:23:45Z"
    assert candidate["over_best_book"] == "draftkings"
    assert candidate["over_best_price"] == 120
    assert candidate["best_ev"] == 0.32
    assert candidate["best_kelly"] == 0.266667
    assert candidate["full_kelly"] == 0.266667
    assert candidate["quarter_kelly"] == 0.066667
    assert candidate["edge_pct"] == 32.0
    assert candidate["ev_per_100"] == 32.0
    assert candidate["book_price"] == "draftkings +120"
    assert candidate["eligible"] is True
    assert candidate["reason"] == ""
    assert projected["summary"]["eligible_lines"] == 1
    assert projected["strategy_mode"] == "full_board"


def test_project_execution_report_applies_pre_bet_gate() -> None:
    report = _base_report(pre_bet_ready=False)
    event_props = [
        {
            "event_id": "event-1",
            "player": "Player A",
            "market": "player_points",
            "point": 20.5,
            "side": "over",
            "book": "draftkings",
            "price": 110,
            "link": "",
            "last_update": "2026-02-13T01:23:45Z",
        }
    ]
    config = ExecutionProjectionConfig(
        bookmakers=("draftkings",),
        top_n=5,
        requires_pre_bet_ready=True,
        requires_meets_play_to=False,
        tier_a_min_ev=0.03,
        tier_b_min_ev=0.05,
    )

    projected = project_execution_report(report, event_props, config)
    candidate = projected["candidates"][0]

    assert candidate["selected_price"] == 110
    assert candidate["eligible"] is False
    assert candidate["reason"] == "execution_pre_bet_not_ready"
    assert projected["summary"]["eligible_lines"] == 0
    assert projected["ranked_plays"] == []
    assert projected["watchlist"][0]["reason"] == "execution_pre_bet_not_ready"
    assert projected["strategy_mode"] == "watchlist_only"
    assert report["summary"]["eligible_lines"] == 1
