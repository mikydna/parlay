from prop_ev.cli import _build_discovery_execution_report


def test_build_discovery_execution_report_filters_actionable() -> None:
    discovery = {
        "candidates": [
            {
                "eligible": True,
                "event_id": "e1",
                "game": "A @ B",
                "player": "Player A",
                "market": "player_points",
                "point": 20.5,
                "recommended_side": "over",
                "selected_price": 120,
                "selected_book": "book_x",
                "play_to_american": 105,
                "model_p_hit": 0.55,
                "best_ev": 0.1,
                "pre_bet_ready": True,
            }
        ]
    }
    execution = {
        "candidates": [
            {
                "event_id": "e1",
                "player": "Player A",
                "market": "player_points",
                "point": 20.5,
                "over_best_price": 110,
                "over_best_book": "draftkings",
                "under_best_price": -130,
                "under_best_book": "draftkings",
                "tier": "A",
                "best_ev": 0.04,
            }
        ]
    }

    report = _build_discovery_execution_report(
        discovery_snapshot_id="disc",
        execution_snapshot_id="exec",
        discovery_report=discovery,
        execution_report=execution,
        top_n=10,
    )

    summary = report["summary"]
    assert summary["discovery_eligible_rows"] == 1
    assert summary["matched_execution_rows"] == 1
    assert summary["actionable_rows"] == 1
    assert len(report["actionable"]) == 1
    assert report["actionable"][0]["meets_play_to"] is True
