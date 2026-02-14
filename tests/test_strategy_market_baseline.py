from datetime import UTC, datetime

from prop_ev.strategy import build_strategy_report


def test_market_baseline_median_book_and_gates() -> None:
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    rows = [
        {
            "event_id": "event-1",
            "market": "player_points",
            "player": "Player A",
            "point": 22.5,
            "side": "Over",
            "price": -110,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-1",
            "market": "player_points",
            "player": "Player A",
            "point": 22.5,
            "side": "Under",
            "price": -110,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-1",
            "market": "player_points",
            "player": "Player A",
            "point": 22.5,
            "side": "Over",
            "price": 300,
            "book": "book_b",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-1",
            "market": "player_points",
            "player": "Player A",
            "point": 22.5,
            "side": "Under",
            "price": -1000,
            "book": "book_b",
            "link": "",
            "last_update": now_utc,
        },
    ]
    report = build_strategy_report(
        snapshot_id="snap-1",
        manifest={"requests": {}},
        rows=rows,
        top_n=10,
        event_context={
            "event-1": {
                "home_team": "Boston Celtics",
                "away_team": "Miami Heat",
                "commence_time": now_utc,
            }
        },
        roster={
            "status": "ok",
            "count_teams": 2,
            "teams": {
                "boston celtics": {"active": ["playera"], "inactive": [], "all": ["playera"]},
                "miami heat": {"active": [], "inactive": [], "all": []},
            },
        },
        injuries={"official": {"status": "ok"}, "secondary": {"status": "ok", "rows": []}},
        require_official_injuries=True,
        market_baseline_method="median_book",
        market_baseline_fallback="best_sides",
        min_book_pairs=2,
    )
    candidates = report["candidates"]
    assert candidates
    row = candidates[0]
    assert row["baseline_used"] in {
        "median_book",
        "median_book_interpolated",
        "best_sides_fallback",
    }
    assert row["book_pair_count"] == 2
    assert row["p_over_book_median"] is not None
    assert row["hold_book_median"] is not None
    assert row["quality_score"] is not None
    assert row["uncertainty_band"] is not None
    assert row["p_hit_low"] is not None
    assert row["ev_low"] is not None
    assert report["summary"]["actionability_rate"] is not None

    gated = build_strategy_report(
        snapshot_id="snap-2",
        manifest={"requests": {}},
        rows=rows,
        top_n=10,
        event_context={
            "event-1": {
                "home_team": "Boston Celtics",
                "away_team": "Miami Heat",
                "commence_time": now_utc,
            }
        },
        roster={
            "status": "ok",
            "count_teams": 2,
            "teams": {
                "boston celtics": {"active": ["playera"], "inactive": [], "all": ["playera"]},
                "miami heat": {"active": [], "inactive": [], "all": []},
            },
        },
        injuries={"official": {"status": "ok"}, "secondary": {"status": "ok", "rows": []}},
        require_official_injuries=True,
        min_book_pairs=3,
    )
    assert gated["summary"]["eligible_lines"] == 0
    assert gated["watchlist"][0]["reason"] == "book_pairs_gate"


def test_market_baseline_uncertainty_gate_blocks_candidates() -> None:
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    rows = [
        {
            "event_id": "event-1",
            "market": "player_points",
            "player": "Player A",
            "point": 22.5,
            "side": "Over",
            "price": -110,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-1",
            "market": "player_points",
            "player": "Player A",
            "point": 22.5,
            "side": "Under",
            "price": -110,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
    ]
    report = build_strategy_report(
        snapshot_id="snap-3",
        manifest={"requests": {}},
        rows=rows,
        top_n=10,
        event_context={
            "event-1": {
                "home_team": "Boston Celtics",
                "away_team": "Miami Heat",
                "commence_time": now_utc,
            }
        },
        roster={
            "status": "ok",
            "count_teams": 2,
            "teams": {
                "boston celtics": {"active": ["playera"], "inactive": [], "all": ["playera"]},
                "miami heat": {"active": [], "inactive": [], "all": []},
            },
        },
        injuries={"official": {"status": "ok"}, "secondary": {"status": "ok", "rows": []}},
        require_official_injuries=False,
        allow_tier_b=True,
        max_uncertainty_band=0.005,
    )
    assert report["summary"]["eligible_lines"] == 0
    assert report["watchlist"][0]["reason"] == "uncertainty_band_gate"


def test_market_baseline_interpolates_when_exact_book_pairs_missing() -> None:
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    rows = [
        {
            "event_id": "event-2",
            "market": "player_points",
            "player": "Player B",
            "point": 20.5,
            "side": "Over",
            "price": -120,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-2",
            "market": "player_points",
            "player": "Player B",
            "point": 20.5,
            "side": "Under",
            "price": 100,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-2",
            "market": "player_points",
            "player": "Player B",
            "point": 24.5,
            "side": "Over",
            "price": 120,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-2",
            "market": "player_points",
            "player": "Player B",
            "point": 24.5,
            "side": "Under",
            "price": -140,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-2",
            "market": "player_points",
            "player": "Player B",
            "point": 22.5,
            "side": "Over",
            "price": -110,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-2",
            "market": "player_points",
            "player": "Player B",
            "point": 22.5,
            "side": "Under",
            "price": -110,
            "book": "book_b",
            "link": "",
            "last_update": now_utc,
        },
    ]

    report = build_strategy_report(
        snapshot_id="snap-4",
        manifest={"requests": {}},
        rows=rows,
        top_n=10,
        event_context={
            "event-2": {
                "home_team": "Boston Celtics",
                "away_team": "Miami Heat",
                "commence_time": now_utc,
            }
        },
        roster={
            "status": "ok",
            "count_teams": 2,
            "teams": {
                "boston celtics": {"active": ["playerb"], "inactive": [], "all": ["playerb"]},
                "miami heat": {"active": [], "inactive": [], "all": []},
            },
        },
        injuries={"official": {"status": "ok"}, "secondary": {"status": "ok", "rows": []}},
        require_official_injuries=True,
        allow_tier_b=True,
        market_baseline_method="median_book",
        market_baseline_fallback="none",
    )

    candidate = next(row for row in report["candidates"] if row["point"] == 22.5)
    assert candidate["baseline_used"] == "median_book_interpolated"
    assert candidate["reference_line_method"] == "interpolated"
    assert candidate["reference_points_count"] >= 2
    assert candidate["p_over_fair"] is not None
