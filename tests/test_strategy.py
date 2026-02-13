from datetime import UTC, datetime

from prop_ev.strategy import build_strategy_report


def test_build_strategy_report_tiers_and_ranked() -> None:
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    manifest = {
        "created_at_utc": "2026-02-11T00:00:00Z",
        "schema_version": 1,
        "quota": {"remaining": "490", "used": "10", "last": "1"},
        "requests": {
            "a": {"status": "ok"},
            "b": {"status": "ok"},
        },
    }
    rows = [
        {
            "event_id": "event-1",
            "market": "player_points",
            "player": "Player A",
            "point": 22.5,
            "side": "Over",
            "price": 100,
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
            "price": 95,
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
            "price": 120,
            "book": "book_b",
            "link": "",
            "last_update": now_utc,
        },
        {
            "event_id": "event-2",
            "market": "player_points",
            "player": "Player B",
            "point": 18.5,
            "side": "Over",
            "price": -110,
            "book": "book_a",
            "link": "",
            "last_update": now_utc,
        },
    ]
    event_context = {
        "event-1": {
            "home_team": "Boston Celtics",
            "away_team": "Miami Heat",
            "commence_time": "2026-02-11T00:00:00Z",
        },
        "event-2": {
            "home_team": "New York Knicks",
            "away_team": "Atlanta Hawks",
            "commence_time": "2026-02-11T01:00:00Z",
        },
    }
    roster = {
        "status": "ok",
        "count_teams": 4,
        "teams": {
            "boston celtics": {"active": ["playera"], "inactive": [], "all": ["playera"]},
            "miami heat": {"active": [], "inactive": [], "all": []},
            "new york knicks": {"active": [], "inactive": [], "all": []},
            "atlanta hawks": {"active": ["playerb"], "inactive": [], "all": ["playerb"]},
        },
    }
    injuries = {
        "official": {
            "status": "ok",
            "parse_status": "ok",
            "rows_count": 1,
            "rows": [
                {
                    "player": "Player A",
                    "player_norm": "playera",
                    "team": "Boston Celtics",
                    "team_norm": "boston celtics",
                    "status": "available",
                    "note": "",
                    "source": "official_nba_pdf",
                }
            ],
        },
        "secondary": {"status": "ok", "rows": []},
    }

    report = build_strategy_report(
        snapshot_id="snap-1",
        manifest=manifest,
        rows=rows,
        top_n=10,
        event_context=event_context,
        roster=roster,
        injuries=injuries,
        require_official_injuries=True,
    )
    ranked = report["ranked_plays"]
    watchlist = report["watchlist"]

    assert report["strategy_status"] == "modeled_with_gates"
    assert report["state_key"]["strategy_status"]["modeled_with_gates"]
    assert report["state_key"]["strategy_mode"]["full_board"]
    assert report["summary"]["tier_a_lines"] == 1
    assert report["summary"]["tier_b_lines"] == 1
    assert report["summary"]["under_sweep_status"] in {"ok", "insufficient"}
    assert "slate_snapshot" in report
    assert "top_ev_plays" in report
    assert "one_source_edges" in report
    assert ranked[0]["player"] == "Player A"
    assert ranked[0]["tier"] == "A"
    assert ranked[0]["over_best_price"] == 100
    assert ranked[0]["play_to_american"] is not None
    assert watchlist[0]["player"] == "Player B"


def test_build_strategy_report_official_injury_gate_watchlist_only() -> None:
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    report = build_strategy_report(
        snapshot_id="snap-2",
        manifest={"requests": {}},
        rows=[
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "side": "Over",
                "price": 110,
                "book": "book_a",
                "link": "",
                "last_update": now_utc,
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "side": "Under",
                "price": -120,
                "book": "book_b",
                "link": "",
                "last_update": now_utc,
            },
        ],
        top_n=5,
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
        injuries={"official": {"status": "error"}, "secondary": {"status": "ok", "rows": []}},
        require_official_injuries=True,
    )

    assert report["strategy_mode"] == "watchlist_only"
    assert report["summary"]["eligible_lines"] == 0
    health = report["health_report"]
    assert "official_injury_missing" in health["health_gates"]


def test_build_strategy_report_stale_context_gate() -> None:
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    report = build_strategy_report(
        snapshot_id="snap-3",
        manifest={"requests": {}},
        rows=[
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "side": "Over",
                "price": 110,
                "book": "book_a",
                "link": "",
                "last_update": now_utc,
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "side": "Under",
                "price": -120,
                "book": "book_b",
                "link": "",
                "last_update": now_utc,
            },
        ],
        top_n=5,
        event_context={
            "event-1": {
                "home_team": "Boston Celtics",
                "away_team": "Miami Heat",
                "commence_time": now_utc,
            }
        },
        roster={
            "status": "ok",
            "stale": True,
            "count_teams": 2,
            "teams": {
                "boston celtics": {"active": ["playera"], "inactive": [], "all": ["playera"]},
                "miami heat": {"active": [], "inactive": [], "all": []},
            },
        },
        injuries={
            "stale": True,
            "official": {
                "status": "ok",
                "parse_status": "ok",
                "rows_count": 1,
                "rows": [
                    {
                        "player": "Player A",
                        "player_norm": "playera",
                        "team": "Boston Celtics",
                        "team_norm": "boston celtics",
                        "status": "available",
                        "note": "",
                        "source": "official_nba_pdf",
                    }
                ],
            },
            "secondary": {"status": "ok", "rows": []},
        },
        require_official_injuries=True,
        require_fresh_context=True,
    )
    assert report["strategy_mode"] == "watchlist_only"
    assert report["summary"]["eligible_lines"] == 0
    health = report["health_report"]
    assert "injuries_context_stale" in health["health_gates"]
    assert "roster_context_stale" in health["health_gates"]


def test_build_strategy_report_official_rows_override_secondary() -> None:
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    report = build_strategy_report(
        snapshot_id="snap-4",
        manifest={"requests": {}},
        rows=[
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "side": "Over",
                "price": 110,
                "book": "book_a",
                "link": "",
                "last_update": now_utc,
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "side": "Under",
                "price": -120,
                "book": "book_b",
                "link": "",
                "last_update": now_utc,
            },
        ],
        top_n=5,
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
        injuries={
            "official": {
                "status": "ok",
                "parse_status": "ok",
                "rows_count": 1,
                "rows": [
                    {
                        "player": "Player A",
                        "player_norm": "playera",
                        "team": "Boston Celtics",
                        "team_norm": "boston celtics",
                        "status": "out",
                        "note": "Injury/Illness - Knee",
                        "source": "official_nba_pdf",
                    }
                ],
            },
            "secondary": {
                "status": "ok",
                "rows": [
                    {
                        "player": "Player A",
                        "player_norm": "playera",
                        "team": "Boston Celtics",
                        "team_norm": "boston celtics",
                        "status": "probable",
                        "note": "",
                        "source": "secondary_injuries",
                    }
                ],
            },
        },
        require_official_injuries=True,
    )

    assert report["summary"]["eligible_lines"] == 0
    watchlist = report["watchlist"]
    assert watchlist[0]["injury_status"] == "out"
    assert watchlist[0]["reason"] == "injury_gate"


def test_build_strategy_report_marks_unlisted_official_players_as_available() -> None:
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    report = build_strategy_report(
        snapshot_id="snap-5",
        manifest={"requests": {}},
        rows=[
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "side": "Over",
                "price": 110,
                "book": "book_a",
                "link": "",
                "last_update": now_utc,
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "side": "Under",
                "price": -120,
                "book": "book_b",
                "link": "",
                "last_update": now_utc,
            },
        ],
        top_n=5,
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
        injuries={
            "official": {
                "status": "ok",
                "parse_status": "ok",
                "rows_count": 1,
                "rows": [
                    {
                        "player": "Someone Else",
                        "player_norm": "someoneelse",
                        "team": "Miami Heat",
                        "team_norm": "miami heat",
                        "status": "out",
                        "note": "",
                        "source": "official_nba_pdf",
                    }
                ],
            },
            "secondary": {"status": "ok", "rows": []},
        },
        require_official_injuries=True,
    )

    candidates = report["candidates"]
    assert candidates
    assert candidates[0]["injury_status"] == "available_unlisted"
    assert candidates[0]["pre_bet_ready"] is True
