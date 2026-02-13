from datetime import UTC, datetime

from prop_ev.strategies import get_strategy
from prop_ev.strategies.base import (
    StrategyInputs,
    StrategyRecipe,
    StrategyRunConfig,
    compose_strategy_recipes,
)


def _sample_inputs() -> StrategyInputs:
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return StrategyInputs(
        snapshot_id="snap-plugins",
        manifest={"requests": {}},
        rows=[
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
                "price": -105,
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
                "price": -115,
                "book": "book_b",
                "link": "",
                "last_update": now_utc,
            },
        ],
        injuries={
            "official": {
                "status": "ok",
                "rows_count": 1,
                "parse_status": "ok",
                "rows": [],
            },
            "secondary": {"status": "ok", "rows": []},
        },
        roster={
            "status": "ok",
            "count_teams": 2,
            "teams": {
                "boston celtics": {"active": ["playera"], "inactive": [], "all": ["playera"]},
                "miami heat": {"active": [], "inactive": [], "all": []},
            },
        },
        event_context={
            "event-1": {
                "home_team": "Boston Celtics",
                "away_team": "Miami Heat",
                "commence_time": now_utc,
            }
        },
        slate_rows=[],
        player_identity_map={},
    )


def _sample_config() -> StrategyRunConfig:
    return StrategyRunConfig(
        top_n=10,
        min_ev=0.01,
        allow_tier_b=False,
        require_official_injuries=False,
        stale_quote_minutes=20,
        require_fresh_context=False,
    )


def test_compose_strategy_recipes_combines_layers() -> None:
    combined = compose_strategy_recipes(
        StrategyRecipe(),
        StrategyRecipe(min_book_pairs=2),
        StrategyRecipe(hold_cap=0.08, force_allow_tier_b=True),
    )
    assert combined.force_allow_tier_b is True
    assert combined.min_book_pairs == 2
    assert combined.hold_cap == 0.08


def test_s002_forces_allow_tier_b() -> None:
    result = get_strategy("s002").run(inputs=_sample_inputs(), config=_sample_config())
    assert result.config.allow_tier_b is True


def test_s003_uses_median_no_vig_recipe() -> None:
    report = get_strategy("s003").run(inputs=_sample_inputs(), config=_sample_config()).report
    audit = report["audit"]
    assert audit["market_baseline_method"] == "median_book"
    assert audit["market_baseline_fallback"] == "best_sides"


def test_gate_strategies_set_recipe_audit_fields() -> None:
    report_s004 = get_strategy("s004").run(inputs=_sample_inputs(), config=_sample_config()).report
    report_s005 = get_strategy("s005").run(inputs=_sample_inputs(), config=_sample_config()).report
    report_s006 = get_strategy("s006").run(inputs=_sample_inputs(), config=_sample_config()).report
    assert report_s004["audit"]["min_book_pairs"] == 2
    assert report_s005["audit"]["hold_cap"] == 0.08
    assert report_s006["audit"]["p_over_iqr_cap"] == 0.08
