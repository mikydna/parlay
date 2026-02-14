from prop_ev.portfolio import (
    PORTFOLIO_REASON_DAILY_CAP,
    PORTFOLIO_REASON_GAME_CAP,
    PORTFOLIO_REASON_PLAYER_CAP,
    PortfolioConstraints,
    select_portfolio_candidates,
)


def _row(
    *,
    event_id: str,
    player: str,
    market: str = "player_points",
    side: str = "over",
    ev_low: float,
    best_ev: float,
    quality_score: float,
    prior_delta: float = 0.0,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "player": player,
        "market": market,
        "recommended_side": side,
        "point": 20.5,
        "ev_low": ev_low,
        "best_ev": best_ev,
        "quality_score": quality_score,
        "historical_prior_delta": prior_delta,
    }


def test_select_portfolio_enforces_player_and_game_caps() -> None:
    selected, excluded = select_portfolio_candidates(
        eligible_rows=[
            _row(event_id="g1", player="Player A", ev_low=0.05, best_ev=0.07, quality_score=0.7),
            _row(
                event_id="g1",
                player="Player A",
                market="player_rebounds",
                ev_low=0.049,
                best_ev=0.069,
                quality_score=0.7,
            ),
            _row(event_id="g1", player="Player B", ev_low=0.048, best_ev=0.065, quality_score=0.7),
            _row(event_id="g1", player="Player C", ev_low=0.047, best_ev=0.064, quality_score=0.7),
            _row(event_id="g2", player="Player D", ev_low=0.046, best_ev=0.063, quality_score=0.7),
        ],
        constraints=PortfolioConstraints(max_picks=3, max_per_player=1, max_per_game=2),
    )

    assert [row["player"] for row in selected] == ["Player A", "Player B", "Player D"]
    reasons = {str(row.get("player")): str(row.get("portfolio_reason")) for row in excluded}
    assert reasons["Player A"] == PORTFOLIO_REASON_PLAYER_CAP
    assert reasons["Player C"] == PORTFOLIO_REASON_GAME_CAP


def test_select_portfolio_enforces_daily_cap() -> None:
    selected, excluded = select_portfolio_candidates(
        eligible_rows=[
            _row(event_id="g1", player="Player A", ev_low=0.05, best_ev=0.07, quality_score=0.7),
            _row(event_id="g2", player="Player B", ev_low=0.04, best_ev=0.06, quality_score=0.7),
            _row(event_id="g3", player="Player C", ev_low=0.03, best_ev=0.05, quality_score=0.7),
        ],
        constraints=PortfolioConstraints(max_picks=2, max_per_player=1, max_per_game=2),
    )

    assert [row["player"] for row in selected] == ["Player A", "Player B"]
    assert len(excluded) == 1
    assert excluded[0]["portfolio_reason"] == PORTFOLIO_REASON_DAILY_CAP


def test_select_portfolio_uses_historical_prior_in_tie_break() -> None:
    selected, _excluded = select_portfolio_candidates(
        eligible_rows=[
            _row(
                event_id="g1",
                player="Player A",
                ev_low=0.03,
                best_ev=0.04,
                quality_score=0.7,
                prior_delta=0.02,
            ),
            _row(
                event_id="g2",
                player="Player B",
                ev_low=0.031,
                best_ev=0.04,
                quality_score=0.7,
                prior_delta=0.0,
            ),
        ],
        constraints=PortfolioConstraints(max_picks=1, max_per_player=1, max_per_game=2),
    )

    assert len(selected) == 1
    assert selected[0]["player"] == "Player A"
