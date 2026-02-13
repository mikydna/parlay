from prop_ev.state_keys import (
    playbook_mode_key,
    strategy_health_state_key,
    strategy_report_state_key,
)
from prop_ev.strategies.base import StrategyInfo, StrategyRunConfig, decorate_report


def test_strategy_report_state_key_has_human_labels() -> None:
    mapping = strategy_report_state_key()
    assert "modeled_with_gates" in mapping["strategy_status"]
    assert "full_board" in mapping["strategy_mode"]
    assert "official_injury_missing" in mapping["health_gates"]


def test_strategy_health_state_key_has_status_and_gates() -> None:
    mapping = strategy_health_state_key()
    assert "healthy" in mapping["status"]
    assert "event_mapping_failed" in mapping["gates"]


def test_playbook_mode_key_has_live_and_offline_modes() -> None:
    mapping = playbook_mode_key()
    assert "live_snapshot" in mapping
    assert "offline_context_gate" in mapping


def test_decorate_report_adds_strategy_id_map() -> None:
    report: dict[str, object] = {
        "summary": {},
        "audit": {},
        "state_key": strategy_report_state_key(),
    }
    strategy = StrategyInfo(id="v0", name="v0", description="Baseline strategy")
    config = StrategyRunConfig(
        top_n=5,
        min_ev=0.01,
        allow_tier_b=False,
        require_official_injuries=True,
        stale_quote_minutes=30,
        require_fresh_context=True,
    )

    decorated = decorate_report(report, strategy=strategy, config=config)

    assert decorated["strategy_id"] == "v0"
    assert decorated["strategy"] == {
        "id": "v0",
        "name": "v0",
        "description": "Baseline strategy",
    }
    assert decorated["state_key"]["strategy_id"]["v0"] == "Baseline strategy"
