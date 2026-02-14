import pytest

from prop_ev.cli import _parse_strategy_ids
from prop_ev.strategies import get_strategy, list_strategies, resolve_strategy_id, strategy_aliases


def test_strategy_aliases_disabled() -> None:
    assert strategy_aliases() == {}
    assert resolve_strategy_id("s001") == "s001"


def test_get_strategy_accepts_s00x_ids() -> None:
    assert get_strategy("s001").info.id == "s001"
    assert get_strategy("s004").info.id == "s004"
    assert get_strategy("s008").info.id == "s008"


def test_get_strategy_rejects_legacy_ids() -> None:
    with pytest.raises(ValueError, match="unknown strategy id"):
        get_strategy("legacy_core")


def test_parse_strategy_ids_dedupes_s00x_ids() -> None:
    parsed = _parse_strategy_ids("s001,s001,s002,s002")
    assert parsed == ["s001", "s002"]


def test_strategy_registry_has_titles_and_descriptions() -> None:
    rows = list_strategies()
    by_id = {row.info.id: row.info for row in rows}
    assert by_id["s001"].name == "Baseline Core"
    assert by_id["s002"].name == "Baseline Core + Tier B"
    assert by_id["s003"].description
    assert by_id["s007"].name == "Quality Composite Gate"
    assert by_id["s008"].name == "Conservative Quality Floor"
