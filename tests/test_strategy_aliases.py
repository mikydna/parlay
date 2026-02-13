from prop_ev.cli import _parse_strategy_ids
from prop_ev.strategies import get_strategy, resolve_strategy_id, strategy_aliases


def test_strategy_aliases_resolve_to_canonical_ids() -> None:
    aliases = strategy_aliases()
    assert aliases["baseline"] == "v0"
    assert aliases["baseline_tier_b"] == "v0_tier_b"
    assert aliases["s001"] == "v0"
    assert aliases["s006"] == "gate_dispersion_iqr"
    assert resolve_strategy_id("baseline_core") == "v0"
    assert resolve_strategy_id("baseline_core_tier_b") == "v0_tier_b"
    assert resolve_strategy_id("s001") == "v0"
    assert resolve_strategy_id("s002") == "v0_tier_b"


def test_get_strategy_accepts_alias_names() -> None:
    assert get_strategy("baseline").info.id == "v0"
    assert get_strategy("baseline_tier_b").info.id == "v0_tier_b"
    assert get_strategy("s001").info.id == "v0"
    assert get_strategy("s004").info.id == "gate_book_pairs_min2"


def test_parse_strategy_ids_dedupes_aliases_to_canonical() -> None:
    parsed = _parse_strategy_ids("baseline,s001,v0,baseline_tier_b,s002,v0_tier_b")
    assert parsed == ["v0", "v0_tier_b"]
