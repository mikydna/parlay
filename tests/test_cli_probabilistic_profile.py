from __future__ import annotations

from prop_ev.cli import _resolve_input_probabilistic_profile


def test_resolve_input_profile_uses_strategy_recipe_when_cli_omitted() -> None:
    resolved = _resolve_input_probabilistic_profile(
        default_profile="off",
        probabilistic_profile_arg="",
        strategy_ids=["s020"],
    )
    assert resolved == "minutes_v1"


def test_resolve_input_profile_uses_minutes_when_any_strategy_requires_it() -> None:
    resolved = _resolve_input_probabilistic_profile(
        default_profile="off",
        probabilistic_profile_arg="",
        strategy_ids=["s001", "s020"],
    )
    assert resolved == "minutes_v1"


def test_resolve_input_profile_honors_explicit_override() -> None:
    resolved = _resolve_input_probabilistic_profile(
        default_profile="off",
        probabilistic_profile_arg="off",
        strategy_ids=["s020"],
    )
    assert resolved == "off"
