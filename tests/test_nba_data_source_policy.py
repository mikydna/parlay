import pytest

from prop_ev.nba_data.source_policy import normalize_results_source_mode


@pytest.mark.parametrize("value", ["auto", "historical", "live", "cache_only"])
def test_normalize_results_source_mode_accepts_valid_values(value: str) -> None:
    assert normalize_results_source_mode(value) == value


def test_normalize_results_source_mode_rejects_invalid_values() -> None:
    with pytest.raises(ValueError):
        normalize_results_source_mode("bad-mode")
