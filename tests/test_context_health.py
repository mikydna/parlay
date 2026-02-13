from __future__ import annotations

from prop_ev.context_health import (
    official_rows_count,
    official_source_ready,
    secondary_source_ready,
)


def test_official_rows_count_handles_common_shapes() -> None:
    assert official_rows_count(None) == 0
    assert official_rows_count({"rows": [1, 2]}) == 2
    assert official_rows_count({"rows": [1], "rows_count": "3"}) == 3
    assert official_rows_count({"rows": [1, 2], "rows_count": True}) == 2
    assert official_rows_count({"rows": [1], "rows_count": "-1"}) == 0
    assert official_rows_count({"rows": [1, 2], "rows_count": "nope"}) == 2


def test_official_source_ready_requires_ok_status_and_parse_status() -> None:
    assert official_source_ready({"status": "ok", "rows_count": 1}) is True
    assert official_source_ready({"status": "ok", "rows_count": 1, "parse_status": "ok"}) is True
    assert official_source_ready({"status": "ok", "rows_count": 0}) is False
    assert official_source_ready({"status": "error", "rows_count": 2}) is False
    assert (
        official_source_ready({"status": "ok", "rows_count": 1, "parse_status": "failed"}) is False
    )


def test_secondary_source_ready_count_coercion() -> None:
    assert secondary_source_ready(None) is False
    assert secondary_source_ready({"status": "error", "count": 4}) is False
    assert secondary_source_ready({"status": "ok", "count": 0}) is False
    assert secondary_source_ready({"status": "ok", "count": "2"}) is True
    assert secondary_source_ready({"status": "ok", "rows": [{}], "count": True}) is True
    assert secondary_source_ready({"status": "ok", "rows": [{}], "count": "bad"}) is True
