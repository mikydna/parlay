from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

from prop_ev.time_utils import iso_z, parse_iso_z, utc_now, utc_now_str


def test_utc_now_is_utc_without_microseconds() -> None:
    now = utc_now()

    assert now.tzinfo == UTC
    assert now.microsecond == 0


def test_utc_now_str_uses_z_suffix() -> None:
    value = utc_now_str()

    assert value.endswith("Z")
    assert "+00:00" not in value


def test_iso_z_normalizes_naive_datetime() -> None:
    value = datetime(2026, 2, 13, 10, 0, 0)

    assert iso_z(value) == "2026-02-13T10:00:00Z"


def test_iso_z_normalizes_non_utc_datetime() -> None:
    eastern = datetime(2026, 2, 13, 5, 0, 0, tzinfo=timezone(timedelta(hours=-5)))

    assert iso_z(eastern) == "2026-02-13T10:00:00Z"


def test_parse_iso_z_parses_z_and_naive() -> None:
    parsed_z = parse_iso_z("2026-02-13T10:00:00Z")
    parsed_naive = parse_iso_z("2026-02-13T10:00:00")

    assert parsed_z == datetime(2026, 2, 13, 10, 0, 0, tzinfo=UTC)
    assert parsed_naive == datetime(2026, 2, 13, 10, 0, 0, tzinfo=UTC)


def test_parse_iso_z_invalid_returns_none() -> None:
    assert parse_iso_z("not-a-date") is None
