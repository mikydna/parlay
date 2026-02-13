from prop_ev.nba_data.date_resolver import resolve_snapshot_date_str


def test_resolve_snapshot_date_daily_format() -> None:
    assert resolve_snapshot_date_str("daily-20260212T230052Z") == "2026-02-12"


def test_resolve_snapshot_date_day_dataset_suffix() -> None:
    assert resolve_snapshot_date_str("day-bdfa890a-2026-02-12") == "2026-02-12"


def test_resolve_snapshot_date_iso_prefix() -> None:
    assert resolve_snapshot_date_str("2026-02-11T19-15-00Z") == "2026-02-11"


def test_resolve_snapshot_date_et_snapshot_format() -> None:
    assert resolve_snapshot_date_str("2026-02-11T02-15-00-ET") == "2026-02-11"
