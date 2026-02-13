from __future__ import annotations

from pathlib import Path

from prop_ev.nba_data.ingest.fetch import ingest_resources
from prop_ev.nba_data.io_utils import atomic_write_json
from prop_ev.nba_data.store.layout import build_layout
from prop_ev.nba_data.store.lock import LockConfig
from prop_ev.nba_data.store.manifest import ensure_row


def _providers() -> dict[str, list[str]]:
    return {
        "boxscore": ["data_nba", "stats_nba"],
        "enhanced_pbp": ["data_nba", "stats_nba"],
        "possessions": ["data_nba", "stats_nba"],
    }


def test_ingest_skips_when_ok_and_valid(tmp_path: Path, monkeypatch) -> None:
    layout = build_layout(tmp_path / "nba_data")
    rows = {}
    row = ensure_row(rows, season="2025-26", season_type="Regular Season", game_id="g1")
    box_path = layout.raw_resource_path(
        resource="boxscore",
        season="2025-26",
        season_type="regular_season",
        game_id="g1",
        ext="json",
    )
    atomic_write_json(box_path, {"players": []})
    row["resources"]["boxscore"]["status"] = "ok"
    row["resources"]["boxscore"]["path"] = box_path.relative_to(layout.root).as_posix()

    called = {"count": 0}

    def _fake_load(**kwargs):
        called["count"] += 1
        return {"players": []}

    monkeypatch.setattr("prop_ev.nba_data.ingest.fetch._load_via_source", _fake_load)
    summary = ingest_resources(
        layout=layout,
        rows=rows,
        season="2025-26",
        season_type="Regular Season",
        resources=["boxscore"],
        only_missing=True,
        retry_errors=False,
        max_games=0,
        rpm=1000,
        providers=_providers(),  # pyright: ignore[reportArgumentType]
        fail_fast=False,
        lock_config=LockConfig(),
    )
    assert summary["skipped"] == 1
    assert called["count"] == 0


def test_ingest_provider_fallback(tmp_path: Path, monkeypatch) -> None:
    layout = build_layout(tmp_path / "nba_data")
    rows = {}
    ensure_row(rows, season="2025-26", season_type="Regular Season", game_id="g1")

    calls: list[tuple[str, str]] = []

    def _fake_load(*, source: str, provider: str, **kwargs):
        calls.append((source, provider))
        if source == "file":
            raise RuntimeError("file miss")
        if provider == "data_nba":
            raise RuntimeError("provider fail")
        return [{"event_num": 1, "clock": "12:00"}]

    monkeypatch.setattr("prop_ev.nba_data.ingest.fetch._load_via_source", _fake_load)
    summary = ingest_resources(
        layout=layout,
        rows=rows,
        season="2025-26",
        season_type="Regular Season",
        resources=["enhanced_pbp"],
        only_missing=True,
        retry_errors=False,
        max_games=0,
        rpm=1000,
        providers=_providers(),  # pyright: ignore[reportArgumentType]
        fail_fast=False,
        lock_config=LockConfig(),
    )

    assert summary["ok"] == 1
    assert ("file", "data_nba") in calls
    assert ("web", "stats_nba") in calls
    row = rows[("2025-26", "regular_season", "g1")]
    assert row["resources"]["enhanced_pbp"]["status"] == "ok"
    assert row["resources"]["enhanced_pbp"]["provider"] == "stats_nba"


def test_ingest_retry_errors_toggle(tmp_path: Path, monkeypatch) -> None:
    layout = build_layout(tmp_path / "nba_data")
    rows = {}
    row = ensure_row(rows, season="2025-26", season_type="Regular Season", game_id="g1")
    row["resources"]["possessions"]["status"] = "error"

    called = {"count": 0}

    def _fake_load(**kwargs):
        called["count"] += 1
        return [{"possession_id": 1}]

    monkeypatch.setattr("prop_ev.nba_data.ingest.fetch._load_via_source", _fake_load)
    summary_skip = ingest_resources(
        layout=layout,
        rows=rows,
        season="2025-26",
        season_type="Regular Season",
        resources=["possessions"],
        only_missing=True,
        retry_errors=False,
        max_games=0,
        rpm=1000,
        providers=_providers(),  # pyright: ignore[reportArgumentType]
        fail_fast=False,
        lock_config=LockConfig(),
    )
    assert summary_skip["skipped"] == 1
    assert called["count"] == 0

    summary_retry = ingest_resources(
        layout=layout,
        rows=rows,
        season="2025-26",
        season_type="Regular Season",
        resources=["possessions"],
        only_missing=True,
        retry_errors=True,
        max_games=0,
        rpm=1000,
        providers=_providers(),  # pyright: ignore[reportArgumentType]
        fail_fast=False,
        lock_config=LockConfig(),
    )
    assert summary_retry["ok"] == 1
    assert called["count"] > 0
