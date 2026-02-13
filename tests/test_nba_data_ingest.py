from __future__ import annotations

from pathlib import Path
from typing import Any

from prop_ev.nba_data.ingest.fetch import _load_via_source, ingest_resources
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


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


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


def test_load_via_source_web_uses_cdn_fallback_for_enhanced_pbp(
    tmp_path: Path, monkeypatch
) -> None:
    layout = build_layout(tmp_path / "nba_data")

    monkeypatch.setattr("prop_ev.nba_data.ingest.fetch.build_client", lambda **kwargs: object())
    monkeypatch.setattr(
        "prop_ev.nba_data.ingest.fetch.load_game_resource",
        lambda **kwargs: (_ for _ in ()).throw(KeyError("resultSets")),
    )

    def _fake_get(url: str, **kwargs) -> _FakeResponse:
        assert "cdn.nba.com" in url
        return _FakeResponse(
            {
                "game": {
                    "actions": [
                        {
                            "actionNumber": 1,
                            "clock": "PT12M00.00S",
                            "actionType": "period",
                            "subType": "start",
                            "teamId": 0,
                            "personId": 0,
                            "description": "start",
                            "possession": 1610612745,
                        },
                        {
                            "actionNumber": 2,
                            "clock": "PT11M40.00S",
                            "actionType": "shot",
                            "subType": "2pt",
                            "teamId": 1610612745,
                            "personId": 123,
                            "description": "made shot",
                            "possession": 1610612745,
                        },
                    ]
                }
            }
        )

    monkeypatch.setattr("prop_ev.nba_data.ingest.fetch.requests.get", _fake_get)
    payload = _load_via_source(
        layout=layout,
        source="web",
        provider="stats_nba",
        resource="enhanced_pbp",
        season="2025-26",
        season_type="Regular Season",
        game_id="0022500001",
    )
    assert isinstance(payload, list)
    assert payload[0]["event_num"] == 1
    assert payload[1]["event_type"] == "shot"


def test_load_via_source_web_uses_cdn_fallback_for_boxscore(tmp_path: Path, monkeypatch) -> None:
    layout = build_layout(tmp_path / "nba_data")

    monkeypatch.setattr("prop_ev.nba_data.ingest.fetch.build_client", lambda **kwargs: object())
    monkeypatch.setattr(
        "prop_ev.nba_data.ingest.fetch.load_game_resource",
        lambda **kwargs: (_ for _ in ()).throw(KeyError("resultSets")),
    )

    def _fake_get(url: str, **kwargs) -> _FakeResponse:
        assert "cdn.nba.com" in url
        return _FakeResponse(
            {
                "game": {
                    "homeTeam": {
                        "teamId": 1610612745,
                        "players": [
                            {
                                "personId": 123,
                                "statistics": {
                                    "minutes": "PT30M30.00S",
                                    "points": 20,
                                    "reboundsTotal": 7,
                                    "assists": 4,
                                },
                            }
                        ],
                    },
                    "awayTeam": {"teamId": 1610612737, "players": []},
                }
            }
        )

    monkeypatch.setattr("prop_ev.nba_data.ingest.fetch.requests.get", _fake_get)
    payload = _load_via_source(
        layout=layout,
        source="web",
        provider="stats_nba",
        resource="boxscore",
        season="2025-26",
        season_type="Regular Season",
        game_id="0022500001",
    )
    assert isinstance(payload, dict)
    players = payload.get("players")
    assert isinstance(players, list)
    assert players[0]["team_id"] == "1610612745"
    assert players[0]["player_id"] == "123"
    assert players[0]["minutes"] == 30.5
