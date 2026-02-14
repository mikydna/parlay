from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from prop_ev.nba_data.minutes_prob import artifacts as minutes_artifacts
from prop_ev.nba_data.minutes_prob.artifacts import (
    load_minutes_prob_index_for_snapshot,
    minutes_prob_root,
    predictions_path_for_day,
)
from prop_ev.nba_data.store.layout import build_layout


def _write_predictions(path: Path, *, snapshot_day: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "event_id": ["event-1"],
            "market": ["player_points"],
            "player_id": ["p1"],
            "player_name": ["Player One"],
            "player_norm": ["playerone"],
            "minutes_p10": [24.0],
            "minutes_p50": [30.0],
            "minutes_p90": [36.0],
            "minutes_mu": [30.5],
            "minutes_sigma_proxy": [4.0],
            "p_active": [0.98],
            "games_on_team": [25],
            "days_on_team": [80],
            "new_team_phase": ["gte_10"],
            "confidence_score": [0.81],
            "data_quality_flags": [""],
            "snapshot_date": [snapshot_day],
        }
    ).write_parquet(path)


def test_load_minutes_prob_index_profile_off(tmp_path: Path) -> None:
    layout = build_layout(tmp_path / "nba_data")
    payload = load_minutes_prob_index_for_snapshot(
        layout=layout,
        snapshot_day="2026-02-08",
        probabilistic_profile="off",
        auto_build=False,
    )
    assert payload["exact"] == {}
    assert payload["player"] == {}
    assert payload["meta"]["profile"] == "off"


def test_load_minutes_prob_index_does_not_use_latest_other_day(tmp_path: Path) -> None:
    layout = build_layout(tmp_path / "nba_data")
    root = minutes_prob_root(layout)
    latest_predictions = root / "latest" / "predictions.parquet"
    _write_predictions(latest_predictions, snapshot_day="2026-02-08")
    (root / "latest" / "predictions.meta.json").write_text(
        json.dumps({"as_of_date": "2026-02-08"}) + "\n",
        encoding="utf-8",
    )

    payload = load_minutes_prob_index_for_snapshot(
        layout=layout,
        snapshot_day="2026-02-09",
        probabilistic_profile="minutes_v1",
        auto_build=False,
    )
    snapshot_path = predictions_path_for_day(root_dir=root, snapshot_day="2026-02-09")
    assert int(payload["meta"]["rows"]) == 0
    assert payload["meta"]["cache_mode"] == "missing"
    assert Path(str(payload["meta"]["path"])) == snapshot_path
    assert not snapshot_path.exists()


def test_load_minutes_prob_index_uses_matching_latest_with_write_through(tmp_path: Path) -> None:
    layout = build_layout(tmp_path / "nba_data")
    root = minutes_prob_root(layout)
    latest_predictions = root / "latest" / "predictions.parquet"
    _write_predictions(latest_predictions, snapshot_day="2026-02-08")
    (root / "latest" / "predictions.meta.json").write_text(
        json.dumps({"as_of_date": "2026-02-08"}) + "\n",
        encoding="utf-8",
    )

    payload = load_minutes_prob_index_for_snapshot(
        layout=layout,
        snapshot_day="2026-02-08",
        probabilistic_profile="minutes_v1",
        auto_build=False,
    )
    snapshot_path = predictions_path_for_day(root_dir=root, snapshot_day="2026-02-08")
    key = "event-1|playerone|player_points"
    assert snapshot_path.exists()
    assert key in payload["exact"]
    assert int(payload["meta"]["rows"]) == 1
    assert payload["meta"]["cache_mode"] == "latest_write_through"
    assert Path(str(payload["meta"]["path"])) == snapshot_path


def test_load_minutes_prob_index_auto_build_writes_snapshot_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = build_layout(tmp_path / "nba_data")
    called = {"value": False}

    def _fake_auto_build(*, layout: object, model_root_dir: Path, snapshot_day: str) -> Path:
        called["value"] = True
        out_path = predictions_path_for_day(root_dir=model_root_dir, snapshot_day=snapshot_day)
        _write_predictions(out_path, snapshot_day=snapshot_day)
        return out_path

    monkeypatch.setattr(minutes_artifacts, "maybe_auto_build_predictions_for_day", _fake_auto_build)
    payload = load_minutes_prob_index_for_snapshot(
        layout=layout,
        snapshot_day="2026-02-10",
        probabilistic_profile="minutes_v1",
        auto_build=True,
    )
    key = "event-1|playerone|player_points"
    assert called["value"] is True
    assert key in payload["exact"]
    assert int(payload["meta"]["rows"]) == 1
    assert payload["meta"]["cache_mode"] == "snapshot_cache"
