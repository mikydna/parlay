from __future__ import annotations

import json
from pathlib import Path

import pytest

from prop_ev.nba_data.errors import NBADataError
from prop_ev.nba_data.io_utils import atomic_write_json, atomic_write_jsonl
from prop_ev.nba_data.store.layout import build_layout, slugify_season_type
from prop_ev.nba_data.store.lock import LockConfig, lock_root
from prop_ev.nba_data.store.manifest import (
    ensure_row,
    load_manifest,
    reconcile_ok_statuses,
    set_resource_ok,
    write_manifest_deterministic,
)


def test_layout_creates_required_directories(tmp_path: Path) -> None:
    layout = build_layout(tmp_path / "nba_data")
    assert layout.pbpstats_response_dir.exists()
    assert (layout.pbpstats_response_dir / "game_details").exists()
    assert (layout.pbpstats_response_dir / "overrides").exists()
    assert (layout.pbpstats_response_dir / "pbp").exists()
    assert (layout.pbpstats_response_dir / "schedule").exists()
    assert slugify_season_type("Regular Season") == "regular_season"


def test_lock_blocks_second_acquire(tmp_path: Path) -> None:
    root = tmp_path / "nba_data"
    with (
        lock_root(root, config=LockConfig()),
        pytest.raises(NBADataError),
        lock_root(root, config=LockConfig()),
    ):
        pass


def test_stale_lock_is_recovered(tmp_path: Path) -> None:
    root = tmp_path / "nba_data"
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".lock"
    lock_path.write_text(
        json.dumps(
            {
                "pid": 999999,
                "hostname": "test",
                "started_at_utc": "2024-01-01T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with lock_root(root, config=LockConfig(stale_lock_minutes=1)):
        assert lock_path.exists()


def test_atomic_writes_and_manifest_roundtrip(tmp_path: Path) -> None:
    root = tmp_path / "nba_data"
    layout = build_layout(root)
    json_path = root / "tmp" / "a.json"
    jsonl_path = root / "tmp" / "b.jsonl"
    atomic_write_json(json_path, {"a": 1})
    atomic_write_jsonl(jsonl_path, [{"b": 1}, {"b": 2}])
    assert json_path.exists()
    assert jsonl_path.exists()

    manifest_path = layout.manifest_path(season="2025-26", season_type="Regular Season")
    rows = {}
    row = ensure_row(rows, season="2025-26", season_type="Regular Season", game_id="g1")
    raw_path = layout.raw_resource_path(
        resource="boxscore",
        season="2025-26",
        season_type="regular_season",
        game_id="g1",
        ext="json",
    )
    atomic_write_json(raw_path, {"players": []})
    set_resource_ok(
        root=layout.root, row=row, resource="boxscore", provider="data_nba", path=raw_path
    )
    write_manifest_deterministic(manifest_path, rows)

    loaded = load_manifest(manifest_path)
    assert ("2025-26", "regular_season", "g1") in loaded
    loaded_row = loaded[("2025-26", "regular_season", "g1")]
    assert loaded_row["resources"]["boxscore"]["status"] == "ok"
    raw_path.unlink()
    reconcile_ok_statuses(root=layout.root, row=loaded_row)
    assert loaded_row["resources"]["boxscore"]["status"] == "missing"
