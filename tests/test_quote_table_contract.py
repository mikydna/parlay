from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from prop_ev.normalize import normalize_event_odds, normalize_featured_odds
from prop_ev.quote_table import (
    EVENT_PROPS_COLUMNS,
    EVENT_PROPS_SORT_COLUMNS,
    FEATURED_ODDS_COLUMNS,
    canonicalize_event_props_rows,
    validate_event_props_rows,
    validate_featured_odds_rows,
)
from prop_ev.snapshot_artifacts import lake_snapshot_derived
from prop_ev.storage import SnapshotStore


def test_normalize_event_odds_follows_quote_table_contract() -> None:
    payload = json.loads(Path("tests/fixtures/event_sample.json").read_text(encoding="utf-8"))
    rows = normalize_event_odds(payload, snapshot_id="snap-1", provider="odds_api")

    assert rows
    for row in rows:
        assert tuple(row.keys()) == EVENT_PROPS_COLUMNS
    validate_event_props_rows(rows)


def test_normalize_featured_odds_follows_quote_table_contract() -> None:
    payload = json.loads(Path("tests/fixtures/featured_sample.json").read_text(encoding="utf-8"))
    rows = normalize_featured_odds(payload, snapshot_id="snap-1", provider="odds_api")

    assert rows
    for row in rows:
        assert tuple(row.keys()) == FEATURED_ODDS_COLUMNS
    validate_featured_odds_rows(rows)


def test_snapshot_lake_event_props_parquet_matches_canonical_jsonl(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path / "data" / "odds_api")
    snapshot_id = "2026-02-13T12-00-00Z"
    snapshot_dir = store.ensure_snapshot(snapshot_id)

    raw_rows = [
        {
            "provider": "odds_api",
            "snapshot_id": snapshot_id,
            "schema_version": 1,
            "event_id": "event-2",
            "market": "player_points",
            "player": "Player B",
            "side": "Under",
            "price": "-115",
            "point": "21.5",
            "book": "fanduel",
            "last_update": "2026-02-13T11:59:00Z",
            "link": "",
        },
        {
            "provider": "odds_api",
            "snapshot_id": snapshot_id,
            "schema_version": 1,
            "event_id": "event-1",
            "market": "player_points",
            "player": "Player A",
            "side": "Over",
            "price": -105,
            "point": 20.5,
            "book": "draftkings",
            "last_update": "2026-02-13T11:58:00Z",
            "link": "",
        },
    ]
    store.write_jsonl(store.derived_path(snapshot_id, "event_props.jsonl"), raw_rows)

    written = lake_snapshot_derived(snapshot_dir)
    assert any(path.name == "event_props.parquet" for path in written)

    parquet_path = snapshot_dir / "derived" / "event_props.parquet"
    frame = pl.read_parquet(parquet_path)
    assert tuple(frame.columns) == EVENT_PROPS_COLUMNS
    assert frame.height == len(raw_rows)

    expected = pl.DataFrame(canonicalize_event_props_rows(raw_rows)).sort(
        list(EVENT_PROPS_SORT_COLUMNS)
    )
    actual = frame.sort(list(EVENT_PROPS_SORT_COLUMNS))
    assert actual.to_dicts() == expected.to_dicts()
