from __future__ import annotations

import json
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.odds_data.day_index import save_dataset_spec, save_day_status, snapshot_id_for_day
from prop_ev.odds_data.spec import DatasetSpec, dataset_id
from prop_ev.storage import SnapshotStore


def _complete_status(*, day: str, snapshot_id: str) -> dict[str, object]:
    return {
        "day": day,
        "complete": True,
        "missing_count": 0,
        "total_events": 1,
        "present_event_odds": 1,
        "snapshot_id_for_day": snapshot_id,
        "note": "",
        "error": "",
    }


def _incomplete_status(*, day: str, snapshot_id: str) -> dict[str, object]:
    return {
        "day": day,
        "complete": False,
        "missing_count": 0,
        "total_events": 0,
        "present_event_odds": 0,
        "snapshot_id_for_day": snapshot_id,
        "note": "missing events list response",
        "error": "",
    }


def _build_spec() -> DatasetSpec:
    return DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
    )


def test_data_repair_derived_repairs_canonical_jsonl_and_parquet(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    spec = _build_spec()
    save_dataset_spec(data_root, spec)

    day = "2026-02-10"
    snapshot_id = snapshot_id_for_day(spec, day)
    store = SnapshotStore(data_root)
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    store.write_jsonl(
        snapshot_dir / "derived" / "event_props.jsonl",
        [
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
                "book": "draftkings",
                "last_update": "2026-02-10T18:00:00Z",
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
                "last_update": "2026-02-10T18:00:00Z",
                "link": "",
            },
        ],
    )
    save_day_status(data_root, spec, day, _complete_status(day=day, snapshot_id=snapshot_id))

    code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "repair-derived",
            "--dataset-id",
            dataset_id(spec),
            "--from",
            day,
            "--to",
            day,
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["issue_count"] == 0
    assert payload["repaired_days"] == 1
    assert payload["days"][0]["jsonl_rewritten"] == 1
    assert payload["days"][0]["parquet_written"] >= 1

    verify_code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "verify",
            "--dataset-id",
            dataset_id(spec),
            "--from",
            day,
            "--to",
            day,
            "--require-complete",
            "--require-parquet",
            "--require-canonical-jsonl",
            "--json",
        ]
    )
    assert verify_code == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["issue_count"] == 0


def test_data_repair_derived_skips_incomplete_day(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    spec = _build_spec()
    save_dataset_spec(data_root, spec)

    day = "2026-02-11"
    snapshot_id = snapshot_id_for_day(spec, day)
    save_day_status(data_root, spec, day, _incomplete_status(day=day, snapshot_id=snapshot_id))

    code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "repair-derived",
            "--dataset-id",
            dataset_id(spec),
            "--from",
            day,
            "--to",
            day,
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["issue_count"] == 0
    assert payload["repaired_days"] == 0
    assert payload["skipped_incomplete_days"] == 1
    assert payload["days"][0]["status"] == "skipped_incomplete"
