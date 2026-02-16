from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from prop_ev.cli import main
from prop_ev.odds_data.day_index import save_dataset_spec, save_day_status
from prop_ev.odds_data.spec import DatasetSpec, dataset_id


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(
        json.dumps(row, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n"
        for row in rows
    )
    path.write_text(payload, encoding="utf-8")


def _status_payload(
    *,
    day: str,
    snapshot_id: str,
    complete: bool,
    missing_count: int,
    total_events: int,
) -> dict[str, Any]:
    return {
        "day": day,
        "complete": complete,
        "missing_count": missing_count,
        "total_events": total_events,
        "snapshot_id_for_day": snapshot_id,
        "reason_codes": ["complete"] if complete else ["missing_event_odds"],
        "odds_coverage_ratio": 1.0 if complete else 0.0,
        "events_timestamp": "2026-02-12T17:00:00Z",
        "updated_at_utc": "2026-02-13T20:03:01Z",
    }


def _write_snapshot(
    *,
    data_root: Path,
    snapshot_id: str,
    event_id: str,
    include_event_request: bool,
    link_value: str,
) -> None:
    snapshot_dir = data_root / "snapshots" / snapshot_id
    event_props_rows = [
        {
            "provider": "odds_api",
            "snapshot_id": snapshot_id,
            "schema_version": 1,
            "event_id": event_id,
            "market": "player_points",
            "player": "A.J. Green",
            "side": "Over",
            "price": -116.0,
            "point": 10.5,
            "book": "draftkings",
            "last_update": "2026-02-12T23:35:51Z",
            "link": link_value,
        },
        {
            "provider": "odds_api",
            "snapshot_id": snapshot_id,
            "schema_version": 1,
            "event_id": event_id,
            "market": "player_points",
            "player": "A.J. Green",
            "side": "Under",
            "price": -110.0,
            "point": 10.5,
            "book": "draftkings",
            "last_update": "2026-02-12T23:35:51Z",
            "link": link_value,
        },
    ]
    _write_jsonl(snapshot_dir / "derived" / "event_props.jsonl", event_props_rows)

    requests: dict[str, Any] = {
        "rk-events": {
            "label": "events_list",
            "path": "/historical/sports/basketball_nba/events",
            "params": {
                "date": "2026-02-12T17:00:00Z",
                "dateFormat": "iso",
            },
            "status": "cached",
            "updated_at_utc": "2026-02-13T20:02:59Z",
            "error": "",
        }
    }
    if include_event_request:
        requests["rk-event-1"] = {
            "label": f"event_odds:{event_id}",
            "path": f"/historical/sports/basketball_nba/events/{event_id}/odds",
            "params": {
                "bookmakers": "draftkings",
                "date": "2026-02-12T23:40:00Z",
                "dateFormat": "iso",
                "markets": "player_points",
                "oddsFormat": "american",
            },
            "status": "ok",
            "updated_at_utc": "2026-02-13T20:03:00Z",
            "error": "",
        }

    _write_json(
        snapshot_dir / "manifest.json",
        {
            "schema_version": 1,
            "snapshot_id": snapshot_id,
            "created_at_utc": "2026-02-13T20:02:30Z",
            "client_version": "0.1.0",
            "git_sha": "",
            "quota": {},
            "run_config": {},
            "requests": requests,
        },
    )


def test_data_export_denorm_writes_split_tables(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=True,
        include_sids=False,
        historical=True,
        historical_anchor_hour_local=12,
        historical_pre_tip_minutes=60,
    )
    save_dataset_spec(data_root, spec)
    day = "2026-02-12"
    snapshot_id = "day-4f9a1a9c-2026-02-12"
    save_day_status(
        data_root,
        spec,
        day,
        _status_payload(
            day=day,
            snapshot_id=snapshot_id,
            complete=True,
            missing_count=0,
            total_events=1,
        ),
    )
    _write_snapshot(
        data_root=data_root,
        snapshot_id=snapshot_id,
        event_id="1c3809071b6a6e313c87dee15e46bdc1",
        include_event_request=True,
        link_value="https://sportsbook.example/bet/abc123",
    )

    code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "export-denorm",
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
    out_root = Path(str(payload["output_root"]))
    assert payload["exported_days"] == 1
    assert payload["warning_count"] == 0

    fact_path = out_root / "fact_outcomes" / f"day={day}" / "part-00000.parquet"
    fact_frame = pl.read_parquet(fact_path)
    assert fact_frame.columns == [
        "dataset_id",
        "day",
        "snapshot_id",
        "event_id",
        "request_key",
        "market",
        "player",
        "side",
        "point",
        "book",
        "price",
        "last_update",
        "link",
    ]
    assert fact_frame.height == 2
    assert set(fact_frame.get_column("request_key").to_list()) == {"rk-event-1"}
    assert all(
        str(value).startswith("https://sportsbook.example/")
        for value in fact_frame.get_column("link").to_list()
    )

    request_path = out_root / "dim_request" / f"day={day}" / "part-00000.parquet"
    request_frame = pl.read_parquet(request_path)
    assert request_frame.height == 2
    assert set(request_frame.get_column("request_key").to_list()) == {"rk-events", "rk-event-1"}
    by_key = {
        str(row["request_key"]): row
        for row in request_frame.to_dicts()
        if isinstance(row.get("request_key"), str)
    }
    events_row = by_key["rk-events"]
    assert events_row["param_bookmakers"] == ""
    assert events_row["param_regions"] == ""
    assert events_row["param_markets"] == ""
    assert events_row["param_odds_format"] == ""

    day_status_path = out_root / "dim_day_status" / f"day={day}" / "part-00000.parquet"
    day_status_frame = pl.read_parquet(day_status_path)
    assert day_status_frame.height == 1
    assert day_status_frame.row(0, named=True)["day_complete"] is True


def test_data_export_denorm_skips_incomplete_days(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
        historical=True,
        historical_anchor_hour_local=12,
        historical_pre_tip_minutes=60,
    )
    save_dataset_spec(data_root, spec)
    complete_day = "2026-02-12"
    incomplete_day = "2026-02-13"
    complete_snapshot_id = "day-4f9a1a9c-2026-02-12"
    save_day_status(
        data_root,
        spec,
        complete_day,
        _status_payload(
            day=complete_day,
            snapshot_id=complete_snapshot_id,
            complete=True,
            missing_count=0,
            total_events=1,
        ),
    )
    save_day_status(
        data_root,
        spec,
        incomplete_day,
        _status_payload(
            day=incomplete_day,
            snapshot_id="day-4f9a1a9c-2026-02-13",
            complete=False,
            missing_count=1,
            total_events=2,
        ),
    )
    _write_snapshot(
        data_root=data_root,
        snapshot_id=complete_snapshot_id,
        event_id="1c3809071b6a6e313c87dee15e46bdc1",
        include_event_request=True,
        link_value="",
    )

    code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "export-denorm",
            "--dataset-id",
            dataset_id(spec),
            "--from",
            complete_day,
            "--to",
            incomplete_day,
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    out_root = Path(str(payload["output_root"]))
    assert payload["selected_days"] == 2
    assert payload["exported_days"] == 1
    assert payload["skipped_incomplete_days"] == 1
    assert not (out_root / "fact_outcomes" / f"day={incomplete_day}").exists()


def test_data_export_denorm_allows_missing_event_request_join(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=True,
        include_sids=False,
        historical=True,
        historical_anchor_hour_local=12,
        historical_pre_tip_minutes=60,
    )
    save_dataset_spec(data_root, spec)
    day = "2026-02-12"
    snapshot_id = "day-4f9a1a9c-2026-02-12"
    save_day_status(
        data_root,
        spec,
        day,
        _status_payload(
            day=day,
            snapshot_id=snapshot_id,
            complete=True,
            missing_count=0,
            total_events=1,
        ),
    )
    _write_snapshot(
        data_root=data_root,
        snapshot_id=snapshot_id,
        event_id="1c3809071b6a6e313c87dee15e46bdc1",
        include_event_request=False,
        link_value="https://sportsbook.example/bet/abc123",
    )

    code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "export-denorm",
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
    out_root = Path(str(payload["output_root"]))

    fact_path = out_root / "fact_outcomes" / f"day={day}" / "part-00000.parquet"
    fact_frame = pl.read_parquet(fact_path)
    assert fact_frame.get_column("request_key").null_count() == fact_frame.height

    request_path = out_root / "dim_request" / f"day={day}" / "part-00000.parquet"
    request_frame = pl.read_parquet(request_path)
    assert request_frame.height == 1
    assert request_frame.row(0, named=True)["request_key"] == "rk-events"


def test_data_export_denorm_overwrite_guard(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
        historical=True,
        historical_anchor_hour_local=12,
        historical_pre_tip_minutes=60,
    )
    save_dataset_spec(data_root, spec)
    day = "2026-02-12"
    snapshot_id = "day-4f9a1a9c-2026-02-12"
    save_day_status(
        data_root,
        spec,
        day,
        _status_payload(
            day=day,
            snapshot_id=snapshot_id,
            complete=True,
            missing_count=0,
            total_events=1,
        ),
    )
    _write_snapshot(
        data_root=data_root,
        snapshot_id=snapshot_id,
        event_id="1c3809071b6a6e313c87dee15e46bdc1",
        include_event_request=True,
        link_value="",
    )

    first_code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "export-denorm",
            "--dataset-id",
            dataset_id(spec),
            "--from",
            day,
            "--to",
            day,
        ]
    )
    assert first_code == 0
    capsys.readouterr()

    second_code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "export-denorm",
            "--dataset-id",
            dataset_id(spec),
            "--from",
            day,
            "--to",
            day,
        ]
    )
    captured = capsys.readouterr()
    assert second_code == 2
    assert "output partition exists" in captured.err


def test_data_export_denorm_skips_invalid_jsonl_day_before_conflict_checks(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    out_root = tmp_path / "exports" / "odds" / "export_denorm" / "test"
    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
        historical=True,
        historical_anchor_hour_local=12,
        historical_pre_tip_minutes=60,
    )
    save_dataset_spec(data_root, spec)

    valid_day = "2026-02-12"
    invalid_day = "2026-02-13"
    valid_snapshot_id = "day-4f9a1a9c-2026-02-12"
    invalid_snapshot_id = "day-4f9a1a9c-2026-02-13"
    save_day_status(
        data_root,
        spec,
        valid_day,
        _status_payload(
            day=valid_day,
            snapshot_id=valid_snapshot_id,
            complete=True,
            missing_count=0,
            total_events=1,
        ),
    )
    save_day_status(
        data_root,
        spec,
        invalid_day,
        _status_payload(
            day=invalid_day,
            snapshot_id=invalid_snapshot_id,
            complete=True,
            missing_count=0,
            total_events=1,
        ),
    )
    _write_snapshot(
        data_root=data_root,
        snapshot_id=valid_snapshot_id,
        event_id="1c3809071b6a6e313c87dee15e46bdc1",
        include_event_request=True,
        link_value="",
    )
    _write_snapshot(
        data_root=data_root,
        snapshot_id=invalid_snapshot_id,
        event_id="ad2db051928cbc4c1ed6a3b861ac4538",
        include_event_request=True,
        link_value="",
    )
    (data_root / "snapshots" / invalid_snapshot_id / "derived" / "event_props.jsonl").write_text(
        "{broken jsonl\n",
        encoding="utf-8",
    )

    existing_partition = out_root / "fact_outcomes" / f"day={invalid_day}"
    existing_partition.mkdir(parents=True, exist_ok=True)
    (existing_partition / "part-00000.parquet").write_text("placeholder", encoding="utf-8")

    code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "export-denorm",
            "--dataset-id",
            dataset_id(spec),
            "--from",
            valid_day,
            "--to",
            invalid_day,
            "--out",
            str(out_root),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["exported_days"] == 1
    assert payload["skipped_missing_event_props_days"] == 1
    assert payload["warning_count"] == 1
    warnings = payload.get("warnings", [])
    assert isinstance(warnings, list)
    warning_codes = [
        str(item.get("code", ""))
        for item in warnings
        if isinstance(item, dict) and str(item.get("code", "")).strip()
    ]
    assert warning_codes == ["invalid_event_props_jsonl"]
