from __future__ import annotations

import json
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.odds_data.day_index import save_dataset_spec, save_day_status
from prop_ev.odds_data.spec import DatasetSpec, dataset_id


def _status_payload(
    *,
    day: str,
    complete: bool,
    missing_count: int,
    total_events: int,
    note: str = "",
    error: str = "",
) -> dict[str, object]:
    return {
        "day": day,
        "complete": complete,
        "missing_count": missing_count,
        "total_events": total_events,
        "snapshot_id_for_day": f"day-test-{day}",
        "note": note,
        "error": error,
    }


def test_data_datasets_ls_json_reports_dataset_summaries(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"

    first_spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings,fanduel",
        include_links=False,
        include_sids=False,
        historical=True,
        historical_anchor_hour_local=12,
        historical_pre_tip_minutes=60,
    )
    second_spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_rebounds"],
        regions="us",
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
    )
    save_dataset_spec(data_root, first_spec)
    save_dataset_spec(data_root, second_spec)

    save_day_status(
        data_root,
        first_spec,
        "2026-02-01",
        _status_payload(day="2026-02-01", complete=True, missing_count=0, total_events=10),
    )
    save_day_status(
        data_root,
        first_spec,
        "2026-02-02",
        _status_payload(
            day="2026-02-02",
            complete=False,
            missing_count=1,
            total_events=8,
            error="Client error '404 Not Found' for url 'https://api.the-odds-api.com/v4/...' ",
        ),
    )
    save_day_status(
        data_root,
        second_spec,
        "2026-02-03",
        _status_payload(day="2026-02-03", complete=True, missing_count=0, total_events=6),
    )

    code = main(["--data-dir", str(data_root), "data", "datasets", "ls", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["dataset_count"] == 2
    rows = {
        str(item["dataset_id"]): item
        for item in payload.get("datasets", [])
        if isinstance(item, dict) and "dataset_id" in item
    }
    first_row = rows[dataset_id(first_spec)]
    assert first_row["day_count"] == 2
    assert first_row["complete_count"] == 1
    assert first_row["incomplete_count"] == 1
    assert first_row["incomplete_reason_counts"] == {"upstream_404": 1}
    assert first_row["incomplete_error_code_counts"] == {"upstream_404": 1}
    assert first_row["avg_odds_coverage_ratio"] == pytest.approx(0.9375, rel=1e-6)
    assert first_row["minimum_odds_coverage_ratio"] == pytest.approx(0.875, rel=1e-6)

    second_row = rows[dataset_id(second_spec)]
    assert second_row["day_count"] == 1
    assert second_row["complete_count"] == 1
    assert second_row["incomplete_count"] == 0
    assert second_row["incomplete_error_code_counts"] == {}
    assert second_row["avg_odds_coverage_ratio"] == 1.0
    assert second_row["minimum_odds_coverage_ratio"] == 1.0


def test_data_datasets_show_json_supports_day_filters(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"

    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings,fanduel",
        include_links=False,
        include_sids=False,
    )
    save_dataset_spec(data_root, spec)
    save_day_status(
        data_root,
        spec,
        "2026-02-01",
        _status_payload(day="2026-02-01", complete=True, missing_count=0, total_events=8),
    )
    save_day_status(
        data_root,
        spec,
        "2026-02-02",
        _status_payload(day="2026-02-02", complete=False, missing_count=2, total_events=8),
    )
    save_day_status(
        data_root,
        spec,
        "2026-02-03",
        _status_payload(day="2026-02-03", complete=True, missing_count=0, total_events=6),
    )

    code = main(
        [
            "--data-dir",
            str(data_root),
            "data",
            "datasets",
            "show",
            "--dataset-id",
            dataset_id(spec),
            "--from",
            "2026-02-01",
            "--to",
            "2026-02-02",
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["dataset_id"] == dataset_id(spec)
    assert payload["available_day_count"] == 3
    assert payload["available_from_day"] == "2026-02-01"
    assert payload["available_to_day"] == "2026-02-03"
    assert payload["from_day"] == "2026-02-01"
    assert payload["to_day"] == "2026-02-02"
    assert payload["total_days"] == 2
    assert payload["complete_count"] == 1
    assert payload["incomplete_count"] == 1
