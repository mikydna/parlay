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


def test_data_status_json_summary_reports_completion_and_reasons(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_root))

    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
    )

    save_day_status(
        data_root,
        spec,
        "2026-02-01",
        _status_payload(day="2026-02-01", complete=True, missing_count=0, total_events=6),
    )
    save_day_status(
        data_root,
        spec,
        "2026-02-02",
        _status_payload(day="2026-02-02", complete=False, missing_count=2, total_events=7),
    )
    save_day_status(
        data_root,
        spec,
        "2026-02-03",
        _status_payload(
            day="2026-02-03",
            complete=False,
            missing_count=10,
            total_events=10,
            error="estimated credits 100 exceed remaining budget 70 for day 2026-02-03",
        ),
    )
    save_day_status(
        data_root,
        spec,
        "2026-02-04",
        _status_payload(
            day="2026-02-04",
            complete=False,
            missing_count=0,
            total_events=0,
            note="missing events list response",
        ),
    )
    save_day_status(
        data_root,
        spec,
        "2026-02-05",
        _status_payload(
            day="2026-02-05",
            complete=False,
            missing_count=1,
            total_events=9,
            error="Client error '404 Not Found' for url 'https://api.the-odds-api.com/v4/...' ",
        ),
    )

    code = main(
        [
            "data",
            "status",
            "--sport-key",
            "basketball_nba",
            "--markets",
            "player_points",
            "--bookmakers",
            "draftkings",
            "--from",
            "2026-02-01",
            "--to",
            "2026-02-05",
            "--json-summary",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["total_days"] == 5
    assert payload["complete_count"] == 1
    assert payload["incomplete_count"] == 4
    assert payload["complete_days"] == ["2026-02-01"]
    assert payload["incomplete_days"] == ["2026-02-02", "2026-02-03", "2026-02-04", "2026-02-05"]
    assert payload["incomplete_reason_counts"] == {
        "budget_exceeded": 1,
        "missing_event_odds": 1,
        "missing_events_list": 1,
        "upstream_404": 1,
    }
    assert payload["incomplete_error_code_counts"] == {
        "budget_exceeded": 1,
        "missing_event_odds": 1,
        "missing_events_list": 1,
        "upstream_404": 1,
    }
    assert payload["missing_events_total"] == 13
    assert payload["minimum_odds_coverage_ratio"] == 0.0
    assert payload["avg_odds_coverage_ratio"] == pytest.approx(0.5206349, rel=1e-6)
    assert payload["generated_at_utc"].endswith("Z")
    days = payload.get("days", [])
    assert isinstance(days, list)
    by_day = {str(item["day"]): item for item in days if isinstance(item, dict)}
    assert by_day["2026-02-03"]["status_code"] == "incomplete_budget_exceeded"
    assert by_day["2026-02-03"]["reason_codes"] == ["budget_exceeded"]
    assert by_day["2026-02-03"]["error_code"] == "budget_exceeded"
    assert by_day["2026-02-01"]["odds_coverage_ratio"] == 1.0


def test_data_status_default_output_lines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_root))

    spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="draftkings",
        include_links=False,
        include_sids=False,
    )
    save_day_status(
        data_root,
        spec,
        "2026-02-11",
        _status_payload(day="2026-02-11", complete=True, missing_count=0, total_events=9),
    )

    code = main(
        [
            "data",
            "status",
            "--sport-key",
            "basketball_nba",
            "--markets",
            "player_points",
            "--bookmakers",
            "draftkings",
            "--from",
            "2026-02-11",
            "--to",
            "2026-02-11",
        ]
    )
    assert code == 0
    output = capsys.readouterr().out
    assert "day=2026-02-11" in output
    assert "complete=true" in output
    assert "missing=0" in output


def test_data_status_with_dataset_id_uses_stored_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_root))

    stored_spec = DatasetSpec(
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
    save_dataset_spec(data_root, stored_spec)
    save_day_status(
        data_root,
        stored_spec,
        "2026-02-12",
        _status_payload(day="2026-02-12", complete=True, missing_count=0, total_events=3),
    )

    code = main(
        [
            "data",
            "status",
            "--dataset-id",
            dataset_id(stored_spec),
            "--sport-key",
            "bad_sport_key_ignored",
            "--markets",
            "player_rebounds",
            "--bookmakers",
            "betmgm",
            "--from",
            "2026-02-12",
            "--to",
            "2026-02-12",
            "--json-summary",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["dataset_id"] == dataset_id(stored_spec)
    assert payload["sport_key"] == "basketball_nba"
    assert payload["markets"] == ["player_points"]
    assert payload["bookmakers"] == "draftkings,fanduel"
    warnings = payload.get("warnings", [])
    assert isinstance(warnings, list)
    assert warnings and warnings[0]["code"] == "dataset_id_override"


def test_data_status_json_summary_warns_for_missing_spec_dataset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_root))

    existing_spec = DatasetSpec(
        sport_key="basketball_nba",
        markets=["player_points"],
        regions="us",
        bookmakers="fanduel",
        include_links=False,
        include_sids=False,
    )
    save_dataset_spec(data_root, existing_spec)
    save_day_status(
        data_root,
        existing_spec,
        "2026-02-11",
        _status_payload(day="2026-02-11", complete=True, missing_count=0, total_events=9),
    )

    code = main(
        [
            "data",
            "status",
            "--sport-key",
            "basketball_nba",
            "--markets",
            "player_points",
            "--bookmakers",
            "draftkings",
            "--from",
            "2026-02-11",
            "--to",
            "2026-02-11",
            "--json-summary",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    warnings = payload.get("warnings", [])
    assert isinstance(warnings, list)
    assert warnings and warnings[0]["code"] == "dataset_not_found_for_spec"
