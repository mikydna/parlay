from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.odds_data.day_index import save_dataset_spec, save_day_status
from prop_ev.odds_data.spec import DatasetSpec, dataset_id
from prop_ev.report_paths import snapshot_reports_dir
from prop_ev.storage import SnapshotStore


def _write_backtest_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _complete_day_status(*, day: str, snapshot_id_for_day: str) -> dict[str, object]:
    return {
        "day": day,
        "complete": True,
        "missing_count": 0,
        "total_events": 1,
        "snapshot_id_for_day": snapshot_id_for_day,
        "note": "",
        "error": "",
    }


def test_strategy_backtest_summarize_all_complete_days_aggregates_rows(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    spec = DatasetSpec(
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
    save_dataset_spec(data_root, spec)

    snapshot_day_one = "day-a-2026-02-01"
    snapshot_day_two = "day-b-2026-02-02"
    store.ensure_snapshot(snapshot_day_one)
    store.ensure_snapshot(snapshot_day_two)
    save_day_status(
        data_root,
        spec,
        "2026-02-01",
        _complete_day_status(day="2026-02-01", snapshot_id_for_day=snapshot_day_one),
    )
    save_day_status(
        data_root,
        spec,
        "2026-02-02",
        _complete_day_status(day="2026-02-02", snapshot_id_for_day=snapshot_day_two),
    )

    for snapshot_id, result in [(snapshot_day_one, "win"), (snapshot_day_two, "loss")]:
        reports_dir = snapshot_reports_dir(store, snapshot_id)
        _write_backtest_csv(
            reports_dir / "backtest-results-template.s008.csv",
            [
                {
                    "snapshot_id": snapshot_id,
                    "strategy_id": "s008",
                    "market": "player_points",
                    "recommended_side": "over",
                    "selected_price_american": 100,
                    "stake_units": 1,
                    "result": result,
                }
            ],
        )

    code = main(
        [
            "--data-dir",
            str(data_root),
            "strategy",
            "backtest-summarize",
            "--snapshot-id",
            snapshot_day_two,
            "--strategies",
            "s008",
            "--all-complete-days",
            "--dataset-id",
            dataset_id(spec),
            "--min-graded",
            "1",
        ]
    )
    out = capsys.readouterr().out

    assert code == 0
    summary_path = ""
    for line in out.splitlines():
        if line.startswith("summary_json="):
            summary_path = line.split("=", 1)[1].strip()
            break
    assert summary_path
    payload = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    summary = payload["summary"]
    assert summary["all_complete_days"] is True
    assert summary["dataset_id"] == dataset_id(spec)
    assert summary["complete_days"] == 2
    assert summary["days_with_any_results"] == 2
    assert payload["strategies"][0]["rows_graded"] == 2


def test_strategy_backtest_summarize_all_complete_days_requires_dataset_when_ambiguous(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    save_dataset_spec(
        data_root,
        DatasetSpec(
            sport_key="basketball_nba",
            markets=["player_points"],
            regions="us",
            bookmakers="draftkings",
            include_links=False,
            include_sids=False,
        ),
    )
    save_dataset_spec(
        data_root,
        DatasetSpec(
            sport_key="basketball_nba",
            markets=["player_rebounds"],
            regions="us",
            bookmakers="fanduel",
            include_links=False,
            include_sids=False,
        ),
    )
    SnapshotStore(data_root).ensure_snapshot("day-a-2026-02-01")

    code = main(
        [
            "--data-dir",
            str(data_root),
            "strategy",
            "backtest-summarize",
            "--snapshot-id",
            "day-a-2026-02-01",
            "--strategies",
            "s008",
            "--all-complete-days",
        ]
    )
    err = capsys.readouterr().err

    assert code == 2
    assert "multiple datasets found" in err
