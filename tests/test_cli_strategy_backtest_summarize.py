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


def _extract_output_path(stdout: str, *, key: str) -> str:
    prefix = f"{key}="
    for line in stdout.splitlines():
        if line.startswith(prefix):
            return line.split("=", 1)[1].strip()
    return ""


def _canonical_scoreboard_payload(payload: dict[str, object]) -> dict[str, object]:
    normalized = dict(payload)
    normalized.pop("generated_at_utc", None)
    normalized.pop("analysis_run_id", None)
    return normalized


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
            "--write-analysis-scoreboard",
            "--analysis-run-id",
            "eval-smoke",
            "--require-power-gate",
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


def test_strategy_backtest_summarize_all_complete_days_fails_when_no_backtest_rows(
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
        ]
    )
    err = capsys.readouterr().err

    assert code == 2
    assert "no backtest rows found for selected complete days/strategies" in err


def test_strategy_backtest_summarize_writes_walk_forward_calibration_map(
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

    _write_backtest_csv(
        snapshot_reports_dir(store, snapshot_day_one) / "backtest-results-template.s008.csv",
        [
            {
                "snapshot_id": snapshot_day_one,
                "strategy_id": "s008",
                "market": "player_points",
                "recommended_side": "over",
                "selected_price_american": 100,
                "stake_units": 1,
                "model_p_hit": 0.55,
                "result": "win",
            }
        ],
    )
    _write_backtest_csv(
        snapshot_reports_dir(store, snapshot_day_two) / "backtest-results-template.s008.csv",
        [
            {
                "snapshot_id": snapshot_day_two,
                "strategy_id": "s008",
                "market": "player_points",
                "recommended_side": "over",
                "selected_price_american": 100,
                "stake_units": 1,
                "model_p_hit": 0.56,
                "result": "loss",
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
            "--write-calibration-map",
            "--calibration-map-mode",
            "walk_forward",
        ]
    )
    out = capsys.readouterr().out

    assert code == 0
    calibration_map_path = ""
    for line in out.splitlines():
        if line.startswith("calibration_map_json="):
            calibration_map_path = line.split("=", 1)[1].strip()
            break
    assert calibration_map_path
    payload = json.loads(Path(calibration_map_path).read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["mode"] == "walk_forward"
    strategy_payload = payload["strategies"]["s008"]
    assert strategy_payload["rows_scored"] == 2
    assert strategy_payload["by_day"]["2026-02-01"]["rows_scored"] == 0
    assert strategy_payload["by_day"]["2026-02-02"]["rows_scored"] == 1


def test_strategy_backtest_summarize_writes_power_guidance_for_complete_days(
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

    for snapshot_id, baseline_result, candidate_result in [
        (snapshot_day_one, "win", "win"),
        (snapshot_day_two, "loss", "win"),
    ]:
        reports_dir = snapshot_reports_dir(store, snapshot_id)
        _write_backtest_csv(
            reports_dir / "backtest-results-template.s007.csv",
            [
                {
                    "snapshot_id": snapshot_id,
                    "strategy_id": "s007",
                    "market": "player_points",
                    "recommended_side": "over",
                    "selected_price_american": 100,
                    "stake_units": 1,
                    "result": baseline_result,
                }
            ],
        )
        _write_backtest_csv(
            reports_dir / "backtest-results-template.s010.csv",
            [
                {
                    "snapshot_id": snapshot_id,
                    "strategy_id": "s010",
                    "market": "player_points",
                    "recommended_side": "over",
                    "selected_price_american": 100,
                    "stake_units": 1,
                    "result": candidate_result,
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
            "s007,s010",
            "--all-complete-days",
            "--dataset-id",
            dataset_id(spec),
            "--min-graded",
            "1",
            "--write-analysis-scoreboard",
            "--analysis-run-id",
            "eval-smoke",
            "--require-power-gate",
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
    power_guidance = payload.get("power_guidance", {})
    assert power_guidance["baseline_strategy_id"] == "s007"
    strategy_rows = power_guidance.get("strategies", [])
    assert len(strategy_rows) == 1
    strategy_row = strategy_rows[0]
    assert strategy_row["strategy_id"] == "s010"
    assert strategy_row["overlap_days"] == 2
    assert strategy_row["required_days_by_target"]
    analysis_path = ""
    for line in out.splitlines():
        if line.startswith("analysis_scoreboard_json="):
            analysis_path = line.split("=", 1)[1].strip()
            break
    assert analysis_path
    analysis_payload = json.loads(Path(analysis_path).read_text(encoding="utf-8"))
    assert analysis_payload["report_kind"] == "aggregate_scoreboard"
    assert analysis_payload["analysis_run_id"] == "eval-smoke"
    by_strategy = {row["strategy_id"]: row for row in analysis_payload["strategies"]}
    assert by_strategy["s007"]["power_gate"]["status"] == "baseline"
    assert by_strategy["s010"]["power_gate"]["target_roi_uplift_per_bet"] == 0.02
    assert "underpowered_for_target_uplift" in by_strategy["s010"]["promotion_gate"]["reasons"]


def test_strategy_backtest_summarize_analysis_scoreboard_is_deterministic(
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

    run_ids = ["eval-deterministic-a", "eval-deterministic-b"]
    analysis_payloads: list[dict[str, object]] = []
    for run_id in run_ids:
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
                "--write-analysis-scoreboard",
                "--analysis-run-id",
                run_id,
            ]
        )
        out = capsys.readouterr().out
        assert code == 0
        analysis_path = _extract_output_path(out, key="analysis_scoreboard_json")
        assert analysis_path
        analysis_payloads.append(json.loads(Path(analysis_path).read_text(encoding="utf-8")))

    assert _canonical_scoreboard_payload(analysis_payloads[0]) == _canonical_scoreboard_payload(
        analysis_payloads[1]
    )
