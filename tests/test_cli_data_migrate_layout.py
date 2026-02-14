from __future__ import annotations

import json
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.report_paths import canonical_report_outputs_root, snapshot_reports_dir
from prop_ev.storage import SnapshotStore


def test_data_migrate_layout_applies_p0_moves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    nba_root = tmp_path / "data" / "nba_data"
    nba_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_root))

    store = SnapshotStore(data_root)
    snapshot_id = "snap-1"
    snapshot_dir = store.ensure_snapshot(snapshot_id)

    legacy_context = snapshot_dir / "context"
    legacy_context.mkdir(parents=True, exist_ok=True)
    (legacy_context / "injuries.json").write_text(
        json.dumps({"status": "ok", "fetched_at_utc": "2026-02-14T00:00:00Z"}) + "\n",
        encoding="utf-8",
    )
    (legacy_context / "roster.json").write_text(
        json.dumps({"status": "ok", "fetched_at_utc": "2026-02-14T00:00:00Z"}) + "\n",
        encoding="utf-8",
    )
    (legacy_context / "results.json").write_text(
        json.dumps({"status": "ok", "fetched_at_utc": "2026-02-14T00:00:00Z"}) + "\n",
        encoding="utf-8",
    )

    legacy_reports = snapshot_dir / "reports"
    legacy_reports.mkdir(parents=True, exist_ok=True)
    (legacy_reports / "strategy-report.json").write_text("{}\n", encoding="utf-8")

    (data_root / "reference").mkdir(parents=True, exist_ok=True)
    (data_root / "reference" / "player_identity_map.json").write_text("{}\n", encoding="utf-8")

    (data_root / "cache" / "requests").mkdir(parents=True, exist_ok=True)
    (data_root / "cache" / "requests" / "key-1.json").write_text("{}\n", encoding="utf-8")

    code = main(["data", "migrate-layout", "--apply", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is False

    canonical_context = nba_root / "context" / "snapshots" / snapshot_id
    assert (canonical_context / "injuries.json").exists()
    assert (canonical_context / "roster.json").exists()
    assert (canonical_context / "results.json").exists()
    assert (snapshot_dir / "context_ref.json").exists()
    assert not (legacy_context / "injuries.json").exists()

    reports_dir = snapshot_reports_dir(
        store,
        snapshot_id,
        reports_root=canonical_report_outputs_root(store),
    )
    assert (reports_dir / "strategy-report.json").exists()
    assert not (legacy_reports / "strategy-report.json").exists()

    assert (nba_root / "reference" / "player_identity_map.json").exists()
    assert (data_root.parent / "runtime" / "odds_cache" / "requests" / "key-1.json").exists()

    guardrails = payload.get("guardrails", {})
    assert isinstance(guardrails, dict)
    assert int(guardrails.get("violation_count", 0)) == 0
