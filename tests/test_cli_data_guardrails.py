from __future__ import annotations

import json
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.storage import SnapshotStore


def test_data_guardrails_command_reports_ok(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    store.ensure_snapshot("snap-1")

    code = main(["--data-dir", str(data_root), "data", "guardrails", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["violation_count"] == 0


def test_data_guardrails_command_fails_on_violations(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data" / "odds_api"
    store = SnapshotStore(data_root)
    snapshot_dir = store.ensure_snapshot("snap-1")
    (snapshot_dir / "context").mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "context" / "injuries.json").write_text("{}\n", encoding="utf-8")

    code = main(["--data-dir", str(data_root), "data", "guardrails", "--json"])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "violations"
    assert int(payload["violation_count"]) >= 1
