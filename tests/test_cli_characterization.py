from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import prop_ev.cli as cli
from prop_ev.cli import main
from prop_ev.storage import SnapshotStore


def _write_props_snapshot(store: SnapshotStore, snapshot_id: str) -> None:
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    store.write_jsonl(
        snapshot_dir / "derived" / "event_props.jsonl",
        [
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "side": "Over",
                "price": -105,
                "book": "book_a",
                "link": "",
            }
        ],
    )


def test_main_with_no_args_returns_help_exit_code_zero(capsys) -> None:
    code = main([])
    out = capsys.readouterr().out

    assert code == 0
    assert "usage:" in out


def test_playbook_run_characterizes_default_window_and_output_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")

    store = SnapshotStore(data_dir)
    offline_snapshot = "2026-02-11T10-00-00Z"
    _write_props_snapshot(store, offline_snapshot)

    requested_window: dict[str, str] = {}
    captured: dict[str, str] = {}

    class FakeOddsClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def list_events(self, **kwargs):
            requested_window["commence_from"] = str(kwargs.get("commence_from", ""))
            requested_window["commence_to"] = str(kwargs.get("commence_to", ""))
            now = datetime.now(UTC).replace(microsecond=0)
            return SimpleNamespace(
                data=[{"id": "event-1", "commence_time": now.isoformat().replace("+00:00", "Z")}]
            )

    def fake_strategy(**kwargs) -> int:
        captured["strategy_snapshot"] = kwargs["snapshot_id"]
        return 0

    def fake_generate(**kwargs):
        return {
            "report_markdown": "x.md",
            "report_tex": "x.tex",
            "report_pdf": "x.pdf",
            "report_meta": "x.meta.json",
            "llm_pass1_status": "fallback",
            "llm_pass2_status": "fallback",
            "pdf_status": "missing_tool",
        }

    monkeypatch.setattr(
        cli, "_default_window", lambda: ("2026-02-13T00:00:00Z", "2026-02-14T08:00:00Z")
    )
    monkeypatch.setattr(cli, "OddsAPIClient", FakeOddsClient)
    monkeypatch.setattr(
        cli,
        "_run_snapshot_bundle_for_playbook",
        lambda args, snapshot_id: (_ for _ in ()).throw(AssertionError("should not be called")),
    )
    monkeypatch.setattr(cli, "_run_strategy_for_playbook", fake_strategy)
    monkeypatch.setattr(cli, "generate_brief_for_snapshot", fake_generate)

    code = main(["playbook", "run", "--block-paid", "--month", "2026-02"])
    out = capsys.readouterr().out

    assert code == 0
    assert requested_window["commence_from"] == "2026-02-13T00:00:00Z"
    assert requested_window["commence_to"] == "2026-02-14T08:00:00Z"
    assert captured["strategy_snapshot"] == offline_snapshot
    assert "snapshot_id=" in out
    assert "mode=offline_paid_block" in out
    assert "strategy_id=" in out
