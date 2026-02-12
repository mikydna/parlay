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


def test_playbook_run_offline_uses_latest_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")

    store = SnapshotStore(data_dir)
    offline_snapshot = "2026-02-11T10-00-00Z"
    _write_props_snapshot(store, offline_snapshot)

    called: dict[str, str] = {}

    def fake_strategy(**kwargs) -> int:
        called["strategy_snapshot"] = kwargs["snapshot_id"]
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

    monkeypatch.setattr(cli, "_run_strategy_for_playbook", fake_strategy)
    monkeypatch.setattr(cli, "generate_brief_for_snapshot", fake_generate)

    code = main(["playbook", "run", "--offline", "--month", "2026-02"])
    out = capsys.readouterr().out

    assert code == 0
    assert called["strategy_snapshot"] == offline_snapshot
    assert "mode=offline_forced_latest" in out


def test_playbook_run_live_mode_creates_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")

    store = SnapshotStore(data_dir)

    class FakeOddsClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def list_events(self, **kwargs):
            now = datetime.now(UTC).replace(microsecond=0)
            return SimpleNamespace(
                data=[{"id": "event-1", "commence_time": now.isoformat().replace("+00:00", "Z")}]
            )

    created: dict[str, str] = {}

    def fake_snapshot(args, snapshot_id: str) -> int:
        created["snapshot"] = snapshot_id
        _write_props_snapshot(store, snapshot_id)
        return 0

    def fake_strategy(**kwargs) -> int:
        created["strategy_snapshot"] = kwargs["snapshot_id"]
        return 0

    def fake_generate(**kwargs):
        created["brief_snapshot"] = kwargs["snapshot_id"]
        return {
            "report_markdown": "x.md",
            "report_tex": "x.tex",
            "report_pdf": "x.pdf",
            "report_meta": "x.meta.json",
            "llm_pass1_status": "ok",
            "llm_pass2_status": "ok",
            "pdf_status": "missing_tool",
        }

    monkeypatch.setattr(cli, "OddsAPIClient", FakeOddsClient)
    monkeypatch.setattr(cli, "_run_snapshot_bundle_for_playbook", fake_snapshot)
    monkeypatch.setattr(cli, "_run_strategy_for_playbook", fake_strategy)
    monkeypatch.setattr(cli, "generate_brief_for_snapshot", fake_generate)

    code = main(["playbook", "run", "--month", "2026-02"])
    out = capsys.readouterr().out

    assert code == 0
    assert created["snapshot"]
    assert created["strategy_snapshot"] == created["snapshot"]
    assert created["brief_snapshot"] == created["snapshot"]
    assert "mode=live_snapshot" in out


def test_playbook_run_fails_when_live_snapshot_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")

    class FakeOddsClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def list_events(self, **kwargs):
            now = datetime.now(UTC).replace(microsecond=0)
            return SimpleNamespace(
                data=[{"id": "event-1", "commence_time": now.isoformat().replace("+00:00", "Z")}]
            )

    monkeypatch.setattr(cli, "OddsAPIClient", FakeOddsClient)
    monkeypatch.setattr(cli, "_run_snapshot_bundle_for_playbook", lambda args, snapshot_id: 2)

    code = main(["playbook", "run", "--month", "2026-02"])
    err = capsys.readouterr().err

    assert code == 2
    assert "live snapshot failed" in err


def test_playbook_run_block_paid_uses_latest_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")

    store = SnapshotStore(data_dir)
    offline_snapshot = "2026-02-11T10-00-00Z"
    _write_props_snapshot(store, offline_snapshot)

    class FakeOddsClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def list_events(self, **kwargs):
            now = datetime.now(UTC).replace(microsecond=0)
            return SimpleNamespace(
                data=[{"id": "event-1", "commence_time": now.isoformat().replace("+00:00", "Z")}]
            )

    called: dict[str, str] = {}

    def fake_strategy(**kwargs) -> int:
        called["strategy_snapshot"] = kwargs["snapshot_id"]
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
    assert called["strategy_snapshot"] == offline_snapshot
    assert "mode=offline_paid_block" in out


def test_playbook_run_context_preflight_official_missing_hard_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")

    store = SnapshotStore(data_dir)
    offline_snapshot = "2026-02-11T10-00-00Z"
    _write_props_snapshot(store, offline_snapshot)

    class FakeOddsClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def list_events(self, **kwargs):
            now = datetime.now(UTC).replace(microsecond=0)
            return SimpleNamespace(
                data=[{"id": "event-1", "commence_time": now.isoformat().replace("+00:00", "Z")}]
            )

    called: dict[str, str] = {}

    def fake_strategy(**kwargs) -> int:
        called["strategy_snapshot"] = kwargs["snapshot_id"]
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

    monkeypatch.setattr(cli, "OddsAPIClient", FakeOddsClient)
    monkeypatch.setattr(
        cli,
        "_preflight_context_for_snapshot",
        lambda **kwargs: {"health_gates": ["official_injury_missing"]},
    )
    monkeypatch.setattr(
        cli,
        "_run_snapshot_bundle_for_playbook",
        lambda args, snapshot_id: (_ for _ in ()).throw(AssertionError("should not fetch odds")),
    )
    monkeypatch.setattr(cli, "_run_strategy_for_playbook", fake_strategy)
    monkeypatch.setattr(cli, "generate_brief_for_snapshot", fake_generate)

    code = main(["playbook", "run", "--month", "2026-02"])
    err = capsys.readouterr().err

    assert code == 2
    assert "official injury report unavailable" in err
    assert "strategy_snapshot" not in called


def test_playbook_run_context_preflight_allows_secondary_with_explicit_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")

    store = SnapshotStore(data_dir)
    offline_snapshot = "2026-02-11T10-00-00Z"
    _write_props_snapshot(store, offline_snapshot)

    class FakeOddsClient:
        def __init__(self, settings) -> None:
            self.settings = settings

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def list_events(self, **kwargs):
            now = datetime.now(UTC).replace(microsecond=0)
            return SimpleNamespace(
                data=[{"id": "event-1", "commence_time": now.isoformat().replace("+00:00", "Z")}]
            )

    called: dict[str, str] = {}

    def fake_strategy(**kwargs) -> int:
        called["strategy_snapshot"] = kwargs["snapshot_id"]
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

    monkeypatch.setattr(cli, "OddsAPIClient", FakeOddsClient)
    monkeypatch.setattr(
        cli,
        "_preflight_context_for_snapshot",
        lambda **kwargs: {"health_gates": ["official_injury_missing"]},
    )
    monkeypatch.setattr(
        cli,
        "_run_snapshot_bundle_for_playbook",
        lambda args, snapshot_id: (_ for _ in ()).throw(AssertionError("should not fetch odds")),
    )
    monkeypatch.setattr(cli, "_run_strategy_for_playbook", fake_strategy)
    monkeypatch.setattr(cli, "generate_brief_for_snapshot", fake_generate)

    code = main(["playbook", "run", "--month", "2026-02", "--allow-secondary-injuries"])
    out = capsys.readouterr().out

    assert code == 0
    assert called["strategy_snapshot"] == offline_snapshot
    assert "mode=offline_context_gate" in out
    assert "context_preflight_gates=official_injury_missing" in out
