import json
from pathlib import Path

import pytest

from prop_ev.cli import main
from prop_ev.storage import SnapshotStore


@pytest.fixture
def local_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    return data_dir


def test_snapshot_ls_show_diff(local_data_dir: Path, capsys) -> None:
    store = SnapshotStore(local_data_dir)
    snap_a = "2026-02-11T10-00-00Z"
    snap_b = "2026-02-11T10-05-00Z"
    store.ensure_snapshot(snap_a)
    store.ensure_snapshot(snap_b)
    store.write_jsonl(store.derived_path(snap_a, "event_props.jsonl"), [{"event_id": "1"}])
    store.write_jsonl(
        store.derived_path(snap_b, "event_props.jsonl"),
        [{"event_id": "1"}, {"event_id": "2"}],
    )

    assert main(["snapshot", "ls"]) == 0
    assert snap_a in capsys.readouterr().out

    assert main(["snapshot", "show", "--snapshot-id", snap_a]) == 0
    show_out = capsys.readouterr().out
    parsed = json.loads(show_out)
    assert parsed["snapshot_id"] == snap_a

    assert main(["snapshot", "diff", "--a", snap_a, "--b", snap_b]) == 0
    diff_out = capsys.readouterr().out
    assert "event_props.jsonl" in diff_out

    assert main(["snapshot", "verify", "--snapshot-id", snap_a]) == 0


def test_slate_dry_run_without_api_key(
    local_data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    monkeypatch.delenv("PROP_EV_ODDS_API_KEY", raising=False)

    code = main(["snapshot", "slate", "--dry-run"])
    assert code == 0


def test_strategy_run_from_snapshot(local_data_dir: Path) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T10-10-00Z"
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    store.write_jsonl(
        snapshot_dir / "derived" / "event_props.jsonl",
        [
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 22.5,
                "side": "Over",
                "price": -105,
                "book": "book_a",
                "link": "",
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 22.5,
                "side": "Under",
                "price": -115,
                "book": "book_a",
                "link": "",
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 22.5,
                "side": "Over",
                "price": -102,
                "book": "book_b",
                "link": "",
            },
        ],
    )

    assert (
        main(
            [
                "strategy",
                "run",
                "--snapshot-id",
                snapshot_id,
                "--top-n",
                "5",
                "--offline",
            ]
        )
        == 0
    )
    assert (snapshot_dir / "reports" / "strategy-report.json").exists()
    assert (snapshot_dir / "reports" / "strategy-report.md").exists()
