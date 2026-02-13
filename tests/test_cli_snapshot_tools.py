import json
import shutil
from pathlib import Path

import pytest

from prop_ev.cli import _run_strategy_for_playbook, main
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
    context_dir = snapshot_dir / "context"
    context_dir.mkdir(parents=True, exist_ok=True)
    (context_dir / "injuries.json").write_text(
        json.dumps(
            {
                "fetched_at_utc": "2026-02-11T10:10:00Z",
                "official": {
                    "source": "official_nba",
                    "url": "https://official.nba.com/nba-injury-report-2025-26-season/",
                    "status": "ok",
                    "count": 1,
                    "fetched_at_utc": "2026-02-11T10:10:00Z",
                    "pdf_links": ["https://example.com/injury.pdf"],
                    "pdf_download_status": "ok",
                    "selected_pdf_url": "https://example.com/injury.pdf",
                    "parse_status": "ok",
                    "parse_coverage": 1.0,
                    "rows_count": 1,
                    "rows": [
                        {
                            "player": "Player A",
                            "player_norm": "playera",
                            "team": "Boston Celtics",
                            "team_norm": "boston celtics",
                            "status": "available",
                            "note": "",
                            "source": "official_nba_pdf",
                        }
                    ],
                },
                "secondary": {"status": "ok", "rows": [], "count": 0},
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (context_dir / "roster.json").write_text(
        json.dumps(
            {
                "source": "nba_live_scoreboard",
                "url": "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
                "status": "ok",
                "fetched_at_utc": "2026-02-11T10:10:00Z",
                "count_teams": 0,
                "missing_roster_teams": [],
                "teams": {},
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
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
    assert (snapshot_dir / "reports" / "strategy-report.s001.json").exists()
    assert (snapshot_dir / "reports" / "strategy-report.s001.md").exists()
    assert (snapshot_dir / "reports" / "backtest-seed.jsonl").exists()
    assert (snapshot_dir / "reports" / "backtest-readiness.json").exists()
    assert (snapshot_dir / "reports" / "backtest-seed.s001.jsonl").exists()
    assert (snapshot_dir / "reports" / "backtest-results-template.s001.csv").exists()
    assert (snapshot_dir / "reports" / "backtest-readiness.s001.json").exists()

    assert (
        main(
            [
                "strategy",
                "backtest-prep",
                "--snapshot-id",
                snapshot_id,
                "--selection",
                "ranked",
                "--top-n",
                "1",
            ]
        )
        == 0
    )


def test_strategy_compare_writes_suffixed_outputs(
    local_data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", "false")
    monkeypatch.setenv("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", "false")
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T10-11-00Z"
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
        ],
    )

    assert (
        main(
            [
                "strategy",
                "compare",
                "--snapshot-id",
                snapshot_id,
                "--strategies",
                "s001,s002",
                "--top-n",
                "5",
                "--offline",
            ]
        )
        == 0
    )
    assert (snapshot_dir / "reports" / "strategy-compare.json").exists()
    assert (snapshot_dir / "reports" / "strategy-compare.md").exists()
    assert (snapshot_dir / "reports" / "strategy-report.s001.json").exists()
    assert (snapshot_dir / "reports" / "strategy-report.s002.json").exists()


def test_run_strategy_for_playbook_passes_strategy_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_cmd(args) -> int:
        captured["strategy"] = getattr(args, "strategy", "")
        captured["write_canonical"] = getattr(args, "write_canonical", None)
        return 0

    monkeypatch.setattr("prop_ev.cli._cmd_strategy_run", _fake_cmd)
    code = _run_strategy_for_playbook(
        snapshot_id="snap-1",
        strategy_id="s002",
        top_n=25,
        min_ev=0.01,
        allow_tier_b=False,
        offline=True,
        block_paid=True,
        refresh_context=False,
        write_canonical=True,
    )

    assert code == 0
    assert captured["strategy"] == "s002"
    assert captured["write_canonical"] is True


def test_global_data_dir_override_from_subcommand_position(
    local_data_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(tmp_path / "other" / "odds_api"))
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T11-11-11Z"
    store.ensure_snapshot(snapshot_id)

    assert main(["snapshot", "ls", "--data-dir", str(local_data_dir)]) == 0
    out = capsys.readouterr().out
    assert snapshot_id in out


@pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd binary required")
def test_snapshot_lake_pack_unpack_roundtrip(local_data_dir: Path) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T12-12-12Z"
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    store.write_jsonl(
        snapshot_dir / "derived" / "event_props.jsonl",
        [
            {
                "provider": "odds_api",
                "snapshot_id": snapshot_id,
                "schema_version": 1,
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "side": "Over",
                "price": -110,
                "point": 20.5,
                "book": "book_a",
                "last_update": "2026-02-11T12:10:00Z",
                "link": "",
            }
        ],
    )
    store.write_jsonl(
        snapshot_dir / "derived" / "featured_odds.jsonl",
        [
            {
                "provider": "odds_api",
                "snapshot_id": snapshot_id,
                "schema_version": 1,
                "game_id": "event-1",
                "market": "spreads",
                "book": "book_a",
                "price": -105,
                "point": 3.5,
                "side": "home",
                "last_update": "2026-02-11T12:10:00Z",
            }
        ],
    )

    assert (
        main(["--data-dir", str(local_data_dir), "snapshot", "lake", "--snapshot-id", snapshot_id])
        == 0
    )
    assert (snapshot_dir / "derived" / "event_props.parquet").exists()
    assert (snapshot_dir / "derived" / "featured_odds.parquet").exists()

    bundle_path = local_data_dir / "bundles" / "test-roundtrip.tar.zst"
    assert (
        main(
            [
                "--data-dir",
                str(local_data_dir),
                "snapshot",
                "pack",
                "--snapshot-id",
                snapshot_id,
                "--out",
                str(bundle_path),
            ]
        )
        == 0
    )
    assert bundle_path.exists()
    assert bundle_path.with_name("test-roundtrip.bundle.json").exists()

    shutil.rmtree(snapshot_dir)
    assert (
        main(
            [
                "--data-dir",
                str(local_data_dir),
                "snapshot",
                "unpack",
                "--bundle",
                str(bundle_path),
            ]
        )
        == 0
    )
    assert (
        main(
            ["--data-dir", str(local_data_dir), "snapshot", "verify", "--snapshot-id", snapshot_id]
        )
        == 0
    )


def test_playbook_publish_copies_only_compact_outputs(local_data_dir: Path) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T13-13-13Z"
    reports_dir = store.ensure_snapshot(snapshot_id) / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "strategy-report.json").write_text("{}\n", encoding="utf-8")
    (reports_dir / "strategy-brief.md").write_text("# Brief\n", encoding="utf-8")
    (reports_dir / "strategy-brief.meta.json").write_text("{}\n", encoding="utf-8")
    (reports_dir / "brief-input.json").write_text("{}\n", encoding="utf-8")

    assert (
        main(
            ["--data-dir", str(local_data_dir), "playbook", "publish", "--snapshot-id", snapshot_id]
        )
        == 0
    )

    daily_dir = local_data_dir / "reports" / "daily" / "2026-02-11" / f"snapshot={snapshot_id}"
    latest_dir = local_data_dir / "reports" / "latest"
    assert (daily_dir / "strategy-report.json").exists()
    assert (daily_dir / "strategy-brief.md").exists()
    assert (daily_dir / "strategy-brief.meta.json").exists()
    assert (daily_dir / "publish.json").exists()
    assert (latest_dir / "strategy-report.json").exists()
    assert (latest_dir / "strategy-brief.md").exists()
    assert (latest_dir / "strategy-brief.meta.json").exists()
    assert (latest_dir / "latest.json").exists()
    assert not (daily_dir / "brief-input.json").exists()


def test_playbook_publish_derives_date_for_legacy_daily_snapshot_id(local_data_dir: Path) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "daily-20260212T230052Z"
    reports_dir = store.ensure_snapshot(snapshot_id) / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "strategy-report.json").write_text("{}\n", encoding="utf-8")
    (reports_dir / "strategy-brief.md").write_text("# Brief\n", encoding="utf-8")
    (reports_dir / "strategy-brief.meta.json").write_text("{}\n", encoding="utf-8")

    assert (
        main(
            ["--data-dir", str(local_data_dir), "playbook", "publish", "--snapshot-id", snapshot_id]
        )
        == 0
    )

    expected_daily_dir = (
        local_data_dir / "reports" / "daily" / "2026-02-12" / f"snapshot={snapshot_id}"
    )
    assert (expected_daily_dir / "strategy-report.json").exists()
