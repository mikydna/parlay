import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from prop_ev.cli import _run_strategy_for_playbook, main
from prop_ev.playbook import report_outputs_root
from prop_ev.report_paths import snapshot_reports_dir
from prop_ev.storage import SnapshotStore


@pytest.fixture
def local_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data" / "odds_api"
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(data_dir))
    return data_dir


def _seed_strategy_snapshot(store: SnapshotStore, snapshot_id: str) -> Path:
    snapshot_dir = store.ensure_snapshot(snapshot_id)
    now_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    context_dir = snapshot_dir / "context"
    context_dir.mkdir(parents=True, exist_ok=True)
    (context_dir / "injuries.json").write_text(
        json.dumps(
            {
                "fetched_at_utc": now_utc,
                "official": {
                    "status": "ok",
                    "parse_status": "ok",
                    "rows_count": 1,
                    "count": 1,
                    "pdf_links": ["https://example.com/injury.pdf"],
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
                "url": "https://example.com/roster",
                "status": "ok",
                "fetched_at_utc": now_utc,
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
                "book": "draftkings",
                "link": "https://example.com/dk-over",
                "last_update": now_utc,
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 22.5,
                "side": "Under",
                "price": -115,
                "book": "draftkings",
                "link": "https://example.com/dk-under",
                "last_update": now_utc,
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 22.5,
                "side": "Over",
                "price": -108,
                "book": "fanduel",
                "link": "https://example.com/fd-over",
                "last_update": now_utc,
            },
            {
                "event_id": "event-1",
                "market": "player_points",
                "player": "Player A",
                "point": 22.5,
                "side": "Under",
                "price": -112,
                "book": "fanduel",
                "link": "https://example.com/fd-under",
                "last_update": now_utc,
            },
        ],
    )
    return snapshot_dir


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
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    assert (reports_dir / "strategy-report.json").exists()
    assert not (reports_dir / "strategy-report.md").exists()
    assert (reports_dir / "backtest-seed.jsonl").exists()
    assert (reports_dir / "backtest-readiness.json").exists()
    assert not (reports_dir / "strategy-report.s001.json").exists()
    assert not (reports_dir / "strategy-report.s001.md").exists()
    assert not (reports_dir / "backtest-seed.s001.jsonl").exists()
    assert not (reports_dir / "backtest-results-template.s001.csv").exists()
    assert not (reports_dir / "backtest-readiness.s001.json").exists()

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
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    assert (reports_dir / "strategy-compare.json").exists()
    assert (reports_dir / "strategy-compare.md").exists()
    assert (reports_dir / "strategy-report.s001.json").exists()
    assert (reports_dir / "strategy-report.s002.json").exists()


def test_strategy_run_replay_mode_adjusts_freshness_config(
    local_data_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T10-12-00Z"
    _seed_strategy_snapshot(store, snapshot_id)

    code = main(
        [
            "strategy",
            "run",
            "--snapshot-id",
            snapshot_id,
            "--offline",
            "--mode",
            "replay",
        ]
    )
    out = capsys.readouterr().out

    assert code == 0
    assert "strategy_run_mode=replay" in out
    assert "stale_quote_minutes=1000000" in out
    assert "require_fresh_context=false" in out


def test_strategy_run_writes_execution_tagged_report(
    local_data_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T10-13-00Z"
    _seed_strategy_snapshot(store, snapshot_id)

    code = main(
        [
            "strategy",
            "run",
            "--snapshot-id",
            snapshot_id,
            "--strategy",
            "s002",
            "--offline",
            "--execution-bookmakers",
            "draftkings",
            "--execution-requires-pre-bet-ready",
            "--execution-requires-meets-play-to",
            "--execution-top-n",
            "5",
        ]
    )
    out = capsys.readouterr().out

    assert code == 0
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    assert (reports_dir / "strategy-report.execution-draftkings.json").exists()
    assert not (reports_dir / "strategy-report.execution-draftkings.md").exists()
    assert not (reports_dir / "strategy-card.execution-draftkings.md").exists()
    assert "execution_bookmakers=draftkings" in out
    assert "execution_tag=execution-draftkings" in out
    assert "execution_report_json=" in out
    assert "execution_report_md=disabled" in out


def test_run_strategy_for_playbook_passes_strategy_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_cmd(args) -> int:
        captured["strategy"] = getattr(args, "strategy", "")
        captured["mode"] = getattr(args, "mode", "")
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
        strategy_mode="replay",
        write_canonical=True,
    )

    assert code == 0
    assert captured["strategy"] == "s002"
    assert captured["mode"] == "replay"
    assert captured["write_canonical"] is True


def test_playbook_render_non_canonical_strategy_report_skips_refresh(
    local_data_dir: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T10-14-00Z"
    store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    reports_dir.mkdir(parents=True, exist_ok=True)
    non_canonical = reports_dir / "strategy-report.execution-draftkings.json"
    non_canonical.write_text("{}\n", encoding="utf-8")

    captured: dict[str, object] = {}

    def _fake_run_strategy(**kwargs) -> int:
        raise AssertionError("strategy rerun should not execute for non-canonical report file")

    def _fake_generate(**kwargs):
        captured["strategy_report_path"] = kwargs.get("strategy_report_path")
        captured["write_markdown"] = kwargs.get("write_markdown")
        return {
            "report_markdown": "",
            "report_tex": "brief.tex",
            "report_pdf": "brief.pdf",
            "report_meta": "brief.meta.json",
        }

    monkeypatch.setattr("prop_ev.cli._run_strategy_for_playbook", _fake_run_strategy)
    monkeypatch.setattr("prop_ev.cli.generate_brief_for_snapshot", _fake_generate)

    code = main(
        [
            "playbook",
            "render",
            "--snapshot-id",
            snapshot_id,
            "--offline",
            "--strategy-report-file",
            "strategy-report.execution-draftkings.json",
        ]
    )
    out = capsys.readouterr().out

    assert code == 0
    assert captured["strategy_report_path"] == non_canonical
    assert captured["write_markdown"] is False
    assert f"strategy_report_path={non_canonical}" in out
    assert "strategy_brief_md=disabled" in out


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


def test_snapshot_verify_check_derived_enforces_required_table_and_parquet(
    local_data_dir: Path, capsys
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T12-22-22Z"
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

    code = main(
        [
            "--data-dir",
            str(local_data_dir),
            "snapshot",
            "verify",
            "--snapshot-id",
            snapshot_id,
            "--check-derived",
            "--require-table",
            "event_props",
            "--require-parquet",
        ]
    )
    assert code == 2
    assert "missing_required_parquet" in capsys.readouterr().out

    assert (
        main(["--data-dir", str(local_data_dir), "snapshot", "lake", "--snapshot-id", snapshot_id])
        == 0
    )
    assert (
        main(
            [
                "--data-dir",
                str(local_data_dir),
                "snapshot",
                "verify",
                "--snapshot-id",
                snapshot_id,
                "--check-derived",
                "--require-table",
                "event_props",
                "--require-parquet",
            ]
        )
        == 0
    )


def test_playbook_publish_copies_only_compact_outputs(local_data_dir: Path) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T13-13-13Z"
    store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "strategy-report.json").write_text("{}\n", encoding="utf-8")
    (reports_dir / "strategy-brief.meta.json").write_text("{}\n", encoding="utf-8")
    (reports_dir / "brief-input.json").write_text("{}\n", encoding="utf-8")

    assert (
        main(
            ["--data-dir", str(local_data_dir), "playbook", "publish", "--snapshot-id", snapshot_id]
        )
        == 0
    )

    reports_root = report_outputs_root(store)
    daily_dir = reports_root / "daily" / "2026-02-11" / f"snapshot={snapshot_id}"
    latest_dir = reports_root / "latest"
    assert (daily_dir / "strategy-report.json").exists()
    assert (daily_dir / "strategy-brief.meta.json").exists()
    assert (daily_dir / "publish.json").exists()
    assert (latest_dir / "strategy-report.json").exists()
    assert (latest_dir / "strategy-brief.meta.json").exists()
    assert (latest_dir / "latest.json").exists()
    assert not (daily_dir / "brief-input.json").exists()
    assert not (daily_dir / "strategy-brief.md").exists()
    assert not (latest_dir / "strategy-brief.md").exists()


def test_playbook_publish_derives_date_for_legacy_daily_snapshot_id(local_data_dir: Path) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "daily-20260212T230052Z"
    store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "strategy-report.json").write_text("{}\n", encoding="utf-8")
    (reports_dir / "strategy-brief.meta.json").write_text("{}\n", encoding="utf-8")

    assert (
        main(
            ["--data-dir", str(local_data_dir), "playbook", "publish", "--snapshot-id", snapshot_id]
        )
        == 0
    )

    reports_root = report_outputs_root(store)
    expected_daily_dir = reports_root / "daily" / "2026-02-12" / f"snapshot={snapshot_id}"
    assert (expected_daily_dir / "strategy-report.json").exists()


def test_global_reports_dir_override_from_subcommand_position(
    local_data_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = SnapshotStore(local_data_dir)
    snapshot_id = "2026-02-11T14-14-14Z"
    store.ensure_snapshot(snapshot_id)
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "strategy-report.json").write_text("{}\n", encoding="utf-8")
    (reports_dir / "strategy-brief.md").write_text("# Brief\n", encoding="utf-8")
    (reports_dir / "strategy-brief.meta.json").write_text("{}\n", encoding="utf-8")

    override_root = tmp_path / "custom-reports"
    assert (
        main(
            [
                "--data-dir",
                str(local_data_dir),
                "playbook",
                "publish",
                "--snapshot-id",
                snapshot_id,
                "--reports-dir",
                str(override_root),
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert f"daily_dir={override_root / 'daily' / '2026-02-11' / f'snapshot={snapshot_id}'}" in out
    assert (override_root / "latest" / "strategy-report.json").exists()
    assert (override_root / "latest" / "strategy-brief.meta.json").exists()
