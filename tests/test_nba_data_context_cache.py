from pathlib import Path

from prop_ev.nba_data.context_cache import load_or_fetch_context


def test_load_or_fetch_context_fallback_path(tmp_path: Path) -> None:
    primary = tmp_path / "snapshot" / "injuries.json"
    fallback = tmp_path / "reference" / "injuries.json"
    fallback.parent.mkdir(parents=True, exist_ok=True)
    fallback.write_text(
        '{"status":"ok","fetched_at_utc":"2026-02-11T12:00:00Z","source":"fallback"}',
        encoding="utf-8",
    )

    value = load_or_fetch_context(
        cache_path=primary,
        offline=False,
        refresh=False,
        fetcher=lambda: {"status": "never"},
        fallback_paths=[fallback],
    )
    assert value["status"] == "ok"
    assert value["source"] == "fallback"
    assert primary.exists()


def test_load_or_fetch_context_offline_cache_miss(tmp_path: Path) -> None:
    primary = tmp_path / "snapshot" / "roster.json"
    value = load_or_fetch_context(
        cache_path=primary,
        offline=True,
        refresh=False,
        fetcher=lambda: {"status": "never"},
    )
    assert value["status"] == "missing"
    assert value["offline"] is True
    assert value["stale"] is True
