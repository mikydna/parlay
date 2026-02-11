from pathlib import Path

import prop_ev.context_sources as context_sources
from prop_ev.context_sources import (
    _parse_injury_status,
    canonical_team_name,
    fetch_official_injury_links,
    load_or_fetch_context,
    normalize_person_name,
)


def test_canonical_team_name_aliases() -> None:
    assert canonical_team_name("LA Clippers") == "los angeles clippers"
    assert canonical_team_name("Los Angeles Lakers") == "los angeles lakers"
    assert canonical_team_name("BKN") == "brooklyn nets"
    assert canonical_team_name("PHI") == "philadelphia 76ers"


def test_normalize_person_name() -> None:
    assert normalize_person_name("Luka Dončić") == "lukadoncic"
    assert normalize_person_name("D'Angelo Russell") == "dangelorussell"


def test_parse_injury_status_day_to_day_variants() -> None:
    assert _parse_injury_status("Day-To-Day") == "day_to_day"
    assert _parse_injury_status("day to day") == "day_to_day"
    assert _parse_injury_status("OUT") == "out"


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


def test_fetch_official_injury_links_caches_pdf(tmp_path: Path, monkeypatch) -> None:
    html = '<html><body><a href="/reports/injury-report.pdf">Injury Report</a></body></html>'

    class _Resp:
        def __init__(self, *, text: str = "", content: bytes = b"") -> None:
            self.text = text
            self.content = content

    def _fake_get(url: str, *, timeout_s: float = 12.0) -> _Resp:
        del timeout_s
        if url.endswith(".pdf"):
            return _Resp(content=b"%PDF-1.4 fake")
        return _Resp(text=html, content=html.encode("utf-8"))

    monkeypatch.setattr(context_sources, "_http_get", _fake_get)
    monkeypatch.setattr(context_sources, "OFFICIAL_INJURY_URLS", ["https://example.com/injuries"])

    payload = fetch_official_injury_links(pdf_cache_dir=tmp_path)

    assert payload["status"] == "ok"
    assert payload["pdf_download_status"] == "ok"
    assert payload["pdf_cached_path"]
    assert Path(payload["pdf_cached_path"]).exists()
    assert Path(payload["pdf_latest_path"]).exists()
