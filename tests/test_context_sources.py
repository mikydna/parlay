from pathlib import Path

import prop_ev.context_sources as context_sources
from prop_ev.context_sources import (
    _parse_injury_status,
    _parse_official_injury_text,
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
    html = (
        "<html><body>"
        '<a href="/reports/Injury-Report_2026-01-25_01AM.pdf">Injury Report</a>'
        '<a href="/reports/Injury-Report_2026-01-25_06PM.pdf">Injury Report</a>'
        "</body></html>"
    )

    class _Resp:
        def __init__(self, *, text: str = "", content: bytes = b"") -> None:
            self.text = text
            self.content = content

    def _fake_get(url: str, *, timeout_s: float = 12.0) -> _Resp:
        del timeout_s
        if url.endswith(".pdf"):
            return _Resp(content=b"%PDF-1.4 fake")
        return _Resp(text=html, content=html.encode("utf-8"))

    def _fake_parse(_: bytes) -> dict[str, object]:
        return {
            "rows": [
                {
                    "player": "Zach LaVine",
                    "player_norm": "zachlavine",
                    "team": "Sacramento Kings",
                    "team_norm": "sacramento kings",
                    "status": "out",
                    "note": "Injury/Illness - Low Back; Soreness",
                    "source": "official_nba_pdf",
                }
            ],
            "rows_count": 1,
            "parse_status": "ok",
            "parse_error": "",
            "parse_coverage": 1.0,
            "parse_status_tokens": 1,
            "parse_orphan_status_tokens": 0,
            "parse_skipped_players": 0,
            "report_generated_at_utc": "2026-01-25T23:15:00Z",
            "parse_extractor": "pdftotext",
        }

    monkeypatch.setattr(context_sources, "_http_get", _fake_get)
    monkeypatch.setattr(context_sources, "_parse_official_injury_pdf", _fake_parse)
    monkeypatch.setattr(context_sources, "OFFICIAL_INJURY_URLS", ["https://example.com/injuries"])

    payload = fetch_official_injury_links(pdf_cache_dir=tmp_path)

    assert payload["status"] == "ok"
    assert payload["pdf_download_status"] == "ok"
    assert payload["selected_pdf_url"].endswith("_06PM.pdf")
    assert payload["pdf_download_url"].endswith("_06PM.pdf")
    assert payload["parse_status"] == "ok"
    assert payload["rows_count"] == 1
    assert payload["rows"][0]["player"] == "Zach LaVine"
    assert payload["pdf_cached_path"]
    assert Path(payload["pdf_cached_path"]).exists()
    assert Path(payload["pdf_latest_path"]).exists()


def test_parse_official_injury_text_structured_rows() -> None:
    text = """
Injury Report: 01/25/26 06:15 PM
Game Date
Game Time
Matchup
Team
Player Name
Current Status
Reason
01/25/2026
03:00 (ET)
SAC@DET
Sacramento Kings
LaVine, Zach
Out
Injury/Illness - Low Back;
Soreness
Murray, Keegan
Out
Injury/Illness - Left Ankle; Sprain
Detroit Pistons
Cunningham, Cade
Available
Injury/Illness - Right Wrist; Injury Management
Denver Nuggets
NOT YET SUBMITTED
"""

    payload = _parse_official_injury_text(text)

    assert payload["parse_status"] == "ok"
    assert payload["rows_count"] == 3
    assert payload["rows"][0]["player"] == "Zach LaVine"
    assert payload["rows"][0]["status"] == "out"
    assert payload["rows"][0]["team_norm"] == "sacramento kings"
    assert payload["rows"][1]["player"] == "Keegan Murray"
    assert payload["rows"][2]["status"] == "available"
    assert "Soreness" in payload["rows"][0]["note"]
