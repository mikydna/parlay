"""External context fetchers for injuries and roster verification."""

from __future__ import annotations

import hashlib
import html
import json
import re
import subprocess
import tempfile
import unicodedata
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx

OFFICIAL_INJURY_URLS = [
    "https://official.nba.com/nba-injury-report-2025-26-season/",
    "https://official.nba.com/nba-injury-report/",
    "https://www.nba.com/injury-report",
]
BREF_INJURY_URL = "https://www.basketball-reference.com/friv/injuries.fcgi"
ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
ESPN_TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"
ESPN_TEAM_ROSTER_URL_TEMPLATE = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/roster"
)
TODAYS_SCOREBOARD_URL = (
    "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
)
BOXSCORE_URL_TEMPLATE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

TEAM_NAME_ALIASES = {
    "atl": "atlanta hawks",
    "atlanta": "atlanta hawks",
    "boston": "boston celtics",
    "bos": "boston celtics",
    "brooklyn": "brooklyn nets",
    "bkn": "brooklyn nets",
    "brk": "brooklyn nets",
    "charlotte": "charlotte hornets",
    "cha": "charlotte hornets",
    "cho": "charlotte hornets",
    "chicago": "chicago bulls",
    "chi": "chicago bulls",
    "cle": "cleveland cavaliers",
    "cleveland": "cleveland cavaliers",
    "dallas": "dallas mavericks",
    "dal": "dallas mavericks",
    "den": "denver nuggets",
    "denver": "denver nuggets",
    "det": "detroit pistons",
    "detroit": "detroit pistons",
    "golden state": "golden state warriors",
    "gs": "golden state warriors",
    "gsw": "golden state warriors",
    "hou": "houston rockets",
    "houston": "houston rockets",
    "ind": "indiana pacers",
    "indiana": "indiana pacers",
    "la clippers": "los angeles clippers",
    "lac": "los angeles clippers",
    "los angeles clippers": "los angeles clippers",
    "la lakers": "los angeles lakers",
    "lal": "los angeles lakers",
    "los angeles lakers": "los angeles lakers",
    "mem": "memphis grizzlies",
    "memphis": "memphis grizzlies",
    "mia": "miami heat",
    "miami": "miami heat",
    "mil": "milwaukee bucks",
    "milwaukee": "milwaukee bucks",
    "min": "minnesota timberwolves",
    "minnesota": "minnesota timberwolves",
    "new orleans": "new orleans pelicans",
    "nop": "new orleans pelicans",
    "nor": "new orleans pelicans",
    "new york": "new york knicks",
    "ny": "new york knicks",
    "nyk": "new york knicks",
    "okc": "oklahoma city thunder",
    "oklahoma city": "oklahoma city thunder",
    "orlando": "orlando magic",
    "orl": "orlando magic",
    "phi": "philadelphia 76ers",
    "philadelphia": "philadelphia 76ers",
    "philadelphia sixers": "philadelphia 76ers",
    "phx": "phoenix suns",
    "pho": "phoenix suns",
    "phoenix": "phoenix suns",
    "por": "portland trail blazers",
    "portland": "portland trail blazers",
    "sac": "sacramento kings",
    "sacramento": "sacramento kings",
    "san antonio": "san antonio spurs",
    "sa": "san antonio spurs",
    "sas": "san antonio spurs",
    "tor": "toronto raptors",
    "toronto": "toronto raptors",
    "utah": "utah jazz",
    "uta": "utah jazz",
    "washington": "washington wizards",
    "was": "washington wizards",
}
OFFICIAL_STATUS_TOKENS = {
    "out",
    "doubtful",
    "questionable",
    "probable",
    "available",
}
OFFICIAL_TEAM_LABELS = {
    "atlanta hawks",
    "boston celtics",
    "brooklyn nets",
    "charlotte hornets",
    "chicago bulls",
    "cleveland cavaliers",
    "dallas mavericks",
    "denver nuggets",
    "detroit pistons",
    "golden state warriors",
    "houston rockets",
    "indiana pacers",
    "los angeles clippers",
    "los angeles lakers",
    "memphis grizzlies",
    "miami heat",
    "milwaukee bucks",
    "minnesota timberwolves",
    "new orleans pelicans",
    "new york knicks",
    "oklahoma city thunder",
    "orlando magic",
    "philadelphia 76ers",
    "phoenix suns",
    "portland trail blazers",
    "sacramento kings",
    "san antonio spurs",
    "toronto raptors",
    "utah jazz",
    "washington wizards",
}
OFFICIAL_HEADER_COLUMNS = {
    "game date",
    "game time",
    "matchup",
    "team",
    "player name",
    "current status",
    "reason",
}
_DATE_LINE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_TIME_LINE_RE = re.compile(r"^\d{1,2}:\d{2}\s+\(ET\)$")
_MATCHUP_LINE_RE = re.compile(r"^[A-Z]{2,4}@[A-Z]{2,4}$")
_PLAYER_LINE_RE = re.compile(r"^[A-Za-z0-9'.\- ]+,\s*[A-Za-z0-9'.\- ]+$")
_OFFICIAL_REPORT_URL_RE = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2})[_-](?P<hour>\d{1,2})(?:[-_:]?(?P<minute>\d{2}))?(?P<ampm>AM|PM)",
    flags=re.IGNORECASE,
)
OFFICIAL_ET_ZONE = ZoneInfo("America/New_York")


def now_utc() -> str:
    """Return an ISO UTC timestamp."""
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_team_name(name: str) -> str:
    """Canonicalize team names for matching."""
    normalized = " ".join(name.lower().split())
    return TEAM_NAME_ALIASES.get(normalized, normalized)


def normalize_person_name(name: str) -> str:
    """Normalize person names for fuzzy joins."""
    lowered = name.lower().strip()
    normalized = unicodedata.normalize("NFKD", lowered)
    ascii_only = "".join(ch for ch in normalized if ord(ch) < 128)
    cleaned = re.sub(r"[^a-z0-9]+", "", ascii_only)
    return cleaned


def _http_get(url: str, *, timeout_s: float = 12.0) -> httpx.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; prop-ev/0.1.0)",
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    }
    response = httpx.get(url, headers=headers, timeout=timeout_s, follow_redirects=True)
    response.raise_for_status()
    return response


def _parse_injury_status(note: str) -> str:
    value = note.lower().replace("-", " ")
    if "out for season" in value:
        return "out_for_season"
    if "available" in value:
        return "available"
    if re.search(r"\bout\b", value):
        return "out"
    if "doubtful" in value:
        return "doubtful"
    if "questionable" in value:
        return "questionable"
    if "game time decision" in value:
        return "questionable"
    if "probable" in value:
        return "probable"
    if "day to day" in value:
        return "day_to_day"
    return "unknown"


def _official_pdf_sort_key(pdf_url: str) -> tuple[int, int, int, int, int, str]:
    match = _OFFICIAL_REPORT_URL_RE.search(pdf_url)
    if match is None:
        return (0, 0, 0, 0, 0, pdf_url)
    try:
        year, month, day = [int(value) for value in match.group("date").split("-")]
        hour = int(match.group("hour"))
        minute = int(match.group("minute") or "0")
    except ValueError:
        return (0, 0, 0, 0, 0, pdf_url)
    ampm = match.group("ampm").upper()
    if hour == 12:
        hour = 0
    if ampm == "PM":
        hour += 12
    return (year, month, day, hour, minute, pdf_url)


def _official_player_display_name(raw: str) -> str:
    if "," not in raw:
        return raw.strip()
    last, first = [piece.strip() for piece in raw.split(",", 1)]
    if not first or not last:
        return raw.strip()
    return f"{first} {last}".strip()


def _is_official_header_line(token: str) -> bool:
    lowered = token.strip().lower()
    if not lowered:
        return True
    if lowered in OFFICIAL_HEADER_COLUMNS:
        return True
    if lowered.startswith("injury report:"):
        return True
    if lowered.startswith("page ") and " of " in lowered:
        return True
    if _DATE_LINE_RE.match(token):
        return True
    if _TIME_LINE_RE.match(token):
        return True
    return bool(_MATCHUP_LINE_RE.match(token))


def _is_official_team_line(token: str) -> bool:
    return canonical_team_name(token) in OFFICIAL_TEAM_LABELS


def _is_official_player_line(token: str) -> bool:
    return _PLAYER_LINE_RE.match(token) is not None


def _extract_official_pdf_text(pdf_bytes: bytes) -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        pdf_path = Path(tmp_dir) / "official.pdf"
        pdf_path.write_bytes(pdf_bytes)
        proc = subprocess.run(
            ["pdftotext", "-enc", "UTF-8", str(pdf_path), "-"],
            capture_output=True,
            text=True,
            check=False,
        )
    if proc.returncode != 0:
        stderr = proc.stderr.strip() or f"pdftotext failed with code {proc.returncode}"
        raise RuntimeError(stderr)
    return proc.stdout.replace("\x0c", "\n")


def _parse_report_generated_at(text: str) -> str:
    match = re.search(
        r"Injury Report:\s*(\d{2})/(\d{2})/(\d{2,4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)",
        text,
        flags=re.IGNORECASE,
    )
    if match is None:
        return ""
    month, day, year, hour, minute, ampm = match.groups()
    try:
        month_num = int(month)
        day_num = int(day)
        year_num = int(year)
        hour_num = int(hour)
        minute_num = int(minute)
    except ValueError:
        return ""
    if year_num < 100:
        year_num += 2000
    if hour_num == 12:
        hour_num = 0
    if ampm.upper() == "PM":
        hour_num += 12
    try:
        parsed = datetime(
            year_num,
            month_num,
            day_num,
            hour_num,
            minute_num,
            tzinfo=OFFICIAL_ET_ZONE,
        )
    except ValueError:
        return ""
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_official_injury_text(text: str) -> dict[str, Any]:
    tokens = [line.strip() for line in text.splitlines() if line.strip()]
    rows: list[dict[str, Any]] = []
    current_team = ""
    pending_player = ""
    pending_status = ""
    pending_note_parts: list[str] = []
    status_tokens = 0
    orphan_status_tokens = 0
    skipped_players = 0

    def _flush_pending() -> None:
        nonlocal pending_player, pending_status, pending_note_parts
        if not pending_player or not pending_status:
            pending_player = ""
            pending_status = ""
            pending_note_parts = []
            return
        player_display = _official_player_display_name(pending_player)
        team_display = current_team
        row = {
            "player": player_display,
            "player_official": pending_player,
            "player_norm": normalize_person_name(player_display),
            "team": team_display,
            "team_norm": canonical_team_name(team_display),
            "date_update": "",
            "status_raw": pending_status,
            "status": _parse_injury_status(pending_status),
            "note": " ".join(pending_note_parts).strip(),
            "source": "official_nba_pdf",
        }
        if row["player_norm"]:
            rows.append(row)
        pending_player = ""
        pending_status = ""
        pending_note_parts = []

    for token in tokens:
        normalized = token.strip()
        lowered = normalized.lower()
        if _is_official_header_line(normalized):
            _flush_pending()
            continue
        if lowered == "not yet submitted":
            _flush_pending()
            continue
        if _is_official_team_line(normalized):
            _flush_pending()
            current_team = normalized
            continue
        if lowered in OFFICIAL_STATUS_TOKENS:
            if pending_player:
                pending_status = normalized
                status_tokens += 1
            else:
                orphan_status_tokens += 1
            continue
        if _is_official_player_line(normalized):
            if pending_player and pending_status:
                _flush_pending()
            elif pending_player and not pending_status:
                skipped_players += 1
            pending_player = normalized
            pending_status = ""
            pending_note_parts = []
            continue
        if pending_player and pending_status:
            pending_note_parts.append(normalized)

    _flush_pending()
    coverage = 0.0
    if status_tokens > 0:
        coverage = round(len(rows) / float(status_tokens), 4)
    parse_status = "ok" if rows else "error"
    parse_error = ""
    if parse_status == "error":
        parse_error = "no structured rows parsed from official PDF"
    return {
        "parse_status": parse_status,
        "parse_error": parse_error,
        "rows": rows,
        "rows_count": len(rows),
        "parse_coverage": coverage,
        "parse_status_tokens": status_tokens,
        "parse_orphan_status_tokens": orphan_status_tokens,
        "parse_skipped_players": skipped_players,
        "report_generated_at_utc": _parse_report_generated_at(text),
    }


def _parse_official_injury_pdf(pdf_bytes: bytes) -> dict[str, Any]:
    text = _extract_official_pdf_text(pdf_bytes)
    parsed = _parse_official_injury_text(text)
    parsed["parse_extractor"] = "pdftotext"
    return parsed


def _extract_official_injury_pdfs(html_text: str, base_url: str) -> list[str]:
    pattern = re.compile(r'<a[^>]+href="(?P<href>[^"]+\.pdf)"[^>]*>(?P<label>.*?)</a>', re.I | re.S)
    strict_links: set[str] = set()
    broad_links: set[str] = set()
    for match in pattern.finditer(html_text):
        href = match.group("href")
        label = re.sub(r"<[^>]+>", "", match.group("label"))
        label = html.unescape(label).strip().lower()
        absolute = urljoin(base_url, href)
        broad_links.add(absolute)
        haystack = f"{href.lower()} {label}"
        if "injury" in haystack and "report" in haystack:
            strict_links.add(absolute)

    if strict_links:
        return sorted(strict_links)

    fallback = [link for link in broad_links if "injury" in link.lower()]
    return sorted(fallback)


def _safe_timestamp_slug(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z_-]", "-", value)


def _cache_pdf(pdf_bytes: bytes, *, pdf_cache_dir: Path, source_url: str) -> dict[str, Any]:
    fetched_at = now_utc()
    pdf_cache_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(pdf_bytes).hexdigest()
    filename = f"official-injury-{_safe_timestamp_slug(fetched_at)}.pdf"
    snapshot_path = pdf_cache_dir / filename
    latest_path = pdf_cache_dir / "latest.pdf"
    snapshot_path.write_bytes(pdf_bytes)
    latest_path.write_bytes(pdf_bytes)
    return {
        "pdf_download_status": "ok",
        "pdf_download_url": source_url,
        "pdf_cached_path": str(snapshot_path),
        "pdf_latest_path": str(latest_path),
        "pdf_sha256": digest,
        "pdf_size_bytes": len(pdf_bytes),
        "pdf_cached_at_utc": fetched_at,
    }


def fetch_official_injury_links(*, pdf_cache_dir: Path | None = None) -> dict[str, Any]:
    """Fetch official NBA injury report page and extract injury report PDF links."""
    attempted_urls: list[str] = []
    last_error = ""
    for url in OFFICIAL_INJURY_URLS:
        attempted_urls.append(url)
        payload: dict[str, Any] = {
            "source": "official_nba",
            "url": url,
            "attempted_urls": attempted_urls,
            "fetched_at_utc": now_utc(),
            "ttl_minutes": 240,
        }
        try:
            response = _http_get(url)
            pdf_links = _extract_official_injury_pdfs(response.text, url)
            if not pdf_links:
                payload["status"] = "error"
                payload["error"] = "no injury-report PDFs found"
                payload["pdf_links"] = []
                payload["count"] = 0
                payload["rows"] = []
                payload["rows_count"] = 0
                payload["parse_status"] = "error"
                payload["parse_error"] = "no injury-report PDFs found"
                last_error = payload["error"]
                continue
            payload["status"] = "ok"
            payload["pdf_links"] = pdf_links
            payload["count"] = len(pdf_links)
            payload["selected_pdf_url"] = max(pdf_links, key=_official_pdf_sort_key)
            payload["pdf_download_status"] = "error"
            payload["pdf_download_url"] = ""
            payload["pdf_cached_path"] = ""
            payload["pdf_latest_path"] = ""
            payload["pdf_sha256"] = ""
            payload["pdf_size_bytes"] = 0
            payload["pdf_cached_at_utc"] = ""
            payload["rows"] = []
            payload["rows_count"] = 0
            payload["parse_status"] = "error"
            payload["parse_error"] = "official PDF not downloaded"
            payload["parse_coverage"] = 0.0
            payload["parse_status_tokens"] = 0
            payload["parse_orphan_status_tokens"] = 0
            payload["parse_skipped_players"] = 0
            payload["report_generated_at_utc"] = ""
            payload["parse_extractor"] = "pdftotext"

            errors: list[str] = []
            ranked_links = sorted(pdf_links, key=_official_pdf_sort_key, reverse=True)
            for pdf_url in ranked_links:
                try:
                    pdf_response = _http_get(pdf_url, timeout_s=20.0)
                except Exception as exc:  # pragma: no cover - network branch
                    errors.append(f"{pdf_url}:{exc}")
                    continue
                content = pdf_response.content
                if not content:
                    errors.append(f"{pdf_url}:empty_pdf_content")
                    continue
                try:
                    parsed = _parse_official_injury_pdf(content)
                except Exception as exc:  # pragma: no cover - external binary branch
                    errors.append(f"{pdf_url}:parse_error:{exc}")
                    continue
                payload["selected_pdf_url"] = pdf_url
                payload["pdf_download_status"] = "ok"
                payload["pdf_download_url"] = pdf_url
                if pdf_cache_dir is not None:
                    payload.update(
                        _cache_pdf(content, pdf_cache_dir=pdf_cache_dir, source_url=pdf_url)
                    )
                payload.update(parsed)
                payload["pdf_download_errors"] = errors
                if str(payload.get("parse_status", "error")) == "ok":
                    return payload
                errors.append(
                    f"{pdf_url}:parse_status:{payload.get('parse_status', 'error')}:"
                    f"{payload.get('parse_error', '')}"
                )

            payload["status"] = "error"
            payload["error"] = "official injury PDF download/parse failed"
            payload["pdf_download_errors"] = errors
            return payload
        except Exception as exc:
            last_error = str(exc)
            payload["status"] = "error"
            payload["error"] = last_error
            payload["pdf_links"] = []
            payload["count"] = 0
            payload["rows"] = []
            payload["rows_count"] = 0
            payload["parse_status"] = "error"
            payload["parse_error"] = last_error

    return {
        "source": "official_nba",
        "url": OFFICIAL_INJURY_URLS[0],
        "attempted_urls": attempted_urls,
        "fetched_at_utc": now_utc(),
        "ttl_minutes": 240,
        "status": "error",
        "error": last_error or "official injury report unavailable",
        "pdf_links": [],
        "count": 0,
        "selected_pdf_url": "",
        "pdf_download_status": "error",
        "pdf_download_url": "",
        "pdf_cached_path": "",
        "pdf_latest_path": "",
        "pdf_sha256": "",
        "pdf_size_bytes": 0,
        "pdf_cached_at_utc": "",
        "rows": [],
        "rows_count": 0,
        "parse_status": "error",
        "parse_error": last_error or "official injury report unavailable",
        "parse_coverage": 0.0,
        "parse_status_tokens": 0,
        "parse_orphan_status_tokens": 0,
        "parse_skipped_players": 0,
        "report_generated_at_utc": "",
        "parse_extractor": "pdftotext",
    }


def fetch_bref_injuries() -> dict[str, Any]:
    """Fetch Basketball Reference injury table."""
    payload: dict[str, Any] = {
        "source": "basketball_reference",
        "url": BREF_INJURY_URL,
        "fetched_at_utc": now_utc(),
        "ttl_minutes": 180,
    }
    try:
        response = _http_get(BREF_INJURY_URL)
    except Exception as exc:
        payload["status"] = "error"
        payload["error"] = str(exc)
        payload["rows"] = []
        payload["count"] = 0
        return payload

    pattern = re.compile(
        r"<tr[^>]*>"
        r'<th[^>]*data-stat="player"[^>]*><a[^>]*>(?P<player>[^<]+)</a></th>'
        r'<td[^>]*data-stat="team_name"[^>]*><a[^>]*>(?P<team>[^<]+)</a></td>'
        r'<td[^>]*data-stat="date_update"[^>]*csk="(?P<date>[^"]+)"[^>]*>.*?</td>'
        r'<td[^>]*data-stat="note"[^>]*>(?P<note>.*?)</td>'
        r"</tr>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    rows: list[dict[str, Any]] = []
    for match in pattern.finditer(response.text):
        player = html.unescape(match.group("player")).strip()
        team = html.unescape(match.group("team")).strip()
        date_update = match.group("date").strip()
        note_html = match.group("note")
        note_text = re.sub(r"<[^>]+>", "", note_html)
        note = html.unescape(note_text).strip()
        status = _parse_injury_status(note)
        rows.append(
            {
                "player": player,
                "player_norm": normalize_person_name(player),
                "team": team,
                "team_norm": canonical_team_name(team),
                "date_update": date_update,
                "status": status,
                "note": note,
            }
        )
    payload["status"] = "ok"
    payload["rows"] = rows
    payload["count"] = len(rows)
    return payload


def fetch_espn_injuries() -> dict[str, Any]:
    """Fetch ESPN injuries feed as secondary fallback."""
    payload: dict[str, Any] = {
        "source": "espn_injuries",
        "url": ESPN_INJURIES_URL,
        "fetched_at_utc": now_utc(),
        "ttl_minutes": 180,
    }
    try:
        response = _http_get(ESPN_INJURIES_URL)
        data = response.json()
    except Exception as exc:
        payload["status"] = "error"
        payload["error"] = str(exc)
        payload["rows"] = []
        payload["count"] = 0
        return payload

    groups = data.get("injuries", []) if isinstance(data, dict) else []
    rows: list[dict[str, Any]] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        team = str(group.get("displayName", "")).strip()
        team_norm = canonical_team_name(team)
        for injury in group.get("injuries", []):
            if not isinstance(injury, dict):
                continue
            athlete = injury.get("athlete", {})
            player = ""
            if isinstance(athlete, dict):
                player = str(athlete.get("displayName", "")).strip()
            if not player:
                continue
            short_comment = str(injury.get("shortComment", "")).strip()
            long_comment = str(injury.get("longComment", "")).strip()
            status_raw = str(injury.get("status", "")).strip()
            note = short_comment or long_comment or status_raw
            status = _parse_injury_status(f"{status_raw} {note}")
            rows.append(
                {
                    "player": player,
                    "player_norm": normalize_person_name(player),
                    "team": team,
                    "team_norm": team_norm,
                    "date_update": str(injury.get("date", "")),
                    "status": status,
                    "note": note,
                }
            )

    payload["status"] = "ok"
    payload["rows"] = rows
    payload["count"] = len(rows)
    return payload


def fetch_secondary_injuries() -> dict[str, Any]:
    """Fetch secondary injuries with fallback order: BRef -> ESPN."""
    bref = fetch_bref_injuries()
    if bref.get("status") == "ok":
        return bref
    espn = fetch_espn_injuries()
    if espn.get("status") == "ok":
        espn["fallback_from"] = "basketball_reference"
        espn["fallback_error"] = str(bref.get("error", ""))
        return espn
    return {
        "source": "secondary_injuries",
        "url": BREF_INJURY_URL,
        "fetched_at_utc": now_utc(),
        "ttl_minutes": 180,
        "status": "error",
        "error": "secondary injuries unavailable",
        "errors": {
            "basketball_reference": str(bref.get("error", "")),
            "espn": str(espn.get("error", "")),
        },
        "rows": [],
        "count": 0,
    }


def _fetch_espn_team_map() -> dict[str, str]:
    data = _http_get(ESPN_TEAMS_URL).json()
    mapping: dict[str, str] = {}
    sports = data.get("sports", []) if isinstance(data, dict) else []
    for sport in sports:
        if not isinstance(sport, dict):
            continue
        leagues = sport.get("leagues", [])
        for league in leagues:
            if not isinstance(league, dict):
                continue
            teams = league.get("teams", [])
            for team_wrap in teams:
                if not isinstance(team_wrap, dict):
                    continue
                team = team_wrap.get("team", {})
                if not isinstance(team, dict):
                    continue
                team_id = str(team.get("id", "")).strip()
                if not team_id:
                    continue
                display_name = canonical_team_name(str(team.get("displayName", "")))
                short_display = canonical_team_name(str(team.get("shortDisplayName", "")))
                location_name = canonical_team_name(
                    f"{team.get('location', '')} {team.get('name', '')}".strip()
                )
                for key in [display_name, short_display, location_name]:
                    if key:
                        mapping[key] = team_id
    return mapping


def _fetch_espn_rosters(teams_in_scope: set[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": "espn_team_rosters",
        "url": ESPN_TEAMS_URL,
        "fetched_at_utc": now_utc(),
        "status": "ok",
        "errors": [],
        "teams": {},
    }
    try:
        team_map = _fetch_espn_team_map()
    except Exception as exc:
        payload["status"] = "error"
        payload["errors"] = [f"team_map:{exc}"]
        return payload

    inactive_by_team: dict[str, set[str]] = {}
    injuries_feed = fetch_espn_injuries()
    if injuries_feed.get("status") == "ok":
        rows = injuries_feed.get("rows", [])
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                status = str(row.get("status", "unknown"))
                if status not in {"out", "out_for_season", "doubtful"}:
                    continue
                team_name = canonical_team_name(str(row.get("team_norm", row.get("team", ""))))
                player_norm = normalize_person_name(str(row.get("player", "")))
                if not team_name or not player_norm:
                    continue
                inactive_by_team.setdefault(team_name, set()).add(player_norm)
    else:
        payload["errors"].append(f"injuries:{injuries_feed.get('error', 'unknown')}")

    for team_name in sorted(teams_in_scope):
        team_id = team_map.get(team_name)
        if not team_id:
            payload["errors"].append(f"missing_team_id:{team_name}")
            continue
        roster_url = ESPN_TEAM_ROSTER_URL_TEMPLATE.format(team_id=team_id)
        try:
            data = _http_get(roster_url).json()
        except Exception as exc:
            payload["errors"].append(f"{team_name}:{exc}")
            continue

        athletes = data.get("athletes", []) if isinstance(data, dict) else []
        names: list[str] = []
        for athlete in athletes:
            if not isinstance(athlete, dict):
                continue
            full_name = str(athlete.get("fullName", "")).strip()
            if not full_name:
                full_name = str(athlete.get("displayName", "")).strip()
            if full_name:
                names.append(normalize_person_name(full_name))

        all_names = sorted(set(names))
        inactive_names = sorted(
            name for name in all_names if name in inactive_by_team.get(team_name, set())
        )
        active_names = sorted(name for name in all_names if name not in set(inactive_names))

        payload["teams"][team_name] = {
            "active": active_names,
            "inactive": inactive_names,
            "all": all_names,
            "game_ids": [],
            "source": "espn_team_rosters_plus_injuries",
        }

    if not payload["teams"]:
        payload["status"] = "error"
    return payload


def fetch_roster_context(*, teams_in_scope: list[str] | None = None) -> dict[str, Any]:
    """Fetch roster availability from NBA live feeds with ESPN fallback."""
    payload: dict[str, Any] = {
        "source": "nba_live_scoreboard",
        "url": TODAYS_SCOREBOARD_URL,
        "fetched_at_utc": now_utc(),
        "ttl_minutes": 1440,
    }
    try:
        scoreboard = _http_get(TODAYS_SCOREBOARD_URL).json()
    except Exception as exc:
        payload["status"] = "error"
        payload["error"] = str(exc)
        payload["teams"] = {}
        payload["games"] = []
        return payload

    games = scoreboard.get("scoreboard", {}).get("games", [])
    if not isinstance(games, list):
        games = []

    teams: dict[str, dict[str, Any]] = {}
    game_rows: list[dict[str, Any]] = []
    teams_from_games: set[str] = set()
    boxscore_errors: list[str] = []

    for game in games:
        if not isinstance(game, dict):
            continue
        game_id = str(game.get("gameId", ""))
        home = game.get("homeTeam", {})
        away = game.get("awayTeam", {})
        if not game_id or not isinstance(home, dict) or not isinstance(away, dict):
            continue
        home_name = canonical_team_name(
            f"{home.get('teamCity', '')} {home.get('teamName', '')}".strip()
        )
        away_name = canonical_team_name(
            f"{away.get('teamCity', '')} {away.get('teamName', '')}".strip()
        )
        if home_name:
            teams_from_games.add(home_name)
        if away_name:
            teams_from_games.add(away_name)
        game_rows.append(
            {
                "game_id": game_id,
                "home_team": home_name,
                "away_team": away_name,
                "game_time_utc": str(game.get("gameTimeUTC", "")),
            }
        )
        box_url = BOXSCORE_URL_TEMPLATE.format(game_id=game_id)
        try:
            boxscore = _http_get(box_url).json()
        except Exception as exc:
            boxscore_errors.append(f"{game_id}:{exc}")
            continue

        game_payload = boxscore.get("game", {})
        for side in ("homeTeam", "awayTeam"):
            team_payload = game_payload.get(side, {})
            if not isinstance(team_payload, dict):
                continue
            team_name = canonical_team_name(
                f"{team_payload.get('teamCity', '')} {team_payload.get('teamName', '')}".strip()
            )
            if not team_name:
                continue
            team_row = teams.setdefault(
                team_name,
                {"active": [], "inactive": [], "all": [], "game_ids": [], "source": "nba_boxscore"},
            )
            players = team_payload.get("players", [])
            if not isinstance(players, list):
                players = []
            for player in players:
                if not isinstance(player, dict):
                    continue
                name = str(player.get("name", "")).strip()
                if not name:
                    continue
                normalized_name = normalize_person_name(name)
                status = str(player.get("status", "")).upper()
                team_row["all"].append(normalized_name)
                if status == "INACTIVE":
                    team_row["inactive"].append(normalized_name)
                else:
                    team_row["active"].append(normalized_name)
            if game_id not in team_row["game_ids"]:
                team_row["game_ids"].append(game_id)

    for team_row in teams.values():
        team_row["active"] = sorted(set(team_row.get("active", [])))
        team_row["inactive"] = sorted(set(team_row.get("inactive", [])))
        team_row["all"] = sorted(set(team_row.get("all", [])))

    requested_scope = {canonical_team_name(name) for name in (teams_in_scope or []) if name}
    fallback_scope = requested_scope or teams_from_games
    missing_roster_teams = {
        team for team in fallback_scope if team not in teams or not teams[team]["all"]
    }
    fallback_payload: dict[str, Any] | None = None
    if missing_roster_teams:
        fallback_payload = _fetch_espn_rosters(missing_roster_teams)
        if fallback_payload.get("status") == "ok":
            fallback_teams = fallback_payload.get("teams", {})
            if isinstance(fallback_teams, dict):
                for team_name, row in fallback_teams.items():
                    if isinstance(row, dict):
                        teams[team_name] = row

    payload["status"] = "ok"
    payload["games"] = game_rows
    payload["teams"] = teams
    payload["count_games"] = len(game_rows)
    payload["count_teams"] = len(teams)
    payload["boxscore_errors"] = boxscore_errors
    payload["missing_roster_teams"] = sorted(missing_roster_teams)
    if fallback_payload:
        payload["fallback"] = {
            "source": fallback_payload.get("source", ""),
            "status": fallback_payload.get("status", ""),
            "errors": fallback_payload.get("errors", []),
            "count_teams": len(fallback_payload.get("teams", {})),
            "fetched_at_utc": fallback_payload.get("fetched_at_utc", ""),
        }
    return payload


def _parse_iso_utc(value: str) -> datetime | None:
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _apply_stale_flag(payload: dict[str, Any], stale_after_hours: float | None) -> dict[str, Any]:
    if stale_after_hours is None:
        return payload
    copy = dict(payload)
    fetched_at = _parse_iso_utc(str(copy.get("fetched_at_utc", "")))
    if fetched_at is None:
        copy["stale"] = True
        copy["stale_reason"] = "missing_fetched_at_utc"
        return copy
    age = datetime.now(UTC) - fetched_at
    stale = age > timedelta(hours=max(0.0, stale_after_hours))
    copy["stale"] = stale
    copy["stale_age_hours"] = round(age.total_seconds() / 3600.0, 2)
    return copy


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(value, dict):
        return value
    return {
        "status": "error",
        "error": f"invalid json object in {path}",
        "fetched_at_utc": now_utc(),
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def load_or_fetch_context(
    *,
    cache_path: Path,
    offline: bool,
    refresh: bool,
    fetcher,
    fallback_paths: list[Path] | None = None,
    write_through_paths: list[Path] | None = None,
    stale_after_hours: float | None = None,
) -> dict[str, Any]:
    """Load cached context or fetch and cache a fresh copy.

    fallback_paths are optional read-only backups (for example, global latest files).
    write_through_paths receive a copy of the fetched payload.
    """
    fallback_paths = fallback_paths or []
    write_through_paths = write_through_paths or []

    if cache_path.exists() and not refresh:
        return _apply_stale_flag(_load_json(cache_path), stale_after_hours)

    for path in fallback_paths:
        if not path.exists() or refresh:
            continue
        value = _load_json(path)
        _write_json(cache_path, value)
        return _apply_stale_flag(value, stale_after_hours)

    if offline:
        return {
            "status": "missing",
            "offline": True,
            "error": f"missing context cache: {cache_path}",
            "fetched_at_utc": now_utc(),
            "stale": True,
        }

    value = fetcher()
    _write_json(cache_path, value)
    for path in write_through_paths:
        _write_json(path, value)
    return _apply_stale_flag(value, stale_after_hours)
