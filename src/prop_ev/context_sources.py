"""External context fetchers for injuries and roster verification."""

from __future__ import annotations

import hashlib
import html
import json
import re
import unicodedata
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

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
    if re.search(r"\bout\b", value):
        return "out"
    if "doubtful" in value:
        return "doubtful"
    if "questionable" in value:
        return "questionable"
    if "probable" in value:
        return "probable"
    if "day to day" in value:
        return "day_to_day"
    return "unknown"


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
                last_error = payload["error"]
                continue
            payload["status"] = "ok"
            payload["pdf_links"] = pdf_links
            payload["count"] = len(pdf_links)
            payload["pdf_download_status"] = "skipped"
            payload["pdf_download_url"] = ""
            payload["pdf_cached_path"] = ""
            payload["pdf_latest_path"] = ""
            payload["pdf_sha256"] = ""
            payload["pdf_size_bytes"] = 0
            payload["pdf_cached_at_utc"] = ""
            if pdf_cache_dir is not None:
                errors: list[str] = []
                for pdf_url in pdf_links:
                    try:
                        pdf_response = _http_get(pdf_url, timeout_s=20.0)
                    except Exception as exc:  # pragma: no cover - network branch
                        errors.append(f"{pdf_url}:{exc}")
                        continue
                    content = pdf_response.content
                    if not content:
                        errors.append(f"{pdf_url}:empty_pdf_content")
                        continue
                    payload.update(
                        _cache_pdf(content, pdf_cache_dir=pdf_cache_dir, source_url=pdf_url)
                    )
                    payload["pdf_download_errors"] = errors
                    return payload
                payload["pdf_download_status"] = "error"
                payload["pdf_download_errors"] = errors
            return payload
        except Exception as exc:
            last_error = str(exc)
            payload["status"] = "error"
            payload["error"] = last_error
            payload["pdf_links"] = []
            payload["count"] = 0

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
        "pdf_download_status": "error",
        "pdf_download_url": "",
        "pdf_cached_path": "",
        "pdf_latest_path": "",
        "pdf_sha256": "",
        "pdf_size_bytes": 0,
        "pdf_cached_at_utc": "",
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
