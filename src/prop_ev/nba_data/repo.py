"""Unified NBA repository for results and context."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prop_ev.data_paths import (
    canonical_context_dir,
    data_home_from_odds_root,
)
from prop_ev.data_paths import (
    resolve_nba_data_root as resolve_nba_data_root_from_odds,
)
from prop_ev.nba_data.cache_store import NBADataCacheStore
from prop_ev.nba_data.config import resolve_data_dir
from prop_ev.nba_data.context_cache import load_or_fetch_context
from prop_ev.nba_data.context_fetchers import (
    fetch_official_injury_links,
    fetch_roster_context,
    fetch_secondary_injuries,
)
from prop_ev.nba_data.date_resolver import resolve_snapshot_date, resolve_snapshot_date_str
from prop_ev.nba_data.endpoints import BOXSCORE_URL_TEMPLATE, TODAYS_SCOREBOARD_URL
from prop_ev.nba_data.gateway import get_json
from prop_ev.nba_data.normalize import canonical_team_name, normalize_person_name
from prop_ev.nba_data.request import NBADataRequest
from prop_ev.nba_data.source_policy import ResultsSourceMode
from prop_ev.storage import SnapshotStore
from prop_ev.time_utils import utc_now_str

RESULTS_SOURCE_LIVE = "nba_live_scoreboard_boxscore"
RESULTS_SOURCE_HISTORICAL = "nba_data_schedule_plus_boxscore"


def _now_utc() -> str:
    return utc_now_str()


def _safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _game_status(code: Any, text: str) -> str:
    if isinstance(code, str):
        parsed = _safe_float(code)
        code = int(parsed) if parsed is not None else None
    elif isinstance(code, float):
        code = int(code)
    elif not isinstance(code, int):
        code = None

    cleaned = text.strip().lower()
    if code == 3 or cleaned.startswith("final"):
        return "final"
    if code == 2:
        return "in_progress"
    if code == 1:
        return "scheduled"
    return "unknown"


def _extract_players(game_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    players: dict[str, dict[str, Any]] = {}
    for side in ("homeTeam", "awayTeam"):
        team_payload = game_payload.get(side, {})
        if not isinstance(team_payload, dict):
            continue
        team_players = team_payload.get("players", [])
        if not isinstance(team_players, list):
            continue
        for item in team_players:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            key = normalize_person_name(name)
            if not key:
                continue
            stats = item.get("statistics", {})
            players[key] = {
                "name": name,
                "statistics": stats if isinstance(stats, dict) else {},
                "status": str(item.get("status", "")),
            }
    return players


def _seed_teams(seed_rows: list[dict[str, Any]]) -> set[str]:
    teams: set[str] = set()
    for row in seed_rows:
        home = canonical_team_name(str(row.get("home_team", "")))
        away = canonical_team_name(str(row.get("away_team", "")))
        if home:
            teams.add(home)
        if away:
            teams.add(away)
    return teams


def _resolve_nba_data_root(odds_data_root: Path) -> Path:
    configured = resolve_data_dir(None).resolve()
    return resolve_nba_data_root_from_odds(odds_data_root, configured=configured)


class NBARepository:
    """Single NBA access handle for results and context consumers."""

    def __init__(
        self,
        *,
        odds_data_root: Path,
        snapshot_id: str,
        snapshot_dir: Path,
        nba_data_root: Path | None = None,
    ) -> None:
        self.odds_data_root = odds_data_root.resolve()
        self.snapshot_id = snapshot_id
        self.snapshot_dir = snapshot_dir.resolve()
        self.nba_data_root = (
            nba_data_root.resolve()
            if nba_data_root is not None
            else _resolve_nba_data_root(self.odds_data_root)
        )
        self.context_dir = canonical_context_dir(self.nba_data_root, self.snapshot_id)
        self.legacy_context_dir = self.snapshot_dir / "context"
        self.context_ref_path = self.snapshot_dir / "context_ref.json"
        self.reference_dir = self.nba_data_root / "reference"
        self.legacy_reference_dir = self.odds_data_root / "reference"
        self.cache = NBADataCacheStore(self.odds_data_root)
        self._boxscore_manifest_index: dict[str, Path] | None = None

    @classmethod
    def from_store(cls, *, store: SnapshotStore, snapshot_id: str) -> NBARepository:
        return cls(
            odds_data_root=store.root,
            snapshot_id=snapshot_id,
            snapshot_dir=store.snapshot_dir(snapshot_id),
        )

    def _context_json_path(self, name: str) -> Path:
        return self.context_dir / f"{name}.json"

    def _legacy_context_json_path(self, name: str) -> Path:
        return self.legacy_context_dir / f"{name}.json"

    def _context_ref_relpath(self, path: Path) -> str:
        data_home = data_home_from_odds_root(self.odds_data_root)
        resolved = path.resolve()
        try:
            return str(resolved.relative_to(data_home))
        except ValueError:
            return str(resolved)

    def _sha256_file(self, path: Path) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _load_context_ref(self) -> dict[str, Any]:
        if not self.context_ref_path.exists():
            return {"schema_version": 1, "snapshot_id": self.snapshot_id, "context": {}}
        try:
            payload = json.loads(self.context_ref_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"schema_version": 1, "snapshot_id": self.snapshot_id, "context": {}}
        if not isinstance(payload, dict):
            return {"schema_version": 1, "snapshot_id": self.snapshot_id, "context": {}}
        context = payload.get("context")
        if not isinstance(context, dict):
            payload["context"] = {}
        return payload

    def _write_context_ref(self, *, updates: dict[str, Path]) -> None:
        payload = self._load_context_ref()
        payload["schema_version"] = 1
        payload["snapshot_id"] = self.snapshot_id
        payload["updated_at_utc"] = _now_utc()
        context = payload.setdefault("context", {})
        if not isinstance(context, dict):
            context = {}
            payload["context"] = context
        for key, path in updates.items():
            if not path.exists():
                continue
            context[key] = {
                "path": self._context_ref_relpath(path),
                "sha256": self._sha256_file(path),
                "bytes": path.stat().st_size,
            }
        self.context_ref_path.parent.mkdir(parents=True, exist_ok=True)
        self.context_ref_path.write_text(
            json.dumps(payload, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    def context_paths(self) -> tuple[Path, Path, Path]:
        """Return injuries, roster, and results context cache paths."""
        injuries = self._context_json_path("injuries")
        roster = self._context_json_path("roster")
        results = self._context_json_path("results")
        if not injuries.exists():
            legacy_injuries = self._legacy_context_json_path("injuries")
            if legacy_injuries.exists():
                injuries = legacy_injuries
        if not roster.exists():
            legacy_roster = self._legacy_context_json_path("roster")
            if legacy_roster.exists():
                roster = legacy_roster
        if not results.exists():
            legacy_results = self._legacy_context_json_path("results")
            if legacy_results.exists():
                results = legacy_results
        return injuries, roster, results

    def official_injury_pdf_dir(self) -> Path:
        """Return canonical official injury PDF cache directory."""
        return self.context_dir / "official_injury_pdf"

    def identity_map_path(self) -> Path:
        """Return canonical identity-map path owned by NBA lake."""
        return self.reference_dir / "player_identity_map.json"

    def refresh_context_ref(self) -> None:
        """Write context reference manifest from existing canonical context files."""
        updates: dict[str, Path] = {}
        for key in ("injuries", "roster", "results"):
            path = self._context_json_path(key)
            if path.exists():
                updates[key] = path
        if updates:
            self._write_context_ref(updates=updates)

    def load_strategy_context(
        self,
        *,
        teams_in_scope: list[str],
        offline: bool,
        refresh: bool,
        injuries_stale_hours: float,
        roster_stale_hours: float,
    ) -> tuple[dict[str, Any], dict[str, Any], Path, Path]:
        reference_injuries = self.reference_dir / "injuries" / "latest.json"
        legacy_reference_injuries = self.legacy_reference_dir / "injuries" / "latest.json"
        today_key = datetime.now(UTC).strftime("%Y-%m-%d")
        reference_roster_daily = self.reference_dir / "rosters" / f"roster-{today_key}.json"
        reference_roster_latest = self.reference_dir / "rosters" / "latest.json"
        legacy_reference_roster_daily = (
            self.legacy_reference_dir / "rosters" / f"roster-{today_key}.json"
        )
        legacy_reference_roster_latest = self.legacy_reference_dir / "rosters" / "latest.json"

        injuries_path = self._context_json_path("injuries")
        roster_path = self._context_json_path("roster")
        legacy_injuries_path = self._legacy_context_json_path("injuries")
        legacy_roster_path = self._legacy_context_json_path("roster")
        official_pdf_dir = self.official_injury_pdf_dir()

        injuries = load_or_fetch_context(
            cache_path=injuries_path,
            offline=offline,
            refresh=refresh,
            fetcher=lambda: {
                "fetched_at_utc": _now_utc(),
                "official": fetch_official_injury_links(pdf_cache_dir=official_pdf_dir),
                "secondary": fetch_secondary_injuries(),
            },
            fallback_paths=[legacy_injuries_path, reference_injuries, legacy_reference_injuries],
            write_through_paths=[reference_injuries],
            stale_after_hours=injuries_stale_hours,
        )
        roster = load_or_fetch_context(
            cache_path=roster_path,
            offline=offline,
            refresh=refresh,
            fetcher=lambda: fetch_roster_context(teams_in_scope=teams_in_scope),
            fallback_paths=[
                legacy_roster_path,
                reference_roster_daily,
                reference_roster_latest,
                legacy_reference_roster_daily,
                legacy_reference_roster_latest,
            ],
            write_through_paths=[reference_roster_daily, reference_roster_latest],
            stale_after_hours=roster_stale_hours,
        )
        self._write_context_ref(updates={"injuries": injuries_path, "roster": roster_path})
        return injuries, roster, injuries_path, roster_path

    def load_results_for_settlement(
        self,
        *,
        seed_rows: list[dict[str, Any]],
        offline: bool,
        refresh: bool,
        mode: ResultsSourceMode,
    ) -> tuple[dict[str, Any], Path]:
        teams_in_scope = _seed_teams(seed_rows)
        snapshot_day = resolve_snapshot_date_str(self.snapshot_id)
        cache_path = self._context_json_path("results")
        legacy_results_path = self._legacy_context_json_path("results")
        results = load_or_fetch_context(
            cache_path=cache_path,
            offline=offline,
            refresh=refresh,
            fallback_paths=[legacy_results_path],
            fetcher=lambda: self._fetch_results(
                mode=mode,
                teams_in_scope=teams_in_scope,
                snapshot_day=snapshot_day,
                refresh=refresh,
            ),
        )
        self._write_context_ref(updates={"results": cache_path})
        return results, cache_path

    def _fetch_results(
        self,
        *,
        mode: ResultsSourceMode,
        teams_in_scope: set[str],
        snapshot_day: str,
        refresh: bool,
    ) -> dict[str, Any]:
        if mode == "cache_only":
            return {
                "source": "nba_cache_only",
                "mode": mode,
                "fetched_at_utc": _now_utc(),
                "status": "error",
                "error": "cache_only mode requires existing cache",
                "games": [],
                "errors": ["cache_only_requires_cached_results"],
                "count_games": 0,
                "count_errors": 1,
                "snapshot_date": snapshot_day,
            }

        today = datetime.now(UTC).date()
        target = resolve_snapshot_date(self.snapshot_id)
        order: list[str]
        if mode == "historical":
            order = ["historical"]
        elif mode == "live":
            order = ["live"]
        elif target < today:
            order = ["historical", "live"]
        else:
            order = ["live", "historical"]

        attempts: list[dict[str, Any]] = []
        for source_mode in order:
            if source_mode == "historical":
                payload = self._fetch_historical_results(
                    snapshot_day=snapshot_day,
                    teams_in_scope=teams_in_scope,
                    refresh=refresh,
                )
            else:
                payload = self._fetch_live_results(teams_in_scope=teams_in_scope)
            payload["mode"] = mode
            payload["snapshot_date"] = snapshot_day
            attempts.append(payload)
            if int(payload.get("count_games", 0)) > 0:
                return payload

        if attempts:
            merged = dict(attempts[0])
            errors: list[str] = []
            for attempt in attempts:
                raw_errors = attempt.get("errors", [])
                if isinstance(raw_errors, list):
                    errors.extend(str(item) for item in raw_errors if str(item).strip())
            merged["errors"] = errors
            merged["count_errors"] = len(errors)
            if errors and not int(merged.get("count_games", 0)):
                merged["status"] = "partial"
            return merged

        return {
            "source": "nba_results_unknown",
            "mode": mode,
            "fetched_at_utc": _now_utc(),
            "status": "error",
            "games": [],
            "errors": ["no_results_source_attempted"],
            "count_games": 0,
            "count_errors": 1,
            "snapshot_date": snapshot_day,
        }

    def _fetch_live_results(self, *, teams_in_scope: set[str]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source": RESULTS_SOURCE_LIVE,
            "url": TODAYS_SCOREBOARD_URL,
            "fetched_at_utc": _now_utc(),
            "status": "ok",
            "games": [],
            "errors": [],
            "cache_level": "network",
        }
        scoreboard = get_json(TODAYS_SCOREBOARD_URL)
        games = scoreboard.get("scoreboard", {}).get("games", [])
        if not isinstance(games, list):
            games = []

        normalized_games: list[dict[str, Any]] = []
        for game in games:
            if not isinstance(game, dict):
                continue
            game_id = str(game.get("gameId", ""))
            home = game.get("homeTeam", {})
            away = game.get("awayTeam", {})
            if not isinstance(home, dict) or not isinstance(away, dict):
                continue
            home_team = canonical_team_name(
                f"{home.get('teamCity', '')} {home.get('teamName', '')}"
            )
            away_team = canonical_team_name(
                f"{away.get('teamCity', '')} {away.get('teamName', '')}"
            )
            if (
                teams_in_scope
                and home_team not in teams_in_scope
                and away_team not in teams_in_scope
            ):
                continue

            game_status_text = str(game.get("gameStatusText", ""))
            status = _game_status(game.get("gameStatus"), game_status_text)
            game_row: dict[str, Any] = {
                "game_id": game_id,
                "home_team": home_team,
                "away_team": away_team,
                "game_status": status,
                "game_status_text": game_status_text,
                "players": {},
                "period": "",
                "game_clock": "",
            }

            if not game_id:
                payload["errors"].append("missing_game_id")
                normalized_games.append(game_row)
                continue

            boxscore_url = BOXSCORE_URL_TEMPLATE.format(game_id=game_id)
            try:
                boxscore = get_json(boxscore_url)
            except Exception as exc:
                payload["errors"].append(f"{game_id}:{exc}")
                normalized_games.append(game_row)
                continue

            normalized = self._normalize_boxscore_game(boxscore, fallback_game_id=game_id)
            if normalized is None:
                normalized_games.append(game_row)
                continue
            if normalized.get("home_team"):
                game_row["home_team"] = normalized["home_team"]
            if normalized.get("away_team"):
                game_row["away_team"] = normalized["away_team"]
            game_row["players"] = normalized.get("players", {})
            game_row["period"] = str(normalized.get("period", ""))
            game_row["game_clock"] = str(normalized.get("game_clock", ""))
            if str(normalized.get("game_status_text", "")):
                game_row["game_status_text"] = str(normalized.get("game_status_text", ""))
            game_row["game_status"] = str(normalized.get("game_status", game_row["game_status"]))
            normalized_games.append(game_row)

        if payload["errors"] and not normalized_games:
            payload["status"] = "error"
        elif payload["errors"]:
            payload["status"] = "partial"
        payload["games"] = normalized_games
        payload["count_games"] = len(normalized_games)
        payload["count_errors"] = len(payload["errors"])
        return payload

    def _fetch_historical_results(
        self,
        *,
        snapshot_day: str,
        teams_in_scope: set[str],
        refresh: bool,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source": RESULTS_SOURCE_HISTORICAL,
            "fetched_at_utc": _now_utc(),
            "status": "ok",
            "games": [],
            "errors": [],
            "cache_level": "mixed",
        }
        game_ids = self._historical_game_ids_for_day(snapshot_day)
        normalized_games: list[dict[str, Any]] = []
        for game_id in game_ids:
            boxscore = self._load_or_fetch_boxscore(game_id=game_id, refresh=refresh)
            if not isinstance(boxscore, dict):
                payload["errors"].append(f"{game_id}:boxscore_missing")
                continue
            normalized = self._normalize_boxscore_game(boxscore, fallback_game_id=game_id)
            if normalized is None:
                payload["errors"].append(f"{game_id}:boxscore_invalid")
                continue
            home_team = canonical_team_name(str(normalized.get("home_team", "")))
            away_team = canonical_team_name(str(normalized.get("away_team", "")))
            if (
                teams_in_scope
                and home_team not in teams_in_scope
                and away_team not in teams_in_scope
            ):
                continue
            normalized_games.append(
                {
                    "game_id": str(normalized.get("game_id", game_id)),
                    "home_team": home_team,
                    "away_team": away_team,
                    "game_status": str(normalized.get("game_status", "final")),
                    "game_status_text": str(normalized.get("game_status_text", "Final")),
                    "players": normalized.get("players", {}),
                    "period": str(normalized.get("period", "")),
                    "game_clock": str(normalized.get("game_clock", "")),
                }
            )

        if payload["errors"] and not normalized_games:
            payload["status"] = "error"
        elif payload["errors"]:
            payload["status"] = "partial"
        payload["games"] = normalized_games
        payload["count_games"] = len(normalized_games)
        payload["count_errors"] = len(payload["errors"])
        return payload

    def _historical_game_ids_for_day(self, day: str) -> list[str]:
        schedule_root = self.nba_data_root / "raw" / "schedule"
        if not schedule_root.exists():
            return []

        game_ids: set[str] = set()
        for path in sorted(schedule_root.glob("season=*/season_type=*/schedule.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            rows = payload.get("games", []) if isinstance(payload, dict) else []
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if str(row.get("date", "")).strip() != day:
                    continue
                game_id = str(row.get("game_id", "")).strip()
                if game_id:
                    game_ids.add(game_id)
        return sorted(game_ids)

    def _load_or_fetch_boxscore(self, *, game_id: str, refresh: bool) -> dict[str, Any] | None:
        req = NBADataRequest(
            method="GET",
            path=f"/nba/boxscore/{game_id}",
            params={"game_id": game_id},
            label="nba_boxscore",
        )
        key = req.key()

        if not refresh and self.cache.has_response(key):
            cached = self.cache.load_response(key)
            if isinstance(cached, dict):
                return cached

        local_payload = self._load_boxscore_from_local_manifest(game_id)
        if local_payload is not None and self._normalize_boxscore_game(
            local_payload, fallback_game_id=game_id
        ):
            self.cache.write_request(
                key, {"method": req.method, "path": req.path, "params": req.params}
            )
            self.cache.write_response(key, local_payload)
            self.cache.write_meta(
                key,
                {
                    "label": req.label,
                    "source": "nba_data_manifest",
                    "fetched_at_utc": _now_utc(),
                    "status": "ok",
                },
            )
            return local_payload

        boxscore_url = BOXSCORE_URL_TEMPLATE.format(game_id=game_id)
        try:
            fetched = get_json(boxscore_url, timeout_s=20.0)
        except Exception:
            return None
        if not isinstance(fetched, dict):
            return None

        self.cache.write_request(
            key, {"method": req.method, "path": req.path, "params": req.params}
        )
        self.cache.write_response(key, fetched)
        self.cache.write_meta(
            key,
            {
                "label": req.label,
                "source": "cdn_boxscore",
                "fetched_at_utc": _now_utc(),
                "status": "ok",
            },
        )
        return fetched

    def _load_boxscore_from_local_manifest(self, game_id: str) -> dict[str, Any] | None:
        index = self._boxscore_manifest_index
        if index is None:
            index = self._build_boxscore_manifest_index()
            self._boxscore_manifest_index = index
        path = index.get(game_id)
        if path is None or not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def _build_boxscore_manifest_index(self) -> dict[str, Path]:
        index: dict[str, Path] = {}
        manifests_root = self.nba_data_root / "manifests"
        if not manifests_root.exists():
            return index
        for manifest_path in sorted(manifests_root.glob("season=*/season_type=*/manifest.jsonl")):
            try:
                lines = manifest_path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line in lines:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    row = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue
                game_id = str(row.get("game_id", "")).strip()
                resources = row.get("resources", {})
                if not game_id or not isinstance(resources, dict):
                    continue
                box_row = resources.get("boxscore", {})
                if not isinstance(box_row, dict):
                    continue
                rel_path = str(box_row.get("path", "")).strip()
                if not rel_path:
                    continue
                abs_path = self.nba_data_root / rel_path
                if abs_path.exists():
                    index[game_id] = abs_path
        return index

    def _normalize_boxscore_game(
        self,
        payload: dict[str, Any],
        *,
        fallback_game_id: str,
    ) -> dict[str, Any] | None:
        game_payload = payload.get("game", payload)
        if not isinstance(game_payload, dict):
            return None
        home = game_payload.get("homeTeam", {})
        away = game_payload.get("awayTeam", {})
        if not isinstance(home, dict) or not isinstance(away, dict):
            return None

        home_team = canonical_team_name(
            f"{home.get('teamCity', '')} {home.get('teamName', '')}".strip()
        )
        away_team = canonical_team_name(
            f"{away.get('teamCity', '')} {away.get('teamName', '')}".strip()
        )
        if not home_team or not away_team:
            return None

        game_status_text = str(game_payload.get("gameStatusText", "")).strip()
        game_status = _game_status(game_payload.get("gameStatus"), game_status_text)

        players = _extract_players(game_payload)
        return {
            "game_id": str(game_payload.get("gameId", fallback_game_id)),
            "home_team": home_team,
            "away_team": away_team,
            "game_status": game_status,
            "game_status_text": game_status_text,
            "players": players,
            "period": str(game_payload.get("period", "")),
            "game_clock": str(game_payload.get("gameClock", "")),
        }
