"""CLI entrypoint for prop-ev."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from prop_ev.budget import current_month_utc
from prop_ev.context_sources import (
    fetch_official_injury_links,
    fetch_roster_context,
    fetch_secondary_injuries,
    load_or_fetch_context,
)
from prop_ev.identity_map import load_identity_map, update_identity_map
from prop_ev.normalize import normalize_event_odds, normalize_featured_odds
from prop_ev.odds_client import (
    OddsAPIClient,
    OddsAPIError,
    OddsResponse,
    estimate_event_credits,
    estimate_featured_credits,
    parse_csv,
    regions_equivalent,
)
from prop_ev.playbook import budget_snapshot, compute_live_window, generate_brief_for_snapshot
from prop_ev.settings import Settings
from prop_ev.storage import SnapshotStore, make_snapshot_id, request_hash
from prop_ev.strategy import build_strategy_report, load_jsonl, write_strategy_reports


class CLIError(RuntimeError):
    """User-facing CLI error."""


class CreditLimitError(CLIError):
    """Raised when estimated credits exceed configured cap."""


class OfflineCacheMissError(CLIError):
    """Raised when offline mode is active and cache is missing."""


def _load_bookmaker_whitelist(path: Path) -> list[str]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return []
    enabled = payload.get("enabled", True)
    if isinstance(enabled, bool) and not enabled:
        return []
    raw_books = payload.get("bookmakers", [])
    if not isinstance(raw_books, list):
        return []
    books: list[str] = []
    for value in raw_books:
        if not isinstance(value, str):
            continue
        normalized = value.strip()
        if normalized:
            books.append(normalized)
    # Stable de-duplication while preserving order.
    return list(dict.fromkeys(books))


def _resolve_bookmakers(explicit: str, *, allow_config: bool = True) -> tuple[str, str]:
    explicit_books = parse_csv(explicit)
    if explicit_books:
        return ",".join(explicit_books), "cli"

    if not allow_config:
        return "", "none"

    config_path = Path(
        os.environ.get("PROP_EV_BOOKMAKERS_CONFIG_PATH", "config/bookmakers.json")
    ).resolve()
    books = _load_bookmaker_whitelist(config_path)
    if books:
        return ",".join(books), f"config:{config_path}"
    return "", "none"


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _default_window() -> tuple[str, str]:
    start = _utc_now().replace(hour=0, minute=0, second=0)
    end = start + timedelta(hours=32)
    return _iso(start), _iso(end)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _quota_from_headers(headers: dict[str, str]) -> dict[str, str]:
    return {
        "remaining": headers.get("x-requests-remaining", ""),
        "used": headers.get("x-requests-used", ""),
        "last": headers.get("x-requests-last", ""),
    }


def _print_estimate(estimate: int, max_credits: int) -> None:
    print(f"estimated_credits={estimate} max_credits={max_credits}")


def _enforce_credit_cap(estimate: int, max_credits: int, force: bool) -> None:
    if estimate > max_credits and not force:
        raise CreditLimitError(
            f"estimated credits {estimate} exceed max {max_credits}; use --force to proceed"
        )


def _execute_request(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    label: str,
    path: str,
    params: dict[str, Any],
    fetcher: Callable[[], OddsResponse],
    offline: bool,
    block_paid: bool,
    is_paid: bool,
    refresh: bool,
    resume: bool,
) -> tuple[Any, dict[str, str], str, str]:
    key = request_hash("GET", path, params)
    request_data = {"method": "GET", "path": path, "params": params}
    store.write_request(snapshot_id, key, request_data)

    previous_status = store.request_status(snapshot_id, key)
    if (
        resume
        and not refresh
        and previous_status in {"ok", "cached"}
        and store.has_response(snapshot_id, key)
    ):
        data = store.load_response(snapshot_id, key)
        meta = store.load_meta(snapshot_id, key) or {}
        headers = meta.get("headers", {})
        if not isinstance(headers, dict):
            headers = {}
        normalized_headers = {str(k): str(v) for k, v in headers.items()}
        store.mark_request(
            snapshot_id,
            key,
            label=label,
            path=path,
            params=params,
            status="skipped",
            quota=_quota_from_headers(normalized_headers),
        )
        return data, normalized_headers, "skipped", key

    if not refresh and store.has_response(snapshot_id, key):
        data = store.load_response(snapshot_id, key)
        meta = store.load_meta(snapshot_id, key) or {}
        headers = meta.get("headers", {})
        if not isinstance(headers, dict):
            headers = {}
        normalized_headers = {str(k): str(v) for k, v in headers.items()}
        store.mark_request(
            snapshot_id,
            key,
            label=label,
            path=path,
            params=params,
            status="cached",
            quota=_quota_from_headers(normalized_headers),
        )
        return data, normalized_headers, "cached", key

    if offline or (block_paid and is_paid):
        reason = "offline cache miss" if offline else "paid endpoint blocked cache miss"
        store.mark_request(
            snapshot_id,
            key,
            label=label,
            path=path,
            params=params,
            status="failed",
            error=reason,
        )
        if offline:
            raise OfflineCacheMissError(f"cache miss while offline for {label}")
        raise OfflineCacheMissError(f"cache miss while paid endpoints are blocked for {label}")

    response = fetcher()
    meta = {
        "endpoint": path,
        "status_code": response.status_code,
        "duration_ms": response.duration_ms,
        "retry_count": response.retry_count,
        "headers": response.headers,
        "fetched_at_utc": _iso(_utc_now()),
    }
    store.write_response(snapshot_id, key, response.data)
    store.write_meta(snapshot_id, key, meta)
    store.append_usage(
        endpoint=path,
        request_key=key,
        snapshot_id=snapshot_id,
        status_code=response.status_code,
        duration_ms=response.duration_ms,
        retry_count=response.retry_count,
        headers=response.headers,
        cached=False,
    )
    store.mark_request(
        snapshot_id,
        key,
        label=label,
        path=path,
        params=params,
        status="ok",
        quota=_quota_from_headers(response.headers),
    )
    return response.data, response.headers, "ok", key


def _parse_markets(value: str) -> list[str]:
    markets = parse_csv(value)
    if not markets:
        raise CLIError("at least one market is required")
    return markets


def _write_derived(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    filename: str,
    rows: list[dict[str, Any]],
) -> None:
    path = store.derived_path(snapshot_id, filename)
    store.write_jsonl(path, rows)


def _cmd_snapshot_slate(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_id = args.snapshot_id or make_snapshot_id()
    default_from, default_to = _default_window()
    commence_from = args.commence_from or default_from
    commence_to = args.commence_to or default_to
    markets = _parse_markets(args.markets)
    bookmakers, bookmakers_source = _resolve_bookmakers(
        args.bookmakers,
        allow_config=not bool(getattr(args, "ignore_bookmaker_config", False)),
    )
    regions_factor = regions_equivalent(args.regions, bookmakers)
    estimate = estimate_featured_credits(markets, regions_factor)
    _print_estimate(estimate, args.max_credits)
    _enforce_credit_cap(estimate, args.max_credits, args.force)
    if args.dry_run:
        print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")
        return 0

    settings = Settings.from_env()
    run_config = {
        "mode": "snapshot_slate",
        "sport_key": args.sport_key,
        "markets": markets,
        "regions": args.regions,
        "bookmakers": bookmakers,
        "bookmakers_source": bookmakers_source,
        "commence_from": commence_from,
        "commence_to": commence_to,
    }
    with store.lock_snapshot(snapshot_id):
        store.ensure_snapshot(snapshot_id, run_config=run_config)
        path = f"/sports/{args.sport_key}/odds"
        params: dict[str, Any] = {
            "markets": ",".join(sorted(set(markets))),
            "oddsFormat": "american",
            "dateFormat": "iso",
            "commenceTimeFrom": commence_from,
            "commenceTimeTo": commence_to,
        }
        if bookmakers:
            params["bookmakers"] = bookmakers
        elif args.regions:
            params["regions"] = args.regions

        with OddsAPIClient(settings) as client:
            data, headers, status, key = _execute_request(
                store=store,
                snapshot_id=snapshot_id,
                label="slate_odds",
                path=path,
                params=params,
                fetcher=lambda: client.get_featured_odds(
                    sport_key=args.sport_key,
                    markets=markets,
                    regions=args.regions,
                    bookmakers=bookmakers,
                    commence_from=commence_from,
                    commence_to=commence_to,
                ),
                offline=args.offline,
                block_paid=bool(getattr(args, "block_paid", False)),
                is_paid=True,
                refresh=args.refresh,
                resume=args.resume,
            )
            rows = normalize_featured_odds(data, snapshot_id=snapshot_id, provider="odds_api")
            _write_derived(
                store=store,
                snapshot_id=snapshot_id,
                filename="featured_odds.jsonl",
                rows=rows,
            )
            print(f"snapshot_id={snapshot_id} request_key={key} status={status}")
            print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")
            print(
                "x_requests_last={} x_requests_remaining={}".format(
                    headers.get("x-requests-last", ""),
                    headers.get("x-requests-remaining", ""),
                )
            )
    return 0


def _cmd_snapshot_props(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_id = args.snapshot_id or make_snapshot_id()
    default_from, default_to = _default_window()
    commence_from = args.commence_from or default_from
    commence_to = args.commence_to or default_to
    markets = _parse_markets(args.markets)
    bookmakers, bookmakers_source = _resolve_bookmakers(
        args.bookmakers,
        allow_config=not bool(getattr(args, "ignore_bookmaker_config", False)),
    )

    with store.lock_snapshot(snapshot_id):
        run_config = {
            "mode": "snapshot_props",
            "sport_key": args.sport_key,
            "markets": markets,
            "regions": args.regions,
            "bookmakers": bookmakers,
            "bookmakers_source": bookmakers_source,
            "commence_from": commence_from,
            "commence_to": commence_to,
            "include_links": args.include_links,
            "include_sids": args.include_sids,
            "max_events": args.max_events,
        }
        store.ensure_snapshot(snapshot_id, run_config=run_config)
        if args.dry_run:
            regions_factor = regions_equivalent(args.regions, bookmakers)
            event_count = args.max_events or 0
            estimate = estimate_event_credits(markets, regions_factor, event_count)
            _print_estimate(estimate, args.max_credits)
            print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")
            return 0

        counters: Counter[str] = Counter()
        all_rows: list[dict[str, Any]] = []
        settings = Settings.from_env()
        with OddsAPIClient(settings) as client:
            events_path = f"/sports/{args.sport_key}/events"
            events_params: dict[str, Any] = {
                "dateFormat": "iso",
                "commenceTimeFrom": commence_from,
                "commenceTimeTo": commence_to,
            }
            events_data, events_headers, _, events_key = _execute_request(
                store=store,
                snapshot_id=snapshot_id,
                label="events_list",
                path=events_path,
                params=events_params,
                fetcher=lambda: client.list_events(
                    sport_key=args.sport_key,
                    commence_from=commence_from,
                    commence_to=commence_to,
                ),
                offline=args.offline,
                block_paid=bool(getattr(args, "block_paid", False)),
                is_paid=False,
                refresh=args.refresh,
                resume=args.resume,
            )
            event_list = events_data if isinstance(events_data, list) else []
            event_ids = [str(item.get("id", "")) for item in event_list if isinstance(item, dict)]
            event_ids = [event_id for event_id in event_ids if event_id]
            if args.max_events:
                event_ids = event_ids[: args.max_events]
            regions_factor = regions_equivalent(args.regions, bookmakers)
            estimate = estimate_event_credits(markets, regions_factor, len(event_ids))
            _print_estimate(estimate, args.max_credits)
            _enforce_credit_cap(estimate, args.max_credits, args.force)
            print(
                "events_key={} x_requests_last={} x_requests_remaining={}".format(
                    events_key,
                    events_headers.get("x-requests-last", ""),
                    events_headers.get("x-requests-remaining", ""),
                )
            )
            print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")

            for event_id in event_ids:
                path = f"/sports/{args.sport_key}/events/{event_id}/odds"
                params: dict[str, Any] = {
                    "markets": ",".join(sorted(set(markets))),
                    "oddsFormat": "american",
                    "dateFormat": "iso",
                }
                if bookmakers:
                    params["bookmakers"] = bookmakers
                elif args.regions:
                    params["regions"] = args.regions
                if args.include_links:
                    params["includeLinks"] = "true"
                if args.include_sids:
                    params["includeSids"] = "true"
                try:
                    data, headers, status, key = _execute_request(
                        store=store,
                        snapshot_id=snapshot_id,
                        label=f"event_odds:{event_id}",
                        path=path,
                        params=params,
                        fetcher=lambda event_id=event_id: client.get_event_odds(
                            sport_key=args.sport_key,
                            event_id=event_id,
                            markets=markets,
                            regions=args.regions,
                            bookmakers=bookmakers,
                            include_links=args.include_links,
                            include_sids=args.include_sids,
                        ),
                        offline=args.offline,
                        block_paid=bool(getattr(args, "block_paid", False)),
                        is_paid=True,
                        refresh=args.refresh,
                        resume=args.resume,
                    )
                    rows = normalize_event_odds(data, snapshot_id=snapshot_id, provider="odds_api")
                    all_rows.extend(rows)
                    counters[status] += 1
                    print(
                        (
                            "event_id={} request_key={} status={} "
                            "x_requests_last={} x_requests_remaining={}"
                        ).format(
                            event_id,
                            key,
                            status,
                            headers.get("x-requests-last", ""),
                            headers.get("x-requests-remaining", ""),
                        )
                    )
                except (OddsAPIError, OfflineCacheMissError, ValueError) as exc:
                    counters["failed"] += 1
                    request_key = request_hash("GET", path, params)
                    store.mark_request(
                        snapshot_id,
                        request_key,
                        label=f"event_odds:{event_id}",
                        path=path,
                        params=params,
                        status="failed",
                        error=str(exc),
                    )
                    print(f"event_id={event_id} status=failed error={exc}")

        _write_derived(
            store=store,
            snapshot_id=snapshot_id,
            filename="event_props.jsonl",
            rows=all_rows,
        )
        print(
            "snapshot_id={} succeeded={} cached={} skipped={} failed={}".format(
                snapshot_id,
                counters.get("ok", 0),
                counters.get("cached", 0),
                counters.get("skipped", 0),
                counters.get("failed", 0),
            )
        )
        return 2 if counters.get("failed", 0) > 0 else 0


def _cmd_snapshot_ls(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    if not store.snapshots_dir.exists():
        print("no snapshots")
        return 0

    snapshots = sorted(path for path in store.snapshots_dir.iterdir() if path.is_dir())
    if not snapshots:
        print("no snapshots")
        return 0
    for snapshot_dir in snapshots:
        manifest_path = snapshot_dir / "manifest.json"
        if not manifest_path.exists():
            print(f"{snapshot_dir.name} created_at=unknown requests=0")
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        request_count = len(manifest.get("requests", {}))
        created_at = str(manifest.get("created_at_utc", ""))
        print(f"{snapshot_dir.name} created_at={created_at} requests={request_count}")
    return 0


def _cmd_snapshot_show(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    manifest = store.load_manifest(args.snapshot_id)
    requests = manifest.get("requests", {})
    counts: Counter[str] = Counter()
    if isinstance(requests, dict):
        for value in requests.values():
            if isinstance(value, dict):
                status = str(value.get("status", ""))
                counts[status] += 1
    output = {
        "snapshot_id": manifest.get("snapshot_id", args.snapshot_id),
        "created_at_utc": manifest.get("created_at_utc", ""),
        "schema_version": manifest.get("schema_version", ""),
        "client_version": manifest.get("client_version", ""),
        "quota": manifest.get("quota", {}),
        "request_counts": dict(counts),
    }
    print(json.dumps(output, sort_keys=True, indent=2))
    return 0


def _cmd_snapshot_diff(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    a_dir = store.derived_path(args.a, "")
    b_dir = store.derived_path(args.b, "")
    a_files = {path.name for path in a_dir.glob("*.jsonl")} if a_dir.exists() else set()
    b_files = {path.name for path in b_dir.glob("*.jsonl")} if b_dir.exists() else set()
    all_files = sorted(a_files | b_files)
    if not all_files:
        print("no derived files to diff")
        return 0
    for filename in all_files:
        a_path = a_dir / filename
        b_path = b_dir / filename
        a_lines = set(a_path.read_text(encoding="utf-8").splitlines()) if a_path.exists() else set()
        b_lines = set(b_path.read_text(encoding="utf-8").splitlines()) if b_path.exists() else set()
        added = len(b_lines - a_lines)
        removed = len(a_lines - b_lines)
        print(f"{filename} added={added} removed={removed}")
    return 0


def _cmd_snapshot_verify(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    manifest = store.load_manifest(args.snapshot_id)
    requests = manifest.get("requests", {})
    if not isinstance(requests, dict):
        print("invalid manifest: requests must be object")
        return 2

    missing = 0
    for request_key in requests:
        request_path = store.snapshot_dir(args.snapshot_id) / "requests" / f"{request_key}.json"
        response_path = store.snapshot_dir(args.snapshot_id) / "responses" / f"{request_key}.json"
        meta_path = store.snapshot_dir(args.snapshot_id) / "meta" / f"{request_key}.json"
        if not request_path.exists() or not response_path.exists() or not meta_path.exists():
            missing += 1
            print(
                f"missing_artifacts request_key={request_key} "
                f"request={request_path.exists()} "
                f"response={response_path.exists()} "
                f"meta={meta_path.exists()}"
            )
    print(f"snapshot_id={args.snapshot_id} checked={len(requests)} missing={missing}")
    return 2 if missing else 0


def _cmd_credits_report(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    month = args.month or _utc_now().strftime("%Y-%m")
    usage_path = store.usage_dir / f"usage-{month}.jsonl"
    if not usage_path.exists():
        print(f"no usage ledger for month={month}")
        return 0

    total = 0
    endpoint_totals: Counter[str] = Counter()
    remaining = ""
    for line in usage_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        endpoint = str(item.get("endpoint", ""))
        last_raw = str(item.get("x_requests_last", "0"))
        remaining = str(item.get("x_requests_remaining", remaining))
        try:
            spent = int(last_raw)
        except ValueError:
            spent = 0
        total += spent
        endpoint_totals[endpoint] += spent

    print(f"month={month} total_credits={total} remaining={remaining}")
    for endpoint, spent in endpoint_totals.most_common(10):
        print(f"endpoint={endpoint} credits={spent}")
    return 0


def _cmd_credits_budget(args: argparse.Namespace) -> int:
    markets = _parse_markets(args.markets)
    bookmakers, bookmakers_source = _resolve_bookmakers(args.bookmakers)
    factor = regions_equivalent(args.regions, bookmakers)
    featured = estimate_featured_credits(markets, factor)
    event = estimate_event_credits(markets, factor, args.events)
    print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")
    print(f"regions_equivalent={factor}")
    print(f"featured_estimate={featured}")
    print(f"event_estimate={event}")
    print(f"recommended_max_credits={max(featured, event)}")
    return 0


def _latest_snapshot_id(store: SnapshotStore) -> str:
    snapshots = sorted(path for path in store.snapshots_dir.iterdir() if path.is_dir())
    if not snapshots:
        raise CLIError("no snapshots found")
    return snapshots[-1].name


def _teams_in_scope(event_context: dict[str, dict[str, str]]) -> set[str]:
    teams: set[str] = set()
    for row in event_context.values():
        if not isinstance(row, dict):
            continue
        home = str(row.get("home_team", "")).strip()
        away = str(row.get("away_team", "")).strip()
        if home:
            teams.add(home)
        if away:
            teams.add(away)
    return teams


def _teams_in_scope_from_events(events: list[dict[str, Any]]) -> set[str]:
    teams: set[str] = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        home = str(event.get("home_team", "")).strip()
        away = str(event.get("away_team", "")).strip()
        if home:
            teams.add(home)
        if away:
            teams.add(away)
    return teams


def _preflight_context_for_snapshot(
    *,
    store: SnapshotStore,
    snapshot_id: str,
    teams_in_scope: set[str],
    refresh_context: bool,
    require_official_injuries: bool,
    require_fresh_context: bool,
    injuries_stale_hours: float,
    roster_stale_hours: float,
) -> dict[str, Any]:
    snapshot_dir = store.snapshot_dir(snapshot_id)
    context_dir = snapshot_dir / "context"
    reference_dir = store.root / "reference"
    reference_injuries = reference_dir / "injuries" / "latest.json"
    today_key = _utc_now().strftime("%Y-%m-%d")
    reference_roster_daily = reference_dir / "rosters" / f"roster-{today_key}.json"
    reference_roster_latest = reference_dir / "rosters" / "latest.json"
    injuries_path = context_dir / "injuries.json"
    roster_path = context_dir / "roster.json"
    official_pdf_dir = context_dir / "official_injury_pdf"

    injuries = load_or_fetch_context(
        cache_path=injuries_path,
        offline=False,
        refresh=refresh_context,
        fetcher=lambda: {
            "fetched_at_utc": _iso(_utc_now()),
            "official": fetch_official_injury_links(pdf_cache_dir=official_pdf_dir),
            "secondary": fetch_secondary_injuries(),
        },
        fallback_paths=[reference_injuries],
        write_through_paths=[reference_injuries],
        stale_after_hours=injuries_stale_hours,
    )
    roster = load_or_fetch_context(
        cache_path=roster_path,
        offline=False,
        refresh=refresh_context,
        fetcher=lambda: fetch_roster_context(teams_in_scope=sorted(teams_in_scope)),
        fallback_paths=[reference_roster_daily, reference_roster_latest],
        write_through_paths=[reference_roster_daily, reference_roster_latest],
        stale_after_hours=roster_stale_hours,
    )

    official = injuries.get("official", {}) if isinstance(injuries, dict) else {}
    health_gates: list[str] = []
    if require_official_injuries and not (
        isinstance(official, dict) and official.get("status") == "ok"
    ):
        health_gates.append("official_injury_missing")
    injuries_stale = bool(injuries.get("stale", False)) if isinstance(injuries, dict) else True
    roster_stale = bool(roster.get("stale", False)) if isinstance(roster, dict) else True
    if require_fresh_context and injuries_stale:
        health_gates.append("injuries_context_stale")
    if require_fresh_context and roster_stale:
        health_gates.append("roster_context_stale")

    return {
        "health_gates": health_gates,
        "injuries_status": (
            str(official.get("status", "missing")) if isinstance(official, dict) else "missing"
        ),
        "roster_status": (
            str(roster.get("status", "missing")) if isinstance(roster, dict) else "missing"
        ),
        "injuries_path": str(injuries_path),
        "roster_path": str(roster_path),
    }


def _to_price(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.startswith("+"):
            raw = raw[1:]
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _american_to_decimal(price: int | None) -> float | None:
    if price is None:
        return None
    if price > 0:
        return 1.0 + (price / 100.0)
    if price < 0:
        return 1.0 + (100.0 / abs(price))
    return None


def _ev_from_prob_and_price(probability: float | None, price: int | None) -> float | None:
    if probability is None or probability <= 0 or probability >= 1:
        return None
    decimal_odds = _american_to_decimal(price)
    if decimal_odds is None or decimal_odds <= 1.0:
        return None
    return (probability * (decimal_odds - 1.0)) - (1.0 - probability)


def _format_price(price: int | None) -> str:
    if price is None:
        return ""
    if price > 0:
        return f"+{price}"
    return str(price)


def _price_meets_threshold(price: int | None, threshold: int | None) -> bool:
    if price is None or threshold is None:
        return False
    return price >= threshold


def _row_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("event_id", "")),
        str(row.get("player", "")),
        str(row.get("market", "")),
        str(row.get("point", "")),
    )


def _price_for_side(row: dict[str, Any], side: str) -> tuple[int | None, str]:
    normalized = side.lower().strip()
    if normalized == "under":
        return _to_price(row.get("under_best_price")), str(row.get("under_best_book", ""))
    return _to_price(row.get("over_best_price")), str(row.get("over_best_book", ""))


def _build_discovery_execution_report(
    *,
    discovery_snapshot_id: str,
    execution_snapshot_id: str,
    discovery_report: dict[str, Any],
    execution_report: dict[str, Any],
    top_n: int,
) -> dict[str, Any]:
    discovery_candidates = (
        discovery_report.get("candidates", [])
        if isinstance(discovery_report.get("candidates"), list)
        else []
    )
    execution_candidates = (
        execution_report.get("candidates", [])
        if isinstance(execution_report.get("candidates"), list)
        else []
    )
    discovery_rows = [
        row for row in discovery_candidates if isinstance(row, dict) and bool(row.get("eligible"))
    ]
    execution_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in execution_candidates:
        if not isinstance(row, dict):
            continue
        execution_by_key[_row_key(row)] = row

    actionable: list[dict[str, Any]] = []
    misses: list[dict[str, Any]] = []
    unmatched = 0
    for row in discovery_rows:
        side = str(row.get("recommended_side", "")).lower().strip()
        if side not in {"over", "under"}:
            continue
        key = _row_key(row)
        execution_row = execution_by_key.get(key)
        if execution_row is None:
            unmatched += 1
            continue
        execution_price, execution_book = _price_for_side(execution_row, side)
        discovery_model_p = row.get("model_p_hit")
        if isinstance(discovery_model_p, bool):
            discovery_model_p = None
        if isinstance(discovery_model_p, int):
            discovery_model_p = float(discovery_model_p)
        if isinstance(discovery_model_p, str):
            try:
                discovery_model_p = float(discovery_model_p)
            except ValueError:
                discovery_model_p = None
        if not isinstance(discovery_model_p, float):
            discovery_model_p = None

        play_to = _to_price(row.get("play_to_american"))
        meets_play_to = _price_meets_threshold(execution_price, play_to)
        execution_ev_at_discovery_p = _ev_from_prob_and_price(discovery_model_p, execution_price)
        record = {
            "event_id": str(row.get("event_id", "")),
            "game": str(row.get("game", "")),
            "player": str(row.get("player", "")),
            "market": str(row.get("market", "")),
            "point": row.get("point"),
            "side": side,
            "ticket": (
                f"{row.get('player', '')} {side.upper()} {row.get('point', '')} "
                f"{row.get('market', '')}"
            ).strip(),
            "discovery_price": _to_price(row.get("selected_price")),
            "discovery_book": str(row.get("selected_book", "")),
            "discovery_best_ev": row.get("best_ev"),
            "discovery_play_to": play_to,
            "execution_price": execution_price,
            "execution_book": execution_book,
            "execution_native_best_ev": execution_row.get("best_ev"),
            "execution_ev_at_discovery_p": execution_ev_at_discovery_p,
            "meets_play_to": meets_play_to,
            "tier": str(execution_row.get("tier", "")),
        }
        if meets_play_to:
            actionable.append(record)
        else:
            misses.append(record)

    actionable.sort(
        key=lambda row: (
            -(
                float(row["execution_ev_at_discovery_p"])
                if row["execution_ev_at_discovery_p"] is not None
                else -999.0
            ),
            -(float(row["discovery_best_ev"]) if row["discovery_best_ev"] is not None else -999.0),
            row["game"],
            row["player"],
        )
    )
    misses.sort(
        key=lambda row: (
            -(float(row["discovery_best_ev"]) if row["discovery_best_ev"] is not None else -999.0),
            row["game"],
            row["player"],
        )
    )

    summary = {
        "discovery_snapshot_id": discovery_snapshot_id,
        "execution_snapshot_id": execution_snapshot_id,
        "discovery_eligible_rows": len(discovery_rows),
        "matched_execution_rows": len(actionable) + len(misses),
        "unmatched_rows": unmatched,
        "actionable_rows": len(actionable),
        "miss_rows": len(misses),
    }

    return {
        "generated_at_utc": _iso(_utc_now()),
        "summary": summary,
        "actionable": actionable[:top_n],
        "misses": misses[:top_n],
    }


def _render_discovery_execution_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    actionable = report.get("actionable", []) if isinstance(report.get("actionable"), list) else []
    misses = report.get("misses", []) if isinstance(report.get("misses"), list) else []

    lines: list[str] = []
    lines.append("# Discovery vs Execution Report")
    lines.append("")
    lines.append("- discovery_snapshot_id: `{}`".format(summary.get("discovery_snapshot_id", "")))
    lines.append("- execution_snapshot_id: `{}`".format(summary.get("execution_snapshot_id", "")))
    lines.append(
        "- discovery_eligible_rows: `{}`".format(summary.get("discovery_eligible_rows", 0))
    )
    lines.append("- matched_execution_rows: `{}`".format(summary.get("matched_execution_rows", 0)))
    lines.append("- actionable_rows: `{}`".format(summary.get("actionable_rows", 0)))
    lines.append("- misses: `{}`".format(summary.get("miss_rows", 0)))
    lines.append("")
    lines.append(
        "Actionable rows are discovery-signaled plays where execution books still meet "
        "discovery PLAY-TO."
    )
    lines.append("")
    lines.append("## Actionable")
    lines.append("")
    if not actionable:
        lines.append("- none")
    else:
        lines.append(
            "| Game | Ticket | Discovery (book/price) | Execution (book/price) | "
            "Discovery PLAY-TO | Exec EV@Discovery p |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for row in actionable:
            if not isinstance(row, dict):
                continue
            ev = row.get("execution_ev_at_discovery_p")
            ev_text = f"{(ev * 100.0):+.2f}%" if isinstance(ev, (int, float)) else ""
            lines.append(
                "| {} | {} | {} {} | {} {} | {} | {} |".format(
                    row.get("game", ""),
                    row.get("ticket", ""),
                    row.get("discovery_book", ""),
                    _format_price(_to_price(row.get("discovery_price"))),
                    row.get("execution_book", ""),
                    _format_price(_to_price(row.get("execution_price"))),
                    _format_price(_to_price(row.get("discovery_play_to"))),
                    ev_text,
                )
            )
    lines.append("")
    lines.append("## Near Misses")
    lines.append("")
    if not misses:
        lines.append("- none")
    else:
        lines.append(
            "| Game | Ticket | Discovery (book/price) | Execution (book/price) | "
            "Discovery PLAY-TO |"
        )
        lines.append("| --- | --- | --- | --- | --- |")
        for row in misses:
            if not isinstance(row, dict):
                continue
            lines.append(
                "| {} | {} | {} {} | {} {} | {} |".format(
                    row.get("game", ""),
                    row.get("ticket", ""),
                    row.get("discovery_book", ""),
                    _format_price(_to_price(row.get("discovery_price"))),
                    row.get("execution_book", ""),
                    _format_price(_to_price(row.get("execution_price"))),
                    _format_price(_to_price(row.get("discovery_play_to"))),
                )
            )
    lines.append("")
    return "\n".join(lines)


def _write_discovery_execution_reports(
    *,
    store: SnapshotStore,
    execution_snapshot_id: str,
    report: dict[str, Any],
) -> tuple[Path, Path]:
    reports_dir = store.snapshot_dir(execution_snapshot_id) / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "discovery-execution.json"
    md_path = reports_dir / "discovery-execution.md"
    json_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(_render_discovery_execution_markdown(report), encoding="utf-8")
    return json_path, md_path


def _cmd_strategy_run(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    snapshot_id = args.snapshot_id or _latest_snapshot_id(store)
    snapshot_dir = store.snapshot_dir(snapshot_id)
    manifest = store.load_manifest(snapshot_id)
    derived_path = snapshot_dir / "derived" / "event_props.jsonl"
    if not derived_path.exists():
        raise CLIError(f"missing derived props file: {derived_path}")

    rows = load_jsonl(derived_path)
    event_context = _load_event_context(store, snapshot_id, manifest)
    slate_rows = _load_slate_rows(store, snapshot_id)
    if not slate_rows and not args.offline and not bool(getattr(args, "block_paid", False)):
        _hydrate_slate_for_strategy(store, snapshot_id, manifest)
        manifest = store.load_manifest(snapshot_id)
        event_context = _load_event_context(store, snapshot_id, manifest)
        slate_rows = _load_slate_rows(store, snapshot_id)

    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    stale_quote_minutes = _env_int("PROP_EV_STRATEGY_STALE_QUOTE_MINUTES", 20)
    injuries_stale_hours = _env_float("PROP_EV_CONTEXT_INJURIES_STALE_HOURS", 6.0)
    roster_stale_hours = _env_float("PROP_EV_CONTEXT_ROSTER_STALE_HOURS", 24.0)
    require_fresh_context = _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True)
    reference_dir = store.root / "reference"
    reference_injuries = reference_dir / "injuries" / "latest.json"
    today_key = _utc_now().strftime("%Y-%m-%d")
    reference_roster_daily = reference_dir / "rosters" / f"roster-{today_key}.json"
    reference_roster_latest = reference_dir / "rosters" / "latest.json"
    identity_map_path = reference_dir / "player_identity_map.json"
    teams_in_scope = sorted(_teams_in_scope(event_context))

    context_dir = snapshot_dir / "context"
    injuries_path = context_dir / "injuries.json"
    roster_path = context_dir / "roster.json"
    official_pdf_dir = context_dir / "official_injury_pdf"
    injuries = load_or_fetch_context(
        cache_path=injuries_path,
        offline=args.offline,
        refresh=args.refresh_context,
        fetcher=lambda: {
            "fetched_at_utc": _iso(_utc_now()),
            "official": fetch_official_injury_links(pdf_cache_dir=official_pdf_dir),
            "secondary": fetch_secondary_injuries(),
        },
        fallback_paths=[reference_injuries],
        write_through_paths=[reference_injuries],
        stale_after_hours=injuries_stale_hours,
    )
    roster = load_or_fetch_context(
        cache_path=roster_path,
        offline=args.offline,
        refresh=args.refresh_context,
        fetcher=lambda: fetch_roster_context(teams_in_scope=teams_in_scope),
        fallback_paths=[reference_roster_daily, reference_roster_latest],
        write_through_paths=[reference_roster_daily, reference_roster_latest],
        stale_after_hours=roster_stale_hours,
    )
    identity_summary = update_identity_map(
        path=identity_map_path,
        rows=rows,
        roster=roster if isinstance(roster, dict) else None,
        event_context=event_context,
    )
    player_identity_map = load_identity_map(identity_map_path)
    report = build_strategy_report(
        snapshot_id=snapshot_id,
        manifest=manifest,
        rows=rows,
        top_n=args.top_n,
        injuries=injuries,
        roster=roster,
        event_context=event_context,
        slate_rows=slate_rows,
        player_identity_map=player_identity_map,
        min_ev=args.min_ev,
        allow_tier_b=args.allow_tier_b,
        require_official_injuries=require_official_injuries,
        stale_quote_minutes=stale_quote_minutes,
        require_fresh_context=require_fresh_context,
    )
    json_path, md_path = write_strategy_reports(
        snapshot_dir=snapshot_dir,
        report=report,
        top_n=args.top_n,
    )
    summary = report.get("summary", {})
    health = (
        report.get("health_report", {}) if isinstance(report.get("health_report"), dict) else {}
    )
    health_gates = (
        health.get("health_gates", []) if isinstance(health.get("health_gates"), list) else []
    )
    print(
        (
            "snapshot_id={} strategy_status={} strategy_mode={} events={} candidate_lines={} "
            "tier_a={} tier_b={} eligible={}"
        ).format(
            snapshot_id,
            report.get("strategy_status", ""),
            report.get("strategy_mode", ""),
            summary.get("events", 0),
            summary.get("candidate_lines", 0),
            summary.get("tier_a_lines", 0),
            summary.get("tier_b_lines", 0),
            summary.get("eligible_lines", 0),
        )
    )
    print(f"health_gates={','.join(health_gates) if health_gates else 'none'}")
    print(f"report_json={json_path}")
    print(f"report_md={md_path}")
    print(f"report_card={snapshot_dir / 'reports' / 'strategy-card.md'}")
    print(f"identity_map={identity_summary['path']} entries={identity_summary['player_entries']}")
    print(f"injuries_context={injuries_path}")
    print(f"roster_context={roster_path}")
    return 0


def _load_slate_rows(store: SnapshotStore, snapshot_id: str) -> list[dict[str, Any]]:
    path = store.derived_path(snapshot_id, "featured_odds.jsonl")
    if not path.exists():
        return []
    return load_jsonl(path)


def _derive_window_from_events(
    event_context: dict[str, dict[str, str]] | None,
) -> tuple[str, str]:
    default_from, default_to = _default_window()
    if not isinstance(event_context, dict) or not event_context:
        return default_from, default_to

    times: list[datetime] = []
    for row in event_context.values():
        if not isinstance(row, dict):
            continue
        raw = str(row.get("commence_time", ""))
        if not raw:
            continue
        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        times.append(parsed.astimezone(UTC))

    if not times:
        return default_from, default_to

    start = min(times).replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)
    end = max(times).replace(minute=0, second=0, microsecond=0) + timedelta(hours=4)
    return _iso(start), _iso(end)


def _hydrate_slate_for_strategy(
    store: SnapshotStore, snapshot_id: str, manifest: dict[str, Any]
) -> None:
    run_config = manifest.get("run_config", {}) if isinstance(manifest, dict) else {}
    if not isinstance(run_config, dict):
        run_config = {}

    event_context = _load_event_context(store, snapshot_id, manifest)
    commence_from, commence_to = _derive_window_from_events(event_context)
    sport_key = str(run_config.get("sport_key", "basketball_nba")) or "basketball_nba"
    regions = str(run_config.get("regions", "us")) or "us"
    bookmakers = str(run_config.get("bookmakers", ""))

    args = argparse.Namespace(
        sport_key=sport_key,
        markets="spreads,totals",
        regions=regions,
        bookmakers=bookmakers,
        snapshot_id=snapshot_id,
        commence_from=commence_from,
        commence_to=commence_to,
        max_credits=10,
        force=False,
        refresh=False,
        resume=True,
        offline=False,
        dry_run=False,
    )
    code = _cmd_snapshot_slate(args)
    if code != 0:
        print(
            "warning: could not fetch slate featured odds during strategy run; "
            "continuing without spread/total context",
            file=sys.stderr,
        )


def _load_event_context(
    store: SnapshotStore, snapshot_id: str, manifest: dict[str, Any]
) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    requests = manifest.get("requests", {})
    if not isinstance(requests, dict):
        return result
    for request_key, row in requests.items():
        if not isinstance(row, dict):
            continue
        label = str(row.get("label", ""))
        payload = store.load_response(snapshot_id, str(request_key))
        if label == "events_list" and isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                event_id = str(item.get("id", ""))
                if not event_id:
                    continue
                result[event_id] = {
                    "home_team": str(item.get("home_team", "")),
                    "away_team": str(item.get("away_team", "")),
                    "commence_time": str(item.get("commence_time", "")),
                }
            continue

        if label == "slate_odds" and isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                event_id = str(item.get("id", ""))
                if not event_id:
                    continue
                result[event_id] = {
                    "home_team": str(item.get("home_team", "")),
                    "away_team": str(item.get("away_team", "")),
                    "commence_time": str(item.get("commence_time", "")),
                }
            continue

        if not label.startswith("event_odds:"):
            continue

        if not isinstance(payload, dict):
            continue
        event_id = str(payload.get("id", ""))
        if not event_id:
            continue
        result[event_id] = {
            "home_team": str(payload.get("home_team", "")),
            "away_team": str(payload.get("away_team", "")),
            "commence_time": str(payload.get("commence_time", "")),
        }
    return result


def _run_snapshot_props_for_playbook(args: argparse.Namespace, snapshot_id: str) -> int:
    snapshot_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets=args.markets,
        regions=args.regions,
        bookmakers=args.bookmakers,
        snapshot_id=snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        include_links=args.include_links,
        include_sids=args.include_sids,
        max_events=args.max_events,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=bool(getattr(args, "block_paid", False)),
        dry_run=False,
    )
    return _cmd_snapshot_props(snapshot_args)


def _run_snapshot_slate_for_playbook(args: argparse.Namespace, snapshot_id: str) -> int:
    snapshot_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets="spreads,totals",
        regions=args.regions,
        bookmakers=args.bookmakers,
        snapshot_id=snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        max_credits=max(2, min(args.max_credits, 10)),
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=bool(getattr(args, "block_paid", False)),
        dry_run=False,
    )
    return _cmd_snapshot_slate(snapshot_args)


def _run_snapshot_bundle_for_playbook(args: argparse.Namespace, snapshot_id: str) -> int:
    slate_code = _run_snapshot_slate_for_playbook(args, snapshot_id)
    props_code = _run_snapshot_props_for_playbook(args, snapshot_id)
    return 0 if slate_code == 0 and props_code == 0 else 2


def _run_strategy_for_playbook(
    *,
    snapshot_id: str,
    top_n: int,
    min_ev: float,
    allow_tier_b: bool,
    offline: bool,
    block_paid: bool,
    refresh_context: bool,
) -> int:
    strategy_args = argparse.Namespace(
        snapshot_id=snapshot_id,
        top_n=top_n,
        min_ev=min_ev,
        allow_tier_b=allow_tier_b,
        offline=offline,
        block_paid=block_paid,
        refresh_context=refresh_context,
    )
    return _cmd_strategy_run(strategy_args)


def _cmd_playbook_run(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    settings = Settings.from_env()
    month = args.month or current_month_utc()
    top_n = args.top_n if args.top_n > 0 else settings.playbook_top_n
    per_game_top_n = (
        args.per_game_top_n if args.per_game_top_n > 0 else settings.playbook_per_game_top_n
    )
    start_budget = budget_snapshot(store=store, settings=settings, month=month)
    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    require_fresh_context = _env_bool("PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT", True)
    injuries_stale_hours = _env_float("PROP_EV_CONTEXT_INJURIES_STALE_HOURS", 6.0)
    roster_stale_hours = _env_float("PROP_EV_CONTEXT_ROSTER_STALE_HOURS", 24.0)

    snapshot_id = args.snapshot_id
    mode = "explicit_snapshot" if snapshot_id else ""
    live_window: dict[str, Any] = {
        "status": "not_checked",
        "within_window": False,
        "event_count": 0,
    }
    preflight_context: dict[str, Any] = {"status": "not_run", "health_gates": []}

    if not snapshot_id:
        if args.offline:
            snapshot_id = _latest_snapshot_id(store)
            mode = "offline_forced_latest"
        else:
            default_from, default_to = _default_window()
            commence_from = args.commence_from or default_from
            commence_to = args.commence_to or default_to
            events: list[dict[str, Any]] = []
            try:
                with OddsAPIClient(settings) as client:
                    response = client.list_events(
                        sport_key=args.sport_key,
                        commence_from=commence_from,
                        commence_to=commence_to,
                    )
                events_data = response.data if isinstance(response.data, list) else []
                events = [item for item in events_data if isinstance(item, dict)]
                live_window = compute_live_window(
                    events,
                    now=_utc_now(),
                    pre_tip_h=settings.playbook_live_window_pre_tip_h,
                    post_tip_h=settings.playbook_live_window_post_tip_h,
                )
            except (OddsAPIError, ValueError) as exc:
                live_window = {
                    "status": "events_lookup_failed",
                    "within_window": False,
                    "event_count": 0,
                    "error": str(exc),
                }

            odds_cap_reached = bool(start_budget["odds"].get("cap_reached", False))
            block_paid = bool(getattr(args, "block_paid", False))
            should_live = (
                bool(live_window.get("within_window", False))
                and not odds_cap_reached
                and not block_paid
            )
            if should_live:
                snapshot_id = make_snapshot_id()
                preflight_context = _preflight_context_for_snapshot(
                    store=store,
                    snapshot_id=snapshot_id,
                    teams_in_scope=_teams_in_scope_from_events(events),
                    refresh_context=args.refresh_context,
                    require_official_injuries=require_official_injuries,
                    require_fresh_context=require_fresh_context,
                    injuries_stale_hours=injuries_stale_hours,
                    roster_stale_hours=roster_stale_hours,
                )
                preflight_gates = preflight_context.get("health_gates", [])
                if isinstance(preflight_gates, list) and preflight_gates:
                    try:
                        snapshot_id = _latest_snapshot_id(store)
                        mode = "offline_context_gate"
                    except CLIError:
                        raise CLIError(
                            "context preflight failed and no cached snapshot is available; "
                            f"gates={','.join(preflight_gates)}"
                        ) from None
                else:
                    mode = "live_snapshot"
                    snapshot_code = _run_snapshot_bundle_for_playbook(args, snapshot_id)
                    if snapshot_code != 0:
                        raise CLIError(
                            f"live snapshot failed with exit code {snapshot_code} for {snapshot_id}"
                        )
            else:
                try:
                    snapshot_id = _latest_snapshot_id(store)
                    if block_paid:
                        mode = "offline_paid_block"
                    elif odds_cap_reached:
                        mode = "offline_odds_cap"
                    else:
                        mode = "offline_outside_window"
                except CLIError:
                    if odds_cap_reached or block_paid:
                        raise CLIError(
                            "paid odds calls are blocked (or odds cap reached) and no cached "
                            "snapshot is available"
                        ) from None
                    snapshot_id = make_snapshot_id()
                    mode = "live_bootstrap"
                    preflight_context = _preflight_context_for_snapshot(
                        store=store,
                        snapshot_id=snapshot_id,
                        teams_in_scope=_teams_in_scope_from_events(events),
                        refresh_context=args.refresh_context,
                        require_official_injuries=require_official_injuries,
                        require_fresh_context=require_fresh_context,
                        injuries_stale_hours=injuries_stale_hours,
                        roster_stale_hours=roster_stale_hours,
                    )
                    preflight_gates = preflight_context.get("health_gates", [])
                    if isinstance(preflight_gates, list) and preflight_gates:
                        raise CLIError(
                            "context preflight failed; refusing paid odds fetch in bootstrap mode; "
                            f"gates={','.join(preflight_gates)}"
                        ) from None
                    snapshot_code = _run_snapshot_bundle_for_playbook(args, snapshot_id)
                    if snapshot_code != 0:
                        raise CLIError(
                            "bootstrap live snapshot failed with exit code "
                            f"{snapshot_code} for {snapshot_id}"
                        ) from None

    if not snapshot_id:
        raise CLIError("failed to resolve snapshot id")

    strategy_offline = bool(args.offline or mode.startswith("offline"))
    refresh_context = bool(args.refresh_context and not strategy_offline)
    if bool(args.refresh_context) and strategy_offline:
        print(f"note=refresh_context_ignored_in_offline_mode snapshot_id={snapshot_id}")
    strategy_code = _run_strategy_for_playbook(
        snapshot_id=snapshot_id,
        top_n=args.strategy_top_n,
        min_ev=args.min_ev,
        allow_tier_b=args.allow_tier_b,
        offline=strategy_offline,
        block_paid=bool(getattr(args, "block_paid", False)),
        refresh_context=refresh_context,
    )
    if strategy_code != 0:
        return strategy_code

    brief = generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=snapshot_id,
        top_n=top_n,
        llm_refresh=args.refresh_llm,
        llm_offline=args.offline,
        per_game_top_n=per_game_top_n,
        game_card_min_ev=max(0.0, args.min_ev),
        month=month,
    )
    end_budget = budget_snapshot(store=store, settings=settings, month=month)
    print(
        "snapshot_id={} mode={} within_window={} odds_cap_reached={}".format(
            snapshot_id,
            mode,
            live_window.get("within_window", False),
            end_budget["odds"].get("cap_reached", False),
        )
    )
    preflight_gates = (
        preflight_context.get("health_gates", [])
        if isinstance(preflight_context.get("health_gates"), list)
        else []
    )
    if preflight_gates:
        print(f"context_preflight_gates={','.join(preflight_gates)}")
    print(f"strategy_brief_md={brief['report_markdown']}")
    print(f"strategy_brief_tex={brief['report_tex']}")
    print(f"strategy_brief_pdf={brief['report_pdf']}")
    print(f"strategy_brief_meta={brief['report_meta']}")
    print(
        "llm_pass1_status={} llm_pass2_status={} pdf_status={}".format(
            brief.get("llm_pass1_status", ""),
            brief.get("llm_pass2_status", ""),
            brief.get("pdf_status", ""),
        )
    )
    return 0


def _cmd_playbook_render(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    settings = Settings.from_env()
    month = args.month or current_month_utc()
    top_n = args.top_n if args.top_n > 0 else settings.playbook_top_n
    per_game_top_n = (
        args.per_game_top_n if args.per_game_top_n > 0 else settings.playbook_per_game_top_n
    )
    snapshot_id = args.snapshot_id
    refresh_context = bool(args.refresh_context and not args.offline)
    if bool(args.refresh_context) and bool(args.offline):
        print(f"note=refresh_context_ignored_in_offline_mode snapshot_id={snapshot_id}")
    strategy_json = store.snapshot_dir(snapshot_id) / "reports" / "strategy-report.json"
    if not strategy_json.exists():
        code = _run_strategy_for_playbook(
            snapshot_id=snapshot_id,
            top_n=args.strategy_top_n,
            min_ev=args.min_ev,
            allow_tier_b=args.allow_tier_b,
            offline=args.offline,
            block_paid=bool(getattr(args, "block_paid", False)),
            refresh_context=refresh_context,
        )
        if code != 0:
            return code
    brief = generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=snapshot_id,
        top_n=top_n,
        llm_refresh=args.refresh_llm,
        llm_offline=args.offline,
        per_game_top_n=per_game_top_n,
        game_card_min_ev=max(0.0, args.min_ev),
        month=month,
    )
    print(f"snapshot_id={snapshot_id}")
    print(f"strategy_brief_md={brief['report_markdown']}")
    print(f"strategy_brief_tex={brief['report_tex']}")
    print(f"strategy_brief_pdf={brief['report_pdf']}")
    print(f"strategy_brief_meta={brief['report_meta']}")
    return 0


def _cmd_playbook_budget(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    settings = Settings.from_env()
    month = args.month or current_month_utc()
    payload = budget_snapshot(store=store, settings=settings, month=month)
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0


def _cmd_playbook_discover_execute(args: argparse.Namespace) -> int:
    store = SnapshotStore(os.environ.get("PROP_EV_DATA_DIR", "data/odds_api"))
    settings = Settings.from_env()
    month = args.month or current_month_utc()
    base_snapshot = args.base_snapshot_id or make_snapshot_id()
    discovery_snapshot_id = f"{base_snapshot}-discover"
    execution_snapshot_id = f"{base_snapshot}-execute"

    discovery_slate_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets="spreads,totals",
        regions=args.discovery_regions,
        bookmakers="",
        ignore_bookmaker_config=True,
        snapshot_id=discovery_snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=False,
        dry_run=False,
    )
    discovery_props_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets=args.markets,
        regions=args.discovery_regions,
        bookmakers="",
        ignore_bookmaker_config=True,
        snapshot_id=discovery_snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        include_links=args.include_links,
        include_sids=args.include_sids,
        max_events=args.max_events,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=False,
        dry_run=False,
    )
    execution_slate_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets="spreads,totals",
        regions=args.execution_regions,
        bookmakers=args.execution_bookmakers,
        ignore_bookmaker_config=False,
        snapshot_id=execution_snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=False,
        dry_run=False,
    )
    execution_props_args = argparse.Namespace(
        sport_key=args.sport_key,
        markets=args.markets,
        regions=args.execution_regions,
        bookmakers=args.execution_bookmakers,
        ignore_bookmaker_config=False,
        snapshot_id=execution_snapshot_id,
        commence_from=args.commence_from,
        commence_to=args.commence_to,
        include_links=args.include_links,
        include_sids=args.include_sids,
        max_events=args.max_events,
        max_credits=args.max_credits,
        force=args.force,
        refresh=args.refresh,
        resume=args.resume,
        offline=False,
        block_paid=False,
        dry_run=False,
    )

    if _cmd_snapshot_slate(discovery_slate_args) != 0:
        return 2
    if _cmd_snapshot_props(discovery_props_args) != 0:
        return 2
    if (
        _run_strategy_for_playbook(
            snapshot_id=discovery_snapshot_id,
            top_n=args.strategy_top_n,
            min_ev=args.min_ev,
            allow_tier_b=args.allow_tier_b,
            offline=False,
            block_paid=False,
            refresh_context=args.refresh_context,
        )
        != 0
    ):
        return 2

    if _cmd_snapshot_slate(execution_slate_args) != 0:
        return 2
    if _cmd_snapshot_props(execution_props_args) != 0:
        return 2
    if (
        _run_strategy_for_playbook(
            snapshot_id=execution_snapshot_id,
            top_n=args.strategy_top_n,
            min_ev=args.min_ev,
            allow_tier_b=args.allow_tier_b,
            offline=False,
            block_paid=False,
            refresh_context=args.refresh_context,
        )
        != 0
    ):
        return 2

    discovery_report_path = (
        store.snapshot_dir(discovery_snapshot_id) / "reports" / "strategy-report.json"
    )
    execution_report_path = (
        store.snapshot_dir(execution_snapshot_id) / "reports" / "strategy-report.json"
    )
    discovery_report = json.loads(discovery_report_path.read_text(encoding="utf-8"))
    execution_report = json.loads(execution_report_path.read_text(encoding="utf-8"))
    compare_report = _build_discovery_execution_report(
        discovery_snapshot_id=discovery_snapshot_id,
        execution_snapshot_id=execution_snapshot_id,
        discovery_report=discovery_report,
        execution_report=execution_report,
        top_n=args.top_n,
    )
    compare_json, compare_md = _write_discovery_execution_reports(
        store=store,
        execution_snapshot_id=execution_snapshot_id,
        report=compare_report,
    )

    brief = generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=execution_snapshot_id,
        top_n=max(1, args.top_n),
        llm_refresh=args.refresh_llm,
        llm_offline=args.offline,
        per_game_top_n=max(1, getattr(args, "per_game_top_n", 5)),
        game_card_min_ev=max(0.0, args.min_ev),
        month=month,
    )

    summary = compare_report.get("summary", {})
    print(f"discovery_snapshot_id={discovery_snapshot_id}")
    print(f"execution_snapshot_id={execution_snapshot_id}")
    print(
        "actionable_rows={} matched_rows={} discovery_eligible_rows={}".format(
            summary.get("actionable_rows", 0),
            summary.get("matched_execution_rows", 0),
            summary.get("discovery_eligible_rows", 0),
        )
    )
    print(f"discovery_execution_json={compare_json}")
    print(f"discovery_execution_md={compare_md}")
    print(f"strategy_brief_md={brief['report_markdown']}")
    print(f"strategy_brief_pdf={brief['report_pdf']}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prop-ev")
    subparsers = parser.add_subparsers(dest="command")

    snapshot = subparsers.add_parser("snapshot", help="Create and inspect snapshots")
    snapshot_subparsers = snapshot.add_subparsers(dest="snapshot_command")

    snapshot_slate = snapshot_subparsers.add_parser("slate", help="Fetch slate featured odds")
    snapshot_slate.set_defaults(func=_cmd_snapshot_slate)
    snapshot_slate.add_argument("--sport-key", default="basketball_nba")
    snapshot_slate.add_argument("--markets", default="spreads,totals")
    snapshot_slate.add_argument("--regions", default="us")
    snapshot_slate.add_argument("--bookmakers", default="")
    snapshot_slate.add_argument("--snapshot-id", default="")
    snapshot_slate.add_argument("--commence-from", default="")
    snapshot_slate.add_argument("--commence-to", default="")
    snapshot_slate.add_argument("--max-credits", type=int, default=20)
    snapshot_slate.add_argument("--force", action="store_true")
    snapshot_slate.add_argument("--refresh", action="store_true")
    snapshot_slate.add_argument("--resume", action="store_true")
    snapshot_slate.add_argument("--offline", action="store_true")
    snapshot_slate.add_argument("--block-paid", action="store_true")
    snapshot_slate.add_argument("--dry-run", action="store_true")

    snapshot_props = snapshot_subparsers.add_parser("props", help="Fetch per-event prop odds")
    snapshot_props.set_defaults(func=_cmd_snapshot_props)
    snapshot_props.add_argument("--sport-key", default="basketball_nba")
    snapshot_props.add_argument("--markets", default="player_points")
    snapshot_props.add_argument("--regions", default="us")
    snapshot_props.add_argument("--bookmakers", default="")
    snapshot_props.add_argument("--snapshot-id", default="")
    snapshot_props.add_argument("--commence-from", default="")
    snapshot_props.add_argument("--commence-to", default="")
    snapshot_props.add_argument("--include-links", action="store_true")
    snapshot_props.add_argument("--include-sids", action="store_true")
    snapshot_props.add_argument("--max-events", type=int, default=0)
    snapshot_props.add_argument("--max-credits", type=int, default=20)
    snapshot_props.add_argument("--force", action="store_true")
    snapshot_props.add_argument("--refresh", action="store_true")
    snapshot_props.add_argument("--resume", action="store_true")
    snapshot_props.add_argument("--offline", action="store_true")
    snapshot_props.add_argument("--block-paid", action="store_true")
    snapshot_props.add_argument("--dry-run", action="store_true")

    snapshot_ls = snapshot_subparsers.add_parser("ls", help="List snapshots")
    snapshot_ls.set_defaults(func=_cmd_snapshot_ls)

    snapshot_show = snapshot_subparsers.add_parser("show", help="Show snapshot summary")
    snapshot_show.set_defaults(func=_cmd_snapshot_show)
    snapshot_show.add_argument("--snapshot-id", required=True)

    snapshot_diff = snapshot_subparsers.add_parser("diff", help="Diff derived snapshot outputs")
    snapshot_diff.set_defaults(func=_cmd_snapshot_diff)
    snapshot_diff.add_argument("--a", required=True)
    snapshot_diff.add_argument("--b", required=True)

    snapshot_verify = snapshot_subparsers.add_parser("verify", help="Verify snapshot artifacts")
    snapshot_verify.set_defaults(func=_cmd_snapshot_verify)
    snapshot_verify.add_argument("--snapshot-id", required=True)

    credits = subparsers.add_parser("credits", help="Credit tooling")
    credits_subparsers = credits.add_subparsers(dest="credits_command")

    credits_report = credits_subparsers.add_parser("report", help="Report usage ledger")
    credits_report.set_defaults(func=_cmd_credits_report)
    credits_report.add_argument("--month", default="")

    credits_budget = credits_subparsers.add_parser("budget", help="Estimate budget")
    credits_budget.set_defaults(func=_cmd_credits_budget)
    credits_budget.add_argument("--events", type=int, default=0)
    credits_budget.add_argument("--markets", default="player_points")
    credits_budget.add_argument("--regions", default="us")
    credits_budget.add_argument("--bookmakers", default="")

    strategy = subparsers.add_parser("strategy", help="Run offline strategy reports")
    strategy_subparsers = strategy.add_subparsers(dest="strategy_command")

    strategy_run = strategy_subparsers.add_parser("run", help="Generate strategy report")
    strategy_run.set_defaults(func=_cmd_strategy_run)
    strategy_run.add_argument("--snapshot-id", default="")
    strategy_run.add_argument("--top-n", type=int, default=25)
    strategy_run.add_argument("--min-ev", type=float, default=0.01)
    strategy_run.add_argument("--allow-tier-b", action="store_true")
    strategy_run.add_argument("--offline", action="store_true")
    strategy_run.add_argument("--block-paid", action="store_true")
    strategy_run.add_argument("--refresh-context", action="store_true")

    playbook = subparsers.add_parser("playbook", help="Run playbook briefs")
    playbook_subparsers = playbook.add_subparsers(dest="playbook_command")

    playbook_run = playbook_subparsers.add_parser("run", help="Run live/offline playbook flow")
    playbook_run.set_defaults(func=_cmd_playbook_run)
    playbook_run.add_argument("--snapshot-id", default="")
    playbook_run.add_argument("--sport-key", default="basketball_nba")
    playbook_run.add_argument("--markets", default="player_points")
    playbook_run.add_argument("--regions", default="us")
    playbook_run.add_argument("--bookmakers", default="")
    playbook_run.add_argument("--commence-from", default="")
    playbook_run.add_argument("--commence-to", default="")
    playbook_run.add_argument(
        "--include-links", dest="include_links", action="store_true", default=True
    )
    playbook_run.add_argument("--no-include-links", dest="include_links", action="store_false")
    playbook_run.add_argument(
        "--include-sids", dest="include_sids", action="store_true", default=True
    )
    playbook_run.add_argument("--no-include-sids", dest="include_sids", action="store_false")
    playbook_run.add_argument("--max-events", type=int, default=10)
    playbook_run.add_argument("--max-credits", type=int, default=20)
    playbook_run.add_argument("--force", action="store_true")
    playbook_run.add_argument("--refresh", action="store_true")
    playbook_run.add_argument("--resume", action="store_true")
    playbook_run.add_argument("--offline", action="store_true")
    playbook_run.add_argument("--block-paid", action="store_true")
    playbook_run.add_argument("--refresh-context", action="store_true")
    playbook_run.add_argument("--refresh-llm", action="store_true")
    playbook_run.add_argument("--top-n", type=int, default=0)
    playbook_run.add_argument("--per-game-top-n", type=int, default=0)
    playbook_run.add_argument("--strategy-top-n", type=int, default=25)
    playbook_run.add_argument("--min-ev", type=float, default=0.01)
    playbook_run.add_argument("--allow-tier-b", action="store_true")
    playbook_run.add_argument("--month", default="")

    playbook_render = playbook_subparsers.add_parser(
        "render", help="Render playbook briefs for an existing snapshot"
    )
    playbook_render.set_defaults(func=_cmd_playbook_render)
    playbook_render.add_argument("--snapshot-id", required=True)
    playbook_render.add_argument("--offline", action="store_true")
    playbook_render.add_argument("--block-paid", action="store_true")
    playbook_render.add_argument("--refresh-context", action="store_true")
    playbook_render.add_argument("--refresh-llm", action="store_true")
    playbook_render.add_argument("--top-n", type=int, default=0)
    playbook_render.add_argument("--per-game-top-n", type=int, default=0)
    playbook_render.add_argument("--strategy-top-n", type=int, default=25)
    playbook_render.add_argument("--min-ev", type=float, default=0.01)
    playbook_render.add_argument("--allow-tier-b", action="store_true")
    playbook_render.add_argument("--month", default="")

    playbook_budget = playbook_subparsers.add_parser(
        "budget", help="Show odds + LLM monthly budget status"
    )
    playbook_budget.set_defaults(func=_cmd_playbook_budget)
    playbook_budget.add_argument("--month", default="")

    playbook_discover_execute = playbook_subparsers.add_parser(
        "discover-execute",
        help="Run all-books discovery + execution-book comparison in one flow",
    )
    playbook_discover_execute.set_defaults(func=_cmd_playbook_discover_execute)
    playbook_discover_execute.add_argument("--base-snapshot-id", default="")
    playbook_discover_execute.add_argument("--sport-key", default="basketball_nba")
    playbook_discover_execute.add_argument(
        "--markets",
        default="player_points,player_rebounds,player_assists,player_threes,player_points_rebounds_assists",
    )
    playbook_discover_execute.add_argument("--discovery-regions", default="us")
    playbook_discover_execute.add_argument("--execution-regions", default="us")
    playbook_discover_execute.add_argument("--execution-bookmakers", default="draftkings,fanduel")
    playbook_discover_execute.add_argument("--commence-from", default="")
    playbook_discover_execute.add_argument("--commence-to", default="")
    playbook_discover_execute.add_argument(
        "--include-links", dest="include_links", action="store_true", default=True
    )
    playbook_discover_execute.add_argument(
        "--no-include-links", dest="include_links", action="store_false"
    )
    playbook_discover_execute.add_argument(
        "--include-sids", dest="include_sids", action="store_true", default=True
    )
    playbook_discover_execute.add_argument(
        "--no-include-sids", dest="include_sids", action="store_false"
    )
    playbook_discover_execute.add_argument("--max-events", type=int, default=6)
    playbook_discover_execute.add_argument("--max-credits", type=int, default=40)
    playbook_discover_execute.add_argument("--force", action="store_true")
    playbook_discover_execute.add_argument("--refresh", action="store_true")
    playbook_discover_execute.add_argument("--resume", action="store_true")
    playbook_discover_execute.add_argument("--offline", action="store_true")
    playbook_discover_execute.add_argument("--refresh-context", action="store_true")
    playbook_discover_execute.add_argument("--refresh-llm", action="store_true")
    playbook_discover_execute.add_argument("--top-n", type=int, default=25)
    playbook_discover_execute.add_argument("--per-game-top-n", type=int, default=5)
    playbook_discover_execute.add_argument("--strategy-top-n", type=int, default=50)
    playbook_discover_execute.add_argument("--min-ev", type=float, default=0.01)
    playbook_discover_execute.add_argument("--allow-tier-b", action="store_true")
    playbook_discover_execute.add_argument("--month", default="")

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 0
    try:
        return int(func(args))
    except (CLIError, OddsAPIError, FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
