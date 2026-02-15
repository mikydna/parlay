"""Snapshot and credits CLI command implementations."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from prop_ev.cli_shared import (
    OfflineCacheMissError,
    _default_window,
    _enforce_credit_cap,
    _execute_request,
    _parse_markets,
    _print_estimate,
    _resolve_bookmakers,
    _runtime_odds_data_dir,
    _utc_now,
    _write_derived,
)
from prop_ev.normalize import normalize_event_odds, normalize_featured_odds
from prop_ev.odds_client import (
    OddsAPIClient,
    OddsAPIError,
    estimate_event_credits,
    estimate_featured_credits,
    regions_equivalent,
)
from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.settings import Settings
from prop_ev.snapshot_artifacts import (
    lake_snapshot_derived,
    pack_snapshot,
    unpack_snapshot,
    verify_snapshot_derived_contracts,
)
from prop_ev.storage import SnapshotStore, make_snapshot_id, request_hash


def _cmd_snapshot_slate(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    cache = GlobalCacheStore(store.root)
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
    request_key = request_hash("GET", path, params)
    cached_hit = not args.refresh and (
        store.has_response(snapshot_id, request_key) or cache.has_response(request_key)
    )
    estimate = 0 if cached_hit else estimate_featured_credits(markets, regions_factor)
    _print_estimate(estimate, args.max_credits)
    _enforce_credit_cap(estimate, args.max_credits, args.force)
    if args.dry_run:
        print(f"bookmakers_source={bookmakers_source} bookmakers={bookmakers}")
        return 0

    settings = Settings.from_runtime()
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
    store = SnapshotStore(_runtime_odds_data_dir())
    cache = GlobalCacheStore(store.root)
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
        settings = Settings.from_runtime()
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
            event_request_params: dict[str, Any] = {
                "markets": ",".join(sorted(set(markets))),
                "oddsFormat": "american",
                "dateFormat": "iso",
            }
            if bookmakers:
                event_request_params["bookmakers"] = bookmakers
            elif args.regions:
                event_request_params["regions"] = args.regions
            if args.include_links:
                event_request_params["includeLinks"] = "true"
            if args.include_sids:
                event_request_params["includeSids"] = "true"
            missing_event_ids: list[str] = []
            for event_id in event_ids:
                event_path = f"/sports/{args.sport_key}/events/{event_id}/odds"
                event_key = request_hash("GET", event_path, event_request_params)
                cached_hit = not args.refresh and (
                    store.has_response(snapshot_id, event_key) or cache.has_response(event_key)
                )
                if not cached_hit:
                    missing_event_ids.append(event_id)
            regions_factor = regions_equivalent(args.regions, bookmakers)
            estimate = estimate_event_credits(markets, regions_factor, len(missing_event_ids))
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
                params = dict(event_request_params)
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
    store = SnapshotStore(_runtime_odds_data_dir())
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
    store = SnapshotStore(_runtime_odds_data_dir())
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
    store = SnapshotStore(_runtime_odds_data_dir())
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
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_dir = store.snapshot_dir(args.snapshot_id)
    manifest = store.load_manifest(args.snapshot_id)
    requests = manifest.get("requests", {})
    if not isinstance(requests, dict):
        print("invalid manifest: requests must be object")
        return 2

    missing = 0
    for request_key in requests:
        request_path = snapshot_dir / "requests" / f"{request_key}.json"
        response_path = snapshot_dir / "responses" / f"{request_key}.json"
        meta_path = snapshot_dir / "meta" / f"{request_key}.json"
        if not request_path.exists() or not response_path.exists() or not meta_path.exists():
            missing += 1
            print(
                f"missing_artifacts request_key={request_key} "
                f"request={request_path.exists()} "
                f"response={response_path.exists()} "
                f"meta={meta_path.exists()}"
            )

    derived_issues: list[dict[str, str]] = []
    if bool(getattr(args, "check_derived", False)):
        required_tables = tuple(
            sorted(
                {
                    str(item).strip()
                    for item in getattr(args, "require_table", [])
                    if str(item).strip()
                }
            )
        )
        derived_issues = verify_snapshot_derived_contracts(
            snapshot_dir=snapshot_dir,
            require_parquet=bool(getattr(args, "require_parquet", False)),
            required_tables=required_tables,
        )
        for issue in derived_issues:
            print(
                "derived_issue code={} table={} detail={}".format(
                    str(issue.get("code", "")),
                    str(issue.get("table", "")),
                    str(issue.get("detail", "")),
                )
            )

    print(
        f"snapshot_id={args.snapshot_id} checked={len(requests)} missing={missing} "
        f"derived_issues={len(derived_issues)}"
    )
    return 2 if (missing or derived_issues) else 0


def _cmd_snapshot_lake(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    snapshot_dir = store.snapshot_dir(args.snapshot_id)
    parquet_paths = lake_snapshot_derived(snapshot_dir)
    print(f"snapshot_id={args.snapshot_id} parquet_files={len(parquet_paths)}")
    for path in parquet_paths:
        print(f"parquet={path}")
    return 0


def _cmd_snapshot_pack(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    out_path = Path(str(args.out)).expanduser() if str(args.out).strip() else None
    bundle_path, sidecar_path = pack_snapshot(
        data_root=store.root,
        snapshot_id=args.snapshot_id,
        out_path=out_path,
    )
    print(f"snapshot_id={args.snapshot_id}")
    print(f"bundle={bundle_path}")
    print(f"bundle_meta={sidecar_path}")
    return 0


def _cmd_snapshot_unpack(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    payload = unpack_snapshot(
        data_root=store.root,
        bundle_path=Path(str(args.bundle)).expanduser(),
    )
    snapshot_ids = payload.get("snapshot_ids", [])
    snapshot_list = ",".join(snapshot_ids) if isinstance(snapshot_ids, list) else ""
    print(f"bundle={payload.get('bundle_path', '')}")
    print(f"files_extracted={payload.get('files_extracted', 0)}")
    print(f"snapshot_ids={snapshot_list}")
    return 0


def _cmd_credits_report(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
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
