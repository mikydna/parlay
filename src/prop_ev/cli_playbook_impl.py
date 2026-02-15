"""Playbook CLI command implementations."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from prop_ev.budget import current_month_utc
from prop_ev.cli_playbook_publish import (
    publish_compact_playbook_outputs as _publish_compact_outputs,
)
from prop_ev.cli_playbook_publish import snapshot_date as _snapshot_date_impl
from prop_ev.cli_shared import (
    CLIError,
    _env_bool,
    _env_float,
    _iso,
    _resolve_strategy_id,
    _runtime_odds_data_dir,
    _utc_now,
)
from prop_ev.cli_strategy_impl import (
    _allow_secondary_injuries_override,
    _latest_snapshot_id,
    _official_injury_hard_fail_message,
    _teams_in_scope_from_events,
)
from prop_ev.discovery_execution import (
    build_discovery_execution_report,
    write_discovery_execution_reports,
)
from prop_ev.odds_client import (
    OddsAPIClient,  # noqa: F401
    OddsAPIError,
)
from prop_ev.playbook import budget_snapshot, compute_live_window
from prop_ev.report_paths import (
    snapshot_reports_dir,
)
from prop_ev.settings import Settings
from prop_ev.state_keys import (
    playbook_mode_key,
    strategy_title,
)
from prop_ev.storage import SnapshotStore, make_snapshot_id
from prop_ev.strategies.base import (
    normalize_strategy_id,
)
from prop_ev.strategy import (
    load_jsonl,
)


def _cli_commands_module() -> Any:
    import prop_ev.cli_commands as cli_commands_module

    return cli_commands_module


def _load_slate_rows(store: SnapshotStore, snapshot_id: str) -> list[dict[str, Any]]:
    path = store.derived_path(snapshot_id, "featured_odds.jsonl")
    if not path.exists():
        return []
    return load_jsonl(path)


def _derive_window_from_events(
    event_context: dict[str, dict[str, str]] | None,
) -> tuple[str, str]:
    default_from, default_to = _cli_commands_module()._default_window()
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
    code = int(_cli_commands_module()._cmd_snapshot_slate(args))
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
    return int(_cli_commands_module()._cmd_snapshot_props(snapshot_args))


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
    return int(_cli_commands_module()._cmd_snapshot_slate(snapshot_args))


def _run_snapshot_bundle_for_playbook(args: argparse.Namespace, snapshot_id: str) -> int:
    slate_code = _run_snapshot_slate_for_playbook(args, snapshot_id)
    props_code = _run_snapshot_props_for_playbook(args, snapshot_id)
    return 0 if slate_code == 0 and props_code == 0 else 2


def _run_strategy_for_playbook(
    *,
    snapshot_id: str,
    strategy_id: str,
    top_n: int,
    min_ev: float,
    allow_tier_b: bool,
    offline: bool,
    block_paid: bool,
    refresh_context: bool,
    strategy_mode: str = "auto",
    allow_secondary_injuries: bool = False,
    write_canonical: bool = True,
) -> int:
    strategy_args = argparse.Namespace(
        snapshot_id=snapshot_id,
        strategy=strategy_id,
        top_n=top_n,
        max_picks=0,
        min_ev=min_ev,
        allow_tier_b=allow_tier_b,
        offline=offline,
        block_paid=block_paid,
        refresh_context=refresh_context,
        mode=strategy_mode,
        allow_secondary_injuries=allow_secondary_injuries,
        write_canonical=write_canonical,
        execution_bookmakers="",
        execution_top_n=0,
        execution_requires_pre_bet_ready=False,
        execution_requires_meets_play_to=False,
        execution_tier_a_min_ev=0.03,
        execution_tier_b_min_ev=0.05,
    )
    return int(_cli_commands_module()._cmd_strategy_run(strategy_args))


def _cmd_playbook_run(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    settings = Settings.from_runtime()
    month = args.month or current_month_utc()
    strategy_id = _resolve_strategy_id(
        str(getattr(args, "strategy", "")),
        default_id=settings.strategy_default_id,
    )
    top_n = args.top_n if args.top_n > 0 else settings.playbook_top_n
    per_game_top_n = (
        args.per_game_top_n if args.per_game_top_n > 0 else settings.playbook_per_game_top_n
    )
    start_budget = budget_snapshot(store=store, settings=settings, month=month)
    require_official_injuries = _env_bool("PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES", True)
    allow_secondary_injuries = _allow_secondary_injuries_override(
        cli_flag=bool(getattr(args, "allow_secondary_injuries", False)),
        default=settings.strategy_allow_secondary_injuries,
    )
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
            default_from, default_to = _cli_commands_module()._default_window()
            commence_from = args.commence_from or default_from
            commence_to = args.commence_to or default_to
            events: list[dict[str, Any]] = []
            try:
                with _cli_commands_module().OddsAPIClient(settings) as client:
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

            if (
                bool(getattr(args, "exit_on_no_games", False))
                and str(live_window.get("status", "")) == "no_events"
            ):
                mode = "no_games_exit"
                print("snapshot_id=")
                print(f"mode={mode}")
                mode_desc = playbook_mode_key().get(mode, "")
                if mode_desc:
                    print(f"mode_desc={mode_desc}")
                print("event_count=0")
                print(f"within_window={str(bool(live_window.get('within_window', False))).lower()}")
                print("exit_reason=no_games")
                return 0

            odds_cap_reached = bool(start_budget["odds"].get("cap_reached", False))
            block_paid = bool(getattr(args, "block_paid", False))
            should_live = (
                bool(live_window.get("within_window", False))
                and not odds_cap_reached
                and not block_paid
            )
            if should_live:
                snapshot_id = make_snapshot_id()
                preflight_context = _cli_commands_module()._preflight_context_for_snapshot(
                    store=store,
                    snapshot_id=snapshot_id,
                    teams_in_scope=_teams_in_scope_from_events(events),
                    refresh_context=args.refresh_context,
                    require_official_injuries=require_official_injuries,
                    allow_secondary_injuries=allow_secondary_injuries,
                    require_fresh_context=require_fresh_context,
                    injuries_stale_hours=injuries_stale_hours,
                    roster_stale_hours=roster_stale_hours,
                )
                preflight_gates = preflight_context.get("health_gates", [])
                if (
                    isinstance(preflight_gates, list)
                    and "official_injury_missing" in preflight_gates
                    and not allow_secondary_injuries
                ):
                    raise CLIError(_official_injury_hard_fail_message()) from None
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
                    snapshot_code = int(
                        _cli_commands_module()._run_snapshot_bundle_for_playbook(args, snapshot_id)
                    )
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
                    preflight_context = _cli_commands_module()._preflight_context_for_snapshot(
                        store=store,
                        snapshot_id=snapshot_id,
                        teams_in_scope=_teams_in_scope_from_events(events),
                        refresh_context=args.refresh_context,
                        require_official_injuries=require_official_injuries,
                        allow_secondary_injuries=allow_secondary_injuries,
                        require_fresh_context=require_fresh_context,
                        injuries_stale_hours=injuries_stale_hours,
                        roster_stale_hours=roster_stale_hours,
                    )
                    preflight_gates = preflight_context.get("health_gates", [])
                    if (
                        isinstance(preflight_gates, list)
                        and "official_injury_missing" in preflight_gates
                        and not allow_secondary_injuries
                    ):
                        raise CLIError(_official_injury_hard_fail_message()) from None
                    if isinstance(preflight_gates, list) and preflight_gates:
                        raise CLIError(
                            "context preflight failed; refusing paid odds fetch in bootstrap mode; "
                            f"gates={','.join(preflight_gates)}"
                        ) from None
                    snapshot_code = int(
                        _cli_commands_module()._run_snapshot_bundle_for_playbook(args, snapshot_id)
                    )
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
    strategy_code = int(
        _cli_commands_module()._run_strategy_for_playbook(
            snapshot_id=snapshot_id,
            strategy_id=strategy_id,
            top_n=args.strategy_top_n,
            min_ev=args.min_ev,
            allow_tier_b=args.allow_tier_b,
            offline=strategy_offline,
            block_paid=bool(getattr(args, "block_paid", False)),
            refresh_context=refresh_context,
            strategy_mode=str(getattr(args, "strategy_mode", "auto")),
            allow_secondary_injuries=allow_secondary_injuries,
            write_canonical=True,
        )
    )
    if strategy_code != 0:
        return strategy_code
    run_reports_dir = snapshot_reports_dir(store, snapshot_id)
    calibration_map_path: Path | None = None
    calibration_map_file = str(getattr(args, "calibration_map_file", "")).strip()
    if calibration_map_file:
        candidate = Path(calibration_map_file).expanduser()
        calibration_map_path = (
            candidate if candidate.is_absolute() else (run_reports_dir / candidate)
        )

    brief = _cli_commands_module().generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=snapshot_id,
        top_n=top_n,
        llm_refresh=args.refresh_llm,
        llm_offline=args.offline,
        per_game_top_n=per_game_top_n,
        game_card_min_ev=max(0.0, args.min_ev),
        month=month,
        calibration_map_path=calibration_map_path,
        write_markdown=bool(getattr(args, "write_markdown", False)),
        keep_tex=bool(getattr(args, "keep_tex", False)),
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
    mode_desc = playbook_mode_key().get(mode, "")
    if mode_desc:
        print(f"mode_desc={mode_desc}")
    print(f"strategy_id={strategy_id}")
    title = strategy_title(strategy_id)
    if title:
        print(f"strategy_title={title}")
    preflight_gates = (
        preflight_context.get("health_gates", [])
        if isinstance(preflight_context.get("health_gates"), list)
        else []
    )
    if preflight_gates:
        print(f"context_preflight_gates={','.join(preflight_gates)}")
    if brief.get("report_markdown"):
        print(f"strategy_brief_md={brief['report_markdown']}")
    if brief.get("report_tex"):
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
    store = SnapshotStore(_runtime_odds_data_dir())
    settings = Settings.from_runtime()
    month = args.month or current_month_utc()
    strategy_id = _resolve_strategy_id(
        str(getattr(args, "strategy", "")),
        default_id=settings.strategy_default_id,
    )
    top_n = args.top_n if args.top_n > 0 else settings.playbook_top_n
    per_game_top_n = (
        args.per_game_top_n if args.per_game_top_n > 0 else settings.playbook_per_game_top_n
    )
    snapshot_id = args.snapshot_id
    reports_dir = snapshot_reports_dir(store, snapshot_id)
    strategy_report_file = (
        str(getattr(args, "strategy_report_file", "strategy-report.json")).strip()
        or "strategy-report.json"
    )
    candidate_report_path = Path(strategy_report_file).expanduser()
    if candidate_report_path.is_absolute():
        strategy_report_path = candidate_report_path
    else:
        strategy_report_path = reports_dir / candidate_report_path
    canonical_strategy_path = reports_dir / "strategy-report.json"
    calibration_map_path: Path | None = None
    calibration_map_file = str(getattr(args, "calibration_map_file", "")).strip()
    if calibration_map_file:
        candidate_map = Path(calibration_map_file).expanduser()
        calibration_map_path = (
            candidate_map if candidate_map.is_absolute() else (reports_dir / candidate_map)
        )
    is_canonical_strategy_report = strategy_report_path.resolve(
        strict=False
    ) == canonical_strategy_path.resolve(strict=False)
    refresh_context = bool(args.refresh_context and not args.offline)
    if bool(args.refresh_context) and bool(args.offline):
        print(f"note=refresh_context_ignored_in_offline_mode snapshot_id={snapshot_id}")
    if is_canonical_strategy_report:
        strategy_needs_refresh = not canonical_strategy_path.exists()
        if not strategy_needs_refresh:
            try:
                existing = json.loads(canonical_strategy_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                strategy_needs_refresh = True
            else:
                existing_id = ""
                if isinstance(existing, dict):
                    existing_id = str(existing.get("strategy_id", "")).strip()
                existing_id = normalize_strategy_id(existing_id) if existing_id else "s001"
                strategy_needs_refresh = existing_id != strategy_id
        if strategy_needs_refresh:
            code = int(
                _cli_commands_module()._run_strategy_for_playbook(
                    snapshot_id=snapshot_id,
                    strategy_id=strategy_id,
                    top_n=args.strategy_top_n,
                    min_ev=args.min_ev,
                    allow_tier_b=args.allow_tier_b,
                    offline=args.offline,
                    block_paid=bool(getattr(args, "block_paid", False)),
                    refresh_context=refresh_context,
                    strategy_mode=str(getattr(args, "strategy_mode", "auto")),
                    allow_secondary_injuries=_allow_secondary_injuries_override(
                        cli_flag=bool(getattr(args, "allow_secondary_injuries", False)),
                        default=settings.strategy_allow_secondary_injuries,
                    ),
                    write_canonical=True,
                )
            )
            if code != 0:
                return code
    elif not strategy_report_path.exists():
        raise CLIError(f"missing strategy report file: {strategy_report_path}")

    brief = _cli_commands_module().generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=snapshot_id,
        top_n=top_n,
        llm_refresh=args.refresh_llm,
        llm_offline=args.offline,
        per_game_top_n=per_game_top_n,
        game_card_min_ev=max(0.0, args.min_ev),
        month=month,
        strategy_report_path=strategy_report_path,
        calibration_map_path=calibration_map_path,
        write_markdown=bool(getattr(args, "write_markdown", False)),
        keep_tex=bool(getattr(args, "keep_tex", False)),
    )
    print(f"snapshot_id={snapshot_id}")
    print(f"strategy_id={strategy_id}")
    title = strategy_title(strategy_id)
    if title:
        print(f"strategy_title={title}")
    print(f"strategy_report_path={strategy_report_path}")
    if brief.get("report_markdown"):
        print(f"strategy_brief_md={brief['report_markdown']}")
    if brief.get("report_tex"):
        print(f"strategy_brief_tex={brief['report_tex']}")
    print(f"strategy_brief_pdf={brief['report_pdf']}")
    print(f"strategy_brief_meta={brief['report_meta']}")
    return 0


def _snapshot_date(snapshot_id: str) -> str:
    return _snapshot_date_impl(snapshot_id)


def _cmd_playbook_publish(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    try:
        published, daily_dir, latest_dir, latest_json = _publish_compact_outputs(
            store=store,
            snapshot_id=args.snapshot_id,
            now_utc_iso=_iso(_utc_now()),
        )
    except RuntimeError as exc:
        raise CLIError(str(exc)) from exc
    print(f"snapshot_id={args.snapshot_id}")
    print(f"published_files={','.join(published)}")
    print(f"daily_dir={daily_dir}")
    print(f"latest_dir={latest_dir}")
    print(f"latest_json={latest_json}")
    return 0


def _cmd_playbook_budget(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    settings = Settings.from_runtime()
    month = args.month or current_month_utc()
    payload = budget_snapshot(store=store, settings=settings, month=month)
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0


def _cmd_playbook_discover_execute(args: argparse.Namespace) -> int:
    store = SnapshotStore(_runtime_odds_data_dir())
    settings = Settings.from_runtime()
    month = args.month or current_month_utc()
    strategy_id = _resolve_strategy_id(
        str(getattr(args, "strategy", "")),
        default_id=settings.strategy_default_id,
    )
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

    if int(_cli_commands_module()._cmd_snapshot_slate(discovery_slate_args)) != 0:
        return 2
    if int(_cli_commands_module()._cmd_snapshot_props(discovery_props_args)) != 0:
        return 2
    if (
        _run_strategy_for_playbook(
            snapshot_id=discovery_snapshot_id,
            strategy_id=strategy_id,
            top_n=args.strategy_top_n,
            min_ev=args.min_ev,
            allow_tier_b=args.allow_tier_b,
            offline=False,
            block_paid=False,
            refresh_context=args.refresh_context,
            strategy_mode=str(getattr(args, "strategy_mode", "auto")),
            allow_secondary_injuries=_allow_secondary_injuries_override(
                cli_flag=bool(getattr(args, "allow_secondary_injuries", False)),
                default=settings.strategy_allow_secondary_injuries,
            ),
            write_canonical=True,
        )
        != 0
    ):
        return 2

    if int(_cli_commands_module()._cmd_snapshot_slate(execution_slate_args)) != 0:
        return 2
    if int(_cli_commands_module()._cmd_snapshot_props(execution_props_args)) != 0:
        return 2
    if (
        _run_strategy_for_playbook(
            snapshot_id=execution_snapshot_id,
            strategy_id=strategy_id,
            top_n=args.strategy_top_n,
            min_ev=args.min_ev,
            allow_tier_b=args.allow_tier_b,
            offline=False,
            block_paid=False,
            refresh_context=args.refresh_context,
            strategy_mode=str(getattr(args, "strategy_mode", "auto")),
            allow_secondary_injuries=_allow_secondary_injuries_override(
                cli_flag=bool(getattr(args, "allow_secondary_injuries", False)),
                default=settings.strategy_allow_secondary_injuries,
            ),
            write_canonical=True,
        )
        != 0
    ):
        return 2

    discovery_report_path = (
        snapshot_reports_dir(store, discovery_snapshot_id) / "strategy-report.json"
    )
    execution_report_path = (
        snapshot_reports_dir(store, execution_snapshot_id) / "strategy-report.json"
    )
    discovery_report = json.loads(discovery_report_path.read_text(encoding="utf-8"))
    execution_report = json.loads(execution_report_path.read_text(encoding="utf-8"))
    compare_report = build_discovery_execution_report(
        discovery_snapshot_id=discovery_snapshot_id,
        execution_snapshot_id=execution_snapshot_id,
        discovery_report=discovery_report,
        execution_report=execution_report,
        top_n=args.top_n,
    )
    compare_json, compare_md = write_discovery_execution_reports(
        store=store,
        execution_snapshot_id=execution_snapshot_id,
        report=compare_report,
        write_markdown=bool(getattr(args, "write_markdown", False)),
    )
    execution_reports_dir = snapshot_reports_dir(store, execution_snapshot_id)
    calibration_map_path: Path | None = None
    calibration_map_file = str(getattr(args, "calibration_map_file", "")).strip()
    if calibration_map_file:
        candidate_map = Path(calibration_map_file).expanduser()
        calibration_map_path = (
            candidate_map
            if candidate_map.is_absolute()
            else (execution_reports_dir / candidate_map)
        )

    brief = _cli_commands_module().generate_brief_for_snapshot(
        store=store,
        settings=settings,
        snapshot_id=execution_snapshot_id,
        top_n=max(1, args.top_n),
        llm_refresh=args.refresh_llm,
        llm_offline=args.offline,
        per_game_top_n=max(1, getattr(args, "per_game_top_n", 5)),
        game_card_min_ev=max(0.0, args.min_ev),
        month=month,
        calibration_map_path=calibration_map_path,
        write_markdown=bool(getattr(args, "write_markdown", False)),
        keep_tex=bool(getattr(args, "keep_tex", False)),
    )

    summary = compare_report.get("summary", {})
    print(f"discovery_snapshot_id={discovery_snapshot_id}")
    print(f"execution_snapshot_id={execution_snapshot_id}")
    print(f"strategy_id={strategy_id}")
    title = strategy_title(strategy_id)
    if title:
        print(f"strategy_title={title}")
    print(
        "actionable_rows={} matched_rows={} discovery_eligible_rows={}".format(
            summary.get("actionable_rows", 0),
            summary.get("matched_execution_rows", 0),
            summary.get("discovery_eligible_rows", 0),
        )
    )
    print(f"discovery_execution_json={compare_json}")
    if compare_md is not None:
        print(f"discovery_execution_md={compare_md}")
    if brief.get("report_markdown"):
        print(f"strategy_brief_md={brief['report_markdown']}")
    print(f"strategy_brief_pdf={brief['report_pdf']}")
    return 0
