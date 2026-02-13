"""CLI entrypoint for nba-data workflows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from prop_ev.nba_data.clean.build import build_clean
from prop_ev.nba_data.config import load_config
from prop_ev.nba_data.errors import CLIError, NBADataError
from prop_ev.nba_data.export import export_clean_artifacts, export_raw_archives
from prop_ev.nba_data.ingest.discover import discover_games
from prop_ev.nba_data.ingest.fetch import ingest_resources, parse_resources
from prop_ev.nba_data.io_utils import atomic_write_json
from prop_ev.nba_data.schema_version import SCHEMA_VERSION
from prop_ev.nba_data.store.layout import build_layout
from prop_ev.nba_data.store.lock import LockConfig, lock_root
from prop_ev.nba_data.store.manifest import (
    ensure_row,
    load_manifest,
    set_schedule_path,
    write_manifest_deterministic,
)
from prop_ev.nba_data.verify.checks import run_verify


def _parse_seasons(raw: str) -> list[str]:
    seasons = [item.strip() for item in raw.split(",") if item.strip()]
    if not seasons:
        raise CLIError("at least one season is required")
    return seasons


def _parse_providers(raw: str) -> list[str]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if not values:
        return ["data_nba", "stats_nba"]
    return values


def _cmd_discover(args: argparse.Namespace) -> int:
    config = load_config(data_dir=getattr(args, "data_dir", None))
    layout = build_layout(config.data_dir)
    seasons = _parse_seasons(args.seasons)
    season_type = str(args.season_type)
    discovered_total = 0

    with lock_root(
        layout.root,
        config=LockConfig(
            force_lock=bool(args.force_lock),
            stale_lock_minutes=int(args.stale_lock_minutes),
            no_stale_recover=bool(args.no_stale_recover),
        ),
    ):
        for season in seasons:
            schedule_path = layout.schedule_path(season=season, season_type=season_type)
            manifest_path = layout.manifest_path(season=season, season_type=season_type)
            rows = load_manifest(manifest_path)
            games = discover_games(
                layout=layout,
                season=season,
                season_type=season_type,
                provider_games=args.provider_games,
            )
            discovered_total += len(games)
            if args.overwrite_schedule or not schedule_path.exists():
                atomic_write_json(
                    schedule_path,
                    {
                        "season": season,
                        "season_type": season_type,
                        "provider_games": args.provider_games,
                        "games": games,
                    },
                )

            for game in games:
                game_id = str(game.get("game_id", "")).strip()
                if not game_id:
                    continue
                row = ensure_row(rows, season=season, season_type=season_type, game_id=game_id)
                set_schedule_path(root=layout.root, row=row, schedule_path=schedule_path)
            write_manifest_deterministic(manifest_path, rows)

    payload = {"seasons": seasons, "season_type": season_type, "discovered_games": discovered_total}
    if args.json_output:
        print(json.dumps(payload, sort_keys=True, indent=2))
    else:
        print(
            f"seasons={','.join(seasons)} season_type={season_type} "
            f"discovered_games={discovered_total}"
        )
    return 0


def _cmd_ingest(args: argparse.Namespace) -> int:
    config = load_config(data_dir=getattr(args, "data_dir", None))
    layout = build_layout(config.data_dir)
    seasons = _parse_seasons(args.seasons)
    season_type = str(args.season_type)
    resources = parse_resources(args.resources)
    provider_map = {
        "boxscore": _parse_providers(args.providers_boxscore),
        "enhanced_pbp": _parse_providers(args.providers_enhanced_pbp),
        "possessions": _parse_providers(args.providers_possessions),
    }
    totals = {"ok": 0, "skipped": 0, "error": 0}
    for season in seasons:
        manifest_path = layout.manifest_path(season=season, season_type=season_type)
        rows = load_manifest(manifest_path)
        summary = ingest_resources(
            layout=layout,
            rows=rows,
            season=season,
            season_type=season_type,
            resources=resources,
            only_missing=bool(args.only_missing),
            retry_errors=bool(args.retry_errors),
            max_games=int(args.max_games),
            rpm=int(args.rpm),
            providers=provider_map,  # pyright: ignore[reportArgumentType]
            fail_fast=bool(args.fail_fast),
            lock_config=LockConfig(
                force_lock=bool(args.force_lock),
                stale_lock_minutes=int(args.stale_lock_minutes),
                no_stale_recover=bool(args.no_stale_recover),
            ),
        )
        write_manifest_deterministic(manifest_path, rows)
        for key, value in summary.items():
            totals[key] += int(value)
        print(
            ("season={} season_type={} ok={} skipped={} error={}").format(
                season, season_type, summary["ok"], summary["skipped"], summary["error"]
            )
        )
    return 1 if totals["error"] > 0 else 0


def _cmd_clean(args: argparse.Namespace) -> int:
    config = load_config(data_dir=getattr(args, "data_dir", None))
    layout = build_layout(config.data_dir)
    seasons = _parse_seasons(args.seasons)
    counts = build_clean(
        layout=layout,
        seasons=seasons,
        season_type=str(args.season_type),
        overwrite=bool(args.overwrite),
        schema_version=int(args.schema_version),
    )
    print(
        "schema_version={} games={} boxscore_players={} pbp_events={} possessions={}".format(
            int(args.schema_version),
            counts.get("games", 0),
            counts.get("boxscore_players", 0),
            counts.get("pbp_events", 0),
            counts.get("possessions", 0),
        )
    )
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    config = load_config(data_dir=getattr(args, "data_dir", None))
    layout = build_layout(config.data_dir)
    seasons = _parse_seasons(args.seasons)
    code, report = run_verify(
        layout=layout,
        seasons=seasons,
        season_type=str(args.season_type),
        schema_version=int(args.schema_version),
        fail_on_warn=bool(args.fail_on_warn),
    )
    print(
        (
            "schema_version={} failures={} warnings={} games={} "
            "boxscore_players={} pbp_events={} possessions={}"
        ).format(
            report.get("schema_version", SCHEMA_VERSION),
            len(report.get("failures", [])),
            len(report.get("warnings", [])),
            report.get("counts", {}).get("games", 0),
            report.get("counts", {}).get("boxscore_players", 0),
            report.get("counts", {}).get("pbp_events", 0),
            report.get("counts", {}).get("possessions", 0),
        )
    )
    return int(code)


def _cmd_export_clean(args: argparse.Namespace) -> int:
    source_config = load_config(data_dir=getattr(args, "data_dir", None))
    source_layout = build_layout(source_config.data_dir)
    destination_layout = build_layout(Path(str(args.dst_data_dir)).expanduser())
    summary = export_clean_artifacts(
        src_layout=source_layout,
        dst_layout=destination_layout,
        seasons=_parse_seasons(args.seasons),
        season_type=str(args.season_type),
        schema_version=int(args.schema_version),
        overwrite=bool(args.overwrite),
    )
    if args.json_output:
        print(json.dumps(summary, sort_keys=True, indent=2))
    else:
        print(
            (
                "dst_data_dir={} season_type={} schema_version={} copied_files={} skipped_files={}"
            ).format(
                destination_layout.root,
                summary["season_type"],
                summary["schema_version"],
                summary["copied_files"],
                summary["skipped_files"],
            )
        )
    return 0


def _cmd_export_raw_archive(args: argparse.Namespace) -> int:
    source_config = load_config(data_dir=getattr(args, "data_dir", None))
    source_layout = build_layout(source_config.data_dir)
    destination_layout = build_layout(Path(str(args.dst_data_dir)).expanduser())
    summary = export_raw_archives(
        src_layout=source_layout,
        dst_layout=destination_layout,
        seasons=_parse_seasons(args.seasons),
        season_type=str(args.season_type),
        compression_level=int(args.compression_level),
        overwrite=bool(args.overwrite),
    )
    if args.json_output:
        print(json.dumps(summary, sort_keys=True, indent=2))
    else:
        print(
            ("dst_data_dir={} season_type={} archives={} manifest_path={}").format(
                destination_layout.root,
                summary["season_type"],
                summary["archives"],
                summary["manifest_path"],
            )
        )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="nba-data")
    subparsers = parser.add_subparsers(dest="command")

    discover = subparsers.add_parser("discover", help="Discover final games and seed manifests")
    discover.set_defaults(func=_cmd_discover)
    discover.add_argument("--data-dir", default="")
    discover.add_argument("--seasons", default="2023-24,2024-25,2025-26")
    discover.add_argument("--season-type", default="Regular Season")
    discover.add_argument("--provider-games", default="data_nba")
    discover.add_argument("--overwrite-schedule", action="store_true")
    discover.add_argument("--json", dest="json_output", action="store_true")
    discover.add_argument("--force-lock", action="store_true")
    discover.add_argument("--stale-lock-minutes", type=int, default=120)
    discover.add_argument("--no-stale-recover", action="store_true")

    ingest = subparsers.add_parser("ingest", help="Ingest per-game raw resources")
    ingest.set_defaults(func=_cmd_ingest)
    ingest.add_argument("--data-dir", default="")
    ingest.add_argument("--seasons", default="2023-24,2024-25,2025-26")
    ingest.add_argument("--season-type", default="Regular Season")
    ingest.add_argument("--resources", default="boxscore,enhanced_pbp,possessions")
    ingest.add_argument("--only-missing", action=argparse.BooleanOptionalAction, default=True)
    ingest.add_argument("--retry-errors", action=argparse.BooleanOptionalAction, default=False)
    ingest.add_argument("--max-games", type=int, default=0)
    ingest.add_argument("--rpm", type=int, default=30)
    ingest.add_argument("--providers-enhanced-pbp", default="data_nba,stats_nba")
    ingest.add_argument("--providers-possessions", default="data_nba,stats_nba")
    ingest.add_argument("--providers-boxscore", default="data_nba,stats_nba")
    ingest.add_argument("--fail-fast", action="store_true")
    ingest.add_argument("--force-lock", action="store_true")
    ingest.add_argument("--stale-lock-minutes", type=int, default=120)
    ingest.add_argument("--no-stale-recover", action="store_true")

    clean = subparsers.add_parser("clean", help="Build clean parquet datasets")
    clean.set_defaults(func=_cmd_clean)
    clean.add_argument("--data-dir", default="")
    clean.add_argument("--seasons", default="2023-24,2024-25,2025-26")
    clean.add_argument("--season-type", default="Regular Season")
    clean.add_argument("--schema-version", type=int, default=SCHEMA_VERSION)
    clean.add_argument("--overwrite", action="store_true")

    verify = subparsers.add_parser("verify", help="Run integrity checks on clean datasets")
    verify.set_defaults(func=_cmd_verify)
    verify.add_argument("--data-dir", default="")
    verify.add_argument("--seasons", default="2023-24,2024-25,2025-26")
    verify.add_argument("--season-type", default="Regular Season")
    verify.add_argument("--schema-version", type=int, default=SCHEMA_VERSION)
    verify.add_argument("--fail-on-warn", action="store_true")

    export = subparsers.add_parser("export", help="Export datasets between data roots")
    export_subparsers = export.add_subparsers(dest="export_command")

    export_clean = export_subparsers.add_parser(
        "clean", help="Export clean parquet + manifests + schedule + verify artifacts"
    )
    export_clean.set_defaults(func=_cmd_export_clean)
    export_clean.add_argument("--data-dir", default="")
    export_clean.add_argument("--dst-data-dir", required=True)
    export_clean.add_argument("--seasons", default="2023-24,2024-25,2025-26")
    export_clean.add_argument("--season-type", default="Regular Season")
    export_clean.add_argument("--schema-version", type=int, default=SCHEMA_VERSION)
    export_clean.add_argument("--overwrite", action="store_true")
    export_clean.add_argument("--json", dest="json_output", action="store_true")

    export_raw_archive = export_subparsers.add_parser(
        "raw-archive", help="Build immutable season raw archives with checksum manifest"
    )
    export_raw_archive.set_defaults(func=_cmd_export_raw_archive)
    export_raw_archive.add_argument("--data-dir", default="")
    export_raw_archive.add_argument("--dst-data-dir", required=True)
    export_raw_archive.add_argument("--seasons", default="2023-24,2024-25,2025-26")
    export_raw_archive.add_argument("--season-type", default="Regular Season")
    export_raw_archive.add_argument("--compression-level", type=int, default=19)
    export_raw_archive.add_argument("--overwrite", action="store_true")
    export_raw_archive.add_argument("--json", dest="json_output", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 0
    try:
        return int(func(args))
    except (CLIError, NBADataError, FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
