# NBA Data Module (`nba-data`)

This repo includes a separate data-ingestion module under `src/prop_ev/nba_data` for
play-by-play, possessions, and boxscore lake building.

## Goals

- Keep NBA data engineering isolated from betting strategy/report generation.
- Make ingestion resumable and restart-safe.
- Reuse cached files by default to minimize repeated downloads.
- Build deterministic clean datasets for downstream modeling work.

## Boundaries

- New CLI entrypoint: `nba-data` (separate from `prop-ev`).
- Output root: `data/nba_data`.
- Existing `prop-ev` CLI and contracts are preserved.

## Data flow

1. `discover` collects season game IDs and seeds manifest rows.
2. `ingest` fetches only missing resources by default and records status in manifests.
3. `clean` converts raw mirrors into partitioned Parquet tables (`schema_v1`).
4. `verify` runs integrity and sanity checks and writes report artifacts.

## Resumability

- Manifest rows track per-game, per-resource status (`missing|ok|error`).
- Lockfile prevents concurrent writes.
- Atomic writes avoid partial-file corruption on crashes.
- Existing successful resources are skipped unless explicit retry/overwrite flags are set.
