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
- `prop-ev` runtime consumers read NBA data through a single repository handle.

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

## Runtime Access

`prop-ev` modules (settlement, strategy context, playbook context) should use the unified
NBA repository interface instead of calling source fetchers directly. This keeps source policy,
cache behavior, and fallback semantics in one place.

## Minutes/Usage Baseline Artifact

`nba-data` now includes an offline modeling helper to build deterministic minutes baseline artifacts
from clean parquet:

```bash
uv run nba-data minutes-usage \
  --data-dir /path/to/nba_data \
  --seasons 2025-26 \
  --season-type \"Regular Season\" \
  --history-games 10 \
  --min-history-games 3 \
  --eval-days 30 \
  --out-dir reports/analysis/minutes_usage
```

Outputs:
- `predictions.parquet` (per-player/game minutes predictions with errors)
- `summary.json` (model version, windows, MAE/RMSE/Bias, artifact paths)

Season type matching is normalized, so both `Regular Season` and `regular_season` are accepted.

## Probabilistic Minutes Artifact (`minutes-prob`)

`nba-data` also ships a probabilistic minutes pipeline intended for replay/backtest workflows and
optional strategy correction layers (`s020`).

CLI surface:

```bash
uv run nba-data minutes-prob train --help
uv run nba-data minutes-prob predict --help
uv run nba-data minutes-prob evaluate --help
```

Recommended no-spend workflow:

```bash
uv run nba-data minutes-prob train \
  --data-dir /Users/andy/Documents/Code/parlay-data/nba_data \
  --seasons 2023-24,2024-25,2025-26 \
  --season-type "Regular Season" \
  --history-games 8 \
  --min-history-games 3 \
  --eval-days 30 \
  --model-version minutes_prob_v1
```

Then predict for one snapshot day:

```bash
uv run nba-data minutes-prob predict \
  --data-dir /Users/andy/Documents/Code/parlay-data/nba_data \
  --model-dir <model_dir> \
  --as-of-date 2026-02-12 \
  --snapshot-id day-bdfa890a-2026-02-12 \
  --markets player_points,player_rebounds,player_assists
```

Contract notes:
- model metadata is versioned (`model_version`, train/eval windows, schema versions, seed).
- prediction parquet includes:
  - `minutes_p10`, `minutes_p50`, `minutes_p90`,
  - `minutes_mu`, `minutes_sigma_proxy`,
  - `p_active`,
  - tenure fields (`games_on_team`, `days_on_team`, `new_team_phase`),
  - `confidence_score`, `data_quality_flags`.
- evaluation JSON includes MAE/RMSE/Bias plus quantile coverage metrics.

Offline/default behavior:
- remote player-map enrichment is disabled by default.
- set `PROP_EV_MINUTES_PROB_ALLOW_REMOTE_PLAYER_MAP=1` only when explicit network enrichment is desired.
