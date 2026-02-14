# Integration Evidence — 2026-02-14

This document records the no-spend/offline evidence packet for IM1–IM5 progress tracking.

## Environment + dataset

- Odds root: `/Users/andy/Documents/Code/parlay-data/odds_api`
- NBA root: `/Users/andy/Documents/Code/parlay-data/nba_data`
- Dataset: `bdfa890a741bc36b4b80802232fdc685590c97823222d3a098131786b52def74`
- Window: `2026-01-22` to `2026-02-12`
- Strategies: `s001,s008,s010,s011,s014,s015,s020`

## Baseline scoreboards (aggregate)

Generated artifacts:
- `/Users/andy/Documents/Code/parlay-data/reports/odds/analysis/integration-baseline-max1-2026-02-14/aggregate-scoreboard.json`
- `/Users/andy/Documents/Code/parlay-data/reports/odds/analysis/integration-baseline-max2-2026-02-14/aggregate-scoreboard.json`
- `/Users/andy/Documents/Code/parlay-data/reports/odds/analysis/integration-baseline-max5-2026-02-14/aggregate-scoreboard.json`

Observed winners:
- `max_picks=1`: `s014` (`roi=0.511`, `rows_graded=40`)
- `max_picks=2`: `s014` (`roi=0.511`, `rows_graded=40`)
- `max_picks=5`: `s014` (`roi=0.511`, `rows_graded=40`)

`s020` aggregate in current packet:
- `max_picks=1`: `rows_graded=19`, `roi=0.389605`, gate fail (`insufficient_graded`)
- `max_picks=2`: `rows_graded=38`, `roi=0.345284`, gate pass
- `max_picks=5`: `rows_graded=88`, `roi=0.162747`, gate pass

## IM2 closure evidence (Track B)

- `parlay` merged PR #32 (`feat: Track B probabilistic minutes engine + s020`).
- `parlay-data` merged PR #7 (minutes-prob train/eval/predict artifacts + integration baseline scoreboards).
- Runtime remains conservative by default:
  - `/Users/andy/Documents/Code/parlay/config/runtime.toml` keeps
    `strategy.probabilistic_profile = "off"`.

Representative full-lake train/eval artifact:
- `/Users/andy/Documents/Code/parlay-data/nba_data/reports/analysis/minutes_prob/minutes_prob_v1-2023-24-2025-26-regular_season-h20-e14/evaluation.json`
  - rows: `3626`
  - MAE: `8.604335`
  - RMSE: `11.611174`
  - Brier(active): `0.135301`
  - Coverage p10/p90: `0.733867`

## Determinism checks

Deterministic replay checks (canonicalized JSON; ignoring `generated_at_utc` + run id):

- `max_picks=1`:  
  `/analysis/integration-baseline-max1-2026-02-14-a/aggregate-scoreboard.json`  
  vs  
  `/analysis/integration-baseline-max1-2026-02-14-b/aggregate-scoreboard.json`  
  -> identical

- `max_picks=2`:  
  `/analysis/integration-baseline-max2-2026-02-14-a/aggregate-scoreboard.json`  
  vs  
  `/analysis/integration-baseline-max2-2026-02-14-b/aggregate-scoreboard.json`  
  -> identical

- `max_picks=5`:  
  `/analysis/integration-baseline-max5-2026-02-14/aggregate-scoreboard.json`  
  vs  
  `/analysis/integration-baseline-max5-2026-02-14-rerun/aggregate-scoreboard.json`  
  -> identical

## IM3 closure evidence (deterministic replay + plan reasons/provenance)

Replay command (offline, fixed snapshot):

```bash
uv run prop-ev strategy run \
  --snapshot-id day-bdfa890a-2026-02-12 \
  --strategy s014 \
  --mode replay \
  --offline
```

Determinism check after replay hardening:
- strategy report hash (normalized, run1/run2):
  - `a24f50a6389b2c8f1a5bf9c3aa084a6528251f8dd3ee63cc17eafbd10a30375a`
  - `a24f50a6389b2c8f1a5bf9c3aa084a6528251f8dd3ee63cc17eafbd10a30375a`
- execution-plan hash (normalized, run1/run2):
  - `64209ad627f86230527068246e7e9ed1b8e5e820711b9d987583261d8c114337`
  - `64209ad627f86230527068246e7e9ed1b8e5e820711b9d987583261d8c114337`

Reason/provenance presence:
- plan exclusion reasons:
  - `portfolio_cap_daily=239`, `portfolio_cap_game=1`, `portfolio_cap_player=2`
- pricing provenance in replay candidates:
  - `line_source` contains `exact_point_pairs`
  - `books_used` populated from snapshot odds books

## Track E operational checks

Guardrails:

```bash
uv run prop-ev data guardrails --json
```

Result:
- status `ok`
- `violation_count=0`

Done-days behavior:

```bash
uv run prop-ev data done-days --dataset-id <id> --from 2026-01-22 --to 2026-02-12 --require-complete --json
```

Result:
- exits `2` (as designed) due two incomplete upstream-404 days (`2026-01-24`, `2026-01-25`)

Allowlisted rerun:

```bash
uv run prop-ev data done-days --dataset-id <id> --from 2026-01-22 --to 2026-02-12 --require-complete \
  --allow-incomplete-day 2026-01-24 --allow-incomplete-day 2026-01-25 --json
```

Result:
- exits `0`
- `preflight_pass=true`

## Milestone state at this checkpoint

- IM1: closed on `main` (pricing contract foundation landed).
- IM2: closed (Track B merged to `main`; default-off preserved).
- IM3: closed (deterministic replay proof + execution-plan reason/provenance evidence).
- IM4: closed (scoreboard + promotion-gate reproducibility packet for caps `1/2/5`).
- IM5: deferred (no safe default-on promotion flip yet; requires dedicated gate-pass PR).
