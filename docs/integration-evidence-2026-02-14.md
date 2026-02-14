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
- IM2: in progress (Track B branch refinement pending merge).
- IM3: partial (pricing interpolation + execution-plan baseline landed; closure packet pending).
- IM4: partial (scoreboard/promotion machinery landed; closure packet completed here for baseline evidence).
- IM5: open (no default-flip PR yet).
