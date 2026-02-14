# Runbook

Operator-focused flow for daily execution, offline replay, and health triage.

Assume `config/runtime.toml` points at
`/Users/$USER/Documents/Code/parlay-data/{odds_api,nba_data,reports,runtime}`.
Use CLI flags (`--config`, `--data-dir`, `--nba-data-dir`, `--reports-dir`, `--runtime-dir`)
for per-run overrides.

## Standard Run (Auto-gated Live/Offline)

```bash
ODDS_API_KEY="$(tr -d '\r\n' < ODDS_API_KEY.ignore)" \
OPENAI_API_KEY="$(tr -d '\r\n' < OPENAI_KEY.ignore)" \
uv run prop-ev playbook run \
  --markets player_points \
  --max-events 10 \
  --max-credits 20 \
  --exit-on-no-games
```

Behavior:
- always checks events first (free endpoint),
- exits `0` early on no-slate windows when `--exit-on-no-games` is enabled,
- runs live snapshot only when inside configured window and paid calls are allowed,
- otherwise falls back to latest cached snapshot,
- always runs strategy + brief generation after snapshot selection.
- emits `mode=<id>` plus `mode_desc=<human text>` for run-path selection.

## Offline / No-Paid Modes

No network:

```bash
uv run prop-ev playbook run --offline
```

Allow free endpoints only but block paid odds calls:

```bash
uv run prop-ev playbook run --block-paid
```

Render an existing snapshot only:

```bash
uv run prop-ev playbook render --snapshot-id <SNAPSHOT_ID> --offline
```

Write markdown artifact only when explicitly requested:

```bash
uv run prop-ev playbook render --snapshot-id <SNAPSHOT_ID> --offline --write-markdown
```

## Zero-Credit Readiness Validation

Use this sequence before any paid pull:

```bash
uv run prop-ev credits report --month 2026-02
uv run prop-ev data datasets ls --json
uv run prop-ev data datasets show --dataset-id <DATASET_ID> --json
uv run prop-ev data status --dataset-id <DATASET_ID> --from 2026-01-22 --to 2026-02-12 --json-summary
uv run prop-ev data verify --dataset-id <DATASET_ID> --from 2026-01-22 --to 2026-02-12 --json
uv run prop-ev data repair-derived --dataset-id <DATASET_ID> --from 2026-01-22 --to 2026-02-12 --json
uv run prop-ev data verify --dataset-id <DATASET_ID> --from 2026-01-22 --to 2026-02-12 --require-complete --require-parquet --require-canonical-jsonl --json
uv run prop-ev data guardrails --json
```

Check known cache misses without spending:

```bash
uv run prop-ev data backfill \
  --historical \
  --historical-anchor-hour-local 12 \
  --historical-pre-tip-minutes 60 \
  --markets player_points \
  --bookmakers draftkings,fanduel,espnbet,betmgm,betrivers,williamhill_us,bovada,fanatics \
  --from 2026-01-24 --to 2026-01-25 \
  --no-spend \
  --dry-run
```

Expected behavior:
- `actual_paid_credits=0` on every day row,
- unresolved paid misses are reported as `spend_blocked` in no-spend mode,
- no paid odds calls are executed.

Legacy-path migration (run once per storage root):

```bash
uv run prop-ev data migrate-layout --apply --json
uv run prop-ev data guardrails --json
```

Derived quote-table contract verification (snapshot-level):

```bash
uv run prop-ev snapshot verify \
  --snapshot-id <SNAPSHOT_ID> \
  --check-derived \
  --require-table event_props \
  --require-parquet
```

Expected behavior:
- fails (`exit=2`) when canonical JSONL contract/parity checks fail,
- fails (`exit=2`) when required parquet mirrors are missing,
- succeeds (`exit=0`) only when request artifacts and derived contracts are healthy.

## Health and Injury Policy

Health command:

```bash
uv run prop-ev strategy health --snapshot-id <SNAPSHOT_ID> --offline
```

Exit codes:
- `0` healthy
- `1` degraded
- `2` broken

Defaults:
- official injury source required by default,
- secondary injury source is override-only (`--allow-secondary-injuries` or config),
- stale context gates strategy/playbook into degraded or blocked paths depending on command.

## Threshold Semantics

Current strategy enforces EV floors as:
- Tier A floor: `max(--min-ev, 0.03)`
- Tier B floor: `max(--min-ev, 0.05)`
- portfolio cap default: `strategy.max_picks_default` (runtime config, default `5`)
- per-run override: `uv run prop-ev strategy run --max-picks <N>`

This is current behavior, not a pending proposal.

Strategy IDs:
- `s001` — Baseline Core
- `s002` — Baseline Core + Tier B
- `s003` — Median No-Vig Baseline
- `s004` — Min-2 Book-Pair Gate
- `s005` — Hold-Cap Gate
- `s006` — Dispersion-IQR Gate
- `s007` — Quality Composite Gate (s003 + s004 + s005)
- `s008` — Conservative Quality Floor (s007 + dispersion + quality/uncertainty/EV-low gates)
- `s009` — Conservative Quality + Rolling Priors (s008 + rolling settled-outcome ranking tilt)
- `s010` — Tier B + Quality Floor (s002 + conservative quality/uncertainty gates)
- `s011` — Tier B + Quality + Rolling Priors (s010 + rolling settled-outcome ranking tilt)
- `s012` — Tier B + Aggressive Best EV (s002 + best-EV portfolio ranking)
- `s013` — Tier B + Quality-Weighted EV Low (s002 + quality-weighted conservative EV-low ranking)
- `s014` — Median No-Vig + Tier B (s003 + tier-B edges)
- `s015` — Tier B + Calibrated EV Low (s010 + rolling calibration feedback ranking)

Legacy IDs are not accepted.

Implementation model:
- each strategy is a plugin in `src/prop_ev/strategies/`,
- plugins share a composable recipe layer stack (baseline + optional gates),
- `uv run prop-ev strategy ls` prints `strategy_id`, title, and description.

## Key Artifacts

Per snapshot:
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/strategy-report.json`
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/execution-plan.json`
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/backtest-seed.jsonl`
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/backtest-results-template.csv`
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/backtest-readiness.json`
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/brief-input.json`
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/brief-pass1.json`
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/strategy-brief.md` (only with `--write-markdown`)
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/strategy-brief.tex`
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/strategy-brief.pdf` (when PDF tooling is installed)
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/strategy-brief.meta.json`

Latest mirrors:
- `<REPORTS_DIR>/latest/strategy-report.json`
- `<REPORTS_DIR>/latest/strategy-brief.meta.json`
- `<REPORTS_DIR>/latest/strategy-brief.pdf` (if generated)
- `<REPORTS_DIR>/latest/latest.json`

Cross-day backtest summary:

```bash
uv run prop-ev strategy backtest-summarize \
  --strategies s008 \
  --all-complete-days \
  --dataset-id <DATASET_ID> \
  --power-picks-per-day 5 \
  --power-target-uplifts 0.01,0.02,0.03,0.05 \
  --write-calibration-map \
  --calibration-map-mode walk_forward
```

Outputs:
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/backtest-summary.json`
- `<REPORTS_DIR>/by-snapshot/<snapshot_id>/backtest-calibration-map.json` (when `--write-calibration-map` is enabled)
- `backtest-summary.json` includes `power_guidance` with required-day / required-row estimates versus baseline for each strategy.

Use calibration map during brief render (optional):

```bash
uv run prop-ev playbook render \
  --snapshot-id <SNAPSHOT_ID> \
  --offline \
  --calibration-map-file backtest-calibration-map.json
```
