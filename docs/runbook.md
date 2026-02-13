# Runbook

Operator-focused flow for daily execution, offline replay, and health triage.

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

## Zero-Credit Readiness Validation

Use this sequence before any paid pull:

```bash
uv run prop-ev credits report --month 2026-02
uv run prop-ev data datasets ls --json
uv run prop-ev data datasets show --dataset-id <DATASET_ID> --json
uv run prop-ev data status --dataset-id <DATASET_ID> --from 2026-01-22 --to 2026-02-12 --json-summary
uv run prop-ev data verify --dataset-id <DATASET_ID> --from 2026-01-22 --to 2026-02-12 --json
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
- secondary injury source is override-only (`--allow-secondary-injuries` or env),
- stale context gates strategy/playbook into degraded or blocked paths depending on command.

## Threshold Semantics

Current strategy enforces EV floors as:
- Tier A floor: `max(--min-ev, 0.03)`
- Tier B floor: `max(--min-ev, 0.05)`

This is current behavior, not a pending proposal.

Strategy IDs:
- `s001` — Baseline Core
- `s002` — Baseline Core + Tier B
- `s003` — Median No-Vig Baseline
- `s004` — Min-2 Book-Pair Gate
- `s005` — Hold-Cap Gate
- `s006` — Dispersion-IQR Gate
- `s007` — Quality Composite Gate (s003 + s004 + s005)

Legacy IDs are not accepted.

Implementation model:
- each strategy is a plugin in `src/prop_ev/strategies/`,
- plugins share a composable recipe layer stack (baseline + optional gates),
- `uv run prop-ev strategy ls` prints `strategy_id`, title, and description.

## Key Artifacts

Per snapshot:
- `data/odds_api/snapshots/<snapshot_id>/reports/strategy-report.json`
- `data/odds_api/snapshots/<snapshot_id>/reports/strategy-report.md`
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-seed.jsonl`
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-results-template.csv`
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-readiness.json`
- `data/odds_api/snapshots/<snapshot_id>/reports/brief-input.json`
- `data/odds_api/snapshots/<snapshot_id>/reports/brief-pass1.json`
- `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.md`
- `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.tex`
- `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.pdf` (when PDF tooling is installed)
- `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.meta.json`

Latest mirrors:
- `data/odds_api/reports/latest/strategy-brief.md`
- `data/odds_api/reports/latest/strategy-brief.tex`
- `data/odds_api/reports/latest/strategy-brief.pdf` (if generated)
- `data/odds_api/reports/latest/latest.json`
