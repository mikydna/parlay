# Backtest Prep (Next-Day Grading)

Use the snapshot artifacts from strategy run, then grade outcomes tomorrow.

## 1) Generate backtest artifacts

`strategy run` now auto-writes:

- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-seed.jsonl`
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-results-template.csv`
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-readiness.json`

You can re-generate explicitly:

```bash
uv run prop-ev strategy backtest-prep --snapshot-id <snapshot_id> --selection eligible
```

Selection modes:

- `eligible` (default): all eligible candidate lines
- `ranked`: ranked plays only
- `top_ev`: Tier A top EV plays only
- `one_source`: Tier B plays only
- `all_candidates`: includes ineligible rows

## 2) What to fill tomorrow

In `backtest-results-template.csv`, fill:

- `actual_stat_value`
- `result` (`win`, `loss`, `push`)
- `graded_price_american` (if your execution price differed)
- `stake_units` (optional, required for PnL)
- `pnl_units`
- `graded_at_utc`
- `grading_notes` (optional)

## 3) Readiness gate

Check:

```bash
jq . data/odds_api/snapshots/<snapshot_id>/reports/backtest-readiness.json
```

`ready_for_backtest_seed=true` means snapshot inputs are complete enough for grading prep.

## 4) Auto settlement (final + in-progress)

Use the settlement command to fetch NBA live boxscores and grade seeded tickets.

```bash
uv run prop-ev strategy settle --snapshot-id <snapshot_id> --refresh-results
```

Outputs:

- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-settlement.json`
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-settlement.md`
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-settlement.tex`
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-settlement.pdf`
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-settlement.meta.json`
- optional CSV via `--write-csv`:
  `data/odds_api/snapshots/<snapshot_id>/reports/backtest-settlement.csv`

Behavior:

- Final games are graded `win|loss|push`.
- In-progress and scheduled games remain `pending`.
- Rows that cannot be resolved (missing player/game, unsupported market) are `unresolved`.

Exit codes:

- `0`: all rows fully settled (`win|loss|push`)
- `1`: partial settlement (`pending` or `unresolved` rows remain)
- `2`: command failure (missing seed, fetch/cache error, invalid payload)
