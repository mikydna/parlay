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
