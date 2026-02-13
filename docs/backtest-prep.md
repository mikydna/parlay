# Backtest Prep (Next-Day Grading)

Use the snapshot artifacts from strategy run, then grade outcomes tomorrow.

## 1) Generate backtest artifacts

`strategy run` now auto-writes:

- `<REPORTS_DIR>/snapshots/<report_snapshot>/backtest-seed.jsonl`
- `<REPORTS_DIR>/snapshots/<report_snapshot>/backtest-results-template.csv`
- `<REPORTS_DIR>/snapshots/<report_snapshot>/backtest-readiness.json`

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
jq . <REPORTS_DIR>/snapshots/<report_snapshot>/backtest-readiness.json
```

`ready_for_backtest_seed=true` means snapshot inputs are complete enough for grading prep.

## 4) Auto settlement (final + in-progress)

Use the settlement command to load NBA results via the unified repository and grade seeded tickets.

```bash
uv run prop-ev strategy settle --snapshot-id <snapshot_id> --refresh-results --results-source auto
```

Outputs:

- `<REPORTS_DIR>/snapshots/<report_snapshot>/backtest-settlement.json`
- `<REPORTS_DIR>/snapshots/<report_snapshot>/backtest-settlement.md`
- `<REPORTS_DIR>/snapshots/<report_snapshot>/backtest-settlement.tex`
- `<REPORTS_DIR>/snapshots/<report_snapshot>/backtest-settlement.pdf`
- `<REPORTS_DIR>/snapshots/<report_snapshot>/backtest-settlement.meta.json`
- optional CSV via `--write-csv`:
  `<REPORTS_DIR>/snapshots/<report_snapshot>/backtest-settlement.csv`

Behavior:

- Source policy is controlled with `--results-source`:
  - `auto` (default): historical-first for past snapshots, live-first for same-day snapshots.
  - `historical`: use historical schedule + boxscores path only.
  - `live`: use live scoreboard + boxscores path only.
  - `cache_only`: require existing cached results.
- `--offline` forces cache-only behavior and ignores `--refresh-results`.
- Final games are graded `win|loss|push`.
- In-progress and scheduled games remain `pending`.
- Rows that cannot be resolved (missing player/game, unsupported market) are `unresolved`.

Exit codes:

- `0`: all rows fully settled (`win|loss|push`)
- `1`: partial settlement (`pending` or `unresolved` rows remain)
- `2`: command failure (missing seed, fetch/cache error, invalid payload)
