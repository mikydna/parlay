# Backtest Prep (Next-Day Grading)

Use the snapshot artifacts from strategy run, then grade outcomes tomorrow.

## 1) Generate backtest artifacts

Backtest artifacts are generated explicitly (to keep reports compact):

```bash
uv run prop-ev strategy backtest-prep --snapshot-id <snapshot_id> --selection ranked
```

Outputs:

- `<REPORTS_DIR>/by-snapshot/<report_snapshot>/backtest-seed.jsonl`
- `<REPORTS_DIR>/by-snapshot/<report_snapshot>/backtest-results-template.csv`
- `<REPORTS_DIR>/by-snapshot/<report_snapshot>/backtest-readiness.json`

Optional during strategy run (same outputs, noisier):

```bash
uv run prop-ev strategy run --snapshot-id <snapshot_id> --write-backtest-artifacts
```

Note: `--write-backtest-artifacts` writes a `ranked` seed by default (the executed portfolio picks).

Selection modes:

- `ranked`: executed portfolio picks (bounded by `--max-picks`; recommended for backtests)
- `eligible`: all eligible candidate lines (diagnostic; not a realistic execution backtest)
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
jq . <REPORTS_DIR>/by-snapshot/<report_snapshot>/backtest-readiness.json
```

`ready_for_backtest_seed=true` means snapshot inputs are complete enough for grading prep.

## 4) Auto settlement (final + in-progress)

Use the settlement command to load NBA results via the unified repository and grade seeded tickets.

```bash
uv run prop-ev strategy settle --snapshot-id <snapshot_id> --refresh-results --results-source auto
```

Outputs:

- `<REPORTS_DIR>/by-snapshot/<report_snapshot>/settlement.json`
- `<REPORTS_DIR>/by-snapshot/<report_snapshot>/settlement.pdf` (omit with `--no-pdf`)
- `<REPORTS_DIR>/by-snapshot/<report_snapshot>/settlement.meta.json`
- optional markdown via `--write-markdown`:
  `<REPORTS_DIR>/by-snapshot/<report_snapshot>/settlement.md`
- optional tex via `--keep-tex`:
  `<REPORTS_DIR>/by-snapshot/<report_snapshot>/settlement.tex`
- optional CSV via `--write-csv`:
  `<REPORTS_DIR>/by-snapshot/<report_snapshot>/settlement.csv` (or `settlement.<strategy>.csv` when settling a non-canonical seed)

Behavior:

- Source policy is controlled with `--results-source`:
  - `auto` (default): historical-first for past snapshots, live-first for same-day snapshots.
  - `historical`: use historical schedule + boxscores path only.
  - `live`: use live scoreboard + boxscores path only.
  - `cache_only`: require existing cached results.
- By default, settle uses the strategy report path recorded in
  `strategy-brief.meta.json` (or `strategy-report.json` fallback), so settlement aligns with the
  brief. Use `--seed-path` to force a specific seed file.
- `--offline` forces cache-only behavior and ignores `--refresh-results`.
- When `--seed-path` points at a suffixed strategy seed (e.g. `backtest-seed.s008.jsonl`),
  settlement outputs are written with the same suffix to avoid overwriting the canonical report.
- Final games are graded `win|loss|push`.
- In-progress and scheduled games remain `pending`.
- Rows that cannot be resolved (missing player/game, unsupported market) are `unresolved`.

Exit codes:

- `0`: all rows fully settled (`win|loss|push`)
- `1`: partial settlement (`pending` or `unresolved` rows remain)
- `2`: command failure (missing seed, fetch/cache error, invalid payload)
