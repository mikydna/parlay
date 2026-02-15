# Integration Milestone Execution (IM1–IM5)

Date: 2026-02-14

This runbook operationalizes milestone closure from `docs/plan.md` with deterministic, offline,
no-credit workflows.

## Preconditions

- Runtime config points to `parlay-data` roots (default `config/runtime.toml` in this repo).
- Historical day-index dataset is already backfilled.
- `uv sync --all-groups` completed.

## Canonical dataset + strategy set

- Dataset: `bdfa890a741bc36b4b80802232fdc685590c97823222d3a098131786b52def74`
- Strategies for integration scoreboard:
  - `s001,s008,s010,s011,s014,s015,s020`
- Max-picks sweeps:
  - `1`, `2`, `5`

## Phase 0 — Baseline lock

Run once per cap. This captures a comparable aggregate scoreboard from complete days:

```bash
cd /Users/andy/Documents/Code/parlay
export PROP_EV_DATA_DIR=/Users/andy/Documents/Code/parlay-data/odds_api
export PROP_EV_NBA_DATA_DIR=/Users/andy/Documents/Code/parlay-data/nba_data

uv run prop-ev strategy backtest-summarize \
  --snapshot-id day-bdfa890a-2026-02-12 \
  --strategies s001,s008,s010,s011,s014,s015,s020 \
  --all-complete-days \
  --dataset-id bdfa890a741bc36b4b80802232fdc685590c97823222d3a098131786b52def74 \
  --min-graded 20 \
  --bin-size 0.1 \
  --write-analysis-scoreboard \
  --analysis-run-id integration-baseline-max5-2026-02-14
```

Repeat with `integration-baseline-max1-2026-02-14` and `integration-baseline-max2-2026-02-14`
after regenerating backtest templates with those caps.

Expected output:

- `/Users/andy/Documents/Code/parlay-data/reports/odds/analysis/<run_id>/aggregate-scoreboard.json`

## Determinism check (required for milestone packets)

Run the same summarize command twice with different run ids and compare payloads after removing
timestamp/run-id fields:

```bash
cd /Users/andy/Documents/Code/parlay
uv run pytest -q tests/test_cli_strategy_backtest_summarize.py::test_strategy_backtest_summarize_analysis_scoreboard_is_deterministic
```

## Track B fixed-point loop (minutes-prob + `s020`)

Use bounded knobs only:

- `history_games`
- `min_history_games`
- `min_prob_confidence`
- `max_minutes_band`

Mandatory constraints:

- walk-forward split only (no same-day leakage),
- no default strategy flip,
- keep profile explicit (`minutes_v1`) for evaluation.

## IM closure packet checklist

For each IM closure PR include:

1. Contract/behavior summary and whether defaults changed.
2. Compatibility + rollback note.
3. Deterministic replay evidence.
4. Impact summary on:
   - strategy report,
   - execution plan,
   - aggregate scoreboard.

## IM5 policy

IM5 is only closed with an explicit default-flip PR that includes:

- before/after scoreboard deltas,
- promotion-gate pass reasons,
- rollback command/config path.

If gates are mixed, document defer reasons and keep defaults unchanged.
