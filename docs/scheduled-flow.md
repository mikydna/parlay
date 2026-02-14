# Scheduled Flow (Reproducible)

This flow uses a single playbook command that self-gates between live and offline modes.

For operator policy and contract references, see:
- `docs/runbook.md`
- `docs/contracts.md`

`REPORTS_DIR` defaults to `paths.reports_dir` from `config/runtime.toml`.

## Hourly Schedule Command

```bash
ODDS_API_KEY="$(tr -d '\r\n' < ODDS_API_KEY.ignore)" \
OPENAI_API_KEY="$(tr -d '\r\n' < OPENAI_KEY.ignore)" \
uv run prop-ev playbook run \
  --markets player_points \
  --max-events 10 \
  --max-credits 20 \
  --exit-on-no-games
```

`--exit-on-no-games` returns `0` without running strategy/brief generation when
events lookup is empty.

If `--bookmakers` is not passed, the command uses `config/bookmakers.json`
(default: `draftkings,fanduel`).

`playbook run` will:
- fetch events (free endpoint),
- check if now is inside the configured live window (`pre_tip=3h`, `post_tip=1h`),
- skip live odds fetch when outside window or monthly odds cap is reached,
- run deterministic strategy report,
- build PDF + LaTeX brief artifacts (`--write-markdown` is opt-in).

## Offline Re-Run (No Credits)

```bash
uv run prop-ev playbook run --offline
```

Render a specific cached snapshot:

```bash
uv run prop-ev playbook render --snapshot-id 2026-02-11T16-44-54-ET --offline
```

## Validate Snapshot Integrity

```bash
uv run prop-ev snapshot verify --snapshot-id 2026-02-11T16-44-54-ET
```

## Validate Day-Index Coverage (No Spend)

```bash
uv run prop-ev data datasets ls --json
uv run prop-ev data datasets show --dataset-id <DATASET_ID> --json
uv run prop-ev data status --dataset-id <DATASET_ID> --from 2026-01-22 --to 2026-02-12 --json-summary
```

## What You Get Per Run

- Raw request/response/meta cache under `<odds_data_dir>/snapshots/<snapshot_id>/`
- Normalized props table:
  - `<odds_data_dir>/snapshots/<snapshot_id>/derived/event_props.jsonl`
- Strategy outputs:
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/strategy-report.json`
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/backtest-seed.jsonl`
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/backtest-results-template.csv`
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/backtest-readiness.json`
- Playbook outputs:
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/brief-input.json`
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/brief-pass1.json`
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/strategy-brief.md` (only with `--write-markdown`)
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/strategy-brief.tex`
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/strategy-brief.pdf` (if `tectonic` installed)
  - `<REPORTS_DIR>/by-snapshot/<report_snapshot>/strategy-brief.meta.json`
- Latest mirrors:
  - `<REPORTS_DIR>/latest/strategy-report.json`
  - `<REPORTS_DIR>/latest/strategy-brief.meta.json`
  - `<REPORTS_DIR>/latest/strategy-brief.pdf` (if generated)
  - `<REPORTS_DIR>/latest/latest.json`

## Current Gaps (Explicit)

- Official NBA injury page can be unreachable from some environments.
- EV/Kelly currently uses market-implied probabilities with availability adjustments
  (not a full minutes/usage/pace model yet).

The current strategy report is deterministic and includes:
- Tier A/B market depth checks
- Injury + roster eligibility gates
- EV and Kelly from best available prices
