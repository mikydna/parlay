# Scheduled Flow (Reproducible)

This flow uses a single playbook command that self-gates between live and offline modes.

For operator policy and contract references, see:
- `docs/runbook.md`
- `docs/contracts.md`

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
- build markdown + LaTeX + PDF brief artifacts.

## Offline Re-Run (No Credits)

```bash
uv run prop-ev playbook run --offline
```

Render a specific cached snapshot:

```bash
uv run prop-ev playbook render --snapshot-id 2026-02-11T16-44-54Z --offline
```

## Validate Snapshot Integrity

```bash
uv run prop-ev snapshot verify --snapshot-id 2026-02-11T16-44-54Z
```

## What You Get Per Run

- Raw request/response/meta cache under `data/odds_api/snapshots/<snapshot_id>/`
- Normalized props table:
  - `data/odds_api/snapshots/<snapshot_id>/derived/event_props.jsonl`
- Strategy outputs:
  - `data/odds_api/snapshots/<snapshot_id>/reports/strategy-report.json`
  - `data/odds_api/snapshots/<snapshot_id>/reports/strategy-report.md`
  - `data/odds_api/snapshots/<snapshot_id>/reports/backtest-seed.jsonl`
  - `data/odds_api/snapshots/<snapshot_id>/reports/backtest-results-template.csv`
  - `data/odds_api/snapshots/<snapshot_id>/reports/backtest-readiness.json`
- Playbook outputs:
  - `data/odds_api/snapshots/<snapshot_id>/reports/brief-input.json`
  - `data/odds_api/snapshots/<snapshot_id>/reports/brief-pass1.json`
  - `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.md`
  - `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.tex`
  - `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.pdf` (if `tectonic` installed)
  - `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.meta.json`
- Latest mirrors:
  - `data/odds_api/reports/latest/strategy-brief.md`
  - `data/odds_api/reports/latest/strategy-brief.tex`
  - `data/odds_api/reports/latest/strategy-brief.pdf` (if generated)
  - `data/odds_api/reports/latest/latest.json`

## Current Gaps (Explicit)

- Official NBA injury page can be unreachable from some environments.
- EV/Kelly currently uses market-implied probabilities with availability adjustments
  (not a full minutes/usage/pace model yet).

The current strategy report is deterministic and includes:
- Tier A/B market depth checks
- Injury + roster eligibility gates
- EV and Kelly from best available prices
