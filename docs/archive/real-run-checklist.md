# Real End-to-End Run Checklist

This is the clean execution path after archive cleanup.

## Current Baseline

- Active snapshots kept:
  - `data/odds_api/snapshots/2026-02-11T18-46-19Z`
  - `data/odds_api/snapshots/2026-02-11T19-21-25Z-preflight-check`
- Archived test/demo snapshots:
  - `data/odds_api/archive/pre-real-run-2026-02-11/`
- Latest report pointer:
  - `data/odds_api/reports/latest/latest.json` -> `2026-02-11T18-46-19Z`

## 0) Budget Check (No Paid Calls)

```bash
uv run prop-ev playbook budget --month 2026-02
```

## 1) Real Live End-to-End Run (Paid Odds Calls)

Run this close to tipoff window.

```bash
ODDS_API_KEY="$(tr -d '\r\n' < ODDS_API_KEY.ignore)" \
OPENAI_API_KEY="$(tr -d '\r\n' < OPENAI_KEY.ignore)" \
uv run prop-ev playbook run \
  --bookmakers draftkings,fanduel \
  --allow-tier-b \
  --refresh-context \
  --max-events 10 \
  --max-credits 40 \
  --top-n 5 \
  --per-game-top-n 5
```

What this does:
- Uses live window gating (inside window => live fetch; outside => offline fallback).
- Uses DK/FD as execution books.
- Produces one summary/action section, game-card pages, then disclosures at end.

## 2) Optional East-Coast Only Window

If you want just early ET games, add a bounded commence window.

```bash
--commence-from 2026-02-12T00:00:00Z --commence-to 2026-02-12T02:30:00Z
```

## 3) Dev Iteration (No Paid Odds Calls)

Re-render from cached snapshot while editing layout:

```bash
uv run prop-ev playbook render --snapshot-id 2026-02-11T18-46-19Z --offline --top-n 5 --per-game-top-n 5
```

## 4) Output Locations

- Snapshot outputs:
  - `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.md`
  - `data/odds_api/snapshots/<snapshot_id>/reports/strategy-brief.pdf`
- Latest mirror:
  - `data/odds_api/reports/latest/strategy-brief.pdf`

