# nba-prop-ev

[![CI](https://github.com/<OWNER>/<REPO>/actions/workflows/ci.yml/badge.svg)](https://github.com/<OWNER>/<REPO>/actions/workflows/ci.yml)

Cache-first NBA odds snapshot pipeline for The Odds API v4.

## Install

```bash
uv sync --all-groups
```

## Environment

```bash
cp .env.example .env
```

Set `ODDS_API_KEY` in `.env` (or `PROP_EV_ODDS_API_KEY`).
If env vars are missing, the CLI also checks key files at repo root:
`ODDS_API_KEY` / `ODDS_API_KEY.ignore`.
Set `OPENAI_API_KEY` for LLM summaries, or place the key in `OPENAI_KEY` /
`OPENAI_KEY.ignore` at repo root.

Bookmaker whitelist defaults are in `config/bookmakers.json` (currently DraftKings + FanDuel).
When `--bookmakers` is omitted, snapshot/playbook commands use this whitelist automatically.

## Run

```bash
uv run prop-ev --help
uv run prop-ev snapshot slate --dry-run
uv run prop-ev snapshot props --dry-run --max-events 10
uv run prop-ev playbook budget
make ci
```

## Snapshot Workflow

Create a slate snapshot (featured spreads/totals):

```bash
uv run prop-ev snapshot slate --max-credits 20
```

Create player props snapshots (per-event endpoint, cache-first):

```bash
uv run prop-ev snapshot props --markets player_points --max-events 10 --max-credits 20
```

Override whitelist for a specific run:

```bash
uv run prop-ev snapshot props --bookmakers draftkings
```

Reuse stored data without network:

```bash
uv run prop-ev snapshot ls
uv run prop-ev snapshot show --snapshot-id <SNAPSHOT_ID>
uv run prop-ev snapshot props --snapshot-id <SNAPSHOT_ID> --offline
uv run prop-ev strategy run --snapshot-id <SNAPSHOT_ID> --offline
```

Dev mode with free calls allowed but paid odds endpoints blocked:

```bash
uv run prop-ev playbook run --block-paid
```

This allows free endpoints (like event listing) but makes paid endpoints cache-only.

Strategy reports are written to:
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-report.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-report.md`

Strategy context caches are written to:
- `data/odds_api/snapshots/<SNAPSHOT_ID>/context/injuries.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/context/roster.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/context/official_injury_pdf/latest.pdf`

Global context mirrors (for fallback and reruns) are written to:
- `data/odds_api/reference/injuries/latest.json`
- `data/odds_api/reference/rosters/latest.json`
- `data/odds_api/reference/rosters/roster-YYYY-MM-DD.json`

## Playbook Workflow (Reader-Friendly Briefs)

Run end-to-end with live-window gating and budget controls:

```bash
uv run prop-ev playbook run --month 2026-02
```

Preflight behavior:
- `playbook run` now checks injury/roster context gates before paid odds fetches.
- If context gates fail, the run skips paid odds calls and falls back to latest cached snapshot.

Run one-shot discovery vs execution comparison:

```bash
uv run prop-ev playbook discover-execute \
  --execution-bookmakers draftkings,fanduel \
  --allow-tier-b
```

This runs:
- discovery snapshot (all books in region) for signal,
- execution snapshot (your books) for actionability,
- a comparison report in the execution snapshot reports folder.

Force offline rerun from latest cached snapshot:

```bash
uv run prop-ev playbook run --offline
```

Render a specific snapshot into markdown + LaTeX + PDF artifacts:

```bash
uv run prop-ev playbook render --snapshot-id <SNAPSHOT_ID> --offline
```

Show monthly odds + LLM budget status:

```bash
uv run prop-ev playbook budget --month 2026-02
```

Playbook outputs per snapshot:
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/brief-input.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/brief-pass1.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-brief.md`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-brief.tex`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-brief.pdf` (if `tectonic` exists)
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-brief.meta.json`

Discovery vs execution report output:
- `data/odds_api/snapshots/<EXEC_SNAPSHOT_ID>/reports/discovery-execution.json`
- `data/odds_api/snapshots/<EXEC_SNAPSHOT_ID>/reports/discovery-execution.md`

Strategy report health now includes:
- feed status contract (`official_injuries`, `secondary_injuries`, `roster`)
- odds freshness contract (`latest_quote_utc`, age, stale threshold)
- event mapping contract (missing event ids)
- automatic mode downgrade to `watchlist_only` when required data is missing/stale.

Latest mirrors:
- `data/odds_api/reports/latest/strategy-brief.md`
- `data/odds_api/reports/latest/strategy-brief.tex`
- `data/odds_api/reports/latest/strategy-brief.pdf` (if generated)
- `data/odds_api/reports/latest/latest.json`

## Free-Tier Guardrails (500 credits/month)

- Featured endpoint estimate: `credits ~= (#markets) x (regions_equivalent)`.
- Per-event props estimate: `credits ~= (#events) x (#markets) x (regions_equivalent)`.
- Defaults are conservative:
  - `snapshot slate`: `markets=spreads,totals`, `regions=us`.
  - `snapshot props`: `markets=player_points`.
- Cap each run with `--max-credits`; override intentionally with `--force`.
- Reruns should prefer cached snapshot data; use `--refresh` only when you need fresh lines.
- Use `--block-paid` during local iteration when you want to avoid spending credits.

Credit tools:

```bash
uv run prop-ev credits budget --events 10 --markets player_points --regions us
uv run prop-ev credits report --month 2026-02
```

## Reliability Gates

- Default no-bet gate:
  - if official injury source is missing, strategy mode becomes `watchlist_only`.
  - if quote timestamps are stale, strategy mode becomes `watchlist_only`.
- Tune via env vars:
  - `PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES=true|false`
  - `PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT=true|false`
  - `PROP_EV_STRATEGY_STALE_QUOTE_MINUTES=20`
  - `PROP_EV_CONTEXT_INJURIES_STALE_HOURS=6`
  - `PROP_EV_CONTEXT_ROSTER_STALE_HOURS=24`

## Scheduled Runs

Use the schedule-ready runbook in `docs/scheduled-flow.md`.

## CI Badge

Replace `<OWNER>/<REPO>` in the badge URL after pushing to GitHub.
