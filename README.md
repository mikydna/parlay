# nba-prop-ev

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
uv run prop-ev --data-dir /Users/$USER/Documents/Code/parlay-data/odds_api snapshot ls
uv run prop-ev snapshot slate --dry-run
uv run prop-ev snapshot props --dry-run --max-events 10
uv run prop-ev strategy health --offline
uv run prop-ev strategy ls
uv run prop-ev playbook budget
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest -q
```

## NBA Data CLI (`nba-data`)

Use `nba-data` for resumable historical NBA data ingestion (separate from `prop-ev`):

```bash
uv run nba-data discover --seasons 2023-24,2024-25,2025-26 --season-type "Regular Season"
uv run nba-data ingest --seasons 2023-24,2024-25,2025-26 --season-type "Regular Season"
uv run nba-data clean --seasons 2023-24,2024-25,2025-26 --season-type "Regular Season"
uv run nba-data verify --seasons 2023-24,2024-25,2025-26 --season-type "Regular Season"
uv run nba-data export clean --data-dir data/nba_data --dst-data-dir ../parlay-data/nba_data
uv run nba-data export raw-archive --data-dir data/nba_data --dst-data-dir ../parlay-data/nba_data
```

Artifacts are written under `data/nba_data`. Ingest is resume-safe and skips already valid raw mirrors.

## Unified NBA Handle

Runtime NBA reads now flow through one repository handle (`NBARepository`) for:

- settlement results (historical/live/cache policy),
- injury context,
- roster context.

Consumer modules should not fetch NBA endpoints directly.

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
uv run prop-ev strategy compare --snapshot-id <SNAPSHOT_ID> --strategies v0,baseline_median_novig --offline
```

Bundle snapshots and convert JSONL -> Parquet:

```bash
uv run prop-ev snapshot lake --snapshot-id <SNAPSHOT_ID>
uv run prop-ev snapshot pack --snapshot-id <SNAPSHOT_ID>
uv run prop-ev snapshot unpack --bundle data/odds_api/bundles/snapshots/<SNAPSHOT_ID>.tar.zst
```

Historical day backfill (paid key, per-event historical endpoints):

```bash
uv run prop-ev data backfill \
  --historical \
  --historical-anchor-hour-local 12 \
  --historical-pre-tip-minutes 60 \
  --from 2026-01-20 --to 2026-02-01 \
  --markets player_points \
  --bookmakers draftkings,fanduel,espnbet,betmgm,betrivers,williamhill_us,bovada,fanatics \
  --max-credits 500
```

Machine-readable status summary for a day range:

```bash
uv run prop-ev data status \
  --historical \
  --from 2026-02-01 --to 2026-02-12 \
  --markets player_points \
  --bookmakers draftkings,fanduel,espnbet,betmgm,betrivers,williamhill_us,bovada,fanatics \
  --json-summary
```

Discover stored day-index datasets (avoids spec mismatch confusion):

```bash
uv run prop-ev data datasets ls --json
uv run prop-ev data datasets show --dataset-id <DATASET_ID> --json
uv run prop-ev data status --dataset-id <DATASET_ID> --from 2026-02-01 --to 2026-02-12 --json-summary
```

No-spend completeness check (cache-only, no paid calls):

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

Dev mode with free calls allowed but paid odds endpoints blocked:

```bash
uv run prop-ev playbook run --block-paid
```

This allows free endpoints (like event listing) but makes paid endpoints cache-only.

Strategy reports are written to:
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-report.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-report.md`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/backtest-seed.jsonl`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/backtest-results-template.csv`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/backtest-readiness.json`

Per-strategy runs also write suffixed artifacts:
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-report.<STRATEGY_ID>.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/strategy-report.<STRATEGY_ID>.md`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/reports/backtest-results-template.<STRATEGY_ID>.csv`

Rebuild backtest artifacts for any snapshot:

```bash
uv run prop-ev strategy backtest-prep --snapshot-id <SNAPSHOT_ID> --selection eligible
uv run prop-ev strategy settle --snapshot-id <SNAPSHOT_ID> --results-source auto --refresh-results
uv run prop-ev strategy backtest-summarize --snapshot-id <SNAPSHOT_ID> --strategies v0,baseline_median_novig
```

Strategy context caches are written to:
- `data/odds_api/snapshots/<SNAPSHOT_ID>/context/injuries.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/context/roster.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/context/results.json`
- `data/odds_api/snapshots/<SNAPSHOT_ID>/context/official_injury_pdf/latest.pdf`

Global context mirrors (for fallback and reruns) are written to:
- `data/odds_api/reference/injuries/latest.json`
- `data/odds_api/reference/rosters/latest.json`
- `data/odds_api/reference/rosters/roster-YYYY-MM-DD.json`

## Playbook Workflow (Reader-Friendly Briefs)

Run end-to-end with live-window gating and budget controls:

```bash
uv run prop-ev playbook run --month 2026-02
uv run prop-ev playbook run --strategy baseline_median_novig --month 2026-02
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

Publish compact user-facing outputs to daily/latest mirrors:

```bash
uv run prop-ev playbook publish --snapshot-id <SNAPSHOT_ID>
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
- `data/odds_api/reports/latest/strategy-brief.meta.json`
- `data/odds_api/reports/latest/strategy-report.json`
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
  - if official injury source is missing, CLI hard-fails (exit `2`) by default.
  - if quote timestamps are stale, strategy mode becomes `watchlist_only`.
- Explicit override (secondary injuries only):
  - `--allow-secondary-injuries`, or
  - `PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES=true`
  - when enabled and secondary injuries are healthy, run continues in degraded mode.
- Source health command:
  - `uv run prop-ev strategy health --snapshot-id <SNAPSHOT_ID> --offline`
  - returns strict exit codes: `0 healthy`, `1 degraded`, `2 broken`.
- Tune via env vars:
  - `PROP_EV_STRATEGY_REQUIRE_OFFICIAL_INJURIES=true|false`
  - `PROP_EV_STRATEGY_ALLOW_SECONDARY_INJURIES=true|false`
  - `PROP_EV_STRATEGY_REQUIRE_FRESH_CONTEXT=true|false`
  - `PROP_EV_STRATEGY_STALE_QUOTE_MINUTES=20`
  - `PROP_EV_STRATEGY_DEFAULT_ID=v0`
  - `PROP_EV_CONTEXT_INJURIES_STALE_HOURS=6`
  - `PROP_EV_CONTEXT_ROSTER_STALE_HOURS=24`

Source policy details and fallback rules are documented in `docs/sources.md`.

## Scheduled Runs

- Docs guide (active vs archive): `docs/README.md`
- Canonical roadmap/milestones: `docs/plan.md`
- Operator runbook: `docs/runbook.md`
- Schedule-specific command examples: `docs/scheduled-flow.md`
- Artifact and gate contracts: `docs/contracts.md`
- Optional no-slate guard: `prop-ev playbook run --exit-on-no-games`
