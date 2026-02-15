# nba-prop-ev

Cache-first NBA odds snapshot pipeline for The Odds API v4.

## Install

```bash
uv sync --all-groups
```

## Runtime Config

All runtime settings now come from `config/runtime.toml` (hard cutover, no env configuration).

- Canonical paths (`odds_data_dir`, `nba_data_dir`, `reports_dir`, `runtime_dir`) live under
  `[paths]`.
- API key discovery uses configured key files in:
  - `[odds_api].key_files` (for Odds API keys)
  - `[openai].key_files` (for OpenAI keys)
- Per-run overrides are CLI-only:
  - `--config`
  - `--data-dir`
  - `--nba-data-dir`
  - `--reports-dir`
  - `--runtime-dir`

Bookmaker whitelist defaults are in `config/bookmakers.json` (currently DraftKings + FanDuel).
When `--bookmakers` is omitted, snapshot/playbook commands use this whitelist automatically.

## Code Layout

- CLI entrypoint wrappers:
  - `src/prop_ev/cli.py`
  - `src/prop_ev/cli_commands.py`
- CLI command modules:
  - `src/prop_ev/cli_shared.py`
  - `src/prop_ev/cli_snapshot_impl.py`
  - `src/prop_ev/cli_data_impl.py`
  - `src/prop_ev/cli_strategy_impl.py`
  - `src/prop_ev/cli_playbook_impl.py`
- CLI support modules:
  - `src/prop_ev/cli_parser.py`
  - `src/prop_ev/cli_config.py`
  - `src/prop_ev/cli_global_overrides.py`
  - `src/prop_ev/cli_data_helpers.py`
  - `src/prop_ev/cli_ablation_helpers.py`
  - `src/prop_ev/cli_markdown.py`
- Strategy engine wrappers:
  - `src/prop_ev/strategy.py`
  - `src/prop_ev/strategy_core.py`
- Strategy implementation modules:
  - `src/prop_ev/strategy_report_impl.py`
  - `src/prop_ev/strategy_pricing_impl.py`
  - `src/prop_ev/strategy_gating_impl.py`
  - `src/prop_ev/strategy_ranking_impl.py`
- Strategy helper modules:
  - `src/prop_ev/strategy_output_impl.py`
  - `src/prop_ev/strategy_context_impl.py`
  - `src/prop_ev/strategy_minutes_impl.py`

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

Artifacts are written under configured `paths.nba_data_dir`.
Ingest is resume-safe and skips already valid raw mirrors.

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
uv run prop-ev snapshot unpack --bundle <odds_data_dir>/bundles/snapshots/<SNAPSHOT_ID>.tar.zst
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
uv run prop-ev data verify --dataset-id <DATASET_ID> --from 2026-02-01 --to 2026-02-12 --require-complete --require-parquet --json
uv run prop-ev data verify --dataset-id <DATASET_ID> --from 2026-02-01 --to 2026-02-12 --require-complete --require-parquet --require-canonical-jsonl --json
uv run prop-ev data repair-derived --dataset-id <DATASET_ID> --from 2026-02-01 --to 2026-02-12 --json
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
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/strategy-report.json`
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/backtest-seed.jsonl`
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/backtest-results-template.csv`
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/backtest-readiness.json`

Per-strategy runs also write suffixed artifacts:
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/strategy-report.<STRATEGY_ID>.json`
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/strategy-report.<STRATEGY_ID>.md`
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/backtest-results-template.<STRATEGY_ID>.csv`

Rebuild backtest artifacts for any snapshot:

```bash
uv run prop-ev strategy backtest-prep --snapshot-id <SNAPSHOT_ID> --selection eligible
uv run prop-ev strategy settle --snapshot-id <SNAPSHOT_ID> --results-source auto --refresh-results
uv run prop-ev strategy backtest-summarize --snapshot-id <SNAPSHOT_ID> --strategies v0,baseline_median_novig
```

Strategy context caches are written to:
- `<nba_data_dir>/context/snapshots/<SNAPSHOT_ID>/injuries.json`
- `<nba_data_dir>/context/snapshots/<SNAPSHOT_ID>/roster.json`
- `<nba_data_dir>/context/snapshots/<SNAPSHOT_ID>/results.json`
- `<nba_data_dir>/context/snapshots/<SNAPSHOT_ID>/official_injury_pdf/latest.pdf`
- `<odds_data_dir>/snapshots/<SNAPSHOT_ID>/context_ref.json` (lightweight pointer only)

Global context mirrors (for fallback and reruns) are written to:
- `<nba_data_dir>/reference/injuries/latest.json`
- `<nba_data_dir>/reference/rosters/latest.json`
- `<nba_data_dir>/reference/rosters/roster-YYYY-MM-DD.json`

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

Default live snapshot ids are now ET-friendly (example: `2026-02-13T18-05-42-ET`).

Render a specific snapshot into PDF + LaTeX artifacts (markdown is opt-in):

```bash
uv run prop-ev playbook render --snapshot-id <SNAPSHOT_ID> --offline
```

Add markdown artifact only when needed:

```bash
uv run prop-ev playbook render --snapshot-id <SNAPSHOT_ID> --offline --write-markdown
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
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/brief-input.json`
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/brief-pass1.json`
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/strategy-brief.md` (only with `--write-markdown`)
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/strategy-brief.tex`
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/strategy-brief.pdf` (if `tectonic` exists)
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/strategy-brief.meta.json`

Discovery vs execution report output:
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/discovery-execution.json`
- `<REPORTS_DIR>/by-snapshot/<REPORT_SNAPSHOT>/discovery-execution.md`

Strategy report health now includes:
- feed status contract (`official_injuries`, `secondary_injuries`, `roster`)
- odds freshness contract (`latest_quote_utc`, age, stale threshold)
- event mapping contract (missing event ids)
- automatic mode downgrade to `watchlist_only` when required data is missing/stale.

Latest mirrors:
- `<REPORTS_DIR>/latest/strategy-brief.meta.json`
- `<REPORTS_DIR>/latest/strategy-report.json`
- `<REPORTS_DIR>/latest/strategy-brief.pdf` (if generated)
- `<REPORTS_DIR>/latest/latest.json`

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
  - `strategy.allow_secondary_injuries=true` in `config/runtime.toml`
  - when enabled and secondary injuries are healthy, run continues in degraded mode.
- Source health command:
  - `uv run prop-ev strategy health --snapshot-id <SNAPSHOT_ID> --offline`
  - returns strict exit codes: `0 healthy`, `1 degraded`, `2 broken`.
- Tune via `config/runtime.toml`:
  - `[strategy].require_official_injuries`
  - `[strategy].allow_secondary_injuries`
  - `[strategy].require_fresh_context`
  - `[strategy].stale_quote_minutes`
  - `[strategy].default_id`
  - `[strategy].context_injuries_stale_hours`
  - `[strategy].context_roster_stale_hours`

Source policy details and fallback rules are documented in `docs/sources.md`.

## Scheduled Runs

- Docs guide (active vs archive): `docs/README.md`
- Canonical roadmap/milestones: `docs/plan.md`
- Operator runbook: `docs/runbook.md`
- Schedule-specific command examples: `docs/scheduled-flow.md`
- Artifact and gate contracts: `docs/contracts.md`
- Optional no-slate guard: `prop-ev playbook run --exit-on-no-games`
