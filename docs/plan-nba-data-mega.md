# NBA Data Mega Plan (Executed)

## Scope

- Add a separate `nba-data` CLI with no changes to `prop-ev` behavior.
- Build resumable NBA data ingestion into `data/nba_data`.
- Build deterministic clean parquet outputs and verification checks.
- Keep unit tests offline (no real network calls).

## Phases

1. **Preflight**
   - Baseline checks (`ruff`, `pyright`, `pytest`).
   - Add initial module documentation.
2. **Module + CLI skeleton**
   - Add `src/prop_ev/nba_data`.
   - Add `nba-data` console script.
3. **Storage core**
   - Canonical layout + pbpstats cache directories.
   - Lockfile + atomic writes.
   - Resumable JSONL manifest per season/season_type.
4. **Discover + ingest**
   - Discover final games.
   - Ingest resources (`boxscore`, `enhanced_pbp`, `possessions`) with file-first loading.
   - Provider fallback (`data_nba` -> `stats_nba`) and rate limiting.
5. **Clean + verify**
   - Build `schema_v1` parquet datasets.
   - Verify integrity/sanity and write JSON reports.
6. **Docs + tests**
   - Add `docs/nba-data.md` and `docs/nba-data-schemas.md`.
   - Update `README.md` and `docs/sources.md`.
   - Add focused offline tests for store, discover, ingest, clean/verify.

## Verification commands

```bash
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest -q
```

`make ci` is not available in this branch, so explicit checks are the source of truth.
