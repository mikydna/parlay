# P0 Issue: Data Boundary + Report Separation

Status: Closed (2026-02-14)  
Priority: P0  
Owner: Data platform / pipeline

## Problem

`parlay-data` still mixes concerns that should be isolated:

1. **Cross-domain leakage**
   - `odds_api/snapshots/*/context/*` may still hold NBA-derived artifacts.
   - `odds_api/reference/player_identity_map.json` is NBA-owned reference data.
2. **Report material inside odds snapshots**
   - `odds_api/snapshots/*/reports/*` is still possible in legacy snapshots.
3. **Runtime caches co-located with versioned odds data**
   - LLM and NBA runtime caches can appear under `odds_api/*`.

This increases coupling, makes contracts ambiguous, and complicates restore/backfill behavior.

## Required Contract (target state)

### 1) Strict ownership with current roots (no `lakes/` churn)

- `nba_data/**` owns NBA raw/clean/manifests/archives/context/reference only.
- `odds_api/**` owns odds snapshots/datasets/bundles/usage only.
- No NBA payload blobs may exist under `odds_api/**`.

### 2) Reports are not odds snapshot artifacts

- No `reports/` directories under `odds_api/snapshots/*`.
- Reports must live under a separate reporting namespace:
  - `reports/odds/by-snapshot/<snapshot_id>/...`
  - `reports/odds/daily/<date>/snapshot=<snapshot_id>/...`
  - `reports/odds/latest/...`
  - `reports/nba/verify/...`

### 3) Runtime/cache isolation

- Runtime-only artifacts live under `runtime/**` (non-versioned).
- `llm_cache`, `llm_usage`, `nba_cache`, and temporary cache blobs do not live under `odds_api/**`.

## Canonical Layout (no new top-level rename)

```text
parlay-data/
  nba_data/
    raw/
    clean/schema_v*/
    manifests/
    raw_archives/
    context/
    reference/
  odds_api/
    snapshots/<snapshot_id>/{manifest.json,requests/,responses/,meta/,derived/,context_ref.json}
    datasets/<dataset_id>/{spec.json,days/*.json}
    bundles/snapshots/*.tar.zst
    usage/usage-YYYY-MM.jsonl
  reports/
    nba/verify/...
    odds/
      by-snapshot/<snapshot_id>/
      daily/<date>/snapshot=<snapshot_id>/
      latest/
  runtime/            # gitignored
    llm_cache/
    llm_usage/
    nba_cache/
```

## Migration Requirements

1. **Data move**
   - Move NBA context artifacts from `odds_api/snapshots/*/context/*` to NBA-owned paths.
   - Keep lightweight references in odds snapshots (`context_ref.json` + hash/path).
2. **Report relocation**
   - Move snapshot-embedded report files into `reports/odds/by-snapshot/<snapshot_id>/`.
   - Keep only odds-lake artifacts inside `odds_api/snapshots/*`.
3. **Writer changes**
   - Ensure new runs never write `odds_api/snapshots/*/reports/*`.
   - Ensure read paths for strategy/playbook/settlement/report publish use `reports/odds/**`.
4. **Guardrails**
   - Add automated checks that fail if:
     - `odds_api/snapshots/*/reports/*` is present.
     - NBA-owned blobs appear under `odds_api/**`.
     - runtime/cache dirs are present under `odds_api/**`.
5. **Back-compat**
   - Read shims can be temporary; all new writes target canonical paths only.

## Acceptance Criteria

- [x] Fresh runs produce no `odds_api/snapshots/*/reports/*` directories.
- [x] No NBA context/reference blobs remain under `odds_api/**`.
- [x] Report publish/read commands resolve against `reports/odds/**`.
- [x] Guardrail checks are enforced in local validation.
- [x] Existing backtest/settlement/playbook workflows pass on migrated data.

## Closure Evidence (2026-02-14)

- Guardrail pass:
  - `uv run prop-ev --data-dir /Users/andy/Documents/Code/parlay-data/odds_api data guardrails --json`
  - result: `status=ok`, `violation_count=0`.
- Filesystem audit:
  - no `odds_api/snapshots/*/reports/` directories,
  - no NBA-owned context/reference blobs under `odds_api/**`.
- Workflow checks:
  - replay strategy run writes reports under `reports/odds/by-snapshot/<snapshot_id>/...`,
  - complete-day scoreboard path is `reports/odds/analysis/<run_id>/aggregate-scoreboard.json`.

## Non-Goals

- Strategy model changes.
- Odds market/schema redesign.
- Rewriting historical payload semantics.

## Execution Order

1. Contract + path constants update.
2. Writer migration (new writes only).
3. Data migrator for existing `parlay-data`.
4. Guardrails + local enforcement.
5. Remove legacy read shim after stable window.
