# P0 Issue: Lake Boundary + Report Separation

Status: Open  
Priority: P0  
Owner: Data platform / pipeline

## Problem

`parlay-data` currently mixes concerns that should be isolated:

1. **Cross-lake leakage**
   - `odds_api/snapshots/*/context/*` contains NBA-derived artifacts (injuries/roster/pdf context).
   - `odds_api/reference/player_identity_map.json` is NBA-centric reference data.
2. **Report material inside lake snapshots**
   - `odds_api/snapshots/*/reports/*` stores strategy/playbook/settlement outputs inside the same namespace as canonical lake artifacts.
3. **Runtime caches co-located with versioned data**
   - LLM and NBA runtime caches are physically under `odds_api/*` (currently gitignored, but same tree).

This increases coupling, makes contracts ambiguous, and complicates restore/backfill behavior.

## Required Contract (target state)

### 1) Strict lake ownership

- `lakes/nba/**` owns NBA raw/clean/manifests/archives only.
- `lakes/odds/**` owns odds snapshots/datasets/bundles/usage only.
- No NBA payload blobs may exist under odds lake paths.

### 2) Reports are not lake artifacts

- No `reports/` directories under any `lakes/**/snapshots/*`.
- Reports move to a separate reporting namespace:
  - `reports/odds/by-snapshot/<snapshot_id>/...`
  - `reports/odds/daily/<date>/snapshot=<snapshot_id>/...`
  - `reports/odds/latest/...`
  - `reports/nba/verify/...` (or equivalent non-lake reporting root)

### 3) Runtime/cache isolation

- Runtime-only artifacts live under a dedicated non-versioned runtime root (for example `runtime/**`).
- `llm_cache`, `llm_usage`, `nba_cache`, temporary injury/roster cache files are not stored in lake roots.

## Canonical Layout (proposed)

```text
parlay-data/
  lakes/
    nba/
      raw/
      clean/schema_v*/
      manifests/
      raw_archives/
    odds/
      snapshots/<snapshot_id>/{manifest.json,requests/,responses/,meta/,derived/}
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
   - Move NBA context artifacts from odds snapshots to NBA-owned paths.
   - Replace in-odds copies with lightweight references (`context_ref.json` + hash/path).
2. **Report relocation**
   - Move snapshot-embedded report files into `reports/odds/by-snapshot/<snapshot_id>/`.
   - Keep only lake-safe artifacts inside snapshot directories.
3. **Writer changes**
   - Update `prop-ev` writers so new runs never write `snapshots/*/reports/*`.
   - Update read paths for strategy/playbook/settlement/report publish commands.
4. **Guardrails**
   - Add automated checks that fail if:
     - `lakes/**/snapshots/*/reports/*` is present.
     - NBA data appears under odds lake roots.
5. **Back-compat**
   - Temporary migration shim allowed for reading legacy paths, but writes must target new layout only.

## Acceptance Criteria

- [ ] Fresh runs produce no `snapshots/*/reports/*` directories.
- [ ] No NBA context blobs remain under odds lake roots.
- [ ] Report publish/read commands resolve against `reports/odds/**`.
- [ ] Guardrail checks are enforced in local validation.
- [ ] Existing backtest/settlement/playbook workflows pass on migrated data.

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
