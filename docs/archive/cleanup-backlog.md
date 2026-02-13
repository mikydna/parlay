# Cleanup Backlog (2026-02-13)

This backlog is based on current branch baseline checks and code scan.

## P0 Correctness / Risk

### 1) Duplicated context health logic can drift (resolved 2026-02-13)

- **File paths**: `src/prop_ev/cli.py`, `src/prop_ev/strategy.py`
- **Symptom**: Official/secondary injury readiness checks are implemented in multiple places.
- **Suggested fix**: Implemented: centralized pure helpers in `src/prop_ev/context_health.py` with thin wrappers.
- **Test impact**: Added parity coverage in `tests/test_context_health.py` and existing playbook/health tests.

### 2) Monkeypatch-sensitive internals in large CLI module

- **File paths**: `src/prop_ev/cli.py`, `tests/test_playbook_run_live_vs_offline.py`, `tests/test_cli_snapshot_tools.py`
- **Symptom**: Many tests patch `prop_ev.cli` internals; large refactors can silently break tests/contracts.
- **Suggested fix**: Add characterization tests for key `main([...])` exit codes/output lines before medium/high-risk refactors.
- **Test impact**: New characterization tests required before CLI internal extraction.

## P1 Maintainability

### 1) Oversized modules

- **File paths**: `src/prop_ev/cli.py` (~2905), `src/prop_ev/strategy.py` (~2638), `src/prop_ev/brief_builder.py` (~1979)
- **Symptom**: Large files increase coupling and review risk.
- **Suggested fix**: Incremental internal extraction with stable public surfaces.
- **Test impact**: Focused parity/characterization tests before and after extraction.

### 2) Duplicated time helper functions (mostly resolved 2026-02-13)

- **File paths**: `src/prop_ev/strategy.py`, `src/prop_ev/playbook.py`, `src/prop_ev/discovery_execution.py`, `src/prop_ev/context_sources.py`, `src/prop_ev/storage.py`, `src/prop_ev/llm_client.py`, `src/prop_ev/backtest.py`, `src/prop_ev/identity_map.py`
- **Symptom**: Repeated UTC now/ISO parse/ISO format implementations.
- **Suggested fix**: Implemented for primary call sites via `src/prop_ev/time_utils.py`; wrappers retained for stability.
- **Test impact**: Added unit tests in `tests/test_time_utils.py`.

### 3) Duplicated odds math helpers (mostly resolved 2026-02-13)

- **File paths**: `src/prop_ev/strategy.py`, `src/prop_ev/discovery_execution.py`
- **Symptom**: Repeated American/decimal/implied-prob/EV math logic.
- **Suggested fix**: Implemented with `src/prop_ev/odds_math.py` and wrapper-compatible migrations.
- **Test impact**: Added pure math coverage in `tests/test_odds_math.py`.

### 4) Repeated JSON utility wrappers

- **File paths**: `src/prop_ev/playbook.py`, `src/prop_ev/context_sources.py`, `src/prop_ev/backtest.py`, `src/prop_ev/settlement.py`, `src/prop_ev/budget.py`, `src/prop_ev/storage.py`, `src/prop_ev/cli.py`
- **Symptom**: Multiple local JSON load/write helpers with slightly different behavior.
- **Suggested fix**: Defer to a later low-risk utility pass after time/odds extraction.
- **Test impact**: Characterize sort/order/newline behavior before consolidation.

## P2 Style / Docs

### 1) Deep-clean/operator docs overlap

- **File paths**: `docs/plan-deep-clean.md`, `docs/plan-strategy-plugins.md`, `docs/scheduled-flow.md`, `README.md`
- **Symptom**: Operational guidance is spread across multiple plan docs.
- **Suggested fix**: Consolidate stable operator contracts in `docs/runbook.md` + `docs/contracts.md` (separate doc-only pass).
- **Test impact**: None (doc-only).

## Duplication Scan Checklist (Current Hits)

- **Time helpers**: `_utc_now`, `_now_utc`, `now_utc`, `_iso`, `_parse_iso_utc` in multiple modules.
- **Odds math**: `_american_to_decimal`, `_implied_prob_from_american`, `_decimal_to_american`, `_normalize_prob_pair`, `_ev_from_prob_and_price`.
- **Env parsing**: `_env_bool`, `_env_int`, `_env_float` concentrated in CLI.
- **JSON wrappers**: `_load_json`, `_write_json`, `_load_jsonl`, `_write_jsonl` in several modules.
- **Context health**: `_official_rows_count`, `_official_source_ready`, `_secondary_source_ready` split between CLI/strategy.

## Recommended Next Safe PR

1. Consolidate repeated JSON read/write wrappers with explicit newline/sort-key contract tests.
2. Add more CLI characterization tests before any larger command-level extraction.
3. Centralize gate/reason-code mapping for health/reporting paths.
4. Rerun full baseline checks.
