# Deep Clean Summary (2026-02-13)

## What Changed

- Added baseline + backlog artifacts:
  - `docs/cleanup-run-notes.md`
  - `docs/cleanup-backlog.md`
- Extracted shared time helpers:
  - `src/prop_ev/time_utils.py`
  - migrated wrappers/callers in CLI, strategy, playbook, context, storage, backtest, identity, LLM.
- Extracted shared odds math helpers:
  - `src/prop_ev/odds_math.py`
  - migrated wrappers in strategy/discovery-execution.
- Centralized context source readiness logic:
  - `src/prop_ev/context_health.py`
  - wrapper-compatible usage in `src/prop_ev/cli.py` and `src/prop_ev/strategy.py`.
- Extracted v0 minutes/usage model logic:
  - `src/prop_ev/models/v0_minutes_usage.py`
  - wrapper-compatible strategy integration.
- Added low-risk CLI internal delegation:
  - `src/prop_ev/cli_internal.py`
  - delegated default-window/env parsing/team-scope helpers while keeping `prop_ev.cli` symbols stable.
- Added characterization and helper tests:
  - `tests/test_cli_characterization.py`
  - `tests/test_time_utils.py`
  - `tests/test_odds_math.py`
  - `tests/test_context_health.py`
  - `tests/test_v0_minutes_usage.py`
- Added operator and contract docs:
  - `docs/runbook.md`
  - `docs/contracts.md`

## Behavior Preserved vs Tightened

Preserved:
- CLI command names/flags and existing key output lines.
- Playbook live/offline gating defaults and secondary injury override semantics.
- Strategy tiering and EV floor behavior.
- Existing monkeypatch-sensitive targets in `prop_ev.cli` and `prop_ev.strategy`.

Tightened:
- No behavior-tightening changes were applied in this pass.

## Verification

Executed and passing:
- `uv run ruff format --check .`
- `uv run ruff check .`
- `uv run pyright`
- `uv run pytest -q`

## Remaining Tech Debt / Next Steps

1. Add explicit artifact validators behind opt-in flags (contract-tightening phase).
2. Continue CLI shrink with additional characterization tests before larger moves.
3. Consolidate repeated JSON read/write wrappers with newline/sorting parity tests.
4. Evaluate de-duplicating stale/gate reason mapping into one shared policy module.
