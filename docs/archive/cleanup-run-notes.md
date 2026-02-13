# Cleanup Run Notes (2026-02-13)

## Baseline Commands

- `uv sync --all-groups` -> pass
- `uv run ruff format --check .` -> pass (`52 files already formatted`)
- `uv run ruff check .` -> pass
- `uv run pyright` -> pass (`0 errors, 0 warnings`)
- `uv run pytest -q` -> pass (`96 passed`)

## Failures Observed

- None in this run.

## Runtime-Network Leakage Observed

- None in tests during this run.

## Next Smallest Safe Diff

- Extract duplicated context health helpers into `src/prop_ev/context_health.py`.
- Keep compatibility wrappers in `prop_ev.cli` and `prop_ev.strategy` to preserve monkeypatch targets.

## Post-Change Verification (This Run)

- `uv run ruff format --check .` -> pass
- `uv run ruff check .` -> pass
- `uv run pyright` -> pass
- `uv run pytest -q` -> pass
