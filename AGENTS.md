# Agent Guide (parlay-2)

This repo has strong conventions. Follow them by default.
Prefer the smallest correct change with clear tests.

## Repo overview

- Python CLI + pipeline for NBA props EV workflows.
- Core package: `src/prop_ev`.
- Tests: `tests/`.
- Config: `config/`.
- Docs: `docs/`.
- Local runtime artifacts: `data/` and `reports/` (often gitignored, unless explicitly requested).

## Quickstart (local)

```bash
uv sync --all-groups
uv run prop-ev --help
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest -q
```

## Non-negotiables

- Use `uv` for dependency and environment management.
- Keep code under `src/prop_ev` with tests in `tests/`.
- Run `uv run ruff format --check .`, `uv run ruff check .`, `uv run pyright`, and `uv run pytest -q` before opening or merging a PR.
- Keep modules small and typed; avoid hidden side effects at import time.
- Python 3.12+ features are allowed; keep runtime compatibility aligned with `pyproject.toml`.
- Max line length is 100 (Ruff).
- Never commit secrets or key material.
- Never use network calls in tests unless explicitly intended and isolated.

## Workflow

1. Understand the target behavior and impacted boundaries first (CLI, context fetch, strategy, brief output).
2. Make the smallest reviewable change that solves the problem.
3. Add or update focused tests near affected behavior.
4. Run locally:
   - `uv run ruff format .`
   - `uv run ruff check .`
   - `uv run pyright`
   - `uv run pytest -q`
5. Keep output artifacts deterministic where possible.

## Branches and commits

- Branch prefixes: `feat/`, `fix/`, `refactor/`, `test/`, `docs/`, `chore/`, `codex/`.
- Prefer Conventional Commits:
  - `feat(scope): ...`
  - `fix(scope): ...`
  - `refactor(scope): ...`
  - `test(scope): ...`
  - `docs(scope): ...`
  - `chore(scope): ...`
- Keep commits small and scoped.
- Do not amend or rebase published history unless explicitly requested.

## Testing policy

- `pytest` is the enforceable test surface.
- Tests must be deterministic and fast by default.
- Use fixtures/mocks for network, filesystem, and time when behavior depends on them.
- For high-cost or long-running behavior, gate with explicit markers and clear naming.

### Tests: placement and naming

- Place tests in `tests/` using `test_*.py` files.
- Mirror source domain in file names where practical (for example: `test_strategy.py`, `test_context_sources.py`).
- Test names should be concise, behavior-first, and `snake_case`.
- Avoid vague names like `test_basic`.
- Prefer one behavioral assertion group per test; use parametrization when it reduces duplication clearly.

## Coding style

### Typing

- Add type hints for public functions, methods, and module-level constants where useful.
- Prefer explicit return types for non-trivial functions.
- Use `TypedDict`, dataclasses, or Pydantic models when structure matters.
- Avoid `Any` unless unavoidable at boundaries; narrow types quickly after parsing.

### Error handling

- Fail with actionable errors and context.
- Do not swallow exceptions silently.
- Avoid broad `except Exception` unless re-raising with explicit context.
- CLI-facing errors should be user-readable and stable.

### Imports and module boundaries

- Keep imports explicit and sorted (Ruff handles ordering).
- Avoid circular imports via better module boundaries.
- Keep side effects out of import time (no I/O/network at module import).

### Functions and classes

- Prefer pure functions for transformation logic.
- Keep functions short and single-purpose.
- Use classes when state/behavior cohesion is real (not as a default).
- Avoid deep inheritance; prefer composition.

### Naming

- `snake_case` for variables/functions/modules.
- `PascalCase` for classes.
- `UPPER_SNAKE_CASE` for constants.
- Names should be context-first and non-redundant.
  - Good: `strategy.build_report(...)`
  - Avoid: `strategy.build_strategy_report_report(...)`
- Favor clarity over brevity; avoid unexplained abbreviations.

### Data, time, and determinism

- Normalize external payloads immediately at boundaries.
- Use UTC internally for timestamps; convert only at display edges.
- Keep JSON outputs stable where possible (consistent keys/shape).
- Do not change schema contracts without updating tests and docs.

### Logging and observability

- Emit concise, actionable logs for CLI workflows.
- Include key IDs (`snapshot_id`, paths, gate reasons) in diagnostics.
- Avoid noisy debug output in normal runs.

## CLI and output contracts

- Preserve backward-compatible CLI behavior unless explicitly changing a contract.
- When adding flags/options, document defaults and side effects.
- For machine outputs (JSON), keep fields explicit and stable.
- Exit codes should be intentional and documented.

## Dependencies

- Prefer standard library first.
- Add third-party dependencies only with clear justification.
- Keep dependency surface minimal and aligned with existing stack (`httpx`, `pydantic-settings`, `tenacity`, etc.).

## Documentation

- Update `README.md` and/or `docs/` when behavior, policy, or CLI semantics change.
- Keep examples runnable and current.
- If a source policy or gate changes, document decision rules and overrides.

## Security and secrets

- Never print or commit API keys.
- Respect `.ignore` key files (`ODDS_API_KEY.ignore`, `OPENAI_KEY.ignore`) as local-only.
- Redact secrets from logs, test fixtures, and error messages.

## Agent behavior rules

- Prefer minimal diffs with high signal.
- Preserve existing architecture unless a real defect requires structural change.
- If unexpected unrelated file changes appear, stop and ask before proceeding.
- When asked to include generated artifacts, add only what was explicitly requested.
