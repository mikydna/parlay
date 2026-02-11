# Deep Clean Megaprompt Plan

This is a Codex-ready sequence to deeply clean the repo without changing product intent.

## Objectives

1. Reduce complexity and duplicate logic.
2. Standardize data contracts and artifact paths.
3. Tighten reliability gates and error handling.
4. Improve test quality and speed.
5. Keep odds usage at zero during cleanup (`--offline` / `--block-paid`).

## Ground Rules

1. No new betting/model features.
2. No paid Odds API calls while cleaning.
3. Preserve all existing CLI interfaces unless explicitly deprecated.
4. Every refactor must include tests.
5. Commit in small, reviewable blocks.

## Prompt A: Repo Audit + Cleanup Backlog

You are doing a deep cleanup audit only. No feature work.

Tasks:
1. Scan `src/prop_ev`, `tests`, and `docs` for dead code, duplicate helpers, inconsistent naming, and oversized modules.
2. Produce `docs/cleanup-backlog.md` with:
   - P0: correctness/risk issues
   - P1: maintainability issues
   - P2: style/docs issues
3. For each item include: file path, issue, proposed fix, and test impact.
4. Add a dependency map for main modules (`cli.py`, `playbook.py`, `strategy.py`, `brief_builder.py`, `latex_renderer.py`).
5. Do not edit production code in this block.

Commit:
- `docs: add deep cleanup backlog`

## Prompt B: Structure Refactor (No Behavior Change)

Implement structural cleanup from backlog P0/P1 only, preserving behavior.

Tasks:
1. Split large modules where needed:
   - Move reusable formatting/parsing helpers into focused utility modules.
   - Keep CLI orchestration thin.
2. Remove unreachable or duplicate code.
3. Normalize naming conventions for internal helpers.
4. Add/adjust unit tests proving no behavior change.
5. Run:
   - `uv run ruff check src tests`
   - `uv run pytest -q`

Commit:
- `refactor: reduce module complexity and duplicate helpers`

## Prompt C: Data Contract Hardening

Harden snapshot/report contracts and failure behavior.

Tasks:
1. Add strict typed validators for key artifacts:
   - `strategy-report.json`
   - `brief-input.json`
   - `brief-pass1.json`
   - `backtest-readiness.json`
2. Validate required keys before render/export.
3. Add explicit downgrade modes (never silent partial success).
4. Standardize health-gate reason codes and document them in `docs/contracts.md`.
5. Add tests for malformed/missing artifacts and expected failure messages.

Commit:
- `chore: harden report and readiness contracts`

## Prompt D: CLI UX and Safety Cleanup

Improve CLI clarity and operational safety.

Tasks:
1. Standardize CLI output lines to stable `key=value` format for automation parsing.
2. Ensure every command prints deterministic artifact paths.
3. Add `--dry-run` support where missing in non-network commands.
4. Improve error messages for:
   - missing snapshot
   - missing report files
   - offline cache miss
5. Add tests for CLI failure modes and exit codes.

Commit:
- `chore: normalize cli output and safety errors`

## Prompt E: Tests, Fixtures, and Performance

Deep clean the test suite.

Tasks:
1. Remove redundant tests and merge overlapping scenarios.
2. Add fixtures/builders for strategy report and snapshot directories.
3. Ensure no test does paid network calls.
4. Add a fast/slow marker strategy and document local commands.
5. Improve coverage around:
   - playbook rendering decisions
   - backtest prep generation
   - health gate transitions

Commit:
- `test: consolidate fixtures and strengthen edge-case coverage`

## Prompt F: Docs and Runbook Consolidation

Clean and unify docs for operators.

Tasks:
1. Merge overlapping docs into:
   - `docs/runbook.md`
   - `docs/contracts.md`
   - `docs/backtest-prep.md`
2. Add a single "daily operator flow" section with absolute run order.
3. Add a "dev mode vs live mode" table.
4. Add a troubleshooting matrix for common failures.
5. Remove stale docs and update cross-links.

Commit:
- `docs: consolidate runbook and contracts`

## Prompt G: Final Verification and Release PR Summary

Perform final cleanup verification and prepare release notes.

Tasks:
1. Run:
   - `uv run ruff check src tests`
   - `uv run pytest -q`
   - `uv run pyright src`
2. Create `docs/deep-clean-summary.md` with:
   - what changed
   - behavior preserved vs behavior tightened
   - residual risks
   - next technical debt items
3. Provide a concise PR summary and rollback notes.

Commit:
- `chore: finalize deep-clean validation and summary`

## Suggested Execution Order

1. Prompt A
2. Prompt B
3. Prompt C
4. Prompt D
5. Prompt E
6. Prompt F
7. Prompt G

## Optional Guardrail for All Blocks

Use this run prefix during cleanup:

```bash
PROP_EV_STRATEGY_STALE_QUOTE_MINUTES=10000 uv run ...
```

This avoids accidental watchlist-only downgrades when replaying old snapshots in dev.
