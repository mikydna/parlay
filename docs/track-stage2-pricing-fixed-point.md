# Track A: Pricing Fixed-Point (Stage 2)

Date: 2026-02-14
Owner branch: `codex/track-a-pricing-fixed-point`

## 1) Objective

Ship a deterministic pricing-quality layer that improves decision quality without changing data
acquisition behavior or spending additional odds credits.

This track executes fully offline against existing snapshots and backtest artifacts.

## 2) Outcome Hypothesis

Primary hypothesis:
- Adding conservative pricing metrics (`p_low`, `ev_low`, quality sub-scores) will reduce fragile
  tickets and improve calibration stability relative to `s007`.

Secondary hypothesis:
- A conservative strategy variant (`s008`) using the new metrics will produce a higher-quality
  action set on the same snapshots, even if total picks decrease.

## 3) What this yields

This track yields both:
1. **Base metrics contract** used by all strategies and evaluation tooling.
2. **New optional strategy plugin (`s008`)** that consumes those metrics.

Default strategy behavior remains unchanged unless operators explicitly select `--strategy s008`.

## 4) In Scope

- Extend strategy candidate rows with pricing-quality and uncertainty fields.
- Extend audit/summary metrics for actionability and conservative EV context.
- Extend backtest seed/template and backtest summary metrics.
- Add `s008` strategy plugin with conservative gates.
- Update docs/contracts and tests.

## 5) Out of Scope

- Odds API pull changes, spend policy changes, or day-index changes.
- LLM brief prompt rewrites.
- Portfolio optimizer / multi-ticket correlation logic.
- Any default strategy flip to `s008`.

## 6) Contract Additions (target)

## 6.1 Candidate-level fields (strategy report)

For each candidate row:
- `quote_age_minutes`
- `depth_score`
- `hold_score`
- `dispersion_score`
- `freshness_score`
- `quality_score`
- `uncertainty_band`
- `p_hit_low`
- `p_hit_high`
- `ev_low`
- `ev_high`

Scoring intent:
- Scores are normalized to `[0, 1]` (higher is better).
- `p_hit_low/high` and `ev_low/high` are deterministic transforms of current model probability,
  per-line dispersion, depth, and freshness.

## 6.2 Summary-level fields (strategy report)

Add strategy-level observability fields:
- `actionability_rate` (`eligible_lines / candidate_lines`)
- `avg_quality_score_all`
- `avg_quality_score_eligible`
- `avg_ev_low_eligible`

## 6.3 Backtest artifacts

`backtest-seed.jsonl` and `backtest-results-template*.csv` include:
- `quality_score`, `depth_score`, `hold_score`, `dispersion_score`, `freshness_score`
- `p_hit_low`, `p_hit_high`, `ev_low`, `ev_high`

## 6.4 Backtest summary metrics

Per strategy summary adds:
- `avg_ev_low`
- `avg_quality_score`
- `avg_p_hit_low`
- `brier_low`
- `actionability_rate`

## 7) `s008` Strategy Definition

Strategy ID: `s008`
Name: `Conservative Quality Floor`

Recipe intent:
- Build on `s007` baseline configuration.
- Add stricter uncertainty-quality eligibility filter:
  - minimum quality score
  - minimum conservative EV (`ev_low`) threshold
  - optional max uncertainty band

Initial defaults (version 1):
- `min_book_pairs = 2`
- `hold_cap = 0.08`
- `min_quality_score = 0.55`
- `min_ev_low = 0.01`
- `max_uncertainty_band = 0.08`

All thresholds remain deterministic and encoded in report audit payload.

## 8) Implementation Map

Core files:
- `src/prop_ev/strategy.py`
- `src/prop_ev/strategies/base.py`
- `src/prop_ev/strategies/registry.py`
- `src/prop_ev/strategies/s008_conservative_quality_floor.py` (new)
- `src/prop_ev/state_keys.py`
- `src/prop_ev/backtest.py`
- `src/prop_ev/backtest_summary.py`
- `src/prop_ev/cli.py` (backtest summary markdown table)

Docs:
- `docs/contracts.md`
- `docs/plan.md` (stage and track status notes)

Tests (add/update):
- `tests/test_strategy_plugins.py`
- `tests/test_strategy_market_baseline.py`
- `tests/test_backtest.py`
- `tests/test_backtest_summary.py`
- `tests/test_strategy_aliases.py`

## 9) Validation Protocol

Run locally before final commit:

```bash
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest -q
```

Focused checks during development:

```bash
uv run pytest -q tests/test_strategy_plugins.py tests/test_strategy_market_baseline.py
uv run pytest -q tests/test_backtest.py tests/test_backtest_summary.py
```

## 10) Promotion Decision Rules

Do **not** flip defaults in this track.

`s008` consideration criteria (on fixed snapshot window):
- `rows_graded` at/above configured sample floor.
- `brier_low` is not worse than baseline tolerance.
- `avg_ev_low` and ROI show non-negative improvement.
- actionability decline (if any) is justified by better calibration/stability.

## 11) Rollback

Rollback is low risk:
- Continue using existing strategies (`s001`-`s007`).
- Ignore new fields in downstream consumers.
- Remove `s008` from strategy registry if needed.

No data migration or odds re-download is required for rollback.

## 12) Current implementation notes

- `median_book` baseline now supports deterministic nearby-line fallback when exact per-book pairs are missing.
- Candidate rows now include provenance for reference probability selection:
  - `reference_line_method`
  - `reference_points_count`
