# Plan: Strategy Plugins + Multi-Book Execution (NBA Props)

## Goal

Build a strategy plugin system so we can A/B test ideas (market baseline, consensus gates,
execution realism) on the same snapshots, backtest outcomes consistently, and converge on a
single default strategy that produces the daily "best suggested bets" on FD/DK.

North-star:
- A single CLI run produces:
  - a chosen strategy id (default) + an auditable ranked ticket list
  - an execution-time FD/DK actionable list (or explicit NO-BET)
  - per-strategy backtest templates and a scoreboard

## Current State (What Exists Today)

- Strategy report is deterministic and already uses:
  - implied probabilities from American odds
  - a no-vig normalized baseline (paired Over/Under when both are present)
  - a hold/overround metric (currently computed from the best Over + best Under found)
  - reliability gates that can downgrade to watchlist-only mode
- The pipeline supports "discovery vs execution":
  - discovery snapshot: many books for signal
  - execution snapshot: FD/DK for actionability
  - report shows whether execution books still meet the discovery PLAY-TO

## Definitions

- Snapshot:
  - A cached Odds API request/response set plus derived JSONL tables.
- Strategy plugin:
  - A named, deterministic transformation from snapshot-derived rows + context -> strategy-report JSON.
  - Each plugin changes one idea (atomic delta) so attribution is clear.
- Discovery snapshot:
  - Regions/all-books view for consensus and baseline estimation.
- Execution snapshot:
  - Restricted to FD/DK; used to decide what is actually bettable.
- Ranked top-N:
  - The list of tickets we treat as the "bet list" for backtesting.
- Actionable:
  - A ranked ticket where FD/DK still offers a matching market/point and meets the PLAY-TO threshold.

## Milestones (Recommended Sequence)

### M0: Freeze Contracts and Keys (Foundation)

Goal: comparisons must be apples-to-apples.

Deliverables:
- Define a stable ticket identity key used everywhere:
  - (event_id, player, market, point, side)
- Ensure every report carries:
  - strategy_id
  - audit config knobs
- Ensure every backtest row carries:
  - strategy_id
  - the exact execution price used for grading (graded_price_american)

Acceptance:
- `uv run ruff check .`, `uv run pyright`, and `uv run pytest -q` pass
- The same snapshot rerun produces identical reports.

### M1: Strategy Plugin Framework + CLI Harness

Goal: run multiple strategies on the same snapshot and write artifacts side-by-side.

Deliverables:
- Strategy registry with ids like: s001, s003, s006, ...
- CLI:
  - `prop-ev strategy ls`
  - `prop-ev strategy run --strategy <id>`
  - `prop-ev strategy compare --strategies <csv>`
- Report outputs:
  - canonical `strategy-report.json` for s001
  - suffixed outputs for all strategies:
    - `strategy-report.<id>.json`
    - `strategy-report.<id>.md`
    - `backtest-seed.<id>.jsonl`
    - `backtest-results-template.<id>.csv`
    - `backtest-readiness.<id>.json`

Acceptance:
- `strategy compare` runs offline on a fixture snapshot and produces all suffixed outputs.

### M2: Backtest Summarizer + Probability Quality Metrics

Goal: measurable progress, not "ROI vibes".

Deliverables:
- `prop-ev strategy backtest-summarize ...`:
  - reads filled per-strategy CSVs
  - prints a scoreboard by strategy_id
  - writes JSON/MD summaries

Metrics (minimum):
- ROI (units) with default stake_units=1 if blank
- win/loss/push counts
- avg best_ev (where available)
- Brier score on win/loss rows (exclude pushes)
- calibration buckets (0.05 or 0.10 bins): predicted p vs empirical hit rate

Acceptance:
- Unit tests for ROI and Brier math
- Summary output is deterministic for a given CSV.

### M3: Consensus + Stability Strategy Plugins (Atomic Deltas)

Goal: reduce fake edges caused by stale inputs and outlier lines.

Initial plugins to implement (recommended):
1) `s003` (Median No-Vig Baseline)
- Change only: compute market baseline as:
  - for each book with both sides: no-vig p_over_book, hold_book
  - aggregate baseline p_over_fair = median(p_over_book)
  - aggregate hold = median(hold_book)
- Rationale: makes hold interpretable and avoids "best-over + best-under" artifacts.

2) `s004` (Min-2 Book-Pair Gate)
- Change only: require at least 2 books that have both over+under for that exact line.
- Rationale: avoid false Tier A depth when only one side is populated.

3) `s005` (Hold-Cap Gate)
- Change only: demote/skip when median hold exceeds a threshold.
- Rationale: high overround implies worse price quality and more fragility.

4) `s006` (Dispersion-IQR Gate)
- Change only: demote/skip when dispersion of p_over_book is high (IQR or max-min).
- Rationale: large disagreement signals alt-line confusion, stale feed, or news.

Acceptance:
- Each plugin has:
  - explicit, unit-testable thresholds
  - an audit field explaining any demotion/skip.

### M4: Execution-Time FD/DK Best-of + Outlier Detection

Goal: your actual bets are FD/DK and must be validated at decision time.

Deliverables:
- Extend/standardize the execution workflow so the "bets" are:
  - the actionable subset where FD/DK still meets discovery PLAY-TO
  - with best-of FD vs DK chosen by max EV (given p_model) for exact point matches
- Policy:
  - exact point matching only in v1 (if point differs -> unmatched -> no-bet unless manual)
- Add measurement counters:
  - % of ranked tickets still actionable at execution time
  - % where FD vs DK choice matters (price differs)
  - distribution of execution_ev_at_discovery_p

Acceptance:
- Discovery+execution run produces a deterministic actionable list and writes artifacts.

### M5: Uncertainty Bands + Conservative Betting Gate

Goal: treat p(hit) as noisy; require robustness.

Deliverables:
- Add p_low/p_high (first pass can be heuristic based on dispersion and sample size proxies).
- New plugin `gate_ev_conservative`:
  - require EV(p_low, price) >= threshold for GO

Acceptance:
- Backtest summary adds "conservative pass rate" and compares ROI under conservative gate.

### M6: Promote a Default Strategy ("Best Suggested Strategy")

Goal: converge to one strategy id for daily use without overfitting.

Policy (recommended):
- Select global winner by ROI subject to:
  - minimum graded bet count (example: >= 200)
  - guardrails: no catastrophic calibration (Brier materially worse than s001 baseline)
- When a winner is chosen:
  - set it as the default strategy id in CLI/env
  - document the rationale and evaluation window in the repo

Acceptance:
- One documented default strategy id and a reproducible command that regenerates the evidence.

## How We Get to "Best Suggested Bets" Daily

Runbook (high-level):
1) Generate discovery snapshot (broad books) and execution snapshot (FD/DK).
2) Run the chosen default strategy on discovery.
3) Produce actionable tickets by checking FD/DK still meet discovery PLAY-TO and match the point.
4) Output:
   - "BET" list: actionable ranked top-N with chosen book (FD or DK) and current price
   - "WATCHLIST" list: near-misses and unmatched (explicit reasons)
5) Save raw odds + matching decisions for audit.

## Assumptions

- Push handling:
  - pushes are excluded from Brier and count as 0 pnl
- Default stake_units:
  - if missing, assume 1.0 for ROI
- Determinism:
  - no network calls during strategy evaluation; strategies operate on snapshot-derived rows only.
