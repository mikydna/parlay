# Plan: SOTA-ish NBA Props EV Pipeline Roadmap (Methods Review + Repo Shape)

Date: 2026-02-13

This is the “single-hand, single-design” roadmap doc that ties together:
- the current repo state (what’s already implemented),
- a sanity check on whether the current statistical approach is well-regarded / “SOTA-ish”,
- an explicit staged pipeline architecture (reference books vs execution books),
- a parallelizable work breakdown (so minutes modeling can evolve independently),
- both an engineering plan and a betting-savvy product plan.

Related docs (deeper specifics):
- Odds API data plane + quotas: `docs/plan-odds-api.md`
- Strategy plugins + multi-book execution: `docs/plan-strategy-plugins.md`
- DK Top-5 engineering + non-technical: `docs/plan-dk-top5-engineering.md`,
  `docs/plan-dk-top5-non-technical.md`
- Scheduled operator flow: `docs/scheduled-flow.md`, `docs/runbook.md`
- Contracts: `docs/contracts.md`

---

## 1) Current Repo State (What Landed)

### 1.1 Odds API “data plane” (snapshot + global cache + daily index)

Already implemented:
- `src/prop_ev/odds_data/`:
  - read-through/write-through repository (`repo.py`)
  - global cache store (`cache_store.py`)
  - spend policy (`policy.py`) with offline/no-spend semantics
  - dataset spec + daily windows/index/backfill (`spec.py`, `window.py`, `day_index.py`, `backfill.py`)
- Snapshot persistence (requests/responses/meta/manifest + derived tables):
  - `src/prop_ev/storage.py`
- CLI support:
  - `prop-ev data status/backfill ...` (and snapshot commands) in `src/prop_ev/cli.py`

Operational intent:
- Once a paid payload is fetched once, it becomes historical via the global cache.
- Re-runs are cache hits (offline-capable) unless explicitly refreshed.
- `--offline` forbids all network. `--no-spend` / `--max-credits 0` forbids paid misses.

### 1.2 NBA historical “minutes/usage” data lake (pbp/possessions/boxscore)

Already implemented:
- `src/prop_ev/nba_data/` and a separate CLI entrypoint `nba-data`
- Resumable ingestion + clean parquet + verify:
  - Raw mirrors in `data/nba_data/raw/...`
  - Clean parquet in `data/nba_data/clean/schema_v1/...`

This is intentionally independent from betting/odds logic so minutes modeling can evolve
without destabilizing prop selection.

### 1.3 Strategy system (plugins exist; core logic still centralized)

Already implemented:
- Plugin architecture: `src/prop_ev/strategies/` with ids like `s001`–`s007`
- Registry/recipes: `src/prop_ev/strategies/registry.py`, `src/prop_ev/strategies/base.py`
- Strategies currently delegate into the (large) core report builder:
  - `src/prop_ev/strategy.py` (still the hotspot)

Design intent (already reflected in docs and code):
- The market is the baseline; any ML should be a correction layer, not a replacement.

---

## 2) Are Our Statistical Methods “SOTA” / Well-Regarded?

### 2.1 What we effectively do today (high-level)

From the strategy system + docs:
- Convert American odds to implied probabilities.
- Remove vig on paired O/U via normalization (“no-vig”).
- Aggregate a “fair” baseline probability across books (often via median per-book no-vig).
- Track quality signals like:
  - hold/overround,
  - disagreement/dispersion across books,
  - depth (# of books with both sides at same line),
  - freshness (quote staleness),
  - data integrity/context gates.
- Compute EV vs the *execution* price (e.g. DraftKings) and rank/select.

This family of methods is widely used because it is:
- interpretable,
- cheap (no training required),
- and often strong as a baseline due to market efficiency.

### 2.2 Why it’s defensible (even without ML)

In practical sports betting analytics, “market consensus no-vig probability” is a very common and
well-regarded baseline. It’s also the most realistic baseline to beat: if you can’t beat the
market-derived probability with strong hygiene + execution realism, adding ML tends to add noise,
leakage risk, and maintenance cost.

The following are especially good signs:
- **Median across books**: robust to outliers and stale lines.
- **Hold caps + dispersion caps**: avoid “fake edge” caused by bad price quality or disagreement.
- **Tiering**: recognizing that single-book quotes should face a higher bar is correct.

### 2.3 Where we’re not “SOTA-ish” yet (and what “SOTA” would mean here)

“SOTA” in betting is mostly proprietary and domain-specific. For this repo, a credible “SOTA-ish”
target means the pipeline is:
- **cache-first + replayable** (already mostly solved),
- **execution-realistic** (explicit execution books + limits, not “best across all books”),
- **statistically conservative** (uncertainty bands + robust gates),
- **measurable** (backtests with calibration metrics, not only ROI),
- **modular** (so improvements don’t require rewriting the whole CLI).

Concrete method upgrades that are generally well-regarded and high leverage:
1) **Alt-line handling (biggest near-term lift):**
   - When books don’t share the exact same point, fit a monotone curve
     `p_over_fair(point)` from multiple alternate lines and evaluate at the execution point.
   - First-pass: weighted isotonic regression across discovery books.
2) **De-vig method quality:**
   - The simple “normalize two implied probs to sum to 1” method is standard,
     but there are known alternative de-vig methods (power-style, Shin-style) that can be stronger,
     especially when holds are large or longshot bias matters.
3) **Uncertainty bands + conservative EV:**
   - Compute `p_low/p_high` (even heuristic to start, based on dispersion + depth + staleness),
     and gate bets on `EV(p_low, exec_price)`.
4) **Book weighting / reference book selection:**
   - “Discovery books” should be treated as a reference universe (possibly weighted)
     and should default to excluding the execution book to avoid circularity.
5) **Portfolio optimization and correlation hygiene:**
   - Max 5 bets/day is a portfolio problem, not just ranking.
   - Even simple diversification constraints beat naive top-5 by EV.
6) **Calibration layer (minutes/usage + context features):**
   - Treat as a correction model: `p_model = f(p_market_novig, context_features)`,
     trained and validated walk-forward (time-split).

---

## 3) Core Product Decision: “DK Top 5” is NOT a View Filter

DraftKings Top-5 should not be “compute best across all books then filter for DK in the UI.”

Correct framing:
- **Reference (discovery) books** exist to estimate fair probability + uncertainty.
- **Execution books** are where we can place bets, so ranking/selection must use those prices.

What can be a “view”:
- Rendering: show candidates across all books vs show only the chosen execution plan.

What cannot be a “view”:
- The selection logic itself. The portfolio is defined by execution constraints.

Implication:
- The repo should have an explicit ExecutionPlan artifact that names:
  - which books we can execute on,
  - which ticket identities are selected,
  - and the exact prices used for selection/grading.

---

## 4) Target Architecture: A Staged Pipeline (Conceptually Resonant)

This is the “single-hand design” stage model; each stage has a crisp input/output contract.

### Stage 0 — Acquire (budgeted, cache-first)

Input:
- dataset spec (sport, markets, regions/bookmakers, includeLinks/includeSids)
- time window
- spend policy (`offline`, `no-spend`, `max-credits`, `refresh`, `resume`)

Output:
- a `snapshot_id` and cached raw payloads in snapshot + global cache

Owner modules:
- `src/prop_ev/odds_data/`
- `src/prop_ev/storage.py`

### Stage 1 — Normalize quotes into a canonical “QuoteTable”

Goal: one deterministic table for all downstream logic.

Output row keys (minimum):
- `(event_id, market_key, outcome_key, point, side, book)`

Additional columns:
- `price_american`, `last_update_utc`, `commence_time_utc`, `links/sids` (when available)
- normalized player identity keys (when available)

### Stage 2 — Neutralize vig + compute quality signals (per book, per line)

For each paired O/U line:
- implied probabilities
- no-vig probabilities
- hold/overround
- staleness, integrity signals

### Stage 3 — Reference probability model (discovery books only)

Output:
- `p_ref` at the execution line point
- `p_low/p_high` for conservative EV gates
- evidence: contributing books/lines, dispersion metrics

Baseline approaches:
- v0: median per-book no-vig at exact point (strict)
- v1: alt-line monotone fit (isotonic) evaluated at execution point
- later: calibration layer, but always anchored to market

### Stage 4 — Execution pricing + EV (execution books only)

For each candidate ticket:
- look up execution book price for the exact point/side
- compute break-even and EV (and conservative EV via `p_low`)

### Stage 5 — Eligibility gates (risk + integrity)

Common gates (typical defaults; tune later):
- minimum discovery depth (paired books and/or paired quotes)
- hold cap, dispersion cap
- freshness cap
- player/event identity integrity
- injury/context health gates (when required)

All gates should emit stable reason codes for audit.

### Stage 6 — Portfolio selection (max 5, diversification)

Input: eligible candidates with conservative EV.

Output: an `ExecutionPlan` (<=5 bets) + watchlist + explicit “no bet” reasons.

Portfolio constraints (first version):
- max 1 bet per player
- max 2 bets per game
- avoid highly correlated combos unless EV is exceptional

### Stage 7 — Render outputs (views)

Artifacts:
- `strategy-report.json`
- `execution-plan.json` (new, explicit)
- markdown/tex/pdf briefs (optional)

### Stage 8 — Settle + evaluate (backtest + scoreboards)

Backtest should produce:
- ROI + distribution of outcomes (variance is real)
- calibration: Brier/log loss, reliability buckets
- actionability rate (how often execution book has matching lines)
- CLV proxy where possible (line move after selection)

---

## 5) Repo Shape (Proposed, Gradual Refactor)

The goal is smaller modules + clearer boundaries, without destabilizing the CLI.

Already good separations:
- `prop_ev/odds_data` (data plane)
- `prop_ev/nba_data` (historical NBA data)
- `prop_ev/strategies` (plugin interface)

Recommended next extractions (low-risk, contract-first):
- `src/prop_ev/quotes/`:
  - build canonical QuoteTable from snapshot-derived rows
- `src/prop_ev/pricing/`:
  - implied prob, no-vig methods, alt-line fit, uncertainty bands
- `src/prop_ev/execution/`:
  - execution-book filtering, exact-point matching, actionability
- `src/prop_ev/portfolio/`:
  - max-5 selection + diversification constraints
- `src/prop_ev/evaluation/`:
  - backtest summaries + calibration metrics

The plugin layer (`prop_ev/strategies/s00x_*.py`) should remain thin and compositional:
- each strategy is “baseline + one idea delta”
- the core stage code lives in the extracted modules

---

## 6) Parallelizable Workstreams (Independent Subprojects)

This is the “parallel dev” breakdown so multiple PRs can land without merge hell.

1) **Odds acquisition + spend controls**
   - keep improving `odds_data` (robustness, dataset coverage, dry runs)
2) **Quote normalization + identity hygiene**
   - canonical QuoteTable, player/team mapping, consistent keys
3) **Reference probability modeling**
   - median no-vig baseline, alt-line fit, uncertainty bands
4) **Execution planning + portfolio**
   - explicit execution books, selection constraints, ExecutionPlan artifact
5) **Settlement + evaluation**
   - results ingestion, backtest engine, scoreboards, calibration reports
6) **Minutes/usage modeling (calibration layer)**
   - trained offline from `nba-data` parquet; ships as a correction plugin when proven
7) **Reporting/UX**
   - briefs, operator runbooks, “what changed since last run,” etc.

Minutes/usage modeling is intentionally separable until it demonstrates lift under backtest.

---

## 7) Suggested Next Steps (Small, Reviewable PRs)

If you want the highest ROI sequence without big regressions:

P0) Characterize current outputs
- Freeze a small fixture snapshot in tests and assert key report fields are stable.

P1) Introduce explicit “reference books” vs “execution books” config plumbing
- Keep defaults identical (no behavior change).
- Add CLI flags as additive config (documented).

P2) Add `ExecutionPlan` artifact + portfolio selector (max 5)
- This is the point where “DK Top 5” becomes a first-class compute artifact, not a view.

P3) Add evaluation harness
- Scoreboards by strategy id, Brier/calibration bins, actionability rate.

P4) Alt-line monotone baseline plugin (isotonic)
- Most leverage without ML.

P5) Minutes/usage calibration layer (separate repo subproject until proven)
- Train/validate walk-forward, then integrate as optional plugin.

---

## 8) Product Plan (Betting-Savvy, Non-Technical)

### 8.1 What the system does

Each day, the system:
1) Takes a snapshot of odds for NBA player props.
2) Estimates a fair win probability for each prop (from the market consensus).
3) Compares that fair probability to the prices at the books you can actually bet on.
4) Selects up to 5 bets (or none), with reasons and a watchlist.
5) Tracks performance over time without “hindsight cheating.”

### 8.2 What you control

You choose:
- Which book(s) you can execute on (e.g., DraftKings only).
- Which books you want for reference signal (the “consensus” set).
- How much you’re willing to spend on data today (credits).
- The cutoff time (the snapshot time used for “no lookahead”).
- Your risk tolerance (conservative EV gates, max bets, diversification rules).

### 8.3 What you get as outputs

You get:
- **Top bets (<=5)**: the actual execution plan at your execution book(s).
- **Watchlist**: good candidates missing execution lines or failing a gate.
- **Audit reasons**: why something was selected or skipped (hold too high, stale, too few books, etc.).
- **Performance reports**: ROI + calibration metrics (probability quality), not just “wins/losses.”

### 8.4 How to judge whether it’s working

In the short run (small sample), variance dominates.
Use these “health metrics” over time:
- Actionability rate: how often the execution book has matching lines for good candidates.
- Calibration: when the model says 55%, does it hit ~55% over many bets?
- Stability: do the top bets disappear on small data changes (bad sign)?
- ROI (eventually): only meaningful with enough volume and consistent process.

### 8.5 What to expect (realistic)

- A good system often has many “no bet” days; forcing 5/day is usually a leak.
- Edges are small; process discipline and avoiding fake edges matter more than clever models.
- The best upgrade is often *better use of the market* (alt lines, uncertainty, execution realism),
  not jumping straight to a complex ML model.

