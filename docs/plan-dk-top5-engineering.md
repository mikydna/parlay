# Plan: DraftKings Profit Workflow (Top 5 Bets/Day) - Engineering Design

## Scope + Non-Negotiables

- Objective: maximize long-run profit betting **only on DraftKings**, constrained to **max 5 bets/day**.
- No lookahead: pick bets using only odds/info available by the chosen cutoff time.
- Snapshot-first: evaluation/backtests must run offline from cached snapshots.
- Model design: the market is the baseline; any ML is a correction layer, not a replacement.

Related docs:
- Odds API caching + quotas: `docs/plan-odds-api.md`
- Strategy system + discovery/execution: `docs/plan-strategy-plugins.md`
- Scheduled run outputs (MD/TeX/PDF): `docs/scheduled-flow.md`

## Data Requirements

### Odds (required)

For each quote at snapshot time:
- `event_id`, `commence_time`
- `book`
- `market` (e.g., `player_points`, `player_rebounds`, ...)
- `player`
- `point` (line)
- `side` (`over`/`under`)
- `price` (American)
- `last_update`

### Results/Settlement (required for backtests)

For each graded ticket:
- `event_id`, `player`, `market`, `point`, `side`
- final stat value (from box score)
- outcome: win/loss/push

### Optional context (later)

- Injury statuses, projected minutes/role signals, rest/travel
- Team totals/spreads (can be from the same odds feed)

## Book Coverage Requirements (Explicit)

We separate:
- **Execution book**: `draftkings`
- **Discovery books**: non-DK books used to estimate fair probability + uncertainty

### Request-time book list

To keep Odds API cost predictable (and typically within a single "region-equivalent" group), keep the bookmaker list to **<= 10**.

Recommended list size: **8 books total**:
- `draftkings` (execution)
- 7 discovery books (major US books with consistent prop coverage)

### Per-ticket minimums (recommended defaults)

For a ticket to be eligible for `BET`:
- **Discovery book pairs**: >= 3 books with **both** Over and Under for the **same** `point`
- **Execution presence**: DraftKings must have the matching market + point + side at decision time

If the ticket fails these, it can still be `WATCHLIST` with an explicit reason.

## Probability Baseline (Market No-Vig)

Per book, for a prop where we have both sides at the same point:
- Convert American odds to implied probabilities:
  - `p_over_raw`, `p_under_raw`
- Remove vig by normalization:
  - `p_over_novig = p_over_raw / (p_over_raw + p_under_raw)`
  - `p_under_novig = 1 - p_over_novig`
- Compute book quality signals:
  - `hold_book = (p_over_raw + p_under_raw) - 1`

Consensus baseline (exclude DK by default to avoid circularity):
- `p_market_novig = median(p_over_novig across discovery books)`
- `hold_median = median(hold_book across discovery books)`
- `dispersion = IQR(p_over_novig across discovery books)`
- `n_pairs = count(discovery books with O/U pair)`

## Alt-Line Support (Most SOTA / Most Leverage)

Problem:
- Books often disagree on the exact line point (and they move quickly), so "same point across 3 books"
  can throw away a lot of usable signal and reduce the chance we can find 5 good DK bets.

Upgrade:
- Build a "market-implied distribution" per `(event_id, player, market)` using many alternate lines
  across discovery books, then evaluate the fair probability at the DraftKings point.

Concrete approach:
1. Collect paired quotes across discovery books:
   - each datapoint is `(point, p_over_novig, hold_book, last_update, book)`
2. Fit a monotone curve of fair probability vs line:
   - fit `p_over_fair(point)` as a monotone-decreasing function of `point`
   - practical first version: weighted isotonic regression on `(point -> p_over_novig)`
   - optional: add smoothing so the curve is not step-like (keep monotonicity)
3. Evaluate at DK line:
   - `p_market_novig = p_over_fair(point_DK)`
4. Derive uncertainty bands:
   - bootstrap across books/quotes to produce `p_low/p_high`
   - bet gate uses conservative EV: `EV(p_low, DK_price) > 0`

Eligibility gates for this mode (recommended):
- minimum paired quote count across discovery books (example: >= 6 total pairs)
- minimum unique discovery books contributing pairs (example: >= 3)
- require at least 2 distinct points reasonably near DK's point (to avoid fragile extrapolation)

Net effect:
- materially improves coverage and ranking quality without needing a "from scratch" player model.

## Execution Scoring (DraftKings Only)

For each candidate ticket, compute EV using the **DraftKings** price at the decision snapshot:
- `p_break_even_DK = implied_prob(DK_price)` (vig-included threshold)
- `edge = p_market_novig - p_break_even_DK`
- `ev_1u = p_market_novig * profit_if_win(DK_price) - (1 - p_market_novig) * 1`

Key rule:
- Rank/select using `ev_1u` (and later conservative EV), not "best price across books."

## Uncertainty Bands + Conservative EV

We treat `p_market_novig` as noisy; we want `p_low/p_high` to prevent "fake edge."

First-pass (heuristic) `p_low`:
- start from `p_market_novig`
- subtract a penalty based on:
  - small `n_pairs`
  - high `dispersion`
  - high `hold_median`
  - stale odds (large `now - last_update`)

Bet gate (recommended):
- require `EV(p_low, DK_price) > 0` (or > small buffer like 0.5%-1% in 1u terms)

## Stability Gates (Edge + Stability, Not Edge Alone)

Recommended gates for `BET` eligibility:
- `n_pairs >= 3` (discovery depth)
- `hold_median <= hold_cap`
- `dispersion <= dispersion_cap`
- odds freshness: `max_age_seconds <= freshness_cap`
- data integrity: known player mapping + known event + valid market schema
- optional injury/role gate when available (avoid "unknown roster / GTD chaos")

All gate failures should produce a stable, user-readable reason code.

## Portfolio Selection (Max 5 Bets/Day)

We need an explicit "portfolio" layer that chooses exactly up to 5 bets from eligible candidates.

Selection policy (simple and effective):
1. Filter to `BET`-eligible by gates.
2. Score by **conservative EV**: `ev_1u_low = EV(p_low, DK_price)`.
3. Apply diversification constraints:
   - max 1 bet per player
   - max 2 bets per game
   - avoid highly-correlated combos (e.g., Points + PRA same player) unless EV is exceptional
4. Take top 5 by score.
5. If < 1 passes, output `NO BET`.

Outputs:
- `bets[]` (<=5), `watchlist[]`, `no_bet_reasons[]`

## Backtesting Design (30 Days First)

Backtest must simulate real operations:
- Choose a cutoff policy:
  - primary: `tip_time - 60 minutes`
  - optional: `10am ET` run (watchlist)
- For each date:
  - build the candidate set from the snapshot at/preceding the cutoff
  - select **<=5** bets (portfolio selection)
  - grade using final stats (win/loss/push)
- Report:
  - PnL/ROI (flat 1u baseline)
  - hit rate
  - Brier score
  - calibration buckets
  - "actionability rate" (how often we could actually find 5 bets)

Important: do not tune thresholds on the full window without a walk-forward scheme.

## ML Add-On (Only After Baseline/Gates Are Stable)

If we add ML, it should be a correction model:
- `p_model = f(p_market_novig, context_features...)`

Rules:
- Walk-forward training/validation by date (no leakage).
- Calibrate (isotonic/logistic) if needed.
- Keep a "no-ML baseline" strategy active as a control.

## LLM Add-Ons (Optional, Non-Deterministic)

Principle:
- LLMs should not be the probability engine. They should add *decision hygiene* and *execution quality*
  around the deterministic EV stack.

High-leverage uses:
- **Strict structured outputs**: require JSON schemas for any non-deterministic step (no missing keys,
  no invented columns, no free-form drift). Use this for: risk tags, portfolio picks (max 5),
  and "why" text that references existing fields.
- **Risk tagging (qualitative)**: label rows with hard-to-model uncertainty (minutes volatility,
  role ambiguity, blowout risk, trade/debut risk, stale info). Convert tags into deterministic gates
  or EV penalties.
- **Portfolio selection under constraints**: choose exactly up to 5 bets from a candidate set while
  enforcing diversification (avoid correlated exposures) and obeying hard rules (injury gates,
  min book depth, freshness). The model must select only from provided candidates (no new bets).
- **Vision fallback for UI-only data**: if DK lines must be verified from screenshots, use a vision-capable
  model to extract (player, market, point, side, price) and compare to snapshot rows.
- **Audio/briefing outputs (optional)**: generate a short morning/60-min-pre-tip briefing based on the
  deterministic shortlist (no web, no new facts).

Guardrails and logging:
- LLM outputs may never override numeric inputs (`p_*`, odds, EV). Deterministic values win.
- Store raw prompts/outputs + model IDs + prompt hashes + usage in snapshot artifacts for auditability.
- Add an explicit LLM budget cap and caching (re-run should be stable where possible).
