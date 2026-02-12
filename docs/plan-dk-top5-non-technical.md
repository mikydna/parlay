# Plan: DraftKings Profit Workflow (Top 5 Bets/Day)

## Goal

Make money betting NBA player props on DraftKings, **limited to at most 5 bets per day**.

This plan assumes we use The Odds API for multi-book odds and keep all runs snapshot-first (reproducible).

## What Good Looks Like

- Every day, the system outputs:
  - `BET (max 5)`: the only bets you should place on DraftKings
  - `WATCHLIST`: close-but-no-bet candidates with explicit reasons
  - `NO BET`: a valid outcome when conditions are not stable
- We improve over time by measuring:
  - Profit/loss (units)
  - Hit rate (secondary)
  - Probability quality (Brier + calibration buckets)

## The Core Idea (Simple)

1. **Treat the market as the baseline.**
   - We compute a fair (no-vig) probability from multiple non-DK books.
2. **Only bet when DraftKings is mispriced vs that baseline.**
   - We only care about value at the **DraftKings price**, since that's where we execute.
3. **Only bet when the "edge" is stable.**
   - We require multiple books to agree and we avoid stale/uncertain situations.
4. **Pick only the best 5.**
   - We rank by conservative expected value and enforce diversification (avoid 5 correlated bets).

## How Many Books We Need (Clear Rules)

We separate:
- **Execution book**: DraftKings (where you place bets)
- **Discovery books**: other books used to estimate fair probability and stability

**Recommended total**: **8 books** (DraftKings + 7 discovery books), kept to **<=10** books total.

**Minimum viable**: DraftKings + 3 discovery books (works, but will be noisier and require stricter gates).

**Per-prop eligibility requirement (recommended):**
- At decision time, a prop must have **at least 3 discovery books** showing **both Over and Under** for the **same line** (same points).
- If fewer, it's WATCHLIST only (or requires a much higher edge).

## Which Books (Target Set)

Pick DraftKings plus 5-7 "major US" books that consistently post NBA props (using their Odds API bookmaker keys), for example:

- DraftKings (execution)
- FanDuel
- BetMGM
- Caesars
- bet365
- ESPN BET
- BetRivers
- Fanatics (or next best consistently-populated book in your feed)

If Pinnacle/Circa props are available in your feed, they're valuable for discovery, but not required.

## Daily Schedule (Two Runs)

1. **10am ET (watchlist run)**
   - Builds a short list of interesting spots and flags what data is missing (injuries, low book coverage).
   - Does not have to produce 5 bets.
2. **~60 minutes pre-tip (final run)**
   - Uses the freshest odds and stricter gating.
   - Produces the final **Top 5 DK bets** (or NO BET).

## Staged Rollout (Pragmatic)

1. **Stage 1: Market-baseline only**
   - No-vig baseline + stability gates + top-5 selection.
2. **Stage 2: Add calibration tracking**
   - Brier + calibration buckets to detect fake edges.
3. **Stage 3: Add uncertainty bands**
   - Require positive EV under a conservative probability (`p_low`).
4. **Stage 4: Add ML only as a correction**
   - A small "market + features" model (never "from scratch").

## Where This Fits In The Repo

- Odds API snapshot-first flow: `docs/plan-odds-api.md`
- Strategy plugins + multi-book execution: `docs/plan-strategy-plugins.md`
- Scheduled daily run + PDF brief artifacts: `docs/scheduled-flow.md`
