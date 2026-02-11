# Strategy: Odds API Integration for NBA Props EV Pipeline

## Goal

Replace fragile odds/props web scraping with structured The Odds API calls, while keeping injury and roster verification on authoritative sources.

## Original Prompt Flow (Facts -> Model -> Bet)

1. Pre-flight facts:
   - Lock today's slate (tip times, spreads, totals).
   - Verify team/roster.
   - Ingest official NBA injury report plus a secondary source.
   - Price-shop props:
     - Tier A: 2 quotes.
     - Tier B: 1 quote with a higher bar.
2. Modeling:
   - Convert minutes/usage + pace into `p(hit)` across markets.
   - Compute fair odds, EV, play-to, Kelly.
3. Output:
   - Ranked plays.
   - Watchlist.
   - Audit trail with links and timestamps.

Main pain point in the old flow: LLM-driven scraping/parsing/stitching for props/lines across multiple pages.

## Why The Odds API Improves Speed + Reliability

1. Slate snapshot in one call:
   - Use `/odds` to pull games with spreads/totals.
   - Feed this directly into the slate snapshot table.
2. Player props via per-event calls:
   - Use `/events/{eventId}/odds` for additional markets.
   - Run requests concurrently with bounded parallelism.
3. Cleaner price shopping:
   - Select best Over and best Under across bookmakers in one payload.
   - Store each bookmaker `last_update` for freshness tracking.
4. Tiering rule becomes explicit:
   - Tier A: require >=2 distinct bookmakers for the same line/market.
   - Tier B: allow 1 bookmaker, but require higher EV threshold.
5. Better audit trail:
   - Use `includeLinks=true` and `includeSids=true` to attach deep links to bets/events/markets/outcomes.
6. Predictable operations:
   - Additional markets update on roughly 1-minute cadence.
   - Cost model is explicit: credits scale with markets x regions.

## Drop-in Mapping to Existing Spec

- A2 Slate Enumeration:
  - Replace ESPN scraping with Odds API `/odds` for spreads/totals.
- A5 Odds & Price Shopping:
  - Replace web odds boards with Odds API `/events/{eventId}/odds` for player props.
- Keep all other stages:
  - Injury and roster verification remain in place.

## Boundaries: What Not to Delegate to Odds API

- Official injury status:
  - Keep NBA injury report as authoritative.
- Team/roster verification:
  - Keep NBA.com/ESPN as anti-hallucination checks.
- LLM role:
  - Use for summaries, rationale writing, and consistency checks.
  - Do not use LLM as the source of truth for odds.

## Recommended Sourcing Policy (Explicit)

Use this statement in the run spec:

> Odds come exclusively from The Odds API. Web browsing is used only for injury and roster verification.
