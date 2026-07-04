## Snapshot
- snapshot_id: `daily-20260704T224229Z`
- modeled_date_et: `Saturday, Jul 04, 2026 (ET)`
- generated_at_et: `2026-07-04 06:42:47 PM EDT`
- source_data: `snapshot_inputs`
- scoring: `deterministic`
- narrative: `llm`
Generated: 2026-07-04 06:42:47 PM ET (America/New_York)  
Modeled date: Saturday, Jul 04, 2026 (ET)  
Snapshot ID: daily-20260704T224229Z  
Strategy status: modeled_with_gates (watchlist-only)  
execution_books: []  

Key gaps flagged:
- Official NBA injury report data was not cleanly parsed.
- Roster verification feed unavailable.
- Slate spreads/totals unavailable.
- Health gates triggered watchlist-only mode.
- Minutes/usage and correlation modules use deterministic rules (not learned distributions).

## What The Bet Is
This brief concerns player prop over/under bets (individual player statistics totals), not game-winner or team-outcome bets.

## Executive Summary
No candidate or eligible player-prop lines were generated in this snapshot. Because official injury and roster feeds failed and health gates placed the model in watchlist-only mode, this snapshot is for review only and not actionable for wagering. Do not execute bets based on this snapshot.


<!-- pagebreak -->

## Analyst Take

- mode: `llm_web`

### Read This First

The snapshot (daily-20260704T224229Z) shows no top plays because core external feeds (injury report, roster verification, and slate lines) were unavailable or failed to parse. Public web evidence confirms that the primary sources needed by the model do exist and were actively publishing NBA roster/injury and odds information on July 4, 2026 — meaning the problem appears to be a local ingestion/parse/watchlist gati...

### News Signals

- Supports:
  - [S1] ESPN maintains a continuously updated NBA injuries page (data provided by Rotowire), indicating that an authoritative injury feed exists and is updated independently of the snapshot.
  - [S2] The NBA's official site has active offseason and transaction pages updated on July 4, 2026, showing broad roster movement across teams — supporting the model’s need for roster verification to correctly adjust player-prop projections.
- Refutes/Risks:
  - [S3] Major sportsbooks (e.g., DraftKings Sportsbook) are publishing NBA odds and game lines (spreads/totals) publicly, contradicting the snapshot claim that slate spreads/totals were unavailable.
  - [S4] NBA.com publishes schedules and summer-league schedules (July 9–19, 2026) and has published injury/transaction documents (injury report PDFs are accessible on nba.com), which indicates upstream schedule/injury documents were present on league sites despite the snapshot's parse failure.

### Bottom Line

Public sources show the upstream data the model depends on (official NBA injury/transaction pages and sportsbook odds/schedules) were available on July 4, 2026. The snapshot’s 'no playable top plays' outcome therefore most likely stems from local ingestion/parse failures, missing roster verification feed, or the snapshot being placed into watchlist-only health-gate mode — not from an absolute absence of public inj...

### Source Index

- [S1] NBA Injury Status - ESPN (www.espn.com)
- [S2] NBA Offseason: Every free agency deal, extension & trade for all 30 teams | NBA.com (www.nba.com)
- [S3] NBA Betting Odds & Lines | DraftKings Sportsbook (sportsbook.draftkings.com)
- [S4] 2026 NBA Summer League: What to watch and key dates | NBA.com (www.nba.com)
- Full source URLs are stored in `brief-analyst.json` for audit/debug use.

<!-- pagebreak -->

## Best Available Bet Right Now

- status: no actionable GO/LEAN play passed pre-bet availability checks.
- action: wait for injury/roster updates, then rerun.

<!-- pagebreak -->

## Action Plan (GO / LEAN / NO-GO)

### Top 5 Across All Games

This is player-prop over/under betting, not game winner bets.

- none

Note: If future snapshots restore official injury and roster feeds and lift health gates, re-evaluate for GO/LEAN opportunities.

<!-- pagebreak -->

## Data Quality
Plain statement: Data is missing or weak. Official injury report parsing failed; roster verification feed and slate spreads/totals are unavailable. Health gates restricted execution to watchlist-only. Minutes/usage and SGP correlation use deterministic rules rather than learned distributions, limiting uncertainty modeling.

## Confidence
Low. The snapshot explicitly states modeled_with_gates and contains no candidate lines; confidence in any derived betting signal would be unreliable.

### Interpreting p(hit)

- `p(hit)` = estimated chance the recommended side wins at that line.
- When shown as `X% → Y%`, it is conservative `p(hit)` mapped through calibration history.
- Built from no-vig odds + small injury/roster/spread adjustments (clamped 1%-99%).
- Use it to rank EV, not as a guarantee; judge it by calibration over many bets.
- Can be wrong when odds are stale, coverage is thin, or minutes/role are uncertain.
