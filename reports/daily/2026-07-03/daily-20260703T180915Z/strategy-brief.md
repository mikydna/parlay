## Snapshot
- snapshot_id: `daily-20260703T180915Z`
- modeled_date_et: `Friday, Jul 03, 2026 (ET)`
- generated_at_et: `2026-07-03 02:09:35 PM EDT`
- source_data: `snapshot_inputs`
- scoring: `deterministic`
- narrative: `llm`
- Generated: 2026-07-03 02:09:35 PM ET (America/New_York)
- Modeled date: Friday, Jul 03, 2026 (ET)
- Snapshot ID: daily-20260703T180915Z
- Strategy status: modeled_with_gates
- Summary: No candidate or eligible lines; model ran in watchlist-only gated mode.

## What The Bet Is
This brief covers player-prop over/under bets (not game-winner or spread bets). It assesses whether there are actionable player prop OVER/UNDER opportunities for the modeled slate. No specific tickets or lines are available in this snapshot.

## Executive Summary
- No actionable player-prop lines were produced. candidate_lines = 0 and eligible_lines = 0.
- Key data feeds were missing or failed (official injury reports, roster verification, slate spreads/totals).
- The model was restricted by health gates to watchlist-only mode; outputs are non-actionable.
- Recommendation: do not place wagers based on this snapshot. Obtain a fresh snapshot with complete feeds before betting.


<!-- pagebreak -->

## Analyst Take

- mode: `llm_web`

### Read This First

The model snapshot (daily-20260703T180915Z) reported no candidate lines because official game/roster/injury data was missing. Web evidence shows (A) the NBA regular season concluded in April 2026 and there is not a normal regular-season slate on July 3, 2026 (supporting the absence of a standard sportsbook slate), (B) the NBA and third-party sites list 2026 Summer League events on July 3 (showing there are playabl...

### News Signals

- Supports:
  - [S1] The NBA regular season for 2025–26 concluded in April 2026, so there is no standard regular-season slate on July 3, 2026.
  - [S2] The NBA’s injury-report policy and distribution are game-day centric (teams must report participation status prior to each game), so if game-day feeds were not available the model would lack official injury rows to parse.
- Refutes/Risks:
  - [S3] The NBA lists 2026 Summer League events (including California/San Francisco games) with dates that include July 3, 2026 — third-party slates exist even though they are not regular-season games.
  - [S4] Multiple independent schedule aggregators (RealGM, Sporting News, Sporting schedules) show NBA/Summer League games on July 3, 2026, meaning there are public game schedules available for that date.
  - [S5] Third-party injury pages (Covers, SportBusy, ESPN injury pages) are publishing injury listings/updates in early July 2026 — indicating injury data is available outside the model snapshot.

### Bottom Line

The model’s watchlist-only result is plausible given missing official game-day roster/injury feed ingestion (official NBA injury reports are game-day PDFs and the regular season ended April 12, 2026). However, public sources (NBA Summer League pages, RealGM, Sporting News, Covers, SportBusy, ESPN injury pages) confirm there are Summer League games on July 3, 2026 and third-party injury/schedule data exists.

### Source Index

- [S1] NBA announces schedule for 2025-26 regular season (www.nba.com)
- [S2] NBA Injury Report: 2025-26 Season (official NBA guidance) (official.nba.com)
- [S3] 2026 Summer League | Latest (NBA.com) (www.nba.com)
- [S4] NBA Basketball Scoreboard - July 3, 2026 (RealGM) / Sporting News Summer League schedule (basketball.realgm.com)
- [S5] NBA Injuries — Full Injury Report for Jul 03, 2026 (Covers) and SportBusy injury list (www.covers.com)
- Full source URLs are stored in `brief-analyst.json` for audit/debug use.

<!-- pagebreak -->

## Best Available Bet Right Now

- status: no actionable GO/LEAN play passed pre-bet availability checks.
- action: wait for injury/roster updates, then rerun.

<!-- pagebreak -->

## Action Plan (GO / LEAN / NO-GO)

### Top 5 Across All Games

State: This is player-prop over/under betting, not game winner bets.
Data quality is weak; no playable tickets were generated.

- none

<!-- pagebreak -->

## Data Quality
Major data issues:
- Official NBA injury report data not parsed or missing.
- Roster verification feed unavailable (roster_team_rows = 0).
- Slate spreads/totals absent (candidate_lines = 0).
- Health gates limited model to watchlist-only mode.
Because of these gaps, outputs are non-actionable.

## Confidence
Low. No actionable plays were produced and core input feeds were missing or not trusted. From the model: "No actionable plays due to lack of input markets and roster/injury confirmation." Obtain fresh, complete data before any betting.

### Interpreting p(hit)

- `p(hit)` = estimated chance the recommended side wins at that line.
- When shown as `X% → Y%`, it is conservative `p(hit)` mapped through calibration history.
- Built from no-vig odds + small injury/roster/spread adjustments (clamped 1%-99%).
- Use it to rank EV, not as a guarantee; judge it by calibration over many bets.
- Can be wrong when odds are stale, coverage is thin, or minutes/role are uncertain.
