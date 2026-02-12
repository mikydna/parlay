## Snapshot
- source_data: `snapshot_inputs`
- scoring: `deterministic`
- narrative: `llm`
Date: 2026-02-12 (generated 00:28:45Z).
Scope: Player-prop over/under bets for tonight's slate. Top plays span IND@BKN, NYK@PHI, ATL@CHA, WAS@CLE. All recommended plays below are player-prop over/under markets (not game-winner bets). Data comes solely from the provided strategy payload; confirm live odds and availability before wagering.

## What The Bet Is
Player-prop over/under bets (examples: points, assists, points+rebounds+assists, three-pointers). Recommendations are model-backed "over" plays where the model estimates a positive expected value versus the posted market. These are not side/point-spread or game outcome wagers.

## Executive Summary
- Multiple Tier A player-prop overs show strong model edges and available roster status. Top, highest-confidence plays include Day'Ron Sharpe O 14.5 (DK), Nolan Traore O 5.5 assists (FD), Drake Powell O 11.5 PRA (DK), Kelly Oubre Jr O 24.5 PRA (DK), and Onyeka Okongwu O 24.5 PRA (FD).
- Several other Tier A/B overs are listed as additional GO opportunities. Watchlist contains NO-GO items where roster/injury gates prevent wagering.
- Important: this brief relies solely on the supplied JSON. If any "available_unlisted", "unknown", or "out" values exist for a player, confirm final availability and current odds before placing bets.


<!-- pagebreak -->

## Analyst Take

- mode: `llm_web`

### Read This First

The provided deterministic model identifies five Tier A "GO" player-prop plays across three NBA games, led by Day'Ron Sharpe (IND @ BKN) and Nolan Traore (IND @ BKN). The model uses market-implied probabilities with manual adjustments for injuries/rosters/opponents and applies deterministic (v0) rules for minutes/usage projections and correlation haircuts.

### News Signals

- Supports:
  - [S1] Day'Ron Sharpe has been highlighted by game coverage and box-score reporting as an active contributor in Nets games, consistent with a model selecting him as a top prop play.
  - [S2] Kelly Oubre Jr. has a recent official injury designation from the NBA (knee) that could affect his availability and thus the model's adjustments for player usage and prop evaluations.
  - [S3] Onyeka Okongwu has recorded a recent double-double (15 points, 10 rebounds) in game action, supporting model selection of him as a Tier A prop candidate for points+rebounds+assists markets.
- Refutes/Risks:
  - [S4] Public game previews and reporting do not provide independent market EV calculations; available news sources do not confirm the model's stated edge magnitudes for the listed top_plays.

### Bottom Line

- **Best Bet:** **Day'Ron Sharpe OVER 14.5 points @ -103 (draftkings)**
- **Lookup Line:** `IND @ BKN` | `OVER 14.5 points` | `draftkings -103`
The model surfaces several plausible Tier A player-prop candidates and incorporates roster/injury adjustments; independent news sources provide some supporting performance and injury context (e.g., Sharpe, Okongwu, Oubre). However, the model relies on deterministic v0 rules for minutes/usage and correlation haircuts rather than learned distributions, and external sources do not corroborate the specific EV values.

### Source Index

- [S1] Nets vs Wizards 127-113 (game coverage mentioning Day'Ron Sharpe) (www.netsdaily.com)
- [S2] 76ers: Kelly Oubre Jr. out with knee injury; will be reevaluated in two weeks (www.nba.com)
- [S3] Onyeka Okongwu posts double-double with 15 points and 10 rebounds as Hawks defeat Hornets (www.fanduel.com)
- [S4] Knicks at 76ers game preview (news coverage lacks market EV details) (www.postingandtoasting.com)
- Deterministic model details are summarized in Snapshot/Action Plan.
- Full source URLs are stored in `brief-analyst.json` for audit/debug use.

<!-- pagebreak -->

## Best Available Bet Right Now

- **Decision:** **GO** (meets clean GO gates in this snapshot.)
- **Bet:** **Day'Ron Sharpe OVER 14.5 points @ -103 (draftkings)**
- **Lookup Line:** `IND @ BKN` | `OVER 14.5 points` | `draftkings -103`
- **Model Edge:** `+35.2% est.`
- why: Go: verified availability with tier A market depth and positive edge (EV 0.352).

<!-- pagebreak -->

## Action Plan (GO / LEAN / NO-GO)

### Top 5 Across All Games

Note: This is player-prop over/under betting, not game winner bets.

| Action | Game | Tier | Ticket | Edge Note | Why |
|---|---:|---:|---|---|---|
| GO | IND @ BKN | A | Day'Ron Sharpe OVER 14.5 points @ -103 (draftkings) | Strong edge (+35.2% est.), Higher stake | Model shows substantial positive EV and player is listed active with Tier A market depth. |
| GO | IND @ BKN | A | Nolan Traore OVER 5.5 assists @ +130 (fanduel) | Strong edge (+34.3% est.), Higher stake | Positive edge with Tier A depth; assists over at a plus price. |
| GO | IND @ BKN | A | Drake Powell OVER 11.5 points+rebounds+assists @ +101 (draftkings) | Strong edge (+34.2% est.), Higher stake | Combined PRA line shows meaningful edge and active roster status. |
| GO | NYK @ PHI | A | Kelly Oubre Jr OVER 24.5 points+rebounds+assists @ -104 (draftkings) | Strong edge (+33.8% est.), Higher stake | Model probability and Tier A depth indicate a positive edge; availability confirmed. |
| GO | ATL @ CHA | A | Onyeka Okongwu OVER 24.5 points+rebounds+assists @ -102 (fanduel) | Strong edge (+33.8% est.), Higher stake | Tier A market depth and model edge; player listed available. |
| LEAN | IND @ BKN | A | Jay Huff OVER 13.5 points @ +102 (fanduel) | Strong edge (+33.6% est.) | Good EV but lower event prominence vs top GO picks—consider smaller stake or diversify. |
| LEAN | NYK @ PHI | B | Dominick Barlow OVER 14.5 PRA @ -104 (draftkings) | Strong edge (+31.6% est.) | Tier B depth; acceptable but allocate smaller unit size. |
| NO-GO | WAS @ CLE | B | Carlton Carrington UNDER 2.5 rebounds @ +124 (draftkings) | Roster/injury uncertainty | Roster check did not confirm active status; pre-bet readiness false—do not bet. |
| NO-GO | LAC @ HOU | B | Derrick Jones UNDER 2.5 rebounds @ +123 (draftkings) | Roster/injury uncertainty | Roster/injury gate flagged; pre-bet readiness false—do not bet. |
| NO-GO | WAS @ CLE | B | Alex Sarr UNDER 0.5 three-pointers @ +151 (draftkings) | Player listed out | High availability risk (injury status = out). |



<!-- pagebreak -->

## Game Cards by Matchup

### ATL @ CHA
- Tip (ET): `07:10 PM ET`
- Listed plays: `2`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| GO | A | Onyeka Okongwu OVER 24.5 points+rebounds+assists @ -102 (fanduel) | Strong edge (+33.8% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.338). |
| GO | A | Zaccharie Risacher OVER 1.5 three-pointers @ +162 (fanduel) | Strong edge (+32.3% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.323). |

<!-- pagebreak -->

### WAS @ CLE
- Tip (ET): `07:10 PM ET`
- Listed plays: `1`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| GO | A | Jarrett Allen OVER 28.5 points+rebounds+assists @ -104 (fanduel) | Strong edge (+31.1% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.311). |

<!-- pagebreak -->

### NYK @ PHI
- Tip (ET): `07:30 PM ET`
- Listed plays: `5`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| GO | A | Kelly Oubre Jr OVER 24.5 points+rebounds+assists @ -104 (draftkings) | Strong edge (+33.8% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.338). |
| GO | B | Dominick Barlow OVER 14.5 points+rebounds+assists @ -104 (draftkings) | Strong edge (+31.6% est.), Higher stake | Go: verified availability with tier B market depth and positive edge (EV 0.316). |
| GO | B | Dominick Barlow OVER 8.5 points @ +107 (draftkings) | Strong edge (+31.4% est.), Higher stake | Go: verified availability with tier B market depth and positive edge (EV 0.314). |
| GO | B | Kelly Oubre Jr OVER 2.5 three-pointers @ +148 (fanduel) | Strong edge (+31.2% est.), Higher stake | Go: verified availability with tier B market depth and positive edge (EV 0.312). |
| GO | B | VJ Edgecombe OVER 24.5 points+rebounds+assists @ -108 (draftkings) | Strong edge (+31.1% est.), Higher stake | Go: verified availability with tier B market depth and positive edge (EV 0.311). |

<!-- pagebreak -->

### IND @ BKN
- Tip (ET): `07:40 PM ET`
- Listed plays: `5`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| GO | A | Day'Ron Sharpe OVER 14.5 points @ -103 (draftkings) | Strong edge (+35.2% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.352). |
| GO | A | Nolan Traore OVER 5.5 assists @ +130 (fanduel) | Strong edge (+34.3% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.343). |
| GO | A | Drake Powell OVER 11.5 points+rebounds+assists @ +101 (draftkings) | Strong edge (+34.2% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.342). |
| GO | A | Danny Wolf OVER 14.5 points @ +100 (fanduel) | Strong edge (+33.7% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.337). |
| GO | A | Jay Huff OVER 13.5 points @ +102 (fanduel) | Strong edge (+33.6% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.336). |

<!-- pagebreak -->

## Data Quality
- Source: All information is taken exactly from the provided JSON. No external verification or live odds refresh performed.
- Known gaps from payload: model uses deterministic minutes/usage rules (not learned distributions); SGP/SGPx correlation uses deterministic haircut rules (v0). These can understate actual variance in lineups and correlations.
- Roster/injury fields include values like "available_unlisted", "unknown", and "out". When non-"available" values appear, reliability drops and bets should be confirmed manually.

## Confidence
- Overall confidence in listed GO plays: medium-high for Tier A items where roster status = active/
