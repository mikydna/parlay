## Snapshot
- source_data: `snapshot_inputs`
- scoring: `deterministic`
- narrative: `llm`
Generated: 2026-02-11T20:25:07Z. Slate concentrated on IND @ BKN, WAS @ CLE, and a single NYK @ PHI play. All recommended plays are player-prop over/under bets (not game-winner bets). Many plays are marked LEAN because injury or roster signals are unclear; some players were excluded (NO-GO) due to roster checks showing "not_on_roster."

## What The Bet Is
Player-prop over/under bets (points, rebounds, assists, threes, or combined PRA). These are bets on individual player stat totals (OVER a given line). Do not interpret these as bets on team outcomes or game winners.

## Executive Summary
- Top opportunities show model edges vs market across several player props, but most recommendations are LEAN due to non-clean injury/roster signals.
- Highest-priority matchups: IND @ BKN and WAS @ CLE produce multiple A-tier LEAN plays (Jay Huff, Kam Jones, Ben Sheppard, Tre Johnson, Jarrett Allen, etc.).
- Watchlist contains NO-GO items where roster checks flagged players as not on roster — do not bet those unless status changes to active.
- Verify live injury reports and final rotations before placing any wagers; data gaps reduce decision quality.

## Action Plan (GO / LEAN / NO-GO)

### Top 5 Across All Games

| Action | Game | Tier | Ticket | Edge Note | Why |
|---|---:|---:|---|---|---|
| GO | — | — | — | — | No plays meet a clean GO threshold in the input; all eligible, high-edge plays are flagged LEAN because injury/roster signals are not clean. |
| LEAN | IND @ BKN | A | Jay Huff OVER 13.5 points @ +100 (fanduel) | Strong edge (+22.5% est.) | Model shows value vs market but injury/roster status is listed "unknown"; lean only. |
| LEAN | IND @ BKN | A | Kam Jones OVER 22.5 P+R+A @ -119 (draftkings) | Strong edge (+21.0% est.) | Good model edge but non-clean injury/roster signals — cautious stake. |
| LEAN | IND @ BKN | A | Ben Sheppard OVER 10.5 points @ -109 (draftkings) | Strong edge (+20.7% est.) | Value present; final rotation/injury checks unresolved. |
| LEAN | IND @ BKN | A | Jay Huff OVER 5.5 rebounds @ +110 (fanduel) | Strong edge (+20.0% est.) | Same caveat: rotation/injury status unclear. |
| LEAN | WAS @ CLE | A | Tre Johnson OVER 14.5 points @ -102 (fanduel) | Strong edge (+20.3% est.) | Model edge exists; injury/roster listed "unknown" — lean. |
| LEAN | WAS @ CLE | A | Jarrett Allen OVER 16.5 points @ +102 (fanduel) | Strong edge (+18.7% est.) | Value flagged but non-clean signals. |
| LEAN | NYK @ PHI | A | Landry Shamet OVER 2.5 threes @ +128 (fanduel) | Strong edge (+18.3% est.) | Single qualified play on slate; follow late scratches/role changes. |
| NO-GO | WAS @ CLE | A/B | Carlton Carrington (various tickets) — UNDER lines | Roster gate: not_on_roster | Watchlist flagged these as not_on_roster; do not bet unless roster confirms active. |
| NO-GO | LAC @ HOU | A | Derrick Jones (various tickets) — UNDER lines | Roster gate: not_on_roster | Excluded by roster check; do not bet. |

(Ordered by decision quality: GO first, then LEAN, then NO-GO. No GO entries available from the input.)



<!-- pagebreak -->

## Game Cards by Matchup

### WAS @ CLE
- Tip (ET): `07:10 PM ET`
- Listed plays: `5`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| LEAN | A | Tre Johnson OVER 14.5 points @ -102 (fanduel) | Strong edge (+20.3% est.), Higher stake | Lean only: edge exists (EV 0.203) with non-clean injury status. |
| LEAN | B | Justin Champagnie OVER 1.5 three-pointers @ +173 (draftkings) | Strong edge (+19.6% est.), Higher stake | Lean only: edge exists (EV 0.196) with non-clean injury status. |
| LEAN | A | Jarrett Allen OVER 16.5 points @ +102 (fanduel) | Strong edge (+18.7% est.), Higher stake | Lean only: edge exists (EV 0.187) with non-clean injury status. |
| LEAN | B | Will Riley OVER 19.5 points+rebounds+assists @ -103 (draftkings) | Strong edge (+18.0% est.), Higher stake | Lean only: edge exists (EV 0.180) with non-clean injury status. |
| LEAN | A | Jaylon Tyson OVER 13.5 points @ -111 (draftkings) | Strong edge (+18.0% est.), Higher stake | Lean only: edge exists (EV 0.180) with non-clean injury status. |

<!-- pagebreak -->

### NYK @ PHI
- Tip (ET): `07:30 PM ET`
- Listed plays: `1`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| LEAN | A | Landry Shamet OVER 2.5 three-pointers @ +128 (fanduel) | Strong edge (+18.3% est.), Higher stake | Lean only: edge exists (EV 0.183) with non-clean injury status. |

<!-- pagebreak -->

### IND @ BKN
- Tip (ET): `07:40 PM ET`
- Listed plays: `5`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| LEAN | A | Jay Huff OVER 13.5 points @ +100 (fanduel) | Strong edge (+22.5% est.), Higher stake | Lean only: edge exists (EV 0.225) with non-clean injury status. |
| LEAN | A | Kam Jones OVER 22.5 points+rebounds+assists @ -119 (draftkings) | Strong edge (+21.0% est.), Higher stake | Lean only: edge exists (EV 0.210) with non-clean injury status. |
| LEAN | A | Ben Sheppard OVER 10.5 points @ -109 (draftkings) | Strong edge (+20.7% est.), Higher stake | Lean only: edge exists (EV 0.207) with non-clean injury status. |
| LEAN | B | Kam Jones OVER 13.5 points @ -106 (draftkings) | Strong edge (+20.6% est.), Higher stake | Lean only: edge exists (EV 0.206) with non-clean injury status. |
| LEAN | A | Jay Huff OVER 5.5 rebounds @ +110 (fanduel) | Strong edge (+20.0% est.), Higher stake | Lean only: edge exists (EV 0.200) with non-clean injury status. |

<!-- pagebreak -->

## Data Quality
- Key data gaps: many plays show injury_status = "unknown" and several watchlist items were excluded due to roster_status = "not_on_roster."
- Model limitations flagged in input: deterministic minutes/usage projections and deterministic SGP/SGPx correlation rules (v0) — these increase model uncertainty versus learned-distribution approaches.
- Roster gating operated: some tickets were removed as NO-GO because roster data did not confirm the player active.

If data is missing or weak: stated plainly — injury and roster feeds are incomplete/uncertain for several plays; confirm official sources before betting.

## Confidence
- Overall confidence: Moderate-to-low for placing full stakes because most top edges are contingent on final injury/roster confirmations.
- Source note: All confidence and edge metrics are taken directly from the provided payload. Recommendations are LEAN where the payload indicates non-clean injury/roster status.
- Action recommendation: Re-check official injury reports and starting lineups just before lock; if roster/injury clears (player confirmed active and role unchanged), consider upgrading LEANs to fuller stakes per your staking plan.
