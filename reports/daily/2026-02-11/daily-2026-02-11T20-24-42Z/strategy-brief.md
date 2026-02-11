# Strategy Brief

## Snapshot

- snapshot_id: `daily-2026-02-11T20-24-42Z`
- generated_at_utc: `2026-02-11T20:25:07Z`
- source: `deterministic`

## What The Bet Is

- Bet type: single-player props (over/under stat lines).
- Not a game-winner (moneyline) strategy.
- Injury and roster checks are used as action gates.
- Tier A = 2+ book quotes for a line; Tier B = a single-book quote.

## Executive Summary

- 8 games, 450 candidate lines, 302 eligible plays after gates.
- Eligible plays: `302` out of `450` candidate lines.
- Tier-B mode: `enabled`. Candidate mix: Tier A `279`, Tier B `171`.
- Gated-out lines hidden from this brief: `5`.
- First-page table below shows the top `5` plays across all games.

## Action Plan (GO / LEAN / NO-GO)

### Top 5 Across All Games

| Action | Game | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- | --- |
| LEAN | IND @ BKN | A | Jay Huff OVER 13.5 points @ +100 (fanduel) | Strong edge (+22.5% est.), Higher stake | Lean only: edge exists (EV 0.225) with non-clean injury status. (tier=A; injury=unknown; roster=active) |
| LEAN | IND @ BKN | A | Kam Jones OVER 22.5 points+rebounds+assists @ -119 (draftkings) | Strong edge (+21.0% est.), Higher stake | Lean only: edge exists (EV 0.210) with non-clean injury status. (tier=A; injury=unknown; roster=active) |
| LEAN | IND @ BKN | A | Ben Sheppard OVER 10.5 points @ -109 (draftkings) | Strong edge (+20.7% est.), Higher stake | Lean only: edge exists (EV 0.207) with non-clean injury status. (tier=A; injury=unknown; roster=active) |
| LEAN | IND @ BKN | B | Kam Jones OVER 13.5 points @ -106 (draftkings) | Strong edge (+20.6% est.), Higher stake | Lean only: edge exists (EV 0.206) with non-clean injury status. (tier=B; injury=unknown; roster=active) |
| LEAN | WAS @ CLE | A | Tre Johnson OVER 14.5 points @ -102 (fanduel) | Strong edge (+20.3% est.), Higher stake | Lean only: edge exists (EV 0.203) with non-clean injury status. (tier=A; injury=unknown; roster=active) |



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

- Model uses market-implied fair probabilities with injury/roster/opponent adjustments.
- Minutes/usage projection uses deterministic v0 rules, not learned distributions.
- SGP/SGPx correlation uses deterministic haircut rules (v0).
- `unknown_roster` means the roster feed did not return a trusted active/inactive record for that player in that game snapshot.
- `unknown_event` means event-to-team mapping was missing, so player-to-game roster checks could not be resolved.

## Confidence

- Tier A lines are preferred over Tier B by default.
- Model probabilities are market-implied with injury/roster adjustments.
