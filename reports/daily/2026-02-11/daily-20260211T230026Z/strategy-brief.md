## Snapshot
- source_data: `snapshot_inputs`
- scoring: `deterministic`
- narrative: `llm`
- llm_pass1_status: `fallback`
Date: generated 2026-02-11 UTC snapshot_id daily-20260211T230026Z.  
Slate summary: 8 games, 497 candidate lines, 329 eligible plays after gates. This brief covers player-prop over/under bets only (not game-winner or spread bets). Many plays show model edge but carry roster/injury uncertainty.

## What The Bet Is
Player-prop over/under wagers — single-player totals (points, threes, rebounds, assists, or combined PRA). Each recommended ticket below is the specific market (e.g., "Player OVER X") and the listed book/price.

## Executive Summary
- No clear GOs — model shows many positive-expected-value plays but the decision gate defaults to LEAN because injury or roster signals are not clean.  
- Highest-priority opportunities (by modeled EV) are Jay Huff OVER 13.5 (IND @ BKN) and several Kam Jones lines (IND @ BKN), plus Jarrett Allen OVER 28.5 PRA (WAS @ CLE). All are LEAN recommendations due to uncertainty.  
- Several tickets are explicitly NO-GO because of roster/injury gates (players not on roster or confirmed out); do not bet those.  
- Data gaps: minutes/usage and correlation rules are deterministic (v0), not learned from distributions — treat projected usage as less robust.


<!-- pagebreak -->

## Analyst Take

- mode: `llm_web`

### Read This First

The model produced a slate-level summary and top plays for an 8-game slate with 329 eligible lines. Top recommended 'LEAN' plays concentrate on IND @ BKN props (Jay Huff and Kam Jones) and a WAS @ CLE prop (Jarrett Allen).

### News Signals

- Refutes/Risks:
  - [S1] Recent game recaps and box scores show Jarrett Allen logged a 22/13 double-double in a Cavaliers game, which is lower than the model's suggested OVER 28.5 points+rebounds+assists ticket.
  - [S2] Jay Huff has recorded a career-high 29 points in at least one recent Pacers game, indicating volatility around his point totals relative to the model's Jay Huff OVER 13.5 points play.

### Bottom Line

- **Best Bet:** **Jay Huff OVER 13.5 points @ +102 (fanduel)**
- **Lookup Line:** `IND @ BKN` | `OVER 13.5 points` | `fanduel +102`
The deterministic model offers specific actionable 'LEAN' prop recommendations and clearly states its methodological limits (deterministic minutes/usage and correlation rules). External box-score evidence partially challenges the Allen OVER 28.5 PRA ticket (recent 22/13) while showing Huff has upside (career-high 29) but also variability.

### Source Index

- [S1] CBSSports recap: Cavaliers Jarrett Allen double-double (www.cbssports.com)
- [S2] CBSSports: Pacers Jay Huff nets career-high 29 points (www.cbssports.com)
- Deterministic model details are summarized in Snapshot/Action Plan.
- Full source URLs are stored in `brief-analyst.json` for audit/debug use.

<!-- pagebreak -->

## Best Available Bet Right Now

- **Decision:** **LEAN** (no GO passed clean gates in this snapshot; this is the highest-ranked LEAN.)
- **Bet:** **Jay Huff OVER 13.5 points @ +102 (fanduel)**
- **Lookup Line:** `IND @ BKN` | `OVER 13.5 points` | `fanduel +102`
- **Model Edge:** `+24.0% est.`
- why: Lean only: edge exists (EV 0.240) with non-clean injury status.

<!-- pagebreak -->

## Action Plan (GO / LEAN / NO-GO)

### Top 5 Across All Games

This is player-prop over/under betting, not game winner bets.

| Action | Game | Tier | Ticket | Edge Note | Why |
|---|---:|:---:|---|---|---|
| LEAN | IND @ BKN | A | Jay Huff OVER 13.5 points @ +102 (fanduel) | Strong edge (+24.0% est.) | High EV but injury/availability flagged non-clean — size down. |
| LEAN | IND @ BKN | B | Kam Jones OVER 22.5 points+rebounds+assists @ -113 (fanduel) | Strong edge (+21.8% est.) | Good EV; roster/injury status uncertain — prefer smaller stake. |
| LEAN | IND @ BKN | A | Kam Jones OVER 1.5 three-pointers @ +130 (draftkings) | Strong edge (+21.1% est.) | Positive EV; same availability caveat. |
| LEAN | WAS @ CLE | A | Jarrett Allen OVER 28.5 points+rebounds+assists @ -104 (fanduel) | Strong edge (+21.0% est.) | High modeled probability but injury/role not fully clean. |
| LEAN | IND @ BKN | B | Kam Jones OVER 13.5 points @ -104 (fanduel) | Strong edge (+20.9% est.) | Similar reasons — lean only. |
| NO-GO | WAS @ CLE | B | Carlton Carrington UNDER 2.5 rebounds @ +124 (draftkings) | Watch: roster_gate | Player not confirmed on roster — do not bet. |
| NO-GO | LAC @ HOU | B | Derrick Jones UNDER 2.5 rebounds @ +123 (draftkings) | Watch: roster_gate | Roster check failed — no bet. |
| NO-GO | IND @ BKN | A | Jarace Walker UNDER 5.5 rebounds @ +111 (draftkings) | Watch: injury_gate | Player injury risk too high — no bet. |

Order: GO first (none), then LEAN, then NO-GO. If roster or injury status clears positively pregame and you accept model assumptions, consider moving LEANs toward small bets.



<!-- pagebreak -->

## Game Cards by Matchup

### WAS @ CLE
- Tip (ET): `07:10 PM ET`
- Listed plays: `5`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| LEAN | A | Jarrett Allen OVER 28.5 points+rebounds+assists @ -104 (fanduel) | Strong edge (+21.0% est.), Higher stake | Lean only: edge exists (EV 0.210) with non-clean injury status. |
| LEAN | A | Tre Johnson OVER 14.5 points @ -102 (fanduel) | Strong edge (+19.0% est.), Higher stake | Lean only: edge exists (EV 0.190) with non-clean injury status. |
| LEAN | A | Donovan Mitchell OVER 3.5 three-pointers @ +130 (fanduel) | Strong edge (+18.9% est.), Higher stake | Lean only: edge exists (EV 0.189) with non-clean injury status. |
| LEAN | A | Alex Sarr OVER 26.5 points+rebounds+assists @ -105 (draftkings) | Strong edge (+18.8% est.), Higher stake | Lean only: edge exists (EV 0.188) with non-clean injury status. |
| LEAN | A | Jaylon Tyson OVER 13.5 points @ +102 (fanduel) | Strong edge (+18.7% est.), Higher stake | Lean only: edge exists (EV 0.187) with non-clean injury status. |

<!-- pagebreak -->

### NYK @ PHI
- Tip (ET): `07:30 PM ET`
- Listed plays: `1`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| LEAN | A | Landry Shamet OVER 2.5 three-pointers @ +124 (fanduel) | Strong edge (+18.2% est.), Higher stake | Lean only: edge exists (EV 0.182) with non-clean injury status. |

<!-- pagebreak -->

### IND @ BKN
- Tip (ET): `07:40 PM ET`
- Listed plays: `5`

| Action | Tier | Ticket | Edge Note | Why |
| --- | --- | --- | --- | --- |
| LEAN | A | Jay Huff OVER 13.5 points @ +102 (fanduel) | Strong edge (+24.0% est.), Higher stake | Lean only: edge exists (EV 0.240) with non-clean injury status. |
| LEAN | B | Kam Jones OVER 22.5 points+rebounds+assists @ -113 (fanduel) | Strong edge (+21.8% est.), Higher stake | Lean only: edge exists (EV 0.218) with non-clean injury status. |
| LEAN | A | Kam Jones OVER 1.5 three-pointers @ +130 (draftkings) | Strong edge (+21.1% est.), Higher stake | Lean only: edge exists (EV 0.211) with non-clean injury status. |
| LEAN | B | Kam Jones OVER 13.5 points @ -104 (fanduel) | Strong edge (+20.9% est.), Higher stake | Lean only: edge exists (EV 0.209) with non-clean injury status. |
| LEAN | B | Kam Jones OVER 21.5 points+rebounds+assists @ -121 (draftkings) | Strong edge (+20.2% est.), Higher stake | Lean only: edge exists (EV 0.202) with non-clean injury status. |

<!-- pagebreak -->

## Data Quality
Known limitations (plain):  
- Minutes/usage projections use deterministic rules (v0), not probabilistic distributions — projections can be brittle, especially on role changes.  
- Correlation rules for multi-leg/SGP exposure use deterministic haircuts (v0).  
- Model probabilities are market-implied with adjustments for injury/roster/opponent, but many lines still show "unknown" injury status. Data is therefore mixed quality for late-breaking availability. If roster/injury info is missing or weak, that increases risk.

## Confidence
Overall confidence: Moderate for direction (model consistently finds overs) but Low-to-Moderate for execution because of roster/injury uncertainty and deterministic usage assumptions. Tier A lines preferred over Tier B when sizing. If you act, use reduced stake sizes on LEAN plays and avoid NO-GO tickets.
