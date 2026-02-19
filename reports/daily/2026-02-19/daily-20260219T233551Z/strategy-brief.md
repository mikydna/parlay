## Snapshot
- snapshot_id: `daily-20260219T233551Z`
- modeled_date_et: `Thursday, Feb 19, 2026 (ET)`
- generated_at_et: `2026-02-19 06:36:00 PM EST`
- source_data: `snapshot_inputs`
- scoring: `deterministic`
- narrative: `llm`
Generated: 2026-02-19 06:36:00 PM ET (America/New_York)  
Modeled date: Thursday, Feb 19, 2026 (ET)  
Execution_books: []  

Top plays (tip times ET):
- Jarace Walker OVER 27.5 points+rebounds+assists — IND @ WAS — tip: 07:10 PM ET — ticket: Jarace Walker OVER 27.5 points+rebounds+assists @ -104 (fanduel)  
- Jalen Green OVER 14.5 points — PHX @ SAS — tip: 08:40 PM ET — ticket: Jalen Green OVER 14.5 points @ +100 (fanduel)

Note: This brief covers player-prop over/under betting only (not game-winner or spread bets).

## What The Bet Is
Two A-tier player-prop over plays (both recommended as GO in the model payload):
- Jarace Walker — Over 27.5 points+rebounds+assists (IND @ WAS) — book: fanduel — price: -104  
- Jalen Green — Over 14.5 points (PHX @ SAS) — book: fanduel — price: +100

Both tickets are player-prop over/under wagers (player totals), not game outcome bets.

## Executive Summary
- Model recommends GO on two A-tier player props with sizable estimated positive EVs and verified active roster status in the input.  
- Jarace Walker 3-stat over: model_prob 0.6932, calibrated 0.6423, EV ~ +36.0% (best_ev 0.3598). Tip 07:10 PM ET.  
- Jalen Green points over: model_prob 0.6862, calibrated 0.6359, EV ~ +37.2% (best_ev 0.3725). Tip 08:40 PM ET.  
- Data gaps exist for other candidates (watchlist and tier-B items) where roster/injury checks failed — those are NO-GO until confirmed. Uncertainty bands ~0.05 suggest moderate model variance; size bets accordingly.


<!-- pagebreak -->

## Analyst Take

- mode: `llm_web`

### Read This First

Checked injury reports, team news, and recent game logs for the two top plays (Jalen Green O14.5 pts, Jarace Walker O27.5 3-stat). Evidence is mixed for Jalen Green: some outlets list him available/off the report for the Suns vs Spurs game (supporting the model edge that he’ll play and can reach 14.5), while multiple injury updates and historical hamstring setbacks suggest limited floor and managed minutes (which...

### News Signals

- Supports:
  - [S1] Fox Sports' February 19 Suns vs Spurs injury report lists Jalen Green as 'off the injury report' (available) for the Feb 19 game, indicating he was expected to be in the lineup.
  - [S2] NBA.com player news shows Jalen Green on the Suns roster with recent game activity and multiple status updates (questionable / not listed), consistent with the player being active in recent team reports.
  - [S3] Jarace Walker posted a 23-point, 5-assist effort on Feb 11, 2026 (game log), demonstrating he can put up a large counting-stats performance when given extended minutes/usage.
  - [S4] Recent Pacers vs Wizards previews and injury reports list Walker as an active rotational starter for Indiana and show he averages meaningful minutes, supporting the model's assumption that he will have opportunity to accumulate counting stats.
- Refutes/Risks:
  - [S5] Multiple news pieces document Jalen Green's hamstring issues (including reports he aggravated the hamstring and was out for re-evaluation), indicating ongoing injury risk and potential minutes/usage management that could depress scoring volume.
  - [S6] Analyses and team-updates (Bright Side of the Sun and others) report hamstring setbacks and re-evaluations for Green, including multi-week re-evaluation timetables earlier in the season, which undermines confidence that he will play normal minutes needed to hit scoring lines reliably.
  - [S7] Jarace Walker's 2025-26 season averages (approx. 10.5 points, 4.5 rebounds, 2.0 assists = ~17 combined) make a 27.5 combined line a substantial outlier versus his typical production.
  - [S8] Multiple Pacers/Wizards injury-report previews note several team injuries and line-up fluctuations; those roster instabilities can both help (more opportunity) or hurt (rotations change, minutes reduced) Walker's ability to reach an unusually high 27.5 3-stat total—introducing uncertainty rather than a clear push toward the model's optimistic EV.

### Bottom Line

- **Best Bet:** **Jalen Green OVER 14.5 points @ +100 (fanduel)**
- **Lookup Line:** `PHX @ SAS` | `OVER 14.5 points` | `fanduel +100`
Evidence is mixed. For Jalen Green O14.5: there is explicit, same-day reporting (Fox Sports, NBA player news) that he was off/questionable on the injury report and likely available, which supports the model edge that the line is playable if he actually plays starter minutes.

### Source Index

- [S1] Phoenix Suns vs. San Antonio Spurs - Live Score - February 19, 2026 | FOX Sports (www.foxsports.com)
- [S2] Jalen Green | Guard | Phoenix Suns | NBA.com (www.nba.com)
- [S3] Jarace Walker 2025-26 Basic Game Log - NBA Players Stats (Land of Basketball) (www.landofbasketball.com)
- [S4] Pacers vs. Wizards Injury Report – Feb. 19 | Fox Sports 1360 (foxsports1360.iheart.com)
- [S5] Suns’ Jalen Green leaves game vs. Clippers, apparently aggravating hamstring injury | NBA.com (www.nba.com)
- [S6] Injury Update: Jalen Green leaves game with hamstring tightness | Bright Side of the Sun (www.brightsideofthesun.com)
- [S7] Jarace Walker Profile and 2025-26 season averages - Land of Basketball (www.landofbasketball.com)
- [S8] Pacers vs Wizards injury report for February 19 - TalkBasket.net (www.talkbasket.net)
- Full source URLs are stored in `brief-analyst.json` for audit/debug use.

<!-- pagebreak -->

## Best Available Bet Right Now

- **Decision:** **GO** (meets clean GO gates in this snapshot.)
- **Bet:** **Jalen Green OVER 14.5 points @ +100 (fanduel)**
- **Lookup Line:** `PHX @ SAS` | `OVER 14.5 points` | `fanduel +100`
- **Model Edge:** `+37.2% est.`
- why: Go: verified availability with tier A market depth and positive edge (EV 0.372).

<!-- pagebreak -->

## Action Plan (GO / LEAN / NO-GO)

### Top 2 Across All Games

| Action | Game | Tier | Ticket | p(hit) | Edge Note | Why |
| --- | --- | --- | --- | --- | --- | --- |
| GO | PHX @ SAS | A | Jalen Green OVER 14.5 points @ +100 (fanduel) | 63.6% | Strong edge (+37.2% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.372). (tier=A; injury=available_unlisted; roster=active; quality=69.8%; uncertainty=5.0%) |
| GO | IND @ WAS | A | Jarace Walker OVER 27.5 points+rebounds+assists @ -104 (fanduel) | 64.2% | Strong edge (+36.0% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.360). (tier=A; injury=available_unlisted; roster=active; quality=68.9%; uncertainty=5.1%) |

Order: GO first, then NO-GO. There are no LEAN recommendations in the provided payload.



<!-- pagebreak -->

## Game Cards by Matchup

### IND @ WAS
- Tip (ET): `07:10 PM ET`
- Listed plays: `1`

| Action | Tier | Ticket | p(hit) | Edge Note | Why |
| --- | --- | --- | --- | --- | --- |
| GO | A | Jarace Walker OVER 27.5 points+rebounds+assists @ -104 (fanduel) | 64.2% | Strong edge (+36.0% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.360). |

<!-- pagebreak -->

### PHX @ SAS
- Tip (ET): `08:40 PM ET`
- Listed plays: `1`

| Action | Tier | Ticket | p(hit) | Edge Note | Why |
| --- | --- | --- | --- | --- | --- |
| GO | A | Jalen Green OVER 14.5 points @ +100 (fanduel) | 63.6% | Strong edge (+37.2% est.), Higher stake | Go: verified availability with tier A market depth and positive edge (EV 0.372). |

<!-- pagebreak -->

## Data Quality
- Strengths: Provided payload includes model probabilities, calibrated probabilities, uncertainty bands, and roster_status for top A-tier plays. Injuries flagged from official/secondary sources per summary.  
- Limitations (present in payload): execution_books list is empty (no additional book scanning beyond listed best_book). Some players in watchlist/tier-B have injury_status=unknown or roster_status=not_on_roster, so those plays were correctly gated. Model uses deterministic minutes/usage and deterministic SGP correlations (not learned distributions), which may understate real-world variance. Uncertainty_band values (~0.05) are non-negligible.

If you need larger stakes or added plays, confirm live roster/injury feeds and line shopping across books (execution_books currently empty).

## Confidence
- Overall confidence for the two recommended A-tier plays: moderate-high based on provided payload (roster_status=active, Tier A market depth, calibrated probabilities still > line-implied).  
- Caveats: uncertainty_band ≈ 0.05 reduces confidence on tight lines; missing external book checks and the deterministic modeling choices lower confidence versus a fully cross-checked live workflow. If data is missing or weak for any ticket, that is stated above and those tickets are NO-GO until confirmed.

### Interpreting p(hit)

- `p(hit)` = estimated chance the recommended side wins at that line.
- When shown as `X% → Y%`, it is conservative `p(hit)` mapped through calibration history.
- Built from no-vig odds + small injury/roster/spread adjustments (clamped 1%-99%).
- Use it to rank EV, not as a guarantee; judge it by calibration over many bets.
- Can be wrong when odds are stale, coverage is thin, or minutes/role are uncertain.
