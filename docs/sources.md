# Data Source Policy

## Odds Source (Hard Requirement)

- Odds and props data must come from **The Odds API v4** only.
- Do not switch to another odds provider in normal operation.
- Do not introduce paid replacement providers.

## Injury Source Policy

- Official NBA injury report pages and linked PDF files are authoritative for official injury source health.
- A healthy official injury source requires:
  - official page fetch succeeds,
  - injury-report PDF link extraction is non-empty,
  - cached metadata is valid.
- Secondary injury feeds are fallbacks only and must be labeled as fallback provenance.
- LLM/web search output is never an authoritative injury status source.

## Roster Source Policy

- Current default primary roster path is the existing in-repo pipeline (`nba_live_scoreboard` / boxscore path).
- Existing secondary fallback is ESPN roster + injury-derived inactive merge.
- `nba_api commonteamroster` should only be promoted to primary if audit evidence shows the current primary fails frequently.
- Any source promotion must include before/after reliability metrics and explicit provenance labels.

## Conflict Resolution

- Odds provider conflicts: prefer The Odds API v4 (only allowed provider).
- Injury conflicts: official NBA injury report source wins.
- Roster conflicts: configured primary source wins; fallback source values are accepted only for uncovered teams and must be marked as fallback-derived.
- If required health checks fail, classify as degraded or broken and gate downstream usage.

## Fallback Triggers and Escalation

- Injury fallback trigger: official source unavailable or invalid.
- Roster fallback trigger: teams missing from primary roster coverage.
- Degraded mode:
  - stale inputs,
  - roster fallback used,
  - unknown roster entries remain.
- Broken mode:
  - official injury check fails,
  - roster fetch/check fails for teams in scope,
  - event mapping fails for snapshot events.
- Escalation:
  1. Refresh context.
  2. Re-run health checks.
  3. If still degraded/broken across repeated snapshots, evaluate source replacement using measured evidence.

## Health Command

Command:

```bash
uv run prop-ev strategy health --snapshot-id <SNAPSHOT_ID> --offline
```

Optional flags:

```bash
uv run prop-ev strategy health --snapshot-id <SNAPSHOT_ID> --refresh-context
uv run prop-ev strategy health --snapshot-id <SNAPSHOT_ID> --json
```

Output contract (JSON):

- `status`: `healthy|degraded|broken`
- `exit_code`: `0|1|2`
- `snapshot_id`
- `checks`: pass/fail details for injury source, roster source, mapping, freshness
- `counts`: `unknown_event`, `unknown_roster`, `missing_injury`, `stale_inputs`
- `gates`: triggered reasons
- `source_details`: injuries/roster/mapping/odds freshness details
- `recommendations`: action list

Exit semantics:

- `0` healthy: all required checks pass.
- `1` degraded: watchlist-only recommended; required checks pass but quality is reduced.
- `2` broken: required checks failed; do not produce picks.
