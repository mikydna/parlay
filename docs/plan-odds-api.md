# Plan: Odds API Integration (Free-Tier Safe)

This is a Codex-ready implementation plan + megaprompt sequence to integrate The Odds API v4 into this repo, replacing odds scraping and making reruns cache-first so we don't burn free-tier credits.

## Constraints (Non-Negotiable)

- Stay within the free plan: 500 credits/month. See plans: https://the-odds-api.com/
- Never store `ODDS_API_KEY` in files or logs.
- All network fetches must be cache-aware:
  - If a prior snapshot exists for the same request, reuse it.
  - Require an explicit `--refresh` (or similar) to refetch.

## Relevant Odds API Facts (v4)

Primary docs: https://the-odds-api.com/liveapi/guides/v4/

### Endpoints We Care About

- List sports (free):
  - `GET /v4/sports?apiKey=...`
  - Usage: does not count against quota.
- List events for a sport (free):
  - `GET /v4/sports/{sport}/events?apiKey=...`
  - Supports `commenceTimeFrom` / `commenceTimeTo` filters (ISO8601).
  - Usage: does not count against quota.
- Featured markets (slate spreads/totals/moneyline):
  - `GET /v4/sports/{sport}/odds?apiKey=...&regions=...&markets=...`
  - Featured markets only: `h2h`, `spreads`, `totals`, `outrights`.
  - Usage cost: `1 credit per region per market`.
- Event odds (any markets, including player props; 1 event at a time):
  - `GET /v4/sports/{sport}/events/{eventId}/odds?apiKey=...&regions=...&markets=...`
  - Usage cost: `[unique markets returned] x [regions specified]`.
  - If you request 5 markets but only 2 are available, cost is 2 (per region).
- Event markets (optional discovery; costs credits):
  - `GET /v4/sports/{sport}/events/{eventId}/markets?apiKey=...&regions=...`
  - Usage cost: `1 credit` per call.

### Key Query Params

- `regions` vs `bookmakers`:
  - `regions` returns all bookmakers in a region.
  - `bookmakers` (comma-separated) takes priority over `regions` if both provided.
  - Quota equivalence: every group of 10 bookmakers counts as 1 region.
- `oddsFormat`: use `american` for US workflow.
- `dateFormat`: use `iso`.
- `commenceTimeFrom`, `commenceTimeTo`: filter to today's slate window.
- `eventIds`: filter to known events (helps payload size; cost unchanged).
- `includeLinks=true`, `includeSids=true`: include deep links + source ids when available.

Deep links announcement: https://the-odds-api.com/releases/deep-links.html

### Update Intervals

Update intervals vary by market type. For NBA player props ("additional markets"), pre-match and in-play update interval is 60 seconds.

Reference: https://the-odds-api.com/sports-odds-data/update-intervals.html

### Market Keys (NBA player props)

Reference: https://the-odds-api.com/sports-odds-data/betting-markets.html

Core set to start (free-tier friendly):
- `player_points`
- `player_rebounds`
- `player_assists`
- `player_threes`
- `player_points_rebounds_assists`

Rule reminder: player props are NOT supported on the featured `/odds` endpoint; use the per-event `/events/{eventId}/odds` endpoint.

Reference (invalid market example): https://the-odds-api.com/liveapi/guides/v4/api-error-codes.html

### Quota Tracking (Must Implement)

Every response includes:
- `x-requests-remaining`
- `x-requests-used`
- `x-requests-last`

We should persist these headers for each call into a local usage ledger.

## Correct Call Patterns (NBA)

Sport key:
- Use `basketball_nba` (or whatever the `/sports` endpoint returns for NBA).

Two-step flow (no scraping):
1. Enumerate events (free):
   - `GET /v4/sports/basketball_nba/events?apiKey=...&commenceTimeFrom=...&commenceTimeTo=...`
   - Save the response into the snapshot so reruns don't change the event set.
2. Fetch odds:
   - Slate snapshot (cheap): `GET /v4/sports/basketball_nba/odds?...&markets=spreads,totals`
   - Player props (per-event): `GET /v4/sports/basketball_nba/events/{eventId}/odds?...&markets=player_points`

Example curl (sanitized; key via env):
```bash
# Events (free)
curl -sS \
  "https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey=$ODDS_API_KEY&dateFormat=iso&commenceTimeFrom=2026-02-11T00:00:00Z&commenceTimeTo=2026-02-12T08:00:00Z"

# Featured slate (2 credits if regions=us and markets=spreads,totals)
curl -sS \
  "https://api.the-odds-api.com/v4/sports/basketball_nba/odds?apiKey=$ODDS_API_KEY&regions=us&markets=spreads,totals&oddsFormat=american&dateFormat=iso&commenceTimeFrom=2026-02-11T00:00:00Z&commenceTimeTo=2026-02-12T08:00:00Z"

# Per-event props (cost ~= 1 credit per event per market returned)
curl -sS \
  "https://api.the-odds-api.com/v4/sports/basketball_nba/events/<EVENT_ID>/odds?apiKey=$ODDS_API_KEY&regions=us&markets=player_points&oddsFormat=american&dateFormat=iso&includeLinks=true&includeSids=true"
```

## Price Shopping + Tiering (Within One Feed)

Definition (for storage/derived tables; still no modeling):
- For each `(event_id, market_key, outcome_key, point)`:
  - Tier A: >=2 distinct bookmakers with prices for that exact line.
  - Tier B: exactly 1 bookmaker (requires stricter EV thresholds later).

Persist both:
- Raw per-book quotes (audit trail + backtesting from snapshots).
- Derived best-of-book (best Over price, best Under price, source `last_update`, and deep link if present).

## Credit Budgeting (Practical Defaults)

Free plan is 500 credits/month. Avoid designs that do `N_games x N_markets` every rerun.

Useful approximations (1 region):
- Slate featured snapshot with `spreads,totals` costs 2 credits total.
- Props snapshot costs `(#events) x (#markets returned)` credits.

NBA example:
- 10 games, 1 prop market (e.g., `player_points`) => ~10 credits per snapshot.
- 10 games, 5 prop markets => ~50 credits per snapshot (too expensive to do frequently on free tier).

Recommended defaults:
- Always do a cheap daily slate snapshot: `markets=spreads,totals` on `/odds`.
- Start props with 1 market max across all events (default `player_points`), or restrict props to selected eventIds.
- Default to cache-first; require `--refresh` to spend credits.
- Add a per-run credit ceiling (e.g., 20) and abort unless `--force`.

## Storage + Caching Contract (Snapshot-First)

Goal: once we pay credits for a dataset, we can rerun parsing/modeling offline without re-fetching.

Robustness requirements:
- Snapshot runs must be resumable without re-spending credits.
- Writes must be atomic (write temp file, then rename).
- Concurrent runs must not corrupt snapshots (use a lock file).
- Every network call must produce a cache artifact (request/response/meta) or fail fast before spending credits.

Directory layout:

- `data/odds_api/`
  - `snapshots/`
    - `{snapshot_id}/` (e.g., `2026-02-11T15-00-00Z`)
      - `manifest.json`
      - `requests/` (sanitized request descriptors)
      - `responses/` (raw JSON payloads)
      - `meta/` (response headers + timings + status)
  - `usage/`
    - `usage-YYYY-MM.jsonl` (append-only ledger from response headers)

Snapshot id rules:
- Use UTC ISO-ish folder name with `:` replaced (filesystem safe), e.g. `2026-02-11T15-00-00Z`.
- A "rerun" should take `--snapshot {snapshot_id}` and never call the network.

Cache key rules:
- Compute a deterministic hash from:
  - endpoint path (no host)
  - sorted query params (excluding `apiKey`)
  - request body (should be empty for GET)
- Use that hash for filenames:
  - `requests/{hash}.json`
  - `responses/{hash}.json`
  - `meta/{hash}.json`

Manifest (`manifest.json`) should include:
- `snapshot_id`, `created_at_utc`
- `schema_version` (manifest + storage layout)
- `client_version` (package version) and `git_sha` (if available)
- list of request hashes and friendly labels (`slate_odds`, `event_odds:<event_id>`)
- consolidated quota headers after the run (`remaining`, `used`)
- run config: sport, markets, regions/bookmakers, time window
- per-request status summary: `ok`, `cached`, `skipped`, `failed`

Git hygiene:
- Add `data/` to `.gitignore`.

## Robust Defaults (Future-Proof)

These are deliberate design choices to avoid rewrites later:

- Provider-agnostic internal records:
  - All derived rows include `provider` (e.g., `odds_api`) so a second odds feed can be added later.
- Offline-first reruns:
  - Any command that can operate from a snapshot must accept `--snapshot-id` and an `--offline` switch that forbids network.
- Partial failure tolerance:
  - A props snapshot should be able to succeed even if some events/markets are missing, but must record failures clearly and support `--resume`.
- Credit governance:
  - Add `--max-credits` (default), `--force`, and a hard stop on unknown/unsupported market keys.
  - Add `--max-events` and allow selecting a subset of events to stay within budget.
- Deterministic derived outputs:
  - Stable sorting, explicit schema version, and no non-deterministic fields (except timestamps that are recorded in meta).

## Megaprompt Sequence (Codex-Ready)

Use these prompts in order. Each prompt should end with local verification commands and should not introduce unrelated business logic (no modeling yet).

### Prompt 1: Data contract + cache primitives

You are implementing a cache-first snapshot store for The Odds API responses.

Repo: `nba-prop-ev` (Python 3.12, `uv`, `src/` layout).

Requirements:
1. Create a `data/odds_api/` snapshot contract as described in this doc.
2. Add `.gitignore` rules for `data/` and any snapshot artifacts.
3. Add `src/prop_ev/storage.py` (or similar) that:
   - builds snapshot dirs
   - computes request hash keys (exclude `apiKey`)
   - writes `requests/*.json`, `responses/*.json`, `meta/*.json`, `manifest.json` atomically
   - takes an exclusive lock for a snapshot id while writing
   - loads cached responses by hash
4. Add tests that:
   - verify hashing is stable and excludes secrets
   - verify cache hit prevents a "fetch" path from being called (use dependency injection / stubs)
   - verify atomic write behavior (no partial files on exceptions)

Verify:
- `make ci` passes.

### Prompt 2: Odds API client with quota guard rails

You are implementing an HTTP client for The Odds API v4 with strict credit safeguards and cache integration.

Requirements:
1. Add `src/prop_ev/odds_client.py`:
   - uses `httpx` and `tenacity` for retries on 429/5xx
   - respects `Retry-After` on 429 when present
   - sets conservative connection limits to avoid bursty retries
   - pulls config from `Settings` (`ODDS_API_KEY`, base url, timeout)
   - supports `regions` OR `bookmakers`
   - supports `includeLinks` and `includeSids`
2. Implement methods (sync or async, but be consistent):
   - `list_sports()` (free endpoint)
   - `list_events(sport_key, commence_from, commence_to)` (free endpoint)
   - `get_featured_odds(sport_key, markets, regions/bookmakers, commence_from/to, event_ids=...)`
   - `get_event_odds(sport_key, event_id, markets, regions/bookmakers, includeLinks/includeSids)`
3. Add a "credit estimator" for planned runs:
   - featured `/odds`: `len(markets) * regions_equiv`
   - event odds: worst-case `len(markets) * regions_equiv` per event
4. Add runtime guard rails:
   - `--max-credits` default (ex: 20)
   - abort before network if estimated cost exceeds cap, unless `--force`
   - always persist response headers (`x-requests-*`) into `data/odds_api/usage/`
   - record per-request metadata: endpoint, params (sanitized), status code, duration, retry count
5. Cache behavior:
   - By default, check snapshot cache; if present, return it and skip network.
   - Add `--refresh` to force fetch and overwrite cache entry within a snapshot.

Verify:
- `make ci` passes.

### Prompt 3: CLI commands for snapshots (no modeling)

You are adding CLI commands that produce deterministic snapshot folders.

Requirements:
1. Replace the CLI stub with argparse subcommands:
   - `prop-ev snapshot slate`:
     - gets featured odds for `basketball_nba` with default `markets=spreads,totals`
     - writes a snapshot folder + manifest
   - `prop-ev snapshot props`:
     - uses events endpoint to list today's events
     - fetches per-event odds for default `markets=player_points`
     - bounded concurrency (if async) to avoid rate limiting
2. The CLI must support:
   - `--snapshot-id` (otherwise generate current UTC)
   - `--commence-from`, `--commence-to` (ISO8601)
   - `--regions` OR `--bookmakers`
   - `--include-links`, `--include-sids`
   - `--max-credits`, `--force`, `--refresh`
   - `--offline` (forbid network; error if cache miss)
   - `--resume` (fetch only missing/failed requests in an existing snapshot)
   - `--max-events` (cap per-event requests for free tier)
3. Output should show:
   - estimated credits before execution
   - actual `x-requests-last` per call
   - remaining credits at end (from latest header)
   - succeeded/cached/failed counts and an explicit exit code on partial failure

Verify:
- `uv run prop-ev snapshot slate --dry-run` works (no network).
- `make ci` passes.

### Prompt 4: Normalization (minimal, for reuse)

You are implementing minimal extraction so later modeling doesn’t have to parse raw JSON repeatedly.

Requirements:
1. Add `src/prop_ev/normalize.py` with pure functions that:
   - take raw featured odds JSON and output a compact JSONL table (`game_id`, `market`, `book`, `price`, `point`, `last_update`)
   - take raw event odds JSON and output a compact JSONL table (`event_id`, `market`, `player`, `side`, `price`, `point`, `book`, `last_update`)
2. Write derived outputs into:
   - `data/odds_api/snapshots/{snapshot_id}/derived/*.jsonl`
3. Keep it deterministic and side-effect free beyond file writes initiated by CLI.
4. Include `provider` and `snapshot_id` fields in derived outputs.

Verify:
- `make ci` passes.

### Prompt 5: Documentation + operating mode

You are documenting how to run this safely on the free plan.

Requirements:
1. Update README with:
   - credit math examples
   - recommended defaults (daily slate + 1 prop market)
   - how to re-run offline from a saved snapshot id
2. Add a short doc:
   - `docs/credits.md` with run budgets and example schedules that fit 500 credits/month.

Verify:
- `make ci` passes.

### Prompt 6 (Recommended): Snapshot inspection + diff (offline tools)

You are adding offline utilities so future debugging/backfills don't require re-fetching.

Requirements:
1. Add CLI commands that do not call the network:
   - `prop-ev snapshot ls` (list snapshot ids + summary from manifest)
   - `prop-ev snapshot show --snapshot-id ...` (print manifest summary + request status counts)
   - `prop-ev snapshot diff --a ... --b ...` (report changes in derived outputs; no modeling)
2. Tests:
   - create a tiny synthetic snapshot folder in `tmp_path` and verify these commands work offline.

Verify:
- `make ci` passes.

### Prompt 7 (Optional): Provider interface for future multi-feed price shopping

You are isolating provider-specific code so a second odds provider can be added later without rewriting storage/normalization.

Requirements:
1. Define a small `OddsProvider` protocol (or ABC) in `src/prop_ev/providers/base.py`.
2. Implement `OddsAPIProvider` in `src/prop_ev/providers/odds_api.py` wrapping the client methods.
3. Keep storage, normalization, and CLI provider-agnostic (select provider via CLI flag).

Verify:
- `make ci` passes.

### Prompt 8 (Recommended): Schema guards + golden fixtures

You are making the pipeline resilient to upstream payload changes without over-modeling the entire API.

Requirements:
1. Add lightweight validation for the fields we actually consume in normalization:
   - ensure required keys exist and have expected types
   - fail with a clear error that points to the request hash + snapshot id
2. Add "golden fixture" tests:
   - store small synthetic sample payloads under `tests/fixtures/` (no real keys, no real bets)
   - assert normalization output shape and stable sorting
3. Add a `schema_version` for derived outputs and write it into derived headers (or a separate `derived_manifest.json`).

Verify:
- `make ci` passes.

### Prompt 9 (Optional): Credit reporting + budgets

You are adding tooling to keep the free tier predictable over time.

Requirements:
1. Add `prop-ev credits report`:
   - reads `data/odds_api/usage/usage-YYYY-MM.jsonl`
   - prints total credits used, remaining (if available), and top endpoints by spend
2. Add `prop-ev credits budget`:
   - given `--events N --markets ... --regions ...`, prints an estimated cost and recommended `--max-credits`

Verify:
- `make ci` passes.

### Prompt 10 (Optional): Snapshot integrity + compression

You are hardening the snapshot store so it can be trusted as a long-lived source of truth.

Requirements:
1. Add integrity checks:
   - write `sha256` for each request/response/meta artifact into the manifest
   - add `prop-ev snapshot verify --snapshot-id ...` to validate hashes and required files
2. Add optional compression for large payloads:
   - support writing `responses/*.json.gz` (gzip) with transparent read
   - keep `requests/*.json` and `meta/*.json` uncompressed for inspectability
3. Add retention helpers (non-destructive by default):
   - `prop-ev snapshot prune --keep N --dry-run` shows what would be deleted
   - require explicit `--apply` to actually delete

Verify:
- `make ci` passes.

## Suggested Free-Tier Operating Pattern

- Daily (or on-demand) cheap slate:
  - `/odds` with `markets=spreads,totals`, `regions=us`, cache-first.
- Props:
  - 1 market across all events (default `player_points`) OR a small set of eventIds.
  - Only refresh near decision time; keep snapshots and reuse them during analysis.
