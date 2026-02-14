# Plan: SOTA-Grade NBA Props Pipeline (Canonical)

Date: 2026-02-13

This is the single source of truth for architecture, milestones, and execution order.

## 1) Product Goal

Build a production-ready NBA props pipeline that is:

1. **Actionable**: picks are executable at configured books (DraftKings/FanDuel by default).
2. **Replayable**: every decision is reproducible from saved snapshots, offline.
3. **Spend-safe**: no accidental paid usage outside explicit policy.
4. **Auditable**: stable reason codes, stable JSON artifacts, deterministic reruns.
5. **Measurable**: strategy changes are promoted only through backtest + calibration evidence.

## 2) Non-Negotiables

- No lookahead; cutoff-time snapshots define available information.
- Market baseline first; ML (if used) is a correction layer, not replacement.
- Execution realism: rank/select by execution-book prices, not “best across all books.”
- Snapshot-first everywhere:
  - `--offline` forbids all network.
  - `--no-spend` / `--max-credits 0` forbids paid cache misses.
- Contract stability:
  - JSON/CLI schemas stay stable unless explicitly versioned.
  - every gate emits explicit reason codes.

## 3) Current Baseline (as of 2026-02-13)

- Odds data plane is implemented in `/Users/andy/Documents/Code/parlay/src/prop_ev/odds_data/`:
  - day-index backfill/status,
  - global request cache,
  - spend policies.
- Stage 0/1 contract hardening landed:
  - typed day-index error/status semantics (`error_code`, `reason_codes`, `status_code`),
  - canonical QuoteTable module + validation,
  - contract verification commands (`snapshot verify --check-derived`, `data verify`).
- Strategy plugin framework exists (`s001`–`s008`) with compare + backtest summarization.
- `nba-data` historical lake is available with clean parquet + verify.
- `parlay-data` contains live datasets used by this repo:
  - NBA lake (clean/manifests/raw archives),
  - Odds historical day-index dataset (`bdfa890a...`, 2026-01-22 to 2026-02-12).
- Scheduled flow guard exists for empty slates (`--exit-on-no-games`).

## 4) Pipeline Architecture (Stage Model)

The pipeline is intentionally stage-based so each stage has explicit input/output contracts.

### Stage Delivery Matrix (current state)

| Stage | Scope | Status | What is already landed | Remaining gap to target | Recommended parallel worktree |
| --- | --- | --- | --- | --- | --- |
| 0 | Acquire/cache/day-index | Done (contract-locked) | cache store, snapshot persistence, spend-policy controls, historical day-index backfill/status, typed completeness/error semantics, `data verify` contract checks | expand operator dashboards for long-range dataset health summaries | `wt-data-plane` |
| 1 | Normalize QuoteTable | Done (contract-locked) | canonical QuoteTable module, deterministic normalization, schema validation, `snapshot verify --check-derived`, JSONL/parquet contract parity checks | extend same strict contract style to additional derived tables as they are introduced | `wt-quote-normalize` |
| 2 | De-vig + quality signals | Partial | implied/no-vig baseline logic exists in strategy path and plugins | extract into dedicated pricing module with explicit per-book quality outputs and contract tests | `wt-pricing-neutralize-vig` |
| 3 | Reference probability model | Partial | median no-vig style strategies (`s003+`) exist | add alt-line monotone interpolation + uncertainty estimation artifacts | `wt-ref-model-altline` |
| 4 | Execution pricing + EV | Partial | execution-vs-discovery flow exists and is reported | centralize exact-point matching + conservative EV scoring as first-class stage output | `wt-execution-pricing` |
| 5 | Eligibility gates | Partial | context/freshness/gate reasons already emitted in current reports | unify gate contracts and ensure all path decisions map to stable reason enums | `wt-gates-contracts` |
| 6 | Portfolio + `ExecutionPlan` | Not started | ranked plays exist, but no canonical execution-plan artifact | implement deterministic portfolio selector and `execution-plan.json` contract | `wt-execution-plan` |
| 7 | Render/publish | Partial | strategy/brief/publish flows and latest mirrors exist | align all published outputs to compact contract and deterministic rerender diff policy | `wt-report-publish` |
| 8 | Settle/evaluate | Partial | settlement + backtest summary commands exist | add promotion-ready scoreboard package (ROI + Brier + calibration + actionability + CLV proxy) | `wt-eval-scoreboard` |

### Parallel worktree strategy (explicit)

Use separate git worktrees for independent milestone lanes to reduce merge contention in large files
like CLI/strategy modules.

Recommended lanes:

1. **Pricing lane (neutralize vig)**  
   - Focus: Stage 2 (`de-vig + quality signals`) and shared pricing primitives.  
   - Worktree: `wt-pricing-neutralize-vig`.  
   - Merge contract: expose pure deterministic functions + tests; avoid CLI contract changes in same PR.

2. **Minutes/usage lane**  
   - Focus: Stage 5/M5 calibration inputs from `nba-data` (separate from odds pricing internals).  
   - Worktree: `wt-minutes-usage-model`.  
   - Merge contract: produce versioned feature/output artifact; no direct mutation of core EV baseline logic until lift is proven.

3. **Execution-plan lane**  
   - Focus: Stage 6 (`execution-plan.json` + portfolio constraints).  
   - Worktree: `wt-execution-plan`.  
   - Merge contract: stable artifact schema, deterministic selection, reason-code coverage.

4. **Evaluation lane**  
   - Focus: Stage 8 scoreboard/promotion policy.  
   - Worktree: `wt-eval-scoreboard`.  
   - Merge contract: standardized metrics artifact consumed by promotion decisions.

Cross-lane rule:
- Treat these as interface boundaries: pricing output -> eligibility -> portfolio -> evaluation.
- Merge lower-stage contracts first (Stages 2/3) before policy-level selectors (Stage 6) where possible.

### Stage 0 — Acquire (budgeted, cache-first)

**Input**
- dataset spec:
  - `sport_key`, `markets`, `regions|bookmakers`, `includeLinks`, `includeSids`,
  - historical settings (`historical`, anchor hour, pre-tip offset).
- time window (`from/to`, timezone).
- spend policy (`offline`, `no-spend`, `max-credits`, `refresh`, `resume`).

**Process**
- resolve request keys deterministically.
- read-through/write-through via global cache + snapshot store.
- enforce paid-call policy before network.

**Output**
- `snapshot_id` and raw artifacts:
  - requests/responses/meta/manifest.
- day-index status rows (complete/incomplete, reasons).

**Primary modules**
- `/Users/andy/Documents/Code/parlay/src/prop_ev/odds_data/`
- `/Users/andy/Documents/Code/parlay/src/prop_ev/storage.py`

---

### Stage 1 — Normalize to canonical QuoteTable

**Goal**
- produce one deterministic quote table used by all downstream stages.

**Canonical identity (minimum)**
- `(event_id, player, market, point, side, book)`.

**Core columns**
- `price_american`, `last_update_utc`, `commence_time_utc`,
- optional links/sids,
- normalized player/event identifiers.

**Output**
- stable derived tables in snapshot (JSONL + parquet mirror where applicable).

---

### Stage 2 — De-vig + quality signals (per book, per line)

For each paired over/under quote set:
- implied probabilities,
- no-vig probabilities,
- hold/overround,
- freshness/staleness metrics,
- structural quality signals (depth/coverage).

**Output**
- per-line quality and fair-probability primitives for modeling.

---

### Stage 3 — Reference probability model (discovery books)

**Input**
- discovery-book quotes only (execution books optional, typically excluded to avoid circularity).

**Baseline**
- median no-vig consensus at exact line where possible.

**SOTA-leaning upgrade**
- alt-line monotone interpolation (weighted isotonic) across nearby points.

**Output**
- `p_ref` at execution point,
- uncertainty bounds (`p_low`, `p_high`),
- supporting evidence (books used, dispersion, depth).

---

### Stage 4 — Execution pricing + EV (execution books)

For each candidate ticket:
- exact-point matching on execution books,
- break-even probability at execution price,
- EV at `p_ref`,
- conservative EV at `p_low`.

**Output**
- priced candidate set with actionable/unactionable distinction.

---

### Stage 5 — Eligibility gates (risk + integrity)

Typical gates:
- minimum discovery depth (paired book count / quote count),
- hold cap and dispersion cap,
- quote freshness cap,
- event/player identity integrity,
- context health (injury/roster/source freshness policy).

All gate decisions must emit stable reason codes.

**Output**
- eligible candidates + watchlist with explicit rejection reasons.

---

### Stage 6 — Portfolio selection (max 5/day)

**Input**
- eligible candidates (prefer conservative EV score).

**Constraints (v1 defaults)**
- max 5 tickets/day,
- max 1 bet per player,
- max 2 bets per game,
- avoid high-correlation bundles unless exceptional edge.

**Output**
- `execution-plan.json`:
  - selected tickets,
  - execution book/price used,
  - watchlist + no-bet reasons.

---

### Stage 7 — Render and publish

Generate reader/operator artifacts:
- `strategy-report.json` / `.md`,
- brief (`strategy-brief.md/.tex/.pdf`) + metadata,
- compact mirrors (`reports/daily`, `reports/latest`),
- discovery/execution comparison outputs when run.

---

### Stage 8 — Settle and evaluate

Backtest/settlement outputs include:
- ROI and W/L/P distribution,
- Brier + calibration buckets,
- actionability rate,
- optional CLV proxy.

This stage drives promotion decisions and strategy default changes.

## 5) Data and Decision Contracts

### 5.1 Odds data contracts

- raw snapshot contract is immutable/auditable:
  - requests/responses/meta/manifest.
- day-index contract tracks completeness by day and reason codes.
- lake mirrors are deterministic and replayable.

### 5.2 Strategy/report contracts

Every report/bet artifact must carry:
- `snapshot_id`,
- strategy identifier/version,
- gate/reason outputs,
- execution price basis for grading.

### 5.3 ExecutionPlan contract (must-have milestone)

`execution-plan.json` should include:
- run metadata (`snapshot_id`, strategy id, generated time),
- selected tickets (<=5) with exact execution price/book,
- exclusions/watchlist with explicit reason codes.

## 6) Modeling Policy (robust + evidence-driven)

### 6.1 Baseline probability policy

- no-vig market consensus is the base signal.
- median across books is default robust aggregator.

### 6.2 Alt-line policy (primary near-term upgrade)

- use alternate lines to estimate fair probability at execution point.
- avoid brittle exact-point-only filtering.

### 6.3 Uncertainty policy

- compute `p_low/p_high` from disagreement/depth/freshness.
- gate by `EV(p_low, execution_price)`.

### 6.4 ML policy

- only after baseline + gating are stable.
- model predicts correction on top of market baseline.
- walk-forward only; no leakage; calibration required.

## 7) Milestones and Done-Criteria

### M0 — Contracts + operator clarity

Deliver:
- dataset discovery/status clarity,
- reason-code clarity,
- no-spend runbook flow.

Done when:
- operators can determine completeness in one pass,
- no-spend checks prove zero paid usage.

---

### M1 — ExecutionPlan artifact

Deliver:
- deterministic `execution-plan.json` generation.

Done when:
- same snapshot rerun reproduces same plan.

---

### M2 — Scoreboard + promotion gate

Deliver:
- unified backtest summary across strategies.

Promotion gate requires:
- minimum graded sample size,
- calibration floor,
- ROI and stability tie-breakers.

---

### M3 — Alt-line reference model

Deliver:
- monotone fair-probability interpolation at execution point.

Done when:
- actionability increases without calibration regression.

---

### M4 — Conservative uncertainty bands

Deliver:
- `p_low/p_high` and conservative-EV gating.

Done when:
- fewer fragile picks and improved drawdown behavior.

---

### M5 — Minutes/usage correction layer (optional)

Deliver:
- separate train/eval pipeline from NBA lake,
- market-anchored correction model.

Done when:
- measured out-of-sample lift over baseline.

---

### M6 — Production operations hardening

Deliver:
- stable scheduled profiles,
- no-game safe behavior,
- compact publish pipeline,
- incident triage/runbook patterns.

Done when:
- daily automation is predictable, explainable, and spend-safe.

---

### Integration Milestones (value-harvest to `main`)

These milestones are explicitly about integrating value from parallel tracks into mainline, not just
finishing branch-local work.

### IM1 — Harvest pricing contract foundation

Source slices:
- Track A: A1 + A2.

Deliver:
- canonical QuoteTable + de-vig contract merged to `main` behind stable interfaces.

Done when:
- mainline outputs are parity-stable (or intentional diffs are documented),
- downstream tracks can consume pricing artifacts without custom branch code.

---

### IM2 — Harvest minutes/usage artifact pipeline

Source slices:
- Track B: B1 + B2 (and optionally B3 behind flag).

Deliver:
- reproducible minutes/usage artifacts merged to `main` with versioned metadata.

Done when:
- artifacts can be built and loaded from mainline tooling,
- default behavior remains unchanged unless correction flag is enabled.

---

### IM3 — Harvest decision-path improvements

Source slices:
- Track A: A3 + A4,
- Track C baseline.

Deliver:
- reference interpolation + uncertainty outputs integrated into deterministic
  `execution-plan.json` generation path.

Done when:
- replay on fixed snapshots is deterministic end-to-end,
- plan artifacts include pricing provenance + exclusion reasons.

---

### IM4 — Harvest evaluation + promotion controls

Source slices:
- Track D baseline.

Deliver:
- unified scoreboard and explicit promotion gate logic merged to `main`.

Done when:
- promotion decisions are reproducible from artifacted metrics,
- default-strategy flips require small dedicated PRs with evidence references.

---

### IM5 — Harvest default policy upgrades (controlled flips)

Source slices:
- any completed track with evidence that passes promotion criteria.

Deliver:
- conservative default-on changes (strategy defaults, gating thresholds, correction toggles).

Done when:
- before/after delta is reviewed in one integration PR,
- rollback path is explicit (flag/config) with no schema rollback required.

## 8) Delivery Tracks (parallelizable, worktree-friendly)

These tracks are intended to be independently plannable by Codex with clear interfaces and merge
boundaries.

### Track A — Pricing core + neutralize-vig (start now)

- Detailed implementation spec: `docs/track-stage2-pricing-fixed-point.md`.

- **Owner worktree**: `wt-pricing-neutralize-vig` (branch prefix: `codex/pricing-*`).
- **Stage coverage**: Stage 1, 2, 3, and Stage 4 pricing primitives.
- **Goal**: produce a canonical pricing contract that all downstream stages consume.
- **In scope**:
  - canonical QuoteTable schema + deterministic normalization,
  - dedicated de-vig module with per-book per-line quality outputs,
  - alt-line interpolation to target execution point,
  - uncertainty outputs (`p_low`, `p_high`) with documented method.
- **Out of scope**:
  - portfolio selection policy,
  - report styling/presentation changes unrelated to pricing contracts.
- **Consumes**:
  - snapshot raw/derived odds artifacts,
  - current strategy plugin outputs for parity checks.
- **Produces (contract)**:
  - `quote-table` canonical artifact (stable schema),
  - pricing artifact with at least:
    - `p_implied`, `p_novig`, `hold`, freshness/depth quality fields,
    - `p_ref`, `p_low`, `p_high`,
    - provenance fields (`books_used`, `line_source`).
- **Milestone slices**:
  1. **A1 QuoteTable freeze**: formal schema + validation tests.
  2. **A2 De-vig extraction**: pure pricing module + contract tests.
  3. **A3 Alt-line interpolation**: monotone interpolation + edge-case tests.
  4. **A4 Uncertainty bands**: conservative bounds + determinism tests.
- **Done criteria**:
  - deterministic rerun equality on fixed snapshots,
  - stable schema snapshots in tests,
  - existing default strategy behavior is unchanged unless explicitly toggled.

### Track B — Minutes/usage modeling pipeline (start now, parallel with Track A)

- **Owner worktree**: `wt-minutes-usage-model` (branch prefix: `codex/minutes-*`).
- **Stage coverage**: model/correction path feeding Stage 5+ decisions.
- **Goal**: ship a versioned minutes/usage correction artifact trained/evaluated from `nba-data`.
- **In scope**:
  - feature extraction spec from NBA lake (`clean/schema_v1` inputs),
  - deterministic train/eval split policy (walk-forward),
  - prediction artifact format and metadata versioning,
  - optional correction merge hook (feature-flagged) against market baseline.
- **Out of scope**:
  - replacing market baseline as primary signal,
  - online/live training paths.
- **Consumes**:
  - `/Users/andy/Documents/Code/parlay-data/nba_data`,
  - odds-derived context needed for aligned joins (event/player/time keys).
- **Produces (contract)**:
  - versioned model artifact + metadata (`model_version`, train window, eval window),
  - prediction artifact keyed for merge with pricing candidates,
  - evaluation report with lift/calibration diagnostics.
- **Milestone slices**:
  1. **B1 Feature contract**: explicit schema + null/coverage checks.
  2. **B2 Baseline model**: reproducible train/eval command + artifact registry path.
  3. **B3 Correction integration**: additive correction mode behind flag.
  4. **B4 Evidence gate**: promotion only if out-of-sample lift is positive.
- **Done criteria**:
  - rerunnable pipeline from raw nba lake to scored artifact,
  - no leakage in walk-forward evaluation,
  - integration remains optional and does not change default picks without gate.

### Track C — ExecutionPlan + portfolio selector (start after A2)

- **Owner worktree**: `wt-execution-plan` (branch prefix: `codex/execution-plan-*`).
- **Stage coverage**: Stage 6.
- **Goal**: produce deterministic `execution-plan.json` from eligible candidates.
- **Depends on**:
  - Track A pricing contract (`p_ref`, `p_low`, quality metrics),
  - stable gate reason codes.
- **In scope**:
  - selection constraints (max/day, per-player, per-game),
  - tie-break policy (conservative EV, quality/freshness),
  - explicit exclusion reasons and watchlist outputs.
- **Produces (contract)**:
  - canonical `execution-plan.json` schema + validator,
  - deterministic ordering and selection evidence.
- **Done criteria**:
  - fixed snapshot replay yields byte-stable plan output,
  - reason coverage for all exclusions.

### Track D — Evaluation scoreboard + promotion policy (start after C baseline)

- **Owner worktree**: `wt-eval-scoreboard` (branch prefix: `codex/eval-*`).
- **Stage coverage**: Stage 8.
- **Goal**: unify settlement outputs into a promotion-ready scoreboard.
- **Depends on**:
  - stable `execution-plan` and settled outcomes.
- **In scope**:
  - ROI/W-L-P, Brier, calibration bins, actionability, optional CLV proxy,
  - strategy comparison table with promotion gate status.
- **Produces (contract)**:
  - machine-readable scoreboard artifact + concise markdown summary.
- **Done criteria**:
  - repeatable metrics for same input window,
  - promotion decision is derived from explicit thresholds.

### Track E — Data-plane + ops hardening (can run in parallel)

- **Owner worktree**: `wt-data-plane` and/or `wt-report-publish`.
- **Stage coverage**: Stage 0 and Stage 7 operational concerns.
- **Goal**: improve operator confidence without increasing paid-call risk.
- **In scope**:
  - day-index completeness UX and dataset gap semantics,
  - no-game safe exits in automation paths,
  - compact publish policy and report artifact hygiene.
- **Done criteria**:
  - operators can answer "what is complete?" from index/status alone,
  - automation exits cleanly on no-game slates with explicit reason.

### Cross-track integration rules

1. Treat artifacts as APIs: merge schema/contract PRs before policy PRs that consume them.
2. Keep one concern per PR: contract first, behavior switch second, default flip last.
3. Require deterministic replay checks before integrating between tracks.
4. Keep default runtime spend-safe (`--offline`/`--no-spend`) for dev and CI checks.

### Parallel-to-mainline integration plan (required after each track slice)

Every completed slice (A1, A2, B1, etc.) must ship with a small integration plan so improvements
land in mainline safely instead of accumulating in long-lived branches.

**Integration trigger**
- a track slice is marked "done" and has passing local validation.

**Integration packet (required)**
- summary of contract/behavior changes and whether defaults changed,
- compatibility notes (schema versioning, feature flags, fallback path),
- deterministic replay evidence on fixed snapshots,
- impact summary on primary outputs (strategy report, brief, execution-plan where applicable).

**Merge sequencing**
1. Rebase track branch onto latest `main`.
2. Run full gates (`ruff format/check`, `pyright`, `pytest`, replay checks).
3. Open a focused integration PR that includes only that slice.
4. If multiple slices are ready simultaneously, merge by dependency order:
   - pricing contracts (Track A) -> optional correction artifacts (Track B) ->
     execution plan (Track C) -> scoreboard/promotion policy (Track D).

**Default-flip policy**
- behavior-changing features merge behind explicit flags first,
- default-on flips require scoreboard evidence and a dedicated small PR.

**Post-merge check**
- run one no-spend/offline smoke flow on mainline,
- compare brief/report deltas and confirm differences are expected and attributable.

### Codex planning handoff template (per track)

For each track, Codex should generate:

1. **Contract spec**: fields, invariants, and compatibility notes.
2. **Implementation slices**: 3-5 PR-sized steps with file-level boundaries.
3. **Validation matrix**: unit + integration + deterministic replay checks.
4. **Merge gate**: explicit done-criteria mapped to milestone slice IDs.

## 9) Acceptance Gates (every milestone/PR)

- code quality:
  - `uv run ruff format --check .`
  - `uv run ruff check .`
  - `uv run pyright`
  - `uv run pytest -q`
- deterministic replay on fixed snapshots,
- stable contracts (or explicit version bump),
- no unintended paid calls in no-spend/offline modes.
- for parallel slices: include integration packet + merge sequencing notes.

## 10) Immediate Next Execution Sequence

1. Merge Stage 0/1 hardening PR and explicitly close IM1 on `main`.
2. Execute Track B (B1+B2) in parallel and harvest IM2.
3. Execute Track A pricing upgrades (A3+A4) + Track C baseline, then harvest IM3.
4. Execute Track D baseline and harvest IM4.
5. Propose only evidence-backed default flips and harvest IM5 incrementally.

## 11) Out of Scope (current horizon)

- New external odds providers.
- Cross-sport expansion.
- Non-replayable “live-only” logic that cannot be audited.
- Complexity-first modeling changes without calibration proof.
