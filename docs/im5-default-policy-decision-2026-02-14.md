# IM5 Default-Policy Decision (2026-02-14)

Decision: **defer default-on strategy/policy flips**.

## Scope reviewed

- Aggregate complete-day scoreboards for caps `max_picks=1,2,5`.
- Promotion-gate outputs and graded-row floors.
- Current default/runtime safety posture.

## Why deferred

1. Evidence is not uniformly strong across operating caps.
   - `s020` promotion gate still fails at `max_picks=1` due graded sample floor.
2. Existing defaults are stable and conservative.
   - `strategy.default_id = "s001"`
   - `strategy.probabilistic_profile = "off"`
3. Default flips should be one-purpose and reversible.
   - IM5 requires a dedicated before/after packet and explicit rollback.

## Re-open criteria

Open IM5 only with a dedicated PR that includes:

- scoreboards for `max_picks=1,2,5`,
- gate-pass evidence (or explicit pass/fail thresholds) on target operating cap,
- explicit rollback path via runtime config/flag,
- confirmation that schema contracts do not change.
