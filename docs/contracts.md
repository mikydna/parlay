# Contracts

This document defines stable artifact shapes and gate reason codes used by CLI flows.

## Strategy Report Contract

Path:
- `data/odds_api/snapshots/<snapshot_id>/reports/strategy-report.json`

Required top-level keys:
- `snapshot_id`
- `generated_at_utc`
- `strategy_status`
- `strategy_mode`
- `state_key`
- `summary`
- `health_report`
- `candidates`
- `ranked_plays`
- `watchlist`
- `audit`

Expected audit fields:
- `report_schema_version`
- `min_ev`
- `tier_a_min_ev`
- `tier_b_min_ev`
- `allow_tier_b`
- `market_baseline_method`
- `market_baseline_fallback`

## Brief Artifact Contract

Paths:
- `brief-input.json`
- `brief-pass1.json`
- `strategy-brief.meta.json`

`strategy-brief.meta.json` required keys:
- `schema_version`
- `generated_at_utc`
- `snapshot_id`
- `model`
- `brief_input_path`
- `brief_pass1_path`
- `brief_markdown_path`
- `brief_tex_path`
- `brief_pdf_path`
- `llm`
- `odds_budget`
- `pdf`
- `latest`

## Backtest Readiness Contract

Path:
- `data/odds_api/snapshots/<snapshot_id>/reports/backtest-readiness.json`

Required keys:
- `snapshot_id`
- `strategy_status`
- `candidate_lines`
- `ranked_lines`
- `eligible_lines`
- `generated_at_utc`

## Health Gate Reason Codes

Primary strategy/playbook health gates:
- `official_injury_missing`
- `odds_snapshot_stale`
- `injuries_context_stale`
- `roster_context_stale`

Strategy health command degraded/broken gates:
- `injury_source_failed`
- `roster_source_failed`
- `event_mapping_failed`
- `stale_inputs`
- `unknown_roster_detected`
- `roster_fallback_used`
- `official_injury_secondary_override`

## State ID Maps

To keep machine IDs stable and operator text readable, artifacts include map objects:
- `strategy-report.json`: top-level `state_key` with maps for `strategy_status`, `strategy_mode`, `health_gates`, `strategy_id`, and `strategy_description`.
- `strategy health` JSON output: top-level `state_key` with maps for `status` and `gates`.

Playbook run mode IDs are stable in `mode=<id>` output and map to text in `src/prop_ev/state_keys.py`.

## Strategy IDs

Canonical strategy IDs:
- `s001` — Baseline Core
- `s002` — Baseline Core + Tier B
- `s003` — Median No-Vig Baseline
- `s004` — Min-2 Book-Pair Gate
- `s005` — Hold-Cap Gate
- `s006` — Dispersion-IQR Gate
- `s007` — Quality Composite Gate

Legacy strategy IDs are intentionally unsupported.

## CLI Output Stability

For machine parsing, keep stable `key=value` lines where currently emitted, including:
- `snapshot_id=<...> mode=<...> within_window=<...> odds_cap_reached=<...>`
- `strategy_id=<...>`
- `context_preflight_gates=<...>` (when present)
- `strategy_brief_md=<...>`
- `strategy_brief_tex=<...>`
- `strategy_brief_pdf=<...>`
- `strategy_brief_meta=<...>`
