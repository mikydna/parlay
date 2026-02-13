"""Playbook utilities for reader-friendly strategy briefs."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from shutil import copy2
from typing import Any

from prop_ev.brief_builder import (
    append_game_cards_section,
    build_analyst_synthesis_prompt,
    build_analyst_web_prompt,
    build_brief_input,
    build_pass1_prompt,
    build_pass2_prompt,
    default_analyst_take,
    default_pass1,
    enforce_p_hit_notes,
    enforce_readability_labels,
    enforce_snapshot_dates_et,
    enforce_snapshot_mode_labels,
    ensure_pagebreak_before_action_plan,
    extract_json_object,
    merge_analyst_take_sources,
    move_disclosures_to_end,
    normalize_pass2_markdown,
    render_analyst_take_section,
    render_fallback_markdown,
    sanitize_analyst_take,
    sanitize_pass1,
    strip_empty_go_placeholder_rows,
    strip_risks_and_watchouts_section,
    strip_tier_b_view_section,
    upsert_action_plan_table,
    upsert_analyst_take_section,
    upsert_best_available_section,
)
from prop_ev.budget import current_month_utc, llm_budget_status, odds_budget_status
from prop_ev.latex_renderer import render_pdf_from_markdown, write_latex
from prop_ev.llm_client import (
    LLMBudgetExceededError,
    LLMClient,
    LLMClientError,
    LLMOfflineCacheMissError,
)
from prop_ev.settings import Settings
from prop_ev.storage import SnapshotStore
from prop_ev.time_utils import parse_iso_z, utc_now_str

_LATEST_REPORT_FILES: tuple[str, ...] = (
    "strategy-report.json",
    "strategy-brief.meta.json",
    "strategy-brief.pdf",
)


def _now_utc() -> str:
    return utc_now_str()


def _parse_iso_utc(value: str) -> datetime | None:
    return parse_iso_z(value)


def _write_json(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object JSON in {path}")
    return payload


def _pass1_structured_output_options() -> dict[str, Any]:
    """Request strict pass1 JSON shape from the Responses API."""
    return {
        "text": {
            "format": {
                "type": "json_schema",
                "name": "playbook_pass1",
                "strict": True,
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "slate_summary",
                        "top_plays_explained",
                        "watchouts",
                        "data_quality_flags",
                        "confidence_notes",
                    ],
                    "properties": {
                        "slate_summary": {"type": "string"},
                        "top_plays_explained": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "game",
                                    "player",
                                    "market",
                                    "point",
                                    "side",
                                    "best_price",
                                    "best_book",
                                    "ev",
                                    "kelly",
                                    "ticket",
                                    "action",
                                    "edge_note",
                                    "why",
                                ],
                                "properties": {
                                    "game": {"type": "string"},
                                    "player": {"type": "string"},
                                    "market": {"type": "string"},
                                    "point": {
                                        "anyOf": [
                                            {"type": "number"},
                                            {"type": "string"},
                                            {"type": "null"},
                                        ]
                                    },
                                    "side": {"type": "string"},
                                    "best_price": {
                                        "anyOf": [
                                            {"type": "number"},
                                            {"type": "string"},
                                            {"type": "null"},
                                        ]
                                    },
                                    "best_book": {"type": "string"},
                                    "ev": {
                                        "anyOf": [
                                            {"type": "number"},
                                            {"type": "string"},
                                            {"type": "null"},
                                        ]
                                    },
                                    "kelly": {
                                        "anyOf": [
                                            {"type": "number"},
                                            {"type": "string"},
                                            {"type": "null"},
                                        ]
                                    },
                                    "ticket": {"type": "string"},
                                    "action": {"type": "string", "enum": ["GO", "LEAN", "NO-GO"]},
                                    "edge_note": {"type": "string"},
                                    "why": {"type": "string"},
                                },
                            },
                        },
                        "watchouts": {"type": "array", "items": {"type": "string"}},
                        "data_quality_flags": {"type": "array", "items": {"type": "string"}},
                        "confidence_notes": {"type": "array", "items": {"type": "string"}},
                    },
                },
            }
        }
    }


def compute_live_window(
    events: list[dict[str, Any]],
    *,
    now: datetime,
    pre_tip_h: int,
    post_tip_h: int,
) -> dict[str, Any]:
    """Compute whether current time is in the game-day live window."""
    tips: list[datetime] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        tip = _parse_iso_utc(str(event.get("commence_time", "")))
        if tip is not None:
            tips.append(tip)

    if not tips:
        return {
            "status": "no_events",
            "now_utc": now.isoformat().replace("+00:00", "Z"),
            "event_count": 0,
            "within_window": False,
            "first_tip_utc": "",
            "last_tip_utc": "",
            "window_start_utc": "",
            "window_end_utc": "",
        }

    first_tip = min(tips)
    last_tip = max(tips)
    window_start = first_tip - timedelta(hours=max(0, pre_tip_h))
    window_end = last_tip + timedelta(hours=max(0, post_tip_h))
    within_window = window_start <= now <= window_end
    return {
        "status": "ok",
        "now_utc": now.isoformat().replace("+00:00", "Z"),
        "event_count": len(tips),
        "within_window": within_window,
        "first_tip_utc": first_tip.isoformat().replace("+00:00", "Z"),
        "last_tip_utc": last_tip.isoformat().replace("+00:00", "Z"),
        "window_start_utc": window_start.isoformat().replace("+00:00", "Z"),
        "window_end_utc": window_end.isoformat().replace("+00:00", "Z"),
    }


def budget_snapshot(
    *, store: SnapshotStore, settings: Settings, month: str | None
) -> dict[str, Any]:
    """Read budget status for odds and LLM ledgers."""
    month_key = month or current_month_utc()
    odds = odds_budget_status(store.root, month_key, settings.odds_monthly_cap_credits)
    llm = llm_budget_status(store.root, month_key, settings.llm_monthly_cap_usd)
    return {
        "month": month_key,
        "odds": odds,
        "llm": llm,
    }


def _publish_latest(
    snapshot_reports_dir: Path, latest_dir: Path, snapshot_id: str
) -> dict[str, str]:
    latest_dir.mkdir(parents=True, exist_ok=True)
    published: dict[str, str] = {}
    for filename in _LATEST_REPORT_FILES:
        src = snapshot_reports_dir / filename
        if not src.exists():
            continue
        dst = latest_dir / filename
        copy2(src, dst)
        published[filename] = str(dst)
    latest_pointer = {
        "snapshot_id": snapshot_id,
        "updated_at_utc": _now_utc(),
    }
    latest_json = latest_dir / "latest.json"
    _write_json(latest_json, latest_pointer)
    published["latest.json"] = str(latest_json)
    return published


def generate_brief_for_snapshot(
    *,
    store: SnapshotStore,
    settings: Settings,
    snapshot_id: str,
    top_n: int,
    llm_refresh: bool,
    llm_offline: bool,
    per_game_top_n: int = 5,
    game_card_min_ev: float = 0.01,
    month: str | None = None,
) -> dict[str, Any]:
    """Generate markdown + LaTeX + PDF brief for one snapshot."""
    snapshot_dir = store.snapshot_dir(snapshot_id)
    reports_dir = snapshot_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    strategy_json_path = reports_dir / "strategy-report.json"
    if not strategy_json_path.exists():
        raise FileNotFoundError(f"missing strategy report: {strategy_json_path}")

    strategy_report = _load_json(strategy_json_path)
    brief_input = build_brief_input(
        strategy_report,
        top_n=top_n,
        per_game_top_n=per_game_top_n,
        game_card_min_ev=game_card_min_ev,
    )
    brief_input_path = _write_json(reports_dir / "brief-input.json", brief_input)

    llm = LLMClient(settings=settings, data_root=store.root)
    model = settings.openai_model
    month_key = month or current_month_utc()

    pass1 = default_pass1(brief_input)
    pass1_meta: dict[str, Any] = {"status": "fallback", "reason": "deterministic_default"}
    pass2_text = ""
    pass2_meta: dict[str, Any] = {"status": "fallback", "reason": "deterministic_default"}

    pass1_prompt = build_pass1_prompt(brief_input)
    pass1_payload = {
        "brief_input": brief_input,
        "task": "playbook_pass1",
    }

    pass1_errors: list[str] = []
    pass1_result: dict[str, Any] | None = None
    pass1_attempts_run = 0
    pass1_attempts = [
        {
            "prompt_version": "v2",
            "prompt": pass1_prompt,
            "max_output_tokens": 1400,
            "request_options": _pass1_structured_output_options(),
        },
        {
            "prompt_version": "v2_retry1",
            "prompt": (
                pass1_prompt + "\n\nIMPORTANT: Return a single top-level JSON object only. "
                "No prose, no markdown fences."
            ),
            "max_output_tokens": 1200,
            "request_options": _pass1_structured_output_options(),
        },
        {
            "prompt_version": "v2_retry2",
            "prompt": (
                pass1_prompt
                + "\n\nCRITICAL OUTPUT CONTRACT: output valid minified JSON object only, "
                "with required keys."
            ),
            "max_output_tokens": 1000,
            "request_options": _pass1_structured_output_options(),
        },
    ]
    for idx, attempt in enumerate(pass1_attempts):
        pass1_attempts_run = idx + 1
        try:
            result = llm.cached_completion(
                task="playbook_pass1",
                prompt_version=str(attempt["prompt_version"]),
                prompt=str(attempt["prompt"]),
                payload=pass1_payload,
                snapshot_id=snapshot_id,
                model=model,
                max_output_tokens=int(attempt["max_output_tokens"]),
                temperature=0.1,
                refresh=llm_refresh if idx == 0 else True,
                offline=llm_offline,
                request_options=attempt["request_options"],
            )
            parsed = extract_json_object(str(result.get("text", "")))
            sanitized = sanitize_pass1(parsed, brief_input)
            if not parsed:
                pass1_errors.append(f"attempt_{idx + 1}:empty_or_unparseable_json")
                continue
            pass1 = sanitized
            pass1_result = result
            break
        except (LLMBudgetExceededError, LLMOfflineCacheMissError) as exc:
            pass1_errors.append(f"attempt_{idx + 1}:{exc}")
            break
        except LLMClientError as exc:
            pass1_errors.append(f"attempt_{idx + 1}:{exc}")
            continue

    if pass1_result is not None:
        pass1_meta = {
            "status": "ok",
            "cached": bool(pass1_result.get("cached", False)),
            "cache_key": str(pass1_result.get("cache_key", "")),
            "usage": pass1_result.get("usage", {}),
            "attempts": pass1_attempts_run,
            "errors": pass1_errors,
        }
    else:
        pass1_meta = {
            "status": "fallback",
            "reason": pass1_errors[-1] if pass1_errors else "pass1_retry_exhausted",
            "attempts": pass1_attempts_run,
            "errors": pass1_errors,
        }

    pass1_path = _write_json(reports_dir / "brief-pass1.json", pass1)

    pass2_prompt = build_pass2_prompt(brief_input, pass1)
    pass2_payload = {
        "brief_input": brief_input,
        "pass1": pass1,
        "task": "playbook_pass2",
    }

    pass2_errors: list[str] = []
    pass2_attempts_run = 0
    pass2_result: dict[str, Any] | None = None
    pass2_attempts = [
        {
            "prompt_version": "v2",
            "prompt": pass2_prompt,
            "max_output_tokens": 1200,
        },
        {
            "prompt_version": "v2_retry1",
            "prompt": (
                pass2_prompt + "\n\nIMPORTANT: Return markdown only with all required headings."
            ),
            "max_output_tokens": 950,
        },
    ]

    for idx, attempt in enumerate(pass2_attempts):
        pass2_attempts_run = idx + 1
        try:
            result = llm.cached_completion(
                task="playbook_pass2",
                prompt_version=str(attempt["prompt_version"]),
                prompt=str(attempt["prompt"]),
                payload=pass2_payload,
                snapshot_id=snapshot_id,
                model=model,
                max_output_tokens=int(attempt["max_output_tokens"]),
                temperature=0.1,
                refresh=llm_refresh if idx == 0 else True,
                offline=llm_offline,
            )
            text = str(result.get("text", ""))
            if not text.strip():
                pass2_errors.append(f"attempt_{idx + 1}:empty_text")
                continue
            pass2_text = text
            pass2_result = result
            break
        except (LLMBudgetExceededError, LLMOfflineCacheMissError) as exc:
            pass2_errors.append(f"attempt_{idx + 1}:{exc}")
            break
        except LLMClientError as exc:
            pass2_errors.append(f"attempt_{idx + 1}:{exc}")
            continue

    if pass2_result is not None:
        pass2_meta = {
            "status": "ok",
            "cached": bool(pass2_result.get("cached", False)),
            "cache_key": str(pass2_result.get("cache_key", "")),
            "usage": pass2_result.get("usage", {}),
            "attempts": pass2_attempts_run,
            "errors": pass2_errors,
        }
    else:
        pass2_meta = {
            "status": "fallback",
            "reason": pass2_errors[-1] if pass2_errors else "pass2_retry_exhausted",
            "attempts": pass2_attempts_run,
            "errors": pass2_errors,
        }

    analyst_take = default_analyst_take(brief_input, pass1)
    analyst_mode = "deterministic_fallback"
    analyst_meta: dict[str, Any] = {"status": "fallback", "reason": "deterministic_default"}
    analyst_prompt = build_analyst_web_prompt(brief_input, pass1)
    analyst_payload = {
        "brief_input": brief_input,
        "pass1": pass1,
        "task": "playbook_analyst_web",
    }
    analyst_errors: list[str] = []
    analyst_attempts_run = 0
    analyst_result: dict[str, Any] | None = None
    analyst_web_source_count = 0
    analyst_attempts = [
        {
            "prompt_version": "v1",
            "request_options": {
                "tools": [{"type": "web_search"}],
                "tool_choice": "auto",
                "reasoning": {"effort": "low"},
                "include": ["web_search_call.action.sources"],
            },
            "max_output_tokens": 2400,
        },
        {
            "prompt_version": "v1_retry1",
            "request_options": {
                "tools": [{"type": "web_search"}],
                "tool_choice": "auto",
                "reasoning": {"effort": "low"},
            },
            "max_output_tokens": 1800,
        },
    ]
    for idx, attempt in enumerate(analyst_attempts):
        analyst_attempts_run = idx + 1
        try:
            result = llm.cached_completion(
                task="playbook_analyst_web",
                prompt_version=str(attempt["prompt_version"]),
                prompt=analyst_prompt,
                payload=analyst_payload,
                snapshot_id=snapshot_id,
                model=model,
                max_output_tokens=int(attempt["max_output_tokens"]),
                temperature=0.1,
                refresh=llm_refresh if idx == 0 else True,
                offline=llm_offline,
                request_options=attempt["request_options"],
            )
            raw_web_sources = result.get("web_sources", [])
            web_sources = raw_web_sources if isinstance(raw_web_sources, list) else []
            parsed = extract_json_object(str(result.get("text", "")))
            if not parsed:
                if web_sources:
                    synthesis_prompt = build_analyst_synthesis_prompt(
                        brief_input,
                        pass1,
                        web_sources,
                    )
                    synth_result = llm.cached_completion(
                        task="playbook_analyst_synthesis",
                        prompt_version=f"{attempt['prompt_version']}_synth",
                        prompt=synthesis_prompt,
                        payload={
                            "brief_input": brief_input,
                            "pass1": pass1,
                            "web_sources": web_sources,
                            "task": "playbook_analyst_synthesis",
                        },
                        snapshot_id=snapshot_id,
                        model=model,
                        max_output_tokens=1100,
                        temperature=0.1,
                        refresh=llm_refresh if idx == 0 else True,
                        offline=llm_offline,
                    )
                    synth_parsed = extract_json_object(str(synth_result.get("text", "")))
                    if synth_parsed:
                        analyst_take = sanitize_analyst_take(
                            synth_parsed,
                            brief_input=brief_input,
                            pass1=pass1,
                        )
                        analyst_take = merge_analyst_take_sources(analyst_take, web_sources)
                        analyst_mode = "llm_web"
                        analyst_result = synth_result
                        analyst_web_source_count = len(web_sources)
                        break
                    analyst_errors.append(f"attempt_{idx + 1}:synthesis_empty_or_unparseable_json")
                analyst_errors.append(f"attempt_{idx + 1}:empty_or_unparseable_json")
                continue
            analyst_take = sanitize_analyst_take(parsed, brief_input=brief_input, pass1=pass1)
            analyst_take = merge_analyst_take_sources(
                analyst_take,
                web_sources,
            )
            analyst_mode = "llm_web"
            analyst_result = result
            analyst_web_source_count = len(web_sources)
            break
        except (LLMBudgetExceededError, LLMOfflineCacheMissError) as exc:
            analyst_errors.append(f"attempt_{idx + 1}:{exc}")
            break
        except LLMClientError as exc:
            analyst_errors.append(f"attempt_{idx + 1}:{exc}")
            continue

    if analyst_result is not None:
        analyst_meta = {
            "status": "ok",
            "mode": analyst_mode,
            "cached": bool(analyst_result.get("cached", False)),
            "cache_key": str(analyst_result.get("cache_key", "")),
            "usage": analyst_result.get("usage", {}),
            "web_source_count": analyst_web_source_count,
            "attempts": analyst_attempts_run,
            "errors": analyst_errors,
        }
    else:
        analyst_meta = {
            "status": "fallback",
            "mode": analyst_mode,
            "reason": analyst_errors[-1] if analyst_errors else "analyst_retry_exhausted",
            "attempts": analyst_attempts_run,
            "errors": analyst_errors,
        }

    analyst_path = _write_json(reports_dir / "brief-analyst.json", analyst_take)

    fallback_md = render_fallback_markdown(
        brief_input=brief_input,
        pass1=pass1,
        source_label="deterministic" if pass2_meta.get("status") != "ok" else "llm",
    )
    markdown = normalize_pass2_markdown(pass2_text, fallback_md)
    markdown = strip_risks_and_watchouts_section(markdown)
    markdown = strip_tier_b_view_section(markdown)
    markdown = strip_empty_go_placeholder_rows(markdown)
    markdown = enforce_readability_labels(markdown, top_n=top_n)
    markdown = upsert_action_plan_table(markdown, brief_input=brief_input, top_n=top_n)
    analyst_section = render_analyst_take_section(
        analyst_take,
        mode=analyst_mode,
        brief_input=brief_input,
    )
    markdown = upsert_analyst_take_section(markdown, analyst_section)
    markdown = upsert_best_available_section(markdown, brief_input=brief_input)
    markdown = ensure_pagebreak_before_action_plan(markdown)
    markdown = append_game_cards_section(markdown, brief_input=brief_input)
    markdown = move_disclosures_to_end(markdown)
    markdown = enforce_p_hit_notes(markdown)
    markdown = enforce_snapshot_mode_labels(
        markdown,
        llm_pass1_status=str(pass1_meta.get("status", "")),
        llm_pass2_status=str(pass2_meta.get("status", "")),
    )
    markdown = enforce_snapshot_dates_et(markdown, brief_input=brief_input)
    markdown_path = reports_dir / "strategy-brief.md"
    markdown_path.write_text(markdown, encoding="utf-8")

    tex_path = reports_dir / "strategy-brief.tex"
    write_latex(markdown, tex_path=tex_path, title="NBA Strategy Brief", landscape=True)

    pdf_path = reports_dir / "strategy-brief.pdf"
    pdf_result = render_pdf_from_markdown(
        markdown,
        tex_path=tex_path,
        pdf_path=pdf_path,
        title="NBA Strategy Brief",
        landscape=True,
    )

    meta = {
        "schema_version": 1,
        "generated_at_utc": _now_utc(),
        "snapshot_id": snapshot_id,
        "model": model,
        "brief_input_path": str(brief_input_path),
        "brief_pass1_path": str(pass1_path),
        "brief_analyst_path": str(analyst_path),
        "brief_markdown_path": str(markdown_path),
        "brief_tex_path": str(tex_path),
        "brief_pdf_path": str(pdf_path),
        "llm": {
            "pass1": pass1_meta,
            "pass2": pass2_meta,
            "analyst": analyst_meta,
            "budget": llm_budget_status(store.root, month_key, settings.llm_monthly_cap_usd),
        },
        "odds_budget": odds_budget_status(store.root, month_key, settings.odds_monthly_cap_credits),
        "pdf": pdf_result,
        "latest": {},
    }
    meta_path = _write_json(reports_dir / "strategy-brief.meta.json", meta)
    latest_dir = store.root / "reports" / "latest"
    published = _publish_latest(reports_dir, latest_dir, snapshot_id)
    meta["latest"] = published
    _write_json(meta_path, meta)

    return {
        "snapshot_id": snapshot_id,
        "report_markdown": str(markdown_path),
        "report_tex": str(tex_path),
        "report_pdf": str(pdf_path),
        "report_meta": str(meta_path),
        "brief_input": str(brief_input_path),
        "brief_pass1": str(pass1_path),
        "brief_analyst": str(analyst_path),
        "llm_pass1_status": pass1_meta.get("status", ""),
        "llm_pass2_status": pass2_meta.get("status", ""),
        "pdf_status": pdf_result.get("status", ""),
        "latest_dir": str(latest_dir),
    }
