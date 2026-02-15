"""Budget and usage summaries for Odds API and LLM runs."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prop_ev.data_paths import resolve_runtime_root
from prop_ev.util.parsing import safe_float as _safe_float_impl
from prop_ev.util.parsing import safe_int as _safe_int_impl


def current_month_utc() -> str:
    """Return current UTC month as YYYY-MM."""
    return datetime.now(UTC).strftime("%Y-%m")


def _safe_int(value: Any) -> int:
    parsed = _safe_int_impl(value)
    return parsed if parsed is not None else 0


def _safe_float(value: Any) -> float:
    parsed = _safe_float_impl(value)
    return parsed if parsed is not None else 0.0


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            rows.append(item)
    return rows


def read_odds_usage(data_root: Path, month: str) -> dict[str, Any]:
    """Summarize Odds API usage for one month from the usage ledger."""
    path = data_root / "usage" / f"usage-{month}.jsonl"
    rows = _load_jsonl(path)
    total_credits = 0
    provider_remaining: str = ""
    for row in rows:
        total_credits += _safe_int(row.get("x_requests_last", 0))
        provider_remaining = str(row.get("x_requests_remaining", provider_remaining))
    return {
        "month": month,
        "path": str(path),
        "rows": len(rows),
        "total_credits": total_credits,
        "provider_remaining": provider_remaining,
    }


def read_llm_usage(data_root: Path, month: str) -> dict[str, Any]:
    """Summarize LLM usage for one month from the usage ledger."""
    root = Path(data_root).resolve()
    if root.name == "runtime":
        path = root / "llm_usage" / f"usage-{month}.jsonl"
    else:
        path = resolve_runtime_root(root) / "llm_usage" / f"usage-{month}.jsonl"
    rows = _load_jsonl(path)
    total_cost_usd = 0.0
    total_input_tokens = 0
    total_output_tokens = 0
    total_requests = 0
    for row in rows:
        total_cost_usd += _safe_float(row.get("cost_usd", 0.0))
        total_input_tokens += _safe_int(row.get("input_tokens", 0))
        total_output_tokens += _safe_int(row.get("output_tokens", 0))
        total_requests += 1
    return {
        "month": month,
        "path": str(path),
        "rows": len(rows),
        "request_count": total_requests,
        "total_cost_usd": round(total_cost_usd, 6),
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "total_tokens": total_input_tokens + total_output_tokens,
    }


def odds_budget_status(data_root: Path, month: str, cap_credits: int) -> dict[str, Any]:
    """Build Odds API budget status for the month."""
    usage = read_odds_usage(data_root, month)
    used = _safe_int(usage.get("total_credits", 0))
    cap = max(0, cap_credits)
    remaining = max(0, cap - used)
    return {
        "month": month,
        "used_credits": used,
        "cap_credits": cap,
        "remaining_credits": remaining,
        "cap_reached": used >= cap,
        "provider_remaining": str(usage.get("provider_remaining", "")),
        "usage_path": usage.get("path", ""),
    }


def llm_budget_status(data_root: Path, month: str, cap_usd: float) -> dict[str, Any]:
    """Build LLM budget status for the month."""
    usage = read_llm_usage(data_root, month)
    used = _safe_float(usage.get("total_cost_usd", 0.0))
    cap = max(0.0, cap_usd)
    remaining = max(0.0, cap - used)
    return {
        "month": month,
        "used_usd": round(used, 6),
        "cap_usd": round(cap, 6),
        "remaining_usd": round(remaining, 6),
        "cap_reached": used >= cap,
        "usage_path": usage.get("path", ""),
        "request_count": usage.get("request_count", 0),
        "total_input_tokens": usage.get("total_input_tokens", 0),
        "total_output_tokens": usage.get("total_output_tokens", 0),
    }
