"""Low-cost cached LLM client for playbook report generation."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from prop_ev.budget import current_month_utc, llm_budget_status
from prop_ev.data_paths import resolve_runtime_root
from prop_ev.odds_client import parse_csv
from prop_ev.settings import Settings
from prop_ev.time_utils import utc_now_str

INPUT_RATE_PER_1M_USD = 0.25
OUTPUT_RATE_PER_1M_USD = 1.0
OPENAI_BASE_URL = "https://api.openai.com/v1"


class LLMClientError(RuntimeError):
    """Base error for LLM workflow failures."""


class MissingOpenAIKeyError(LLMClientError):
    """Raised when no OpenAI key is available."""


class LLMBudgetExceededError(LLMClientError):
    """Raised when the monthly LLM budget cap is exceeded."""


class LLMOfflineCacheMissError(LLMClientError):
    """Raised when an offline run has no cached result."""


class LLMResponseFormatError(LLMClientError):
    """Raised when the model response text cannot be parsed as expected."""


PostFn = Callable[[str, dict[str, str], dict[str, Any], float], dict[str, Any]]


def _now_utc() -> str:
    return utc_now_str()


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return 0
        try:
            return int(raw)
        except ValueError:
            return 0
    return 0


def _extract_text(payload: dict[str, Any]) -> str:
    direct = payload.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    output = payload.get("output", [])
    if isinstance(output, list):
        pieces: list[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            summary = item.get("summary")
            if isinstance(summary, str) and summary.strip():
                pieces.append(summary.strip())
            elif isinstance(summary, list):
                for row in summary:
                    if isinstance(row, dict):
                        text = row.get("text")
                        if isinstance(text, str) and text.strip():
                            pieces.append(text.strip())
            content = item.get("content", [])
            if not isinstance(content, list):
                continue
            for row in content:
                if not isinstance(row, dict):
                    continue
                text = row.get("text")
                if isinstance(text, str) and text.strip():
                    pieces.append(text.strip())
                    continue
                text = row.get("output_text")
                if isinstance(text, str) and text.strip():
                    pieces.append(text.strip())
                    continue
                nested = row.get("summary")
                if isinstance(nested, str) and nested.strip():
                    pieces.append(nested.strip())
                elif isinstance(nested, list):
                    for value in nested:
                        if isinstance(value, dict):
                            text = value.get("text")
                            if isinstance(text, str) and text.strip():
                                pieces.append(text.strip())
        if pieces:
            return "\n".join(pieces).strip()

    choices = payload.get("choices", [])
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message", {})
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()
    return ""


def _extract_usage(payload: dict[str, Any]) -> tuple[int, int, int]:
    usage = payload.get("usage", {})
    if not isinstance(usage, dict):
        return 0, 0, 0
    input_tokens = _to_int(usage.get("input_tokens", usage.get("prompt_tokens", 0)))
    output_tokens = _to_int(usage.get("output_tokens", usage.get("completion_tokens", 0)))
    total_tokens = _to_int(usage.get("total_tokens", input_tokens + output_tokens))
    if total_tokens <= 0:
        total_tokens = input_tokens + output_tokens
    return input_tokens, output_tokens, total_tokens


def _estimate_cost_usd(input_tokens: int, output_tokens: int) -> float:
    input_cost = (input_tokens / 1_000_000.0) * INPUT_RATE_PER_1M_USD
    output_cost = (output_tokens / 1_000_000.0) * OUTPUT_RATE_PER_1M_USD
    return round(input_cost + output_cost, 6)


def _extract_web_sources(payload: dict[str, Any]) -> list[dict[str, str]]:
    output = payload.get("output", [])
    if not isinstance(output, list):
        return []

    seen: set[tuple[str, str]] = set()
    sources: list[dict[str, str]] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        candidates: list[Any] = []
        action = item.get("action")
        if isinstance(action, dict):
            action_sources = action.get("sources")
            if isinstance(action_sources, list):
                candidates.extend(action_sources)
        item_sources = item.get("sources")
        if isinstance(item_sources, list):
            candidates.extend(item_sources)

        for source in candidates:
            if not isinstance(source, dict):
                continue
            url = str(source.get("url", "")).strip()
            if not url:
                continue
            title = str(source.get("title", "")).strip()
            domain = str(source.get("domain", "")).strip()
            key = (url, title)
            if key in seen:
                continue
            seen.add(key)
            sources.append({"title": title, "url": url, "domain": domain})
    return sources


def _default_post(
    url: str, headers: dict[str, str], payload: dict[str, Any], timeout: float
) -> dict[str, Any]:
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            response = httpx.post(url, headers=headers, json=payload, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise LLMClientError("unexpected OpenAI response payload")
            return data
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status in {429, 500, 502, 503, 504} and attempt < attempts:
                time.sleep(0.7 * attempt)
                continue
            detail = exc.response.text.strip()
            snippet = detail[-400:] if detail else ""
            raise LLMClientError(
                f"openai request failed: status={exc.response.status_code} detail={snippet}"
            ) from exc
        except httpx.HTTPError as exc:
            retryable = isinstance(
                exc,
                (
                    httpx.ReadTimeout,
                    httpx.ConnectTimeout,
                    httpx.RemoteProtocolError,
                    httpx.ReadError,
                    httpx.ConnectError,
                ),
            )
            if retryable and attempt < attempts:
                time.sleep(0.7 * attempt)
                continue
            raise LLMClientError(f"openai request transport error: {exc}") from exc

    raise LLMClientError("openai request failed after retries")


def _parse_key_file(path: Path) -> str:
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return ""
    first_line = raw.splitlines()[0].strip()
    if "=" in first_line:
        key, value = first_line.split("=", 1)
        if key.strip().upper() in {"OPENAI_API_KEY", "OPENAI_KEY"}:
            return value.strip().strip('"').strip("'")
    return first_line.strip().strip('"').strip("'")


def _supports_temperature(model: str) -> bool:
    normalized = model.strip().lower()
    return not normalized.startswith("gpt-5")


def _is_gpt5_model(model: str) -> bool:
    return model.strip().lower().startswith("gpt-5")


def _has_web_search_tool(request_options: dict[str, Any] | None) -> bool:
    if not isinstance(request_options, dict):
        return False
    tools = request_options.get("tools")
    if not isinstance(tools, list):
        return False
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        if str(tool.get("type", "")).strip().lower().startswith("web_search"):
            return True
    return False


def resolve_openai_api_key(settings: Settings, root: Path | None = None) -> str:
    """Resolve key from env first, then configured key files."""
    env_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if env_key:
        return env_key
    if settings.openai_api_key:
        return settings.openai_api_key.strip()

    base = root or Path.cwd()
    for candidate in parse_csv(settings.openai_key_file_candidates):
        path = base / candidate
        if not path.exists() or not path.is_file():
            continue
        try:
            key = _parse_key_file(path)
        except OSError:
            continue
        if key:
            return key
    raise MissingOpenAIKeyError(
        "missing OpenAI API key; set OPENAI_API_KEY or provide one of "
        f"{parse_csv(settings.openai_key_file_candidates)}"
    )


class LLMClient:
    """Cache-first wrapper around OpenAI Responses API for playbook generation."""

    def __init__(
        self,
        *,
        settings: Settings,
        data_root: Path,
        post_fn: PostFn | None = None,
        key_root: Path | None = None,
    ) -> None:
        self.settings = settings
        self.data_root = data_root.resolve()
        self.runtime_root = resolve_runtime_root(self.data_root)
        self.post_fn = post_fn or _default_post
        self.key_root = key_root or Path.cwd()
        self.cache_dir = self.runtime_root / "llm_cache"
        self.usage_dir = self.runtime_root / "llm_usage"
        self.legacy_cache_dir = self.data_root / "llm_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.usage_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(
        self,
        *,
        task: str,
        prompt_version: str,
        model: str,
        payload: dict[str, Any],
        request_options: dict[str, Any] | None = None,
    ) -> str:
        object_for_hash = {
            "task": task,
            "prompt_version": prompt_version,
            "model": model,
            "payload": payload,
            "request_options": request_options or {},
        }
        serialized = json.dumps(object_for_hash, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _cache_path(self, cache_key: str) -> Path:
        return self.cache_dir / f"{cache_key}.json"

    def _legacy_cache_path(self, cache_key: str) -> Path:
        return self.legacy_cache_dir / f"{cache_key}.json"

    def _usage_path(self, month: str) -> Path:
        return self.usage_dir / f"usage-{month}.jsonl"

    def _append_usage(
        self,
        *,
        month: str,
        task: str,
        prompt_version: str,
        model: str,
        cache_key: str,
        snapshot_id: str,
        cached: bool,
        input_tokens: int,
        output_tokens: int,
        total_tokens: int,
        cost_usd: float,
    ) -> None:
        row = {
            "timestamp_utc": _now_utc(),
            "task": task,
            "prompt_version": prompt_version,
            "model": model,
            "cache_key": cache_key,
            "snapshot_id": snapshot_id,
            "cached": cached,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "cost_usd": round(cost_usd, 6),
        }
        path = self._usage_path(month)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n")

    def cached_completion(
        self,
        *,
        task: str,
        prompt_version: str,
        prompt: str,
        payload: dict[str, Any],
        snapshot_id: str,
        model: str,
        max_output_tokens: int,
        temperature: float,
        refresh: bool,
        offline: bool,
        request_options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run one model call with cache + budget enforcement."""
        cache_key = self._cache_key(
            task=task,
            prompt_version=prompt_version,
            model=model,
            payload=payload,
            request_options=request_options,
        )
        cache_path = self._cache_path(cache_key)
        month = current_month_utc()

        if not refresh:
            existing_path = (
                cache_path if cache_path.exists() else self._legacy_cache_path(cache_key)
            )
            if existing_path.exists():
                cached_row = json.loads(existing_path.read_text(encoding="utf-8"))
                result = {
                    "cache_key": cache_key,
                    "cached": True,
                    "model": model,
                    "text": str(cached_row.get("response_text", "")),
                    "usage": cached_row.get("usage", {}),
                    "source": "cache",
                    "web_sources": cached_row.get("web_sources", []),
                }
                self._append_usage(
                    month=month,
                    task=task,
                    prompt_version=prompt_version,
                    model=model,
                    cache_key=cache_key,
                    snapshot_id=snapshot_id,
                    cached=True,
                    input_tokens=0,
                    output_tokens=0,
                    total_tokens=0,
                    cost_usd=0.0,
                )
                return result

        if offline:
            raise LLMOfflineCacheMissError(f"offline cache miss for task={task}")

        budget = llm_budget_status(self.runtime_root, month, self.settings.llm_monthly_cap_usd)
        if bool(budget.get("cap_reached", False)):
            raise LLMBudgetExceededError(
                f"llm monthly cap reached: used={budget['used_usd']} cap={budget['cap_usd']}"
            )

        api_key = resolve_openai_api_key(self.settings, self.key_root)
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        request_payload = {
            "model": model,
            "input": prompt,
            "max_output_tokens": max_output_tokens,
        }
        if _is_gpt5_model(model) and not _has_web_search_tool(request_options):
            # Prevent reasoning-only incomplete responses for long prompts.
            request_payload["reasoning"] = {"effort": "minimal"}
        if _supports_temperature(model):
            request_payload["temperature"] = temperature
        if isinstance(request_options, dict):
            for key in ["tools", "tool_choice", "include", "reasoning", "text"]:
                if key in request_options:
                    request_payload[key] = request_options[key]
        raw = self.post_fn(
            f"{OPENAI_BASE_URL}/responses",
            headers,
            request_payload,
            max(20.0, float(self.settings.openai_timeout_s)),
        )
        text = _extract_text(raw)
        if not text:
            status = str(raw.get("status", "")).strip().lower()
            response_id = str(raw.get("id", "")).strip()
            incomplete = raw.get("incomplete_details")
            detail_parts: list[str] = []
            if status:
                detail_parts.append(f"status={status}")
            if response_id:
                detail_parts.append(f"id={response_id}")
            if incomplete is not None:
                detail_parts.append(f"incomplete={incomplete}")
            suffix = f" ({', '.join(detail_parts)})" if detail_parts else ""
            raise LLMResponseFormatError(f"empty response text for task={task}{suffix}")

        input_tokens, output_tokens, total_tokens = _extract_usage(raw)
        cost_usd = _estimate_cost_usd(input_tokens, output_tokens)
        web_sources = _extract_web_sources(raw)
        usage_row = {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "cost_usd": cost_usd,
        }
        cache_data = {
            "schema_version": 1,
            "cache_key": cache_key,
            "created_at_utc": _now_utc(),
            "task": task,
            "prompt_version": prompt_version,
            "model": model,
            "payload": payload,
            "request_options": request_options or {},
            "response_text": text,
            "web_sources": web_sources,
            "usage": usage_row,
        }
        cache_path.write_text(
            json.dumps(cache_data, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        self._append_usage(
            month=month,
            task=task,
            prompt_version=prompt_version,
            model=model,
            cache_key=cache_key,
            snapshot_id=snapshot_id,
            cached=False,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            cost_usd=cost_usd,
        )
        return {
            "cache_key": cache_key,
            "cached": False,
            "model": model,
            "text": text,
            "usage": usage_row,
            "source": "live",
            "web_sources": web_sources,
        }
