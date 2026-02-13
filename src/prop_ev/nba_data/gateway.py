"""HTTP gateway for unified NBA repository network calls."""

from __future__ import annotations

from typing import Any

import httpx


def _get(url: str, *, timeout_s: float, accept: str) -> httpx.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; prop-ev/0.1.0)",
        "Accept": accept,
    }
    response = httpx.get(url, headers=headers, timeout=timeout_s, follow_redirects=True)
    response.raise_for_status()
    return response


def get_json(url: str, *, timeout_s: float = 12.0) -> dict[str, Any]:
    """Fetch JSON payload via a single approved NBA HTTP entrypoint."""
    response = _get(url, timeout_s=timeout_s, accept="application/json;q=0.9,*/*;q=0.8")
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def get_text(url: str, *, timeout_s: float = 12.0) -> str:
    """Fetch text payload via the approved NBA HTTP entrypoint."""
    response = _get(url, timeout_s=timeout_s, accept="text/html,application/json;q=0.9,*/*;q=0.8")
    return response.text


def get_bytes(url: str, *, timeout_s: float = 12.0) -> bytes:
    """Fetch binary payload via the approved NBA HTTP entrypoint."""
    response = _get(url, timeout_s=timeout_s, accept="application/pdf,application/octet-stream,*/*")
    return response.content
