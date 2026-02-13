"""HTTP gateway for unified NBA repository network calls."""

from __future__ import annotations

from typing import Any

import httpx


def get_json(url: str, *, timeout_s: float = 12.0) -> dict[str, Any]:
    """Fetch JSON payload via a single approved NBA HTTP entrypoint."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; prop-ev/0.1.0)",
        "Accept": "application/json;q=0.9,*/*;q=0.8",
    }
    response = httpx.get(url, headers=headers, timeout=timeout_s, follow_redirects=True)
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}
