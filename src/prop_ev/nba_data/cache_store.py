"""Global cache for unified NBA repository responses."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

from prop_ev.data_paths import resolve_runtime_root


def _atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".tmp-{path.name}-{uuid.uuid4().hex}")
    try:
        payload = json.dumps(value, sort_keys=True, ensure_ascii=True, indent=2) + "\n"
        tmp_path.write_text(payload, encoding="utf-8")
        os.replace(tmp_path, path)
    except OSError:
        with suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


class NBADataCacheStore:
    """Shared key-addressed cache for NBA repository requests."""

    def __init__(self, root: Path | str = Path("data/odds_api")) -> None:
        self.root = Path(root).resolve()
        self.runtime_root = resolve_runtime_root(self.root)
        self.cache_dir = self.runtime_root / "nba_cache"
        self.requests_dir = self.cache_dir / "requests"
        self.responses_dir = self.cache_dir / "responses"
        self.meta_dir = self.cache_dir / "meta"
        self.legacy_cache_dir = self.root / "nba_cache"
        self.legacy_requests_dir = self.legacy_cache_dir / "requests"
        self.legacy_responses_dir = self.legacy_cache_dir / "responses"
        self.legacy_meta_dir = self.legacy_cache_dir / "meta"
        self.requests_dir.mkdir(parents=True, exist_ok=True)
        self.responses_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)

    def _request_path(self, key: str) -> Path:
        return self.requests_dir / f"{key}.json"

    def _response_path(self, key: str) -> Path:
        return self.responses_dir / f"{key}.json"

    def _meta_path(self, key: str) -> Path:
        return self.meta_dir / f"{key}.json"

    def _legacy_request_path(self, key: str) -> Path:
        return self.legacy_requests_dir / f"{key}.json"

    def _legacy_response_path(self, key: str) -> Path:
        return self.legacy_responses_dir / f"{key}.json"

    def _legacy_meta_path(self, key: str) -> Path:
        return self.legacy_meta_dir / f"{key}.json"

    def has_response(self, key: str) -> bool:
        return self._response_path(key).exists() or self._legacy_response_path(key).exists()

    def load_response(self, key: str) -> Any | None:
        for path in (self._response_path(key), self._legacy_response_path(key)):
            if not path.exists():
                continue
            return json.loads(path.read_text(encoding="utf-8"))
        return None

    def load_meta(self, key: str) -> dict[str, Any] | None:
        for path in (self._meta_path(key), self._legacy_meta_path(key)):
            if not path.exists():
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else None
        return None

    def write_request(self, key: str, request_data: dict[str, Any]) -> None:
        _atomic_write_json(self._request_path(key), request_data)

    def write_response(self, key: str, response_data: Any) -> None:
        _atomic_write_json(self._response_path(key), response_data)

    def write_meta(self, key: str, meta_data: dict[str, Any]) -> None:
        _atomic_write_json(self._meta_path(key), meta_data)
