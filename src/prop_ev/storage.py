"""Snapshot and cache storage for Odds API payloads."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prop_ev import __version__
from prop_ev.time_utils import utc_now_str

SCHEMA_VERSION = 1


def now_utc() -> str:
    """Return a UTC timestamp string."""
    return utc_now_str()


def make_snapshot_id() -> str:
    """Build a filesystem-safe snapshot id."""
    return now_utc().replace(":", "-")


def sanitize_params(params: dict[str, Any]) -> dict[str, Any]:
    """Remove secret params and return a deterministic object."""
    clean: dict[str, Any] = {}
    for key, value in params.items():
        if key.lower() in {"apikey", "api_key"}:
            continue
        clean[key] = value
    return clean


def request_hash(method: str, path: str, params: dict[str, Any]) -> str:
    """Compute a stable hash for request identity."""
    payload = {
        "method": method.upper(),
        "path": path,
        "params": sanitize_params(params),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".tmp-{path.name}-{uuid.uuid4().hex}")
    try:
        tmp_path.write_text(content, encoding="utf-8")
        os.replace(tmp_path, path)
    except OSError:
        with suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


def _atomic_write_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=True, indent=2) + "\n"
    _atomic_write_text(path, payload)


class SnapshotStore:
    """Manage snapshot artifacts and manifests."""

    def __init__(self, root: Path | str = Path("data/odds_api")) -> None:
        self.root = Path(root)
        self.snapshots_dir = self.root / "snapshots"
        self.usage_dir = self.root / "usage"
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        self.usage_dir.mkdir(parents=True, exist_ok=True)

    def snapshot_dir(self, snapshot_id: str) -> Path:
        return self.snapshots_dir / snapshot_id

    def _manifest_path(self, snapshot_id: str) -> Path:
        return self.snapshot_dir(snapshot_id) / "manifest.json"

    def _request_path(self, snapshot_id: str, key: str) -> Path:
        return self.snapshot_dir(snapshot_id) / "requests" / f"{key}.json"

    def _response_path(self, snapshot_id: str, key: str) -> Path:
        return self.snapshot_dir(snapshot_id) / "responses" / f"{key}.json"

    def _meta_path(self, snapshot_id: str, key: str) -> Path:
        return self.snapshot_dir(snapshot_id) / "meta" / f"{key}.json"

    def _derived_dir(self, snapshot_id: str) -> Path:
        return self.snapshot_dir(snapshot_id) / "derived"

    @contextmanager
    def lock_snapshot(self, snapshot_id: str):
        """Take an exclusive lock for a snapshot id."""
        snapshot_dir = self.snapshot_dir(snapshot_id)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        lock_path = snapshot_dir / ".lock"
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError(f"snapshot {snapshot_id} is locked") from exc
        try:
            os.write(fd, str(os.getpid()).encode("utf-8"))
            yield
        finally:
            os.close(fd)
            with suppress(FileNotFoundError):
                lock_path.unlink()

    def ensure_snapshot(self, snapshot_id: str, run_config: dict[str, Any] | None = None) -> Path:
        """Create snapshot folders and manifest if missing."""
        root = self.snapshot_dir(snapshot_id)
        (root / "requests").mkdir(parents=True, exist_ok=True)
        (root / "responses").mkdir(parents=True, exist_ok=True)
        (root / "meta").mkdir(parents=True, exist_ok=True)
        self._derived_dir(snapshot_id).mkdir(parents=True, exist_ok=True)

        manifest_path = self._manifest_path(snapshot_id)
        if not manifest_path.exists():
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "snapshot_id": snapshot_id,
                "created_at_utc": now_utc(),
                "client_version": __version__,
                "git_sha": os.environ.get("GIT_SHA", ""),
                "run_config": run_config or {},
                "quota": {},
                "requests": {},
            }
            _atomic_write_json(manifest_path, manifest)
        return root

    def load_manifest(self, snapshot_id: str) -> dict[str, Any]:
        return json.loads(self._manifest_path(snapshot_id).read_text(encoding="utf-8"))

    def save_manifest(self, snapshot_id: str, manifest: dict[str, Any]) -> None:
        _atomic_write_json(self._manifest_path(snapshot_id), manifest)

    def has_response(self, snapshot_id: str, key: str) -> bool:
        return self._response_path(snapshot_id, key).exists()

    def load_response(self, snapshot_id: str, key: str) -> Any | None:
        response_path = self._response_path(snapshot_id, key)
        if not response_path.exists():
            return None
        return json.loads(response_path.read_text(encoding="utf-8"))

    def load_meta(self, snapshot_id: str, key: str) -> dict[str, Any] | None:
        meta_path = self._meta_path(snapshot_id, key)
        if not meta_path.exists():
            return None
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def write_request(self, snapshot_id: str, key: str, request_data: dict[str, Any]) -> None:
        _atomic_write_json(self._request_path(snapshot_id, key), request_data)

    def write_response(self, snapshot_id: str, key: str, response_data: Any) -> None:
        _atomic_write_json(self._response_path(snapshot_id, key), response_data)

    def write_meta(self, snapshot_id: str, key: str, meta_data: dict[str, Any]) -> None:
        _atomic_write_json(self._meta_path(snapshot_id, key), meta_data)

    def mark_request(
        self,
        snapshot_id: str,
        key: str,
        *,
        label: str,
        path: str,
        params: dict[str, Any],
        status: str,
        quota: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        manifest = self.load_manifest(snapshot_id)
        manifest.setdefault("requests", {})
        manifest["requests"][key] = {
            "label": label,
            "path": path,
            "params": sanitize_params(params),
            "status": status,
            "updated_at_utc": now_utc(),
            "error": error or "",
        }
        if quota:
            manifest["quota"] = quota
        self.save_manifest(snapshot_id, manifest)

    def request_status(self, snapshot_id: str, key: str) -> str | None:
        manifest = self.load_manifest(snapshot_id)
        request_row = manifest.get("requests", {}).get(key)
        if not request_row:
            return None
        value = request_row.get("status")
        if isinstance(value, str):
            return value
        return None

    def write_jsonl(self, path: Path, rows: list[dict[str, Any]]) -> None:
        lines = [f"{json.dumps(row, sort_keys=True, ensure_ascii=True)}\n" for row in rows]
        _atomic_write_text(path, "".join(lines))

    def derived_path(self, snapshot_id: str, filename: str) -> Path:
        return self._derived_dir(snapshot_id) / filename

    def append_usage(
        self,
        *,
        endpoint: str,
        request_key: str,
        snapshot_id: str,
        status_code: int,
        duration_ms: int,
        retry_count: int,
        headers: dict[str, str],
        cached: bool,
    ) -> None:
        month_key = datetime.now(UTC).strftime("%Y-%m")
        usage_path = self.usage_dir / f"usage-{month_key}.jsonl"
        row = {
            "timestamp_utc": now_utc(),
            "endpoint": endpoint,
            "request_key": request_key,
            "snapshot_id": snapshot_id,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "retry_count": retry_count,
            "cached": cached,
            "x_requests_last": headers.get("x-requests-last", ""),
            "x_requests_used": headers.get("x-requests-used", ""),
            "x_requests_remaining": headers.get("x-requests-remaining", ""),
        }
        usage_path.parent.mkdir(parents=True, exist_ok=True)
        with usage_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n")
