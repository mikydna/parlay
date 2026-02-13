"""Read-through/write-through repository for odds payloads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from prop_ev.odds_client import OddsResponse
from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.errors import OfflineCacheMiss, SpendBlockedError
from prop_ev.odds_data.policy import SpendPolicy, effective_max_credits
from prop_ev.odds_data.request import OddsRequest
from prop_ev.storage import SnapshotStore
from prop_ev.time_utils import utc_now_str


def _normalize_headers(value: dict[str, Any] | None) -> dict[str, str]:
    headers = value if isinstance(value, dict) else {}
    return {str(key): str(item) for key, item in headers.items()}


def _quota_from_headers(headers: dict[str, str]) -> dict[str, str]:
    return {
        "remaining": headers.get("x-requests-remaining", ""),
        "used": headers.get("x-requests-used", ""),
        "last": headers.get("x-requests-last", ""),
    }


@dataclass(frozen=True)
class FetchResult:
    """Materialized response from cache or network."""

    data: Any
    headers: dict[str, str]
    status: str
    key: str
    cache_level: str


class OddsRepository:
    """Snapshot + global cache repository with optional network fallback."""

    def __init__(self, *, store: SnapshotStore, cache: GlobalCacheStore | None = None) -> None:
        self.store = store
        self.cache = cache or GlobalCacheStore(store.root)

    def get_or_fetch(
        self,
        *,
        snapshot_id: str,
        req: OddsRequest,
        fetcher,
        policy: SpendPolicy,
    ) -> FetchResult:
        key = req.key()
        self.store.write_request(
            snapshot_id,
            key,
            {"method": req.method, "path": req.path, "params": req.params},
        )
        previous_status = self.store.request_status(snapshot_id, key)

        if (
            policy.resume
            and not policy.refresh
            and previous_status in {"ok", "cached"}
            and self.store.has_response(snapshot_id, key)
        ):
            data = self.store.load_response(snapshot_id, key)
            meta = self.store.load_meta(snapshot_id, key) or {}
            headers = _normalize_headers(meta.get("headers"))
            self.store.mark_request(
                snapshot_id,
                key,
                label=req.label,
                path=req.path,
                params=req.params,
                status="skipped",
                quota=_quota_from_headers(headers),
            )
            return FetchResult(
                data=data,
                headers=headers,
                status="skipped",
                key=key,
                cache_level="snapshot",
            )

        if not policy.refresh and self.store.has_response(snapshot_id, key):
            data = self.store.load_response(snapshot_id, key)
            meta = self.store.load_meta(snapshot_id, key) or {}
            headers = _normalize_headers(meta.get("headers"))
            self.store.mark_request(
                snapshot_id,
                key,
                label=req.label,
                path=req.path,
                params=req.params,
                status="cached",
                quota=_quota_from_headers(headers),
            )
            return FetchResult(
                data=data,
                headers=headers,
                status="cached",
                key=key,
                cache_level="snapshot",
            )

        if not policy.refresh and self.cache.has_response(key):
            self.cache.materialize_into_snapshot(self.store, snapshot_id, key)
            data = self.store.load_response(snapshot_id, key)
            meta = self.store.load_meta(snapshot_id, key) or {}
            headers = _normalize_headers(meta.get("headers"))
            self.store.mark_request(
                snapshot_id,
                key,
                label=req.label,
                path=req.path,
                params=req.params,
                status="cached",
                quota=_quota_from_headers(headers),
            )
            return FetchResult(
                data=data,
                headers=headers,
                status="cached",
                key=key,
                cache_level="global",
            )

        if policy.offline:
            self.store.mark_request(
                snapshot_id,
                key,
                label=req.label,
                path=req.path,
                params=req.params,
                status="failed",
                error=f"offline cache miss for {req.label}",
            )
            raise OfflineCacheMiss(f"cache miss while offline for {req.label}")

        blocked_paid = req.is_paid and (policy.block_paid or effective_max_credits(policy) == 0)
        if blocked_paid:
            self.store.mark_request(
                snapshot_id,
                key,
                label=req.label,
                path=req.path,
                params=req.params,
                status="failed",
                error=f"paid cache miss blocked for {req.label}",
            )
            raise SpendBlockedError(f"paid cache miss blocked for {req.label}")

        response: OddsResponse = fetcher()
        headers = _normalize_headers(response.headers)
        meta = {
            "endpoint": req.path,
            "status_code": int(response.status_code),
            "duration_ms": int(response.duration_ms),
            "retry_count": int(response.retry_count),
            "headers": headers,
            "fetched_at_utc": utc_now_str(),
        }
        self.store.write_response(snapshot_id, key, response.data)
        self.store.write_meta(snapshot_id, key, meta)
        self.cache.write_request(
            key, {"method": req.method, "path": req.path, "params": req.params}
        )
        self.cache.write_response(key, response.data)
        self.cache.write_meta(key, meta)
        self.store.append_usage(
            endpoint=req.path,
            request_key=key,
            snapshot_id=snapshot_id,
            status_code=int(response.status_code),
            duration_ms=int(response.duration_ms),
            retry_count=int(response.retry_count),
            headers=headers,
            cached=False,
        )
        self.store.mark_request(
            snapshot_id,
            key,
            label=req.label,
            path=req.path,
            params=req.params,
            status="ok",
            quota=_quota_from_headers(headers),
        )
        return FetchResult(
            data=response.data,
            headers=headers,
            status="ok",
            key=key,
            cache_level="network",
        )
