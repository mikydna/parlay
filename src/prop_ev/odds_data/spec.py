"""Dataset specification + stable id for backfill/index artifacts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass


def _normalize_csv_like(values: list[str]) -> list[str]:
    normalized = [str(item).strip() for item in values if str(item).strip()]
    return sorted(set(normalized))


def _normalize_optional(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


@dataclass(frozen=True)
class DatasetSpec:
    """Canonical shape for day-indexed odds datasets."""

    sport_key: str
    markets: list[str]
    regions: str | None
    bookmakers: str | None
    include_links: bool
    include_sids: bool
    odds_format: str = "american"
    date_format: str = "iso"
    historical: bool = False
    historical_anchor_hour_local: int = 12
    historical_pre_tip_minutes: int = 60


def canonical_dict(spec: DatasetSpec) -> dict[str, object]:
    return {
        "sport_key": spec.sport_key.strip(),
        "markets": _normalize_csv_like(spec.markets),
        "regions": _normalize_optional(spec.regions),
        "bookmakers": _normalize_optional(spec.bookmakers),
        "include_links": bool(spec.include_links),
        "include_sids": bool(spec.include_sids),
        "odds_format": spec.odds_format.strip() or "american",
        "date_format": spec.date_format.strip() or "iso",
        "historical": bool(spec.historical),
        "historical_anchor_hour_local": int(spec.historical_anchor_hour_local),
        "historical_pre_tip_minutes": int(spec.historical_pre_tip_minutes),
    }


def dataset_id(spec: DatasetSpec) -> str:
    payload = json.dumps(canonical_dict(spec), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
