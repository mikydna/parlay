"""Odds data repository, cache, and backfill helpers."""

from prop_ev.odds_data.backfill import backfill_days
from prop_ev.odds_data.cache_store import GlobalCacheStore
from prop_ev.odds_data.day_index import (
    compute_day_status_from_cache,
    dataset_days_dir,
    dataset_spec_path,
    load_day_status,
    save_dataset_spec,
    save_day_status,
    snapshot_id_for_day,
)
from prop_ev.odds_data.errors import (
    CreditBudgetExceeded,
    OddsDataError,
    OfflineCacheMiss,
    SpendBlockedError,
)
from prop_ev.odds_data.policy import SpendPolicy, effective_max_credits
from prop_ev.odds_data.repo import FetchResult, OddsRepository
from prop_ev.odds_data.request import OddsRequest
from prop_ev.odds_data.spec import DatasetSpec, canonical_dict, dataset_id
from prop_ev.odds_data.window import day_window

__all__ = [
    "CreditBudgetExceeded",
    "DatasetSpec",
    "FetchResult",
    "GlobalCacheStore",
    "OddsDataError",
    "OddsRepository",
    "OddsRequest",
    "OfflineCacheMiss",
    "SpendBlockedError",
    "SpendPolicy",
    "backfill_days",
    "canonical_dict",
    "compute_day_status_from_cache",
    "dataset_days_dir",
    "dataset_id",
    "dataset_spec_path",
    "day_window",
    "effective_max_credits",
    "load_day_status",
    "save_dataset_spec",
    "save_day_status",
    "snapshot_id_for_day",
]
