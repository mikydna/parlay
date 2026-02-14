"""Filesystem contract helpers for minutes-prob artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from prop_ev.nba_data.minutes_prob.model import maybe_auto_build_predictions_for_day
from prop_ev.nba_data.normalize import normalize_person_name
from prop_ev.nba_data.store.layout import NBADataLayout


def minutes_prob_root(layout: NBADataLayout) -> Path:
    return layout.reports_dir / "analysis" / "minutes_prob"


def predictions_path_for_day(*, root_dir: Path, snapshot_day: str) -> Path:
    return root_dir / "predictions" / f"snapshot_date={snapshot_day}" / "predictions.parquet"


def load_predictions_index(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exact": {}, "player": {}, "meta": {"path": str(path), "rows": 0}}
    frame = pl.read_parquet(path)
    exact: dict[str, dict[str, Any]] = {}
    player: dict[str, dict[str, Any]] = {}
    for row in frame.to_dicts():
        event_id = str(row.get("event_id", "")).strip()
        market = str(row.get("market", "")).strip().lower()
        player_id = str(row.get("player_id", "")).strip()
        player_name = str(row.get("player_name", "")).strip()
        player_norm_raw = str(row.get("player_norm", "")).strip()
        player_norm = (
            normalize_person_name(player_norm_raw)
            or normalize_person_name(player_name)
            or normalize_person_name(player_id)
        )
        payload = {
            "minutes_p10": row.get("minutes_p10"),
            "minutes_p50": row.get("minutes_p50"),
            "minutes_p90": row.get("minutes_p90"),
            "minutes_mu": row.get("minutes_mu"),
            "minutes_sigma_proxy": row.get("minutes_sigma_proxy"),
            "p_active": row.get("p_active"),
            "player_id": player_id,
            "player_name": player_name,
            "player_norm": player_norm,
            "games_on_team": row.get("games_on_team"),
            "days_on_team": row.get("days_on_team"),
            "new_team_phase": row.get("new_team_phase"),
            "confidence_score": row.get("confidence_score"),
            "data_quality_flags": row.get("data_quality_flags"),
            "snapshot_date": row.get("snapshot_date"),
        }
        if player_norm:
            player[player_norm] = payload
        if event_id and player_norm and market:
            key = f"{event_id}|{player_norm}|{market}"
            exact[key] = payload
    return {
        "exact": exact,
        "player": player,
        "meta": {"path": str(path), "rows": int(frame.height)},
    }


def load_minutes_prob_index_for_snapshot(
    *,
    layout: NBADataLayout,
    snapshot_day: str,
    probabilistic_profile: str,
    auto_build: bool = True,
) -> dict[str, Any]:
    profile = probabilistic_profile.strip().lower()
    if profile != "minutes_v1":
        return {"exact": {}, "player": {}, "meta": {"profile": "off"}}
    root = minutes_prob_root(layout)
    path = predictions_path_for_day(root_dir=root, snapshot_day=snapshot_day)
    if not path.exists() and auto_build:
        maybe_auto_build_predictions_for_day(
            layout=layout,
            model_root_dir=root,
            snapshot_day=snapshot_day,
        )
    if not path.exists():
        latest = root / "latest" / "predictions.parquet"
        if latest.exists():
            path = latest
    payload = load_predictions_index(path)
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    meta["profile"] = profile
    meta["snapshot_day"] = snapshot_day
    payload["meta"] = meta
    return payload
