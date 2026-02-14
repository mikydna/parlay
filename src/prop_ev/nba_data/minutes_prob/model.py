"""Training, inference, and evaluation for probabilistic minutes model."""

from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingClassifier

from prop_ev.nba_data.minutes_prob.conformal import symmetric_halfwidth_from_residuals
from prop_ev.nba_data.minutes_prob.features import (
    FEATURE_COLUMNS,
    MinutesProbFeatureConfig,
    apply_walk_forward_split,
    build_minutes_prob_feature_frame,
)
from prop_ev.nba_data.normalize import normalize_person_name
from prop_ev.nba_data.store.layout import NBADataLayout

DEFAULT_MARKETS: tuple[str, ...] = (
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_points_rebounds_assists",
    "player_points_rebounds",
    "player_points_assists",
    "player_rebounds_assists",
    "player_turnovers",
    "player_blocks",
    "player_steals",
    "player_blocks_steals",
)


@dataclass(frozen=True)
class MinutesProbTrainConfig:
    seasons: list[str]
    season_type: str
    history_games: int
    min_history_games: int
    eval_days: int
    schema_version: int
    random_seed: int = 42
    model_version: str = "minutes_prob_v1"


@dataclass(frozen=True)
class _ConstantProbabilityModel:
    probability: float

    def predict_proba(self, matrix: np.ndarray) -> np.ndarray:
        clipped = float(max(0.0, min(1.0, self.probability)))
        ones = np.full((matrix.shape[0],), clipped, dtype=np.float64)
        return np.column_stack((1.0 - ones, ones))


@dataclass(frozen=True)
class _ConstantRegressor:
    value: float

    def predict(self, matrix: np.ndarray) -> np.ndarray:
        return np.full((matrix.shape[0],), float(self.value), dtype=np.float64)


@dataclass(frozen=True)
class MinutesProbModelBundle:
    model_version: str
    seasons: tuple[str, ...]
    season_type: str
    history_games: int
    min_history_games: int
    schema_version: int
    feature_schema_version: int
    feature_columns: tuple[str, ...]
    fill_values: dict[str, float]
    random_seed: int
    conformal_halfwidth: float
    p_active_model: Any
    q10_model: Any
    q50_model: Any
    q90_model: Any
    train_window_start: str
    train_window_end: str
    eval_window_start: str
    eval_window_end: str
    generated_at_utc: str


def _iso_z_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None
    return None


def _to_numpy_matrix(frame: pl.DataFrame, *, fill_values: dict[str, float]) -> np.ndarray:
    if frame.is_empty():
        return np.zeros((0, len(FEATURE_COLUMNS)), dtype=np.float64)
    ordered = [
        frame.get_column(column).fill_null(fill_values[column]).to_numpy().astype(np.float64)
        for column in FEATURE_COLUMNS
    ]
    return np.column_stack(ordered)


def _feature_fill_values(frame: pl.DataFrame) -> dict[str, float]:
    fills: dict[str, float] = {}
    for column in FEATURE_COLUMNS:
        if column not in frame.columns:
            fills[column] = 0.0
            continue
        value = frame.select(pl.col(column).median()).item()
        if value is None:
            fills[column] = 0.0
        else:
            fills[column] = float(value)
    return fills


def _fit_classifier(matrix: np.ndarray, target: np.ndarray, *, random_seed: int) -> Any:
    if matrix.shape[0] < 10 or np.unique(target).size < 2:
        probability = float(np.mean(target)) if target.size else 0.95
        return _ConstantProbabilityModel(probability=probability)
    model = HistGradientBoostingClassifier(
        max_depth=4,
        max_iter=250,
        learning_rate=0.05,
        min_samples_leaf=20,
        random_state=random_seed,
    )
    model.fit(matrix, target)
    return model


def _fit_quantile_model(
    matrix: np.ndarray, target: np.ndarray, *, alpha: float, random_seed: int
) -> Any:
    if matrix.shape[0] < 25:
        if target.size == 0:
            return _ConstantRegressor(value=24.0)
        return _ConstantRegressor(value=float(np.quantile(target, alpha)))
    model = GradientBoostingRegressor(
        loss="quantile",
        alpha=alpha,
        n_estimators=300,
        learning_rate=0.05,
        max_depth=3,
        min_samples_leaf=20,
        random_state=random_seed,
    )
    model.fit(matrix, target)
    return model


def _ensure_monotone_quantiles(
    q10: np.ndarray,
    q50: np.ndarray,
    q90: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    stacked = np.column_stack((q10, q50, q90))
    stacked.sort(axis=1)
    return stacked[:, 0], stacked[:, 1], stacked[:, 2]


def _phase_flags(*, games_on_team: int, games_played: int, p_active: float) -> str:
    flags: list[str] = []
    if games_on_team < 5:
        flags.append("new_team")
    if games_played < 10:
        flags.append("low_history")
    if p_active < 0.75:
        flags.append("availability_risk")
    return ",".join(flags)


def _quality_confidence(
    *,
    games_played: int,
    games_on_team: int,
    minutes_band: float,
    p_active: float,
) -> float:
    history_score = min(1.0, max(0.0, games_played / 40.0))
    tenure_score = min(1.0, max(0.0, games_on_team / 20.0))
    band_penalty = min(1.0, max(0.0, minutes_band / 20.0))
    active_score = min(1.0, max(0.0, p_active))
    confidence = (
        (0.35 * history_score)
        + (0.25 * tenure_score)
        + (0.2 * (1.0 - band_penalty))
        + (0.2 * active_score)
    )
    return round(float(max(0.0, min(1.0, confidence))), 6)


def _load_player_id_name_map(layout: NBADataLayout) -> dict[str, str]:
    identity_map_path = layout.root / "reference" / "player_identity_map.json"
    if not identity_map_path.exists():
        return {}
    try:
        payload = json.loads(identity_map_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    players = payload.get("players", {}) if isinstance(payload, dict) else {}
    if not isinstance(players, dict):
        return {}

    mapping: dict[str, str] = {}
    for key, value in players.items():
        if not isinstance(value, dict):
            continue
        canonical_name = str(value.get("canonical_name", "")).strip() or str(key).strip()
        if not canonical_name:
            continue
        candidate_ids: list[str] = []
        for single_key in ("nba_player_id", "player_id", "id"):
            raw = str(value.get(single_key, "")).strip()
            if raw:
                candidate_ids.append(raw)
        for list_key in ("nba_player_ids", "player_ids", "ids"):
            raw_list = value.get(list_key, [])
            if not isinstance(raw_list, list):
                continue
            for item in raw_list:
                raw = str(item).strip()
                if raw:
                    candidate_ids.append(raw)
        for player_id in sorted(set(candidate_ids)):
            mapping[player_id] = canonical_name
    return mapping


def _predict_internal(
    *,
    frame: pl.DataFrame,
    bundle: MinutesProbModelBundle,
    snapshot_id: str,
    markets: tuple[str, ...],
    player_name_map: dict[str, str] | None = None,
) -> pl.DataFrame:
    matrix = _to_numpy_matrix(frame, fill_values=bundle.fill_values)
    if matrix.shape[0] == 0:
        return pl.DataFrame(
            schema={
                "snapshot_id": pl.Utf8,
                "snapshot_date": pl.Utf8,
                "event_id": pl.Utf8,
                "player_id": pl.Utf8,
                "player_name": pl.Utf8,
                "player_norm": pl.Utf8,
                "team_id": pl.Utf8,
                "market": pl.Utf8,
                "minutes_p10": pl.Float64,
                "minutes_p50": pl.Float64,
                "minutes_p90": pl.Float64,
                "minutes_mu": pl.Float64,
                "minutes_sigma_proxy": pl.Float64,
                "p_active": pl.Float64,
                "games_on_team": pl.Int64,
                "days_on_team": pl.Int64,
                "new_team_phase": pl.Utf8,
                "confidence_score": pl.Float64,
                "data_quality_flags": pl.Utf8,
                "actual_minutes": pl.Float64,
            }
        )

    p_active = bundle.p_active_model.predict_proba(matrix)[:, 1]
    q10_raw = np.asarray(bundle.q10_model.predict(matrix), dtype=np.float64)
    q50_raw = np.asarray(bundle.q50_model.predict(matrix), dtype=np.float64)
    q90_raw = np.asarray(bundle.q90_model.predict(matrix), dtype=np.float64)
    q10_raw, q50_raw, q90_raw = _ensure_monotone_quantiles(q10_raw, q50_raw, q90_raw)

    halfwidth_raw = np.maximum(q50_raw - q10_raw, q90_raw - q50_raw)
    halfwidth = np.maximum(halfwidth_raw, bundle.conformal_halfwidth)
    q10 = np.clip(q50_raw - halfwidth, 0.0, 48.0)
    q90 = np.clip(q50_raw + halfwidth, 0.0, 48.0)
    q10, q50, q90 = _ensure_monotone_quantiles(q10, np.clip(q50_raw, 0.0, 48.0), q90)
    band = q90 - q10
    sigma_proxy = band / 2.5632

    base = frame.select(
        [
            "game_id",
            "game_date",
            "player_id",
            "team_id",
            "games_on_team",
            "days_on_team",
            "new_team_phase",
            "games_played",
            "actual_minutes",
        ]
    )
    player_id_values = base.get_column("player_id").to_list()
    player_name_lookup = player_name_map or {}
    player_name_values = [
        player_name_lookup.get(str(player_id).strip(), "") for player_id in player_id_values
    ]
    player_norm_values = [
        normalize_person_name(player_name) if player_name else normalize_person_name(str(player_id))
        for player_name, player_id in zip(player_name_values, player_id_values, strict=False)
    ]
    predictions = base.with_columns(
        pl.Series(name="minutes_p10", values=np.round(q10, 6)),
        pl.Series(name="minutes_p50", values=np.round(q50, 6)),
        pl.Series(name="minutes_p90", values=np.round(q90, 6)),
        pl.Series(name="minutes_mu", values=np.round(q50, 6)),
        pl.Series(name="minutes_sigma_proxy", values=np.round(sigma_proxy, 6)),
        pl.Series(name="p_active", values=np.round(p_active, 6)),
        pl.Series(name="player_name", values=player_name_values),
        pl.Series(name="player_norm", values=player_norm_values),
    ).rename({"game_id": "event_id"})
    predictions = predictions.with_columns(
        pl.col("event_id").cast(pl.Utf8),
        pl.col("player_id").cast(pl.Utf8),
        pl.col("player_name").cast(pl.Utf8),
        pl.col("player_norm").cast(pl.Utf8),
        pl.col("team_id").cast(pl.Utf8),
        pl.col("game_date").cast(pl.Utf8).alias("snapshot_date"),
    )
    predictions = predictions.with_columns(
        pl.struct(["games_played", "games_on_team", "minutes_p10", "minutes_p90", "p_active"])
        .map_elements(
            lambda row: _quality_confidence(
                games_played=int(row.get("games_played", 0) or 0),
                games_on_team=int(row.get("games_on_team", 0) or 0),
                minutes_band=float(row.get("minutes_p90", 0.0) or 0.0)
                - float(row.get("minutes_p10", 0.0) or 0.0),
                p_active=float(row.get("p_active", 0.0) or 0.0),
            ),
            return_dtype=pl.Float64,
        )
        .alias("confidence_score"),
        pl.struct(["games_played", "games_on_team", "p_active"])
        .map_elements(
            lambda row: _phase_flags(
                games_on_team=int(row.get("games_on_team", 0) or 0),
                games_played=int(row.get("games_played", 0) or 0),
                p_active=float(row.get("p_active", 0.0) or 0.0),
            ),
            return_dtype=pl.Utf8,
        )
        .alias("data_quality_flags"),
    )
    predictions = predictions.with_columns(
        pl.col("snapshot_date").fill_null(""),
        pl.lit(str(snapshot_id)).alias("snapshot_id"),
    )
    predictions = predictions.drop("games_played")

    market_frames = [predictions.with_columns(pl.lit(market).alias("market")) for market in markets]
    return pl.concat(market_frames, how="vertical")


def train_minutes_prob_model(
    *,
    layout: NBADataLayout,
    config: MinutesProbTrainConfig,
    out_dir: Path,
) -> dict[str, Any]:
    feature_frame = build_minutes_prob_feature_frame(
        layout=layout,
        config=MinutesProbFeatureConfig(
            seasons=config.seasons,
            season_type=config.season_type,
            history_games=config.history_games,
            min_history_games=config.min_history_games,
            eval_days=config.eval_days,
            schema_version=config.schema_version,
        ),
    )
    feature_frame = apply_walk_forward_split(feature_frame, eval_days=config.eval_days)
    if feature_frame.is_empty():
        raise ValueError("minutes-prob feature frame is empty for selected seasons/season-type")

    train_rows = feature_frame.filter(~pl.col("is_eval"))
    eval_rows = feature_frame.filter(pl.col("is_eval"))
    if train_rows.is_empty():
        raise ValueError("minutes-prob training split is empty")

    fill_values = _feature_fill_values(train_rows)
    train_matrix = _to_numpy_matrix(train_rows, fill_values=fill_values)
    train_active = train_rows.get_column("active_target").to_numpy().astype(np.int64)
    train_minutes = train_rows.get_column("actual_minutes").to_numpy().astype(np.float64)

    classifier = _fit_classifier(train_matrix, train_active, random_seed=config.random_seed)
    active_mask = train_active == 1
    active_matrix = train_matrix[active_mask]
    active_minutes = train_minutes[active_mask]
    q10_model = _fit_quantile_model(
        active_matrix,
        active_minutes,
        alpha=0.10,
        random_seed=config.random_seed,
    )
    q50_model = _fit_quantile_model(
        active_matrix,
        active_minutes,
        alpha=0.50,
        random_seed=config.random_seed,
    )
    q90_model = _fit_quantile_model(
        active_matrix,
        active_minutes,
        alpha=0.90,
        random_seed=config.random_seed,
    )

    conformal_halfwidth = 4.0
    if active_matrix.shape[0] >= 40:
        q50_train = np.asarray(q50_model.predict(active_matrix), dtype=np.float64)
        conformal_halfwidth = symmetric_halfwidth_from_residuals(
            actual=active_minutes,
            median_prediction=q50_train,
            quantile=0.9,
        )

    max_date = feature_frame.select(pl.max("game_date")).item()
    min_date = feature_frame.select(pl.min("game_date")).item()
    eval_min_date = (
        eval_rows.select(pl.min("game_date")).item() if not eval_rows.is_empty() else None
    )
    eval_max_date = (
        eval_rows.select(pl.max("game_date")).item() if not eval_rows.is_empty() else None
    )
    if not isinstance(min_date, date) or not isinstance(max_date, date):
        raise ValueError("failed resolving train window dates")

    run_id = (
        f"{config.model_version}-{config.seasons[0]}-{config.seasons[-1]}-"
        f"{config.season_type.strip().lower().replace(' ', '_')}"
        f"-h{int(config.history_games)}-e{int(config.eval_days)}"
    )
    model_dir = out_dir / run_id
    model_dir.mkdir(parents=True, exist_ok=True)

    bundle = MinutesProbModelBundle(
        model_version=config.model_version,
        seasons=tuple(config.seasons),
        season_type=config.season_type,
        history_games=int(config.history_games),
        min_history_games=int(config.min_history_games),
        schema_version=int(config.schema_version),
        feature_schema_version=1,
        feature_columns=FEATURE_COLUMNS,
        fill_values=fill_values,
        random_seed=int(config.random_seed),
        conformal_halfwidth=float(round(conformal_halfwidth, 6)),
        p_active_model=classifier,
        q10_model=q10_model,
        q50_model=q50_model,
        q90_model=q90_model,
        train_window_start=min_date.isoformat(),
        train_window_end=max_date.isoformat(),
        eval_window_start=eval_min_date.isoformat() if isinstance(eval_min_date, date) else "",
        eval_window_end=eval_max_date.isoformat() if isinstance(eval_max_date, date) else "",
        generated_at_utc=_iso_z_now(),
    )

    model_path = model_dir / "model.pkl"
    with model_path.open("wb") as handle:
        pickle.dump(bundle, handle)

    eval_predictions = _predict_internal(
        frame=eval_rows,
        bundle=bundle,
        snapshot_id=f"eval-{run_id}",
        markets=("player_points",),
    )
    eval_path = model_dir / "eval_predictions.parquet"
    eval_predictions.write_parquet(eval_path)
    evaluation = evaluate_minutes_prob_predictions(eval_predictions)
    evaluation_path = model_dir / "evaluation.json"
    evaluation_path.write_text(
        json.dumps(evaluation, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    metadata = {
        "schema_version": 1,
        "model_version": config.model_version,
        "run_id": run_id,
        "generated_at_utc": bundle.generated_at_utc,
        "seasons": list(config.seasons),
        "season_type": config.season_type,
        "history_games": int(config.history_games),
        "min_history_games": int(config.min_history_games),
        "eval_days": int(config.eval_days),
        "random_seed": int(config.random_seed),
        "feature_schema_version": bundle.feature_schema_version,
        "feature_columns": list(bundle.feature_columns),
        "conformal_halfwidth": bundle.conformal_halfwidth,
        "train_window": {
            "date_start": bundle.train_window_start,
            "date_end": bundle.train_window_end,
        },
        "eval_window": {
            "date_start": bundle.eval_window_start,
            "date_end": bundle.eval_window_end,
        },
        "rows_total": int(feature_frame.height),
        "rows_train": int(train_rows.height),
        "rows_eval": int(eval_rows.height),
        "rows_train_active": int(int(np.sum(train_active))),
        "artifacts": {
            "model_dir": str(model_dir),
            "model_path": str(model_path),
            "eval_predictions_path": str(eval_path),
            "evaluation_path": str(evaluation_path),
            "metadata_path": str(model_dir / "metadata.json"),
        },
        "evaluation": evaluation,
    }
    metadata_path = model_dir / "metadata.json"
    metadata_path.write_text(
        json.dumps(metadata, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    latest_dir = out_dir / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    latest_path = latest_dir / "model.json"
    latest_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "model_dir": str(model_dir),
                "model_path": str(model_path),
                "metadata_path": str(metadata_path),
                "updated_at_utc": _iso_z_now(),
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return metadata


def load_minutes_prob_model(model_dir: Path) -> MinutesProbModelBundle:
    model_path = model_dir / "model.pkl"
    if not model_path.exists():
        raise FileNotFoundError(f"minutes-prob model file not found: {model_path}")
    with model_path.open("rb") as handle:
        payload = pickle.load(handle)
    if not isinstance(payload, MinutesProbModelBundle):
        raise ValueError(f"invalid minutes-prob model payload: {model_path}")
    return payload


def predict_minutes_probabilities(
    *,
    layout: NBADataLayout,
    model_dir: Path,
    as_of_date: str,
    out_path: Path,
    snapshot_id: str = "",
    markets: tuple[str, ...] = DEFAULT_MARKETS,
) -> dict[str, Any]:
    bundle = load_minutes_prob_model(model_dir)
    target_day = _coerce_date(as_of_date)
    if target_day is None:
        raise ValueError(f"invalid as-of date: {as_of_date}")
    player_name_map = _load_player_id_name_map(layout)
    feature_frame = build_minutes_prob_feature_frame(
        layout=layout,
        config=MinutesProbFeatureConfig(
            seasons=list(bundle.seasons),
            season_type=bundle.season_type,
            history_games=int(bundle.history_games),
            min_history_games=int(bundle.min_history_games),
            eval_days=30,
            schema_version=int(bundle.schema_version),
        ),
    )
    if feature_frame.is_empty():
        raise ValueError("minutes-prob feature frame is empty")
    inference = feature_frame.filter(pl.col("game_date") == pl.lit(target_day))
    if inference.is_empty():
        raise ValueError(f"no rows found for as-of date: {target_day.isoformat()}")
    resolved_snapshot_id = snapshot_id.strip() or f"day-{target_day.isoformat()}"
    predictions = _predict_internal(
        frame=inference,
        bundle=bundle,
        snapshot_id=resolved_snapshot_id,
        markets=markets,
        player_name_map=player_name_map,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    predictions.write_parquet(out_path)

    out_root = model_dir.parent
    latest_dir = out_root / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    latest_predictions = latest_dir / "predictions.parquet"
    predictions.write_parquet(latest_predictions)

    output_meta = {
        "schema_version": 1,
        "model_version": bundle.model_version,
        "snapshot_id": resolved_snapshot_id,
        "as_of_date": target_day.isoformat(),
        "rows": int(predictions.height),
        "markets": list(markets),
        "out_path": str(out_path),
        "generated_at_utc": _iso_z_now(),
        "latest_predictions_path": str(latest_predictions),
    }
    meta_path = out_path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(output_meta, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    latest_meta = latest_dir / "predictions.meta.json"
    latest_meta.write_text(
        json.dumps(output_meta, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return output_meta


def evaluate_minutes_prob_predictions(predictions: pl.DataFrame) -> dict[str, Any]:
    if predictions.is_empty():
        return {
            "rows": 0,
            "rows_scored": 0,
            "metrics": {
                "mae": None,
                "rmse": None,
                "bias": None,
                "coverage_p10_p90": None,
                "brier_active": None,
            },
            "segments": [],
        }

    scored = predictions.filter(pl.col("actual_minutes").is_not_null())
    if scored.is_empty():
        return {
            "rows": int(predictions.height),
            "rows_scored": 0,
            "metrics": {
                "mae": None,
                "rmse": None,
                "bias": None,
                "coverage_p10_p90": None,
                "brier_active": None,
            },
            "segments": [],
        }

    scored = scored.with_columns(
        (pl.col("actual_minutes") - pl.col("minutes_mu")).abs().alias("abs_error"),
        (pl.col("actual_minutes") - pl.col("minutes_mu")).pow(2).alias("sq_error"),
        (pl.col("actual_minutes") - pl.col("minutes_mu")).alias("bias_error"),
        (
            (pl.col("actual_minutes") >= pl.col("minutes_p10"))
            & (pl.col("actual_minutes") <= pl.col("minutes_p90"))
        )
        .cast(pl.Float64)
        .alias("covered"),
        (pl.col("actual_minutes") > 0.0).cast(pl.Float64).alias("active_actual"),
        (pl.col("p_active") - (pl.col("actual_minutes") > 0.0).cast(pl.Float64))
        .pow(2)
        .alias("brier_active_term"),
    )
    mae = scored.select(pl.mean("abs_error")).item()
    mse = scored.select(pl.mean("sq_error")).item()
    rmse = (float(mse) ** 0.5) if mse is not None else None
    bias = scored.select(pl.mean("bias_error")).item()
    coverage = scored.select(pl.mean("covered")).item()
    brier_active = scored.select(pl.mean("brier_active_term")).item()

    segment_rows = (
        scored.group_by("new_team_phase")
        .agg(
            pl.len().alias("rows"),
            pl.mean("abs_error").alias("mae"),
            pl.mean("covered").alias("coverage"),
            pl.mean("brier_active_term").alias("brier_active"),
            pl.mean("confidence_score").alias("confidence_score"),
        )
        .sort("new_team_phase")
    )
    segments = segment_rows.to_dicts()
    for row in segments:
        row["rows"] = int(row.get("rows", 0) or 0)
        for key in ("mae", "coverage", "brier_active", "confidence_score"):
            value = row.get(key)
            row[key] = None if value is None else round(float(value), 6)

    return {
        "rows": int(predictions.height),
        "rows_scored": int(scored.height),
        "metrics": {
            "mae": None if mae is None else round(float(mae), 6),
            "rmse": None if rmse is None else round(float(rmse), 6),
            "bias": None if bias is None else round(float(bias), 6),
            "coverage_p10_p90": None if coverage is None else round(float(coverage), 6),
            "brier_active": None if brier_active is None else round(float(brier_active), 6),
        },
        "segments": segments,
    }


def evaluate_minutes_prob_predictions_file(
    *,
    predictions_path: Path,
    out_path: Path,
) -> dict[str, Any]:
    if not predictions_path.exists():
        raise FileNotFoundError(f"predictions parquet not found: {predictions_path}")
    predictions = pl.read_parquet(predictions_path)
    payload = {
        "schema_version": 1,
        "generated_at_utc": _iso_z_now(),
        "predictions_path": str(predictions_path),
        **evaluate_minutes_prob_predictions(predictions),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return payload


def resolve_default_predictions_out(
    *,
    model_dir: Path,
    as_of_date: str,
) -> Path:
    target_day = _coerce_date(as_of_date)
    if target_day is None:
        raise ValueError(f"invalid as-of date: {as_of_date}")
    root = model_dir.parent
    return root / "predictions" / f"snapshot_date={target_day.isoformat()}" / "predictions.parquet"


def resolve_latest_model_dir(*, out_dir: Path) -> Path:
    latest_path = out_dir / "latest" / "model.json"
    if not latest_path.exists():
        raise FileNotFoundError(f"latest model pointer not found: {latest_path}")
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    model_dir_raw = str(payload.get("model_dir", "")).strip()
    if not model_dir_raw:
        raise ValueError(f"invalid latest model pointer: {latest_path}")
    model_dir = Path(model_dir_raw).expanduser()
    if not model_dir.is_absolute():
        model_dir = (latest_path.parent / model_dir).resolve()
    return model_dir


def maybe_auto_build_predictions_for_day(
    *,
    layout: NBADataLayout,
    model_root_dir: Path,
    snapshot_day: str,
) -> Path | None:
    target_day = _coerce_date(snapshot_day)
    if target_day is None:
        return None
    out_path = (
        model_root_dir
        / "predictions"
        / f"snapshot_date={target_day.isoformat()}"
        / "predictions.parquet"
    )
    if out_path.exists():
        return out_path
    model_dir = resolve_latest_model_dir(out_dir=model_root_dir)
    try:
        predict_minutes_probabilities(
            layout=layout,
            model_dir=model_dir,
            as_of_date=target_day.isoformat(),
            out_path=out_path,
            snapshot_id=f"day-{target_day.isoformat()}",
            markets=DEFAULT_MARKETS,
        )
    except (FileNotFoundError, ValueError):
        return None
    return out_path if out_path.exists() else None
