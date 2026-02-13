from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Protocol

from prop_ev.state_keys import (
    attach_strategy_code_key,
    attach_strategy_id_key,
    strategy_meta,
)


def normalize_strategy_id(value: str) -> str:
    raw = value.strip().lower().replace("-", "_")
    if not raw:
        raise ValueError("strategy id is required")
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789_")
    if any(ch not in allowed for ch in raw):
        raise ValueError(f"invalid strategy id: {value}")
    return raw


@dataclass(frozen=True)
class StrategyInfo:
    id: str
    name: str
    description: str


@dataclass(frozen=True)
class StrategyInputs:
    snapshot_id: str
    manifest: dict[str, Any]
    rows: list[dict[str, Any]]
    injuries: dict[str, Any] | None
    roster: dict[str, Any] | None
    event_context: dict[str, dict[str, str]] | None
    slate_rows: list[dict[str, Any]] | None
    player_identity_map: dict[str, Any] | None


@dataclass(frozen=True)
class StrategyRunConfig:
    top_n: int
    min_ev: float
    allow_tier_b: bool
    require_official_injuries: bool
    stale_quote_minutes: int
    require_fresh_context: bool


@dataclass(frozen=True)
class StrategyResult:
    report: dict[str, Any]
    config: StrategyRunConfig


class StrategyPlugin(Protocol):
    info: StrategyInfo

    def run(self, *, inputs: StrategyInputs, config: StrategyRunConfig) -> StrategyResult:
        raise NotImplementedError


def decorate_report(
    report: dict[str, Any], *, strategy: StrategyInfo, config: StrategyRunConfig
) -> dict[str, Any]:
    """Attach plugin identity and config to a report without changing its meaning."""
    strategy_id = normalize_strategy_id(strategy.id)
    strategy_payload = strategy_meta(
        strategy_id=strategy_id,
        strategy_name=strategy.name,
        strategy_description=strategy.description,
    )
    report["strategy_id"] = strategy_id
    report["strategy"] = strategy_payload
    report["strategy_code"] = strategy_payload.get("code", "")
    report["state_key"] = attach_strategy_id_key(
        report.get("state_key"),
        strategy_id=strategy_id,
        strategy_description=strategy.description,
    )
    report["state_key"] = attach_strategy_code_key(
        report.get("state_key"),
        strategy_id=strategy_id,
        strategy_description=strategy.description,
    )

    audit = report.get("audit", {})
    if not isinstance(audit, dict):
        audit = {}
        report["audit"] = audit

    audit["strategy_id"] = strategy_id
    audit["strategy_name"] = strategy.name
    audit["strategy_description"] = strategy.description
    audit["strategy_config"] = asdict(config)

    summary = report.get("summary", {})
    if isinstance(summary, dict):
        summary["strategy_id"] = strategy_id

    return report
