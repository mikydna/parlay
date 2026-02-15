"""Compatibility wrapper for modular strategy report package."""

from __future__ import annotations

from typing import Any

import prop_ev.strategy_report as _impl

build_strategy_report = _impl.build_strategy_report
load_jsonl = _impl.load_jsonl
render_strategy_markdown = _impl.render_strategy_markdown
write_execution_plan = _impl.write_execution_plan
write_strategy_reports = _impl.write_strategy_reports
write_tagged_strategy_reports = _impl.write_tagged_strategy_reports

__all__ = [
    "build_strategy_report",
    "load_jsonl",
    "render_strategy_markdown",
    "write_execution_plan",
    "write_strategy_reports",
    "write_tagged_strategy_reports",
]


def __getattr__(name: str) -> Any:
    return getattr(_impl, name)
