"""Strategy report package with stable public exports."""

from __future__ import annotations

import prop_ev.strategy_output_impl as _strategy_output_impl

from .build import build_strategy_report
from .helpers import _fmt_american, _prop_label, _safe_float, _short_game_label, load_jsonl

_strategy_output_impl._short_game_label = _short_game_label
_strategy_output_impl._prop_label = _prop_label
_strategy_output_impl._safe_float = _safe_float
_strategy_output_impl._fmt_american = _fmt_american

render_strategy_markdown = _strategy_output_impl.render_strategy_markdown
write_strategy_reports = _strategy_output_impl.write_strategy_reports
write_execution_plan = _strategy_output_impl.write_execution_plan
write_tagged_strategy_reports = _strategy_output_impl.write_tagged_strategy_reports

__all__ = [
    "build_strategy_report",
    "load_jsonl",
    "render_strategy_markdown",
    "write_execution_plan",
    "write_strategy_reports",
    "write_tagged_strategy_reports",
]
