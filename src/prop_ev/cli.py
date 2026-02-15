"""CLI compatibility wrapper with delegated implementation module."""

from __future__ import annotations

import sys
from typing import Any

import prop_ev.cli_commands as _impl


def __getattr__(name: str) -> Any:
    return getattr(_impl, name)


def _sync_overrides() -> None:
    module = sys.modules[__name__]
    for name, value in vars(module).items():
        if name in {
            "_impl",
            "__getattr__",
            "_sync_overrides",
            "main",
        }:
            continue
        if name.startswith("__"):
            continue
        if name == "_run_strategy_for_playbook":
            if value is _DEFAULT_RUN_STRATEGY_FOR_PLAYBOOK:
                setattr(_impl, name, _IMPL_RUN_STRATEGY_FOR_PLAYBOOK)
            else:
                setattr(_impl, name, value)
            continue
        if hasattr(_impl, name):
            setattr(_impl, name, value)


def _run_strategy_for_playbook(*args: Any, **kwargs: Any) -> int:
    _sync_overrides()
    return int(_impl._run_strategy_for_playbook(*args, **kwargs))


_DEFAULT_RUN_STRATEGY_FOR_PLAYBOOK = _run_strategy_for_playbook
_IMPL_RUN_STRATEGY_FOR_PLAYBOOK = _impl._run_strategy_for_playbook


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""
    _sync_overrides()
    return _impl.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
