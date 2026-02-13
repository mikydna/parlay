"""Request descriptor for cached odds payload retrieval."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from prop_ev.storage import request_hash


@dataclass(frozen=True)
class OddsRequest:
    """Describes one request identity and billing class."""

    method: str
    path: str
    params: dict[str, Any]
    label: str
    is_paid: bool

    def key(self) -> str:
        return request_hash(self.method, self.path, self.params)
