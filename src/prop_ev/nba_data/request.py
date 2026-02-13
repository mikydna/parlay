"""Request descriptor for unified NBA repository caches."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from prop_ev.storage import request_hash


@dataclass(frozen=True)
class NBADataRequest:
    """Stable identity for one NBA data lookup."""

    method: str
    path: str
    params: dict[str, Any]
    label: str

    def key(self) -> str:
        return request_hash(self.method, self.path, self.params)
