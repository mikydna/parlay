"""Atomic and deterministic file I/O helpers."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Iterable, Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any


def sha256_bytes(data: bytes) -> str:
    """Return SHA-256 hex digest for raw bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    """Return SHA-256 hex digest for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".tmp-{path.name}-{uuid.uuid4().hex}")
    try:
        with tmp_path.open("wb") as handle:
            handle.write(data)
        os.replace(tmp_path, path)
    except OSError:
        with suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


def atomic_write_bytes(path: Path, data: bytes) -> None:
    _atomic_write_bytes(path, data)


def atomic_write_text(path: Path, text: str) -> None:
    _atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(path: Path, payload: Any) -> None:
    text = json.dumps(payload, sort_keys=True, ensure_ascii=True, indent=2) + "\n"
    atomic_write_text(path, text)


def atomic_write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    lines = [
        json.dumps(row, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n"
        for row in rows
    ]
    atomic_write_text(path, "".join(lines))
