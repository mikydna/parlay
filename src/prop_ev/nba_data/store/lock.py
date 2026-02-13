"""Lockfile utilities for resumable nba-data jobs."""

from __future__ import annotations

import json
import os
import socket
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path

from prop_ev.nba_data.errors import NBADataError
from prop_ev.time_utils import parse_iso_z, utc_now, utc_now_str


@dataclass(frozen=True)
class LockConfig:
    force_lock: bool = False
    stale_lock_minutes: int = 120
    no_stale_recover: bool = False


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _to_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    return 0


def _should_recover_stale(lock_payload: dict[str, object], *, stale_lock_minutes: int) -> bool:
    pid = _to_int(lock_payload.get("pid", 0))
    if _pid_alive(pid):
        return False
    started_at = str(lock_payload.get("started_at_utc", ""))
    parsed = parse_iso_z(started_at)
    if parsed is None:
        return True
    age_minutes = (utc_now() - parsed).total_seconds() / 60.0
    return age_minutes >= float(stale_lock_minutes)


@contextmanager
def lock_root(root: Path, *, config: LockConfig):
    """Acquire exclusive lock for nba-data root."""
    root.mkdir(parents=True, exist_ok=True)
    path = root / ".lock"
    payload = {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "started_at_utc": utc_now_str(),
    }
    if path.exists() and config.force_lock:
        with suppress(FileNotFoundError):
            path.unlink()
    if path.exists() and not config.force_lock:
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing = {}
        recoverable = not config.no_stale_recover and _should_recover_stale(
            existing if isinstance(existing, dict) else {},
            stale_lock_minutes=max(1, int(config.stale_lock_minutes)),
        )
        if recoverable:
            with suppress(FileNotFoundError):
                path.unlink()
        else:
            raise NBADataError(
                "lock already held at {} (pid={}, host={}); use --force-lock to override".format(
                    path,
                    existing.get("pid", ""),
                    existing.get("hostname", ""),
                )
            )

    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise NBADataError(f"lock already held at {path}; use --force-lock to override") from exc
    try:
        os.write(
            fd, (json.dumps(payload, sort_keys=True, ensure_ascii=True) + "\n").encode("utf-8")
        )
        yield
    finally:
        os.close(fd)
        with suppress(FileNotFoundError):
            path.unlink()
