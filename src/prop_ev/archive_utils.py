"""Helpers for tar.zst archive creation/extraction."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tarfile
from pathlib import Path


class ArchiveError(RuntimeError):
    """Raised for archive pack/unpack failures."""


def sha256_file(path: Path) -> str:
    """Return SHA-256 digest for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _zstd_bin() -> str:
    binary = shutil.which("zstd")
    if not binary:
        raise ArchiveError("zstd binary is required; install zstd and retry")
    return binary


def compress_tar_zst(*, tar_path: Path, out_path: Path, level: int = 19) -> None:
    """Compress a `.tar` file into `.tar.zst` using the zstd CLI."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    zstd = _zstd_bin()
    bounded_level = max(1, min(22, int(level)))
    completed = subprocess.run(
        [
            zstd,
            "-T0",
            f"-{bounded_level}",
            "-f",
            str(tar_path),
            "-o",
            str(out_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise ArchiveError(f"zstd compression failed: {completed.stderr.strip()}")


def decompress_zst_to_tar(*, zst_path: Path, tar_path: Path) -> None:
    """Decompress a `.zst` file into a `.tar` file using the zstd CLI."""
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    zstd = _zstd_bin()
    completed = subprocess.run(
        [
            zstd,
            "-d",
            "-f",
            str(zst_path),
            "-o",
            str(tar_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise ArchiveError(f"zstd decompression failed: {completed.stderr.strip()}")


def write_tar(*, tar_path: Path, root: Path, files: list[Path]) -> None:
    """Write a tar archive rooted at `root` for a list of files."""
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "w") as tar:
        for file_path in sorted(files):
            arcname = file_path.relative_to(root).as_posix()
            tar.add(file_path, arcname=arcname, recursive=False)


def safe_extract_tar(*, tar_path: Path, destination: Path) -> list[str]:
    """Extract tar contents, rejecting path traversal entries."""
    destination.mkdir(parents=True, exist_ok=True)
    destination_resolved = destination.resolve()
    with tarfile.open(tar_path, "r") as archive:
        names: list[str] = []
        for member in archive.getmembers():
            member_target = (destination_resolved / member.name).resolve()
            common = os.path.commonpath([str(destination_resolved), str(member_target)])
            if common != str(destination_resolved):
                raise ArchiveError(f"unsafe tar member path: {member.name}")
            names.append(member.name)
        archive.extractall(destination_resolved, filter="data")
        return names
