"""Lake-boundary guardrail checks for odds/nba/report separation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GuardrailViolation:
    """One storage contract violation."""

    code: str
    path: Path
    detail: str = ""

    def as_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "path": str(self.path),
            "detail": self.detail,
        }


def _iter_snapshot_dirs(odds_root: Path) -> list[Path]:
    snapshots_root = odds_root / "snapshots"
    if not snapshots_root.exists():
        return []
    return sorted(path for path in snapshots_root.iterdir() if path.is_dir())


def _scan_embedded_snapshot_reports(odds_root: Path) -> list[GuardrailViolation]:
    violations: list[GuardrailViolation] = []
    for snapshot_dir in _iter_snapshot_dirs(odds_root):
        reports_dir = snapshot_dir / "reports"
        if reports_dir.exists():
            violations.append(
                GuardrailViolation(
                    code="embedded_snapshot_reports",
                    path=reports_dir,
                    detail="snapshot reports must live under reports/odds/**",
                )
            )
    return violations


def _scan_nba_blobs_under_odds(odds_root: Path) -> list[GuardrailViolation]:
    violations: list[GuardrailViolation] = []
    patterns = (
        ("legacy_nba_context", "snapshots/*/context/injuries.json"),
        ("legacy_nba_context", "snapshots/*/context/roster.json"),
        ("legacy_nba_context", "snapshots/*/context/results.json"),
        ("legacy_nba_context", "snapshots/*/context/results-*.json"),
        ("legacy_nba_context", "snapshots/*/context/official_injury_pdf"),
        ("legacy_nba_reference", "reference/player_identity_map.json"),
        ("legacy_nba_reference", "reference/injuries"),
        ("legacy_nba_reference", "reference/rosters"),
    )
    for code, pattern in patterns:
        for path in sorted(odds_root.glob(pattern)):
            if not path.exists():
                continue
            violations.append(
                GuardrailViolation(
                    code=code,
                    path=path,
                    detail="NBA-owned artifacts cannot live under odds lake",
                )
            )
    return violations


def _scan_runtime_inside_lake(odds_root: Path) -> list[GuardrailViolation]:
    violations: list[GuardrailViolation] = []
    runtime_dirs = ("cache", "llm_cache", "llm_usage", "nba_cache")
    for name in runtime_dirs:
        path = odds_root / name
        if not path.exists():
            continue
        violations.append(
            GuardrailViolation(
                code="runtime_artifact_in_lake",
                path=path,
                detail="runtime/cache artifacts must live under runtime/**",
            )
        )
    return violations


def collect_guardrail_violations(odds_root: Path | str) -> list[GuardrailViolation]:
    """Collect all P0 storage-contract violations under one odds root."""
    root = Path(odds_root).expanduser().resolve()
    violations: list[GuardrailViolation] = []
    violations.extend(_scan_embedded_snapshot_reports(root))
    violations.extend(_scan_nba_blobs_under_odds(root))
    violations.extend(_scan_runtime_inside_lake(root))
    return sorted(violations, key=lambda row: (row.code, str(row.path)))


def build_guardrail_report(odds_root: Path | str) -> dict[str, Any]:
    """Build JSON-ready guardrail status report."""
    root = Path(odds_root).expanduser().resolve()
    violations = collect_guardrail_violations(root)
    return {
        "odds_root": str(root),
        "status": "ok" if not violations else "violations",
        "violation_count": len(violations),
        "violations": [row.as_dict() for row in violations],
    }
