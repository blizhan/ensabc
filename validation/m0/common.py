#!/usr/bin/env python3
"""Shared helpers for the M0 baseline validation scripts.

The M0 evidence contract ([specs/001-m0-baseline-validation/contracts/evidence.md])
requires every artifact to be addressed by content hash, forbids non-standard
JSON (NaN/Infinity), and requires the source identity of a run to be recorded
even when the worktree is dirty. This module provides those primitives in one
place so the individual scripts cannot drift apart.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

# The repository root is the parent of `validation/m0/`.
REPO_ROOT = Path(__file__).resolve().parents[2]
VALIDATION_DIR = REPO_ROOT / "validation" / "m0"
FIXTURES_DIR = VALIDATION_DIR / "fixtures"
ARTIFACTS_DIR = REPO_ROOT / "artifacts" / "m0"

# Files that are hashed when recording the source identity of a run. The list is
# intentionally explicit: an implicit "hash everything" rule would change the
# recorded identity whenever an unrelated file is touched.
SOURCE_HASH_TARGETS = (
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    "validation/m0/common.py",
    "validation/m0/schema.py",
    "validation/m0/extract_sample.py",
    "validation/m0/verify_sample.py",
    "validation/m0/run_reference.py",
    "validation/m0/compare_reference.py",
    "validation/m0/verify_extension.py",
    "validation/m0/measure_tasks.py",
    "validation/m0/run_m0_gate.py",
    "validation/m0/verify_report.py",
    "validation/m0/traceability.py",
    "validation/m0/verify_sample_recovery.py",
    "validation/m0/reproduce_independently.py",
    "validation/m0/verify_minimal_runtime.sh",
    "validation/m0/minimal_runtime_cases.py",
    "crates/ensabc-duckdb/Cargo.toml",
    "crates/ensabc-duckdb/src/lib.rs",
)


class M0Error(RuntimeError):
    """Raised when an M0 validation step fails in a way the caller must handle."""


def canonical_json_bytes(payload: Any) -> bytes:
    """Serialize `payload` to the canonical byte sequence used for hashing.

    Canonical form is UTF-8, sorted keys, no insignificant whitespace, and no
    NaN/Infinity. `allow_nan=False` makes the forbidden non-finite values a
    hard error instead of silently emitting invalid JSON.
    """
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_text(payload: Any) -> str:
    """Render `payload` as canonical JSON text with a trailing newline."""
    return canonical_json_bytes(payload).decode("utf-8") + "\n"


def write_json(path: Path, payload: Any) -> str:
    """Write `payload` to `path` as pretty JSON and return the file SHA-256.

    Evidence files are written with sorted keys and a trailing newline so the
    bytes are stable across runs and diffable in review.
    """
    text = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return sha256_file(path)


def read_json(path: Path) -> Any:
    """Read a JSON document, rejecting the non-standard NaN/Infinity literals."""
    text = path.read_text(encoding="utf-8")
    return json.loads(text, parse_constant=_reject_non_finite_literal)


def _reject_non_finite_literal(name: str) -> float:
    raise M0Error(f"non-standard JSON literal {name!r} is not allowed in M0 evidence")


def sha256_bytes(data: bytes) -> str:
    """Return the lowercase hex SHA-256 digest of `data`."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    """Return the lowercase hex SHA-256 digest of the file at `path`."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_json(payload: Any) -> str:
    """Return the SHA-256 digest of the canonical JSON encoding of `payload`."""
    return sha256_bytes(canonical_json_bytes(payload))


def is_finite_number(value: Any) -> bool:
    """Return True for a real, finite number (bools are not numbers here)."""
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    return math.isfinite(value)


def all_finite(values: Iterable[Any]) -> bool:
    """Return True when every element of `values` is a finite number."""
    return all(is_finite_number(value) for value in values)


def run_command(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run `command` and return the completed process.

    Credentials are never captured: only the tool version lines the evidence
    contract asks for are recorded, and the caller decides what to keep.
    """
    completed = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if check and completed.returncode != 0:
        raise M0Error(
            f"command failed ({completed.returncode}): {' '.join(command)}\n"
            f"stdout: {completed.stdout.strip()}\n"
            f"stderr: {completed.stderr.strip()}"
        )
    return completed


def tool_version(command: list[str]) -> str:
    """Return the first line of `command`'s output, or a marker when unavailable."""
    try:
        completed = run_command(command, check=False, timeout=60)
    except (OSError, subprocess.SubprocessError) as error:
        return f"unavailable: {error}"
    output = (completed.stdout or completed.stderr or "").strip()
    if not output:
        return "unavailable: no output"
    return output.splitlines()[0].strip()


def git_output(*args: str) -> str | None:
    """Return the stripped output of `git <args>`, or None when git fails."""
    try:
        completed = run_command(["git", *args], cwd=REPO_ROOT, check=False, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


@dataclass(frozen=True)
class SourceIdentity:
    """Identity of the source tree a run was produced from."""

    git_commit: str | None
    git_dirty: bool
    git_status_porcelain: list[str]
    tracked_files: dict[str, str]
    untracked_files: list[str]
    extra_files: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "git_commit": self.git_commit,
            "git_dirty": self.git_dirty,
            "git_status_porcelain": self.git_status_porcelain,
            "tracked_files": self.tracked_files,
            "untracked_files": self.untracked_files,
            "extra_files": self.extra_files,
        }


def collect_source_identity(extra_files: Iterable[Path] = ()) -> SourceIdentity:
    """Record the source identity of the worktree.

    The evidence contract requires the source manifest and file hashes to be
    saved when the worktree is not committed, because a Git HEAD alone does not
    identify the code that produced a result. `extra_files` names files that
    are hashed in addition to the tracked source targets (for example a frozen
    fixture that is not tracked yet).
    """
    tracked: dict[str, str] = {}
    for relative in SOURCE_HASH_TARGETS:
        path = REPO_ROOT / relative
        if path.is_file():
            tracked[relative] = sha256_file(path)

    extra: dict[str, str] = {}
    for path in extra_files:
        if not path.is_file():
            # A file that is not present is recorded as absent rather than
            # failing the run: the derived sample payload is deliberately not
            # committed, and the run may rebuild it.
            continue
        try:
            relative = str(path.resolve().relative_to(REPO_ROOT))
        except ValueError:
            relative = path.name
        extra[relative] = sha256_file(path)

    commit = git_output("rev-parse", "HEAD")
    status = git_output("status", "--porcelain") or ""
    status_lines = [line for line in status.splitlines() if line.strip()]
    untracked = [line[3:] for line in status_lines if line.startswith("?? ")]

    return SourceIdentity(
        git_commit=commit,
        git_dirty=bool(status_lines),
        git_status_porcelain=status_lines,
        tracked_files=tracked,
        untracked_files=untracked,
        extra_files=extra,
    )


def environment_snapshot() -> dict[str, Any]:
    """Record the tool versions that a verifier needs to rebuild the environment.

    Only reproduction-relevant versions are recorded. The full environment is
    explicitly not captured, so no credential can leak into the evidence.
    """
    return {
        "platform": {
            "system": os.uname().sysname,
            "release": os.uname().release,
            "machine": os.uname().machine,
        },
        "python_version": tool_version(["python3", "--version"]),
        "rustc_version": tool_version(["rustc", "--version"]),
        "cargo_version": tool_version(["cargo", "--version"]),
        "duckdb_version": tool_version(["duckdb", "--version"]),
        "git_commit": git_output("rev-parse", "HEAD"),
    }


def utc_now() -> str:
    """Return the current UTC time as an ISO 8601 string with a `Z` suffix."""
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
