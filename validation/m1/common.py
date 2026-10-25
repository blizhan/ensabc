#!/usr/bin/env python3
"""Shared helpers for M1 scenario computation validation scripts.

Implements M1 evidence and validation primitives:
- Strict JSON loading and canonical serialization rejecting NaN/Infinity
- Content addressing: SHA-256 for bytes, files, and canonical JSON
- Unique run-directory creation refusing directory collision
- Located validation errors (M1Error / LocatedError)
- Relevant source inventory with dirty tracked and untracked file tracking
  without copying secrets or real-data payloads
- Standard library only
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

REPO_ROOT = Path(__file__).resolve().parents[2]
VALIDATION_DIR = REPO_ROOT / "validation" / "m1"
FIXTURES_DIR = VALIDATION_DIR / "fixtures"
M0_VALIDATION_DIR = REPO_ROOT / "validation" / "m0"
M0_FIXTURES_DIR = M0_VALIDATION_DIR / "fixtures"
ARTIFACTS_DIR = REPO_ROOT / "artifacts" / "m1"

# Relevant source files whose exact content identifies the M1 codebase state.
SOURCE_HASH_TARGETS = (
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    # M1 Rust crates
    "crates/ensabc-core/Cargo.toml",
    "crates/ensabc-core/src/lib.rs",
    "crates/ensabc-core/src/preprocess.rs",
    "crates/ensabc-core/src/kmeans.rs",
    "crates/ensabc-core/src/summary.rs",
    "crates/ensabc-core/examples/m1_runner.rs",
    "crates/ensabc-duckdb/Cargo.toml",
    "crates/ensabc-duckdb/src/lib.rs",
    "crates/ensabc-duckdb/src/cluster.rs",
    "crates/ensabc-duckdb/src/output.rs",
    # M1 validation scripts
    "validation/m1/setup_env.sh",
    "validation/m1/common.py",
    "validation/m1/schema.py",
    "validation/m1/prepare_input.py",
    "validation/m1/run_reference.py",
    "validation/m1/verify_core.py",
    "validation/m1/compare_results.py",
    "validation/m1/build_extension.sh",
    "validation/m1/verify_extension.py",
    "validation/m1/verify_minimal_runtime.sh",
    "validation/m1/measure_tasks.py",
    "validation/m1/reproduce_independently.py",
    "validation/m1/traceability.py",
    "validation/m1/verify_report.py",
    "validation/m1/run_m1_gate.py",
    "validation/m1/test_evidence.py",
    # M1 fixtures
    "validation/m1/fixtures/native_config.json",
    "validation/m1/fixtures/comparison_rules.json",
    "validation/m1/fixtures/synthetic_tasks.json",
    "validation/m1/examples/real_sample.sql",
    # Reused M0 dependencies
    "validation/m0/common.py",
    "validation/m0/schema.py",
    "validation/m0/run_reference.py",
    "validation/m0/compare_reference.py",
    "validation/m0/verify_extension.py",
    "validation/m0/extract_sample.py",
    "validation/m0/requirements-data.lock",
    "validation/m0/requirements-reference.lock",
    "validation/m0/fixtures/reference_config.json",
    "validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json",
)

EXCLUDED_SOURCE_PATTERNS = (
    ".env",
    "*.key",
    "*.secret",
    "*.pem",
    "*_data.json",
    "artifacts/",
    "target/",
    "__pycache__/",
    ".git/",
)


class M1Error(RuntimeError):
    """Raised when an M1 validation step fails."""

    def __init__(self, message: str, *, location: str | None = None, code: str | None = None):
        super().__init__(message)
        self.location = location
        self.code = code

    def __str__(self) -> str:
        prefix = f"[{self.code}] " if self.code else ""
        loc = f" at {self.location}" if self.location else ""
        return f"{prefix}{super().__str__()}{loc}"


def canonical_json_bytes(payload: Any) -> bytes:
    """Serialize payload to UTF-8 canonical JSON bytes without NaN/Infinity."""
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_text(payload: Any) -> str:
    """Render payload as canonical JSON text with a trailing newline."""
    return canonical_json_bytes(payload).decode("utf-8") + "\n"


def _reject_non_finite_literal(name: str) -> float:
    raise M1Error(f"Non-standard JSON literal {name!r} is forbidden in M1 evidence", code="INVALID_JSON")


def read_json(path: Path | str) -> Any:
    """Read a JSON document, strictly rejecting non-standard NaN/Infinity."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
        return json.loads(text, parse_constant=_reject_non_finite_literal)
    except Exception as e:
        if isinstance(e, M1Error):
            raise
        raise M1Error(f"Failed to read JSON file {p}: {e}", location=str(p), code="JSON_READ_ERROR") from e


def write_json(path: Path | str, payload: Any) -> str:
    """Write payload to path as pretty JSON with sorted keys and return file SHA-256."""
    p = Path(path)
    text = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return sha256_file(p)


def sha256_bytes(data: bytes) -> str:
    """Return lowercase hex SHA-256 digest of data bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path | str) -> str:
    """Return lowercase hex SHA-256 digest of file at path."""
    p = Path(path)
    digest = hashlib.sha256()
    with p.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_json(payload: Any) -> str:
    """Return lowercase hex SHA-256 digest of canonical JSON encoding of payload."""
    return sha256_bytes(canonical_json_bytes(payload))


def semantic_config_hash(config: dict[str, Any]) -> str:
    """Return hash of semantic calculation fields only (ignoring comments/descriptions)."""
    semantic_fields = (
        "projection_dim",
        "cluster_count",
        "initialization",
        "seed",
        "restarts",
        "initial_centers",
        "stopping_rule",
        "tolerance",
        "max_iterations",
        "rank_threshold",
    )
    filtered = {k: config[k] for k in semantic_fields if k in config}
    return sha256_json(filtered)


def is_finite_number(value: Any) -> bool:
    """Return True for real, finite number (excluding bool)."""
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    return math.isfinite(value)


def all_finite(values: Iterable[Any]) -> bool:
    """Return True when every item in values is a finite number."""
    return all(is_finite_number(v) for v in values)


def create_run_dir(run_id: str, base_dir: Path = ARTIFACTS_DIR) -> Path:
    """Create a unique run directory, refusing collision."""
    if not run_id or not run_id.strip():
        raise M1Error("run-id must be non-empty", code="EMPTY_RUN_ID")
    run_dir = base_dir / run_id.strip()
    if run_dir.exists():
        raise M1Error(f"Run directory already exists at {run_dir}: refusing collision", location=str(run_dir), code="DIR_COLLISION")
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def run_command(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    """Execute command with standard library subprocess."""
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
        raise M1Error(
            f"Command failed with code {completed.returncode}: {' '.join(command)}\n"
            f"stdout: {completed.stdout.strip()}\n"
            f"stderr: {completed.stderr.strip()}",
            code="COMMAND_FAILED",
        )
    return completed


def git_output(*args: str) -> str | None:
    """Return output of git command in REPO_ROOT or None if failed."""
    try:
        completed = run_command(["git", *args], cwd=REPO_ROOT, check=False, timeout=60)
        if completed.returncode == 0:
            return completed.stdout.strip()
    except Exception:
        pass
    return None


@dataclass(frozen=True)
class SourceManifestEntry:
    path: str
    sha256: str
    is_tracked: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "is_tracked": self.is_tracked,
        }


def is_relevant_source_file(rel_path: str) -> bool:
    """Return True if rel_path is an allowed source or fixture file (not secrets or payload)."""
    p = Path(rel_path)
    # Check exclusions
    for pattern in EXCLUDED_SOURCE_PATTERNS:
        if pattern.endswith("/"):
            if rel_path.startswith(pattern) or f"/{pattern}" in rel_path:
                return False
        elif pattern.startswith("*"):
            if p.name.endswith(pattern[1:]):
                return False
        elif pattern in rel_path:
            return False

    # Check known targets or code directories
    if rel_path in SOURCE_HASH_TARGETS:
        return True
    if rel_path.startswith("crates/") and (rel_path.endswith(".rs") or rel_path.endswith(".toml")):
        return True
    if rel_path.startswith("validation/m1/") and (rel_path.endswith(".py") or rel_path.endswith(".sh") or rel_path.endswith(".json") or rel_path.endswith(".sql")):
        return True
    return False


def collect_source_identity() -> dict[str, Any]:
    """Collect source identity including commit, dirty status, and all relevant files and hashes."""
    head_commit = git_output("rev-parse", "HEAD")
    status_porcelain = git_output("status", "--porcelain") or ""
    status_lines = [line for line in status_porcelain.splitlines() if line.strip()]

    files_dict: dict[str, SourceManifestEntry] = {}

    # Check explicit targets
    for rel_path in SOURCE_HASH_TARGETS:
        fpath = REPO_ROOT / rel_path
        if fpath.is_file():
            # Check if tracked by git
            is_tracked = git_output("ls-files", "--error-unmatch", rel_path) is not None
            files_dict[rel_path] = SourceManifestEntry(
                path=rel_path,
                sha256=sha256_file(fpath),
                is_tracked=is_tracked,
            )

    # Check untracked files
    for line in status_lines:
        if line.startswith("?? "):
            rel_path = line[3:].strip()
            fpath = REPO_ROOT / rel_path
            if fpath.is_file() and is_relevant_source_file(rel_path):
                files_dict[rel_path] = SourceManifestEntry(
                    path=rel_path,
                    sha256=sha256_file(fpath),
                    is_tracked=False,
                )
            elif fpath.is_dir():
                for subfile in fpath.rglob("*"):
                    if subfile.is_file():
                        sub_rel = str(subfile.relative_to(REPO_ROOT))
                        if is_relevant_source_file(sub_rel):
                            files_dict[sub_rel] = SourceManifestEntry(
                                path=sub_rel,
                                sha256=sha256_file(subfile),
                                is_tracked=False,
                            )

    manifest_entries = [files_dict[k].to_dict() for k in sorted(files_dict.keys())]
    manifest_bytes = canonical_json_bytes(manifest_entries)
    manifest_sha256 = sha256_bytes(manifest_bytes)

    return {
        "git_commit": head_commit,
        "git_dirty": bool(status_lines),
        "status_porcelain": status_lines,
        "manifest_sha256": manifest_sha256,
        "files_count": len(manifest_entries),
        "files": manifest_entries,
    }


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


def environment_snapshot() -> dict[str, Any]:
    """Record reproduction-relevant tool versions."""
    return {
        "schema_version": "m1.environment.v1",
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
        "created_at": utc_now(),
    }


def utc_now() -> str:
    """Return current UTC time in ISO 8601 format with Z suffix."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
