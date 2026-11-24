#!/usr/bin/env python3
"""Shared helpers for M2 scenario tracking validation scripts.

Implements M2 evidence and validation primitives:
- Strict JSON loading and canonical serialization (rejecting NaN/Infinity, normalizing -0.0)
- Content addressing: SHA-256 for bytes, files, and canonical JSON
- Atomic file writes avoiding partial state
- Unique run-directory creation refusing directory collision
- Located validation errors (M2Error / LocatedError)
- Relevant source inventory with dirty tracked and untracked file tracking
- Semantic binding creation excluding run noise (paths/timestamps)
- Logic identity verification using (birth_ordinal, birth_canonical_rank)
- Standard library only
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
VALIDATION_DIR = REPO_ROOT / "validation" / "m2"
FIXTURES_DIR = VALIDATION_DIR / "fixtures"
M1_VALIDATION_DIR = REPO_ROOT / "validation" / "m1"
ARTIFACTS_DIR = REPO_ROOT / "artifacts" / "m2"

SOURCE_HASH_TARGETS = (
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    # M1 + M2 Rust crates
    "crates/ensabc-core/Cargo.toml",
    "crates/ensabc-core/src/lib.rs",
    "crates/ensabc-core/src/preprocess.rs",
    "crates/ensabc-core/src/kmeans.rs",
    "crates/ensabc-core/src/summary.rs",
    "crates/ensabc-core/src/fsum.rs",
    "crates/ensabc-core/src/dispersion.rs",
    "crates/ensabc-core/src/matching.rs",
    "crates/ensabc-core/examples/m1_runner.rs",
    "crates/ensabc-core/examples/m2_runner.rs",
    "crates/ensabc-duckdb/Cargo.toml",
    "crates/ensabc-duckdb/src/lib.rs",
    "crates/ensabc-duckdb/src/match_ext.rs",
    # M2 SQL scripts
    "validation/m2/sql/schema.sql",
    "validation/m2/sql/stage_and_publish.sql",
    "validation/m2/sql/real_8run_replay.sql",
    # M2 validation scripts
    "validation/m2/setup_env.sh",
    "validation/m2/common.py",
    "validation/m2/schema.py",
    "validation/m2/oracle.py",
    "validation/m2/build_extension.sh",
    "validation/m2/verify_matching_core.py",
    "validation/m2/test_sql_history.py",
    "validation/m2/verify_sql_and_history.py",
    "validation/m2/prepare_om_sample.py",
    "validation/m2/test_om_sample.py",
    "validation/m2/verify_om_paths.py",
    "validation/m2/test_stability.py",
    "validation/m2/run_case_and_sensitivity.py",
    "validation/m2/measure_pipeline.py",
    "validation/m2/run_m1_regression.py",
    "validation/m2/verify_minimal_runtime.sh",
    "validation/m2/test_evidence.py",
    "validation/m2/traceability.py",
    "validation/m2/verify_report.py",
    "validation/m2/minimal_runtime.py",
    "validation/m2/test_review_regressions.py",
    "validation/m2/run_m2_gate.py",
    "validation/m2/reproduce_independently.py",
    # Requirements locks & fixtures
    "validation/m2/requirements-om.lock",
    "validation/m2/fixtures/duckomo_releases.json",
    "validation/m2/fixtures/wb2_8run_source.json",
    "validation/m2/fixtures/spatial_weights_eu3x3.json",
    "validation/m2/fixtures/tracking_experiment_config.json",
    "validation/m2/fixtures/synthetic_match_cases.json",
)


class M2Error(Exception):
    """Base error for M2 validation."""


class LocatedError(M2Error):
    """Validation failure with file and contextual location."""

    def __init__(self, message: str, *, file: str | Path | None = None, location: str | None = None) -> None:
        self.message = message
        self.file = str(file) if file is not None else None
        self.location = location
        prefix = f"{self.file}: " if self.file else ""
        loc = f"[{self.location}] " if self.location else ""
        super().__init__(f"{prefix}{loc}{message}")


def is_finite_number(value: Any) -> bool:
    """Return True if value is an int or finite float."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    return False


def all_finite(values: Iterable[Any]) -> bool:
    """Return True if all items in values are finite numbers."""
    return all(is_finite_number(v) for v in values)


def normalize_floats(obj: Any) -> Any:
    """Recursively normalize negative zeros to positive zero."""
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise ValueError(f"Non-finite float value encountered: {obj}")
        return 0.0 if obj == 0.0 else obj
    if isinstance(obj, dict):
        return {k: normalize_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_floats(v) for v in obj]
    return obj


def canonical_json_bytes(obj: Any) -> bytes:
    """Serialize object to deterministic UTF-8 bytes (sorted keys, compact, finite floats)."""
    normalized = normalize_floats(obj)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        indent=None,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_str(obj: Any, *, indent: int | None = 2) -> str:
    """Serialize object to formatted canonical JSON string."""
    normalized = normalize_floats(obj)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        indent=indent,
        allow_nan=False,
    )


def load_json(path: str | Path) -> Any:
    """Load and parse JSON from a file, rejecting NaN and Infinity."""
    p = Path(path)
    if not p.is_file():
        raise LocatedError("JSON file not found", file=p)
    text = p.read_text(encoding="utf-8")
    try:
        return json.loads(text, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(f"Illegal constant {c}")))
    except Exception as e:
        raise LocatedError(f"Failed to parse JSON: {e}", file=p) from e


def sha256_bytes(data: bytes) -> str:
    """Compute hex SHA-256 digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | Path) -> str:
    """Compute hex SHA-256 digest of file contents."""
    p = Path(path)
    if not p.is_file():
        raise LocatedError("Cannot hash non-existent file", file=p)
    h = hashlib.sha256()
    with p.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def sha256_json(obj: Any) -> str:
    """Compute hex SHA-256 digest of canonical JSON serialization."""
    return sha256_bytes(canonical_json_bytes(obj))


def atomic_write(path: str | Path, content: str | bytes) -> None:
    """Atomically write content to path via temporary file and replace."""
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    encoding = None if isinstance(content, bytes) else "utf-8"
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=".tmp_write_", dir=str(p.parent))
    try:
        with os.fdopen(tmp_fd, mode, encoding=encoding) as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, str(p))
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def atomic_write_json(path: str | Path, obj: Any, *, indent: int | None = 2) -> None:
    """Atomically serialize and write object as canonical JSON."""
    atomic_write(path, canonical_json_str(obj, indent=indent))


def create_run_dir(run_id: str, *, base_dir: Path | None = None, allow_existing: bool = False) -> Path:
    """Create an exclusive run directory. Refuse collision unless allow_existing is True."""
    base = base_dir or ARTIFACTS_DIR
    target = base / run_id
    if target.exists() and not allow_existing:
        raise LocatedError(f"Run directory already exists: {target}. Refusing to overwrite evidence.", file=target)
    target.mkdir(parents=True, exist_ok=True)
    return target


def get_git_commit(repo_path: Path = REPO_ROOT) -> str:
    """Get current HEAD commit hash."""
    try:
        res = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return res.stdout.strip()
    except Exception:
        return "unversioned"


def collect_source_manifest(repo_path: Path = REPO_ROOT) -> dict[str, Any]:
    """Inventory relevant source files and their sha256 hashes."""
    sources: dict[str, str] = {}
    missing: list[str] = []
    for rel in SOURCE_HASH_TARGETS:
        full = repo_path / rel
        if full.is_file():
            sources[rel] = sha256_file(full)
        else:
            missing.append(rel)
    return {
        "head_commit": get_git_commit(repo_path),
        "tracked_sources_count": len(sources),
        "missing_sources_count": len(missing),
        "sources": sources,
        "missing": missing,
        "manifest_sha256": sha256_json(sources),
    }


def parse_duckdb_json(stdout: str) -> list[Any]:
    """Decode the concatenated JSON arrays emitted by DuckDB CLI."""
    decoder = json.JSONDecoder()
    results = []
    index = 0
    while index < len(stdout):
        if stdout[index].isspace():
            index += 1
            continue
        value, index = decoder.raw_decode(stdout, index)
        results.append(value)
    return results


def run_isolated_duckdb(sql: str, *, cli: str = "duckdb", database: str = ":memory:",
                        timeout: float = 120, bail: bool = True) -> subprocess.CompletedProcess:
    """Run with a fresh HOME and extension directory; verify explicit DuckOMO loads."""
    import re
    releases = load_json(FIXTURES_DIR / "duckomo_releases.json")
    for match in list(re.finditer(r"LOAD\s+'([^']*duckomo\.duckdb_extension)'\s*;", sql, re.I)):
        path = Path(match.group(1)).resolve()
        digest = sha256_file(path)
        release = next((r for r in releases.values() if r["extension_sha256"] == digest), None)
        if release is None:
            raise ValueError(f"Unverified DuckOMO extension: {path}")
        guard = ("\nSELECT CASE WHEN count(*) = 1 THEN true ELSE error('DuckOMO identity mismatch') END "
                 "FROM duckdb_extensions() WHERE extension_name = 'duckomo' AND loaded "
                 f"AND extension_version = '{release['extension_version']}' AND install_mode = 'NOT_INSTALLED';")
        # Avoid introducing a result array into callers' output stream.
        guard = guard.replace("SELECT CASE", "CREATE OR REPLACE TEMP TABLE m2_extension_identity AS SELECT CASE")
        sql = sql.replace(match.group(0), match.group(0) + guard)
    with tempfile.TemporaryDirectory(prefix="ensabc-m2-duckdb-") as directory:
        env = os.environ.copy()
        env["HOME"] = directory
        prefix = (f"SET extension_directory = '{directory}/extensions';\n"
                  "SET autoinstall_known_extensions = false;\n"
                  "SET autoload_known_extensions = false;\nSET TimeZone = 'UTC';\n")
        # Remove callers' legacy shared extension directories.
        sql = re.sub(r"SET\s+extension_directory\s*=\s*'[^']*'\s*;", "", sql, flags=re.I)
        command = [cli, "-unsigned", "-json"] + (["-bail"] if bail else []) + [database]
        return subprocess.run(command, input=prefix + sql, capture_output=True, text=True,
                              timeout=timeout, env=env)


def sql_value(value: Any) -> str:
    """Encode JSON-compatible values as DuckDB SQL literals, without identifier quotes."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, dict):
        return "{" + ", ".join(sql_value(k) + ": " + sql_value(v) for k, v in value.items()) + "}"
    if isinstance(value, list):
        return "[" + ", ".join(sql_value(v) for v in value) + "]"
    return json.dumps(value, allow_nan=False)


def real_replay_binding(sample_dir: Path, extension_path: Path, duckomo_path: Path) -> dict[str, Any]:
    """Bind actual ordered OM payloads, frozen configuration, weights and software."""
    manifest = load_json(sample_dir / "om_sample_manifest.json")
    inputs = [{"ordinal": b["ordinal"], "metadata": b,
               "om_sha256": sha256_file(sample_dir / b["om_filename"])}
              for b in manifest["batches"]]
    scenario_config = {"projection_dim": 2, "cluster_count": 3,
                       "initialization": "kmeans++_chacha8_v1", "seed": 3,
                       "restarts": 10, "initial_centers": [],
                       "stopping_rule": "sklearn_variance_scaled_v1", "tolerance": 0.0001,
                       "max_iterations": 300, "rank_threshold": 1e-10}
    weights = load_json(FIXTURES_DIR / "spatial_weights_eu3x3.json")
    binding = {
        "input_manifest_sha256": sha256_json(inputs),
        "scenario_config_sha256": sha256_json(scenario_config),
        "match_config_sha256": sha256_json({"threshold_coefficient": 1.0, "algorithm": "m2.match.v1"}),
        "weights_version": sha256_json(weights), "threshold_coefficient": 1.0, "cluster_count": 3,
        "software": {"ensabc_extension_sha256": sha256_file(extension_path),
                     "duckomo_version": duckomo_path.parent.name,
                     "duckomo_sha256": sha256_file(duckomo_path), "duckdb_version": "v1.5.5",
                     "cluster_algorithm_version": "m1.native.v1", "match_algorithm_version": "m2.match.v1"},
    }
    return {"run_id": sha256_json(binding), **binding}


def compute_semantic_run_key(
    *,
    input_manifest_hash: str,
    clustering_config_hash: str,
    matching_config_hash: str,
    spatial_weights_hash: str,
    ensabc_extension_hash: str,
) -> str:
    """Compute semantic 16-hex run_key excluding path/timestamp noise."""
    content = {
        "input_manifest_sha256": input_manifest_hash,
        "clustering_config_sha256": clustering_config_hash,
        "matching_config_sha256": matching_config_hash,
        "spatial_weights_sha256": spatial_weights_hash,
        "ensabc_extension_sha256": ensabc_extension_hash,
    }
    return sha256_bytes(canonical_json_bytes(content))[:16]
