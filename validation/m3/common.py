#!/usr/bin/env python3
"""Shared helpers for M3 scenario evolution validation scripts (T004).

Implements M3 evidence, reproducibility, and DuckDB isolation primitives:
- Exclusive run-directory creation refusing collision unless explicitly allowed
- Strict JSON loading and canonical serialization normalizing -0.0 -> 0.0 with
  lossless IEEE-754 finite float round-trip verification
- Atomic text, bytes, and canonical JSON file writes via temp-file + fsync + replace
- Content addressing: SHA-256 for bytes, files, and canonical JSON objects
- Effective source inventory covering crates/**, validation/m0..m3/**, and
  specs/004-m3-scenario-evolution/** while excluding gitignored derived payloads
- Isolated DuckDB CLI runner (fresh HOME + extension_directory, autoinstall/autoload
  disabled, verified DuckOMO SHA-256 & NOT_INSTALLED identity guard)
- Version-binding builders for M2 tracking_runs and M3 evolution_runs
- Semantic evolution projection verifying authentic tracking_run_key[:12]: prefixes
  on every branch_id before projecting to (birth_ordinal, birth_canonical_rank)
- Python 3.13 standard library only
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
VALIDATION_DIR = REPO_ROOT / "validation" / "m3"
FIXTURES_DIR = VALIDATION_DIR / "fixtures"
SQL_DIR = VALIDATION_DIR / "sql"

M0_VALIDATION_DIR = REPO_ROOT / "validation" / "m0"
M1_VALIDATION_DIR = REPO_ROOT / "validation" / "m1"
M2_VALIDATION_DIR = REPO_ROOT / "validation" / "m2"
M2_FIXTURES_DIR = M2_VALIDATION_DIR / "fixtures"
M2_CACHE_DIR = M2_VALIDATION_DIR / ".cache"

ARTIFACTS_DIR = REPO_ROOT / "artifacts" / "m3"

NON_PHYSICAL_DISCLAIMER = "algorithmic_attribution_only_not_physical_member_migration"
CLUSTER_ALGORITHM_VERSION = "m1.native.v1"
MATCH_ALGORITHM_VERSION = "m2.match.v1"
EVOLUTION_ALGORITHM_VERSION = "m3.evolve.v1"

TOP_LEVEL_SOURCE_FILES = (
    ".gitignore",
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
)

EFFECTIVE_SOURCE_TREES = (
    "crates",
    "validation/m0",
    "validation/m1",
    "validation/m2",
    "validation/m3",
    "specs/004-m3-scenario-evolution",
)

CODE_SOURCE_PREFIXES = (
    "crates/",
    "validation/m0/",
    "validation/m1/",
    "validation/m2/",
    "validation/m3/",
)

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

EXCLUDED_SOURCE_REL_PATHS = frozenset({
    "validation/m0/fixtures/real_ifs_ens_20200101_t2m_data.json",
})

EXCLUDED_DIR_NAMES = frozenset({
    "__pycache__",
    ".venv",
    ".cache",
    "data",
    "target",
    ".git",
})

_BRANCH_SUFFIX_RE = re.compile(r"^b(\d+):k(\d+)$")


class M3Error(Exception):
    """Base error for M3 validation."""


class LocatedError(M3Error):
    """Validation failure carrying file and contextual location."""

    def __init__(
        self,
        message: str,
        *,
        file: str | Path | None = None,
        location: str | None = None,
    ) -> None:
        self.message = message
        self.file = str(file) if file is not None else None
        self.location = location
        prefix = f"{self.file}: " if self.file else ""
        loc = f"[{self.location}] " if self.location else ""
        super().__init__(f"{prefix}{loc}{message}")


def is_finite_number(value: Any) -> bool:
    """Return True if value is an int (not bool) or finite float."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    return False


def all_finite(values: Iterable[Any]) -> bool:
    """Return True if every item in values is a finite int or float."""
    return all(is_finite_number(v) for v in values)


def normalize_floats(obj: Any) -> Any:
    """Recursively normalize -0.0 -> 0.0, reject non-finite floats, and verify round-trip."""
    if isinstance(obj, bool) or obj is None or isinstance(obj, (int, str)):
        return obj
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise ValueError(f"Non-finite float value encountered: {obj!r}")
        val = 0.0 if obj == 0.0 else obj
        # Python 3.13 repr/json.dumps uses shortest round-trip decimal representation.
        rt = float(repr(val))
        if rt != val or math.copysign(1.0, rt) != math.copysign(1.0, val):
            raise ValueError(f"Float failed lossless round-trip check: {val!r}")
        return val
    if isinstance(obj, dict):
        return {str(k): normalize_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [normalize_floats(v) for v in obj]
    raise TypeError(f"Unsupported type for canonical JSON normalization: {type(obj).__name__}")


def canonical_json_bytes(obj: Any) -> bytes:
    """Serialize object to deterministic compact UTF-8 JSON bytes."""
    normalized = normalize_floats(obj)
    text = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        indent=None,
        separators=(",", ":"),
        allow_nan=False,
    )
    return text.encode("utf-8")


def canonical_json_str(obj: Any, *, indent: int | None = 2) -> str:
    """Serialize object to formatted canonical JSON string with trailing newline."""
    normalized = normalize_floats(obj)
    text = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        indent=indent,
        allow_nan=False,
    )
    return text + ("\n" if indent is not None else "")


def load_json(path: str | Path) -> Any:
    """Load and parse JSON from a file, rejecting NaN and Infinity constants."""
    p = Path(path)
    if not p.is_file():
        raise LocatedError("JSON file not found", file=p)
    text = p.read_text(encoding="utf-8")
    try:
        return json.loads(
            text,
            parse_constant=lambda c: (_ for _ in ()).throw(ValueError(f"Illegal constant {c}")),
        )
    except Exception as exc:
        raise LocatedError(f"Failed to parse JSON: {exc}", file=p) from exc


def is_hex_sha256(value: Any) -> bool:
    """Return True iff value is a lowercase 64-character hexadecimal SHA-256 string."""
    return isinstance(value, str) and _HEX64_RE.fullmatch(value) is not None


def ensure_no_symlink_inside(run_dir: Path, target: Path) -> None:
    """Reject if run_dir or any path component from run_dir to target is a symlink."""
    if run_dir.is_symlink():
        raise LocatedError(f"Symlink run_dir is forbidden: {run_dir}", file=run_dir)
    if target.is_symlink():
        raise LocatedError(f"Symlink target is forbidden: {target}", file=target)
    base = run_dir.resolve()
    try:
        rel = target.relative_to(base)
    except ValueError:
        try:
            rel = target.relative_to(run_dir)
        except ValueError as exc:
            raise LocatedError(f"Target path escapes run_dir: {target}", file=target) from exc
    if ".." in rel.parts:
        raise LocatedError(f"Parent traversal '..' forbidden inside run_dir: {target}", file=target)
    cur = base
    for part in rel.parts:
        cur = cur / part
        if cur.is_symlink():
            raise LocatedError(
                f"Symlink forbidden inside run_dir: {cur.relative_to(base).as_posix()}",
                file=cur,
            )


def sha256_bytes(data: bytes) -> str:
    """Compute hex SHA-256 digest of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | Path) -> str:
    """Compute hex SHA-256 digest of a file."""
    p = Path(path)
    if not p.is_file():
        raise LocatedError("Cannot hash non-existent file", file=p)
    h = hashlib.sha256()
    with p.open("rb") as fh:
        while chunk := fh.read(65536):
            h.update(chunk)
    return h.hexdigest()


def canonical_json_sha256(obj: Any) -> str:
    """Compute hex SHA-256 digest of canonical JSON serialization."""
    return sha256_bytes(canonical_json_bytes(obj))


sha256_json = canonical_json_sha256


def atomic_write(path: str | Path, content: str | bytes) -> None:
    """Atomically write bytes or UTF-8 text to path via temporary file + fsync + replace."""
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    encoding = None if isinstance(content, bytes) else "utf-8"
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=".tmp_write_", dir=str(p.parent))
    try:
        with os.fdopen(tmp_fd, mode, encoding=encoding) as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, str(p))
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def atomic_write_text(path: str | Path, text: str) -> None:
    """Atomically write UTF-8 text to path."""
    if not isinstance(text, str):
        raise TypeError(f"atomic_write_text expected str, got {type(text).__name__}")
    atomic_write(path, text)


def atomic_write_json(path: str | Path, obj: Any, *, indent: int | None = 2) -> None:
    """Atomically serialize and write object as canonical JSON."""
    atomic_write(path, canonical_json_str(obj, indent=indent))


def create_run_dir(
    run_id: str,
    *,
    base_dir: Path | None = None,
    allow_existing: bool = False,
) -> Path:
    """Create an exclusive run directory; refuse collision unless allow_existing is True."""
    if not run_id or "/" in run_id or "\\" in run_id or run_id in {".", ".."}:
        raise LocatedError(f"Invalid run_id: {run_id!r}")
    base = (base_dir or ARTIFACTS_DIR).resolve()
    target = (base / run_id).resolve()
    if target.parent != base:
        raise LocatedError(f"Run directory escapes base directory: {target}", file=target)
    if target.exists() and not allow_existing:
        raise LocatedError(
            f"Run directory already exists: {target}. Refusing to overwrite evidence.",
            file=target,
        )
    target.mkdir(parents=True, exist_ok=True)
    return target


def get_git_commit(repo_path: Path = REPO_ROOT) -> str:
    """Return current HEAD commit SHA, or 'unversioned' if unavailable."""
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


def _is_excluded_source_path(rel_posix: str) -> bool:
    if rel_posix in EXCLUDED_SOURCE_REL_PATHS:
        return True
    parts = rel_posix.split("/")
    if any(part in EXCLUDED_DIR_NAMES for part in parts[:-1]):
        return True
    filename = parts[-1]
    if filename.startswith(".tmp_") or filename.endswith((".pyc", ".pyo", ".pyd", ".rs.bk")):
        return True
    if filename.endswith((".om", ".duckdb", ".duckdb.wal")):
        return True
    return False


def collect_effective_source_manifest(repo_path: Path = REPO_ROOT) -> dict[str, Any]:
    """Inventory effective source files across crates/**, validation/m0..m3/**, and M3 specs."""
    repo = repo_path.resolve()
    sources: dict[str, str] = {}

    for rel_top in TOP_LEVEL_SOURCE_FILES:
        fp = repo / rel_top
        if fp.is_file():
            sources[rel_top] = sha256_file(fp)

    for tree_rel in EFFECTIVE_SOURCE_TREES:
        tree_root = repo / tree_rel
        if not tree_root.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(tree_root):
            dirnames[:] = sorted(d for d in dirnames if d not in EXCLUDED_DIR_NAMES)
            for fname in sorted(filenames):
                full_path = Path(dirpath) / fname
                if not full_path.is_file():
                    continue
                rel_posix = full_path.relative_to(repo).as_posix()
                if _is_excluded_source_path(rel_posix):
                    continue
                sources[rel_posix] = sha256_file(full_path)

    dirty_entries: list[str] = []
    try:
        status_proc = subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "status",
                "--porcelain",
                "--untracked-files=all",
                "--",
                *TOP_LEVEL_SOURCE_FILES,
                *EFFECTIVE_SOURCE_TREES,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        dirty_entries = [line for line in status_proc.stdout.splitlines() if line.strip()]
    except Exception:
        dirty_entries = []

    ordered_sources = dict(sorted(sources.items()))
    code_sources = [
        {"path": rel_posix, "sha256": digest}
        for rel_posix, digest in ordered_sources.items()
        if (
            rel_posix in TOP_LEVEL_SOURCE_FILES
            or rel_posix.startswith(CODE_SOURCE_PREFIXES)
        )
        and not rel_posix.endswith(".md")
    ]
    code_manifest_sha256 = sha256_bytes(
        "".join(f"{s['path']}:{s['sha256']}\n" for s in code_sources).encode("utf-8")
    )
    return {
        "schema_version": "m3.source_manifest.v1",
        "head_commit": get_git_commit(repo),
        "tracked_and_effective_count": len(ordered_sources),
        "code_sources_count": len(code_sources),
        "dirty_or_untracked_count": len(dirty_entries),
        "dirty_or_untracked": dirty_entries,
        "sources": ordered_sources,
        "manifest_sha256": canonical_json_sha256(ordered_sources),
        "code_manifest_sha256": code_manifest_sha256,
    }


collect_source_manifest = collect_effective_source_manifest


def parse_duckdb_json(stdout: str) -> list[Any]:
    """Decode concatenated JSON values/arrays emitted by DuckDB CLI -json mode."""
    decoder = json.JSONDecoder(
        parse_constant=lambda c: (_ for _ in ()).throw(ValueError(f"Illegal constant {c}"))
    )
    results: list[Any] = []
    index = 0
    length = len(stdout)
    while index < length:
        if stdout[index].isspace():
            index += 1
            continue
        value, index = decoder.raw_decode(stdout, index)
        results.append(value)
    return results


def run_isolated_duckdb(
    sql: str,
    *,
    cli: str = "duckdb",
    database: str = ":memory:",
    timeout: float = 120,
    bail: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Execute SQL in DuckDB CLI with isolated HOME/extension_directory and DuckOMO hash guard."""
    releases = load_json(M2_FIXTURES_DIR / "duckomo_releases.json")
    for match in list(re.finditer(r"LOAD\s+'([^']*duckomo\.duckdb_extension)'\s*;", sql, re.I)):
        ext_path = Path(match.group(1)).resolve()
        digest = sha256_file(ext_path)
        release = next((r for r in releases.values() if r["extension_sha256"] == digest), None)
        if release is None:
            raise LocatedError(f"Unverified DuckOMO extension binary: {ext_path}", file=ext_path)
        guard = (
            "\nCREATE OR REPLACE TEMP TABLE m3_extension_identity AS "
            "SELECT CASE WHEN count(*) = 1 THEN true ELSE error('DuckOMO identity mismatch') END "
            "FROM duckdb_extensions() WHERE extension_name = 'duckomo' AND loaded "
            f"AND extension_version = '{release['extension_version']}' "
            "AND install_mode = 'NOT_INSTALLED';"
        )
        sql = sql.replace(match.group(0), match.group(0) + guard)

    with tempfile.TemporaryDirectory(prefix="ensabc-m3-duckdb-") as directory:
        env = os.environ.copy()
        env["HOME"] = directory
        prefix = (
            f"SET extension_directory = '{directory}/extensions';\n"
            "SET autoinstall_known_extensions = false;\n"
            "SET autoload_known_extensions = false;\n"
            "SET TimeZone = 'UTC';\n"
        )
        cleaned_sql = re.sub(r"SET\s+extension_directory\s*=\s*'[^']*'\s*;", "", sql, flags=re.I)
        command = [cli, "-unsigned", "-json"] + (["-bail"] if bail else []) + [str(database)]
        return subprocess.run(
            command,
            input=prefix + cleaned_sql,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )


def sql_value(value: Any) -> str:
    """Encode a JSON-compatible Python value as a DuckDB SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Cannot encode non-finite float as SQL literal: {value!r}")
        norm = 0.0 if value == 0.0 else value
        return repr(norm)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{sql_value(str(k))}: {sql_value(v)}" for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(sql_value(v) for v in value) + "]"
    raise TypeError(f"Unsupported value type for sql_value: {type(value).__name__}")


def default_scenario_config() -> dict[str, Any]:
    """Return the frozen main M1 ScenarioConfig dictionary."""
    return {
        "projection_dim": 2,
        "cluster_count": 3,
        "initialization": "kmeans++_chacha8_v1",
        "seed": 3,
        "restarts": 10,
        "initial_centers": [],
        "stopping_rule": "sklearn_variance_scaled_v1",
        "tolerance": 0.0001,
        "max_iterations": 300,
        "rank_threshold": 1e-10,
    }


def default_evolution_config() -> dict[str, Any]:
    """Return the frozen main M3 EvolutionConfig dictionary."""
    return {
        "secondary_rule": "nearest_feasible_v1",
        "flow_rule": "star_min_mass_proportional_v1",
        "uncertainty_rule": "m3.uncertainty.v1",
        "fraction_sum_tolerance": 1e-9,
        "high_uncertainty_threshold_ratio": 0.10,
        "high_uncertainty_optimality_ratio": 0.10,
        "high_uncertainty_secondary_ratio": 0.10,
    }


def real_tracking_binding(
    sample_dir: Path,
    extension_path: Path,
    duckomo_path: Path,
    *,
    scenario_config: dict[str, Any] | None = None,
    threshold_coefficient: float = 1.0,
) -> dict[str, Any]:
    """Compute M2-compatible tracking_runs binding and run_key for an 8-batch OM sample."""
    manifest = load_json(sample_dir / "om_sample_manifest.json")
    inputs = [
        {
            "ordinal": b["ordinal"],
            "metadata": b,
            "om_sha256": sha256_file(sample_dir / b["om_filename"]),
        }
        for b in manifest["batches"]
    ]
    sc_cfg = normalize_floats(scenario_config or default_scenario_config())
    tc = normalize_floats(float(threshold_coefficient))
    weights = load_json(M2_FIXTURES_DIR / "spatial_weights_eu3x3.json")
    binding = {
        "input_manifest_sha256": canonical_json_sha256(inputs),
        "scenario_config_sha256": canonical_json_sha256(sc_cfg),
        "match_config_sha256": canonical_json_sha256(
            {"threshold_coefficient": tc, "algorithm": MATCH_ALGORITHM_VERSION}
        ),
        "weights_version": canonical_json_sha256(weights),
        "threshold_coefficient": tc,
        "cluster_count": int(sc_cfg["cluster_count"]),
        "software": {
            "ensabc_extension_sha256": sha256_file(extension_path),
            "duckomo_version": duckomo_path.parent.name,
            "duckomo_sha256": sha256_file(duckomo_path),
            "duckdb_version": "v1.5.5",
            "cluster_algorithm_version": CLUSTER_ALGORITHM_VERSION,
            "match_algorithm_version": MATCH_ALGORITHM_VERSION,
        },
    }
    run_id = canonical_json_sha256(binding)
    return {"run_id": run_id, **binding}


def real_evolution_binding(
    sample_dir: Path,
    extension_path: Path,
    duckomo_path: Path,
    *,
    scenario_config: dict[str, Any] | None = None,
    threshold_coefficient: float = 1.0,
    evolution_config: dict[str, Any] | None = None,
    tracking_run_key: str | None = None,
) -> dict[str, Any]:
    """Bind ordered OM payloads, M1/M2/M3 configs, spatial weights, and software into evolution_run_id."""
    sc_cfg = normalize_floats(scenario_config or default_scenario_config())
    ev_cfg = normalize_floats(evolution_config or default_evolution_config())
    tc = normalize_floats(float(threshold_coefficient))

    tracking = real_tracking_binding(
        sample_dir,
        extension_path,
        duckomo_path,
        scenario_config=sc_cfg,
        threshold_coefficient=tc,
    )
    resolved_tracking_key = tracking_run_key or tracking["run_id"]

    evolve_config_hash_payload = {
        "secondary_rule": ev_cfg["secondary_rule"],
        "flow_rule": ev_cfg["flow_rule"],
        "uncertainty_rule": ev_cfg.get("uncertainty_rule", "m3.uncertainty.v1"),
        "fraction_sum_tolerance": ev_cfg["fraction_sum_tolerance"],
        "high_uncertainty_threshold_ratio": ev_cfg["high_uncertainty_threshold_ratio"],
        "high_uncertainty_optimality_ratio": ev_cfg["high_uncertainty_optimality_ratio"],
        "high_uncertainty_secondary_ratio": ev_cfg["high_uncertainty_secondary_ratio"],
        "algorithm": EVOLUTION_ALGORITHM_VERSION,
    }

    evolution_binding = {
        "tracking_run_key": resolved_tracking_key,
        "input_manifest_sha256": tracking["input_manifest_sha256"],
        "scenario_config_sha256": tracking["scenario_config_sha256"],
        "match_config_sha256": tracking["match_config_sha256"],
        "evolve_config_sha256": canonical_json_sha256(evolve_config_hash_payload),
        "weights_version": tracking["weights_version"],
        "secondary_rule": ev_cfg["secondary_rule"],
        "flow_rule": ev_cfg["flow_rule"],
        "uncertainty_rule": ev_cfg.get("uncertainty_rule", "m3.uncertainty.v1"),
        "fraction_sum_tolerance": ev_cfg["fraction_sum_tolerance"],
        "high_uncertainty_threshold_ratio": ev_cfg["high_uncertainty_threshold_ratio"],
        "high_uncertainty_optimality_ratio": ev_cfg["high_uncertainty_optimality_ratio"],
        "high_uncertainty_secondary_ratio": ev_cfg["high_uncertainty_secondary_ratio"],
        "software": {
            "ensabc_extension_sha256": tracking["software"]["ensabc_extension_sha256"],
            "duckomo_version": tracking["software"]["duckomo_version"],
            "duckomo_sha256": tracking["software"]["duckomo_sha256"],
            "duckdb_version": tracking["software"]["duckdb_version"],
            "cluster_algorithm_version": CLUSTER_ALGORITHM_VERSION,
            "match_algorithm_version": MATCH_ALGORITHM_VERSION,
            "evolution_algorithm_version": EVOLUTION_ALGORITHM_VERSION,
        },
    }
    evolution_run_id = canonical_json_sha256(evolution_binding)
    return {
        "evolution_run_id": evolution_run_id,
        "tracking_run_key": resolved_tracking_key,
        "evolution_binding": evolution_binding,
        "tracking_binding": tracking,
    }


def parse_branch_birth_key(
    branch_id: str,
    *,
    expected_tracking_run_key: str,
    location: str | None = None,
) -> tuple[int, int]:
    """Verify authentic tracking_run_key[:12]: prefix on branch_id and return (birth_ordinal, birth_canonical_rank)."""
    if not isinstance(expected_tracking_run_key, str) or len(expected_tracking_run_key) < 12:
        raise LocatedError(
            f"expected_tracking_run_key must be at least 12 chars, got {expected_tracking_run_key!r}",
            location=location,
        )
    if not isinstance(branch_id, str):
        raise LocatedError(
            f"branch_id must be a string, got {type(branch_id).__name__}",
            location=location,
        )
    expected_prefix = f"{expected_tracking_run_key[:12]}:"
    if not branch_id.startswith(expected_prefix):
        raise LocatedError(
            f"Forged or mismatched branch_id prefix: expected {expected_prefix!r}, got {branch_id!r}",
            location=location,
        )
    suffix = branch_id[len(expected_prefix):]
    m = _BRANCH_SUFFIX_RE.match(suffix)
    if m is None:
        raise LocatedError(
            f"Malformed branch_id birth suffix: expected 'b<ord>:k<rank>', got {suffix!r} in {branch_id!r}",
            location=location,
        )
    return (int(m.group(1)), int(m.group(2)))


_SINGLE_BRANCH_ID_FIELDS = frozenset({
    "branch_id",
    "prev_branch_id",
    "curr_branch_id",
    "primary_branch_id",
    "split_from_parent_branch_id",
    "merged_into_branch_id",
})

_LIST_BRANCH_ID_FIELDS = frozenset({
    "branch_ids",
    "spawned_secondary_branch_ids",
    "absorbed_secondary_branch_ids",
})

_VOLATILE_RUN_FIELDS = frozenset({
    "evolution_run_id",
    "tracking_run_key",
    "content_sha256",
    "evolution_binding",
    "tracking_binding",
})


def _project_structure(
    obj: Any,
    *,
    expected_tracking_run_key: str,
    path: str,
) -> Any:
    if isinstance(obj, dict):
        projected: dict[str, Any] = {}
        for k, v in obj.items():
            if k in _VOLATILE_RUN_FIELDS:
                continue
            field_path = f"{path}.{k}" if path else k
            if k in _SINGLE_BRANCH_ID_FIELDS:
                if v is None or v == "":
                    projected[k] = None
                else:
                    b_ord, b_rank = parse_branch_birth_key(
                        v,
                        expected_tracking_run_key=expected_tracking_run_key,
                        location=field_path,
                    )
                    projected[k] = [b_ord, b_rank]
            elif k in _LIST_BRANCH_ID_FIELDS:
                if not isinstance(v, list):
                    raise LocatedError(
                        f"Expected list for {k}, got {type(v).__name__}",
                        location=field_path,
                    )
                projected[k] = [
                    list(
                        parse_branch_birth_key(
                            item,
                            expected_tracking_run_key=expected_tracking_run_key,
                            location=f"{field_path}[{idx}]",
                        )
                    )
                    for idx, item in enumerate(v)
                ]
            elif k in {"branches", "branch_summaries"} and isinstance(v, dict):
                items: list[dict[str, Any]] = []
                for map_bid, summary_obj in v.items():
                    b_ord, b_rank = parse_branch_birth_key(
                        map_bid,
                        expected_tracking_run_key=expected_tracking_run_key,
                        location=f"{field_path}[{map_bid!r}]",
                    )
                    proj_summary = _project_structure(
                        summary_obj,
                        expected_tracking_run_key=expected_tracking_run_key,
                        path=f"{field_path}[{map_bid!r}]",
                    )
                    if isinstance(proj_summary, dict):
                        if "branch_id" in proj_summary and proj_summary["branch_id"] != [b_ord, b_rank]:
                            raise LocatedError(
                                f"Branch map key {[b_ord, b_rank]} disagrees with inner branch_id {proj_summary['branch_id']}",
                                location=f"{field_path}[{map_bid!r}]",
                            )
                        if "birth_ordinal" in proj_summary and int(proj_summary["birth_ordinal"]) != b_ord:
                            raise LocatedError(
                                f"Branch birth_ordinal {proj_summary['birth_ordinal']} disagrees with branch_id birth_ordinal {b_ord}",
                                location=f"{field_path}[{map_bid!r}]",
                            )
                        proj_summary["branch_id"] = [b_ord, b_rank]
                    items.append(proj_summary)
                items.sort(key=lambda item: tuple(item["branch_id"]) if isinstance(item, dict) and "branch_id" in item else ())
                projected[k] = items
            else:
                projected[k] = _project_structure(
                    v,
                    expected_tracking_run_key=expected_tracking_run_key,
                    path=field_path,
                )

        if "birth_ordinal" in projected and isinstance(projected.get("branch_id"), list):
            if int(projected["birth_ordinal"]) != projected["branch_id"][0]:
                raise LocatedError(
                    f"Branch summary birth_ordinal {projected['birth_ordinal']} disagrees with branch_id {projected['branch_id']}",
                    location=path,
                )
        return projected

    if isinstance(obj, list):
        projected_list = [
            _project_structure(
                item,
                expected_tracking_run_key=expected_tracking_run_key,
                path=f"{path}[{idx}]",
            )
            for idx, item in enumerate(obj)
        ]
        # Sort branch summary lists deterministically by projected (birth_ordinal, birth_canonical_rank)
        if (
            projected_list
            and all(
                isinstance(it, dict)
                and isinstance(it.get("branch_id"), list)
                and "birth_ordinal" in it
                and "end_ordinal" in it
                for it in projected_list
            )
        ):
            projected_list.sort(key=lambda it: tuple(it["branch_id"]))
        return projected_list

    return obj


def semantic_evolution_projection(
    payload: dict[str, Any],
    *,
    expected_tracking_run_key: str | None = None,
    expected_run_key: str | None = None,
) -> dict[str, Any]:
    """Verify authentic tracking_run_key[:12]: prefixes on all branch_ids and project to [birth_ordinal, birth_canonical_rank].

    Strips build/path-derived version hashes (evolution_run_id, tracking_run_key,
    content_sha256, evolution_binding) while preserving all numeric metrics, event
    classifications, roles, margins, disclaimers, and birth-node identities.
    """
    if not isinstance(payload, dict):
        raise LocatedError(f"semantic_evolution_projection expected dict, got {type(payload).__name__}")

    run_key = expected_tracking_run_key or expected_run_key
    if run_key is None:
        run_key = payload.get("tracking_run_key")
        if run_key is None and isinstance(payload.get("run"), dict):
            run_key = payload["run"].get("tracking_run_key")
    if not isinstance(run_key, str) or len(run_key) < 12:
        raise LocatedError(
            "expected_tracking_run_key (at least 12 chars) is required for semantic_evolution_projection"
        )

    # Verify top-level or nested tracking_run_key if present on payload itself
    if "tracking_run_key" in payload and payload["tracking_run_key"] != run_key:
        raise LocatedError(
            f"Payload tracking_run_key {payload['tracking_run_key']!r} != expected {run_key!r}",
            location="tracking_run_key",
        )
    if isinstance(payload.get("run"), dict) and "tracking_run_key" in payload["run"]:
        if payload["run"]["tracking_run_key"] != run_key:
            raise LocatedError(
                f"Payload run.tracking_run_key {payload['run']['tracking_run_key']!r} != expected {run_key!r}",
                location="run.tracking_run_key",
            )

    cloned = copy.deepcopy(payload)
    projected = _project_structure(cloned, expected_tracking_run_key=run_key, path="")
    return normalize_floats(projected)
