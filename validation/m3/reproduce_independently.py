#!/usr/bin/env python3
"""Fresh-clone independent reproduction harness for M3 scenario evolution (T042, FR-018, SC-008).

Supports three execution modes:
1. Parent mode:
       validation/m2/.venv/bin/python validation/m3/reproduce_independently.py \
           --run-dir DIR [--sample-dir DIR] [--cache-dir DIR] [--work-dir DIR]
   Clones `REPO_ROOT` (`git clone --no-hardlinks`) into an isolated temporary directory,
   overlays all untracked/modified/deleted files from `collect_effective_source_manifest`
   (verifying identical effective source SHA-256), invokes child mode to rebuild Rust
   binaries from scratch (`ENSABC_M3_TARGET_DIR` isolated) and execute the deterministic
   M3 verification subset, verifies authentic `tracking_run_key[:12]:` prefixes via
   `semantic_evolution_projection`, asserts 100% semantic equality across nodes,
   primary+secondary edges, events, 3 metric families, branch/chain summaries, and HTML
   provenance, copies child summary JSONs into `<run_dir>/reproduction/child/`, and writes
   `<run_dir>/independent-reproduction.json`.

2. Child mode:
       validation/m2/.venv/bin/python validation/m3/reproduce_independently.py \
           --reproduction-child --output-dir DIR --source-manifest PATH \
           --sample-dir DIR --cache-dir DIR
   Never invokes `run_m3_gate.py`, `traceability.py`, `verify_report.py`, or recursive
   reproduction.

3. Self-check mode:
       validation/m2/.venv/bin/python validation/m3/reproduce_independently.py --self-check
   Verifies `semantic_evolution_projection` accepts legitimate build-hash prefix changes
   while rejecting forged prefixes, birth-node swaps, edge-role swaps, and metric
   mutations across all 3 metric families, plus effective-source overlay parity.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    LocatedError,
    M2_CACHE_DIR,
    NON_PHYSICAL_DISCLAIMER,
    VALIDATION_DIR,
    atomic_write_json,
    canonical_json_sha256,
    collect_effective_source_manifest,
    load_json,
    normalize_floats,
    semantic_evolution_projection,
    sha256_file,
)

REPRO_SCHEMA_VERSION = "m3.independent-reproduction.v1"
CHILD_SUMMARY_SCHEMA_VERSION = "m3.reproduction-child-summary.v1"
SELF_CHECK_SCHEMA_VERSION = "m3.reproduction-self-check.v1"

CHILD_SUMMARY_REL_FILES: tuple[str, ...] = (
    "reproduction-child-summary.json",
    "build/build_manifest.json",
    "core/synthetic-oracle.json",
    "core/orthogonality-separation.json",
    "core/permutation-invariance.json",
    "core/error-cases.json",
    "sql-history/sql-contract.json",
    "sql-history/real-evolution-replay.json",
    "sql-history/synthetic-evolution-replay.json",
    "sql-history/three-process-repro.json",
    "sql-history/transaction-atomicity.json",
    "om-verification/test-om-sample-report.json",
    "om-verification/cell-matrix-manifest.json",
    "om-verification/om-cells-report.json",
    "om-verification/duckomo-version-comparison.json",
    "visualization/real-traceability-audit.json",
    "visualization/synthetic-traceability-audit.json",
)

_PROVENANCE_SCRIPT_RE = re.compile(
    r'<script\s+id="ensabc-evolution-provenance"\s+type="application/json">\s*(.*?)\s*</script>',
    flags=re.DOTALL,
)


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_python_bin() -> Path:
    """Return absolute path to the locked venv Python binary WITHOUT dereferencing its symlink.

    Calling `Path.resolve()` on a `uv`/`venv` `bin/python` symlink strips the `.venv`
    parent directory and breaks `pyvenv.cfg` discovery (`omfiles`, `numpy`, `numcodecs`).
    `os.path.abspath` normalizes relative segments while preserving the venv symlink.
    """
    env_py = os.environ.get("ENSABC_M3_PYTHON")
    if env_py:
        p = Path(os.path.abspath(env_py))
        if p.exists() and os.access(p, os.X_OK):
            return p
    venv_py = Path(os.path.abspath(REPO_ROOT / "validation" / "m2" / ".venv" / "bin" / "python"))
    if venv_py.exists() and os.access(venv_py, os.X_OK):
        return venv_py
    return Path(os.path.abspath(sys.executable))


def extract_html_provenance_manifest(html_path: Path) -> dict[str, Any]:
    """Extract embedded canonical JSON provenance manifest from rendered evolution HTML."""
    text = html_path.read_text(encoding="utf-8")
    m = _PROVENANCE_SCRIPT_RE.search(text)
    if m is None:
        raise LocatedError(
            "Missing <script id='ensabc-evolution-provenance' type='application/json'>",
            file=html_path,
        )
    raw_json = m.group(1)
    if "<" in raw_json or ">" in raw_json:
        raise LocatedError(
            "Unescaped '<' or '>' inside <script id='ensabc-evolution-provenance'>",
            file=html_path,
        )
    return json.loads(
        raw_json,
        parse_constant=lambda c: (_ for _ in ()).throw(ValueError(f"Illegal constant {c}")),
    )


def project_replay_report(replay_doc: dict[str, Any]) -> dict[str, Any]:
    """Verify authentic `tracking_run_key[:12]:` prefixes and project a replay JSON bundle."""
    trk = replay_doc.get("tracking_run_key")
    if not isinstance(trk, str) or len(trk) < 12:
        raise LocatedError(f"Invalid tracking_run_key in replay bundle: {trk!r}")
    payload = {
        "tracking_run_key": trk,
        "evolution_run_id": replay_doc.get("evolution_run_id"),
        "content_sha256": replay_doc.get("content_sha256"),
        "non_physical_disclaimer": replay_doc.get("non_physical_disclaimer"),
        "invariants_e1_to_e8_passed": replay_doc.get("invariants_e1_to_e8_passed"),
        "preparation_negative_gate_rejected_with_zero_residual": replay_doc.get(
            "preparation_negative_gate_rejected_with_zero_residual"
        ),
        "high_uncertainty_transition_prev_ordinals": replay_doc.get(
            "high_uncertainty_transition_prev_ordinals"
        ),
        "runner_parity": replay_doc.get("runner_parity"),
        "chain_summary": replay_doc.get("chain_summary"),
        "batches": replay_doc.get("batches"),
        "nodes": replay_doc.get("nodes"),
        "transitions": replay_doc.get("transitions"),
        "edges": replay_doc.get("edges"),
        "events": replay_doc.get("events"),
        "branches": replay_doc.get("branches"),
    }
    return semantic_evolution_projection(payload, expected_tracking_run_key=trk)


def project_html_manifest(
    manifest: dict[str, Any],
    *,
    expected_evolution_run_id: str,
    expected_tracking_run_key: str,
) -> dict[str, Any]:
    """Verify run bindings & batch `run_key`s on an HTML provenance manifest and project semantics."""
    run_info = manifest.get("run")
    if not isinstance(run_info, dict):
        raise LocatedError("HTML provenance manifest missing 'run' object")
    if run_info.get("evolution_run_id") != expected_evolution_run_id:
        raise LocatedError(
            f"HTML run.evolution_run_id {run_info.get('evolution_run_id')!r} != {expected_evolution_run_id!r}",
            location="run.evolution_run_id",
        )
    if run_info.get("tracking_run_key") != expected_tracking_run_key:
        raise LocatedError(
            f"HTML run.tracking_run_key {run_info.get('tracking_run_key')!r} != {expected_tracking_run_key!r}",
            location="run.tracking_run_key",
        )

    cloned = copy.deepcopy(manifest)
    batches = cloned.get("batches")
    if not isinstance(batches, list):
        raise LocatedError("HTML provenance manifest 'batches' must be a list", location="batches")
    for idx, b in enumerate(batches):
        if not isinstance(b, dict):
            raise LocatedError(f"HTML batch[{idx}] is not a dict", location=f"batches[{idx}]")
        if b.get("run_key") != expected_tracking_run_key:
            raise LocatedError(
                f"HTML batch[{idx}].run_key {b.get('run_key')!r} != expected {expected_tracking_run_key!r}",
                location=f"batches[{idx}].run_key",
            )
        b.pop("run_key", None)

    return semantic_evolution_projection(cloned, expected_tracking_run_key=expected_tracking_run_key)


def _strip_core_volatiles(doc: dict[str, Any]) -> dict[str, Any]:
    """Strip host-path and wall-clock fields from core verification JSONs before exact comparison."""
    cleaned = copy.deepcopy(doc)
    for k in ("runner_binary", "runner_sha256", "elapsed_seconds", "checked_at_utc"):
        cleaned.pop(k, None)
    if isinstance(cleaned.get("core_error_byte_budget"), dict):
        cleaned["core_error_byte_budget"].pop("elapsed_seconds", None)
    return normalize_floats(cleaned)


def overlay_effective_sources(
    source_repo: Path,
    checkout_dir: Path,
    expected_manifest: dict[str, Any],
) -> dict[str, Any]:
    """Overlay modified/untracked files and remove deleted files so checkout matches source_repo 100%."""
    expected_sources: dict[str, str] = expected_manifest["sources"]
    initial_checkout_manifest = collect_effective_source_manifest(checkout_dir)

    # Remove any tracked file in checkout that was deleted in source_repo
    for rel_posix in initial_checkout_manifest["sources"]:
        if rel_posix not in expected_sources:
            stale_path = checkout_dir / rel_posix
            if stale_path.is_file() or stale_path.is_symlink():
                stale_path.unlink()

    # Copy every effective source file from source_repo into checkout_dir
    for rel_posix, exp_sha in expected_sources.items():
        src_file = source_repo / rel_posix
        dst_file = checkout_dir / rel_posix
        if not src_file.is_file():
            raise LocatedError(f"Source file vanished during overlay: {rel_posix}", file=src_file)
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_file, dst_file)
        copied_sha = sha256_file(dst_file)
        if copied_sha != exp_sha:
            raise LocatedError(
                f"Overlay SHA-256 mismatch on {rel_posix}: expected {exp_sha}, got {copied_sha}",
                file=dst_file,
            )

    verified_manifest = collect_effective_source_manifest(checkout_dir)
    if verified_manifest["sources"] != expected_sources:
        raise LocatedError("Effective source file map mismatch after overlay", file=checkout_dir)
    if verified_manifest["manifest_sha256"] != expected_manifest["manifest_sha256"]:
        raise LocatedError(
            f"Effective source manifest_sha256 mismatch: {verified_manifest['manifest_sha256']} != {expected_manifest['manifest_sha256']}",
            file=checkout_dir,
        )
    if (
        "code_manifest_sha256" in expected_manifest
        and verified_manifest["code_manifest_sha256"] != expected_manifest["code_manifest_sha256"]
    ):
        raise LocatedError(
            f"Effective source code_manifest_sha256 mismatch: {verified_manifest['code_manifest_sha256']} != {expected_manifest['code_manifest_sha256']}",
            file=checkout_dir,
        )
    return verified_manifest


def run_child(
    output_dir: Path,
    source_manifest_path: Path,
    sample_dir: Path,
    cache_dir: Path,
) -> int:
    """Execute fresh build + deterministic verification subset inside the cloned checkout."""
    output_dir = output_dir.resolve()
    source_manifest_path = source_manifest_path.resolve()
    sample_dir = sample_dir.resolve()
    cache_dir = cache_dir.resolve()

    if not source_manifest_path.is_file():
        print(f"Error: --source-manifest not found: {source_manifest_path}", file=sys.stderr)
        return 2
    if not (sample_dir / "om_sample_manifest.json").is_file():
        print(f"Error: OM sample manifest not found in {sample_dir}", file=sys.stderr)
        return 2
    for ver in ("v0.3.5-rc.1", "v0.3.4"):
        if not (cache_dir / ver / "duckomo.duckdb_extension").is_file():
            print(f"Error: DuckOMO {ver} missing in {cache_dir}", file=sys.stderr)
            return 2

    # 1. Verify cloned checkout effective source SHA-256 matches parent source-manifest
    expected_manifest = load_json(source_manifest_path)
    actual_manifest = collect_effective_source_manifest(REPO_ROOT)
    if actual_manifest["manifest_sha256"] != expected_manifest["manifest_sha256"]:
        print(
            f"FAIL: Child checkout manifest_sha256 {actual_manifest['manifest_sha256']} "
            f"!= parent {expected_manifest['manifest_sha256']}",
            file=sys.stderr,
        )
        return 1
    if (
        "code_manifest_sha256" in expected_manifest
        and actual_manifest["code_manifest_sha256"] != expected_manifest["code_manifest_sha256"]
    ):
        print(
            f"FAIL: Child checkout code_manifest_sha256 {actual_manifest['code_manifest_sha256']} "
            f"!= parent {expected_manifest['code_manifest_sha256']}",
            file=sys.stderr,
        )
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    build_dir = output_dir / "build"
    build_dir.mkdir(parents=True, exist_ok=True)
    viz_dir = output_dir / "visualization"
    viz_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    python_bin = _resolve_python_bin()
    step_records: list[dict[str, Any]] = []

    def _exec_child_step(
        step_name: str,
        cmd: list[str],
        *,
        extra_env: dict[str, str] | None = None,
        timeout: float = 1800.0,
    ) -> None:
        t0 = time.perf_counter()
        env = os.environ.copy()
        env["ENSABC_M3_PYTHON"] = str(python_bin)
        if extra_env:
            env.update(extra_env)
        log_file = logs_dir / f"{step_name}.log"
        with log_file.open("w", encoding="utf-8") as lf:
            proc = subprocess.run(
                cmd,
                cwd=REPO_ROOT,
                env=env,
                stdout=lf,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout,
                check=False,
            )
        elapsed = round(time.perf_counter() - t0, 4)
        step_records.append(
            {
                "step": step_name,
                "command": cmd,
                "exit_code": proc.returncode,
                "elapsed_seconds": elapsed,
                "status": "passed" if proc.returncode == 0 else "failed",
            }
        )
        if proc.returncode != 0:
            tail = log_file.read_text(encoding="utf-8")[-2000:]
            raise RuntimeError(f"Child step {step_name} failed (rc={proc.returncode}):\n{tail}")

    try:
        # 2. Rebuild Rust binaries from scratch with isolated ENSABC_M3_TARGET_DIR
        with tempfile.TemporaryDirectory(dir="/tmp", prefix="ensabc-m3-child-cargo-") as cargo_target:
            _exec_child_step(
                "build_extension_from_scratch",
                [
                    "bash",
                    str(VALIDATION_DIR / "build_extension.sh"),
                    "--output-dir",
                    str(build_dir),
                    "--target-dir",
                    cargo_target,
                ],
                extra_env={
                    "ENSABC_M3_TARGET_DIR": cargo_target,
                    "CARGO_TARGET_DIR": cargo_target,
                },
            )

        ext_path = build_dir / "ensabc_m0_extension.duckdb_extension"
        m1_runner = build_dir / "m1_runner"
        m2_runner = build_dir / "m2_runner"
        m3_runner = build_dir / "m3_runner"
        for req_bin in (ext_path, m1_runner, m2_runner, m3_runner, build_dir / "build_manifest.json"):
            if not req_bin.is_file():
                raise RuntimeError(f"Fresh child build did not produce {req_bin}")

        # 3. Deterministic subset: verify_evolution_core.py
        _exec_child_step(
            "verify_evolution_core",
            [
                str(python_bin),
                "-B",
                str(VALIDATION_DIR / "verify_evolution_core.py"),
                "--run-dir",
                str(output_dir),
                "--runner",
                str(m3_runner),
            ],
        )

        # 4. Deterministic subset: verify_sql_and_history.py --scope all
        _exec_child_step(
            "verify_sql_and_history_all",
            [
                str(python_bin),
                "-B",
                str(VALIDATION_DIR / "verify_sql_and_history.py"),
                "--scope",
                "all",
                "--run-dir",
                str(output_dir),
                "--sample-dir",
                str(sample_dir),
                "--cache-dir",
                str(cache_dir),
                "--extension",
                str(ext_path),
            ],
        )

        real_replay = load_json(output_dir / "sql-history" / "real-evolution-replay.json")
        synth_replay = load_json(output_dir / "sql-history" / "synthetic-evolution-replay.json")
        real_run_id = str(real_replay["evolution_run_id"])
        synth_run_id = str(
            synth_replay.get("showcase_evolution_run_id") or synth_replay["evolution_run_id"]
        )
        db_path = output_dir / "sql-history" / "evolution-runs.duckdb"
        real_html = viz_dir / "real-8run-main.html"
        synth_html = viz_dir / "synthetic-showcase.html"

        # 5. Deterministic subset: render_evolution_html.py (real + synthetic showcase)
        _exec_child_step(
            "render_real_8run_html",
            [
                str(python_bin),
                "-B",
                str(VALIDATION_DIR / "render_evolution_html.py"),
                "--db",
                str(db_path),
                "--evolution-run-id",
                real_run_id,
                "--output",
                str(real_html),
            ],
        )
        _exec_child_step(
            "render_synthetic_showcase_html",
            [
                str(python_bin),
                "-B",
                str(VALIDATION_DIR / "render_evolution_html.py"),
                "--db",
                str(db_path),
                "--evolution-run-id",
                synth_run_id,
                "--output",
                str(synth_html),
            ],
        )

        # 6. Deterministic subset: verify_visualization_traceability.py (real + synthetic showcase)
        _exec_child_step(
            "verify_real_8run_html_traceability",
            [
                str(python_bin),
                "-B",
                str(VALIDATION_DIR / "verify_visualization_traceability.py"),
                "--html",
                str(real_html),
                "--db",
                str(db_path),
                "--evolution-run-id",
                real_run_id,
                "--out-report",
                str(viz_dir / "real-traceability-audit.json"),
            ],
        )
        _exec_child_step(
            "verify_synthetic_showcase_html_traceability",
            [
                str(python_bin),
                "-B",
                str(VALIDATION_DIR / "verify_visualization_traceability.py"),
                "--html",
                str(synth_html),
                "--db",
                str(db_path),
                "--evolution-run-id",
                synth_run_id,
                "--out-report",
                str(viz_dir / "synthetic-traceability-audit.json"),
            ],
        )

        build_manifest = load_json(build_dir / "build_manifest.json")
        child_summary = {
            "schema_version": CHILD_SUMMARY_SCHEMA_VERSION,
            "status": "PASS",
            "completed_at_utc": _utc_now(),
            "checkout_root": str(REPO_ROOT),
            "source_manifest_sha256": actual_manifest["manifest_sha256"],
            "code_manifest_sha256": actual_manifest["code_manifest_sha256"],
            "code_sources_count": actual_manifest["code_sources_count"],
            "rust_source_sha256": build_manifest["rust_source_sha256"],
            "binaries": build_manifest["binaries"],
            "real_evolution_run_id": real_run_id,
            "real_tracking_run_key": real_replay["tracking_run_key"],
            "showcase_evolution_run_id": synth_run_id,
            "showcase_tracking_run_key": synth_replay["showcase_tracking_run_key"],
            "real_html_sha256": sha256_file(real_html),
            "synthetic_html_sha256": sha256_file(synth_html),
            "steps": step_records,
        }
        atomic_write_json(output_dir / "reproduction-child-summary.json", child_summary)
        return 0
    except Exception as exc:
        print(f"FAIL: Reproduction child failed: {exc}", file=sys.stderr)
        return 1


def run_parent(
    run_dir: Path,
    *,
    sample_dir: Path | None = None,
    cache_dir: Path | None = None,
    work_dir: Path | None = None,
) -> int:
    """Clone fresh checkout, invoke child mode, verify semantic projections, and write evidence."""
    run_dir = run_dir.resolve()
    if not run_dir.is_dir():
        print(f"Error: --run-dir does not exist: {run_dir}", file=sys.stderr)
        return 2

    resolved_sample = (
        sample_dir.resolve()
        if sample_dir is not None
        else (
            (run_dir / "sample").resolve()
            if (run_dir / "sample" / "om_sample_manifest.json").is_file()
            else (REPO_ROOT / "validation" / "m2" / "data" / "om_sample").resolve()
        )
    )
    resolved_cache = (cache_dir or M2_CACHE_DIR).resolve()

    if not (resolved_sample / "om_sample_manifest.json").is_file():
        print(f"Error: OM sample manifest missing at {resolved_sample}", file=sys.stderr)
        return 2
    for ver in ("v0.3.5-rc.1", "v0.3.4"):
        if not (resolved_cache / ver / "duckomo.duckdb_extension").is_file():
            print(f"Error: DuckOMO {ver} missing at {resolved_cache}", file=sys.stderr)
            return 2

    required_parent_files = (
        "core/synthetic-oracle.json",
        "core/orthogonality-separation.json",
        "core/permutation-invariance.json",
        "core/error-cases.json",
        "sql-history/real-evolution-replay.json",
        "sql-history/synthetic-evolution-replay.json",
        "om-verification/duckomo-version-comparison.json",
        "visualization/real-8run-main.html",
        "visualization/synthetic-showcase.html",
    )
    for rel in required_parent_files:
        if not (run_dir / rel).is_file():
            print(f"Error: Required parent artifact missing before reproduction: {run_dir / rel}", file=sys.stderr)
            return 2

    t0 = time.perf_counter()
    python_bin = _resolve_python_bin()
    parent_manifest = collect_effective_source_manifest(REPO_ROOT)

    repro_dir = run_dir / "reproduction"
    repro_child_archive = repro_dir / "child"
    repro_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(repro_dir / "effective-source-manifest.json", parent_manifest)

    def _run_in_scratch(scratch_root: Path) -> dict[str, Any]:
        checkout_dir = scratch_root / "checkout"
        child_out_dir = scratch_root / "child_output"
        manifest_file = scratch_root / "source-manifest.json"
        atomic_write_json(manifest_file, parent_manifest)

        # 1. Fresh clone without hardlinks
        clone_proc = subprocess.run(
            ["git", "clone", "--no-hardlinks", str(REPO_ROOT), str(checkout_dir)],
            capture_output=True,
            text=True,
            check=False,
        )
        if clone_proc.returncode != 0:
            raise RuntimeError(f"git clone --no-hardlinks failed: {clone_proc.stderr}")

        # 2. Overlay dirty/untracked/deleted effective sources and verify SHA-256 equality
        verified_checkout_manifest = overlay_effective_sources(
            REPO_ROOT, checkout_dir, parent_manifest
        )

        # 3. Invoke child mode inside the isolated checkout
        child_env = os.environ.copy()
        child_env["ENSABC_M3_PYTHON"] = str(python_bin)
        child_cmd = [
            str(python_bin),
            "-B",
            str(checkout_dir / "validation" / "m3" / "reproduce_independently.py"),
            "--reproduction-child",
            "--output-dir",
            str(child_out_dir),
            "--source-manifest",
            str(manifest_file),
            "--sample-dir",
            str(resolved_sample),
            "--cache-dir",
            str(resolved_cache),
        ]
        child_proc = subprocess.run(
            child_cmd,
            cwd=checkout_dir,
            env=child_env,
            capture_output=True,
            text=True,
            timeout=2400.0,
            check=False,
        )
        if child_proc.returncode != 0:
            raise RuntimeError(
                f"Reproduction child exited {child_proc.returncode}:\n"
                f"STDOUT:\n{child_proc.stdout}\nSTDERR:\n{child_proc.stderr}"
            )

        # 4. Copy child summary JSON artifacts into `<run_dir>/reproduction/child/`
        if repro_child_archive.exists():
            shutil.rmtree(repro_child_archive)
        repro_child_archive.mkdir(parents=True, exist_ok=True)
        copied_child_files: list[str] = []
        for rel_summary in CHILD_SUMMARY_REL_FILES:
            src_f = child_out_dir / rel_summary
            if not src_f.is_file():
                raise RuntimeError(f"Expected child artifact missing: {src_f}")
            dst_f = repro_child_archive / rel_summary
            dst_f.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_f, dst_f)
            copied_child_files.append(rel_summary)

        # 5. Compare core verification artifacts (100% equality on all deterministic fields)
        core_comparisons: dict[str, bool] = {}
        for core_rel in (
            "core/synthetic-oracle.json",
            "core/orthogonality-separation.json",
            "core/permutation-invariance.json",
            "core/error-cases.json",
        ):
            p_core = _strip_core_volatiles(load_json(run_dir / core_rel))
            c_core = _strip_core_volatiles(load_json(child_out_dir / core_rel))
            if p_core != c_core:
                raise AssertionError(f"Core artifact differed between parent and child: {core_rel}")
            core_comparisons[core_rel] = True

        # 6. Compare real 8-run SQL replay using semantic_evolution_projection
        parent_real = load_json(run_dir / "sql-history" / "real-evolution-replay.json")
        child_real = load_json(child_out_dir / "sql-history" / "real-evolution-replay.json")
        proj_parent_real = project_replay_report(parent_real)
        proj_child_real = project_replay_report(child_real)
        if proj_parent_real != proj_child_real:
            diff_keys = sorted(
                k
                for k in set(proj_parent_real) | set(proj_child_real)
                if proj_parent_real.get(k) != proj_child_real.get(k)
            )
            raise AssertionError(
                f"Real 8-run semantic projection differed between parent and child on keys: {diff_keys}"
            )

        # 7. Compare dual DuckOMO semantic hash
        p_duckomo = load_json(run_dir / "om-verification" / "duckomo-version-comparison.json")
        c_duckomo = load_json(child_out_dir / "om-verification" / "duckomo-version-comparison.json")
        if p_duckomo["semantic_projection_sha256"] != c_duckomo["semantic_projection_sha256"]:
            raise AssertionError("Dual DuckOMO semantic_projection_sha256 differed in child reproduction")

        # 8. Compare synthetic multi-batch replays
        parent_synth = load_json(run_dir / "sql-history" / "synthetic-evolution-replay.json")
        child_synth = load_json(child_out_dir / "sql-history" / "synthetic-evolution-replay.json")
        if len(parent_synth["cases"]) != len(child_synth["cases"]):
            raise AssertionError("Synthetic replay case count mismatch")
        for pc, cc in zip(parent_synth["cases"], child_synth["cases"]):
            for key in ("case_id", "planned_batch_count", "chain_summary", "runner_parity", "gap_events_strictly_disjoint"):
                if pc[key] != cc[key]:
                    raise AssertionError(f"Synthetic case {pc.get('case_id')} differed on {key}")

        # 9. Compare HTML provenance manifests (real-8run-main.html & synthetic-showcase.html)
        p_real_html_manifest = extract_html_provenance_manifest(
            run_dir / "visualization" / "real-8run-main.html"
        )
        c_real_html_manifest = extract_html_provenance_manifest(
            child_out_dir / "visualization" / "real-8run-main.html"
        )
        proj_p_real_html = project_html_manifest(
            p_real_html_manifest,
            expected_evolution_run_id=parent_real["evolution_run_id"],
            expected_tracking_run_key=parent_real["tracking_run_key"],
        )
        proj_c_real_html = project_html_manifest(
            c_real_html_manifest,
            expected_evolution_run_id=child_real["evolution_run_id"],
            expected_tracking_run_key=child_real["tracking_run_key"],
        )
        if proj_p_real_html != proj_c_real_html:
            raise AssertionError("real-8run-main.html semantic provenance differed between parent and child")

        p_synth_run_id = str(
            parent_synth.get("showcase_evolution_run_id") or parent_synth["evolution_run_id"]
        )
        c_synth_run_id = str(
            child_synth.get("showcase_evolution_run_id") or child_synth["evolution_run_id"]
        )
        p_synth_html_manifest = extract_html_provenance_manifest(
            run_dir / "visualization" / "synthetic-showcase.html"
        )
        c_synth_html_manifest = extract_html_provenance_manifest(
            child_out_dir / "visualization" / "synthetic-showcase.html"
        )
        proj_p_synth_html = project_html_manifest(
            p_synth_html_manifest,
            expected_evolution_run_id=p_synth_run_id,
            expected_tracking_run_key=parent_synth["showcase_tracking_run_key"],
        )
        proj_c_synth_html = project_html_manifest(
            c_synth_html_manifest,
            expected_evolution_run_id=c_synth_run_id,
            expected_tracking_run_key=child_synth["showcase_tracking_run_key"],
        )
        if proj_p_synth_html != proj_c_synth_html:
            raise AssertionError("synthetic-showcase.html semantic provenance differed between parent and child")

        c_real_audit = load_json(child_out_dir / "visualization" / "real-traceability-audit.json")
        c_synth_audit = load_json(child_out_dir / "visualization" / "synthetic-traceability-audit.json")
        if c_real_audit.get("status") != "pass" or c_synth_audit.get("status") != "pass":
            raise AssertionError("Child visualization traceability audit did not pass")

        child_build = load_json(child_out_dir / "build" / "build_manifest.json")
        parent_build_path = run_dir / "build" / "build_manifest.json"
        parent_build = load_json(parent_build_path) if parent_build_path.is_file() else None
        if parent_build is not None:
            if parent_build["rust_source_sha256"] != child_build["rust_source_sha256"]:
                raise AssertionError("Rust source SHA-256 differed between parent and child build")

        elapsed = round(time.perf_counter() - t0, 4)
        return {
            "schema_version": REPRO_SCHEMA_VERSION,
            "status": "PASS",
            "equivalence": True,
            "completed_at_utc": _utc_now(),
            "elapsed_seconds": elapsed,
            "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
            "effective_source_manifest_sha256": verified_checkout_manifest["manifest_sha256"],
            "code_manifest_sha256": verified_checkout_manifest["code_manifest_sha256"],
            "tracked_and_effective_files_count": verified_checkout_manifest["tracked_and_effective_count"],
            "code_sources_count": verified_checkout_manifest["code_sources_count"],
            "rust_source_sha256": child_build["rust_source_sha256"],
            "parent_extension_sha256": parent_real["evolution_binding"]["software"]["ensabc_extension_sha256"],
            "child_extension_sha256": child_real["evolution_binding"]["software"]["ensabc_extension_sha256"],
            "parent_evolution_run_id": parent_real["evolution_run_id"],
            "child_evolution_run_id": child_real["evolution_run_id"],
            "parent_tracking_run_key": parent_real["tracking_run_key"],
            "child_tracking_run_key": child_real["tracking_run_key"],
            "authentic_branch_id_prefixes_verified": True,
            "core_artifacts_verified": core_comparisons,
            "real_semantic_projection_sha256": canonical_json_sha256(proj_parent_real),
            "real_html_semantic_projection_sha256": canonical_json_sha256(proj_p_real_html),
            "synthetic_html_semantic_projection_sha256": canonical_json_sha256(proj_p_synth_html),
            "counts_verified": {
                "batches": len(parent_real["batches"]),
                "nodes": len(parent_real["nodes"]),
                "transitions": len(parent_real["transitions"]),
                "edges": len(parent_real["edges"]),
                "primary_edges": parent_real["chain_summary"]["primary_edge_count"],
                "secondary_split_edges": parent_real["chain_summary"]["secondary_split_edge_count"],
                "secondary_merge_edges": parent_real["chain_summary"]["secondary_merge_edge_count"],
                "events": len(parent_real["events"]),
                "branches": len(parent_real["branches"]),
            },
            "child_archive_dir": "reproduction/child",
            "child_archived_files": copied_child_files,
        }

    try:
        if work_dir is not None:
            scratch = work_dir.resolve()
            if scratch.exists():
                raise LocatedError(f"--work-dir already exists: {scratch}", file=scratch)
            scratch.mkdir(parents=True, exist_ok=False)
            evidence = _run_in_scratch(scratch)
        else:
            with tempfile.TemporaryDirectory(dir="/tmp", prefix="ensabc-m3-repro-") as tmp_str:
                evidence = _run_in_scratch(Path(tmp_str))

        out_path = run_dir / "independent-reproduction.json"
        atomic_write_json(out_path, evidence)
        print(
            f"Independent reproduction: PASS in {evidence['elapsed_seconds']:.2f}s "
            f"(source_sha256={evidence['effective_source_manifest_sha256'][:12]}..., "
            f"real_sem_sha256={evidence['real_semantic_projection_sha256'][:12]}...)"
        )
        return 0
    except Exception as exc:
        print(f"FAIL: Independent reproduction failed: {exc}", file=sys.stderr)
        return 1


def _sample_bundle_for_prefix(tracking_run_key: str, evolution_run_id: str) -> dict[str, Any]:
    pfx = tracking_run_key[:12]
    return {
        "evolution_run_id": evolution_run_id,
        "tracking_run_key": tracking_run_key,
        "content_sha256": "c" * 64,
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "chain_summary": {
            "ok_batch_count": 4,
            "branch_count": 3,
            "primary_edge_count": 2,
            "secondary_split_edge_count": 1,
            "secondary_merge_edge_count": 0,
            "continuation_count": 1,
            "split_count": 1,
            "merge_count": 0,
            "emergence_count": 0,
            "disappearance_count": 0,
            "cumulative_primary_distance": 0.45,
            "mean_transition_tv_redistribution": 0.10,
            "high_uncertainty_edge_count": 0,
        },
        "nodes": [
            {
                "ordinal": 0,
                "cluster_id": 0,
                "canonical_rank": 0,
                "branch_id": f"{pfx}:b00:k00",
                "member_count": 30,
                "member_fraction": 0.60,
                "incoming_role": "first_batch",
                "outgoing_role": "split_parent",
                "high_uncertainty": False,
            },
            {
                "ordinal": 0,
                "cluster_id": 1,
                "canonical_rank": 1,
                "branch_id": f"{pfx}:b00:k01",
                "member_count": 20,
                "member_fraction": 0.40,
                "incoming_role": "first_batch",
                "outgoing_role": "continuation_source",
                "high_uncertainty": False,
            },
            {
                "ordinal": 1,
                "cluster_id": 0,
                "canonical_rank": 0,
                "branch_id": f"{pfx}:b00:k00",
                "member_count": 20,
                "member_fraction": 0.40,
                "incoming_role": "split_primary_child",
                "outgoing_role": "final_batch",
                "high_uncertainty": False,
            },
            {
                "ordinal": 1,
                "cluster_id": 1,
                "canonical_rank": 1,
                "branch_id": f"{pfx}:b01:k01",
                "member_count": 10,
                "member_fraction": 0.20,
                "incoming_role": "split_secondary_child",
                "outgoing_role": "final_batch",
                "high_uncertainty": False,
            },
            {
                "ordinal": 1,
                "cluster_id": 2,
                "canonical_rank": 2,
                "branch_id": f"{pfx}:b00:k01",
                "member_count": 20,
                "member_fraction": 0.40,
                "incoming_role": "continuation_target",
                "outgoing_role": "final_batch",
                "high_uncertainty": False,
            },
        ],
        "edges": [
            {
                "prev_ordinal": 0,
                "prev_cluster_id": 0,
                "curr_ordinal": 1,
                "curr_cluster_id": 0,
                "prev_branch_id": f"{pfx}:b00:k00",
                "curr_branch_id": f"{pfx}:b00:k00",
                "edge_role": "primary",
                "event_type": "split",
                "distance": 0.20,
                "distance_over_scale": 0.20,
                "attributed_flow": 0.40,
                "threshold_margin": 0.80,
                "optimality_margin": 0.30,
                "secondary_margin": 0.0,
                "ambiguous_tie": False,
                "high_uncertainty": False,
            },
            {
                "prev_ordinal": 0,
                "prev_cluster_id": 0,
                "curr_ordinal": 1,
                "curr_cluster_id": 1,
                "prev_branch_id": f"{pfx}:b00:k00",
                "curr_branch_id": f"{pfx}:b01:k01",
                "edge_role": "secondary_split",
                "event_type": "split",
                "distance": 0.50,
                "distance_over_scale": 0.50,
                "attributed_flow": 0.20,
                "threshold_margin": 0.50,
                "optimality_margin": 0.30,
                "secondary_margin": 0.0,
                "ambiguous_tie": False,
                "high_uncertainty": False,
            },
            {
                "prev_ordinal": 0,
                "prev_cluster_id": 1,
                "curr_ordinal": 1,
                "curr_cluster_id": 2,
                "prev_branch_id": f"{pfx}:b00:k01",
                "curr_branch_id": f"{pfx}:b00:k01",
                "edge_role": "primary",
                "event_type": "continuation",
                "distance": 0.25,
                "distance_over_scale": 0.25,
                "attributed_flow": 0.40,
                "threshold_margin": 0.75,
                "optimality_margin": 0.0,
                "secondary_margin": 0.0,
                "ambiguous_tie": False,
                "high_uncertainty": False,
            },
        ],
        "events": [
            {
                "prev_ordinal": 0,
                "event_index": 0,
                "event_type": "split",
                "branch_ids": [f"{pfx}:b00:k00", f"{pfx}:b01:k01"],
                "primary_branch_id": f"{pfx}:b00:k00",
                "max_morphology_distance": 0.50,
                "attributed_flow_sum": 0.60,
                "min_margin_ratio": 0.30,
                "high_uncertainty": False,
            },
            {
                "prev_ordinal": 0,
                "event_index": 1,
                "event_type": "continuation",
                "branch_ids": [f"{pfx}:b00:k01"],
                "primary_branch_id": f"{pfx}:b00:k01",
                "max_morphology_distance": 0.25,
                "attributed_flow_sum": 0.40,
                "min_margin_ratio": 0.75,
                "high_uncertainty": False,
            },
        ],
        "branches": [
            {
                "branch_id": f"{pfx}:b00:k00",
                "birth_ordinal": 0,
                "end_ordinal": 1,
                "node_count": 2,
                "step_count": 1,
                "cumulative_morphology_distance": 0.20,
                "net_delta_fraction": -0.20,
                "spawned_secondary_branch_ids": [f"{pfx}:b01:k01"],
                "split_from_parent_branch_id": None,
                "absorbed_secondary_branch_ids": [],
                "merged_into_branch_id": None,
                "high_uncertainty_step_ratio": 0.0,
            },
            {
                "branch_id": f"{pfx}:b00:k01",
                "birth_ordinal": 0,
                "end_ordinal": 1,
                "node_count": 2,
                "step_count": 1,
                "cumulative_morphology_distance": 0.25,
                "net_delta_fraction": 0.0,
                "spawned_secondary_branch_ids": [],
                "split_from_parent_branch_id": None,
                "absorbed_secondary_branch_ids": [],
                "merged_into_branch_id": None,
                "high_uncertainty_step_ratio": 0.0,
            },
            {
                "branch_id": f"{pfx}:b01:k01",
                "birth_ordinal": 1,
                "end_ordinal": 1,
                "node_count": 1,
                "step_count": 0,
                "cumulative_morphology_distance": 0.0,
                "net_delta_fraction": 0.0,
                "spawned_secondary_branch_ids": [],
                "split_from_parent_branch_id": f"{pfx}:b00:k00",
                "absorbed_secondary_branch_ids": [],
                "merged_into_branch_id": None,
                "high_uncertainty_step_ratio": 0.0,
            },
        ],
    }


def run_self_check(*, out_report: Path | None = None) -> dict[str, Any]:
    """Verify semantic projection & source overlay contracts across positive and negative mutations."""
    checks: list[dict[str, Any]] = []

    parent_trk = "a1b2c3d4e5f6_parent_tracking_run_key_0001"
    child_trk = "998877665544_child_rebuilt_tracking_key_0002"
    parent_bundle = _sample_bundle_for_prefix(parent_trk, "parent_evol_run_001")
    child_bundle = _sample_bundle_for_prefix(child_trk, "child_evol_run_002")

    # 1. Positive check: legitimate build-hash prefix change produces 100% identical projection
    proj_parent = semantic_evolution_projection(parent_bundle, expected_tracking_run_key=parent_trk)
    proj_child = semantic_evolution_projection(child_bundle, expected_tracking_run_key=child_trk)
    if proj_parent != proj_child:
        raise AssertionError("Legitimate build-hash prefix change failed semantic_evolution_projection equality")
    checks.append({"case": "legitimate_build_hash_prefix_change_accepted", "passed": True})

    # 2. Negative check: forged prefix on node/edge/branch_summary rejected
    for sub_name, mut_fn in (
        ("forged_node_branch_prefix", lambda b: b["nodes"][0].__setitem__("branch_id", "forged000000:b00:k00")),
        ("forged_edge_curr_branch_prefix", lambda b: b["edges"][1].__setitem__("curr_branch_id", "forged000000:b01:k01")),
        (
            "forged_spawned_list_branch_prefix",
            lambda b: b["branches"][0].__setitem__("spawned_secondary_branch_ids", ["forged000000:b01:k01"]),
        ),
        ("mismatched_top_tracking_run_key", lambda b: b.__setitem__("tracking_run_key", "wrongkey000000000")),
    ):
        forged = copy.deepcopy(child_bundle)
        mut_fn(forged)
        rejected = False
        try:
            semantic_evolution_projection(forged, expected_tracking_run_key=child_trk)
        except LocatedError:
            rejected = True
        if not rejected:
            raise AssertionError(f"Forged prefix mutation {sub_name} was NOT rejected!")
        checks.append({"case": sub_name, "passed": True})

    # 3. Negative check: birth-node swap (valid prefix, swapped birth canonical_rank k00 <-> k01)
    swapped_birth = copy.deepcopy(child_bundle)
    pfx_c = child_trk[:12]
    swapped_birth["nodes"][2]["branch_id"] = f"{pfx_c}:b00:k01"
    swapped_birth["nodes"][4]["branch_id"] = f"{pfx_c}:b00:k00"
    proj_swapped_birth = semantic_evolution_projection(swapped_birth, expected_tracking_run_key=child_trk)
    if proj_swapped_birth == proj_parent:
        raise AssertionError("Birth-node swap was NOT detected by semantic_evolution_projection!")
    checks.append({"case": "birth_node_swap_rejected", "passed": True})

    # 4. Negative check: edge-role swap (`secondary_split` -> `primary` / `secondary_merge`)
    swapped_role = copy.deepcopy(child_bundle)
    swapped_role["edges"][1]["edge_role"] = "primary"
    proj_swapped_role = semantic_evolution_projection(swapped_role, expected_tracking_run_key=child_trk)
    if proj_swapped_role == proj_parent:
        raise AssertionError("Edge-role swap was NOT detected by semantic_evolution_projection!")
    checks.append({"case": "edge_role_swap_rejected", "passed": True})

    # 5. Negative check: metric mutations across all 3 metric families
    for metric_case, m_fn in (
        ("morphology_distance_mutation", lambda b: b["edges"][0].__setitem__("distance", 0.200001)),
        ("fraction_attributed_flow_mutation", lambda b: b["edges"][1].__setitem__("attributed_flow", 0.199999)),
        ("uncertainty_optimality_margin_mutation", lambda b: b["edges"][1].__setitem__("optimality_margin", 0.299999)),
    ):
        mut_b = copy.deepcopy(child_bundle)
        m_fn(mut_b)
        proj_mut = semantic_evolution_projection(mut_b, expected_tracking_run_key=child_trk)
        if proj_mut == proj_parent:
            raise AssertionError(f"Metric mutation {metric_case} was NOT detected!")
        checks.append({"case": metric_case, "passed": True})

    # 6. HTML provenance manifest projection (accepts prefix change, rejects batch run_key forgery)
    html_p = {
        "schema_version": "m3.visualization.v1",
        "run": {
            "evolution_run_id": "erid_p",
            "tracking_run_key": parent_trk,
            "content_sha256": "1" * 64,
            "chain_summary": parent_bundle["chain_summary"],
        },
        "batches": [{"ordinal": 0, "run_key": parent_trk, "status": "ok", "dispersion": 1.0}],
        "nodes": {"b0:c0": dict(parent_bundle["nodes"][0])},
        "edges": {"b0:c0->b1:c0": dict(parent_bundle["edges"][0])},
        "transitions": [],
        "branches": {parent_bundle["branches"][0]["branch_id"]: dict(parent_bundle["branches"][0])},
    }
    html_c = {
        "schema_version": "m3.visualization.v1",
        "run": {
            "evolution_run_id": "erid_c",
            "tracking_run_key": child_trk,
            "content_sha256": "2" * 64,
            "chain_summary": child_bundle["chain_summary"],
        },
        "batches": [{"ordinal": 0, "run_key": child_trk, "status": "ok", "dispersion": 1.0}],
        "nodes": {"b0:c0": dict(child_bundle["nodes"][0])},
        "edges": {"b0:c0->b1:c0": dict(child_bundle["edges"][0])},
        "transitions": [],
        "branches": {child_bundle["branches"][0]["branch_id"]: dict(child_bundle["branches"][0])},
    }
    if project_html_manifest(
        html_p, expected_evolution_run_id="erid_p", expected_tracking_run_key=parent_trk
    ) != project_html_manifest(
        html_c, expected_evolution_run_id="erid_c", expected_tracking_run_key=child_trk
    ):
        raise AssertionError("HTML provenance projection rejected legitimate prefix change")

    bad_html_c = copy.deepcopy(html_c)
    bad_html_c["batches"][0]["run_key"] = "forged_batch_run_key"
    batch_rk_rejected = False
    try:
        project_html_manifest(
            bad_html_c, expected_evolution_run_id="erid_c", expected_tracking_run_key=child_trk
        )
    except LocatedError:
        batch_rk_rejected = True
    if not batch_rk_rejected:
        raise AssertionError("Forged HTML batch run_key was NOT rejected")
    checks.append({"case": "html_manifest_projection_and_batch_run_key_guard", "passed": True})

    # 7. Effective source overlay check in a clean temporary git repository
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="ensabc-m3-repro-selfcheck-") as tmp_str:
        tmp = Path(tmp_str)
        src_repo = tmp / "src_repo"
        dst_clone = tmp / "dst_clone"
        (src_repo / "crates").mkdir(parents=True)
        (src_repo / "validation" / "m3").mkdir(parents=True)
        (src_repo / "Cargo.toml").write_text("[workspace]\n", encoding="utf-8")
        (src_repo / "crates" / "keep.rs").write_text("pub fn a() -> i32 { 1 }\n", encoding="utf-8")
        (src_repo / "crates" / "to_delete.rs").write_text("pub fn old() {}\n", encoding="utf-8")
        subprocess.run(["git", "init", str(src_repo)], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(src_repo), "config", "user.email", "test@example.invalid"], check=True)
        subprocess.run(["git", "-C", str(src_repo), "config", "user.name", "Test"], check=True)
        subprocess.run(["git", "-C", str(src_repo), "add", "."], check=True)
        subprocess.run(["git", "-C", str(src_repo), "commit", "-m", "init"], check=True, capture_output=True)

        subprocess.run(["git", "clone", "--no-hardlinks", str(src_repo), str(dst_clone)], check=True, capture_output=True)
        # Mutate tracked file, add untracked file, delete tracked file
        (src_repo / "crates" / "keep.rs").write_text("pub fn a() -> i32 { 42 }\n", encoding="utf-8")
        (src_repo / "validation" / "m3" / "untracked_new.py").write_text("X = 1\n", encoding="utf-8")
        (src_repo / "crates" / "to_delete.rs").unlink()

        src_manifest = collect_effective_source_manifest(src_repo)
        dst_manifest = overlay_effective_sources(src_repo, dst_clone, src_manifest)
        assert dst_manifest["manifest_sha256"] == src_manifest["manifest_sha256"]
        assert dst_manifest["code_manifest_sha256"] == src_manifest["code_manifest_sha256"]
        assert not (dst_clone / "crates" / "to_delete.rs").exists()
        assert (dst_clone / "validation" / "m3" / "untracked_new.py").is_file()

        # Verify code_manifest_sha256 is invariant to .md edits while sensitive to .py/.rs edits
        (src_repo / "specs" / "004-m3-scenario-evolution").mkdir(parents=True, exist_ok=True)
        (src_repo / "specs" / "004-m3-scenario-evolution" / "notes.md").write_text("# Doc\n", encoding="utf-8")
        (src_repo / "validation" / "m3" / "README.md").write_text("# Guide\n", encoding="utf-8")
        md_manifest = collect_effective_source_manifest(src_repo)
        assert md_manifest["manifest_sha256"] != src_manifest["manifest_sha256"]
        assert md_manifest["code_manifest_sha256"] == src_manifest["code_manifest_sha256"]

        (src_repo / "validation" / "m3" / "untracked_new.py").write_text("X = 2\n", encoding="utf-8")
        code_edit_manifest = collect_effective_source_manifest(src_repo)
        assert code_edit_manifest["code_manifest_sha256"] != md_manifest["code_manifest_sha256"]
    checks.append({"case": "fresh_clone_effective_source_overlay_sha256_parity", "passed": True})

    # 8. Verify _resolve_python_bin preserves virtualenv bin/python symlink (never strips pyvenv.cfg)
    venv_candidate = REPO_ROOT / "validation" / "m2" / ".venv" / "bin" / "python"
    if venv_candidate.exists():
        old_env_py = os.environ.get("ENSABC_M3_PYTHON")
        try:
            os.environ["ENSABC_M3_PYTHON"] = str(venv_candidate)
            resolved_py = _resolve_python_bin()
            if str(resolved_py) != os.path.abspath(venv_candidate):
                raise AssertionError(
                    f"_resolve_python_bin dereferenced venv symlink: {resolved_py} != {os.path.abspath(venv_candidate)}"
                )
        finally:
            if old_env_py is None:
                os.environ.pop("ENSABC_M3_PYTHON", None)
            else:
                os.environ["ENSABC_M3_PYTHON"] = old_env_py
    checks.append({"case": "venv_python_symlink_preserved_for_child_checkout", "passed": True})

    summary = {
        "schema_version": SELF_CHECK_SCHEMA_VERSION,
        "status": "PASS",
        "checked_at_utc": _utc_now(),
        "checks_tested": len(checks),
        "checks_passed": sum(1 for c in checks if c["passed"]),
        "checks": checks,
    }
    if out_report is not None:
        atomic_write_json(out_report, summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="M3 fresh-clone independent reproduction harness and semantic projection verifier."
    )
    parser.add_argument("--run-dir", type=Path, default=None, help="Parent M3 run directory")
    parser.add_argument("--sample-dir", type=Path, default=None, help="Frozen 8-batch OM sample directory")
    parser.add_argument("--cache-dir", type=Path, default=None, help="Cached DuckOMO extensions directory")
    parser.add_argument("--work-dir", type=Path, default=None, help="Optional explicit scratch directory")
    parser.add_argument("--reproduction-child", action="store_true", help="Internal child execution mode")
    parser.add_argument("--output-dir", type=Path, default=None, help="Child output directory")
    parser.add_argument("--source-manifest", type=Path, default=None, help="Parent effective source manifest")
    parser.add_argument("--self-check", action="store_true", help="Run semantic projection & overlay self-checks")
    parser.add_argument("--out-report", type=Path, default=None, help="Optional JSON output path for --self-check")
    args = parser.parse_args(argv)

    if args.self_check:
        try:
            summary = run_self_check(out_report=args.out_report)
            print(
                f"Reproduction self-check: {summary['status']} "
                f"({summary['checks_passed']}/{summary['checks_tested']} checks passed)"
            )
            return 0
        except Exception as exc:
            print(f"FAIL: Reproduction self-check failed: {exc}", file=sys.stderr)
            return 1

    if args.reproduction_child:
        if not args.output_dir or not args.source_manifest or not args.sample_dir or not args.cache_dir:
            print(
                "Error: --reproduction-child requires --output-dir, --source-manifest, --sample-dir, and --cache-dir",
                file=sys.stderr,
            )
            return 2
        return run_child(
            args.output_dir,
            args.source_manifest,
            args.sample_dir,
            args.cache_dir,
        )

    if not args.run_dir:
        print("Error: --run-dir is required for parent mode (or pass --self-check)", file=sys.stderr)
        return 2

    return run_parent(
        args.run_dir,
        sample_dir=args.sample_dir,
        cache_dir=args.cache_dir,
        work_dir=args.work_dir,
    )


if __name__ == "__main__":
    sys.exit(main())
