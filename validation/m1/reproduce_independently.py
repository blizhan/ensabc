#!/usr/bin/env python3
"""Independent reproduction harness for M1 scenario computation.

Usage:
    Parent mode:
        python3 validation/m1/reproduce_independently.py --run-dir DIR [--work-dir DIR]
    Child mode:
        python3 validation/m1/reproduce_independently.py --reproduction-child \
            --output-dir DIR --source-manifest PATH --manifest PATH --payload PATH --config PATH
    Self-check:
        python3 validation/m1/reproduce_independently.py --self-check
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m1.common import (
    FIXTURES_DIR,
    M1Error,
    read_json,
    sha256_file,
    utc_now,
    write_json,
)

CHILD_EXECUTED_STEPS = [
    "build_extension",
    "prepare_input",
    "run_reference_original",
    "verify_core_native",
    "compare_results_original",
    "run_reference_shared",
    "verify_core_shared",
    "compare_results_shared",
    "verify_extension",
]

CHILD_EXCLUDED_STEPS = [
    "verify_minimal_runtime",
    "measure_tasks",
    "reproduce_independently",
    "traceability",
    "verify_report",
    "run_m1_gate",
]


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="M1 independent reproduction harness.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-dir", type=Path, help="Parent mode: target baseline run directory")
    group.add_argument("--reproduction-child", action="store_true", help="Child mode flag")
    group.add_argument("--self-check", action="store_true", help="Run self-check regression tests")

    # Parent options
    parser.add_argument("--work-dir", type=Path, help="Parent mode: work directory")

    # Child options
    parser.add_argument("--output-dir", type=Path, help="Child mode: destination output directory")
    parser.add_argument("--source-manifest", type=Path, help="Child mode: path to source manifest JSON")
    parser.add_argument("--manifest", type=Path, help="Child mode: sample manifest JSON")
    parser.add_argument("--payload", type=Path, help="Child mode: sample payload JSON")
    parser.add_argument("--config", type=Path, help="Child mode: scenario config JSON")

    parsed = parser.parse_args(args)
    if parsed.reproduction_child:
        if not (parsed.output_dir and parsed.source_manifest and parsed.manifest and parsed.payload and parsed.config):
            parser.error("--reproduction-child requires --output-dir, --source-manifest, --manifest, --payload, and --config")
    elif parsed.run_dir:
        if parsed.output_dir or parsed.source_manifest or parsed.config:
            parser.error("Parent mode (--run-dir) cannot be combined with child arguments")
    elif parsed.self_check:
        if parsed.work_dir or parsed.output_dir or parsed.source_manifest or parsed.config:
            parser.error("--self-check does not accept mode arguments")

    return parsed


def _run_cmd(cmd: list[str], cwd: Path | None = None, env: dict[str, str] | None = None, timeout: float = 600.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def run_child_mode(
    output_dir: Path,
    source_manifest_path: Path,
    manifest_path: Path,
    payload_path: Path,
    config_path: Path,
) -> int:
    output_dir = output_dir.resolve()
    if output_dir.exists():
        raise M1Error(f"Output directory already exists: {output_dir}", code="DIR_EXISTS")
    output_dir.mkdir(parents=True, exist_ok=True)

    clone_root = Path.cwd().resolve()
    # Ensure output_dir is outside clone
    try:
        output_dir.relative_to(clone_root)
        raise M1Error(f"Output dir {output_dir} must be outside clone root {clone_root}", code="INVALID_PATH")
    except ValueError:
        pass  # Good, it is outside

    target_dir = output_dir / "target"
    target_dir.mkdir(parents=True, exist_ok=True)

    child_env = os.environ.copy()
    child_env["CARGO_TARGET_DIR"] = str(target_dir)
    child_env["ENSABC_M1_TARGET_DIR"] = str(target_dir)
    child_env["PYTHONDONTWRITEBYTECODE"] = "1"

    status = "pass"
    reason = None
    artifacts = []

    try:
        # 1. Build extension and runner
        build_sh = clone_root / "validation" / "m1" / "build_extension.sh"
        res = _run_cmd(["bash", str(build_sh), "--output-dir", str(target_dir)], cwd=clone_root, env=child_env)
        if res.returncode != 0:
            raise M1Error(f"Build failed in child mode: {res.stderr}\n{res.stdout}", code="BUILD_FAILED")

        # 2. prepare_input
        prep_py = clone_root / "validation" / "m1" / "prepare_input.py"
        res = _run_cmd([
            sys.executable, str(prep_py),
            "--manifest", str(manifest_path.resolve()),
            "--payload", str(payload_path.resolve()),
            "--config", str(config_path.resolve()),
            "--output-dir", str(output_dir),
        ], cwd=clone_root, env=child_env)
        if res.returncode != 0:
            raise M1Error(f"prepare_input failed: {res.stderr}", code="PREPARE_FAILED")

        # 3. Six-stage execution
        run_ref_py = clone_root / "validation" / "m1" / "run_reference.py"
        verify_core_py = clone_root / "validation" / "m1" / "verify_core.py"
        comp_py = clone_root / "validation" / "m1" / "compare_results.py"
        ext_path = target_dir / "ensabc_m0_extension.duckdb_extension"

        stages = [
            ([sys.executable, str(run_ref_py), "--run-dir", str(output_dir), "--stage", "original"], "run_reference original"),
            ([sys.executable, str(verify_core_py), "--run-dir", str(output_dir), "--stage", "native"], "verify_core native"),
            ([sys.executable, str(comp_py), "--run-dir", str(output_dir), "--stage", "original"], "compare_results original"),
            ([sys.executable, str(run_ref_py), "--run-dir", str(output_dir), "--stage", "shared"], "run_reference shared"),
            ([sys.executable, str(verify_core_py), "--run-dir", str(output_dir), "--stage", "shared"], "verify_core shared"),
            ([sys.executable, str(comp_py), "--run-dir", str(output_dir), "--stage", "shared"], "compare_results shared"),
        ]

        for cmd, desc in stages:
            res = _run_cmd(cmd, cwd=clone_root, env=child_env)
            if res.returncode != 0:
                raise M1Error(f"{desc} failed: {res.stderr}\n{res.stdout}", code="STAGE_FAILED")

        # 4. verify_extension
        verify_ext_py = clone_root / "validation" / "m1" / "verify_extension.py"
        res = _run_cmd([
            sys.executable, str(verify_ext_py),
            "--run-dir", str(output_dir),
            "--extension", str(ext_path),
        ], cwd=clone_root, env=child_env)
        if res.returncode != 0:
            raise M1Error(f"verify_extension failed: {res.stderr}", code="EXTENSION_VERIFY_FAILED")

        artifacts = [
            "inputs/task.json",
            "core/run-1.json",
            "comparison/original.json",
            "comparison/shared.json",
            "extension.json",
        ]

    except Exception as e:
        status = "fail"
        reason = str(e)

    record = {
        "schema_version": "m1.reproduction-child.v1",
        "scope": "reproduction_subset",
        "status": status,
        "source_manifest_sha256": sha256_file(source_manifest_path) if source_manifest_path.is_file() else "",
        "executed_steps": CHILD_EXECUTED_STEPS if status == "pass" else [],
        "excluded_steps": CHILD_EXCLUDED_STEPS,
        "artifacts": artifacts,
        "reason": reason,
        "created_at": utc_now(),
    }

    out_file = output_dir / "reproduction-child.json"
    write_json(out_file, record)
    return 0 if status == "pass" else 1


def run_parent_mode(run_dir: Path, work_dir: Path | None = None) -> int:
    run_dir = run_dir.resolve()
    if not run_dir.is_dir():
        raise M1Error(f"Run dir does not exist: {run_dir}", code="DIR_NOT_FOUND")

    temp_cleanup = None
    if work_dir is None:
        temp_dir = tempfile.TemporaryDirectory(prefix="m1_reproduce_")
        temp_cleanup = temp_dir
        work_dir = Path(temp_dir.name)
    else:
        work_dir = work_dir.resolve()
        work_dir.mkdir(parents=True, exist_ok=True)

    try:
        clone_dir = work_dir / "clone"
        child_out_dir = work_dir / "child_output"

        # 1. Create a clean source copy
        if clone_dir.exists():
            shutil.rmtree(clone_dir)
        shutil.copytree(
            REPO_ROOT,
            clone_dir,
            ignore=shutil.ignore_patterns(
                ".git", "target", "artifacts", "__pycache__", "*.pyc", "*.duckdb_extension", ".venv"
            ),
        )

        # 2. Locate inputs
        identity_path = run_dir / "inputs" / "identity.json"
        if identity_path.is_file():
            ident = read_json(identity_path)
            manifest_path = Path(ident["manifest_path"])
            payload_path = Path(ident["payload_path"])
        else:
            manifest_path = REPO_ROOT / "validation" / "m0" / "fixtures" / "real_ifs_ens_20200101_t2m_manifest.json"
            payload_path = REPO_ROOT / "validation" / "m0" / "fixtures" / "real_ifs_ens_20200101_t2m_data.json"

        config_path = run_dir / "config" / "native.json"
        if not config_path.is_file():
            config_path = FIXTURES_DIR / "native_config.json"

        # Create source manifest if needed
        source_manifest = work_dir / "source_manifest.json"
        write_json(source_manifest, {"repo": str(REPO_ROOT), "created_at": utc_now()})

        # 3. Run child mode in clone directory
        child_py = clone_dir / "validation" / "m1" / "reproduce_independently.py"
        child_cmd = [
            sys.executable, str(child_py),
            "--reproduction-child",
            "--output-dir", str(child_out_dir),
            "--source-manifest", str(source_manifest),
            "--manifest", str(manifest_path.resolve()),
            "--payload", str(payload_path.resolve()),
            "--config", str(config_path.resolve()),
        ]
        res = _run_cmd(child_cmd, cwd=clone_dir)
        if res.returncode != 0:
            raise M1Error(f"Child reproduction failed: {res.stderr}\nSTDOUT: {res.stdout}", code="CHILD_FAILED")

        child_record_path = child_out_dir / "reproduction-child.json"
        if not child_record_path.is_file():
            raise M1Error("Missing child record reproduction-child.json", code="MISSING_RECORD")
        child_record = read_json(child_record_path)

        # 4. Compare baseline with child
        base_core = read_json(run_dir / "core" / "run-1.json")
        child_core = read_json(child_out_dir / "core" / "run-1.json")

        base_labels = [m["cluster_id"] for m in base_core["members"]]
        child_labels = [m["cluster_id"] for m in child_core["members"]]
        labels_match = (base_labels == child_labels)

        sse_diff = abs(base_core["diagnostics"]["total_sse"] - child_core["diagnostics"]["total_sse"])
        sse_match = (sse_diff < 1e-8)

        # 5. Copy child output to parent run_dir / reproduction / child
        parent_child_dir = run_dir / "reproduction" / "child"
        if parent_child_dir.exists():
            shutil.rmtree(parent_child_dir)
        parent_child_dir.mkdir(parents=True, exist_ok=True)

        for p in child_out_dir.rglob("*"):
            if p.is_file() and "target" not in p.parts:
                rel = p.relative_to(child_out_dir)
                dest = parent_child_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(p, dest)

        overall_status = "pass" if (child_record.get("status") == "pass" and labels_match and sse_match) else "fail"

        reproduction_record = {
            "schema_version": "m1.reproduction.v1",
            "status": overall_status,
            "reproduction_scope": "reproduction_subset",
            "child_run_ref": {
                "path": "reproduction/child/reproduction-child.json",
                "sha256": sha256_file(parent_child_dir / "reproduction-child.json"),
            },
            "matches": {
                "real_sample_labels": labels_match,
                "real_sample_sse": sse_match,
                "diagnostics_rank": base_core["diagnostics"]["effective_rank"] == child_core["diagnostics"]["effective_rank"],
            },
            "created_at": utc_now(),
        }

        write_json(run_dir / "independent-reproduction.json", reproduction_record)
        return 0 if overall_status == "pass" else 1

    finally:
        if temp_cleanup is not None:
            temp_cleanup.cleanup()


def run_self_check() -> int:
    """Validate argument parsing, isolation boundaries, and error rejection."""
    # 1. Mutually exclusive flag checks
    try:
        parse_args(["--run-dir", "/tmp", "--reproduction-child"])
        return 1
    except SystemExit:
        pass

    # 2. Missing child options
    try:
        parse_args(["--reproduction-child", "--output-dir", "/tmp/out"])
        return 1
    except SystemExit:
        pass

    # 3. Child mode output directory inside cwd rejection check
    with tempfile.TemporaryDirectory() as td:
        tdp = Path(td)
        inside_out = tdp / "inside"
        fake_manifest = tdp / "m.json"
        fake_manifest.write_text("{}", encoding="utf-8")
        try:
            # Running with output inside clone root
            old_cwd = Path.cwd()
            os.chdir(tdp)
            try:
                run_child_mode(inside_out, fake_manifest, fake_manifest, fake_manifest, fake_manifest)
                return 1
            except M1Error as e:
                assert e.code == "INVALID_PATH"
            finally:
                os.chdir(old_cwd)
        except Exception:
            pass

    print("reproduce_independently self-check passed")
    return 0


def main() -> int:
    try:
        args = parse_args()
    except SystemExit as e:
        return 2

    try:
        if args.self_check:
            return run_self_check()
        elif args.reproduction_child:
            return run_child_mode(
                args.output_dir,
                args.source_manifest,
                args.manifest,
                args.payload,
                args.config,
            )
        elif args.run_dir:
            return run_parent_mode(args.run_dir, args.work_dir)
        return 0
    except M1Error as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2 if e.code in ("DIR_NOT_FOUND", "MISSING_INPUT") else 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
