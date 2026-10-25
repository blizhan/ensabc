#!/usr/bin/env python3
"""Execute the full M1 gate and generate the m1.report.v1 evidence bundle.

Usage:
    python3 validation/m1/run_m1_gate.py --run-id ID [--manifest PATH --payload PATH --config PATH]

Performs complete M1 verification workflow:
1. Environment & source snapshot (environment.json, source-identity.json)
2. Build extension & runner (build_extension.sh)
3. Prepare input & configs (prepare_input.py)
4. Six-stage numerical verification in strict order:
   - run_reference original
   - verify_core native
   - compare_results original
   - run_reference shared
   - verify_core shared
   - compare_results shared
5. Comparator self-check (compare_results.py --self-check)
6. Extension SQL integration & recovery (verify_extension.py)
7. Minimal runtime isolation test (verify_minimal_runtime.sh)
8. Task performance measurements (measure_tasks.py)
9. Independent reproduction parent mode (reproduce_independently.py --run-dir)
10. Executable Traceability Matrix (traceability.py)
11. Gate Report generation & validation (report.json)
12. Independent Report Audit (verify_report.py -> report-audit.json)

Exit codes:
    0: overall == passed
    1: overall == failed
    2: overall == incomplete
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m1.common import (
    ARTIFACTS_DIR,
    FIXTURES_DIR,
    M1Error,
    collect_source_identity,
    environment_snapshot,
    read_json,
    sha256_file,
    utc_now,
    write_json,
)
from validation.m1.schema import (
    REPORT_SCHEMA_VERSION,
    REQUIREMENT_IDS,
    SUCCESS_CRITERION_IDS,
    validate_report,
)

EXIT_PASSED = 0
EXIT_FAILED = 1
EXIT_INCOMPLETE = 2

STANDING_LIMITATIONS = [
    "Only Linux aarch64 with DuckDB CLI 1.5.5 and Python 3.13 was verified.",
    "Real ensemble weather data from WeatherBench 2 is retrieved locally via manifest; payload is not committed.",
    "Pure-Rust numerics implemented using nalgebra 0.35.0 (std-only) without external BLAS/LAPACK runtime dependencies.",
    "Dual-path comparison ensures exact partition matching, orthogonal Procrustes alignment Q, and shared Lloyd iteration trace equivalence.",
]


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run M1 scenario computation gate.")
    parser.add_argument("--run-id", required=True, help="Unique run identifier")
    parser.add_argument("--manifest", type=Path, default=REPO_ROOT / "validation" / "m0" / "fixtures" / "real_ifs_ens_20200101_t2m_manifest.json")
    parser.add_argument("--payload", type=Path, default=REPO_ROOT / "validation" / "m0" / "fixtures" / "real_ifs_ens_20200101_t2m_data.json")
    parser.add_argument("--config", type=Path, default=FIXTURES_DIR / "native_config.json")
    return parser.parse_args(args)


def run_step(cmd: list[str], cwd: Path | None = None, env: dict[str, str] | None = None, timeout: float = 600.0) -> tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def build_report(run_dir: Path, run_id: str, trace_data: dict[str, Any], steps_data: dict[str, Any]) -> dict[str, Any]:
    req_results = trace_data["requirement_results"]
    sc_results = trace_data["success_criteria_results"]
    assertions = {a["id"]: a for a in trace_data["assertions"]}

    # Build artifact ref helper
    def make_ref(rel_path: str) -> dict[str, str]:
        target = run_dir / rel_path
        if not target.is_file():
            raise M1Error(f"Artifact {rel_path} does not exist", code="MISSING_ARTIFACT")
        return {
            "path": rel_path,
            "sha256": sha256_file(target),
        }

    # Map requirements
    req_list = []
    any_fail = False
    any_incomplete = False

    for rid in REQUIREMENT_IDS:
        r_info = req_results[rid]
        st = r_info["status"]
        if st == "fail":
            any_fail = True
        elif st == "not_run":
            any_incomplete = True

        a_ids = r_info["assertion_ids"]
        # Find artifacts associated with these assertions
        rel_artifacts = set()
        for aid in a_ids:
            a = assertions[aid]
            p = a.get("path", "")
            if ":" in p and not p.startswith("repo:"):
                rel_artifacts.add(p.split(":", 1)[0])

        artifact_refs = [make_ref(p) for p in sorted(rel_artifacts) if (run_dir / p).is_file()]

        req_list.append({
            "id": rid,
            "status": st,
            "assertion_ids": a_ids,
            "expected": f"All assertions for {rid} pass",
            "observed": f"Assertions: {', '.join(a_ids)}",
            "artifacts": artifact_refs,
            "reason": r_info.get("reason"),
        })

    # Map success criteria
    sc_list = []
    for scid in SUCCESS_CRITERION_IDS:
        sc_info = sc_results[scid]
        st = sc_info["status"]
        if st == "fail":
            any_fail = True
        elif st == "not_run":
            any_incomplete = True

        a_ids = sc_info["assertion_ids"]
        rel_artifacts = set()
        for aid in a_ids:
            a = assertions[aid]
            p = a.get("path", "")
            if ":" in p and not p.startswith("repo:"):
                rel_artifacts.add(p.split(":", 1)[0])

        artifact_refs = [make_ref(p) for p in sorted(rel_artifacts) if (run_dir / p).is_file()]

        sc_list.append({
            "id": scid,
            "status": st,
            "assertion_ids": a_ids,
            "expected": f"All assertions for {scid} pass",
            "observed": f"Assertions: {', '.join(a_ids)}",
            "artifacts": artifact_refs,
            "reason": sc_info.get("reason"),
        })

    if any_fail:
        overall = "failed"
    elif any_incomplete:
        overall = "incomplete"
    else:
        overall = "passed"

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": run_id,
        "created_at": utc_now(),
        "overall": overall,
        "algorithm_version": "m1.native.v1",
        "source_identity_ref": make_ref("source-identity.json"),
        "environment_ref": make_ref("environment.json"),
        "input_refs": [make_ref("inputs/task.json"), make_ref("inputs/identity.json")],
        "config_refs": [make_ref("config/native.json")],
        "steps_ref": make_ref("steps.json"),
        "traceability_ref": make_ref("traceability.json"),
        "requirements": req_list,
        "success_criteria": sc_list,
        "limitations": STANDING_LIMITATIONS,
    }

    validate_report(report, run_dir)
    write_json(run_dir / "report.json", report)
    return report


def main() -> int:
    try:
        args = parse_args()
    except SystemExit:
        return EXIT_INCOMPLETE

    run_dir = ARTIFACTS_DIR / args.run_id
    if run_dir.exists():
        print(f"Error: Run directory already exists: {run_dir}", file=sys.stderr)
        return EXIT_INCOMPLETE

    run_dir.mkdir(parents=True, exist_ok=True)
    steps: dict[str, Any] = {}
    target_dir = Path("/tmp/ensabc-m1-extension-target")
    target_dir.mkdir(parents=True, exist_ok=True)
    extension_path = target_dir / "ensabc_m0_extension.duckdb_extension"

    def record_step(name: str, status: str, detail: str = ""):
        steps[name] = {
            "status": status,
            "detail": detail,
            "timestamp": utc_now(),
        }
        print(f"[{name}] {status}: {detail}")

    try:
        # Step 1: Environment & source identity
        env_record = environment_snapshot()
        write_json(run_dir / "environment.json", env_record)
        source_record = collect_source_identity()
        write_json(run_dir / "source-identity.json", source_record)
        record_step("environment", "pass", "Environment and source identity recorded")

        # Step 2: Build extension
        code, out, err = run_step(["bash", str(REPO_ROOT / "validation" / "m1" / "build_extension.sh"), "--output-dir", str(target_dir)])
        if code != 0:
            record_step("build_extension", "fail", err or out)
            raise M1Error("Build extension failed")
        record_step("build_extension", "pass", "Extension and runner built")

        # Step 3: Prepare input
        code, out, err = run_step([
            sys.executable, str(REPO_ROOT / "validation" / "m1" / "prepare_input.py"),
            "--manifest", str(args.manifest.resolve()),
            "--payload", str(args.payload.resolve()),
            "--config", str(args.config.resolve()),
            "--output-dir", str(run_dir),
        ])
        if code != 0:
            record_step("prepare_input", "fail", err or out)
            raise M1Error("Prepare input failed")
        record_step("prepare_input", "pass", "Input and configs prepared")

        # Step 4: Six-stage numerical sequence
        # Stage 1: run_reference original
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "run_reference.py"), "--run-dir", str(run_dir), "--stage", "original"])
        if code != 0:
            record_step("run_reference_original", "fail", err or out)
            raise M1Error("run_reference original failed")
        record_step("run_reference_original", "pass")

        # Stage 2: verify_core native
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "verify_core.py"), "--run-dir", str(run_dir), "--stage", "native"])
        if code != 0:
            record_step("verify_core_native", "fail", err or out)
            raise M1Error("verify_core native failed")
        record_step("verify_core_native", "pass")

        # Stage 3: compare_results original
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "compare_results.py"), "--run-dir", str(run_dir), "--stage", "original"])
        if code != 0:
            record_step("compare_results_original", "fail", err or out)
            raise M1Error("compare_results original failed")
        record_step("compare_results_original", "pass")

        # Stage 4: run_reference shared
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "run_reference.py"), "--run-dir", str(run_dir), "--stage", "shared"])
        if code != 0:
            record_step("run_reference_shared", "fail", err or out)
            raise M1Error("run_reference shared failed")
        record_step("run_reference_shared", "pass")

        # Stage 5: verify_core shared
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "verify_core.py"), "--run-dir", str(run_dir), "--stage", "shared"])
        if code != 0:
            record_step("verify_core_shared", "fail", err or out)
            raise M1Error("verify_core shared failed")
        record_step("verify_core_shared", "pass")

        # Stage 6: compare_results shared
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "compare_results.py"), "--run-dir", str(run_dir), "--stage", "shared"])
        if code != 0:
            record_step("compare_results_shared", "fail", err or out)
            raise M1Error("compare_results shared failed")
        record_step("compare_results_shared", "pass")

        # Step 5: Comparator self-check
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "compare_results.py"), "--self-check", "--run-dir", str(run_dir)])
        if code != 0:
            record_step("comparator_self_check", "fail", err or out)
            raise M1Error("Comparator self check failed")
        record_step("comparator_self_check", "pass")

        # Step 6: Verify extension
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "verify_extension.py"), "--run-dir", str(run_dir), "--extension", str(extension_path)])
        if code != 0:
            record_step("verify_extension", "fail", err or out)
            raise M1Error("Verify extension failed")
        record_step("verify_extension", "pass")

        # Step 7: Minimal runtime
        code, out, err = run_step(["bash", str(REPO_ROOT / "validation" / "m1" / "verify_minimal_runtime.sh"), "--run-dir", str(run_dir), "--extension", str(extension_path)])
        if code != 0:
            record_step("verify_minimal_runtime", "fail", err or out)
            raise M1Error("Verify minimal runtime failed")
        record_step("verify_minimal_runtime", "pass")

        # Step 8: Measurements
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "measure_tasks.py"), "--run-dir", str(run_dir), "--extension", str(extension_path)])
        if code != 0:
            record_step("measure_tasks", "fail", err or out)
            raise M1Error("Measure tasks failed")
        record_step("measure_tasks", "pass")

        # Step 9: Independent reproduction
        code, out, err = run_step([sys.executable, str(REPO_ROOT / "validation" / "m1" / "reproduce_independently.py"), "--run-dir", str(run_dir)])
        if code != 0:
            record_step("independent_reproduction", "fail", err or out)
            raise M1Error("Independent reproduction failed")
        record_step("independent_reproduction", "pass")

        # Record steps.json
        write_json(run_dir / "steps.json", {"schema_version": "m1.steps.v1", "steps": steps, "created_at": utc_now()})

        # Step 10: Traceability matrix
        from validation.m1.traceability import run_traceability
        trace_data = run_traceability(run_dir)
        record_step("traceability", trace_data["status"])

        # Step 11: Build report
        report = build_report(run_dir, args.run_id, trace_data, steps)
        record_step("report", "pass", f"Overall: {report['overall']}")

        # Step 12: Independent report audit
        from validation.m1.verify_report import audit_report
        audit_res = audit_report(run_dir)
        record_step("report_audit", audit_res["status"])

        if report["overall"] == "passed" and audit_res["status"] == "pass" and trace_data["status"] == "pass":
            print(f"\n=======================================================")
            print(f"M1 GATE PASSED: Run ID {args.run_id}")
            print(f"Artifacts: {run_dir}")
            print(f"=======================================================\n")
            return EXIT_PASSED
        elif report["overall"] == "failed" or audit_res["status"] == "fail":
            return EXIT_FAILED
        else:
            return EXIT_INCOMPLETE

    except Exception as e:
        record_step("failure", "fail", str(e))
        write_json(run_dir / "steps.json", {"schema_version": "m1.steps.v1", "steps": steps, "created_at": utc_now()})
        print(f"M1 Gate execution failed: {e}", file=sys.stderr)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
