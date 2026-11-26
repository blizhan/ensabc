#!/usr/bin/env python3
"""Anti-circular read-only report auditor and negative tamper self-check for M3 (T044).

Usage:
    validation/m2/.venv/bin/python validation/m3/verify_report.py --run-dir DIR
    validation/m2/.venv/bin/python validation/m3/verify_report.py --self-check

Contract (`specs/004-m3-scenario-evolution/contracts/validation-evidence.md`):
- Strictly read-only on `report.json`, `traceability.json`, and all upstream artifacts;
  writes ONLY `<run_dir>/report-audit.json` (exits `0` iff `audit_status == "passed"`
  and `findings == []`, else exits `1`).
- Verifies `execution-steps.json` contains all 15 `REQUIRED_STEPS` with `exit_code == 0`
  and `status == "passed"`, and enforces `m1_regression` strictly precedes `m2_regression`.
- Blocks `..`, backslashes, absolute paths, and circular self-references (`report.json`,
  `report-audit.json`, `traceability.json`) in `evidence_refs`, recomputing SHA-256 of
  every referenced file on disk.
- Re-runs `TraceabilityEvaluator` in-memory read-only and verifies exact status agreement
  with `traceability.json` and `report.json` (`overall_status == "passed"` iff all steps
  and all 18 FRs & 8 SCs pass).
- Re-runs `verify_html_traceability` read-only on `visualization/real-8run-main.html` and
  `visualization/synthetic-showcase.html` against `sql-history/evolution-runs.duckdb`,
  and scans `case-study/*.json`, `traceability.json`, and `report.json` for prohibited
  affirmative migration/cyclogenesis/calibration/trading overclaims.
- `--self-check`: synthesizes a complete passing run bundle in `/tmp/`, verifies positive
  audit pass (`0` findings, `report.json` & `traceability.json` untouched), and tests 10
  negative audit mutations (covering all 8 mandatory classes + circular self-ref +
  out-of-order regression), asserting 100% interception.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import datetime as dt
import io
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    EVOLUTION_ALGORITHM_VERSION,
    LocatedError,
    NON_PHYSICAL_DISCLAIMER,
    atomic_write,
    atomic_write_json,
    collect_effective_source_manifest,
    default_evolution_config,
    ensure_no_symlink_inside,
    is_hex_sha256,
    load_json,
    sha256_file,
    sql_value,
)
from validation.m3.oracle import solve_chain_oracle
from validation.m3.render_evolution_html import (
    main as render_html_main,
    run_isolated_duckdb,
)
from validation.m3.traceability import (
    FORBIDDEN_EVIDENCE_REL_PATHS,
    REQUIREMENT_IDS,
    SUCCESS_CRITERION_IDS,
    TRACEABILITY_SCHEMA_VERSION,
    TraceabilityEvaluator,
    evaluate_traceability,
)
from validation.m3.verify_visualization_traceability import (
    SHOWCASE_RUN_ID,
    SHOWCASE_TRACKING_KEY,
    scan_prohibited_claims,
    seed_showcase_duckdb,
    verify_html_traceability,
)

REPORT_SCHEMA_VERSION = "m3.report.v1"
AUDIT_SCHEMA_VERSION = "m3.report-audit.v1"
SELF_CHECK_SCHEMA_VERSION = "m3.report-audit-self-check.v1"

REQUIRED_STEPS: tuple[str, ...] = (
    "setup_env_check",
    "cargo_unit_and_clippy",
    "build_extension",
    "prepare_om_sample",
    "unit_suites",
    "verify_evolution_core",
    "verify_sql_and_history",
    "run_case_and_sensitivity",
    "render_and_verify_visualization",
    "verify_minimal_runtime",
    "measure_pipeline",
    "m1_regression",
    "m2_regression",
    "independent_reproduction",
    "traceability",
)


_ensure_no_symlink_inside = ensure_no_symlink_inside

REQUIRED_BUILD_BINARIES: tuple[tuple[str, str, str], ...] = (
    ("duckdb_extension", "extension_sha256", "ensabc_m0_extension.duckdb_extension"),
    ("m1_runner", "m1_runner_sha256", "m1_runner"),
    ("m2_runner", "m2_runner_sha256", "m2_runner"),
    ("m3_runner", "m3_runner_sha256", "m3_runner"),
)


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _collect_json_strings(obj: Any, out: list[str]) -> None:
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_json_strings(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_json_strings(item, out)


def compose_report_dict(run_dir: Path) -> dict[str, Any]:
    """Read-only helper composing a `report.json` object from `execution-steps.json` & `traceability.json`.

    Called by `run_m3_gate.py` Step 16 (and `--self-check` fixture builder), NEVER by `audit_report`.
    """
    run_dir = run_dir.resolve()
    steps_doc = load_json(run_dir / "execution-steps.json") if (run_dir / "execution-steps.json").is_file() else {}
    steps = [
        s
        for s in steps_doc.get("steps", [])
        if isinstance(s, dict) and s.get("step") != "verify_report"
    ]
    trace_path = run_dir / "traceability.json"
    src_manifest_path = run_dir / "source-manifest.json"
    build_manifest_path = run_dir / "build" / "build_manifest.json"
    env_path = run_dir / "environment.json"

    trace_doc = load_json(trace_path)
    source_manifest = load_json(src_manifest_path) if src_manifest_path.is_file() else {}
    trace_sha = sha256_file(trace_path) if trace_path.is_file() else None
    src_manifest_sha = source_manifest.get("manifest_sha256")
    code_manifest_sha = source_manifest.get("code_manifest_sha256")
    build_manifest_sha = sha256_file(build_manifest_path) if build_manifest_path.is_file() else None

    reqs = trace_doc.get("requirements", {})
    scs = trace_doc.get("success_criteria", {})

    steps_ok = all(
        any(
            s.get("step") == req_step and s.get("status") == "passed" and s.get("exit_code") == 0
            for s in steps
        )
        for req_step in REQUIRED_STEPS
    )
    m1_idx = next((i for i, s in enumerate(steps) if s.get("step") == "m1_regression"), -1)
    m2_idx = next((i for i, s in enumerate(steps) if s.get("step") == "m2_regression"), -1)
    serial_regression_ok = 0 <= m1_idx < m2_idx

    all_fr_pass = len(reqs) == len(REQUIREMENT_IDS) and all(
        reqs.get(fid, {}).get("status") == "pass" for fid in REQUIREMENT_IDS
    )
    all_sc_pass = len(scs) == len(SUCCESS_CRITERION_IDS) and all(
        scs.get(sid, {}).get("status") == "pass" for sid in SUCCESS_CRITERION_IDS
    )
    trace_ok = trace_doc.get("status") == "pass" and all_fr_pass and all_sc_pass
    manifests_ok = (
        env_path.is_file()
        and is_hex_sha256(trace_sha)
        and is_hex_sha256(src_manifest_sha)
        and is_hex_sha256(code_manifest_sha)
        and is_hex_sha256(build_manifest_sha)
    )

    overall_status = (
        "passed" if (steps_ok and serial_regression_ok and trace_ok and manifests_ok) else "failed"
    )
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": trace_doc.get("run_id", run_dir.name),
        "created_at_utc": _utc_now(),
        "overall_status": overall_status,
        "traceability_sha256": trace_sha,
        "source_manifest_sha256": src_manifest_sha,
        "code_manifest_sha256": code_manifest_sha,
        "build_manifest_sha256": build_manifest_sha,
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "interpretation_boundaries": [
            "Split/merge edges and attributed_flow are algorithmic attributions, NOT physical ensemble member migration.",
            "Emergence and disappearance denote threshold-relative absence of candidates, NOT physical cyclogenesis or lysis.",
            "member_fraction is an uncalibrated sample proportion, NOT a calibrated probability or trading/skill claim.",
        ],
        "steps": steps,
        "traceability_summary": trace_doc.get("summary", {}),
        "requirements": reqs,
        "success_criteria": scs,
    }


def _audit_evidence_refs(
    run_dir: Path,
    doc_label: str,
    doc: dict[str, Any],
    findings: list[str],
) -> int:
    """Validate path containment, acyclicity, and SHA-256 digests across all `evidence_refs`."""
    audited_count = 0
    for sec_name, expected_ids in (
        ("requirements", REQUIREMENT_IDS),
        ("success_criteria", SUCCESS_CRITERION_IDS),
    ):
        sec = doc.get(sec_name)
        if not isinstance(sec, dict):
            findings.append(f"{doc_label}: missing or non-dict section {sec_name!r}")
            continue
        for item_id in expected_ids:
            entry = sec.get(item_id)
            if not isinstance(entry, dict):
                findings.append(f"{doc_label}.{sec_name}: missing entry {item_id}")
                continue
            refs = entry.get("evidence_refs")
            if not isinstance(refs, list) or len(refs) == 0:
                findings.append(f"{doc_label}.{sec_name}.{item_id}: empty or missing evidence_refs")
                continue
            for idx, ref in enumerate(refs):
                loc = f"{doc_label}.{sec_name}.{item_id}.evidence_refs[{idx}]"
                if not isinstance(ref, dict):
                    findings.append(f"{loc}: entry is not an object")
                    continue
                rel_path = ref.get("path")
                exp_sha = ref.get("sha256")
                if not isinstance(rel_path, str) or not rel_path.strip():
                    findings.append(f"{loc}: empty or non-string path")
                    continue
                if not is_hex_sha256(exp_sha):
                    findings.append(f"{loc}: invalid sha256 digest {exp_sha!r}")
                    continue

                p = Path(rel_path)
                if p.is_absolute() or ".." in p.parts or "\\" in rel_path:
                    findings.append(f"{loc}: path traversal or absolute path blocked: {rel_path!r}")
                    continue
                norm_posix = p.as_posix()
                if norm_posix in FORBIDDEN_EVIDENCE_REL_PATHS:
                    findings.append(
                        f"{loc}: circular evidence self-reference blocked: {norm_posix!r}"
                    )
                    continue
                try:
                    _ensure_no_symlink_inside(run_dir, run_dir / norm_posix)
                except (LocatedError, ValueError) as exc:
                    findings.append(f"{loc}: symlink alias blocked: {exc}")
                    continue
                resolved = (run_dir / norm_posix).resolve()
                if not resolved.is_relative_to(run_dir):
                    findings.append(f"{loc}: resolved path escapes run_dir: {rel_path!r}")
                    continue
                resolved_rel = resolved.relative_to(run_dir).as_posix()
                if resolved_rel in FORBIDDEN_EVIDENCE_REL_PATHS:
                    findings.append(
                        f"{loc}: circular evidence self-reference blocked: {resolved_rel!r}"
                    )
                    continue
                if not resolved.is_file():
                    findings.append(f"{loc}: referenced evidence file missing on disk: {norm_posix!r}")
                    continue
                actual_sha = sha256_file(resolved)
                if actual_sha != exp_sha:
                    findings.append(
                        f"{loc}: SHA-256 mismatch for {norm_posix}: expected {exp_sha}, got {actual_sha}"
                    )
                audited_count += 1
    return audited_count


def audit_report(run_dir: Path, *, write_audit: bool = True) -> dict[str, Any]:
    """Strictly read-only auditor over `report.json`, `traceability.json`, and run artifacts."""
    if run_dir.is_symlink():
        return {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "run_id": run_dir.name,
            "audited_at_utc": _utc_now(),
            "audit_status": "failed",
            "report_sha256": None,
            "traceability_sha256": None,
            "audited_evidence_refs_count": 0,
            "recomputed_traceability_status": "fail",
            "findings": [f"Symlink run_dir is forbidden: {run_dir}"],
        }

    run_dir = run_dir.resolve()
    findings: list[str] = []

    def _load_checked_json_dict(
        rel_str: str,
        *,
        missing_msg: str | None = None,
        required: bool = True,
    ) -> tuple[Path, dict[str, Any] | None, bool]:
        target = run_dir / rel_str
        try:
            _ensure_no_symlink_inside(run_dir, target)
        except (LocatedError, ValueError) as exc:
            findings.append(f"Symlink blocked for {rel_str}: {exc}")
            return (target, None, True)
        if not target.is_file():
            if required:
                findings.append(missing_msg or f"Missing required file: {rel_str}")
            return (target, None, False)
        try:
            loaded = load_json(target)
            if isinstance(loaded, dict):
                return (target, loaded, False)
            findings.append(f"{rel_str} root must be a JSON object")
        except Exception as exc:
            findings.append(f"Failed parsing {rel_str}: {exc}")
        return (target, None, False)

    report_path, report_doc, report_sym = _load_checked_json_dict(
        "report.json",
        missing_msg="Missing report.json (verify_report is strictly read-only and never synthesizes report.json)",
    )
    trace_path, trace_doc, trace_sym = _load_checked_json_dict("traceability.json")
    steps_path, steps_doc, _ = _load_checked_json_dict("execution-steps.json")
    env_path, env_doc, _ = _load_checked_json_dict("environment.json")
    src_manifest_path, src_manifest_doc, _ = _load_checked_json_dict("source-manifest.json")
    build_manifest_path, build_manifest_doc, build_manifest_sym = _load_checked_json_dict(
        "build/build_manifest.json"
    )
    _, repro_manifest_doc, _ = _load_checked_json_dict(
        "reproduction/effective-source-manifest.json",
        required=False,
    )

    if env_doc is not None:
        if str(env_doc.get("status", "")).strip().lower() not in ("pass", "passed"):
            findings.append(f"environment.json status={env_doc.get('status')!r} did not pass")

    # Verify traceability_sha256 binding in report.json
    actual_trace_sha = sha256_file(trace_path) if (not trace_sym and trace_path.is_file()) else None
    if report_doc is not None:
        rep_trace_sha = report_doc.get("traceability_sha256")
        if not is_hex_sha256(rep_trace_sha):
            findings.append(
                f"report.json missing or invalid 64-hex traceability_sha256: {rep_trace_sha!r}"
            )
        elif actual_trace_sha is not None and rep_trace_sha != actual_trace_sha:
            findings.append(
                f"report.json traceability_sha256 ({rep_trace_sha}) != actual traceability.json ({actual_trace_sha})"
            )

    # Verify source-manifest.json & code_manifest_sha256 drift/binding
    if src_manifest_doc is not None:
        sm_sha = src_manifest_doc.get("manifest_sha256")
        cm_sha = src_manifest_doc.get("code_manifest_sha256")
        if not is_hex_sha256(sm_sha):
            findings.append(f"source-manifest.json invalid 64-hex manifest_sha256: {sm_sha!r}")
        if not is_hex_sha256(cm_sha):
            findings.append(f"source-manifest.json invalid 64-hex code_manifest_sha256: {cm_sha!r}")

        if report_doc is not None:
            rep_cm_sha = report_doc.get("code_manifest_sha256")
            if not is_hex_sha256(rep_cm_sha) or rep_cm_sha != cm_sha:
                findings.append(
                    f"report.json code_manifest_sha256 ({rep_cm_sha!r}) != source-manifest.json ({cm_sha!r})"
                )
            if "source_manifest_sha256" in report_doc and report_doc.get("source_manifest_sha256") != sm_sha:
                findings.append(
                    f"report.json source_manifest_sha256 ({report_doc.get('source_manifest_sha256')!r}) != source-manifest.json ({sm_sha!r})"
                )

        if repro_manifest_doc is not None:
            repro_cm = repro_manifest_doc.get("code_manifest_sha256")
            if repro_cm != cm_sha:
                findings.append(
                    f"reproduction/effective-source-manifest.json code_manifest_sha256 ({repro_cm!r}) != source-manifest.json ({cm_sha!r})"
                )

        is_mock_bundle = src_manifest_doc.get("mock_bundle") is True
        inside_repo = run_dir.is_relative_to(REPO_ROOT.resolve())
        if is_mock_bundle and inside_repo:
            findings.append(
                "FORBIDDEN_MOCK_BUNDLE: source-manifest.json sets mock_bundle=true inside REPO_ROOT"
            )
        elif not is_mock_bundle:
            live_manifest = collect_effective_source_manifest(REPO_ROOT)
            if live_manifest["code_manifest_sha256"] != cm_sha:
                findings.append(
                    f"SOURCE_MANIFEST_DRIFT: live code_manifest_sha256 ({live_manifest['code_manifest_sha256']}) "
                    f"!= source-manifest.json ({cm_sha})"
                )

    # Verify build/build_manifest.json & built binary hashes
    actual_build_manifest_sha = (
        sha256_file(build_manifest_path)
        if (not build_manifest_sym and build_manifest_path.is_file())
        else None
    )
    if report_doc is not None:
        rep_bm_sha = report_doc.get("build_manifest_sha256")
        if not is_hex_sha256(rep_bm_sha):
            findings.append(
                f"report.json missing or invalid 64-hex build_manifest_sha256: {rep_bm_sha!r}"
            )
        elif actual_build_manifest_sha is not None and rep_bm_sha != actual_build_manifest_sha:
            findings.append(
                f"report.json build_manifest_sha256 ({rep_bm_sha}) != actual build_manifest.json ({actual_build_manifest_sha})"
            )

    if build_manifest_doc is not None:
        if not is_hex_sha256(build_manifest_doc.get("rust_source_sha256")):
            findings.append("build/build_manifest.json missing valid 64-hex rust_source_sha256")
        binaries_obj = build_manifest_doc.get("binaries")
        for bin_key, flat_sha_key, bin_filename in REQUIRED_BUILD_BINARIES:
            recorded_sha: Any = None
            if isinstance(binaries_obj, dict) and isinstance(binaries_obj.get(bin_key), dict):
                recorded_sha = binaries_obj[bin_key].get("sha256")
            elif flat_sha_key in build_manifest_doc:
                recorded_sha = build_manifest_doc.get(flat_sha_key)

            if not is_hex_sha256(recorded_sha):
                findings.append(
                    f"build/build_manifest.json missing valid 64-hex hash for {bin_key} ({bin_filename})"
                )
                continue
            bin_path = run_dir / "build" / bin_filename
            try:
                _ensure_no_symlink_inside(run_dir, bin_path)
            except (LocatedError, ValueError) as exc:
                findings.append(f"Symlink blocked for build/{bin_filename}: {exc}")
                continue
            if not bin_path.is_file():
                findings.append(f"Missing built binary in run_dir/build: {bin_filename}")
                continue
            actual_bin_sha = sha256_file(bin_path)
            if actual_bin_sha != recorded_sha:
                findings.append(
                    f"Binary SHA-256 mismatch for build/{bin_filename}: expected {recorded_sha}, got {actual_bin_sha}"
                )

    # 1. Verify execution-steps.json contains all REQUIRED_STEPS with exit_code == 0 & status == "passed"
    exec_steps: list[dict[str, Any]] = []
    if steps_doc is not None:
        raw_steps = steps_doc.get("steps")
        if not isinstance(raw_steps, list):
            findings.append("execution-steps.json 'steps' is not a list")
        else:
            exec_steps = [
                s for s in raw_steps if isinstance(s, dict) and s.get("step") != "verify_report"
            ]
            for req_step in REQUIRED_STEPS:
                matching = [s for s in exec_steps if s.get("step") == req_step]
                if not matching:
                    findings.append(f"Missing required step in execution-steps.json: {req_step}")
                else:
                    last_rec = matching[-1]
                    if last_rec.get("exit_code") != 0 or last_rec.get("status") != "passed":
                        findings.append(
                            f"Required step {req_step} did not pass cleanly: "
                            f"status={last_rec.get('status')!r}, exit_code={last_rec.get('exit_code')!r}"
                        )

            m1_indices = [i for i, s in enumerate(exec_steps) if s.get("step") == "m1_regression"]
            m2_indices = [i for i, s in enumerate(exec_steps) if s.get("step") == "m2_regression"]
            if m1_indices and m2_indices and not (m1_indices[-1] < m2_indices[0]):
                findings.append(
                    "m1_regression must run sequentially strictly before m2_regression in execution-steps.json"
                )

    if report_doc is not None:
        if report_doc.get("schema_version") != REPORT_SCHEMA_VERSION:
            findings.append(
                f"report.json schema_version={report_doc.get('schema_version')!r} != {REPORT_SCHEMA_VERSION!r}"
            )
        if report_doc.get("non_physical_disclaimer") != NON_PHYSICAL_DISCLAIMER:
            findings.append("report.json missing or mismatched non_physical_disclaimer")
        rep_steps = [
            s
            for s in (report_doc.get("steps") or [])
            if isinstance(s, dict) and s.get("step") != "verify_report"
        ]
        if steps_doc is not None and rep_steps != exec_steps:
            findings.append("report.json 'steps' disagrees with execution-steps.json")

    if trace_doc is not None and trace_doc.get("schema_version") != TRACEABILITY_SCHEMA_VERSION:
        findings.append(
            f"traceability.json schema_version={trace_doc.get('schema_version')!r} != {TRACEABILITY_SCHEMA_VERSION!r}"
        )

    # 2. Path traversal blocking, circular ref blocking, and SHA-256 recomputation on evidence_refs
    audited_refs_count = 0
    if trace_doc is not None:
        audited_refs_count += _audit_evidence_refs(run_dir, "traceability.json", trace_doc, findings)
    if report_doc is not None:
        audited_refs_count += _audit_evidence_refs(run_dir, "report.json", report_doc, findings)

    # 3. Re-run TraceabilityEvaluator in-memory read-only and verify exact status agreement
    evaluator = TraceabilityEvaluator(run_dir)
    recomputed = evaluator.evaluate()

    for sec_name, item_ids in (
        ("requirements", REQUIREMENT_IDS),
        ("success_criteria", SUCCESS_CRITERION_IDS),
    ):
        for item_id in item_ids:
            rec_item = recomputed[sec_name][item_id]
            rec_status = rec_item["status"]
            if rec_status != "pass":
                findings.append(
                    f"Recomputed {item_id} status is {rec_status!r} (must be 'pass'): {rec_item['details']}"
                )
            if trace_doc is not None:
                tr_status = (trace_doc.get(sec_name) or {}).get(item_id, {}).get("status")
                if tr_status != rec_status:
                    findings.append(
                        f"traceability.json {item_id}.status={tr_status!r} disagrees with recomputed {rec_status!r}"
                    )
            if report_doc is not None:
                rp_status = (report_doc.get(sec_name) or {}).get(item_id, {}).get("status")
                if rp_status != rec_status:
                    findings.append(
                        f"report.json {item_id}.status={rp_status!r} disagrees with recomputed {rec_status!r}"
                    )

    if trace_doc is not None and trace_doc.get("status") != recomputed["status"]:
        findings.append(
            f"traceability.json overall status {trace_doc.get('status')!r} != recomputed {recomputed['status']!r}"
        )
    if trace_doc is not None and trace_doc.get("status") != "pass":
        findings.append(f"traceability.json overall status is {trace_doc.get('status')!r} (expected 'pass')")

    if report_doc is not None:
        all_steps_passed = (
            steps_doc is not None
            and all(
                any(
                    s.get("step") == req_step and s.get("status") == "passed" and s.get("exit_code") == 0
                    for s in exec_steps
                )
                for req_step in REQUIRED_STEPS
            )
        )
        expected_overall = "passed" if (all_steps_passed and recomputed["status"] == "pass") else "failed"
        if report_doc.get("overall_status") != expected_overall:
            findings.append(
                f"report.json overall_status={report_doc.get('overall_status')!r} contradicts expected {expected_overall!r}"
            )
        if report_doc.get("overall_status") != "passed":
            findings.append(
                f"report.json overall_status is {report_doc.get('overall_status')!r} (expected 'passed')"
            )

    # 4. Re-run verify_visualization_traceability read-only on both HTML views vs evolution-runs.duckdb
    db_path = run_dir / "sql-history" / "evolution-runs.duckdb"
    real_html = run_dir / "visualization" / "real-8run-main.html"
    synth_html = run_dir / "visualization" / "synthetic-showcase.html"
    real_replay_p = run_dir / "sql-history" / "real-evolution-replay.json"
    synth_replay_p = run_dir / "sql-history" / "synthetic-evolution-replay.json"

    viz_symlink_blocked = False
    for viz_target in (db_path, real_html, synth_html, real_replay_p, synth_replay_p):
        try:
            _ensure_no_symlink_inside(run_dir, viz_target)
        except (LocatedError, ValueError) as exc:
            findings.append(f"Symlink blocked on visualization/SQL artifact: {exc}")
            viz_symlink_blocked = True

    if not viz_symlink_blocked:
        if not (db_path.is_file() and real_html.is_file() and synth_html.is_file() and real_replay_p.is_file() and synth_replay_p.is_file()):
            findings.append(
                "Missing visualization HTML or sql-history/evolution-runs.duckdb for read-only HTML traceability re-audit"
            )
        else:
            try:
                real_run_id = str(load_json(real_replay_p).get("evolution_run_id", ""))
                synth_doc = load_json(synth_replay_p)
                synth_run_id = str(synth_doc.get("showcase_evolution_run_id") or synth_doc.get("evolution_run_id", ""))
                for label, html_p, erid in (
                    ("real-8run-main.html", real_html, real_run_id),
                    ("synthetic-showcase.html", synth_html, synth_run_id),
                ):
                    html_audit = verify_html_traceability(html_p, db_path, erid)
                    if html_audit["status"] != "pass":
                        for vf in html_audit["findings"][:5]:
                            findings.append(f"HTML traceability failure on {label}: {vf}")
            except Exception as exc:
                findings.append(f"Exception during read-only HTML traceability re-audit: {exc}")

    # 5. Scan case-study/*.json, traceability.json, and report.json for prohibited overclaim phrases
    text_blobs: list[str] = []
    case_dir = run_dir / "case-study"
    try:
        _ensure_no_symlink_inside(run_dir, case_dir)
        if case_dir.is_dir():
            for jf in sorted(case_dir.iterdir()):
                if jf.name.endswith(".json"):
                    try:
                        _ensure_no_symlink_inside(run_dir, jf)
                        _collect_json_strings(load_json(jf), text_blobs)
                    except Exception as exc:
                        findings.append(f"Failed reading {jf.name} during overclaim scan: {exc}")
    except (LocatedError, ValueError) as exc:
        findings.append(f"Symlink blocked on case-study directory: {exc}")
    if trace_doc is not None:
        _collect_json_strings(trace_doc, text_blobs)
    if report_doc is not None:
        _collect_json_strings(report_doc, text_blobs)

    for claim_finding in scan_prohibited_claims("\n".join(text_blobs)):
        findings.append(f"Prohibited overclaim detected: {claim_finding}")

    audit_status = "passed" if len(findings) == 0 else "failed"
    audit_record = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "run_id": (report_doc or trace_doc or {}).get("run_id", run_dir.name),
        "audited_at_utc": _utc_now(),
        "audit_status": audit_status,
        "report_sha256": sha256_file(report_path) if (not report_sym and report_path.is_file()) else None,
        "traceability_sha256": actual_trace_sha,
        "code_manifest_sha256": (src_manifest_doc or {}).get("code_manifest_sha256"),
        "build_manifest_sha256": actual_build_manifest_sha,
        "audited_evidence_refs_count": audited_refs_count,
        "recomputed_traceability_status": recomputed["status"],
        "findings": findings,
    }
    if write_audit:
        audit_out_path = run_dir / "report-audit.json"
        _ensure_no_symlink_inside(run_dir, audit_out_path)
        atomic_write_json(audit_out_path, audit_record)
    return audit_record


# ---------------------------------------------------------------------------
# Mock Passing Run Synthesizer & 10-Case Negative Self-Check (`--self-check`)
# ---------------------------------------------------------------------------


def _seed_mock_main_8run_into_duckdb(
    db_path: Path,
    *,
    evolution_run_id: str,
    tracking_run_key: str,
) -> dict[str, Any]:
    """Solve a deterministic 8-batch chain via `solve_chain_oracle` with exact WB2 topology counts
    (24 nodes, 8 branches, 16 primary + 2 split + 1 merge edges, 13 cont / 2 split / 1 merge /
    3 emerg / 4 disapp, high-uncertainty transitions `[0, 3, 6]`) and insert into all 11 tables.
    """
    batch_specs = [
        (0, "2020-01-01T00:00:00Z", 96, [0.00, 10.0, 20.0], [20, 15, 15], [0.4, 0.3, 0.3]),
        (1, "2020-01-01T12:00:00Z", 84, [0.95, 10.2, 20.2], [20, 15, 15], [0.4, 0.3, 0.3]),
        (2, "2020-01-02T00:00:00Z", 72, [0.65, 1.45, 10.4], [15, 15, 20], [0.3, 0.3, 0.4]),
        (3, "2020-01-02T12:00:00Z", 60, [0.85, 10.6, 30.0], [25, 15, 10], [0.5, 0.3, 0.2]),
        (4, "2020-01-03T00:00:00Z", 48, [1.80, 10.8, 30.2], [25, 15, 10], [0.5, 0.3, 0.2]),
        (5, "2020-01-03T12:00:00Z", 36, [1.50, 2.30, 11.0], [15, 15, 20], [0.3, 0.3, 0.4]),
        (6, "2020-01-04T00:00:00Z", 24, [0.80, 11.2, 45.0], [20, 15, 15], [0.4, 0.3, 0.3]),
        (7, "2020-01-04T12:00:00Z", 12, [1.75, 11.4, 60.0], [20, 15, 15], [0.4, 0.3, 0.3]),
    ]
    ev_cfg = default_evolution_config()
    oracle_batches = [
        {
            "ordinal": b_ord,
            "init_time_utc": iso,
            "lead_hours": lead_h,
            "status": "ok",
            "dispersion": 1.0,
            "scenarios": [
                {
                    "cluster_id": cid,
                    "member_count": mcnt,
                    "member_fraction": mfrac,
                    "mean_features": [xval, xval],
                }
                for cid, (xval, mcnt, mfrac) in enumerate(zip(xs, cnts, fracs))
            ],
        }
        for b_ord, iso, lead_h, xs, cnts, fracs in batch_specs
    ]
    chain = solve_chain_oracle(
        {
            "tracking_run_key": tracking_run_key,
            "threshold_coefficient": 1.0,
            "weights": [0.5, 0.5],
            "config": {k: v for k, v in ev_cfg.items() if k != "uncertainty_rule"},
            "batches": oracle_batches,
        }
    )
    cs = chain["chain_summary"]
    assert len(chain["nodes"]) == 24 and cs["branch_count"] == 8
    assert cs["primary_edge_count"] == 16
    assert cs["secondary_split_edge_count"] == 2 and cs["secondary_merge_edge_count"] == 1
    assert cs["continuation_count"] == 13 and cs["split_count"] == 2 and cs["merge_count"] == 1
    assert cs["emergence_count"] == 3 and cs["disappearance_count"] == 4
    hu_ords = [t["prev_ordinal"] for t in chain["transitions"] if t["high_uncertainty"]]
    assert hu_ords == [0, 3, 6], f"Unexpected mock high-uncertainty ordinals: {hu_ords}"

    exec_binding = {
        "run_id": tracking_run_key,
        "input_manifest_sha256": "a" * 64,
        "scenario_config_sha256": "b" * 64,
        "match_config_sha256": "c" * 64,
        "weights_version": "cos_lat_center_v1",
        "threshold_coefficient": 1.0,
        "cluster_count": 3,
        "software": {
            "ensabc_extension_sha256": "d" * 64,
            "duckomo_version": "v0.3.5-rc.1",
            "duckomo_sha256": "e" * 64,
            "duckdb_version": "v1.5.5",
            "cluster_algorithm_version": "m1.native.v1",
            "match_algorithm_version": "m2.match.v1",
        },
    }
    evol_binding = {
        "tracking_run_key": tracking_run_key,
        "input_manifest_sha256": "a" * 64,
        "scenario_config_sha256": "b" * 64,
        "match_config_sha256": "c" * 64,
        "evolve_config_sha256": "f" * 64,
        "weights_version": "cos_lat_center_v1",
        "secondary_rule": "nearest_feasible_v1",
        "flow_rule": "star_min_mass_proportional_v1",
        "uncertainty_rule": "m3.uncertainty.v1",
        "fraction_sum_tolerance": 1e-9,
        "high_uncertainty_threshold_ratio": 0.10,
        "high_uncertainty_optimality_ratio": 0.10,
        "high_uncertainty_secondary_ratio": 0.10,
        "software": {
            **exec_binding["software"],
            "evolution_algorithm_version": EVOLUTION_ALGORITHM_VERSION,
        },
    }

    rb_vals = []
    for b_ord, iso, lead_h, _, _, _ in batch_specs:
        ts = iso.replace("T", " ").replace("Z", "")
        rb_vals.append(
            f"({sql_value(tracking_run_key)}, {b_ord}, TIMESTAMP '{ts}', {lead_h}, 'ok', NULL, "
            f"'in_{b_ord}', 'om_{b_ord}', 1.0::DOUBLE, 'cr_{b_ord}')"
        )

    sn_vals = []
    en_vals = []
    for n in chain["nodes"]:
        sn_vals.append(
            f"({sql_value(tracking_run_key)}, {n['ordinal']}, {n['cluster_id']}, {n['canonical_rank']}, "
            f"{sql_value(n['mean_features'])}::DOUBLE[], {n['member_count']}, {n['member_fraction']!r}::DOUBLE, "
            f"'m0', 0.1::DOUBLE, {sql_value(n['branch_id'])}, {sql_value(n['assignment_reason'])})"
        )
        en_vals.append(
            f"({sql_value(evolution_run_id)}, {n['ordinal']}, {n['cluster_id']}, {n['canonical_rank']}, "
            f"{sql_value(n['branch_id'])}, {sql_value(n['mean_features'])}::DOUBLE[], {n['member_count']}, "
            f"{n['member_fraction']!r}::DOUBLE, {sql_value(n['assignment_reason'])}, "
            f"{n['in_degree_primary']}, {n['in_degree_secondary']}, {n['out_degree_primary']}, {n['out_degree_secondary']}, "
            f"{sql_value(n['incoming_role'])}, {sql_value(n['outgoing_role'])}, "
            f"{n['primary_delta_fraction']!r}::DOUBLE, {sql_value(n['has_primary_delta_fraction'])}, "
            f"{n['unattributed_inflow_fraction']!r}::DOUBLE, {n['unattributed_outflow_fraction']!r}::DOUBLE, "
            f"{sql_value(n['high_uncertainty'])})"
        )

    sl_vals = []
    ee_vals = []
    for e in chain["edges"]:
        has_opt = e["optimality_margin"] is not None
        opt_m = float(e["optimality_margin"]) if has_opt else 0.0
        opt_r = float(e["optimality_margin_ratio"]) if has_opt else 0.0
        has_sec = e["secondary_margin"] is not None
        sec_m = float(e["secondary_margin"]) if has_sec else 0.0
        sec_r = float(e["secondary_margin_ratio"]) if has_sec else 0.0
        if e["edge_role"] == "primary":
            sl_vals.append(
                f"({sql_value(tracking_run_key)}, {e['prev_ordinal']}, {e['prev_cluster_id']}, "
                f"{e['curr_ordinal']}, {e['curr_cluster_id']}, {e['distance']!r}::DOUBLE, "
                f"{e['distance_over_scale']!r}::DOUBLE, {sql_value(e['primary_alternative_status'])}, "
                f"{sql_value(e['ambiguous_tie'])})"
            )
        ee_vals.append(
            f"({sql_value(evolution_run_id)}, {e['prev_ordinal']}, {e['prev_cluster_id']}, "
            f"{e['curr_ordinal']}, {e['curr_cluster_id']}, {sql_value(e['prev_branch_id'])}, "
            f"{sql_value(e['curr_branch_id'])}, {sql_value(e['edge_role'])}, {sql_value(e['event_type'])}, "
            f"{sql_value(e['rule_basis'])}, {e['distance']!r}::DOUBLE, {e['distance_over_scale']!r}::DOUBLE, "
            f"{e['prev_member_fraction']!r}::DOUBLE, {e['curr_member_fraction']!r}::DOUBLE, "
            f"{e['edge_delta_fraction']!r}::DOUBLE, {e['attributed_flow']!r}::DOUBLE, "
            f"{e['edge_strength']!r}::DOUBLE, {e['threshold_margin']!r}::DOUBLE, "
            f"{e['threshold_margin_ratio']!r}::DOUBLE, {sql_value(e['primary_alternative_status'])}, "
            f"{sql_value(has_opt)}, {opt_m!r}::DOUBLE, {opt_r!r}::DOUBLE, "
            f"{sql_value(has_sec)}, {sec_m!r}::DOUBLE, {sec_r!r}::DOUBLE, "
            f"{sql_value(e['ambiguous_tie'])}, {sql_value(e['high_uncertainty'])}, "
            f"{sql_value(e['uncertainty_reasons'])}::VARCHAR[], {sql_value(NON_PHYSICAL_DISCLAIMER)})"
        )

    et_vals = []
    for t in chain["transitions"]:
        er = t["evolve_result"]
        er_sql = sql_value(
            {
                "algorithm_version": "m3.evolve.v1",
                "summary": {
                    "primary_edge_count": int(er["summary"]["primary_edge_count"]),
                    "secondary_split_edge_count": int(er["summary"]["secondary_split_edge_count"]),
                    "secondary_merge_edge_count": int(er["summary"]["secondary_merge_edge_count"]),
                    "transition_tv_redistribution": float(er["summary"]["transition_tv_redistribution"]),
                    "min_threshold_margin": float(er["summary"]["min_threshold_margin"]),
                    "ambiguous_tie": bool(er["summary"]["ambiguous_tie"]),
                    "high_uncertainty": bool(er["summary"]["high_uncertainty"]),
                },
                "edges": [
                    {
                        "prev_cluster_id": int(ee["prev_cluster_id"]),
                        "curr_cluster_id": int(ee["curr_cluster_id"]),
                        "edge_role": str(ee["edge_role"]),
                        "event_type": str(ee["event_type"]),
                        "rule_basis": str(ee["rule_basis"]),
                        "distance": float(ee["distance"]),
                        "attributed_flow": float(ee["attributed_flow"]),
                    }
                    for ee in er["edges"]
                ],
                "events": [
                    {
                        "event_index": int(eev["event_index"]),
                        "event_type": str(eev["event_type"]),
                        "prev_cluster_ids": [int(x) for x in eev["prev_cluster_ids"]],
                        "curr_cluster_ids": [int(x) for x in eev["curr_cluster_ids"]],
                        "rule_basis": str(eev["rule_basis"]),
                        "net_delta_fraction": float(eev["net_delta_fraction"]),
                        "attributed_flow_sum": float(eev["attributed_flow_sum"]),
                    }
                    for eev in er["events"]
                ],
            }
        )
        et_vals.append(
            f"({sql_value(evolution_run_id)}, {t['prev_ordinal']}, {t['curr_ordinal']}, 'ok', false, NULL, "
            f"1.0::DOUBLE, 1.0::DOUBLE, {t['primary_edge_count']}, {t['secondary_split_edge_count']}, "
            f"{t['secondary_merge_edge_count']}, {t['transition_tv_redistribution']!r}::DOUBLE, "
            f"{t['min_threshold_margin']!r}::DOUBLE, {sql_value(t['ambiguous_tie'])}, "
            f"{sql_value(t['high_uncertainty'])}, {er_sql})"
        )

    ev_vals = []
    for ev in chain["events"]:
        has_mmr = ev["min_margin_ratio"] is not None
        mmr = float(ev["min_margin_ratio"]) if has_mmr else 0.0
        mmos = float(ev["max_morphology_over_scale"]) if ev["max_morphology_over_scale"] is not None else 0.0
        ev_vals.append(
            f"({sql_value(evolution_run_id)}, {ev['prev_ordinal']}, {ev['event_index']}, "
            f"{sql_value(ev['event_type'])}, {sql_value(ev['prev_cluster_ids'])}::INTEGER[], "
            f"{sql_value(ev['curr_cluster_ids'])}::INTEGER[], {sql_value(ev['branch_ids'])}::VARCHAR[], "
            f"{sql_value(ev['primary_branch_id'])}, {sql_value(ev['rule_basis'])}, "
            f"{ev['max_morphology_distance']!r}::DOUBLE, {mmos!r}::DOUBLE, "
            f"{ev['prev_fraction_sum']!r}::DOUBLE, {ev['curr_fraction_sum']!r}::DOUBLE, "
            f"{ev['net_delta_fraction']!r}::DOUBLE, {ev['attributed_flow_sum']!r}::DOUBLE, "
            f"{sql_value(has_mmr)}, {mmr!r}::DOUBLE, {sql_value(ev['ambiguous_tie'])}, "
            f"{sql_value(ev['high_uncertainty'])}, {sql_value(NON_PHYSICAL_DISCLAIMER)})"
        )

    ebs_vals = []
    branches_list = chain["branch_summaries"]
    for br in branches_list:
        ebs_vals.append(
            f"({sql_value(evolution_run_id)}, {sql_value(br['branch_id'])}, {br['birth_ordinal']}, "
            f"{br['end_ordinal']}, {br['node_count']}, {br['step_count']}, "
            f"{sql_value(br['birth_reason'])}, {sql_value(br['termination_reason'])}, "
            f"{br['cumulative_morphology_distance']!r}::DOUBLE, {br['cumulative_morphology_over_scale']!r}::DOUBLE, "
            f"{br['max_step_morphology_distance']!r}::DOUBLE, {br['max_step_morphology_over_scale']!r}::DOUBLE, "
            f"{br['end_to_end_morphology_distance']!r}::DOUBLE, {br['initial_member_fraction']!r}::DOUBLE, "
            f"{br['final_member_fraction']!r}::DOUBLE, {br['min_member_fraction']!r}::DOUBLE, "
            f"{br['max_member_fraction']!r}::DOUBLE, {br['net_delta_fraction']!r}::DOUBLE, "
            f"{br['cumulative_abs_delta_fraction']!r}::DOUBLE, {br['split_out_event_count']}, "
            f"{sql_value(br['spawned_secondary_branch_ids'])}::VARCHAR[], {sql_value(br['split_from_parent_branch_id'])}, "
            f"{br['merge_in_event_count']}, {sql_value(br['absorbed_secondary_branch_ids'])}::VARCHAR[], "
            f"{sql_value(br['merged_into_branch_id'])}, {br['ambiguous_step_count']}, "
            f"{br['high_uncertainty_step_count']}, {br['high_uncertainty_step_ratio']!r}::DOUBLE)"
        )

    sql = f"""
    BEGIN TRANSACTION;
    INSERT INTO tracking_runs VALUES (
        {sql_value(tracking_run_key)},
        {sql_value(exec_binding)},
        [0.5, 0.5]::DOUBLE[],
        8,
        '9999999999999999999999999999999999999999999999999999999999999999'
    );
    INSERT INTO replay_batches VALUES {', '.join(rb_vals)};
    INSERT INTO scenario_nodes VALUES {', '.join(sn_vals)};
    INSERT INTO pair_comparisons
    SELECT
        {sql_value(tracking_run_key)},
        prev_ord,
        prev_ord + 1,
        1.0::DOUBLE,
        1.0::DOUBLE,
        {{
            'algorithm_version': 'm2.match.v1',
            'prev_cluster_ids': [0::BIGINT, 1::BIGINT, 2::BIGINT],
            'curr_cluster_ids': [0::BIGINT, 1::BIGINT, 2::BIGINT],
            'prev_dispersion': 1.0::DOUBLE,
            'curr_dispersion': 1.0::DOUBLE,
            'pair_scale': 1.0::DOUBLE,
            'threshold_coefficient': 1.0::DOUBLE,
            'threshold': 1.0::DOUBLE,
            'distance_matrix': [[0.2::DOUBLE]]::DOUBLE[][],
            'distance_ratio_matrix': [[0.2::DOUBLE]]::DOUBLE[][],
            'eligible_matrix': [[true]]::BOOLEAN[][],
            'edge_count': 2::BIGINT,
            'total_distance': 0.4::DOUBLE,
            'optimal_solution_count': 1::BIGINT,
            'ambiguous': false,
            'threshold_margin': 0.5::DOUBLE,
            'optimality_margin': 0.2::DOUBLE,
            'has_optimality_margin': true,
            'links': []::STRUCT(prev_cluster_id BIGINT, curr_cluster_id BIGINT, distance DOUBLE, distance_ratio DOUBLE, alternative_status VARCHAR, alternative_edge_count BIGINT, alternative_total_distance DOUBLE, alternative_distance_increase DOUBLE, alternative_distance_increase_ratio DOUBLE, has_same_count_alternative BOOLEAN, ambiguous BOOLEAN)[],
            'unmatched_prev': []::STRUCT(cluster_id BIGINT, reason VARCHAR, min_candidate_distance DOUBLE, matched_in_some_optimum BOOLEAN)[],
            'unmatched_curr': []::STRUCT(cluster_id BIGINT, reason VARCHAR, min_candidate_distance DOUBLE, matched_in_some_optimum BOOLEAN)[]
        }}
    FROM range(0, 7) t(prev_ord);
    INSERT INTO scenario_links VALUES {', '.join(sl_vals)};
    INSERT INTO evolution_runs VALUES (
        {sql_value(evolution_run_id)},
        {sql_value(tracking_run_key)},
        {sql_value(evol_binding)},
        [0.5, 0.5]::DOUBLE[],
        8,
        {sql_value(cs)},
        {sql_value(NON_PHYSICAL_DISCLAIMER)},
        'abababababababababababababababababababababababababababababababab'
    );
    INSERT INTO evolution_nodes VALUES {', '.join(en_vals)};
    INSERT INTO evolution_transitions VALUES {', '.join(et_vals)};
    INSERT INTO evolution_edges VALUES {', '.join(ee_vals)};
    INSERT INTO evolution_events VALUES {', '.join(ev_vals)};
    INSERT INTO evolution_branch_summaries VALUES {', '.join(ebs_vals)};
    -- Seed 19 sensitivity evolution_runs sharing the mock tracking_run_key so evolution_runs_count >= 20
    INSERT INTO evolution_runs
    SELECT
        'sens_erid_' || lpad(i::VARCHAR, 2, '0') || repeat('0', 52),
        tracking_run_key,
        evolution_binding,
        weights,
        planned_batch_count,
        chain_summary,
        non_physical_disclaimer,
        content_sha256
    FROM evolution_runs, range(0, 19) r(i)
    WHERE evolution_run_id = {sql_value(evolution_run_id)};
    INSERT INTO evolution_nodes
    SELECT 'sens_erid_' || lpad(i::VARCHAR, 2, '0') || repeat('0', 52),
        ordinal, cluster_id, canonical_rank, branch_id, mean_features, member_count, member_fraction,
        assignment_reason, in_degree_primary, in_degree_secondary, out_degree_primary, out_degree_secondary,
        incoming_role, outgoing_role, primary_delta_fraction, has_primary_delta_fraction,
        unattributed_inflow_fraction, unattributed_outflow_fraction, high_uncertainty
    FROM evolution_nodes, range(0, 19) r(i)
    WHERE evolution_run_id = {sql_value(evolution_run_id)};
    INSERT INTO evolution_transitions
    SELECT 'sens_erid_' || lpad(i::VARCHAR, 2, '0') || repeat('0', 52),
        prev_ordinal, curr_ordinal, transition_status, chain_broken, gap_reason, pair_scale, threshold,
        primary_edge_count, secondary_split_edge_count, secondary_merge_edge_count,
        transition_tv_redistribution, min_threshold_margin, ambiguous_tie, high_uncertainty, evolve_result
    FROM evolution_transitions, range(0, 19) r(i)
    WHERE evolution_run_id = {sql_value(evolution_run_id)};
    INSERT INTO evolution_edges
    SELECT 'sens_erid_' || lpad(i::VARCHAR, 2, '0') || repeat('0', 52),
        prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, prev_branch_id, curr_branch_id,
        edge_role, event_type, rule_basis, distance, distance_over_scale, prev_member_fraction,
        curr_member_fraction, edge_delta_fraction, attributed_flow, edge_strength, threshold_margin,
        threshold_margin_ratio, primary_alternative_status, has_optimality_margin, optimality_margin,
        optimality_margin_ratio, has_secondary_margin, secondary_margin, secondary_margin_ratio,
        ambiguous_tie, high_uncertainty, uncertainty_reasons, non_physical_disclaimer
    FROM evolution_edges, range(0, 19) r(i)
    WHERE evolution_run_id = {sql_value(evolution_run_id)};
    INSERT INTO evolution_events
    SELECT 'sens_erid_' || lpad(i::VARCHAR, 2, '0') || repeat('0', 52),
        prev_ordinal, event_index, event_type, prev_cluster_ids, curr_cluster_ids, branch_ids,
        primary_branch_id, rule_basis, max_morphology_distance, max_morphology_over_scale,
        prev_fraction_sum, curr_fraction_sum, net_delta_fraction, attributed_flow_sum,
        has_min_margin_ratio, min_margin_ratio, ambiguous_tie, high_uncertainty, non_physical_disclaimer
    FROM evolution_events, range(0, 19) r(i)
    WHERE evolution_run_id = {sql_value(evolution_run_id)};
    INSERT INTO evolution_branch_summaries
    SELECT 'sens_erid_' || lpad(i::VARCHAR, 2, '0') || repeat('0', 52),
        branch_id, birth_ordinal, end_ordinal, node_count, step_count, birth_reason, termination_reason,
        cumulative_morphology_distance, cumulative_morphology_over_scale, max_step_morphology_distance,
        max_step_morphology_over_scale, end_to_end_morphology_distance, initial_member_fraction,
        final_member_fraction, min_member_fraction, max_member_fraction, net_delta_fraction,
        cumulative_abs_delta_fraction, split_out_event_count, spawned_secondary_branch_ids,
        split_from_parent_branch_id, merge_in_event_count, absorbed_secondary_branch_ids,
        merged_into_branch_id, ambiguous_step_count, high_uncertainty_step_count, high_uncertainty_step_ratio
    FROM evolution_branch_summaries, range(0, 19) r(i)
    WHERE evolution_run_id = {sql_value(evolution_run_id)};
    COMMIT;
    """
    proc = run_isolated_duckdb(sql, database=db_path, readonly=False)
    if proc.returncode != 0:
        raise RuntimeError(f"Failed seeding mock main 8-run into DuckDB: {proc.stderr}")

    def _replay_doc_for_key(erid: str, trk: str) -> dict[str, Any]:
        c_run = solve_chain_oracle(
            {
                "tracking_run_key": trk,
                "threshold_coefficient": 1.0,
                "weights": [0.5, 0.5],
                "config": {k: v for k, v in ev_cfg.items() if k != "uncertainty_rule"},
                "batches": oracle_batches,
            }
        )
        return {
            "schema_version": "m3.real_evolution_replay.v1",
            "status": "pass",
            "evolution_run_id": erid,
            "tracking_run_key": trk,
            "content_sha256": "abab" * 16,
            "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
            "invariants_e1_to_e8_passed": True,
            "preparation_negative_gate_rejected_with_zero_residual": True,
            "high_uncertainty_transition_prev_ordinals": [0, 3, 6],
            "runner_parity": {
                "exact_parity_with_m3_runner": True,
                "exact_parity_with_chain_oracle": True,
            },
            "chain_summary": c_run["chain_summary"],
            "batches": [
                {
                    "ordinal": b[0],
                    "init_time_utc": b[1],
                    "lead_hours": b[2],
                    "status": "ok",
                    "status_reason": None,
                    "dispersion": 1.0,
                }
                for b in batch_specs
            ],
            "nodes": c_run["nodes"],
            "transitions": [
                {k: v for k, v in tr.items() if k != "evolve_result"} for tr in c_run["transitions"]
            ],
            "edges": c_run["edges"],
            "events": c_run["events"],
            "branches": c_run["branch_summaries"],
        }

    return {
        "parent_replay": _replay_doc_for_key(evolution_run_id, tracking_run_key),
        "child_replay": _replay_doc_for_key("child_" + evolution_run_id[6:], "childrk9876543210"),
    }


def _build_mock_passing_bundle(run_dir: Path) -> None:
    """Synthesize a self-consistent passing M3 run directory in `/tmp/` for negative audit tests."""
    run_dir.mkdir(parents=True, exist_ok=True)
    db_path = run_dir / "sql-history" / "evolution-runs.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # 1. Seed synthetic showcase + mock main 8-run into 11-table DuckDB
    seed_showcase_duckdb(db_path, evolution_run_id=SHOWCASE_RUN_ID, tracking_run_key=SHOWCASE_TRACKING_KEY)
    real_erid = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    real_trk = "parentrk1234567890"
    replays = _seed_mock_main_8run_into_duckdb(db_path, evolution_run_id=real_erid, tracking_run_key=real_trk)

    # 2. Render both HTML views and run live traceability audits
    viz_dir = run_dir / "visualization"
    viz_dir.mkdir(parents=True, exist_ok=True)
    real_html = viz_dir / "real-8run-main.html"
    synth_html = viz_dir / "synthetic-showcase.html"
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        assert render_html_main(["--db", str(db_path), "--evolution-run-id", real_erid, "--output", str(real_html)]) == 0
        assert (
            render_html_main(["--db", str(db_path), "--evolution-run-id", SHOWCASE_RUN_ID, "--output", str(synth_html)])
            == 0
        )

    real_v_audit = verify_html_traceability(real_html, db_path, real_erid)
    synth_v_audit = verify_html_traceability(synth_html, db_path, SHOWCASE_RUN_ID)
    atomic_write_json(
        viz_dir / "traceability-audit.json",
        {
            "schema_version": "m3.visualization-audit.v1",
            "status": "pass",
            "views": {"real_8run_main": real_v_audit, "synthetic_showcase": synth_v_audit},
            "findings": [],
        },
    )
    atomic_write_json(
        viz_dir / "tamper-self-check.json",
        {
            "schema_version": "m3.tamper-self-check.v1",
            "status": "pass",
            "tamper_cases_tested": 10,
            "tamper_cases_rejected": 10,
            "incomplete_provenance_render_rejected": True,
        },
    )

    # 0. Write environment.json, source-manifest.json (mock_bundle=True in /tmp), and build/build_manifest.json + binaries
    atomic_write_json(
        run_dir / "environment.json",
        {
            "schema_version": "m3.environment.v1",
            "status": "pass",
            "failures": [],
            "platform": "aarch64",
            "duckdb_cli": "v1.5.5",
        },
    )
    mock_src_manifest = {
        "schema_version": "m3.source_manifest.v1",
        "mock_bundle": True,
        "head_commit": "0" * 40,
        "tracked_and_effective_count": 10,
        "code_sources_count": 8,
        "dirty_or_untracked_count": 0,
        "dirty_or_untracked": [],
        "sources": {"Cargo.toml": "1" * 64},
        "manifest_sha256": "2" * 64,
        "code_manifest_sha256": "3" * 64,
    }
    atomic_write_json(run_dir / "source-manifest.json", mock_src_manifest)
    atomic_write_json(
        run_dir / "reproduction" / "effective-source-manifest.json",
        mock_src_manifest,
    )

    build_dir = run_dir / "build"
    build_dir.mkdir(parents=True, exist_ok=True)
    binaries_manifest: dict[str, dict[str, str]] = {}
    for bin_key, _, bin_fname in REQUIRED_BUILD_BINARIES:
        bp = build_dir / bin_fname
        atomic_write(bp, f"mock-binary:{bin_fname}\n".encode("utf-8"))
        binaries_manifest[bin_key] = {"path": str(bp), "sha256": sha256_file(bp)}
    atomic_write_json(
        build_dir / "build_manifest.json",
        {
            "schema_version": "m3.build_manifest.v1",
            "extension_name": "ensabc_m0_extension",
            "extension_version": "0.1.0",
            "duckdb_version": "v1.5.5",
            "rust_source_files_count": 23,
            "rust_source_sha256": "4" * 64,
            "binaries": binaries_manifest,
        },
    )

    # 3. Core, OM, SQL history, Case-study, Runtime, Measurements, Regressions, Reproduction JSONs
    atomic_write_json(
        run_dir / "core" / "synthetic-oracle.json",
        {
            "status": "pass",
            "uncertainty_rule": "m3.uncertainty.v1",
            "ulp_tolerance": 0,
            "fixed_pair_cases_count": 25,
            "fixed_pair_mismatches": 0,
            "chain_cases_count": 4,
            "chain_mismatches": 0,
            "chain_gap_disjointness_verified": True,
            "chain_results": [
                {
                    "case_id": f"chain_{bc}batch",
                    "planned_batch_count": bc,
                    "status": "pass",
                    "zero_ulp_verified": True,
                }
                for bc in (4, 5, 6, 8)
            ],
            "berge_theorems_verified": {
                "theorem1_no_unmatched_cross_feasible_and_no_parallel_edges": True,
                "theorem2_split_merge_mutually_exclusive_on_primary_link": True,
                "theorem3_secondary_forced_inclusion_preserves_max_cardinality": True,
                "theorem4_inactive_feasible_cross_edge_invariance": True,
                "corollary_five_class_star_partition_complete": True,
            },
            "fixed_pair_results": [
                {
                    "case_id": f"case_{i:02d}",
                    "status": "pass",
                    "summary": {
                        "primary_edge_count": 2,
                        "continuation_count": 1,
                        "split_count": 1,
                        "merge_count": 0,
                    },
                }
                for i in range(25)
            ],
            "random_oracle_stats": {"cases_tested": 4000, "mismatches": 0},
        },
    )
    atomic_write_json(
        run_dir / "core" / "orthogonality-separation.json",
        {
            "status": "pass",
            "pure_morphology_drift_series": {
                "primary_max_distances": [0.1, 0.25, 0.5, 0.85],
                "transition_tv_redistribution_values": [0.0, 0.0, 0.0, 0.0],
                "fraction_deltas_bitwise_zero": True,
                "transition_tv_bitwise_zero": True,
                "strictly_monotonic_distance": True,
            },
            "pure_fraction_redistribution_series": {
                "primary_total_distances": [0.0, 0.0, 0.0],
                "all_edges_max_distances": [0.0, 0.0, 0.0],
                "expected_tv_values": [0.0, 0.25, 0.375],
                "observed_tv_values": [0.0, 0.25, 0.375],
                "morphology_distances_bitwise_zero": True,
                "tv_values_bitwise_exact": True,
            },
            "uncertainty_channel_bracketing": {
                "all_passed": True,
                "channels_verified": ["near_threshold", "near_primary_switch", "near_secondary_switch"],
            },
        },
    )
    atomic_write_json(
        run_dir / "core" / "permutation-invariance.json",
        {
            "status": "pass",
            "permutation_stats": {"permutations_tested": 2000, "mismatches": 0},
            "exhaustive_3x4_144_permutations": {"permutations_tested": 144, "mismatches": 0},
            "symmetric_tie_canonical_verification": {
                "ambiguous_tie_asserted": True,
                "oracle_canonical_tiebreak_matched": True,
            },
        },
    )
    atomic_write_json(
        run_dir / "core" / "error-cases.json",
        {
            "status": "pass",
            "distinct_error_codes_count": 17,
            "error_cases_count": 20,
            "single_item_exit_1_zero_output_residue_verified": True,
        },
    )

    atomic_write_json(
        run_dir / "om-verification" / "cell-matrix-manifest.json",
        {"status": "PASS", "total_cells_checked": 3600},
    )
    atomic_write_json(
        run_dir / "om-verification" / "om-cells-report.json",
        {"status": "PASS", "sc001_satisfied": True},
    )
    atomic_write_json(
        run_dir / "om-verification" / "test-om-sample-report.json",
        {"status": "PASS", "negative_cases_tested": 5},
    )
    atomic_write_json(
        run_dir / "om-verification" / "duckomo-version-comparison.json",
        {"status": "pass", "exact_semantic_equivalence": True, "authentic_branch_id_prefixes_verified": True},
    )

    atomic_write_json(
        run_dir / "sql-history" / "sql-contract.json",
        {
            "status": "pass",
            "preserved_udfs": {
                "ens_m0_echo": True,
                "ens_cluster": True,
                "ens_dispersion": True,
                "ens_match_scenarios": True,
            },
            "ens_fsum_contract": {"math_fsum_zero_ulp_parity": True},
            "ens_evolve_scenarios_contract": {"oracle_pair_zero_ulp_parity": True},
            "large_chunk_boundary": {"rows_tested": 2100, "late_chunk_bad_row_aborts_whole_query": True},
            "same_session_recovery": {"recovered_cleanly": True},
        },
    )
    atomic_write_json(run_dir / "sql-history" / "real-evolution-replay.json", replays["parent_replay"])
    atomic_write_json(
        run_dir / "sql-history" / "synthetic-evolution-replay.json",
        {
            "status": "pass",
            "evolution_run_id": SHOWCASE_RUN_ID,
            "showcase_evolution_run_id": SHOWCASE_RUN_ID,
            "showcase_tracking_run_key": SHOWCASE_TRACKING_KEY,
            "cases_verified": 5,
            "raw_batch_whitelist_isolation_verified": True,
        },
    )
    atomic_write_json(
        run_dir / "sql-history" / "three-process-repro.json",
        {
            "status": "pass",
            "processes_tested": 3,
            "distinct_pids": [1001, 1002, 1003],
            "synthetic_replay": {"exact_match_across_3_processes": True},
            "real_replay": {"exact_match_across_3_processes": True},
        },
    )
    atomic_write_json(
        run_dir / "sql-history" / "transaction-atomicity.json",
        {
            "status": "pass",
            "idempotent_republish": {"identical_after_republish": True},
            "version_conflict_rejection": {
                "error_code": "EVOLUTION_RUN_VERSION_CONFLICT",
                "rejected": True,
            },
            "multi_version_coexistence": {"both_preserved_without_mutation": True},
            "fault_injection_rollback": {
                "hooks_tested": 7,
                "zero_residual_across_all_11_tables": True,
            },
        },
    )

    atomic_write_json(
        run_dir / "case-study" / "main-evolution-k3-c1.0.json",
        {
            "status": "PASS",
            "evolution_run_id": real_erid,
            "tracking_run_key": real_trk,
            "planned_batch_count": 8,
            "node_count": 24,
            "branch_count": 8,
            "primary_edge_count": 16,
            "secondary_split_edge_count": 2,
            "secondary_merge_edge_count": 1,
            "continuation_count": 13,
            "split_count": 2,
            "merge_count": 1,
            "emergence_count": 3,
            "disappearance_count": 4,
            "high_uncertainty_transitions": [0, 3, 6],
            "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        },
    )
    atomic_write_json(
        run_dir / "case-study" / "sensitivity-thresholds.json",
        {
            "status": "PASS",
            "all_persisted_in_duckdb": True,
            "grid_count": 18,
            "runs": [{"idx": i} for i in range(18)],
        },
    )
    atomic_write_json(
        run_dir / "case-study" / "sensitivity-clustering-configs.json",
        {
            "status": "PASS",
            "all_persisted_in_duckdb": True,
            "failed_batch_runs_count": 2,
            "axes": {"a1": [], "a2": [], "a3": [], "a4": []},
        },
    )
    atomic_write_json(
        run_dir / "case-study" / "perturbation-evolution-level.json",
        {
            "status": "PASS",
            "all_flips_carry_baseline_margins": True,
            "boundary_flips": [],
            "level_1a_mean_field": {
                "all_flips_carry_baseline_margins": True,
                "boundary_flips": [],
                "certified_pair_flips": 0,
                "certified_evaluations_count": 1200,
            },
            "level_1b_fraction_only": {"topology_flips": 0},
        },
    )
    atomic_write_json(
        run_dir / "case-study" / "perturbation-member-level.json",
        {
            "status": "PASS",
            "all_flips_carry_baseline_margins": True,
            "boundary_flips": [
                {
                    "pair_index": 6,
                    "certified": False,
                    "baseline_margins": {
                        "high_uncertainty": True,
                        "min_threshold_margin": 0.012,
                        "min_optimality_margin": 0.011,
                    },
                }
            ],
            "sigma_zero_exact_match": True,
            "certified_pair_flips": 0,
            "certified_evaluations_count": 160,
        },
    )
    atomic_write_json(
        run_dir / "case-study" / "limitations.json",
        {
            "status": "PASS",
            "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
            "clauses": [
                "Algorithmic attribution only; does NOT represent physical ensemble member migration.",
                "Does NOT represent physical cyclogenesis or lysis.",
                "Uncalibrated sample proportion; NOT a calibrated probability or trading strategy.",
            ],
        },
    )

    atomic_write_json(
        run_dir / "minimal-runtime.json",
        {
            "status": "PASS",
            "network_mode": "none",
            "network_zero_routes_verified": True,
            "all_prohibited_tools_absent": True,
            "zero_residual_after_rollback": True,
            "standalone_runner_parity": True,
            "sql_11table_replay_parity": True,
            "same_session_error_recovery": True,
        },
    )
    atomic_write_json(
        run_dir / "measurements.json",
        {
            "status": "PASS",
            "repeats": 3,
            "stages": [
                {
                    "stage_index": i + 1,
                    "repeats": 3,
                    "mean_wall_seconds": 0.05 * (i + 1),
                    "peak_rss_kib": 32000 + i * 1024,
                    "runs": [
                        {"repeat": r + 1, "wall_seconds": 0.05 * (i + 1), "maxrss_kib": 32000 + i * 1024}
                        for r in range(3)
                    ],
                }
                for i in range(7)
            ],
        },
    )

    m1_dir = run_dir / "m1-regression"
    atomic_write_json(m1_dir / "report.json", {"overall": "passed"})
    atomic_write_json(m1_dir / "traceability.json", {"status": "pass"})
    atomic_write_json(
        m1_dir / "report-audit.json",
        {
            "status": "pass",
            "derived_overall": "passed",
            "findings": [],
            "report_sha256": sha256_file(m1_dir / "report.json"),
        },
    )
    atomic_write_json(
        m1_dir / "summary.json",
        {
            "status": "PASS",
            "overall_status": "passed",
            "traceability_status": "pass",
            "audit_status": "passed",
            "findings": [],
            "report_sha256": sha256_file(m1_dir / "report.json"),
            "traceability_sha256": sha256_file(m1_dir / "traceability.json"),
            "report_audit_sha256": sha256_file(m1_dir / "report-audit.json"),
        },
    )

    m2_dir = run_dir / "m2-regression"
    atomic_write_json(m2_dir / "report.json", {"overall_status": "passed"})
    atomic_write_json(m2_dir / "traceability.json", {"status": "pass"})
    atomic_write_json(
        m2_dir / "report-audit.json",
        {
            "audit_status": "passed",
            "findings": [],
            "report_sha256": sha256_file(m2_dir / "report.json"),
        },
    )
    atomic_write_json(
        m2_dir / "summary.json",
        {
            "status": "PASS",
            "overall_status": "passed",
            "traceability_status": "pass",
            "audit_status": "passed",
            "findings": [],
            "report_sha256": sha256_file(m2_dir / "report.json"),
            "traceability_sha256": sha256_file(m2_dir / "traceability.json"),
            "report_audit_sha256": sha256_file(m2_dir / "report-audit.json"),
        },
    )
    atomic_write_json(
        run_dir / "reproduction" / "child" / "sql-history" / "real-evolution-replay.json",
        replays["child_replay"],
    )
    atomic_write_json(
        run_dir / "independent-reproduction.json",
        {"status": "PASS", "equivalence": True, "authentic_branch_id_prefixes_verified": True},
    )

    # 4. Write execution-steps.json, run genuine TraceabilityEvaluator, and write report.json
    atomic_write_json(
        run_dir / "execution-steps.json",
        {
            "steps": [
                {
                    "step": step_name,
                    "command": ["mock", step_name],
                    "exit_code": 0,
                    "elapsed_seconds": 0.1,
                    "status": "passed",
                }
                for step_name in REQUIRED_STEPS
            ]
        },
    )
    trace_doc = evaluate_traceability(run_dir)
    assert trace_doc["status"] == "pass", f"Mock passing bundle failed traceability: {trace_doc}"
    atomic_write_json(run_dir / "report.json", compose_report_dict(run_dir))


def run_self_check(*, out_report: Path | None = None) -> dict[str, Any]:
    """Synthesize a mock passing bundle in `/tmp/` and verify baseline + 10 negative audit cases."""
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="ensabc-m3-audit-selfcheck-") as tmp_str:
        outer = Path(tmp_str)
        outside_secret = outer / "outside_secret.json"
        outside_secret.write_text('{"secret": true}\n', encoding="utf-8")
        outside_sha = sha256_file(outside_secret)

        base_dir = outer / "baseline_run"
        _build_mock_passing_bundle(base_dir)

        # Verify positive baseline passes cleanly and leaves report.json & traceability.json untouched
        rep_sha_before = sha256_file(base_dir / "report.json")
        tr_sha_before = sha256_file(base_dir / "traceability.json")
        base_audit = audit_report(base_dir)
        if base_audit["audit_status"] != "passed" or base_audit["findings"] != []:
            raise AssertionError(f"Mock baseline run failed audit: {base_audit}")
        if sha256_file(base_dir / "report.json") != rep_sha_before:
            raise AssertionError("verify_report mutated report.json!")
        if sha256_file(base_dir / "traceability.json") != tr_sha_before:
            raise AssertionError("verify_report mutated traceability.json!")

        def _clone_case(name: str) -> Path:
            dest = outer / name
            shutil.copytree(base_dir, dest)
            (dest / "report-audit.json").unlink(missing_ok=True)
            return dest

        negative_specs: list[tuple[str, str, Any]] = []

        # Case 1: Missing required step (`m2_regression` removed)
        def _mut_missing_step(d: Path) -> None:
            steps_doc = load_json(d / "execution-steps.json")
            steps_doc["steps"] = [s for s in steps_doc["steps"] if s["step"] != "m2_regression"]
            atomic_write_json(d / "execution-steps.json", steps_doc)
            rep = load_json(d / "report.json")
            rep["steps"] = steps_doc["steps"]
            atomic_write_json(d / "report.json", rep)

        negative_specs.append(("neg_01_missing_required_step", "Missing required step", _mut_missing_step))

        # Case 2: Non-zero step exit code (`verify_minimal_runtime` exit_code=1)
        def _mut_nonzero_exit(d: Path) -> None:
            steps_doc = load_json(d / "execution-steps.json")
            for s in steps_doc["steps"]:
                if s["step"] == "verify_minimal_runtime":
                    s["exit_code"] = 1
                    s["status"] = "failed"
            atomic_write_json(d / "execution-steps.json", steps_doc)

        negative_specs.append(("neg_02_nonzero_step_exit_code", "verify_minimal_runtime", _mut_nonzero_exit))

        # Case 3: `../` path traversal in evidence_refs
        def _mut_path_traversal(d: Path) -> None:
            rep = load_json(d / "report.json")
            rep["requirements"]["FR-017"]["evidence_refs"] = [
                {"path": "../outside_secret.json", "sha256": outside_sha}
            ]
            atomic_write_json(d / "report.json", rep)

        negative_specs.append(("neg_03_parent_path_traversal", "traversal", _mut_path_traversal))

        # Case 4: Tampered evidence file SHA-256 mismatch
        def _mut_tampered_evidence_sha(d: Path) -> None:
            meas = load_json(d / "measurements.json")
            meas["tampered_note"] = "post-report edit"
            atomic_write_json(d / "measurements.json", meas)

        negative_specs.append(("neg_04_tampered_evidence_sha256", "SHA-256 mismatch", _mut_tampered_evidence_sha))

        # Case 5: `not_run` FR disguised as `overall_status: "passed"`
        def _mut_not_run_disguised_passed(d: Path) -> None:
            (d / "minimal-runtime.json").unlink()
            evaluate_traceability(d)
            tr = load_json(d / "traceability.json")
            assert tr["requirements"]["FR-017"]["status"] == "not_run"
            rep = compose_report_dict(d)
            rep["overall_status"] = "passed"  # Fraudulently claim passed!
            atomic_write_json(d / "report.json", rep)

        negative_specs.append(
            ("neg_05_not_run_fr_disguised_as_passed", "FR-017", _mut_not_run_disguised_passed)
        )

        # Case 6: Forged `branch_id` prefix in child reproduction (even with updated SHA-256s!)
        def _mut_forged_repro_prefix(d: Path) -> None:
            child_p = d / "reproduction" / "child" / "sql-history" / "real-evolution-replay.json"
            child_rep = load_json(child_p)
            child_rep["nodes"][0]["branch_id"] = "forged000000:b00:k00"
            atomic_write_json(child_p, child_rep)
            new_sha = sha256_file(child_p)
            for fname in ("traceability.json", "report.json"):
                doc = load_json(d / fname)
                for sec in ("requirements", "success_criteria"):
                    for entry in doc[sec].values():
                        for ref in entry["evidence_refs"]:
                            if ref["path"] == "reproduction/child/sql-history/real-evolution-replay.json":
                                ref["sha256"] = new_sha
                atomic_write_json(d / fname, doc)
            rep = load_json(d / "report.json")
            rep["traceability_sha256"] = sha256_file(d / "traceability.json")
            atomic_write_json(d / "report.json", rep)

        negative_specs.append(("neg_06_forged_branch_id_prefix_in_repro", "branch_id", _mut_forged_repro_prefix))

        # Case 7: Tampered HTML after traceability (even with updated SHA-256 in evidence_refs!)
        def _mut_tampered_html_post_trace(d: Path) -> None:
            html_p = d / "visualization" / "real-8run-main.html"
            txt = html_p.read_text(encoding="utf-8")
            html_p.write_text(
                txt.replace("</svg>", '\n  <path d="M 5 5 L 40 40" stroke="#f00" />\n</svg>', 1),
                encoding="utf-8",
            )
            new_sha = sha256_file(html_p)
            for fname in ("traceability.json", "report.json"):
                doc = load_json(d / fname)
                for sec in ("requirements", "success_criteria"):
                    for entry in doc[sec].values():
                        for ref in entry["evidence_refs"]:
                            if ref["path"] == "visualization/real-8run-main.html":
                                ref["sha256"] = new_sha
                atomic_write_json(d / fname, doc)
            rep = load_json(d / "report.json")
            rep["traceability_sha256"] = sha256_file(d / "traceability.json")
            atomic_write_json(d / "report.json", rep)

        negative_specs.append(("neg_07_tampered_html_after_traceability", "orphan", _mut_tampered_html_post_trace))

        # Case 8: Prohibited calibration & physical member migration claim (even with updated SHA-256!)
        def _mut_prohibited_overclaim(d: Path) -> None:
            lim_p = d / "case-study" / "limitations.json"
            lim = load_json(lim_p)
            lim["clauses"].append(
                "This model provides calibrated forecast probabilities and represents physical ensemble member migration."
            )
            atomic_write_json(lim_p, lim)
            new_sha = sha256_file(lim_p)
            for fname in ("traceability.json", "report.json"):
                doc = load_json(d / fname)
                for sec in ("requirements", "success_criteria"):
                    for entry in doc[sec].values():
                        for ref in entry["evidence_refs"]:
                            if ref["path"] == "case-study/limitations.json":
                                ref["sha256"] = new_sha
                atomic_write_json(d / fname, doc)
            rep = load_json(d / "report.json")
            rep["traceability_sha256"] = sha256_file(d / "traceability.json")
            atomic_write_json(d / "report.json", rep)

        negative_specs.append(
            ("neg_08_prohibited_calibration_migration_claim", "claim", _mut_prohibited_overclaim)
        )

        # Case 9: Circular self-reference to `report.json` in `evidence_refs`
        def _mut_circular_self_ref(d: Path) -> None:
            rep = load_json(d / "report.json")
            rep["success_criteria"]["SC-008"]["evidence_refs"].append(
                {"path": "report.json", "sha256": sha256_file(d / "report.json")}
            )
            atomic_write_json(d / "report.json", rep)

        negative_specs.append(("neg_09_circular_report_self_reference", "circular", _mut_circular_self_ref))

        # Case 10: `m2_regression` executed before `m1_regression`
        def _mut_regression_order_violation(d: Path) -> None:
            steps_doc = load_json(d / "execution-steps.json")
            steps = steps_doc["steps"]
            i1 = next(i for i, s in enumerate(steps) if s["step"] == "m1_regression")
            i2 = next(i for i, s in enumerate(steps) if s["step"] == "m2_regression")
            steps[i1], steps[i2] = steps[i2], steps[i1]
            atomic_write_json(d / "execution-steps.json", steps_doc)
            rep = load_json(d / "report.json")
            rep["steps"] = steps
            atomic_write_json(d / "report.json", rep)

        negative_specs.append(
            ("neg_10_m2_before_m1_regression_order", "m1_regression", _mut_regression_order_violation)
        )

        # Case 11: [H4] Tampered source-manifest.json after report.json
        def _mut_tampered_source_manifest(d: Path) -> None:
            sm = load_json(d / "source-manifest.json")
            sm["code_manifest_sha256"] = "f" * 64
            atomic_write_json(d / "source-manifest.json", sm)

        negative_specs.append(
            ("neg_11_tampered_source_manifest", "code_manifest_sha256", _mut_tampered_source_manifest)
        )

        # Case 12: [H4] Tampered build_manifest.json & binary after report.json
        def _mut_tampered_build_manifest_or_binary(d: Path) -> None:
            atomic_write(d / "build" / "m3_runner", b"corrupted-binary-payload\n")
            bm = load_json(d / "build" / "build_manifest.json")
            bm["toolchain"] = "tampered-rustc"
            atomic_write_json(d / "build" / "build_manifest.json", bm)

        negative_specs.append(
            ("neg_12_tampered_build_manifest_and_binary", "build_manifest", _mut_tampered_build_manifest_or_binary)
        )

        # Case 13: [H4] Tampered traceability.json after report.json
        def _mut_tampered_traceability_post_report(d: Path) -> None:
            tr = load_json(d / "traceability.json")
            tr["evaluated_at_utc"] = "2099-01-01T00:00:00Z"
            atomic_write_json(d / "traceability.json", tr)

        negative_specs.append(
            ("neg_13_tampered_traceability_after_report", "traceability_sha256", _mut_tampered_traceability_post_report)
        )

        # Case 14: [H4] Live source code drift when mock_bundle is False
        def _mut_source_manifest_drift(d: Path) -> None:
            sm = load_json(d / "source-manifest.json")
            sm.pop("mock_bundle", None)
            atomic_write_json(d / "source-manifest.json", sm)
            rep = load_json(d / "report.json")
            rep["source_manifest_sha256"] = sm["manifest_sha256"]
            rep["code_manifest_sha256"] = sm["code_manifest_sha256"]
            atomic_write_json(d / "report.json", rep)

        negative_specs.append(
            ("neg_14_source_manifest_drift", "SOURCE_MANIFEST_DRIFT", _mut_source_manifest_drift)
        )

        # Case 15: [M7] Symlink alias inside run_dir evidence_refs
        def _mut_symlink_alias_in_run_dir(d: Path) -> None:
            real_mr = d / "minimal-runtime.json"
            moved_mr = d / "minimal-runtime-real.json"
            real_mr.rename(moved_mr)
            real_mr.symlink_to(moved_mr.name)

        negative_specs.append(
            ("neg_15_symlink_evidence_file_blocked", "symlink", _mut_symlink_alias_in_run_dir)
        )

        case_results: list[dict[str, Any]] = []
        for case_name, expected_substr, mut_fn in negative_specs:
            case_dir = _clone_case(case_name)
            mut_fn(case_dir)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                rc = main(["--run-dir", str(case_dir)])
            audit = load_json(case_dir / "report-audit.json")
            joined = "\n".join(audit["findings"]).lower()
            caught = (
                rc == 1
                and audit["audit_status"] == "failed"
                and len(audit["findings"]) > 0
                and expected_substr.lower() in joined
            )
            if not caught:
                raise AssertionError(
                    f"Negative audit case {case_name} was NOT caught as expected: rc={rc}, audit={audit}"
                )
            case_results.append(
                {
                    "case": case_name,
                    "exit_code": rc,
                    "audit_status": audit["audit_status"],
                    "findings_count": len(audit["findings"]),
                    "sample_finding": audit["findings"][0],
                    "caught": True,
                }
            )

        # Case 16: [M7] Symlink run_dir itself rejected before .resolve() without writing through symlink
        sym_run = outer / "neg_16_symlink_run_dir"
        sym_run.symlink_to(base_dir)
        (base_dir / "report-audit.json").unlink(missing_ok=True)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rc_sym = main(["--run-dir", str(sym_run)])
        if rc_sym != 1 or (base_dir / "report-audit.json").exists():
            raise AssertionError(f"Symlink run_dir was not rejected cleanly: rc={rc_sym}")
        case_results.append(
            {
                "case": "neg_16_symlink_run_dir",
                "exit_code": rc_sym,
                "audit_status": "failed",
                "findings_count": 1,
                "sample_finding": "Symlink run_dir is forbidden",
                "caught": True,
            }
        )

    summary = {
        "schema_version": SELF_CHECK_SCHEMA_VERSION,
        "status": "PASS",
        "checked_at_utc": _utc_now(),
        "baseline_mock_bundle_passed": True,
        "readonly_on_report_and_traceability_verified": True,
        "negative_cases_tested": len(case_results),
        "negative_cases_caught": sum(1 for c in case_results if c["caught"]),
        "cases": case_results,
    }
    if out_report is not None:
        atomic_write_json(out_report, summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Anti-circular read-only report auditor and self-check for M3 gate."
    )
    parser.add_argument("--run-dir", type=Path, default=None, help="Target M3 run directory")
    parser.add_argument("--self-check", action="store_true", help="Run 10-case negative auditor self-check")
    parser.add_argument("--out-report", type=Path, default=None, help="Optional JSON output path for --self-check")
    args = parser.parse_args(argv)

    if args.self_check:
        try:
            summary = run_self_check(out_report=args.out_report)
            print(
                f"Report auditor self-check: {summary['status']} "
                f"(baseline=PASS, {summary['negative_cases_caught']}/{summary['negative_cases_tested']} "
                f"negative audit cases intercepted)"
            )
            return 0
        except Exception as exc:
            print(f"FAIL: Report auditor self-check failed: {exc}", file=sys.stderr)
            return 1

    if args.run_dir is None:
        print("Error: --run-dir is required unless --self-check is passed", file=sys.stderr)
        return 2

    if args.run_dir.is_symlink():
        print(f"Error: symlink --run-dir is forbidden: {args.run_dir}", file=sys.stderr)
        return 1

    run_dir = args.run_dir.resolve()
    if not run_dir.is_dir():
        print(f"Error: --run-dir does not exist: {run_dir}", file=sys.stderr)
        return 2

    audit_res = audit_report(run_dir, write_audit=True)
    print(
        f"Report audit: {audit_res['audit_status'].upper()} "
        f"({len(audit_res['findings'])} findings, {audit_res['audited_evidence_refs_count']} evidence refs verified)"
    )
    for finding in audit_res["findings"][:10]:
        print(f"  - {finding}", file=sys.stderr)
    return 0 if (audit_res["audit_status"] == "passed" and audit_res["findings"] == []) else 1


if __name__ == "__main__":
    sys.exit(main())
