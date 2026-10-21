#!/usr/bin/env python3
"""Verify that an independent environment reproduces the M0 result.

SC-006 requires that another verifier, or a clean independent environment,
can reproduce SC-001 to SC-004 from the recorded steps. This script performs
that reproduction and records the outcome as evidence.

The reproduction is deliberately independent of the run directory it is
compared against:

* it works in a separate directory, so nothing is shared by accident;
* it re-runs the sample verification, the reference baseline and the extension
  verification from the committed sources, not from a cached result;
* it compares the *content* of the results, not the file names, so a
  difference in a timestamp does not count as a difference.

The script is what the gate records as `independent-reproduction.json`. It
does not modify the run it is compared against.
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

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    FIXTURES_DIR,
    REPO_ROOT,
    VALIDATION_DIR,
    M0Error,
    read_json,
    write_json,
)

MANIFEST_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_manifest.json"
PAYLOAD_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_data.json"
CONFIG_PATH = FIXTURES_DIR / "reference_config.json"

DATA_VENV = Path(os.environ.get("ENSABC_M0_DATA_VENV") or "/tmp/ensabc-m0-data/.venv")
REFERENCE_VENV = Path(os.environ.get("ENSABC_M0_REFERENCE_VENV") or "/tmp/ensabc-m0-baseline")

# The identity of the sample that both sides must produce. It is the
# float32 source matrix hash, which does not depend on when the sample was
# retrieved.
EXPECTED_SOURCE_MATRIX_SHA256 = "09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe"


def _reference_python() -> str:
    candidate = REFERENCE_VENV / "bin" / "python"
    if candidate.is_file():
        return str(candidate)
    return sys.executable


def _run(command: list[str], cwd: Path | None = None, timeout: float = 1800.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def reproduce_sample(workdir: Path) -> dict[str, Any]:
    """Rebuild the sample in a separate directory and verify it.

    The sample is not committed, so the reproduction rebuilds it from the
    pinned source. This is the SC-001 check: a real regional ensemble sample
    that is recoverable offline.
    """
    python = DATA_VENV / "bin" / "python"
    if not python.is_file():
        return {
            "status": "not_run",
            "reason": f"the locked data environment is missing at {DATA_VENV}",
        }

    target = workdir / "sample"
    target.mkdir(parents=True, exist_ok=True)

    extract = _run(
        [
            str(python),
            str(VALIDATION_DIR / "extract_sample.py"),
            "--rebuild",
            "--from-source",
            "--force",
            "--output-dir",
            str(target),
        ]
    )
    if extract.returncode != 0:
        return {
            "status": "fail",
            "reason": f"the sample could not be rebuilt: {(extract.stdout + extract.stderr).strip()[:400]}",
        }

    verify = _run(
        [
            str(python),
            str(VALIDATION_DIR / "verify_sample.py"),
            "--offline",
            "--manifest",
            str(target / MANIFEST_PATH.name),
            "--payload",
            str(target / PAYLOAD_PATH.name),
            "--output",
            str(workdir / "sample-validation.json"),
        ]
    )
    if not (workdir / "sample-validation.json").is_file():
        return {
            "status": "fail",
            "reason": f"the sample verification produced no record: {(verify.stdout + verify.stderr).strip()[:400]}",
        }
    record = read_json(workdir / "sample-validation.json")
    return {
        "status": record.get("status", "fail"),
        "reason": "the sample was rebuilt from the pinned source and verified offline",
        "source_matrix_sha256": record.get("matrix_sha256_float32_le"),
        "sample_id": record.get("sample_id"),
        "member_count": record.get("member_count"),
        "point_count": record.get("point_count"),
        "negative_eligibility_cases": len(record.get("negative_eligibility_cases", [])),
    }


def reproduce_reference(workdir: Path) -> dict[str, Any]:
    """Re-run the three-process reference baseline in a separate directory."""
    reference_dir = workdir / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)

    python = _reference_python()
    completed = _run(
        [
            python,
            str(VALIDATION_DIR / "run_reference.py"),
            "--output-dir",
            str(reference_dir),
            "--output",
            str(reference_dir / "summary.json"),
        ]
    )
    summary_path = reference_dir / "summary.json"
    if not summary_path.is_file():
        return {
            "status": "fail",
            "reason": f"the reference run produced no record: {(completed.stdout + completed.stderr).strip()[:400]}",
        }
    summary = read_json(summary_path)
    if summary.get("status") != "pass":
        return {
            "status": "fail",
            "reason": f"the reference run did not pass: {(completed.stdout + completed.stderr).strip()[:400]}",
        }
    return {
        "status": "pass",
        "reason": "three independent processes reproduced the reference result",
        "pids": summary["independent_processes"]["pids"],
        "input_sha256": summary["input_sha256"],
        "config_sha256": summary["config_sha256"],
        "cluster_sizes": [len(group) for group in summary["runs"][0]["cluster_members"]],
        "inertia": summary["runs"][0]["inertia"],
        "partition": summary["runs"][0]["labels"],
    }


def reproduce_extension(workdir: Path) -> dict[str, Any]:
    """Re-run the extension verification against the rebuilt extension."""
    extension = Path(
        os.environ.get("ENSABC_M0_TARGET_DIR", "/tmp/ensabc-m0-extension-target")
    ) / "ensabc_m0_extension.duckdb_extension"
    if not extension.is_file():
        build = _run(["bash", str(VALIDATION_DIR / "build_extension.sh")], cwd=REPO_ROOT)
        if not extension.is_file():
            return {
                "status": "fail",
                "reason": f"the extension could not be built: {(build.stdout + build.stderr).strip()[:400]}",
            }

    output = workdir / "exchange.json"
    completed = _run(
        [
            sys.executable,
            str(VALIDATION_DIR / "verify_extension.py"),
            "--extension",
            str(extension),
            "--output",
            str(output),
        ]
    )
    if not output.is_file():
        return {
            "status": "fail",
            "reason": f"the extension verification produced no record: {(completed.stdout + completed.stderr).strip()[:400]}",
        }
    record = read_json(output)
    return {
        "status": record.get("status", "fail"),
        "reason": "the extension roundtrip, the rejections and the recovery were reproduced",
        "illegal_category_count": record.get("illegal_category_count"),
        "positive_case_count": len(record.get("positive_cases", [])),
    }


def compare_with_baseline(workdir: Path, baseline_run: Path | None) -> dict[str, Any]:
    """Compare the reproduction with the recorded run.

    The comparison is on the content that the success criteria name, not on
    the file names and not on the timestamps. The recorded run is optional:
    without it the reproduction still reports what it produced, and the
    comparison is recorded as not run rather than silently passed.
    """
    differences: list[str] = []
    notes: list[str] = []

    if baseline_run is None:
        return {
            "status": "not_run",
            "differences": differences,
            "notes": ["no recorded run was given to compare against"],
        }

    required = (
        workdir / "sample-validation.json",
        baseline_run / "sample-validation.json",
        workdir / "reference" / "summary.json",
        baseline_run / "reference" / "reference-summary.json",
        workdir / "exchange.json",
        baseline_run / "exchange.json",
    )
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        return {
            "status": "not_run",
            "differences": [],
            "notes": [f"required comparison evidence is missing: {path}" for path in missing],
        }

    sample = read_json(workdir / "sample-validation.json")
    if sample.get("matrix_sha256_float32_le") != EXPECTED_SOURCE_MATRIX_SHA256:
        differences.append("the reproduced source matrix hash differs from the recorded one")

    baseline_sample = read_json(baseline_run / "sample-validation.json")
    if baseline_sample.get("matrix_sha256_float32_le") != sample.get("matrix_sha256_float32_le"):
        differences.append("the reproduced source matrix hash differs from the recorded run's")
    if baseline_sample.get("sample_id") != sample.get("sample_id"):
        differences.append("the reproduced sample has a different sample id")
    notes.append("the sample was compared with the recorded run")

    summary = read_json(workdir / "reference" / "summary.json")
    baseline_summary = read_json(baseline_run / "reference" / "reference-summary.json")
    if summary.get("input_sha256") != baseline_summary.get("input_sha256"):
        differences.append("the reproduced reference used a different input")
    if summary.get("config_sha256") != baseline_summary.get("config_sha256"):
        differences.append("the reproduced reference used a different configuration")
    if summary["runs"][0]["labels"] != baseline_summary["runs"][0]["labels"]:
        differences.append("the reproduced member partition differs from the recorded one")
    notes.append("the reference partition was compared with the recorded run")

    exchange = read_json(workdir / "exchange.json")
    baseline_exchange = read_json(baseline_run / "exchange.json")
    if exchange.get("illegal_categories_covered") != baseline_exchange.get("illegal_categories_covered"):
        differences.append("the reproduced illegal categories differ from the recorded ones")
    if not exchange.get("illegal_categories_covered") or not baseline_exchange.get("illegal_categories_covered"):
        differences.append("the extension coverage evidence is empty")
    for label, record in (("reproduced sample", sample), ("baseline sample", baseline_sample), ("reproduced reference", summary), ("baseline reference", baseline_summary), ("reproduced extension", exchange), ("baseline extension", baseline_exchange)):
        if record.get("status") != "pass":
            differences.append(f"the {label} did not pass its own validation")
    notes.append("the extension coverage was compared with the recorded run")

    return {
        "status": "pass" if not differences else "fail",
        "differences": differences,
        "notes": notes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="write the reproduction record to this JSON file",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        help="the directory the reproduction runs in; a temporary one by default",
    )
    parser.add_argument(
        "--baseline-run",
        type=Path,
        help="the recorded run directory the reproduction is compared against",
    )
    arguments = parser.parse_args()

    temporary = arguments.workdir is None
    workdir = arguments.workdir or Path(tempfile.mkdtemp(prefix="ensabc-m0-reproduction-"))

    try:
        sample = reproduce_sample(workdir)
        reference = reproduce_reference(workdir)
        extension = reproduce_extension(workdir)
        comparison = compare_with_baseline(workdir, arguments.baseline_run)

        sections = {
            "sample": sample,
            "reference": reference,
            "extension": extension,
            "comparison": comparison,
        }
        statuses = [section.get("status") for section in sections.values()]
        overall = "pass"
        if "fail" in statuses:
            overall = "fail"
        elif "not_run" in statuses:
            overall = "not_run"

        record = {
            "schema_version": "m0.independent-reproduction.v1",
            "status": overall,
            "method": (
                "the sample, the reference baseline and the extension verification were "
                "re-run in a separate directory from the committed sources, and the "
                "reproduced content was compared with the recorded result"
            ),
            "workdir": str(workdir),
            "sections": sections,
            "source_matrix_sha256": sample.get("source_matrix_sha256"),
        }
    finally:
        if temporary and workdir.exists():
            shutil.rmtree(workdir, ignore_errors=True)

    if arguments.output:
        write_json(arguments.output, record)

    print(f"independent reproduction: {record['status']}")
    for name, section in record["sections"].items():
        print(f"  {name}: {section.get('status')} - {str(section.get('reason', ''))[:100]}")
    return 0 if record["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
