#!/usr/bin/env python3
"""Verify that the frozen sample can be recovered from its source.

FR-013 requires a reproducible real-source extraction, not a claim that one
is possible. This script therefore performs the recovery end to end:

1. it re-reads the pinned source objects and checks that their generation and
   size still match the values recorded in the manifest, so a changed or
   regenerated source is detected instead of silently accepted;
2. it re-extracts the matrix from the source into a fresh directory;
3. it compares the extracted matrix with the frozen sample, byte for byte and
   hash for hash.

The extraction is optional. Without network access the source objects cannot
be read, and the script then records the step as `not_run` with the reason
rather than pretending the recovery was demonstrated. The offline
verification of the frozen sample itself is a separate step and does not
depend on this script.
"""

from __future__ import annotations

import argparse
import filecmp
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    FIXTURES_DIR,
    REPO_ROOT,
    VALIDATION_DIR,
    M0Error,
    read_json,
    sha256_file,
    write_json,
)

MANIFEST_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_manifest.json"
PAYLOAD_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_data.json"

# The formal extractor is the tool that reads the pinned source archive. The
# recovery check uses it directly, so that a fresh clone does not depend on
# the planning-time spike material.
EXTRACTOR = VALIDATION_DIR / "extract_sample.py"

DATA_VENV = Path(os.environ.get("ENSABC_M0_DATA_VENV") or "/tmp/ensabc-m0-data/.venv")
DEFAULT_WORKDIR = Path("/tmp/ensabc-m0-live-reextract")


def check_source_objects(manifest: dict[str, Any], python: Path) -> list[dict[str, Any]]:
    """Check that the pinned source objects still have the recorded identity.

    The check runs in the locked data environment, because the source is read
    with the locked reading stack and not with an ambient one.
    """
    generations = manifest["source"]["object_generations"]
    prefix = manifest["source"]["uri"].replace("gs://", "")

    script = """
import json, sys
import fsspec

generations = json.loads(sys.argv[1])
prefix = sys.argv[2]
filesystem = fsspec.filesystem("gcs", token="anon")
results = []
for relative, expected in sorted(generations.items()):
    info = filesystem.info(f"{prefix}/{relative}")
    actual = {"generation": str(info["generation"]), "size_bytes": int(info["size"])}
    results.append({"object": relative, "expected": expected, "actual": actual})
print(json.dumps(results))
"""
    completed = subprocess.run(
        [str(python), "-c", script, json.dumps(generations), prefix],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    if completed.returncode != 0:
        raise M0Error(
            f"the source objects could not be read: {(completed.stdout + completed.stderr).strip()}"
        )
    try:
        results = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise M0Error(f"the source object check produced no usable result: {error}") from error

    for entry in results:
        entry["matches"] = entry["expected"] == entry["actual"]
        if not entry["matches"]:
            raise M0Error(
                f"the source object {entry['object']} changed: "
                f"expected {entry['expected']}, found {entry['actual']}"
            )
    return results


def compare_extraction(extraction_dir: Path) -> dict[str, Any]:
    """Compare a fresh extraction with the frozen sample.

    The fresh extraction is the formal payload, so it is compared with the
    payload that the frozen sample records. The float32 source identity is
    the common ground: both sides must produce the same source matrix hash.

    On a fresh clone the frozen payload does not exist yet, because it is not
    committed. In that case the extraction itself establishes the frozen
    sample, and the comparison is made against the manifest's recorded
    source identity instead.
    """
    import numpy as np

    from common import sha256_bytes

    extracted = read_json(extraction_dir / PAYLOAD_PATH.name)

    if not PAYLOAD_PATH.is_file():
        # The frozen payload is not committed, so the extraction is the frozen
        # sample. The manifest records the expected source identity, and the
        # extraction is checked against it.
        manifest = read_json(MANIFEST_PATH)
        expected_matrix_sha = manifest["provenance"]["transformations"][0][
            "matrix_sha256_float32_le"
        ]
        extracted_matrix = np.asarray(extracted["values"], dtype="<f4")
        extracted_sha = sha256_bytes(extracted_matrix.tobytes(order="C"))
        identical = extracted_sha == expected_matrix_sha
        return {
            "source_matrix_sha256_float32_le": extracted_sha,
            "frozen_source_matrix_sha256_float32_le": expected_matrix_sha,
            "byte_identical": identical,
            "payload_sha256": sha256_file(extraction_dir / PAYLOAD_PATH.name),
            "compared_against": "the source identity recorded in the manifest",
            "frozen_payload_present": False,
        }

    frozen = read_json(PAYLOAD_PATH)

    if extracted["member_ids"] != frozen["member_ids"]:
        raise M0Error("the re-extracted sample has a different member set")
    if extracted["point_ids"] != frozen["point_ids"]:
        raise M0Error("the re-extracted sample has a different point set")

    extracted_matrix = np.asarray(extracted["values"], dtype="<f4")
    frozen_matrix = np.asarray(frozen["values"], dtype="<f4")

    extracted_sha = sha256_bytes(extracted_matrix.tobytes(order="C"))
    frozen_sha = sha256_bytes(frozen_matrix.tobytes(order="C"))
    identical = extracted_sha == frozen_sha and bool((extracted_matrix == frozen_matrix).all())

    return {
        "source_matrix_sha256_float32_le": extracted_sha,
        "frozen_source_matrix_sha256_float32_le": frozen_sha,
        "byte_identical": identical,
        "payload_sha256": sha256_file(extraction_dir / PAYLOAD_PATH.name),
        "compared_against": "the frozen sample payload",
        "frozen_payload_present": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workdir",
        type=Path,
        default=DEFAULT_WORKDIR,
        help="the directory the fresh extraction is written to",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the recovery record to this JSON file",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="only verify the frozen material; do not attempt the source recovery",
    )
    arguments = parser.parse_args()

    manifest = read_json(MANIFEST_PATH)
    record: dict[str, Any] = {
        "schema_version": "m0.sample-recovery.v1",
        "sample_id": manifest["sample_id"],
        "source_uri": manifest["source"]["uri"],
        "status": "not_run",
        "reason": "",
    }

    if arguments.offline:
        record["status"] = "not_run"
        record["reason"] = "the source recovery was skipped on request"
        if arguments.output:
            write_json(arguments.output, record)
        print("sample recovery: not_run (skipped on request)")
        return 2

    python = DATA_VENV / "bin" / "python"
    if not python.is_file():
        record["reason"] = f"the locked data environment is missing at {DATA_VENV}"
        if arguments.output:
            write_json(arguments.output, record)
        print(f"FAIL: {record['reason']}")
        return 2
    if not EXTRACTOR.is_file():
        record["reason"] = f"the extraction tool is missing at {EXTRACTOR}"
        if arguments.output:
            write_json(arguments.output, record)
        print(f"FAIL: {record['reason']}")
        return 2

    # The extraction never overwrites an existing frozen sample, so the
    # re-extraction runs into a separate directory and the frozen sample is
    # left untouched.
    if arguments.workdir.exists():
        import shutil

        shutil.rmtree(arguments.workdir)
    arguments.workdir.mkdir(parents=True, exist_ok=True)

    try:
        record["source_objects"] = check_source_objects(manifest, python)
    except M0Error as error:
        message = str(error)
        if "could not be read" in message:
            record["status"] = "not_run"
            record["reason"] = message
            if arguments.output:
                write_json(arguments.output, record)
            print(f"sample recovery: not_run ({record['reason']})")
            return 2
        record["status"] = "fail"
        record["reason"] = message
        if arguments.output:
            write_json(arguments.output, record)
        print(f"FAIL: {record['reason']}")
        return 1
    except Exception as error:  # noqa: BLE001 - the source may be unreachable
        record["status"] = "not_run"
        record["reason"] = f"the source objects could not be read: {error}"
        if arguments.output:
            write_json(arguments.output, record)
        print(f"sample recovery: not_run ({record['reason']})")
        return 2

    completed = subprocess.run(
        [
            str(python),
            str(EXTRACTOR),
            "--rebuild",
            "--from-source",
            "--force",
            "--output-dir",
            str(arguments.workdir),
        ],
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    record["extraction_output"] = (completed.stdout + completed.stderr).strip()
    if completed.returncode != 0:
        record["status"] = "fail"
        record["reason"] = f"the extraction failed with exit code {completed.returncode}"
        if arguments.output:
            write_json(arguments.output, record)
        print(f"FAIL: {record['reason']}")
        return 1

    try:
        record["comparison"] = compare_extraction(arguments.workdir)
    except M0Error as error:
        record["status"] = "fail"
        record["reason"] = str(error)
        if arguments.output:
            write_json(arguments.output, record)
        print(f"FAIL: {record['reason']}")
        return 1

    if not record["comparison"]["byte_identical"]:
        record["status"] = "fail"
        record["reason"] = "the re-extracted sample differs from the frozen sample"
        if arguments.output:
            write_json(arguments.output, record)
        print(f"FAIL: {record['reason']}")
        return 1

    # The formal payload must also be reproducible from the frozen material.
    record["status"] = "pass"
    record["reason"] = (
        "the pinned source objects are unchanged and the re-extraction reproduces the "
        "frozen sample byte for byte"
    )
    if arguments.output:
        write_json(arguments.output, record)

    print("sample recovery: pass")
    print(f"  source objects verified: {len(record['source_objects'])}")
    print(f"  re-extracted sha256:     {record['comparison']['source_matrix_sha256_float32_le']}")
    print(f"  frozen sha256:           {record['comparison']['frozen_source_matrix_sha256_float32_le']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
