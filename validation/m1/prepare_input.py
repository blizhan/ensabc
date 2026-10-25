#!/usr/bin/env python3
"""Prepare M1 inputs from frozen sample and native configuration.

Usage:
    python3 validation/m1/prepare_input.py \
        --manifest PATH \
        --payload PATH \
        --config PATH \
        --output-dir DIR

Performs:
1. Validates source sample manifest and payload using M0 eligibility checks.
2. Validates native configuration with M1 ScenarioConfig schema.
3. Preserves source float32->f64 conversion and member/grid ordering.
4. Generates:
   - <output-dir>/inputs/task.json
   - <output-dir>/inputs/identity.json
   - <output-dir>/config/native.json
   - <output-dir>/real_sample.sql (if template available in T029)
5. Never mutates input files.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from typing import Any

from validation.m1.common import (
    M1Error,
    read_json,
    sha256_bytes,
    sha256_file,
    utc_now,
    write_json,
)
from validation.m1.schema import (
    validate_scenario_config,
    validate_task_input,
)

# Import M0 sample validation if available
try:
    from validation.m0.verify_sample import evaluate_eligibility
except ImportError:
    evaluate_eligibility = None


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare M1 task input and configuration envelope.")
    parser.add_argument("--manifest", required=True, type=Path, help="Path to sample manifest JSON")
    parser.add_argument("--payload", required=True, type=Path, help="Path to sample payload JSON")
    parser.add_argument("--config", required=True, type=Path, help="Path to native config JSON")
    parser.add_argument("--output-dir", required=True, type=Path, help="Output run directory")
    return parser.parse_args(args)


def prepare_input(manifest_path: Path, payload_path: Path, config_path: Path, output_dir: Path) -> dict[str, Any]:
    if not manifest_path.is_file():
        raise M1Error(f"Manifest not found: {manifest_path}", code="FILE_NOT_FOUND")
    if not payload_path.is_file():
        raise M1Error(f"Payload not found: {payload_path}", code="FILE_NOT_FOUND")
    if not config_path.is_file():
        raise M1Error(f"Config not found: {config_path}", code="FILE_NOT_FOUND")

    manifest = read_json(manifest_path)
    payload = read_json(payload_path)
    config = read_json(config_path)

    # 1. Eligibility validation
    if evaluate_eligibility is not None:
        try:
            evaluate_eligibility(manifest, payload)
        except Exception as e:
            raise M1Error(f"Sample eligibility check failed: {e}", code="ELIGIBILITY_FAILED") from e

    # 2. Config validation
    validate_scenario_config(config)

    # 3. Content identity hashes
    manifest_sha256 = sha256_file(manifest_path)
    payload_sha256 = sha256_file(payload_path)
    source_matrix_sha256 = payload.get("matrix_sha256") or payload.get("data_sha256") or payload_sha256
    config_sha256 = sha256_file(config_path)

    # 4. Construct TaskContext and MemberInput list
    members_raw = manifest.get("members", [])
    values_raw = payload.get("values", [])

    if len(members_raw) != len(values_raw):
        raise M1Error(f"Manifest members count {len(members_raw)} != payload values count {len(values_raw)}", code="SHAPE_MISMATCH")

    member_inputs = []
    for idx, (m_info, feats) in enumerate(zip(members_raw, values_raw)):
        m_id = m_info["member_id"]
        # Convert values to f64 float list
        features_f64 = [float(x) for x in feats]
        member_inputs.append({
            "member_index": idx,
            "member_id": m_id,
            "features": features_f64,
        })

    task_context = {
        "task_id": manifest.get("sample_id", "m1-task"),
        "model": manifest.get("source", {}).get("model", "ifs_ens"),
        "init_time": manifest.get("init_time", ""),
        "valid_time": manifest.get("valid_time", ""),
        "variable": manifest.get("variable", {}).get("name", "t2m"),
        "unit": manifest.get("variable", {}).get("unit", "K"),
        "feature_version": "wb2.ifs_ens.v1",
        "sample_id": manifest.get("sample_id", ""),
        "manifest_sha256": manifest_sha256,
        "payload_sha256": payload_sha256,
        "source_matrix_sha256": source_matrix_sha256,
    }

    task_payload = {
        "schema_version": "m1.task.v1",
        "task_context": task_context,
        "config": config,
        "members": member_inputs,
    }

    # Validate output task envelope
    validate_task_input(task_payload)

    # 5. Write outputs
    inputs_dir = output_dir / "inputs"
    config_dir = output_dir / "config"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    config_dir.mkdir(parents=True, exist_ok=True)

    task_path = inputs_dir / "task.json"
    write_json(task_path, task_payload)

    identity_payload = {
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": manifest_sha256,
        "payload_path": str(payload_path.resolve()),
        "payload_sha256": payload_sha256,
        "source_matrix_sha256": source_matrix_sha256,
        "config_path": str(config_path.resolve()),
        "config_sha256": config_sha256,
        "member_count": len(member_inputs),
        "feature_count": len(member_inputs[0]["features"]) if member_inputs else 0,
        "created_at": utc_now(),
    }
    identity_path = inputs_dir / "identity.json"
    write_json(identity_path, identity_payload)

    config_dest = config_dir / "native.json"
    write_json(config_dest, config)

    # Check for SQL template in T029
    template_path = Path(__file__).resolve().parent / "examples" / "real_sample.sql"
    if template_path.is_file():
        # Will render SQL template if available
        ext_path = Path("/tmp/ensabc-m1-extension-target/ensabc_m0_extension.duckdb_extension")
        sql_template = template_path.read_text(encoding="utf-8")
        rendered_sql = sql_template.replace("{{TASK_JSON_PATH}}", str(task_path.resolve()))
        rendered_sql = rendered_sql.replace("{{EXTENSION_PATH}}", str(ext_path.resolve()))
        (output_dir / "real_sample.sql").write_text(rendered_sql, encoding="utf-8")

    return {
        "task_path": task_path,
        "identity_path": identity_path,
        "config_path": config_dest,
    }


def main() -> int:
    try:
        args = parse_args()
    except SystemExit as e:
        return 2

    try:
        prepare_input(args.manifest, args.payload, args.config, args.output_dir)
        print(f"Successfully prepared M1 input in {args.output_dir}")
        return 0
    except M1Error as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1 if e.code not in ("FILE_NOT_FOUND", "EMPTY_RUN_ID") else 2
    except Exception as e:
        print(f"Unexpected failure: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
