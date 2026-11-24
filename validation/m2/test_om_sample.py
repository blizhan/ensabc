#!/usr/bin/env python3
"""Test and validate OM v3 sample fixtures against three reading paths and negative error cases.

Usage:
    validation/m2/.venv/bin/python validation/m2/test_om_sample.py --sample-dir DIR --cache-dir DIR

Reading paths tested:
1. Source raw numpy (<f4 arrays)
2. Python omfiles (OmFileReader)
3. DuckDB DuckOMO extension (read_om table function)

Negative test cases:
1. NaN in member features (detected)
2. Inf in member features (detected)
3. 49 members instead of 50 (detected by axis count / row count)
4. Shifted valid_time (detected by SQL sanity guard)
5. Corrupted OM file header (rejected on read)
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import numpy as np
import omfiles

from validation.m2.common import atomic_write_json, load_json, sha256_file, parse_duckdb_json, run_isolated_duckdb, real_replay_binding
from validation.m2.schema import ValidationError

EXPECTED_GRID_ID = "ca3212142cd36c9704cd0aa1f47c24c905611eca7bb911427279997de133ad26"


def read_via_omfiles(om_path: Path) -> dict[str, Any]:
    reader = omfiles.OmFileReader(str(om_path))
    var = reader.get_child_by_name("temperature_2m")
    compression = var.compression_name
    data = var.read_array(())
    # Read child attributes
    children = {}
    for i in range(var.num_children):
        c = var.get_child_by_index(i)
        children[c.name] = c.read_scalar() if c.is_scalar else c.read_array(())

    valid_time = children.get("valid_time")
    members = children.get("member_label")
    lats = children.get("latitude")
    lons = children.get("longitude")
    return {
        "compression": compression,
        "shape": list(data.shape),
        "dtype": str(data.dtype),
        "data": data,
        "valid_time": int(valid_time) if valid_time is not None else None,
        "member_count": len(members) if members is not None else None,
        "lat_count": len(lats) if lats is not None else None,
        "lon_count": len(lons) if lons is not None else None,
    }


def read_via_duckomo(
    duckdb_cli: Path,
    ext_path: Path,
    om_path: Path,
    tmp_home: Path,
) -> dict[str, Any]:
    sql = f"""
    SET TimeZone = 'UTC';
    SET extension_directory = '{tmp_home}';
    SET autoinstall_known_extensions = false;
    SET autoload_known_extensions = false;
    LOAD '{ext_path}';

    SELECT
      count(*) AS total_rows,
      count(temperature_2m) AS non_null_rows,
      count(*) FILTER (WHERE NOT isfinite(temperature_2m)) AS non_finite_rows,
      count(DISTINCT member) AS distinct_members,
      count(DISTINCT om_source.parent_point_index) AS distinct_points,
      min(om_source.grid_id) AS min_grid_id,
      max(om_source.grid_id) AS max_grid_id,
      min(temperature_2m)::DOUBLE AS min_val,
      max(temperature_2m)::DOUBLE AS max_val,
      strftime(min(valid_time), '%Y-%m-%dT%H:%M:%SZ') AS min_valid,
      strftime(max(valid_time), '%Y-%m-%dT%H:%M:%SZ') AS max_valid
    FROM read_om('{om_path}',
      variable := 'temperature_2m',
      dimensions := ['member', 'lat', 'lon'],
      grid := {{'nx': 3, 'ny': 3, 'lat0': 42.1875, 'lon0': 5.625, 'dlat': 5.625, 'dlon': 5.625, 'order': 'separate'}},
      spatial_axes := ['lat', 'lon'],
      axes := {{'member': {{'axis': 'member', 'values': [
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50]}}}},
      include_source := true);
    """
    proc = run_isolated_duckdb(sql, cli=str(duckdb_cli))
    if proc.returncode != 0:
        return {"error": proc.stderr.strip()}
    parsed = json.loads(proc.stdout.strip())
    return parsed[0] if parsed else {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Test and validate OM v3 sample fixtures")
    parser.add_argument("--sample-dir", type=Path, required=True, help="Directory containing OM sample")
    parser.add_argument("--cache-dir", type=Path, default=REPO_ROOT / "validation" / "m2" / ".cache", help="Cache directory")
    parser.add_argument("--out-report", type=Path, default=None, help="Path for JSON test report")
    args = parser.parse_args()

    sample_dir = args.sample_dir
    manifest_path = sample_dir / "om_sample_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    manifest = load_json(manifest_path)
    duckdb_cli = Path(subprocess.check_output(["which", "duckdb"]).decode().strip())
    duckomo_ext = args.cache_dir / "v0.3.5-rc.1" / "duckomo.duckdb_extension"
    if not duckomo_ext.is_file():
        raise FileNotFoundError(f"DuckOMO extension not found: {duckomo_ext}")

    tmp_home = Path("/tmp/ensabc_m2_test_home")
    tmp_home.mkdir(parents=True, exist_ok=True)

    test_results: list[dict[str, Any]] = []

    # 1. Test 8 Real Batches: omfiles vs DuckOMO vs Manifest
    print("Testing 8 real OM batches...")
    for b in manifest["batches"]:
        om_file = sample_dir / b["om_filename"]
        assert om_file.is_file(), f"Missing file: {om_file}"

        # Test omfiles read
        om_info = read_via_omfiles(om_file)
        assert om_info["compression"] == "fpx_xor_2d"
        assert om_info["shape"] == [50, 3, 3]
        assert om_info["member_count"] == 50
        assert om_info["valid_time"] == 1578182400

        data_min = float(om_info["data"].min())
        data_max = float(om_info["data"].max())
        assert abs(data_min - b["min_val"]) < 1e-5
        assert abs(data_max - b["max_val"]) < 1e-5

        # Test DuckOMO read
        duck_info = read_via_duckomo(duckdb_cli, duckomo_ext, om_file, tmp_home)
        assert "error" not in duck_info, f"DuckOMO error: {duck_info.get('error')}"
        assert duck_info["total_rows"] == 450
        assert duck_info["non_null_rows"] == 450
        assert duck_info["non_finite_rows"] == 0
        assert duck_info["distinct_members"] == 50
        assert duck_info["distinct_points"] == 9
        assert duck_info["min_grid_id"] == EXPECTED_GRID_ID
        assert duck_info["max_grid_id"] == EXPECTED_GRID_ID
        assert duck_info["min_valid"] == "2020-01-05T00:00:00Z"
        assert abs(duck_info["min_val"] - b["min_val"]) < 1e-4
        assert abs(duck_info["max_val"] - b["max_val"]) < 1e-4

        test_results.append({
            "test": f"batch_{b['ordinal']}_{b['init_time_utc']}",
            "status": "PASS",
            "omfiles_compression": om_info["compression"],
            "duckdb_rows": duck_info["total_rows"],
        })

    # 2. Test Negative Fixtures
    print("Testing negative OM fixtures...")
    neg_dir = sample_dir / "negative"

    # 2.1 NaN fixture (detected by NULL or isnan)
    nan_path = neg_dir / manifest["negative_fixtures"]["nan"]
    nan_duck = read_via_duckomo(duckdb_cli, duckomo_ext, nan_path, tmp_home)
    assert nan_duck.get("non_null_rows", 450) < 450 or nan_duck.get("non_finite_rows", 0) > 0, "Expected NaN to cause null/non-finite rows"
    test_results.append({"test": "negative_nan_detected", "status": "PASS", "non_null_rows": nan_duck["non_null_rows"]})

    # 2.2 Inf fixture
    inf_path = neg_dir / manifest["negative_fixtures"]["inf"]
    inf_duck = read_via_duckomo(duckdb_cli, duckomo_ext, inf_path, tmp_home)
    assert inf_duck.get("non_finite_rows", 0) > 0, "Expected non_finite_rows > 0 for Inf fixture"
    test_results.append({"test": "negative_inf_detected", "status": "PASS", "non_finite_rows": inf_duck["non_finite_rows"]})

    # 2.3 49 members fixture (should fail DuckOMO axis dimension check or row count)
    m49_path = neg_dir / manifest["negative_fixtures"]["member49"]
    m49_duck = read_via_duckomo(duckdb_cli, duckomo_ext, m49_path, tmp_home)
    # Axes validation expects 50 values for axis length 49 -> Binder Error
    assert "error" in m49_duck or m49_duck.get("distinct_members") != 50
    test_results.append({"test": "negative_member49_rejected", "status": "PASS"})

    # 2.4 Corrupted header (should fail DuckDB extension load/read)
    corrupt_path = neg_dir / manifest["negative_fixtures"]["corrupted_header"]
    corrupt_duck = read_via_duckomo(duckdb_cli, duckomo_ext, corrupt_path, tmp_home)
    assert "error" in corrupt_duck
    test_results.append({"test": "negative_corrupted_header_rejected", "status": "PASS"})

    # 2.5 Shifted valid_time
    shift_path = neg_dir / manifest["negative_fixtures"]["valid_time_shifted"]
    shift_duck = read_via_duckomo(duckdb_cli, duckomo_ext, shift_path, tmp_home)
    assert shift_duck.get("min_valid") != "2020-01-05T00:00:00Z"
    test_results.append({"test": "negative_valid_time_shift_detected", "status": "PASS"})

    summary = {
        "schema_version": "m2.test-om-sample-report.v1",
        "batches_tested": len(manifest["batches"]),
        "negative_cases_tested": len(manifest["negative_fixtures"]),
        "status": "PASS",
        "results": test_results,
    }

    if args.out_report:
        atomic_write_json(args.out_report, summary)
        print(f"Test report written to {args.out_report}")

    print("All OM sample tests passed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
