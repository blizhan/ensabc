#!/usr/bin/env python3
"""Prepare the 8-batch WB2 IFS ENS real sample into lossless OM v3 files and generate negative test fixtures.

Must be executed using the isolated OM virtual environment:
    validation/m2/.venv/bin/python validation/m2/prepare_om_sample.py --cache-dir DIR --out-dir DIR

Outputs:
- <out-dir>/wb2-ifs-ens-t2m-20200101T00Z.om ... (8 files)
- <out-dir>/wb2-ifs-ens-t2m-stacked-8run.om
- <out-dir>/om_sample_manifest.json
- <out-dir>/negative/ (invalid/corrupted OM fixtures for test_om_sample.py and verify_om_paths.py)
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shutil
import sys
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import numcodecs
import numpy as np
import omfiles

from validation.m2.common import atomic_write_json, load_json, sha256_bytes, sha256_file

SOURCE_FIXTURE_PATH = REPO_ROOT / "validation" / "m2" / "fixtures" / "wb2_8run_source.json"
COMPRESSION = "fpx_xor_2d"


def unix(s: str) -> int:
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def fetch_file(url: str, cap: int, dest: Path) -> tuple[bytes, dict[str, str]]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file() and dest.stat().st_size <= cap:
        data = dest.read_bytes()
        return data, {}
    with urllib.request.urlopen(url, timeout=45) as r:
        data = r.read(cap + 1)
        headers = {k.lower(): v for k, v in r.headers.items()}
    if len(data) > cap:
        raise RuntimeError(f"Oversize response for {url}")
    dest.write_bytes(data)
    return data, headers


def decode_zarr(meta: dict[str, Any], name: str, raw: bytes) -> tuple[np.ndarray, dict[str, Any]]:
    arr = meta[f"{name}/.zarray"]
    codec = numcodecs.get_codec(arr["compressor"]) if arr.get("compressor") else None
    buf = codec.decode(raw) if codec else raw
    return np.frombuffer(buf, dtype=arr["dtype"]).reshape(arr["chunks"]), arr


def make_om_attrs(w: omfiles.OmFileWriter, doc: dict[str, Any], run_seconds: list[int], lead_seconds: list[int]) -> list[Any]:
    lat = np.asarray(doc["latitudes"], dtype=np.float32)
    lon = np.asarray(doc["longitudes"], dtype=np.float32)
    members = np.asarray(doc["member_labels"], dtype=np.int64)
    run = np.asarray(run_seconds, dtype=np.int64)
    lead = np.asarray(lead_seconds, dtype=np.int64)
    valid_sec = unix(doc["valid_time_utc"])
    return [
        w.write_array(lat, chunks=[lat.size], compression=COMPRESSION, name="latitude"),
        w.write_array(lon, chunks=[lon.size], compression=COMPRESSION, name="longitude"),
        w.write_array(members, chunks=[members.size], compression="pfor_delta_2d", name="member_label"),
        w.write_array(run, chunks=[run.size], compression="pfor_delta_2d", name="run_unix_seconds"),
        w.write_array(lead, chunks=[lead.size], compression="pfor_delta_2d", name="lead_seconds"),
        w.write_scalar(np.int64(valid_sec), name="valid_time"),
        w.write_scalar("K", name="units"),
        w.write_scalar("2m_temperature", name="source_variable"),
    ]


def write_om_file(
    path: Path,
    data: np.ndarray,
    chunks: list[int],
    doc: dict[str, Any],
    run_seconds: list[int],
    lead_seconds: list[int],
    coordinates: str,
    *,
    custom_attrs: list[Any] | None = None,
    compression: str = COMPRESSION,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    w = omfiles.OmFileWriter(str(path))
    children = custom_attrs if custom_attrs is not None else make_om_attrs(w, doc, run_seconds, lead_seconds)
    children.append(w.write_scalar(coordinates, name="coordinates"))
    arr = w.write_array(data, chunks=chunks, compression=compression, name="temperature_2m", children=children)
    root = w.write_group("", [arr])
    w.close(root)


def generate_negative_fixtures(neg_dir: Path, sample_matrix: np.ndarray, doc: dict[str, Any]) -> dict[str, str]:
    """Create negative test fixtures: nan, inf, 49 members, pfor lossy, shifted valid_time."""
    neg_dir.mkdir(parents=True, exist_ok=True)
    fixtures = {}

    # 1. NaN in member 8, point 5
    nan_mat = sample_matrix.copy()
    nan_mat[8, 1, 2] = np.nan
    p_nan = neg_dir / "nan-member8-point5.om"
    write_om_file(p_nan, nan_mat, [50, 3, 3], doc, [unix("2020-01-01T00:00:00Z")], [96 * 3600], "member lat lon")
    fixtures["nan"] = p_nan.name

    # 2. Inf in member 1, point 0
    inf_mat = sample_matrix.copy()
    inf_mat[1, 0, 0] = np.inf
    p_inf = neg_dir / "inf-member1-point0.om"
    write_om_file(p_inf, inf_mat, [50, 3, 3], doc, [unix("2020-01-01T00:00:00Z")], [96 * 3600], "member lat lon")
    fixtures["inf"] = p_inf.name

    # 3. 49 members
    m49_mat = sample_matrix[:49, :, :].copy()
    p_49 = neg_dir / "member49.om"
    doc_49 = dict(doc)
    doc_49["member_labels"] = list(range(1, 50))
    write_om_file(p_49, m49_mat, [49, 3, 3], doc_49, [unix("2020-01-01T00:00:00Z")], [96 * 3600], "member lat lon")
    fixtures["member49"] = p_49.name

    # 4. Valid time shifted (inconsistent with init + lead)
    p_shift = neg_dir / "valid-time-shifted.om"
    w_shift = omfiles.OmFileWriter(str(p_shift))
    lat = np.asarray(doc["latitudes"], dtype=np.float32)
    lon = np.asarray(doc["longitudes"], dtype=np.float32)
    members = np.asarray(doc["member_labels"], dtype=np.int64)
    children = [
        w_shift.write_array(lat, chunks=[lat.size], compression=COMPRESSION, name="latitude"),
        w_shift.write_array(lon, chunks=[lon.size], compression=COMPRESSION, name="longitude"),
        w_shift.write_array(members, chunks=[members.size], compression="pfor_delta_2d", name="member_label"),
        w_shift.write_array(np.asarray([unix("2020-01-01T00:00:00Z")], dtype=np.int64), chunks=[1], compression="pfor_delta_2d", name="run_unix_seconds"),
        w_shift.write_array(np.asarray([96 * 3600], dtype=np.int64), chunks=[1], compression="pfor_delta_2d", name="lead_seconds"),
        w_shift.write_scalar(np.int64(unix("2020-01-06T00:00:00Z")), name="valid_time"),  # shifted by 1 day!
        w_shift.write_scalar("K", name="units"),
        w_shift.write_scalar("2m_temperature", name="source_variable"),
        w_shift.write_scalar("member lat lon", name="coordinates"),
    ]
    arr = w_shift.write_array(sample_matrix, chunks=[50, 3, 3], compression=COMPRESSION, name="temperature_2m", children=children)
    root = w_shift.write_group("", [arr])
    w_shift.close(root)
    fixtures["valid_time_shifted"] = p_shift.name

    # 5. Corrupted header
    p_corrupt = neg_dir / "corrupted_header.om"
    p_corrupt.write_bytes(b"INVALID_OM_HEADER_BYTES_1234567890")
    fixtures["corrupted_header"] = p_corrupt.name

    return fixtures


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare OM v3 files from pinned WB2 8-run sources")
    parser.add_argument("--cache-dir", type=Path, default=REPO_ROOT / "validation" / "m2" / ".cache", help="Cache directory")
    parser.add_argument("--out-dir", type=Path, required=True, help="Output directory for OM files")
    args = parser.parse_args()

    cache_dir = args.cache_dir
    wb2_cache = cache_dir / "wb2"
    wb2_cache.mkdir(parents=True, exist_ok=True)
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    source_doc = load_json(SOURCE_FIXTURE_PATH)
    base_url = source_doc["dataset"]

    # 1. Fetch .zmetadata
    zmeta_raw, _ = fetch_file(base_url + ".zmetadata", 35000, wb2_cache / "zmetadata.json")
    assert hashlib.sha256(zmeta_raw).hexdigest() == source_doc["zmetadata_sha256"]
    meta = json.loads(zmeta_raw)["metadata"]

    # 2. Fetch coordinates
    coords = {}
    for name in ("time", "number", "prediction_timedelta", "latitude", "longitude"):
        raw, _ = fetch_file(base_url + f"{name}/0", 250000, wb2_cache / f"coord-{name}.bin")
        a, _ = decode_zarr(meta, name, raw)
        coords[name] = a

    lat_idx = source_doc["latitude_indices"]
    lon_idx = source_doc["longitude_indices"]
    lat_vals = coords["latitude"][lat_idx].astype(np.float64).tolist()
    lon_vals = coords["longitude"][lon_idx].astype(np.float64).tolist()
    assert lat_vals == source_doc["latitudes"]
    assert lon_vals == source_doc["longitudes"]

    batches_info = []
    matrix_stack = []

    for b in source_doc["runs"]:
        init_iso = b["init_time_utc"]
        lead_h = b["lead_hours"]
        key = b["chunk_key"]
        expected_size = b["chunk_bytes"]
        expected_sha = b["chunk_sha256"]
        gen = b["generation"]

        # Fetch chunk
        url = base_url + f"2m_temperature/{key}?generation={gen}"
        raw, _ = fetch_file(url, 1500000, wb2_cache / f"t2m-{key}.bin")
        assert len(raw) == expected_size
        assert hashlib.sha256(raw).hexdigest() == expected_sha

        a, _ = decode_zarr(meta, "2m_temperature", raw)
        tchunk, _, lchunk, _, _ = (int(x) for x in key.split("."))
        init_dt = dt.datetime.fromisoformat(init_iso.replace("Z", "+00:00"))
        hours_since = (init_dt - dt.datetime(2018, 1, 1, tzinfo=dt.timezone.utc)) / dt.timedelta(hours=1)
        tindex = int(hours_since // 12)

        # Slice 50 perturbed members (1..50), 3x3 grid
        x = a[tindex % 4, :, 0][:, lon_idx][:, :, lat_idx].transpose(0, 2, 1)
        x = np.ascontiguousarray(x, dtype="<f4")
        assert x.shape == (50, 3, 3)
        assert np.isfinite(x).all()

        matrix_stack.append(x)
        payload_sha = hashlib.sha256(x.tobytes()).hexdigest()

        # Write per-batch OM v3 file
        stem = init_dt.strftime("%Y%m%dT%HZ")
        om_path = out_dir / f"wb2-ifs-ens-t2m-{stem}.om"
        write_om_file(
            om_path,
            x,
            [50, 3, 3],
            source_doc,
            [unix(init_iso)],
            [lead_h * 3600],
            "member lat lon",
        )
        file_sha = sha256_file(om_path)

        batches_info.append({
            "ordinal": b["ordinal"],
            "init_time_utc": init_iso,
            "lead_hours": lead_h,
            "om_filename": om_path.name,
            "om_file_sha256": file_sha,
            "om_file_bytes": om_path.stat().st_size,
            "payload_sha256": payload_sha,
            "min_val": float(x.min()),
            "max_val": float(x.max()),
        })

    # Write stacked 8-run OM v3 file
    stacked = np.ascontiguousarray(np.stack(matrix_stack))
    stacked_path = out_dir / "wb2-ifs-ens-t2m-stacked-8run.om"
    write_om_file(
        stacked_path,
        stacked,
        [1, 50, 3, 3],
        source_doc,
        [unix(b["init_time_utc"]) for b in source_doc["runs"]],
        [b["lead_hours"] * 3600 for b in source_doc["runs"]],
        "run member lat lon",
    )
    stacked_sha = sha256_file(stacked_path)

    # Generate negative fixtures using batch 0 as template
    neg_dir = out_dir / "negative"
    neg_fixtures = generate_negative_fixtures(neg_dir, matrix_stack[0], source_doc)

    # Build manifest
    manifest = {
        "schema_version": "m2.om-sample-manifest.v1",
        "dataset_source": base_url,
        "valid_time_utc": source_doc["valid_time_utc"],
        "latitudes": source_doc["latitudes"],
        "longitudes": source_doc["longitudes"],
        "compression": COMPRESSION,
        "batches_count": len(batches_info),
        "batches": batches_info,
        "stacked": {
            "om_filename": stacked_path.name,
            "om_file_sha256": stacked_sha,
            "om_file_bytes": stacked_path.stat().st_size,
            "shape": [8, 50, 3, 3],
        },
        "negative_fixtures": neg_fixtures,
    }

    manifest_path = out_dir / "om_sample_manifest.json"
    atomic_write_json(manifest_path, manifest)
    print(f"Sample preparation complete: 8 batches written to {out_dir}")
    print(f"Manifest written to {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
