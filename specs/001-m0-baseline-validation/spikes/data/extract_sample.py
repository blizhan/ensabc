#!/usr/bin/env python3
"""Non-formal M0 spike: freeze one small WeatherBench2 IFS ENS sample."""

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import fsspec
import numpy as np
import xarray as xr


OUT = Path(__file__).resolve().parent
PREFIX = "weatherbench2/datasets/ifs_ens/2018-2022-64x32_equiangular_conservative.zarr"
URL = f"gs://{PREFIX}"
VARIABLE = "2m_temperature"
INIT = np.datetime64("2020-01-01T00:00:00")
LEAD_HOURS = 24
EXPECTED_OBJECTS = {
    ".zmetadata": ("1704461457118357", 19874),
    "time/0": ("1704461459436554", 1279),
    "number/0": ("1704461459341184", 90),
    "prediction_timedelta/0": ("1704461459252726", 114),
    "latitude/0": ("1704461459285842", 169),
    "longitude/0": ("1704461459039650", 192),
    "2m_temperature/365.0.4.0.0": ("1704550502844049", 1049445),
}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def check_source_objects(fs):
    objects = {}
    for relative_path, (generation, size) in EXPECTED_OBJECTS.items():
        info = fs.info(f"{PREFIX}/{relative_path}")
        actual = {"generation": str(info["generation"]), "size_bytes": int(info["size"])}
        assert actual == {"generation": generation, "size_bytes": size}, (relative_path, actual)
        objects[relative_path] = actual
    return objects


def verify_local(output_dir):
    metadata = json.loads((output_dir / "sample.json").read_text())
    with (output_dir / "sample.csv").open(newline="") as stream:
        rows = list(csv.reader(stream))
    header, body = rows[0], rows[1:]
    members = [int(row[0]) for row in body]
    matrix = np.asarray([[float(value) for value in row[1:]] for row in body], dtype="<f4")
    assert members == metadata["members"] == list(range(1, 51))
    assert header[1:] == metadata["point_columns"]
    assert matrix.shape == (50, 9)
    assert np.isfinite(matrix).all()
    assert sha256(matrix.tobytes(order="C")) == metadata["matrix_sha256_float32_le"]
    assert sha256((output_dir / "sample.csv").read_bytes()) == metadata["sample_csv_sha256"]
    print(f"PASS offline matrix: shape={matrix.shape}, members={len(members)}, sha256={metadata['matrix_sha256_float32_le']}")


def extract(output_dir):
    csv_path = output_dir / "sample.csv"
    metadata_path = output_dir / "sample.json"
    if csv_path.exists() or metadata_path.exists():
        raise FileExistsError(f"refusing to overwrite existing sample evidence in {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    fs = fsspec.filesystem("gcs", token="anon")
    source_objects = check_source_objects(fs)
    ds = xr.open_zarr(URL, consolidated=True, chunks=None, storage_options={"token": "anon"})
    try:
        field = ds[VARIABLE]
        assert field.dims == ("time", "number", "prediction_timedelta", "longitude", "latitude")
        assert field.attrs["units"] == "K"
        assert tuple(field.encoding["chunks"]) == (4, 50, 1, 64, 32)
        assert ds.sizes["number"] == 50

        time_indices = np.flatnonzero(ds.time.values == INIT)
        lead_indices = np.flatnonzero(ds.prediction_timedelta.values == LEAD_HOURS)
        members = ds.number.values.astype(int).tolist()
        assert time_indices.size == lead_indices.size == 1
        assert members == list(range(1, 51))

        latitude = ds.latitude.values
        longitude = ds.longitude.values
        latitude_center = int(np.argmin(np.abs(latitude - 50.0)))
        longitude_center = int(np.argmin(np.abs(longitude - 10.0)))
        latitude_indices = np.arange(latitude_center - 1, latitude_center + 2)
        longitude_indices = np.arange(longitude_center - 1, longitude_center + 2)
        assert latitude_indices.min() >= 0 and latitude_indices.max() < latitude.size
        assert longitude_indices.min() >= 0 and longitude_indices.max() < longitude.size

        selected = field.isel(
            time=int(time_indices[0]),
            prediction_timedelta=int(lead_indices[0]),
            latitude=latitude_indices,
            longitude=longitude_indices,
        ).transpose("number", "latitude", "longitude")
        values = np.asarray(selected.values, dtype="<f4")
        assert values.shape == (50, 3, 3)
        assert np.isfinite(values).all()

        latitudes = latitude[latitude_indices].astype(float).tolist()
        longitudes = longitude[longitude_indices].astype(float).tolist()
        point_columns = [f"lat_{lat:g}_lon_{lon:g}" for lat in latitudes for lon in longitudes]
        source_points = [
            {
                "column": point_columns[row * 3 + column],
                "latitude_index": int(latitude_index),
                "longitude_index": int(longitude_index),
                "latitude": latitudes[row],
                "longitude": longitudes[column],
            }
            for row, latitude_index in enumerate(latitude_indices)
            for column, longitude_index in enumerate(longitude_indices)
        ]
        matrix = values.reshape(50, 9)
        with csv_path.open("w", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(["member_id", *point_columns])
            for member, row in zip(members, matrix, strict=True):
                writer.writerow([member, *(format(float(value), ".9g") for value in row)])

        object_bytes = sum(item["size_bytes"] for item in source_objects.values())
        metadata = {
            "status": "spike_only_not_formal_M0",
            "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
            "dataset_uri": URL,
            "dataset_object_generations_and_sizes": source_objects,
            "source_object_payload_bytes": object_bytes,
            "variable": VARIABLE,
            "source_units": field.attrs["units"],
            "source_dims": list(field.dims),
            "source_chunks": list(field.encoding["chunks"]),
            "initialization_time": str(INIT),
            "lead_time_hours": LEAD_HOURS,
            "valid_time": str(INIT + np.timedelta64(LEAD_HOURS, "h")),
            "source_indices": {
                "time": int(time_indices[0]),
                "prediction_timedelta": int(lead_indices[0]),
                "number_start": 0,
                "number_stop_exclusive": 50,
                "latitude": latitude_indices.astype(int).tolist(),
                "longitude": longitude_indices.astype(int).tolist(),
            },
            "member_coordinate": "number",
            "members": members,
            "member_set_description": "50 perturbed members (number=1..50); no number=0 control member is present in this file.",
            "unit_handling": "unchanged; source values are stored in K without conversion",
            "spatial_bounds": {
                "latitude_min": min(latitudes),
                "latitude_max": max(latitudes),
                "longitude_min": min(longitudes),
                "longitude_max": max(longitudes),
            },
            "latitudes": latitudes,
            "longitudes": longitudes,
            "point_columns": point_columns,
            "source_points": source_points,
            "shape": {"axes": ["member", "latitude", "longitude"], "values": [50, 3, 3]},
            "matrix_shape": [50, 9],
            "matrix_dtype": "little-endian float32",
            "formal_manifest_status": "spike inventory only; license resolution and formal M0 provenance schema remain open",
            "value_min": float(values.min()),
            "value_max": float(values.max()),
            "matrix_sha256_float32_le": sha256(matrix.tobytes(order="C")),
            "sample_csv_sha256": sha256(csv_path.read_bytes()),
        }
        metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    finally:
        ds.close()
    verify_local(output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true", help="verify the frozen local CSV without network access")
    parser.add_argument("--output-dir", type=Path, default=OUT, help="new output directory; existing sample files are never overwritten")
    args = parser.parse_args()
    verify_local(args.output_dir) if args.verify_only else extract(args.output_dir)
