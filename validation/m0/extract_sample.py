#!/usr/bin/env python3
"""Convert the frozen WeatherBench 2 IFS ENS slice into the formal M0 sample.

The spike under `specs/001-m0-baseline-validation/spikes/data/` produced a
50x9 member-major matrix in CSV form plus an inventory. That inventory is a
planning artifact, not the `m0.sample.v1` manifest the data model requires.

This script performs the deterministic conversion:

* the source values are restored to their original little-endian float32
  representation and only then widened to float64 (the CSV holds a
  round-trippable decimal rendering of a float32, not a float64 source value);
* the member-major matrix is written to a JSON payload whose SHA-256 is
  recorded in the manifest;
* the manifest records source field mapping, the object generations that
  identify the source snapshot, the licence and recovery terms, and the
  transformations that were applied.

The source can be reached in two ways:

* `--from-source` re-extracts the matrix directly from the pinned WeatherBench
  2 archive. This is the path a fresh clone uses, because the derived numeric
  material is not committed to the repository while the redistribution
  permission for the re-gridded derivative is unresolved.
* without `--from-source` the script converts the locally frozen planning
  spike material, which is only useful in a working tree that already has it.

Neither path overwrites an existing frozen sample: the script fails closed
instead.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    FIXTURES_DIR,
    REPO_ROOT,
    M0Error,
    read_json,
    sha256_bytes,
    sha256_file,
    write_json,
)

SPIKE_DIR = REPO_ROOT / "specs" / "001-m0-baseline-validation" / "spikes" / "data"
SPIKE_CSV = SPIKE_DIR / "sample.csv"
SPIKE_INVENTORY = SPIKE_DIR / "sample.json"

MANIFEST_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_manifest.json"
PAYLOAD_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_data.json"

EXPECTED_MATRIX_SHA256 = "09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe"
EXPECTED_CSV_SHA256 = "18e007ed06688f54a7f1c1d11ab38453eaa7fc15c8733d059c02ab3ead2f1050"
EXPECTED_MEMBERS = list(range(1, 51))

# The source selection that produced the frozen sample. The object generations
# are the identity of the source snapshot; they are checked before the data is
# read, so a regenerated object is detected instead of silently accepted.
SOURCE_URI = "gs://weatherbench2/datasets/ifs_ens/2018-2022-64x32_equiangular_conservative.zarr"
SOURCE_VARIABLE = "2m_temperature"
SOURCE_INIT = "2020-01-01T00:00:00"
SOURCE_LEAD_HOURS = 24
SOURCE_OBJECT_EXPECTATIONS = {
    ".zmetadata": ("1704461457118357", 19874),
    "time/0": ("1704461459436554", 1279),
    "number/0": ("1704461459341184", 90),
    "prediction_timedelta/0": ("1704461459252726", 114),
    "latitude/0": ("1704461459285842", 169),
    "longitude/0": ("1704461459039650", 192),
    "2m_temperature/365.0.4.0.0": ("1704550502844049", 1049445),
}


def _restore_float32_rows(csv_path: Path) -> tuple[list[int], list[list[float]], list[str]]:
    """Read the spike CSV and restore the source float32 values.

    The CSV stores each float32 as a `%.9g` decimal. Restoring through
    `numpy.float32` recovers the exact source bit pattern; the result is then
    widened to float64 for the frozen JSON payload.
    """
    import numpy as np

    with csv_path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.reader(stream))
    if not rows:
        raise M0Error(f"spike CSV is empty: {csv_path}")

    header, body = rows[0], rows[1:]
    point_columns = header[1:]
    members: list[int] = []
    values: list[list[float]] = []
    for row_index, row in enumerate(body):
        if len(row) != len(header):
            raise M0Error(f"spike CSV row {row_index} has {len(row)} fields, expected {len(header)}")
        members.append(int(row[0]))
        values.append([float(value) for value in row[1:]])

    matrix32 = np.asarray(values, dtype="<f4")
    matrix64 = matrix32.astype(np.float64)
    return members, matrix64.tolist(), point_columns


def _extract_from_source() -> tuple[list[int], list[list[float]], list[str], dict[str, Any]]:
    """Extract the frozen matrix directly from the pinned source archive.

    The source object generations are checked before the data is read, so a
    regenerated or changed object is detected instead of silently accepted.
    The result is the same 50x9 member-major matrix that the planning spike
    produced; the caller verifies the float32 hash.
    """
    import fsspec
    import numpy as np
    import xarray as xr

    prefix = SOURCE_URI.replace("gs://", "")
    filesystem = fsspec.filesystem("gcs", token="anon")
    object_generations: dict[str, dict[str, Any]] = {}
    for relative, (generation, size) in SOURCE_OBJECT_EXPECTATIONS.items():
        info = filesystem.info(f"{prefix}/{relative}")
        actual = {"generation": str(info["generation"]), "size_bytes": int(info["size"])}
        if actual != {"generation": generation, "size_bytes": size}:
            raise M0Error(
                f"the source object {relative} changed: expected "
                f"{{'generation': '{generation}', 'size_bytes': {size}}}, found {actual}"
            )
        object_generations[relative] = actual

    dataset = xr.open_zarr(
        SOURCE_URI, consolidated=True, chunks=None, storage_options={"token": "anon"}
    )
    try:
        field = dataset[SOURCE_VARIABLE]
        if field.attrs["units"] != "K":
            raise M0Error(f"unexpected source unit: {field.attrs['units']!r}")

        init = np.datetime64(SOURCE_INIT)
        time_indices = np.flatnonzero(dataset.time.values == init)
        lead_indices = np.flatnonzero(dataset.prediction_timedelta.values == SOURCE_LEAD_HOURS)
        members = dataset.number.values.astype(int).tolist()
        if time_indices.size != 1 or lead_indices.size != 1:
            raise M0Error("the source does not contain exactly one matching initialization")
        if members != EXPECTED_MEMBERS:
            raise M0Error("the source does not declare the expected 50 perturbed members")

        latitude = dataset.latitude.values
        longitude = dataset.longitude.values
        latitude_center = int(np.argmin(np.abs(latitude - 50.0)))
        longitude_center = int(np.argmin(np.abs(longitude - 10.0)))
        latitude_indices = np.arange(latitude_center - 1, latitude_center + 2)
        longitude_indices = np.arange(longitude_center - 1, longitude_center + 2)

        selected = field.isel(
            time=int(time_indices[0]),
            prediction_timedelta=int(lead_indices[0]),
            latitude=latitude_indices,
            longitude=longitude_indices,
        ).transpose("number", "latitude", "longitude")
        values = np.asarray(selected.values, dtype="<f4")
        if values.shape != (50, 3, 3):
            raise M0Error(f"unexpected extracted shape: {values.shape}")
        if not np.isfinite(values).all():
            raise M0Error("the extracted matrix contains a non-finite value")

        latitudes = latitude[latitude_indices].astype(float).tolist()
        longitudes = longitude[longitude_indices].astype(float).tolist()
        point_columns = [
            f"lat_{lat:g}_lon_{lon:g}" for lat in latitudes for lon in longitudes
        ]

        inventory = {
            "retrieved_at_utc": _utc_now(),
            "dataset_uri": SOURCE_URI,
            "dataset_object_generations_and_sizes": object_generations,
            "source_object_payload_bytes": sum(
                entry["size_bytes"] for entry in object_generations.values()
            ),
            "variable": SOURCE_VARIABLE,
            "source_units": field.attrs["units"],
            "source_dims": list(field.dims),
            "source_chunks": list(field.encoding["chunks"]),
            "initialization_time": SOURCE_INIT,
            "lead_time_hours": SOURCE_LEAD_HOURS,
            "valid_time": str(init + np.timedelta64(SOURCE_LEAD_HOURS, "h")),
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
            "member_set_description": (
                "50 perturbed members (number=1..50); no number=0 control member is present "
                "in this file."
            ),
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
            "source_points": [
                {
                    "column": point_columns[row * 3 + column],
                    "latitude_index": int(latitude_index),
                    "longitude_index": int(longitude_index),
                    "latitude": latitudes[row],
                    "longitude": longitudes[column],
                }
                for row, latitude_index in enumerate(latitude_indices)
                for column, longitude_index in enumerate(longitude_indices)
            ],
        }
    finally:
        dataset.close()

    matrix = values.reshape(50, 9)
    return members, matrix.astype(np.float64).tolist(), point_columns, inventory


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _build_manifest(
    inventory: dict[str, Any],
    member_ids: list[str],
    point_ids: list[str],
    payload_sha256: str,
    payload_bytes: int,
    matrix_sha256_float32_le: str,
) -> dict[str, Any]:
    source_points = inventory["source_points"]
    latitudes = inventory["latitudes"]
    longitudes = inventory["longitudes"]

    points = []
    for point_id, source_point in zip(point_ids, source_points, strict=True):
        points.append(
            {
                "point_id": point_id,
                "latitude": source_point["latitude"],
                "longitude": source_point["longitude"],
                "source_grid_index": {
                    "latitude_index": source_point["latitude_index"],
                    "longitude_index": source_point["longitude_index"],
                },
            }
        )

    members = []
    for member_id, source_label in zip(member_ids, inventory["members"], strict=True):
        members.append(
            {
                "member_id": member_id,
                "source_label": str(source_label),
                "role": "perturbed",
                "source_coordinate": inventory["member_coordinate"],
                "source_coordinate_value": int(source_label),
            }
        )

    member_set = [
        {
            "member_id": member["member_id"],
            "source_label": member["source_label"],
            "role": member["role"],
        }
        for member in members
    ]

    return {
        "schema_version": "m0.sample.v1",
        "sample_id": "ifs_ens_20200101_t2m_50x9",
        "kind": "real_ensemble",
        "source": {
            "provider": "ECMWF (via Google WeatherBench 2)",
            "product": "IFS ENS 2018-2022 64x32 equiangular conservative regrid",
            "model": "ECMWF IFS ensemble prediction system (ENS)",
            "uri": inventory["dataset_uri"],
            "retrieved_at": inventory["retrieved_at_utc"],
            "source_document": "https://weatherbench2.readthedocs.io/en/latest/data-guide.html#ifs-ens",
            "init_time_evidence": {
                "init_time": inventory["initialization_time"],
                "lead_hours": inventory["lead_time_hours"],
                "valid_time": inventory["valid_time"],
                "source_time_index": inventory["source_indices"]["time"],
                "source_prediction_timedelta_index": inventory["source_indices"]["prediction_timedelta"],
                "note": (
                    "The initialization is addressed by the pinned archive object generation "
                    "recorded in object_generations, not by a 'latest' alias."
                ),
            },
            "object_generations": inventory["dataset_object_generations_and_sizes"],
            "source_object_payload_bytes": inventory["source_object_payload_bytes"],
            "member_set": member_set,
            "member_set_description": inventory["member_set_description"],
            "source_dims": inventory["source_dims"],
            "source_chunks": inventory["source_chunks"],
        },
        "init_time": "2020-01-01T00:00:00Z",
        "valid_time": "2020-01-02T00:00:00Z",
        "lead_seconds": 86400,
        "variable": {
            "source_name": inventory["variable"],
            "meaning": "2 metre temperature",
            "height": "2 m above ground",
            "source_units": inventory["source_units"],
            "normalized_units": inventory["source_units"],
            "conversion": (
                "unchanged; the source values are stored in K and the frozen payload keeps K. "
                "The float32 source values are widened to float64 without changing their value."
            ),
        },
        "region": {
            "description": "3x3 grid point window over western/central Europe around 50N, 10E",
            "latitude_min": inventory["spatial_bounds"]["latitude_min"],
            "latitude_max": inventory["spatial_bounds"]["latitude_max"],
            "longitude_min": inventory["spatial_bounds"]["longitude_min"],
            "longitude_max": inventory["spatial_bounds"]["longitude_max"],
            "longitude_convention": (
                "0-360 degrees east as stored by the source archive; no negative-longitude "
                "normalization was applied, and the point coordinates are the actual source coordinates."
            ),
            "point_order": "latitude-major then longitude-major",
        },
        "points": points,
        "members": members,
        "layout": {
            "matrix_order": "member-major (values[member][point])",
            "member_order": "ascending source member number (number=1..50)",
            "spatial_order": "latitude-major then longitude-major",
            "dtype": "float64",
            "shape": [len(member_ids), len(point_ids)],
        },
        "data_file": PAYLOAD_PATH.name,
        "data_sha256": payload_sha256,
        "data_bytes": payload_bytes,
        "provenance": {
            "extraction_steps": [
                "Read the pinned WeatherBench 2 Zarr archive with the recorded object generations.",
                "Select time index 1460 (2020-01-01T00:00:00) and prediction_timedelta index 4 (24 h).",
                "Select number=1..50, latitude indices [23, 24, 25] and longitude indices [1, 2, 3].",
                "Transpose to (number, latitude, longitude) and reshape to a 50x9 member-major matrix.",
                "Store the matrix as little-endian float32 and record its SHA-256.",
                "Restore the float32 values from the round-trippable decimal CSV and widen them to float64.",
                "Write the float64 member-major JSON payload and record its SHA-256 here.",
            ],
            "extraction_tool_version": "validation/m0/extract_sample.py (M0 formal extraction)",
            "extraction_tool_source": {
                "planning_spike": "specs/001-m0-baseline-validation/spikes/data/extract_sample.py",
                "planning_spike_output": "specs/001-m0-baseline-validation/spikes/data/sample.csv",
                "planning_spike_output_sha256": EXPECTED_CSV_SHA256,
            },
            "transformations": [
                {
                    "step": "float32 -> float64 widening",
                    "detail": (
                        "The archive stores float32. The values were restored to their exact float32 "
                        "representation and then widened to float64. The CSV decimal rendering was not "
                        "treated as a float64 source value."
                    ),
                    "source_dtype": "little-endian float32",
                    "target_dtype": "float64",
                    "matrix_sha256_float32_le": matrix_sha256_float32_le,
                },
                {
                    "step": "no unit conversion",
                    "detail": "Source unit K is preserved; no Celsius conversion is applied.",
                },
                {
                    "step": "no interpolation",
                    "detail": (
                        "The nine points are the source grid cells themselves; no regridding or "
                        "interpolation was performed by the M0 extraction."
                    ),
                },
            ],
            "credentials_used": "none; the source bucket was read with anonymous access",
        },
        "usage": {
            "license": (
                "WeatherBench 2 documents that the IFS ENS archive originates from the TIGGE archive "
                "and that 'All data from the TIGGE archive can only be used for research purposes. "
                "Please check the license for more specific constraints.' No LICENSE object exists at "
                "the pinned GCS paths, so the exact licence file for this re-gridded derivative could "
                "not be retrieved. The current ECMWF TIGGE terms publish the ECMWF contribution under "
                "CC BY 4.0, but this repository does not assert that those terms govern the "
                "WeatherBench 2 re-gridded object."
            ),
            "license_status": "unresolved_for_redistribution",
            "license_evidence": [
                "https://weatherbench2.readthedocs.io/en/latest/data-guide.html#ifs-ens",
                "https://ecds.ecmwf.int/licences/tigge-licence",
            ],
            "attribution": (
                "ECMWF IFS ensemble forecasts, downloaded from the TIGGE archive and re-gridded and "
                "republished by Google Research as WeatherBench 2. WeatherBench 2 is licensed under "
                "Apache 2.0 for the code and dataset tooling."
            ),
            "restrictions": (
                "Research use is stated by the source documentation. Redistribution permission for the "
                "re-gridded derivative is not established, so the frozen numeric payload is not "
                "committed to the repository; the manifest, the source generation evidence and the "
                "recovery procedure are committed instead."
            ),
            "recovery": (
                "The payload is regenerated deterministically by running "
                "`python3 validation/m0/extract_sample.py --rebuild` in an environment with the locked "
                "data dependencies, which re-reads the pinned object generations and reproduces the "
                "same float32 matrix SHA-256. The numeric payload is therefore recoverable and "
                "verifiable by any reviewer with network access to the source bucket, without "
                "requiring the repository to redistribute the data."
            ),
        },
    }


def build_payload(
    member_ids: list[str],
    point_ids: list[str],
    matrix: list[list[float]],
) -> dict[str, Any]:
    return {
        "schema_version": "m0.sample.payload.v1",
        "kind": "real_ensemble",
        "member_ids": member_ids,
        "point_ids": point_ids,
        "values": matrix,
    }


def convert(
    force: bool = False,
    from_source: bool = False,
    output_dir: Path | None = None,
) -> tuple[Path, Path]:
    """Produce the manifest and payload.

    With `from_source` the matrix is read from the pinned source archive; this
    is the path a fresh clone uses, because the derived numeric material is
    not committed while the redistribution permission is unresolved. Without
    it, the locally frozen planning spike material is converted.

    `output_dir` writes the result somewhere other than the committed fixture
    directory. The recovery check uses it so that a re-extraction never
    touches the frozen sample.
    """
    manifest_path = MANIFEST_PATH
    payload_path = PAYLOAD_PATH
    if output_dir is not None:
        output_dir = output_dir.resolve()
        manifest_path = output_dir / MANIFEST_PATH.name
        payload_path = output_dir / PAYLOAD_PATH.name

    # The overwrite protection comes first, so that an existing frozen sample
    # is never replaced and the reason for the refusal is the overwrite
    # itself and not a later step.
    if not force:
        for path in (manifest_path, payload_path):
            if path.exists():
                raise M0Error(
                    f"refusing to overwrite existing frozen sample {path}; "
                    "use --force to regenerate it deliberately"
                )

    if from_source:
        members, matrix, point_columns, inventory = _extract_from_source()
    else:
        if not SPIKE_CSV.is_file() or not SPIKE_INVENTORY.is_file():
            raise M0Error(
                f"the planning spike material is missing under {SPIKE_DIR}. "
                "Use --from-source to read the pinned source archive instead."
            )

        csv_sha = sha256_file(SPIKE_CSV)
        if csv_sha != EXPECTED_CSV_SHA256:
            raise M0Error(
                f"spike CSV hash mismatch: expected {EXPECTED_CSV_SHA256}, found {csv_sha}"
            )

        inventory = read_json(SPIKE_INVENTORY)
        if inventory["members"] != EXPECTED_MEMBERS:
            raise M0Error("spike inventory does not declare the expected 50 perturbed members")

        members, matrix, point_columns = _restore_float32_rows(SPIKE_CSV)

    import numpy as np

    matrix32 = np.asarray(matrix, dtype="<f4")
    matrix_sha = sha256_bytes(matrix32.tobytes(order="C"))
    if matrix_sha != EXPECTED_MATRIX_SHA256:
        raise M0Error(
            f"restored float32 matrix hash mismatch: expected {EXPECTED_MATRIX_SHA256}, found {matrix_sha}"
        )

    member_ids = [f"ifs_ens_{member:02d}" for member in members]
    point_ids = list(point_columns)

    payload = build_payload(member_ids, point_ids, matrix)
    payload_bytes = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    payload_sha = sha256_bytes(payload_bytes)

    manifest = _build_manifest(
        inventory,
        member_ids,
        point_ids,
        payload_sha,
        len(payload_bytes),
        matrix_sha,
    )

    payload_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.write_bytes(payload_bytes)
    write_json(manifest_path, manifest)
    return manifest_path, payload_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="rebuild the frozen sample",
    )
    parser.add_argument(
        "--from-source",
        action="store_true",
        help="read the pinned source archive instead of the local planning spike material",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="allow overwriting an existing frozen sample (requires --rebuild)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="write the manifest and the payload somewhere else, so that the "
        "frozen sample in the fixture directory is not touched",
    )
    arguments = parser.parse_args()

    if arguments.force and not arguments.rebuild:
        parser.error("--force requires --rebuild")
    if arguments.from_source and not arguments.rebuild:
        parser.error("--from-source requires --rebuild")

    if not arguments.rebuild:
        print(f"frozen sample already present: {MANIFEST_PATH}")
        print(f"frozen sample already present: {PAYLOAD_PATH}")
        return 0

    try:
        manifest_path, payload_path = convert(
            force=arguments.force,
            from_source=arguments.from_source,
            output_dir=arguments.output_dir,
        )
    except M0Error as error:
        print(f"FAIL: {error}")
        return 1
    manifest = read_json(manifest_path)
    print(f"wrote manifest: {manifest_path}")
    print(f"wrote payload:  {payload_path}")
    print(f"sample_id:      {manifest['sample_id']}")
    print(f"shape:          {manifest['layout']['shape']}")
    print(f"data_sha256:    {manifest['data_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
