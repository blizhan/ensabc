# M0 Real Sample Spike: Findings

## Result

This is a bounded data-retrieval spike, not a formal M0 result. A local CSV matrix was recovered from the public WeatherBench 2 IFS ENS Zarr and passed the extraction script's shape, member, finite-value, and SHA-256 checks. The matrix is `50 members × 3 latitudes × 3 longitudes`; no production code or DuckOMO integration was changed.

## Frozen sample

- Dataset: `gs://weatherbench2/datasets/ifs_ens/2018-2022-64x32_equiangular_conservative.zarr`.
- Field: `2m_temperature`, source unit `K`, copied unchanged with no unit conversion.
- Forecast: initialization `2020-01-01T00:00:00Z`, lead `24 h`, valid time `2020-01-02T00:00:00Z`.
- Members: source coordinate `number=1..50`, all 50 values present; `number=0` (control) is absent. This is the 50 perturbed-member artifact, not a 51-member file with control.
- Ordered point grid: latitude `[42.1875, 47.8125, 53.4375]`, longitude `[5.625, 11.25, 16.875]`; matrix point order is latitude-major then longitude-major.
- Source indices: `time=1460`, `prediction_timedelta=4`, `number` array indices `[0,50)`, latitude `[23,24,25]`, longitude `[1,2,3]`. Per-column index/coordinate mapping is in `sample.json`.
- Shape: source selection `[member=50, latitude=3, longitude=3]`; CSV matrix `[50,9]`; values are little-endian float32. Observed range: `269.146240234375..282.75799560546875 K`.
- Matrix SHA-256 (canonical float32 little-endian bytes): `09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe`.
- CSV SHA-256: `18e007ed06688f54a7f1c1d11ab38453eaa7fc15c8733d059c02ab3ead2f1050`.
- Object identity: GCS generations and object sizes for the consolidated metadata, selected coordinate chunks, and one temperature chunk are recorded in `sample.json`. Those selected object payload sizes sum to `1,071,163` bytes. The temperature object is `1,049,445` bytes; its logical chunk is `(4,50,1,64,32)`, or `1,638,400` uncompressed bytes. It is one low-resolution global chunk and includes four adjacent initialization records; the output matrix selects only one. No high-resolution chunk or full archive was read. The byte total is object payload size, excluding HTTP headers/retries.

## Selection and source notes

WeatherBench 2 documents the 2018–2022 IFS ENS archive as downloaded from TIGGE, lists the 64×32 artifact, states that all TIGGE archive data can only be used for research purposes, and says to check the license for more specific constraints. Its general data guide directs users to each dataset's GCS `LICENSE` file; it also says the supplied datasets are regridded using first-order conservative weights. The four bounded `LICENSE` object probes listed below returned `FileNotFoundError`. See [WeatherBench 2 Data Guide](https://weatherbench2.readthedocs.io/en/latest/data-guide.html#ifs-ens) (checked 2026-10-20).

The official ECMWF TIGGE licence page's section for the ECMWF contribution states CC BY-NC 4.0 and lists attribution, non-commercial-use, and other conditions. That statement is recorded only for the contribution identified on that page; this spike did not establish its exact applicability to the WeatherBench 2 regridded object. No licence interpretation or authorization is inferred. The exact derivative licence and permitted distribution path remain a formal M0 gate. The following exact public object keys returned `FileNotFoundError` during bounded metadata probes: `gs://weatherbench2/LICENSE`, `gs://weatherbench2/datasets/LICENSE`, `gs://weatherbench2/datasets/ifs_ens/LICENSE`, and the selected Zarr root's `LICENSE`. See [ECMWF TIGGE licence](https://apps.ecmwf.int/datasets/licences/tigge/) (checked 2026-10-20).

The Dynamical IFS ENS Icechunk path was triaged but not read. Its current catalog documents a 16.9 MiB uncompressed chunk and 1.7 GiB uncompressed shard, and states that the old `data.dynamical.org` access ends September 30, 2026; the current route requires the Icechunk/STAC access path. This is not a failed read. With the 100 MiB request ceiling, verifying shard range behavior would have added an unbounded transfer risk, so the one allowed source switch was made to the small WeatherBench 2 artifact. See [Dynamical IFS ENS catalog](https://dynamical.org/catalog/ecmwf-ifs-ens-forecast-15-day-0-25-degree/) and [migration note](https://dynamical.org/migration-2026/).

Additional read-only verification via `snv/deepseek-v4.1-flash` confirmed that Dynamical's STAC collection metadata (`https://stac.dynamical.org/ecmwf-ifs-ens-forecast-15-day-0-25-degree/collection.json`) explicitly declares `"license": "CC-BY-4.0"` alongside ECMWF attribution requirements, providing a clear fallback if direct repository redistribution of a frozen slice is required.

## Commands and verification

Run from the repository root. Python dependencies were installed only in `/tmp/ensabc-m0-data/.venv`; `requirements.lock` captures the resolved versions. GCS access explicitly used the anonymous token mode; no AWS configuration or credentials were read.

```sh
uv venv /tmp/ensabc-m0-data/.venv --python 3.13
uv pip install --python /tmp/ensabc-m0-data/.venv/bin/python xarray zarr gcsfs
uv pip freeze --python /tmp/ensabc-m0-data/.venv/bin/python > specs/001-m0-baseline-validation/spikes/data/requirements.lock
/tmp/ensabc-m0-data/.venv/bin/python specs/001-m0-baseline-validation/spikes/data/extract_sample.py
/tmp/ensabc-m0-data/.venv/bin/python specs/001-m0-baseline-validation/spikes/data/extract_sample.py --verify-only
```

The extraction completed with exit code 0 and printed `PASS offline matrix: shape=(50, 9), members=50` with the matrix hash above. The separate `--verify-only` command also completed with exit code 0 before the additive metadata fields and overwrite guard were added. The matrix bytes were not downloaded again after those edits; the main session can run the final offline verification.

The extractor pins the observed GCS generations and sizes, checks the complete `number=1..50` axis, the selected init/lead and all 9 coordinates, and rejects non-finite values. It refuses to overwrite an existing `sample.csv` or `sample.json`; use `--verify-only` for the frozen sample or choose a new `--output-dir` for another extraction.

## Reproduction and recovery

Offline recovery uses the local frozen `sample.csv` plus `sample.json`; verify them without network access:

```sh
/tmp/ensabc-m0-data/.venv/bin/python specs/001-m0-baseline-validation/spikes/data/extract_sample.py --verify-only
```

To rebuild the isolated environment and verify the local matrix:

```sh
uv venv /tmp/ensabc-m0-data/.venv --python 3.13
uv pip sync --python /tmp/ensabc-m0-data/.venv/bin/python specs/001-m0-baseline-validation/spikes/data/requirements.lock
/tmp/ensabc-m0-data/.venv/bin/python specs/001-m0-baseline-validation/spikes/data/extract_sample.py --verify-only
```

To request another source extraction without touching this evidence, pass a new directory, for example:

```sh
/tmp/ensabc-m0-data/.venv/bin/python specs/001-m0-baseline-validation/spikes/data/extract_sample.py --output-dir /tmp/ensabc-m0-data/reextract
```

The extractor fails closed if any recorded source object generation or size changed. The local CSV remains usable even if the public source later changes or disappears.

## Formal M0 gaps

`sample.json` is a spike inventory, not the complete formal M0 manifest or a redistribution decision. The exact licence and permitted distribution path for this WeatherBench 2 derivative remain an explicit formal M0 gate; the source statements above are recorded without reconciling them or inferring authorization. Before treating this as the SC-001/FR-013 sample gate, formal work still needs to retain a source-specific licence document or its durable identifier; define and validate the M0 manifest schema; record the regridding/source lineage and citation/attribution in that schema; and confirm that the intended verifier can lawfully recover the frozen sample.

This sample is adequate to prove a one-time small-region, single-init/single-valid-time member-matrix extraction. It does not establish repeated retrieval from a pinned historical object generation, native-grid equivalence, any DuckDB/DuckOMO ingestion path, or an algorithmic baseline.
