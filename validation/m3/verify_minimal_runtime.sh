#!/usr/bin/env bash
# Verify M3 standalone pure-Rust m3_runner and 11-table DuckDB SQL evolution
# replay inside an airgapped minimal container (`--network none`, `ubuntu:24.04`).
#
# Implements FR-017 & SC-007 (T039):
#   1. Confirms all 8 prohibited reference/scripting tools (`python3`, `python`,
#      `cdo`, `wgrib`, `wgrib2`, `gfortran`, `R`, `Rscript`) are ABSENT.
#   2. Executes standalone `m3_runner` (`--mode pair` AND `--mode chain` on the
#      real 8-batch WB2 inputs) with zero network and zero DuckDB.
#   3. Executes `duckdb -unsigned` loading ONLY pinned `duckomo.duckdb_extension`
#      (`v0.3.5-rc.1`) + `ensabc_m0_extension.duckdb_extension` across the 8 `.om`
#      files through `schema.sql` + `real_8run_evolution.sql`, publishing all 11
#      tables (5 M2 + 6 M3) and exporting JSON rows.
#   4. Executes same-session error recovery: invalid `ens_evolve_scenarios` calls
#      -> mid-transaction `inject_m3_failure` -> `ROLLBACK` -> `0` residual rows
#      across all 6 M3 & 5 M2 tables -> subsequent valid publish (`RECOVERED`).
#   5. Compares container outputs 100% field-by-field against host replay and
#      writes `<run-dir>/minimal-runtime.json`.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_DIR="${ENSABC_M3_TARGET_DIR:-/tmp/ensabc-m3-extension-target}"
ENSABC_EXT="${EXTENSION_PATH:-$TARGET_DIR/ensabc_m0_extension.duckdb_extension}"
RUNNER="${RUNNER_PATH:-$TARGET_DIR/m3_runner}"
CACHE_DIR="${ENSABC_M2_CACHE_DIR:-$ROOT/validation/m2/.cache}"
DUCKOMO_EXT=""
BASE_IMAGE="${MINIMAL_RUNTIME_IMAGE:-ubuntu:24.04}"
RUNTIME_DIR="/tmp/ensabc-m3-minimal-runtime-$$"
CONTAINER_NAME="ensabc-m3-minimal-runtime-$$"
RUN_DIR=""
SAMPLE_DIR=""
KEEP_STAGING=0

usage() {
    cat <<EOF
Usage: $0 --run-dir DIR --extension PATH --runner PATH [options]

Options:
  --run-dir <dir>      Target validation run directory (required)
  --extension <path>   Compiled ensabc_m0_extension.duckdb_extension path
  --runner <path>      Compiled m3_runner binary path
  --sample-dir <dir>   Frozen 8-batch OM sample directory
                       (default: <run-dir>/sample or validation/m2/data/om_sample)
  --cache-dir <dir>    Verified DuckOMO cache directory (default: validation/m2/.cache)
  --duckomo <path>     Explicit duckomo.duckdb_extension override
  --image <name>       Minimal container image (default: $BASE_IMAGE)
  --keep               Keep /tmp staging directory after completion
  -h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-dir)
            RUN_DIR="$2"
            shift 2
            ;;
        --extension)
            ENSABC_EXT="$2"
            shift 2
            ;;
        --runner)
            RUNNER="$2"
            shift 2
            ;;
        --sample-dir)
            SAMPLE_DIR="$2"
            shift 2
            ;;
        --cache-dir)
            CACHE_DIR="$2"
            shift 2
            ;;
        --duckomo)
            DUCKOMO_EXT="$2"
            shift 2
            ;;
        --image)
            BASE_IMAGE="$2"
            shift 2
            ;;
        --keep)
            KEEP_STAGING=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "$RUN_DIR" ]]; then
    echo "FAIL: --run-dir is required" >&2
    exit 2
fi

if [[ -z "$DUCKOMO_EXT" ]]; then
    DUCKOMO_EXT="$CACHE_DIR/v0.3.5-rc.1/duckomo.duckdb_extension"
fi

if [[ ! -f "$ENSABC_EXT" ]]; then
    echo "FAIL: ensabc extension missing: $ENSABC_EXT" >&2
    exit 2
fi

if [[ ! -f "$DUCKOMO_EXT" ]]; then
    echo "FAIL: duckomo extension missing: $DUCKOMO_EXT" >&2
    exit 2
fi

if [[ ! -f "$RUNNER" ]]; then
    echo "FAIL: m3_runner missing: $RUNNER" >&2
    exit 2
fi

DUCKDB_BIN="$(command -v duckdb || true)"
if [[ -z "$DUCKDB_BIN" ]]; then
    echo "FAIL: duckdb CLI is not available on PATH" >&2
    exit 2
fi
DUCKDB_REAL="$(readlink -f "$DUCKDB_BIN")"

if ! command -v docker >/dev/null 2>&1; then
    echo "FAIL: docker is required for minimal runtime verification" >&2
    exit 2
fi

if [[ -z "$SAMPLE_DIR" ]]; then
    if [[ -f "$RUN_DIR/sample/wb2-ifs-ens-t2m-20200101T00Z.om" ]]; then
        SAMPLE_DIR="$RUN_DIR/sample"
    else
        SAMPLE_DIR="$ROOT/validation/m2/data/om_sample"
    fi
fi

if [[ ! -f "$SAMPLE_DIR/wb2-ifs-ens-t2m-20200101T00Z.om" ]] || [[ ! -f "$SAMPLE_DIR/om_sample_manifest.json" ]]; then
    echo "FAIL: 8-batch OM sample or manifest missing in $SAMPLE_DIR" >&2
    exit 2
fi

HOST_PYTHON="python3"
if [[ -x "$ROOT/validation/m2/.venv/bin/python" ]]; then
    HOST_PYTHON="$ROOT/validation/m2/.venv/bin/python"
fi

cleanup() {
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    if [[ "$KEEP_STAGING" == "0" ]]; then
        rm -rf "$RUNTIME_DIR"
    fi
}
trap cleanup EXIT

rm -rf "$RUNTIME_DIR"
mkdir -p \
    "$RUNTIME_DIR/root/usr/local/bin" \
    "$RUNTIME_DIR/root/opt" \
    "$RUNTIME_DIR/root/om" \
    "$RUNTIME_DIR/root/lib" \
    "$RUNTIME_DIR/root/work/validation/m3/sql" \
    "$RUNTIME_DIR/baseline" \
    "$RUNTIME_DIR/results"

echo "Staging M3 airgapped minimal runtime in $RUNTIME_DIR..."

cp -L "$DUCKDB_REAL" "$RUNTIME_DIR/root/usr/local/bin/duckdb"
cp -L "$RUNNER" "$RUNTIME_DIR/root/usr/local/bin/m3_runner"
chmod +x "$RUNTIME_DIR/root/usr/local/bin/duckdb" "$RUNTIME_DIR/root/usr/local/bin/m3_runner"

cp -L "$ENSABC_EXT" "$RUNTIME_DIR/root/opt/ensabc_m0_extension.duckdb_extension"
cp -L "$DUCKOMO_EXT" "$RUNTIME_DIR/root/opt/duckomo.duckdb_extension"
cp "$SAMPLE_DIR"/*.om "$RUNTIME_DIR/root/om/"
cp "$ROOT/validation/m3/sql/schema.sql" "$RUNTIME_DIR/root/work/validation/m3/sql/schema.sql"
cp "$ROOT/validation/m3/sql/stage_and_publish.sql" "$RUNTIME_DIR/root/work/validation/m3/sql/stage_and_publish.sql"
cp "$ROOT/validation/m3/sql/real_8run_evolution.sql" "$RUNTIME_DIR/root/work/validation/m3/sql/real_8run_evolution.sql"

copy_libs() {
    local bin="$1"
    while read -r library; do
        [[ -f "$library" ]] || continue
        cp -L "$library" "$RUNTIME_DIR/root/lib/$(basename "$library")"
    done < <(ldd "$bin" 2>/dev/null | awk '{print $3}' | grep -E '^/' || true)
}

copy_libs "$DUCKDB_REAL"
copy_libs "$RUNNER"
copy_libs "$ENSABC_EXT"
copy_libs "$DUCKOMO_EXT"

LINKER="$(ls /lib/ld-linux-aarch64.so.1 2>/dev/null || true)"
if [[ -n "$LINKER" ]]; then
    cp -L "$LINKER" "$RUNTIME_DIR/root/lib/ld-linux-aarch64.so.1"
fi

# Host-only preparation: verifies DuckOMO v0.3.5-rc.1 SHA-256, runs host baseline
# 11-table replay & standalone m3_runner, and writes container SQL/JSON inputs.
"$HOST_PYTHON" -B "$ROOT/validation/m3/minimal_runtime.py" prepare \
    --staging-dir "$RUNTIME_DIR/root/opt" \
    --baseline-dir "$RUNTIME_DIR/baseline" \
    --sample-dir "$SAMPLE_DIR" \
    --extension "$ENSABC_EXT" \
    --duckomo "$DUCKOMO_EXT" \
    --runner "$RUNNER"

IMAGE_DIGEST="$(docker image inspect "$BASE_IMAGE" --format '{{index .RepoDigests 0}}' 2>/dev/null || echo "$BASE_IMAGE")"

echo "Running airgapped container ($BASE_IMAGE, --network none)..."
CONTAINER_LOG="$RUNTIME_DIR/container-output.txt"
set +e
docker run --rm \
    --network none \
    --name "$CONTAINER_NAME" \
    --platform linux/arm64 \
    -v "$RUNTIME_DIR/root:/runtime:ro" \
    -v "$RUNTIME_DIR/results:/results" \
    "$BASE_IMAGE" \
    /bin/sh -c '
        set -eu
        export HOME=/results/home
        mkdir -p "$HOME" /results/extensions /results/extensions_rec
        export LD_LIBRARY_PATH=/runtime/lib
        export PATH=/runtime/usr/local/bin:/usr/bin:/bin

        echo "=== Network Isolation Check ==="
        ROUTE_LINES="$(awk "NR > 1 { count++ } END { print count + 0 }" /proc/net/route)"
        echo "NETWORK_NONE_NON_LOOPBACK_ROUTES: $ROUTE_LINES"

        echo "=== Prohibited Tool Absence Check ==="
        for tool in python3 python cdo wgrib wgrib2 gfortran R Rscript; do
            if command -v "$tool" >/dev/null 2>&1; then
                echo "PRESENT: $tool"
            else
                echo "ABSENT: $tool"
            fi
        done

        echo "=== Standalone m3_runner (--mode pair & --mode chain) ==="
        /runtime/usr/local/bin/m3_runner \
            --mode pair \
            --input /runtime/opt/runner_pair_input.json \
            --output /results/runner_pair_output.json
        /runtime/usr/local/bin/m3_runner \
            --mode chain \
            --input /runtime/opt/runner_chain_input.json \
            --output /results/runner_chain_output.json

        echo "=== DuckDB 11-Table End-to-End OM Evolution Replay ==="
        cd /runtime/work
        /runtime/usr/local/bin/duckdb -unsigned -bail -json /results/container_history.duckdb \
            < /runtime/opt/replay_11table.sql \
            > /results/sql_11table_output.json

        echo "=== DuckDB Same-Session Error & Rollback Recovery ==="
        /runtime/usr/local/bin/duckdb -unsigned -json /results/recovery_session.duckdb \
            < /runtime/opt/recovery_session.sql \
            > /results/recovery_output.json \
            2> /results/recovery_errors.txt || true
    ' > "$CONTAINER_LOG" 2>&1
CONTAINER_STATUS=$?
set -e

cat "$CONTAINER_LOG"

"$HOST_PYTHON" -B "$ROOT/validation/m3/minimal_runtime.py" verify \
    --run-dir "$RUN_DIR" \
    --baseline-dir "$RUNTIME_DIR/baseline" \
    --results-dir "$RUNTIME_DIR/results" \
    --container-log "$CONTAINER_LOG" \
    --container-exit "$CONTAINER_STATUS" \
    --image "$IMAGE_DIGEST"

echo "M3 minimal runtime verification: PASS -> $RUN_DIR/minimal-runtime.json"
