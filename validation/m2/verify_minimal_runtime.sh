#!/usr/bin/env bash
# Verify M2 pure Rust core runner and DuckDB SQL replay with DuckOMO in a minimal runtime container.
#
# Requirements (FR-018, SC-007):
# - No Python, CDO, wgrib/wgrib2, gfortran, R.
# - Isolated container with --network none.
# - Both core m2_runner and DuckDB CLI (loading duckomo + ensabc extensions) execute replay.
# - Writes <run-dir>/minimal-runtime.json.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_DIR="${ENSABC_M2_TARGET_DIR:-/tmp/test-m2-ext}"
ENSABC_EXT="${EXTENSION_PATH:-$TARGET_DIR/ensabc_m0_extension.duckdb_extension}"
DUCKOMO_EXT="${DUCKOMO_PATH:-$ROOT/validation/m2/.cache/v0.3.5-rc.1/duckomo.duckdb_extension}"
RUNNER="${RUNNER_PATH:-$ROOT/target/release/examples/m2_runner}"
BASE_IMAGE="${MINIMAL_RUNTIME_IMAGE:-ubuntu:24.04}"
RUNTIME_DIR="/tmp/ensabc-m2-minimal-runtime"
CONTAINER_NAME="ensabc-m2-minimal-runtime"
RUN_DIR=""
SAMPLE_DIR=""
KEEP_STAGING=0

usage() {
    cat <<EOF
Usage: $0 --run-dir DIR [options]

Options:
  --run-dir <dir>      Run directory
  --sample-dir <dir>   OM sample directory (defaults to <run-dir>/sample)
  --extension <path>   ensabc extension path
  --duckomo <path>     duckomo extension path
  --runner <path>      m2_runner binary path
  --image <name>       Base container image (default: $BASE_IMAGE)
  --keep               Keep staging directory
  -h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-dir)
            RUN_DIR="$2"
            shift 2
            ;;
        --sample-dir)
            SAMPLE_DIR="$2"
            shift 2
            ;;
        --extension)
            ENSABC_EXT="$2"
            shift 2
            ;;
        --duckomo)
            DUCKOMO_EXT="$2"
            shift 2
            ;;
        --runner)
            RUNNER="$2"
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

if [[ ! -f "$ENSABC_EXT" ]]; then
    echo "FAIL: ensabc extension missing: $ENSABC_EXT" >&2
    exit 2
fi

if [[ ! -f "$DUCKOMO_EXT" ]]; then
    echo "FAIL: duckomo extension missing: $DUCKOMO_EXT" >&2
    exit 2
fi

if [[ ! -f "$RUNNER" ]]; then
    echo "FAIL: m2_runner missing: $RUNNER" >&2
    exit 2
fi

DUCKDB_BIN="$(command -v duckdb || true)"
if [[ -z "$DUCKDB_BIN" ]]; then
    echo "FAIL: duckdb CLI is not available" >&2
    exit 2
fi

if [[ -z "$SAMPLE_DIR" ]]; then
    if [[ -f "$RUN_DIR/sample/wb2-ifs-ens-t2m-20200101T00Z.om" ]]; then
        SAMPLE_DIR="$RUN_DIR/sample"
    else
        SAMPLE_DIR="$ROOT/validation/m2/data/om_sample"
    fi
fi
if [[ ! -f "$SAMPLE_DIR/wb2-ifs-ens-t2m-20200101T00Z.om" ]]; then
    echo "FAIL: OM sample missing in $SAMPLE_DIR" >&2
    exit 2
fi

rm -rf "$RUNTIME_DIR"
mkdir -p "$RUNTIME_DIR/root/usr/local/bin" "$RUNTIME_DIR/root/opt" "$RUNTIME_DIR/root/om" "$RUNTIME_DIR/root/lib" "$RUNTIME_DIR/results"

echo "Staging minimal runtime in $RUNTIME_DIR..."

cp "$DUCKDB_BIN" "$RUNTIME_DIR/root/usr/local/bin/duckdb"
cp "$RUNNER" "$RUNTIME_DIR/root/usr/local/bin/m2_runner"
cp "$ENSABC_EXT" "$RUNTIME_DIR/root/opt/ensabc_m0_extension.duckdb_extension"
cp "$DUCKOMO_EXT" "$RUNTIME_DIR/root/opt/duckomo.duckdb_extension"
cp "$SAMPLE_DIR"/*.om "$RUNTIME_DIR/root/om/"

# Copy dynamic libraries
copy_libs() {
    local bin="$1"
    while read -r library; do
        [[ -f "$library" ]] || continue
        cp -L "$library" "$RUNTIME_DIR/root/lib/$(basename "$library")"
    done < <(ldd "$bin" 2>/dev/null | awk '{print $3}' | grep -E '^/' || true)
}

copy_libs "$DUCKDB_BIN"
copy_libs "$RUNNER"
copy_libs "$ENSABC_EXT"
copy_libs "$DUCKOMO_EXT"

LINKER="$(ls /lib/ld-linux-aarch64.so.1 2>/dev/null || true)"
if [[ -n "$LINKER" ]]; then
    cp -L "$LINKER" "$RUNTIME_DIR/root/lib/ld-linux-aarch64.so.1"
fi

# Prepare runner synthetic input
cat <<'EOF' > "$RUNTIME_DIR/root/opt/runner_input.json"
{
  "mode": "match",
  "task_id": "min_runtime_test",
  "weights": [1.0, 1.0],
  "threshold": 1.0,
  "prev_scenarios": [{"cluster_id": 0, "mean_features": [280.0, 281.0]}],
  "curr_scenarios": [{"cluster_id": 0, "mean_features": [280.2, 281.1]}]
}
EOF

# Prepare full offline replay & same-session recovery SQL outside container
python3 "$ROOT/validation/m2/minimal_runtime.py" prepare --run-dir "$RUN_DIR" --directory "$RUNTIME_DIR/root/opt"

# Confirm the frozen target release before entering the offline runtime.
python3 - "$ROOT" "$DUCKOMO_EXT" <<'PY'
import hashlib, json, pathlib, sys
root, extension = map(pathlib.Path, sys.argv[1:])
expected = json.loads((root / 'validation/m2/fixtures/duckomo_releases.json').read_text())['target']['extension_sha256']
assert hashlib.sha256(extension.read_bytes()).hexdigest() == expected, 'DuckOMO hash mismatch'
PY

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

IMAGE_DIGEST="$(docker image inspect "$BASE_IMAGE" --format '{{index .RepoDigests 0}}' 2>/dev/null || echo "$BASE_IMAGE")"

echo "Running isolated container ($BASE_IMAGE)..."
docker run --rm \
    --network none \
    --name "$CONTAINER_NAME" \
    --platform linux/arm64 \
    -v "$RUNTIME_DIR/root:/runtime:ro" \
    -v "$RUNTIME_DIR/results:/results" \
    "$BASE_IMAGE" \
    /bin/sh -c '
        set -e
        export HOME=/results/home
        mkdir -p "$HOME"
        export LD_LIBRARY_PATH=/runtime/lib
        export PATH=/runtime/usr/local/bin:/usr/bin:/bin
        echo "=== Tool Availability Inside Minimal Runtime ==="
        for tool in python3 python cdo wgrib wgrib2 gfortran R Rscript; do
            if command -v "$tool" >/dev/null 2>&1; then
                echo "PRESENT: $tool"
            else
                echo "ABSENT:  $tool"
            fi
        done
        echo "=== Running m2_runner ==="
        /runtime/usr/local/bin/m2_runner --mode match --input /runtime/opt/runner_input.json --output /results/runner_output.json
        echo "=== Running DuckDB Replay SQL ==="
        /runtime/usr/local/bin/duckdb -unsigned -bail -json :memory: < /runtime/opt/replay.sql > /results/sql_output.json
        /runtime/usr/local/bin/duckdb -unsigned -json :memory: < /runtime/opt/recovery.sql > /results/recovery_output.json 2> /results/recovery_errors.txt || true
    ' > "$RUNTIME_DIR/container-output.txt" 2>&1 && CONTAINER_STATUS=0 || CONTAINER_STATUS=$?

CONTAINER_OUTPUT="$RUNTIME_DIR/container-output.txt"
cat "$CONTAINER_OUTPUT"

STATUS="pass"
REASONS=()

if [[ "$CONTAINER_STATUS" != "0" ]]; then
    STATUS="fail"
    REASONS+=("Container run failed with code $CONTAINER_STATUS")
fi

REF_PRESENT=()
for tool in python3 python cdo wgrib wgrib2 gfortran R Rscript; do
    if ! grep -Fxq "ABSENT:  $tool" "$CONTAINER_OUTPUT"; then
        STATUS="fail"
        REASONS+=("$tool was not absent in minimal runtime")
        REF_PRESENT+=("$tool")
    fi
done

# Verify runner output
if [[ ! -f "$RUNTIME_DIR/results/runner_output.json" ]]; then
    STATUS="fail"
    REASONS+=("runner_output.json was not generated")
fi

# Verify sql output
if [[ ! -f "$RUNTIME_DIR/results/sql_output.json" ]]; then
    STATUS="fail"
    REASONS+=("sql_output.json was not generated")
fi

FULL_REPLAY_OK="false"
RECOVERY_OK="false"
if [[ "$STATUS" == "pass" ]]; then
    if python3 "$ROOT/validation/m2/minimal_runtime.py" verify --run-dir "$RUN_DIR" --directory "$RUNTIME_DIR/results"; then
        FULL_REPLAY_OK="true"
        RECOVERY_OK="true"
    else
        STATUS="fail"
        REASONS+=("Full replay, runner output, or same-session recovery verification failed")
    fi
fi

TOOLS_JSON="[]"
if [[ ${#REF_PRESENT[@]} -gt 0 ]]; then
    TOOLS_JSON="[$(printf '"%s",' "${REF_PRESENT[@]}" | sed 's/,$//')]"
fi

cat <<EOF > "$RUN_DIR/minimal-runtime.json"
{
  "schema_version": "m2.minimal_runtime.v1",
  "status": "$STATUS",
  "reference_tools_present": $TOOLS_JSON,
  "method": "isolated container",
  "image": "$IMAGE_DIGEST",
  "core_runner": {
    "status": "$STATUS"
  },
  "sql_replay": {
    "status": "$STATUS",
    "full_replay": $FULL_REPLAY_OK,
    "same_session_recovery": $RECOVERY_OK
  },
  "created_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "Minimal runtime verification: $STATUS"
for r in "${REASONS[@]:-}"; do
    echo "  reason: $r"
done

if [[ "$KEEP_STAGING" == "0" ]]; then
    rm -rf "$RUNTIME_DIR/root"
fi

[[ "$STATUS" == "pass" ]]
