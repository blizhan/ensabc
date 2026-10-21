#!/usr/bin/env bash
# Verify the M0 extension in a minimal runtime without any reference tool.
#
# The validation contract requires more than an empty PATH or a link audit:
# the extension must actually load and run inside an environment that has no
# Python reference stack, no `cdo` and no `wgrib`/`wgrib2`. This script
# therefore builds a real isolated environment and executes the positive and
# the error-recovery cases inside it.
#
# The isolated environment is a Debian/Ubuntu container that receives only:
#   * the DuckDB CLI and the shared libraries it needs;
#   * the packaged M0 extension;
#   * the SQL cases.
#
# The container has no network access and no package manager step, so no
# reference tool can be present.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_DIR="${ENSABC_M0_TARGET_DIR:-/tmp/ensabc-m0-extension-target}"
EXTENSION="${EXTENSION_PATH:-$TARGET_DIR/ensabc_m0_extension.duckdb_extension}"
BASE_IMAGE="${MINIMAL_RUNTIME_IMAGE:-ubuntu:24.04}"
RUNTIME_DIR="${MINIMAL_RUNTIME_DIR:-/tmp/ensabc-m0-minimal-runtime}"
CONTAINER_NAME="ensabc-m0-minimal-runtime"

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --extension <path>   The packaged extension to verify
                       (default: $EXTENSION)
  --image <name>       The base image for the isolated runtime
                       (default: $BASE_IMAGE)
  --keep               Keep the staging directory after the run
  -h, --help           Show this help
EOF
}

KEEP_STAGING=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --extension)
            EXTENSION="$2"
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

if [[ ! -f "$EXTENSION" ]]; then
    echo "FAIL: the extension artifact is missing: $EXTENSION" >&2
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "FAIL: docker is not available; the minimal runtime cannot be built" >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "FAIL: the docker daemon is not reachable" >&2
    exit 1
fi

DUCKDB_BIN="$(command -v duckdb)"
if [[ -z "$DUCKDB_BIN" ]]; then
    echo "FAIL: the duckdb CLI is not available" >&2
    exit 1
fi

rm -rf "$RUNTIME_DIR"
mkdir -p "$RUNTIME_DIR/root/usr/local/bin" "$RUNTIME_DIR/root/opt" "$RUNTIME_DIR/root/tmp"

echo "staging the minimal runtime in $RUNTIME_DIR"

# --- stage the DuckDB CLI and the libraries it needs --------------------------

cp "$DUCKDB_BIN" "$RUNTIME_DIR/root/usr/local/bin/duckdb"

# The CLI's shared libraries are copied individually, so the isolated runtime
# receives the minimum and not the host's userland.
mkdir -p "$RUNTIME_DIR/root/lib"
while read -r library; do
    [[ -f "$library" ]] || continue
    target="$RUNTIME_DIR/root/lib/$(basename "$library")"
    cp -L "$library" "$target"
    # The interpreter itself must be present as well.
    if [[ "$library" == *"ld-linux"* ]]; then
        cp -L "$library" "$RUNTIME_DIR/root/lib/"
    fi
done < <(ldd "$DUCKDB_BIN" 2>/dev/null | awk '{print $3}' | grep -E '^/' || true)

# The extension's own libraries must be present too.
for library in $(ldd "$EXTENSION" 2>/dev/null | awk '{print $3}' | grep -E '^/' || true); do
    [[ -f "$library" ]] || continue
    cp -L "$library" "$RUNTIME_DIR/root/lib/$(basename "$library")"
done

# The linker is needed to resolve the copied libraries.
LINKER="$(ls /lib/ld-linux-aarch64.so.1 2>/dev/null || true)"
if [[ -n "$LINKER" ]]; then
    cp -L "$LINKER" "$RUNTIME_DIR/root/lib/ld-linux-aarch64.so.1"
fi

# --- stage the extension and the SQL cases ------------------------------------

cp "$EXTENSION" "$RUNTIME_DIR/root/opt/ensabc_m0_extension.duckdb_extension"

# Python only prepares/checks files on the host. The container receives SQL
# and a shell runner, never the Python reference stack.
python3 "$ROOT/validation/m0/minimal_runtime_cases.py" prepare "$RUNTIME_DIR/root/tmp"
mkdir -p "$RUNTIME_DIR/results"

# --- run the isolated environment ----------------------------------------------

if ! docker image inspect "$BASE_IMAGE" >/dev/null 2>&1; then
    echo "pulling $BASE_IMAGE"
    docker pull "$BASE_IMAGE" >/dev/null
fi

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

echo "running the minimal runtime"
docker run --rm \
    --network none \
    --name "$CONTAINER_NAME" \
    --platform linux/arm64 \
    -v "$RUNTIME_DIR/root:/runtime:ro" \
    -v "$RUNTIME_DIR/results:/results" \
    "$BASE_IMAGE" \
    /bin/sh -c '
        set -e
        export LD_LIBRARY_PATH=/runtime/lib
        export PATH=/runtime/usr/local/bin:/usr/bin:/bin
        echo "=== tool availability inside the minimal runtime ==="
        for tool in python3 python cdo wgrib wgrib2 gfortran R Rscript; do
            if command -v "$tool" >/dev/null 2>&1; then
                echo "PRESENT: $tool"
            else
                echo "ABSENT:  $tool"
            fi
        done
        echo "=== dynamic libraries of the extension ==="
        ldd /runtime/opt/ensabc_m0_extension.duckdb_extension 2>&1 || true
        echo "=== the SQL cases ==="
        /runtime/usr/local/bin/duckdb -unsigned :memory: -init /dev/null < /runtime/tmp/cases.sql > /results/cases.txt 2>&1 || true
        cat /results/cases.txt
        /runtime/usr/local/bin/duckdb -unsigned :memory: -init /dev/null < /runtime/tmp/concurrent.sql > /results/concurrent-1.txt 2>&1 &
        first=$!
        /runtime/usr/local/bin/duckdb -unsigned :memory: -init /dev/null < /runtime/tmp/concurrent.sql > /results/concurrent-2.txt 2>&1 &
        second=$!
        wait "$first"
        wait "$second"
    ' > "$RUNTIME_DIR/minimal-runtime-output.txt" 2>&1 && CONTAINER_STATUS=0 || CONTAINER_STATUS=$?

OUTPUT="$RUNTIME_DIR/minimal-runtime-output.txt"
cat "$OUTPUT"

# --- judge the result -----------------------------------------------------------

STATUS="pass"
REASONS=()

if [[ "$CONTAINER_STATUS" != "0" ]]; then
    STATUS="fail"
    REASONS+=("the isolated runner failed with exit code $CONTAINER_STATUS")
fi
for tool in python3 python cdo wgrib wgrib2 gfortran R Rscript; do
    if ! grep -Fxq "ABSENT:  $tool" "$OUTPUT"; then
        STATUS="fail"
        REASONS+=("$tool absence was not demonstrated in the minimal runtime")
    fi
done
if ! python3 "$ROOT/validation/m0/minimal_runtime_cases.py" check "$RUNTIME_DIR/results"; then
    STATUS="fail"
    REASONS+=("the exact roundtrip, all-category rejection, recovery, batch or concurrency checks failed")
fi

# The extension must not link against any reference tool.
if grep -E "libpython|libgfortran|libcdi|libwgrib" "$OUTPUT" >/dev/null 2>&1; then
    STATUS="fail"
    REASONS+=("the extension links against a reference tool")
fi

echo
echo "minimal runtime verification: $STATUS"
for reason in "${REASONS[@]:-}"; do
    [[ -n "$reason" ]] && echo "  reason: $reason"
done
echo "  output: $OUTPUT"

if [[ "$KEEP_STAGING" == "0" ]]; then
    rm -rf "$RUNTIME_DIR/root"
fi

[[ "$STATUS" == "pass" ]]
