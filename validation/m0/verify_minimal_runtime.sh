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

cat > "$RUNTIME_DIR/root/tmp/cases.sql" <<'SQL'
.bail off
LOAD '/runtime/opt/ensabc_m0_extension.duckdb_extension';

-- A legal task must roundtrip.
SELECT 'positive' AS stage, ens_m0_echo([{'member_id': 'ok', 'features': [1.0, 2.0]}]) AS echoed;

-- An illegal task must fail with a located reason.
SELECT 'illegal-null' AS stage, ens_m0_echo([{'member_id': 'ok', 'features': [NULL]}]) AS echoed;

-- The same session must still accept a legal task.
SELECT 'recovery' AS stage, ens_m0_echo([{'member_id': 'ok', 'features': [3.0]}]) AS echoed;

-- A top-level NULL must fail as well.
SELECT 'illegal-top-level' AS stage, ens_m0_echo(NULL::STRUCT(member_id VARCHAR, features DOUBLE[])[]) AS echoed;

SELECT 'recovery-2' AS stage, ens_m0_echo([{'member_id': 'ok', 'features': [4.0]}]) AS echoed;
SQL

# --- run the isolated environment ----------------------------------------------

echo "pulling $BASE_IMAGE"
docker pull "$BASE_IMAGE" >/dev/null

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

echo "running the minimal runtime"
docker run --rm \
    --network none \
    --name "$CONTAINER_NAME" \
    --platform linux/arm64 \
    -v "$RUNTIME_DIR/root:/runtime:ro" \
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
        /runtime/usr/local/bin/duckdb -unsigned :memory: -init /dev/null < /runtime/tmp/cases.sql
    ' > "$RUNTIME_DIR/minimal-runtime-output.txt" 2>&1 || true

OUTPUT="$RUNTIME_DIR/minimal-runtime-output.txt"
cat "$OUTPUT"

# --- judge the result -----------------------------------------------------------

STATUS="pass"
REASONS=()

if ! grep -q "ABSENT:  python3" "$OUTPUT"; then
    STATUS="fail"
    REASONS+=("python3 is present in the minimal runtime")
fi
if ! grep -q "ABSENT:  cdo" "$OUTPUT"; then
    STATUS="fail"
    REASONS+=("cdo is present in the minimal runtime")
fi
if ! grep -q "ABSENT:  wgrib" "$OUTPUT"; then
    STATUS="fail"
    REASONS+=("wgrib is present in the minimal runtime")
fi
if ! grep -q "ABSENT:  wgrib2" "$OUTPUT"; then
    STATUS="fail"
    REASONS+=("wgrib2 is present in the minimal runtime")
fi
if ! grep -q "positive" "$OUTPUT"; then
    STATUS="fail"
    REASONS+=("the positive case did not run")
fi
if ! grep -q "recovery" "$OUTPUT"; then
    STATUS="fail"
    REASONS+=("the recovery case did not run")
fi
if ! grep -q "the feature value is NULL" "$OUTPUT"; then
    STATUS="fail"
    REASONS+=("the nested NULL case was not rejected with the expected reason")
fi
if ! grep -q "the members list of task row 0 is NULL" "$OUTPUT"; then
    STATUS="fail"
    REASONS+=("the top-level NULL case was not rejected with the expected reason")
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
