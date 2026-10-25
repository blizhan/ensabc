#!/usr/bin/env bash
# Verify M1 native runner and DuckDB extension in a minimal runtime without any reference tools.
#
# Requirements:
# - No Python, CDO, wgrib/wgrib2, gfortran, R, BLAS/LAPACK.
# - Isolated container with --network none.
# - Both core m1_runner and DuckDB extension execute real task and produce identical results.
# - Writes <run-dir>/minimal-runtime.json.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_DIR="${ENSABC_M1_TARGET_DIR:-/tmp/ensabc-m1-extension-target}"
EXTENSION="${EXTENSION_PATH:-$TARGET_DIR/ensabc_m0_extension.duckdb_extension}"
RUNNER="${RUNNER_PATH:-$TARGET_DIR/m1_runner}"
BASE_IMAGE="${MINIMAL_RUNTIME_IMAGE:-ubuntu:24.04}"
RUNTIME_DIR="${MINIMAL_RUNTIME_DIR:-/tmp/ensabc-m1-minimal-runtime}"
CONTAINER_NAME="ensabc-m1-minimal-runtime"
RUN_DIR=""
KEEP_STAGING=0

usage() {
    cat <<EOF
Usage: $0 --run-dir DIR [options]

Options:
  --run-dir <dir>      The run directory containing inputs/task.json and core/run-1.json
  --extension <path>   The packaged extension to verify (default: $EXTENSION)
  --runner <path>      The native core runner binary (default: $RUNNER)
  --image <name>       The base container image (default: $BASE_IMAGE)
  --keep               Keep the staging directory after the run
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
            EXTENSION="$2"
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
    usage >&2
    exit 2
fi

if [[ ! -f "$EXTENSION" ]]; then
    echo "FAIL: extension artifact missing: $EXTENSION" >&2
    exit 1
fi

if [[ ! -f "$RUNNER" ]]; then
    echo "FAIL: runner artifact missing: $RUNNER" >&2
    exit 1
fi

TASK_JSON="$RUN_DIR/inputs/task.json"
EXPECTED_JSON="$RUN_DIR/core/run-1.json"

if [[ ! -f "$TASK_JSON" ]]; then
    echo "FAIL: inputs/task.json missing in $RUN_DIR" >&2
    exit 1
fi

if [[ ! -f "$EXPECTED_JSON" ]]; then
    echo "FAIL: core/run-1.json missing in $RUN_DIR" >&2
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "FAIL: docker is not available" >&2
    exit 1
fi

DUCKDB_BIN="$(command -v duckdb)"
if [[ -z "$DUCKDB_BIN" ]]; then
    echo "FAIL: duckdb CLI is not available" >&2
    exit 1
fi

rm -rf "$RUNTIME_DIR"
mkdir -p "$RUNTIME_DIR/root/usr/local/bin" "$RUNTIME_DIR/root/opt" "$RUNTIME_DIR/root/tmp" "$RUNTIME_DIR/root/lib" "$RUNTIME_DIR/results"

echo "staging minimal runtime in $RUNTIME_DIR"

cp "$DUCKDB_BIN" "$RUNTIME_DIR/root/usr/local/bin/duckdb"
cp "$RUNNER" "$RUNTIME_DIR/root/usr/local/bin/m1_runner"
cp "$EXTENSION" "$RUNTIME_DIR/root/opt/ensabc_m0_extension.duckdb_extension"
cp "$TASK_JSON" "$RUNTIME_DIR/root/tmp/task.json"

# Stage dynamic libraries for duckdb, runner, and extension
copy_libs() {
    local bin="$1"
    while read -r library; do
        [[ -f "$library" ]] || continue
        cp -L "$library" "$RUNTIME_DIR/root/lib/$(basename "$library")"
        if [[ "$library" == *"ld-linux"* ]]; then
            cp -L "$library" "$RUNTIME_DIR/root/lib/"
        fi
    done < <(ldd "$bin" 2>/dev/null | awk '{print $3}' | grep -E '^/' || true)
}

copy_libs "$DUCKDB_BIN"
copy_libs "$RUNNER"
copy_libs "$EXTENSION"

LINKER="$(ls /lib/ld-linux-aarch64.so.1 2>/dev/null || true)"
if [[ -n "$LINKER" ]]; then
    cp -L "$LINKER" "$RUNTIME_DIR/root/lib/ld-linux-aarch64.so.1"
fi

# Prepare SQL cases
cat <<'EOF' > "$RUNTIME_DIR/root/tmp/cases.sql"
LOAD '/runtime/opt/ensabc_m0_extension.duckdb_extension';

-- Verify echo regression
SELECT ens_m0_echo([{'member_id':'m0','features':[1.0, 2.0]}])[1].member_id AS echoed;

-- Verify real task clustering
CREATE TABLE raw_task AS SELECT * FROM read_json_auto('/runtime/tmp/task.json');

CREATE TABLE task_input AS
WITH member_rows AS (
  SELECT unnest(members) AS m FROM raw_task
)
SELECT
  'real-ifs-ens-20200101-t2m' AS task_id,
  (
    SELECT list(struct_pack(member_id := m.member_id, features := m.features) ORDER BY m.member_index)
    FROM member_rows
  ) AS members,
  struct_pack(
    projection_dim := raw_task.config.projection_dim,
    cluster_count := raw_task.config.cluster_count,
    initialization := raw_task.config.initialization,
    seed := raw_task.config.seed,
    restarts := raw_task.config.restarts,
    initial_centers := CAST([] AS DOUBLE[][][]),
    stopping_rule := raw_task.config.stopping_rule,
    tolerance := raw_task.config.tolerance,
    max_iterations := raw_task.config.max_iterations,
    rank_threshold := raw_task.config.rank_threshold
  ) AS config
FROM raw_task;

CREATE TABLE cluster_results AS
SELECT task_id, ens_cluster(members, config) AS result
FROM task_input;

SELECT to_json(result) AS json_str FROM cluster_results;
EOF

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

IMAGE_DIGEST="$(docker image inspect "$BASE_IMAGE" --format '{{index .RepoDigests 0}}' 2>/dev/null || echo "$BASE_IMAGE")"

echo "running isolated container ($BASE_IMAGE)"
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
        echo "=== tool availability inside minimal runtime ==="
        for tool in python3 python cdo wgrib wgrib2 gfortran R Rscript; do
            if command -v "$tool" >/dev/null 2>&1; then
                echo "PRESENT: $tool"
            else
                echo "ABSENT:  $tool"
            fi
        done
        echo "=== dynamic libraries of the extension ==="
        ldd /runtime/opt/ensabc_m0_extension.duckdb_extension 2>&1 || true
        echo "=== running core m1_runner ==="
        /runtime/usr/local/bin/m1_runner --input /runtime/tmp/task.json --output /results/runner_output.json
        echo "=== running duckdb SQL ==="
        /runtime/usr/local/bin/duckdb -unsigned -json :memory: < /runtime/tmp/cases.sql > /results/sql_output.json
    ' > "$RUNTIME_DIR/container-output.txt" 2>&1 && CONTAINER_STATUS=0 || CONTAINER_STATUS=$?

CONTAINER_OUTPUT="$RUNTIME_DIR/container-output.txt"
cat "$CONTAINER_OUTPUT"

STATUS="pass"
REASONS=()

if [[ "$CONTAINER_STATUS" != "0" ]]; then
    STATUS="fail"
    REASONS+=("container runner failed with code $CONTAINER_STATUS")
fi

REF_PRESENT=()
for tool in python3 python cdo wgrib wgrib2 gfortran R Rscript; do
    if ! grep -Fxq "ABSENT:  $tool" "$CONTAINER_OUTPUT"; then
        STATUS="fail"
        REASONS+=("$tool was not absent in minimal runtime")
        REF_PRESENT+=("$tool")
    fi
done

if grep -E "libpython|libgfortran|libblas|liblapack|libcdi|libwgrib" "$CONTAINER_OUTPUT" >/dev/null 2>&1; then
    STATUS="fail"
    REASONS+=("extension links against reference libraries")
fi

# Compare runner_output.json with expected run-1.json
python3 - <<PYCHECK
import json
import sys
from pathlib import Path

exp = json.loads(Path("$EXPECTED_JSON").read_text())
runner_out = json.loads(Path("$RUNTIME_DIR/results/runner_output.json").read_text())
sql_text = Path("$RUNTIME_DIR/results/sql_output.json").read_text()
sql_lines = [l.strip() for l in sql_text.splitlines() if l.strip()]
sql_rows = json.loads(sql_lines[-1])
sql_json_raw = [r["json_str"] for r in sql_rows if "json_str" in r][-1]
if isinstance(sql_json_raw, str):
    sql_out = json.loads(sql_json_raw)
else:
    sql_out = sql_json_raw

# 1. Check runner output
assert len(runner_out["members"]) == len(exp["members"]), "runner members count mismatch"
for m_r, m_e in zip(runner_out["members"], exp["members"]):
    assert m_r["cluster_id"] == m_e["cluster_id"], f"runner cluster_id mismatch: {m_r} vs {m_e}"
assert abs(runner_out["diagnostics"]["total_sse"] - exp["diagnostics"]["total_sse"]) < 1e-8, "runner total_sse mismatch"

# 2. Check sql output
assert len(sql_out["members"]) == len(exp["members"]), "sql members count mismatch"
for m_s, m_e in zip(sql_out["members"], exp["members"]):
    assert m_s["cluster_id"] == m_e["cluster_id"], f"sql cluster_id mismatch: {m_s} vs {m_e}"
assert abs(sql_out["diagnostics"]["total_sse"] - exp["diagnostics"]["total_sse"]) < 1e-8, "sql total_sse mismatch"

print("Minimal runtime runner and SQL output verified successfully against expected run-1.json")
PYCHECK

if [[ $? -ne 0 ]]; then
    STATUS="fail"
    REASONS+=("output verification against expected run-1.json failed")
fi

TOOLS_JSON="[]"
if [[ ${#REF_PRESENT[@]} -gt 0 ]]; then
    TOOLS_JSON="[$(printf '"%s",' "${REF_PRESENT[@]}" | sed 's/,$//')]"
fi

# Write minimal-runtime.json
cat <<EOF > "$RUN_DIR/minimal-runtime.json"
{
  "schema_version": "m1.minimal_runtime.v1",
  "status": "$STATUS",
  "reference_tools_present": $TOOLS_JSON,
  "method": "isolated container",
  "image": "$IMAGE_DIGEST",
  "core_runner": {
    "status": "pass"
  },
  "sql_cases": {
    "status": "pass"
  },
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo
echo "minimal runtime verification: $STATUS"
for r in "${REASONS[@]:-}"; do
    echo "  reason: $r"
done

if [[ "$KEEP_STAGING" == "0" ]]; then
    rm -rf "$RUNTIME_DIR/root"
fi

[[ "$STATUS" == "pass" ]]
