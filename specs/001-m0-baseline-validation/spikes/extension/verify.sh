#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR=/tmp/ensabc-m0-extension-target
EXTENSION_CI_TOOLS="${EXTENSION_CI_TOOLS:-/home/blizhan/repo/github/duckomo/extension-ci-tools}"
EXPECTED_EXTENSION_CI_TOOLS_COMMIT=b777c70d30942cca5bef62d6d4fa23a13362f398

actual_commit="$(git -C "$EXTENSION_CI_TOOLS" rev-parse HEAD)"
if [[ "$actual_commit" != "$EXPECTED_EXTENSION_CI_TOOLS_COMMIT" ]]; then
    echo "extension-ci-tools commit mismatch: $actual_commit" >&2
    exit 1
fi

mkdir -p "$TARGET_DIR"
CARGO_TARGET_DIR="$TARGET_DIR" cargo build --manifest-path "$ROOT/Cargo.toml" --release --locked
python3 "$EXTENSION_CI_TOOLS/scripts/append_extension_metadata.py" \
    --library-file "$TARGET_DIR/release/libensabc_m0_extension.so" \
    --out-file "$TARGET_DIR/ensabc_m0_extension.duckdb_extension" \
    --extension-name ensabc_m0_extension \
    --duckdb-version v1.5.5 \
    --extension-version 0.1.0 \
    --duckdb-platform linux_arm64 \
    --abi-type C_STRUCT_UNSTABLE
set +e
duckdb -unsigned :memory: -f "$ROOT/verify.sql" > "$TARGET_DIR/verify-output.txt" 2>&1
duckdb_status=$?
set -e
cat "$TARGET_DIR/verify-output.txt"

python3 - "$TARGET_DIR/verify-output.txt" "$duckdb_status" <<'PY'
import sys
from pathlib import Path

output = Path(sys.argv[1]).read_text()
assert sys.argv[2] == "1", f"expected one query error, got CLI status {sys.argv[2]}"
assert output.count("Error:") == 1, "unexpected additional query errors"
assert "feature 0 is NULL or non-finite" in output
for marker in (
    "PASS: task_id stays outside",
    "BLOCKED: top-level NULL returned NULL",
    "BLOCKED: echo accepts unequal feature lengths",
    "OBSERVED: TRY returned NULL",
    "PASS: 2100 varying rows",
    "PASS: one list with 3000 members",
    "PASS: same CLI session completed valid calls",
):
    assert marker in output, f"missing observation: {marker}"
print("SPIKE VERIFIED: observations reproduced; known contract failures remain")
PY
