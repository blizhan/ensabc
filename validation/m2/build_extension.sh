#!/usr/bin/env bash
# Build the M2 DuckDB extension, m1_runner, and m2_runner, packaging it as a loadable extension.
#
# Pinned to:
#   * Rust toolchain (1.96.0)
#   * DuckDB CLI (v1.5.5) / Linux aarch64
#   * Target directory isolated from M0/M1: ENSABC_M2_TARGET_DIR (/tmp/ensabc-m2-extension-target)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_DIR="${ENSABC_M2_TARGET_DIR:-/tmp/ensabc-m2-extension-target}"
EXTENSION_NAME="ensabc_m0_extension"
DUCKDB_VERSION="v1.5.5"
DUCKDB_PLATFORM="linux_arm64"
ABI_TYPE="C_STRUCT_UNSTABLE"
EXTENSION_VERSION="0.1.0"

EXTENSION_CI_TOOLS="${EXTENSION_CI_TOOLS:-/home/blizhan/repo/github/duckomo/extension-ci-tools}"
EXPECTED_EXTENSION_CI_TOOLS_COMMIT="b777c70d30942cca5bef62d6d4fa23a13362f398"

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --output-dir <dir>   Build and packaging output directory
                       (default: $TARGET_DIR, env: ENSABC_M2_TARGET_DIR)
  --target-dir <dir>   Alias for --output-dir
  --check-tools        Only verify packaging tool revision, then exit
  -h, --help           Show this help
EOF
}

CHECK_TOOLS_ONLY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir|--target-dir)
            TARGET_DIR="$2"
            shift 2
            ;;
        --check-tools)
            CHECK_TOOLS_ONLY=1
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

# --- verify packaging tool ----------------------------------------------------

if [[ ! -d "$EXTENSION_CI_TOOLS" ]]; then
    echo "the extension packaging tool is missing: $EXTENSION_CI_TOOLS" >&2
    echo "set EXTENSION_CI_TOOLS to the extension-ci-tools checkout" >&2
    exit 2
fi

ACTUAL_COMMIT="$(git -C "$EXTENSION_CI_TOOLS" rev-parse HEAD)"
if [[ "$ACTUAL_COMMIT" != "$EXPECTED_EXTENSION_CI_TOOLS_COMMIT" ]]; then
    echo "extension-ci-tools commit mismatch:" >&2
    echo "  expected $EXPECTED_EXTENSION_CI_TOOLS_COMMIT" >&2
    echo "  found    $ACTUAL_COMMIT" >&2
    exit 1
fi
echo "extension-ci-tools revision verified: $ACTUAL_COMMIT"

if [[ "${CHECK_TOOLS_ONLY}" == "1" ]]; then
    exit 0
fi

# --- verify host & platform ---------------------------------------------------

HOST_VERSION="$(duckdb --version 2>/dev/null || true)"
if [[ -z "$HOST_VERSION" ]]; then
    echo "the duckdb CLI is not available on PATH" >&2
    exit 2
fi
echo "host: $HOST_VERSION"

if [[ "$HOST_VERSION" != *"$DUCKDB_VERSION"* ]]; then
    echo "the host DuckDB version does not match the pinned $DUCKDB_VERSION" >&2
    exit 1
fi

HOST_ARCH="$(uname -m)"
if [[ "$HOST_ARCH" != "aarch64" ]]; then
    echo "this build script targets linux_arm64, but the host is $HOST_ARCH" >&2
    exit 2
fi

# --- build --------------------------------------------------------------------

mkdir -p "$TARGET_DIR"
echo "building ensabc-core m1_runner, m2_runner, and ensabc-duckdb into $TARGET_DIR"

CARGO_TARGET_DIR="$TARGET_DIR" cargo build \
    --manifest-path "$ROOT/Cargo.toml" \
    --release \
    --locked \
    -p ensabc-core \
    --example m1_runner

CARGO_TARGET_DIR="$TARGET_DIR" cargo build \
    --manifest-path "$ROOT/Cargo.toml" \
    --release \
    --locked \
    -p ensabc-core \
    --example m2_runner

CARGO_TARGET_DIR="$TARGET_DIR" cargo build \
    --manifest-path "$ROOT/Cargo.toml" \
    --release \
    --locked \
    -p ensabc-duckdb

M1_RUNNER_BIN="$TARGET_DIR/release/examples/m1_runner"
if [[ ! -f "$M1_RUNNER_BIN" ]]; then
    echo "m1_runner executable not found: $M1_RUNNER_BIN" >&2
    exit 1
fi
cp "$M1_RUNNER_BIN" "$TARGET_DIR/m1_runner"

M2_RUNNER_BIN="$TARGET_DIR/release/examples/m2_runner"
if [[ ! -f "$M2_RUNNER_BIN" ]]; then
    echo "m2_runner executable not found: $M2_RUNNER_BIN" >&2
    exit 1
fi
cp "$M2_RUNNER_BIN" "$TARGET_DIR/m2_runner"

LIBRARY="$TARGET_DIR/release/lib${EXTENSION_NAME}.so"
if [[ ! -f "$LIBRARY" ]]; then
    echo "the expected library was not produced: $LIBRARY" >&2
    exit 1
fi

# Verify exported C API symbol
if ! nm -D "$LIBRARY" | grep -q "ensabc_m0_extension_init_c_api"; then
    echo "Missing expected entry point symbol ensabc_m0_extension_init_c_api in $LIBRARY" >&2
    exit 1
fi
echo "verified exported C API symbol ensabc_m0_extension_init_c_api"

# --- package ------------------------------------------------------------------

OUTPUT="$TARGET_DIR/${EXTENSION_NAME}.duckdb_extension"
python3 "$EXTENSION_CI_TOOLS/scripts/append_extension_metadata.py" \
    --library-file "$LIBRARY" \
    --out-file "$OUTPUT" \
    --extension-name "$EXTENSION_NAME" \
    --duckdb-version "$DUCKDB_VERSION" \
    --extension-version "$EXTENSION_VERSION" \
    --duckdb-platform "$DUCKDB_PLATFORM" \
    --abi-type "$ABI_TYPE"

EXT_SHA="$(sha256sum "$OUTPUT" | cut -d' ' -f1)"
M1_SHA="$(sha256sum "$TARGET_DIR/m1_runner" | cut -d' ' -f1)"
M2_SHA="$(sha256sum "$TARGET_DIR/m2_runner" | cut -d' ' -f1)"

# Record build manifest
cat <<EOF > "$TARGET_DIR/build_manifest.json"
{
  "extension_name": "$EXTENSION_NAME",
  "extension_version": "$EXTENSION_VERSION",
  "duckdb_version": "$DUCKDB_VERSION",
  "duckdb_platform": "$DUCKDB_PLATFORM",
  "abi_type": "$ABI_TYPE",
  "toolchain": "$(rustc --version)",
  "git_commit": "$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")",
  "binaries": {
    "duckdb_extension": {
      "path": "$OUTPUT",
      "sha256": "$EXT_SHA"
    },
    "m1_runner": {
      "path": "$TARGET_DIR/m1_runner",
      "sha256": "$M1_SHA"
    },
    "m2_runner": {
      "path": "$TARGET_DIR/m2_runner",
      "sha256": "$M2_SHA"
    }
  }
}
EOF

echo "packaged extension: $OUTPUT"
echo "  duckdb version: $DUCKDB_VERSION"
echo "  platform:       $DUCKDB_PLATFORM"
echo "  abi type:       $ABI_TYPE"
echo "  extension sha256: $EXT_SHA"
echo "  m1_runner sha256: $M1_SHA"
echo "  m2_runner sha256: $M2_SHA"
echo "Build manifest written to $TARGET_DIR/build_manifest.json"
