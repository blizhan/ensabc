#!/usr/bin/env bash
# Build the M0 DuckDB extension and package it as a loadable extension.
#
# The build is pinned:
#   * the Rust toolchain comes from `rust-toolchain.toml` (1.96.0);
#   * the dependency versions come from the committed `Cargo.lock`;
#   * the packaged metadata declares the DuckDB version, the platform and the
#     ABI type, so a mismatched host refuses to load the extension instead of
#     silently misbehaving.
#
# The packaging helper is a development tool. It is not part of the extension's
# dependency graph or its runtime path.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_DIR="${ENSABC_M0_TARGET_DIR:-/tmp/ensabc-m0-extension-target}"
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
  --target-dir <dir>   Build and packaging output directory
                       (default: $TARGET_DIR, env: ENSABC_M0_TARGET_DIR)
  --check-tools        Only verify the packaging tool revision, then exit
  -h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target-dir)
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

# --- verify the packaging tool revision ---------------------------------------

if [[ ! -d "$EXTENSION_CI_TOOLS" ]]; then
    echo "the extension packaging tool is missing: $EXTENSION_CI_TOOLS" >&2
    echo "set EXTENSION_CI_TOOLS to the extension-ci-tools checkout" >&2
    exit 1
fi

ACTUAL_COMMIT="$(git -C "$EXTENSION_CI_TOOLS" rev-parse HEAD)"
if [[ "$ACTUAL_COMMIT" != "$EXPECTED_EXTENSION_CI_TOOLS_COMMIT" ]]; then
    echo "extension-ci-tools commit mismatch:" >&2
    echo "  expected $EXPECTED_EXTENSION_CI_TOOLS_COMMIT" >&2
    echo "  found    $ACTUAL_COMMIT" >&2
    exit 1
fi
echo "extension-ci-tools revision verified: $ACTUAL_COMMIT"

if [[ "${CHECK_TOOLS_ONLY:-0}" == "1" ]]; then
    exit 0
fi

# --- verify the pinned host ----------------------------------------------------

HOST_VERSION="$(duckdb --version 2>/dev/null || true)"
if [[ -z "$HOST_VERSION" ]]; then
    echo "the duckdb CLI is not available on PATH" >&2
    exit 1
fi
echo "host: $HOST_VERSION"

if [[ "$HOST_VERSION" != *"$DUCKDB_VERSION"* ]]; then
    echo "the host DuckDB version does not match the pinned $DUCKDB_VERSION" >&2
    exit 1
fi

# --- verify the platform --------------------------------------------------------

HOST_ARCH="$(uname -m)"
if [[ "$HOST_ARCH" != "aarch64" ]]; then
    echo "this build script targets linux_arm64, but the host is $HOST_ARCH" >&2
    exit 1
fi

# --- build ---------------------------------------------------------------------

mkdir -p "$TARGET_DIR"
echo "building the extension into $TARGET_DIR"
CARGO_TARGET_DIR="$TARGET_DIR" cargo build \
    --manifest-path "$ROOT/Cargo.toml" \
    --release \
    --locked

LIBRARY="$TARGET_DIR/release/lib${EXTENSION_NAME}.so"
if [[ ! -f "$LIBRARY" ]]; then
    echo "the expected library was not produced: $LIBRARY" >&2
    exit 1
fi

# --- package --------------------------------------------------------------------

OUTPUT="$TARGET_DIR/${EXTENSION_NAME}.duckdb_extension"
python3 "$EXTENSION_CI_TOOLS/scripts/append_extension_metadata.py" \
    --library-file "$LIBRARY" \
    --out-file "$OUTPUT" \
    --extension-name "$EXTENSION_NAME" \
    --duckdb-version "$DUCKDB_VERSION" \
    --extension-version "$EXTENSION_VERSION" \
    --duckdb-platform "$DUCKDB_PLATFORM" \
    --abi-type "$ABI_TYPE"

echo
echo "packaged extension: $OUTPUT"
echo "  duckdb version: $DUCKDB_VERSION"
echo "  platform:       $DUCKDB_PLATFORM"
echo "  abi type:       $ABI_TYPE"
echo "  sha256:         $(sha256sum "$OUTPUT" | cut -d' ' -f1)"
