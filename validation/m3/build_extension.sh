#!/usr/bin/env bash
# Build the M3 DuckDB extension, m1_runner, m2_runner, and m3_runner,
# packaging ensabc_m0_extension.duckdb_extension via pinned extension-ci-tools.
#
# Pinned to:
#   * Rust toolchain (1.96.0)
#   * DuckDB CLI (v1.5.5) / Linux aarch64 (C_STRUCT_UNSTABLE)
#   * Target directory isolated from M0/M1/M2:
#     ENSABC_M3_TARGET_DIR (/tmp/ensabc-m3-extension-target)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_DIR="${ENSABC_M3_TARGET_DIR:-/tmp/ensabc-m3-extension-target}"
OUTPUT_DIR=""
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
                       (default: $TARGET_DIR, env: ENSABC_M3_TARGET_DIR)
  --target-dir <dir>   Cargo build target directory override
                       (defaults to --output-dir if given, else $TARGET_DIR)
  --check-tools        Only verify packaging tool revision, then exit
  -h, --help           Show this help
EOF
}

CHECK_TOOLS_ONLY=0
EXPLICIT_TARGET_DIR=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)
            OUTPUT_DIR="$2"
            if [[ "$EXPLICIT_TARGET_DIR" == "0" ]]; then
                TARGET_DIR="$2"
            fi
            shift 2
            ;;
        --target-dir)
            TARGET_DIR="$2"
            EXPLICIT_TARGET_DIR=1
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

if [[ -z "$OUTPUT_DIR" ]]; then
    OUTPUT_DIR="$TARGET_DIR"
fi

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

mkdir -p "$TARGET_DIR" "$OUTPUT_DIR"
echo "building ensabc-core (m1_runner, m2_runner, m3_runner) and ensabc-duckdb into $TARGET_DIR"

CARGO_TARGET_DIR="$TARGET_DIR" cargo build \
    --manifest-path "$ROOT/Cargo.toml" \
    --release \
    --locked \
    -p ensabc-core \
    --example m1_runner \
    --example m2_runner \
    --example m3_runner

CARGO_TARGET_DIR="$TARGET_DIR" cargo build \
    --manifest-path "$ROOT/Cargo.toml" \
    --release \
    --locked \
    -p ensabc-duckdb

for runner in m1_runner m2_runner m3_runner; do
    RUNNER_BIN="$TARGET_DIR/release/examples/$runner"
    if [[ ! -f "$RUNNER_BIN" ]]; then
        echo "$runner executable not found: $RUNNER_BIN" >&2
        exit 1
    fi
    cp "$RUNNER_BIN" "$OUTPUT_DIR/$runner"
done

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

PYTHON_BIN="python3"
if [[ -x "$ROOT/validation/m2/.venv/bin/python" ]]; then
    PYTHON_BIN="$ROOT/validation/m2/.venv/bin/python"
fi

EXT_OUTPUT="$OUTPUT_DIR/${EXTENSION_NAME}.duckdb_extension"
"$PYTHON_BIN" "$EXTENSION_CI_TOOLS/scripts/append_extension_metadata.py" \
    --library-file "$LIBRARY" \
    --out-file "$EXT_OUTPUT" \
    --extension-name "$EXTENSION_NAME" \
    --duckdb-version "$DUCKDB_VERSION" \
    --extension-version "$EXTENSION_VERSION" \
    --duckdb-platform "$DUCKDB_PLATFORM" \
    --abi-type "$ABI_TYPE"

# --- write build_manifest.json with source & binary SHA-256s ------------------

RUSTC_VER="$(rustc --version)"
GIT_COMMIT="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo 'unknown')"

"$PYTHON_BIN" -B - "$ROOT" "$OUTPUT_DIR" "$EXT_OUTPUT" "$EXTENSION_NAME" \
    "$EXTENSION_VERSION" "$DUCKDB_VERSION" "$DUCKDB_PLATFORM" "$ABI_TYPE" \
    "$RUSTC_VER" "$GIT_COMMIT" "$ACTUAL_COMMIT" <<'PY'
import hashlib, json, os, sys, tempfile
from pathlib import Path

(
    root_s, out_dir_s, ext_out_s, ext_name, ext_ver,
    duckdb_ver, duckdb_plat, abi_type, rustc_ver, git_commit, ci_commit
) = sys.argv[1:]

root = Path(root_s)
out_dir = Path(out_dir_s)
ext_out = Path(ext_out_s)

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

rust_sources: dict[str, str] = {}
for rel_top in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
    fp = root / rel_top
    if fp.is_file():
        rust_sources[rel_top] = sha256_file(fp)
for fp in sorted((root / "crates").rglob("*")):
    if fp.is_file() and fp.suffix in (".rs", ".toml") and not fp.name.startswith(".tmp_"):
        rel = fp.relative_to(root).as_posix()
        rust_sources[rel] = sha256_file(fp)

rust_source_sha256 = hashlib.sha256(
    json.dumps(rust_sources, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()

ext_sha = sha256_file(ext_out)
m1_sha = sha256_file(out_dir / "m1_runner")
m2_sha = sha256_file(out_dir / "m2_runner")
m3_sha = sha256_file(out_dir / "m3_runner")

manifest = {
    "schema_version": "m3.build_manifest.v1",
    "extension_name": ext_name,
    "extension_version": ext_ver,
    "duckdb_version": duckdb_ver,
    "duckdb_platform": duckdb_plat,
    "abi_type": abi_type,
    "toolchain": rustc_ver,
    "git_commit": git_commit,
    "extension_ci_tools_commit": ci_commit,
    "rust_source_files_count": len(rust_sources),
    "rust_source_sha256": rust_source_sha256,
    "rust_sources": rust_sources,
    "binaries": {
        "duckdb_extension": {
            "path": str(ext_out),
            "sha256": ext_sha,
        },
        "m1_runner": {
            "path": str(out_dir / "m1_runner"),
            "sha256": m1_sha,
        },
        "m2_runner": {
            "path": str(out_dir / "m2_runner"),
            "sha256": m2_sha,
        },
        "m3_runner": {
            "path": str(out_dir / "m3_runner"),
            "sha256": m3_sha,
        },
    },
}

manifest_path = out_dir / "build_manifest.json"
fd, tmp_path = tempfile.mkstemp(prefix=".tmp_manifest_", dir=str(out_dir))
try:
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, str(manifest_path))
except Exception:
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    raise

print(f"packaged extension: {ext_out}")
print(f"  duckdb version:    {duckdb_ver}")
print(f"  platform:          {duckdb_plat}")
print(f"  abi type:          {abi_type}")
print(f"  rust_source_sha256:{rust_source_sha256}")
print(f"  extension sha256:  {ext_sha}")
print(f"  m1_runner sha256:  {m1_sha}")
print(f"  m2_runner sha256:  {m2_sha}")
print(f"  m3_runner sha256:  {m3_sha}")
print(f"Build manifest written to {manifest_path}")
PY
