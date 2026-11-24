#!/usr/bin/env bash
# Bootstrap and verify the M2 validation environment.
#
# Follows the evidence and validation contracts:
# - Verifies Linux aarch64, Rust 1.96.0, DuckDB 1.5.5, Python 3.13.13, Docker
# - Reuses M0 data/reference virtual environments if available
# - Prepares isolated OM venv with omfiles 1.2.0, numpy 2.5.4, numcodecs 0.17.0
# - Downloads/verifies pinned DuckOMO releases (v0.3.5-rc.1 target, v0.3.4 baseline)
# - Verifies SHA-256 hashes of ZIP, manifest, SHA256SUMS, and extracted duckomo.duckdb_extension
# - Supports --check and --json
# - Exits 0 on success, 2 on missing prerequisites/environment mismatch

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
M0_VALIDATION_DIR="$ROOT/validation/m0"
M2_VALIDATION_DIR="$ROOT/validation/m2"
FIXTURES_DIR="$M2_VALIDATION_DIR/fixtures"

DATA_VENV="${ENSABC_M0_DATA_VENV:-/tmp/ensabc-m0-data/.venv}"
REFERENCE_VENV="${ENSABC_M0_REFERENCE_VENV:-/tmp/ensabc-m0-baseline}"
OM_VENV="${ENSABC_M2_OM_VENV:-$M2_VALIDATION_DIR/.venv}"
CACHE_DIR="${ENSABC_M2_CACHE_DIR:-$M2_VALIDATION_DIR/.cache}"
TARGET_DIR="${ENSABC_M2_TARGET_DIR:-/tmp/ensabc-m2-extension-target}"

REQUIRED_RUSTC="rustc 1.96.0"
REQUIRED_DUCKDB="v1.5.5"
REQUIRED_PYTHON="3.13.13"
REQUIRED_EXTENSION_CI_TOOLS_COMMIT="b777c70d30942cca5bef62d6d4fa23a13362f398"
EXTENSION_CI_TOOLS="${EXTENSION_CI_TOOLS:-/home/blizhan/repo/github/duckomo/extension-ci-tools}"

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --check               Only verify the environment; do not create or download
  --cache-dir DIR       Directory for downloaded packages and DuckOMO archives
  --om-venv DIR         Path for the isolated OM virtual environment
  --json                Print the summary as JSON
  -h, --help            Show this help
EOF
}

CHECK_ONLY=0
AS_JSON=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --check)
            CHECK_ONLY=1
            shift
            ;;
        --cache-dir)
            CACHE_DIR="$2"
            shift 2
            ;;
        --om-venv)
            OM_VENV="$2"
            shift 2
            ;;
        --json)
            AS_JSON=1
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

FAILURES=()

record_failure() {
    FAILURES+=("$1")
    echo "FAIL: $1" >&2
}

if [[ "$CHECK_ONLY" == "0" ]]; then
    mkdir -p "$CACHE_DIR"
    if ! command -v uv >/dev/null 2>&1 && ! command -v python3 >/dev/null 2>&1; then
        record_failure "python3 or uv is required to create the OM Python environment"
    else
        if [[ ! -x "$OM_VENV/bin/python" ]]; then
            echo "creating OM environment in $OM_VENV"
            if command -v uv >/dev/null 2>&1; then
                uv venv --python "$REQUIRED_PYTHON" "$OM_VENV" >/dev/null
                uv pip sync --python "$OM_VENV/bin/python" --require-hashes \
                    "$M2_VALIDATION_DIR/requirements-om.lock" >/dev/null
            else
                python3 -m venv "$OM_VENV"
                "$OM_VENV/bin/pip" install --require-hashes \
                    -r "$M2_VALIDATION_DIR/requirements-om.lock" >/dev/null
            fi
        fi
    fi
fi

# --- verify tool versions and capabilities ------------------------------------

RUSTC_VERSION="$(rustc --version 2>/dev/null || echo 'unavailable')"
CARGO_VERSION="$(cargo --version 2>/dev/null || echo 'unavailable')"
DUCKDB_RAW_VERSION="$(duckdb --version 2>/dev/null || echo 'unavailable')"
DUCKDB_CLI_VERSION="$(duckdb --version 2>/dev/null | awk '{print $1}' | tr -d ',' || echo 'unavailable')"
DOCKER_VERSION="$(docker --version 2>/dev/null || echo 'unavailable')"
UNAME_MACHINE="$(uname -m)"
EXTENSION_CI_TOOLS_COMMIT="$(git -C "$EXTENSION_CI_TOOLS" rev-parse HEAD 2>/dev/null || echo 'unavailable')"

if [[ "$RUSTC_VERSION" != "$REQUIRED_RUSTC"* ]]; then
    record_failure "rustc must be $REQUIRED_RUSTC, found '$RUSTC_VERSION'"
fi
if [[ "$DUCKDB_CLI_VERSION" != "$REQUIRED_DUCKDB" ]]; then
    record_failure "the duckdb CLI must be $REQUIRED_DUCKDB, found '$DUCKDB_CLI_VERSION'"
fi
if [[ "$UNAME_MACHINE" != "aarch64" ]]; then
    record_failure "the M2 target platform is linux aarch64, found '$UNAME_MACHINE'"
fi
if [[ "$DOCKER_VERSION" == "unavailable" ]]; then
    record_failure "docker is required for minimal runtime isolation verification"
fi
if [[ "$EXTENSION_CI_TOOLS_COMMIT" != "$REQUIRED_EXTENSION_CI_TOOLS_COMMIT" ]]; then
    record_failure "extension-ci-tools must be at $REQUIRED_EXTENSION_CI_TOOLS_COMMIT, found '$EXTENSION_CI_TOOLS_COMMIT'"
fi

# OM environment stack check
OM_STACK="unavailable"
if [[ -x "$OM_VENV/bin/python" ]]; then
    OM_STACK="$("$OM_VENV/bin/python" -c "
import importlib.metadata, numpy, numcodecs
v_om = importlib.metadata.version('omfiles')
v_np = numpy.__version__
v_nc = numcodecs.__version__
print(f'omfiles={v_om} numpy={v_np} numcodecs={v_nc}')
" 2>/dev/null || echo 'unavailable')"
    if [[ "$OM_STACK" != "omfiles=1.2.0 numpy=2.5.4 numcodecs=0.17.0" ]]; then
        record_failure "the OM environment must have omfiles 1.2.0, numpy 2.5.4, numcodecs 0.17.0, found '$OM_STACK'"
    fi
else
    if [[ "$CHECK_ONLY" == "1" ]]; then
        record_failure "the OM environment is missing at $OM_VENV"
    fi
fi

# Verify DuckOMO releases
RELEASES_SPEC="$FIXTURES_DIR/duckomo_releases.json"
if [[ ! -f "$RELEASES_SPEC" ]]; then
    record_failure "missing DuckOMO releases specification at $RELEASES_SPEC"
else
    # Python script to check/download and verify DuckOMO releases
    python3 -B - <<PY || record_failure "DuckOMO releases verification failed"
import json, sys, os, urllib.request, hashlib, zipfile

with open("$RELEASES_SPEC", "r", encoding="utf-8") as f:
    spec = json.load(f)

cache_dir = "$CACHE_DIR"
check_only = ($CHECK_ONLY == 1)

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

for role, rel in spec.items():
    ver = rel["version"]
    zip_name = rel["zip_filename"]
    zip_path = os.path.join(cache_dir, zip_name)
    expected_zip_hash = rel["zip_sha256"]
    expected_ext_hash = rel["extension_sha256"]

    # If not present and not check-only, download
    if not os.path.exists(zip_path):
        if check_only:
            print(f"FAIL: missing archive {zip_name} in {cache_dir}", file=sys.stderr)
            sys.exit(1)
        os.makedirs(cache_dir, exist_ok=True)
        print(f"Downloading {rel['url']} -> {zip_path}")
        urllib.request.urlretrieve(rel["url"], zip_path)

    actual_zip_hash = sha256_file(zip_path)
    if actual_zip_hash != expected_zip_hash:
        print(f"FAIL: {zip_name} sha256 mismatch! Expected {expected_zip_hash}, got {actual_zip_hash}", file=sys.stderr)
        sys.exit(1)

    ext_dir = os.path.join(cache_dir, ver)
    ext_path = os.path.join(ext_dir, "duckomo.duckdb_extension")
    manifest_path = os.path.join(ext_dir, "manifest.json")

    if not check_only:
        os.makedirs(ext_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(ext_dir)
    elif not os.path.exists(ext_path) or not os.path.exists(manifest_path):
        print(f"FAIL: extracted release files missing in {ext_dir} (run setup_env.sh without --check)", file=sys.stderr)
        sys.exit(1)

    actual_ext_hash = sha256_file(ext_path)
    if actual_ext_hash != expected_ext_hash:
        print(f"FAIL: {ver} duckomo.duckdb_extension sha256 mismatch! Expected {expected_ext_hash}, got {actual_ext_hash}", file=sys.stderr)
        sys.exit(1)

    with open(manifest_path, "r", encoding="utf-8") as mf:
        pkg_manifest = json.load(mf)
    if (
        pkg_manifest.get("tag") != ver
        or pkg_manifest.get("source_commit") != rel["source_commit"]
        or pkg_manifest.get("duckdb_version") != rel["duckdb_version"]
        or pkg_manifest.get("platform") != rel["platform"]
        or pkg_manifest.get("extension_sha256") != expected_ext_hash
    ):
        print(f"FAIL: {ver} extracted manifest.json identity mismatch: {pkg_manifest}", file=sys.stderr)
        sys.exit(1)

    print(f"Verified DuckOMO {role} ({ver}): ZIP, manifest, and extension hashes valid.")
PY
fi

# --- report ---------------------------------------------------------------------

STATUS="$([[ ${#FAILURES[@]} -eq 0 ]] && echo pass || echo fail)"

if [[ "$AS_JSON" == "1" ]]; then
    printf '{'
    printf '"status": "%s", ' "$STATUS"
    printf '"platform": "%s", ' "$UNAME_MACHINE"
    printf '"rustc": "%s", ' "$RUSTC_VERSION"
    printf '"cargo": "%s", ' "$CARGO_VERSION"
    printf '"duckdb_cli": "%s", ' "$DUCKDB_RAW_VERSION"
    printf '"docker": "%s", ' "$DOCKER_VERSION"
    printf '"extension_ci_tools_commit": "%s", ' "$EXTENSION_CI_TOOLS_COMMIT"
    printf '"om_stack": "%s", ' "$OM_STACK"
    printf '"cache_dir": "%s", ' "$CACHE_DIR"
    printf '"target_dir": "%s", ' "$TARGET_DIR"
    printf '"failures": ['
    first=1
    for failure in "${FAILURES[@]:-}"; do
        [[ -z "$failure" ]] && continue
        [[ $first -eq 0 ]] && printf ', '
        printf '"%s"' "$failure"
        first=0
    done
    printf ']\n'
    printf '}\n'
else
    echo
    echo "M2 environment"
    echo "  platform:              $UNAME_MACHINE"
    echo "  rustc:                 $RUSTC_VERSION"
    echo "  cargo:                 $CARGO_VERSION"
    echo "  duckdb CLI:            $DUCKDB_RAW_VERSION"
    echo "  docker:                $DOCKER_VERSION"
    echo "  extension-ci-tools:    $EXTENSION_CI_TOOLS_COMMIT"
    echo "  om stack:              $OM_STACK"
    echo "  cache dir:             $CACHE_DIR"
    echo "  target dir:            $TARGET_DIR"
    if [[ ${#FAILURES[@]} -eq 0 ]]; then
        echo "  status:                pass"
    else
        echo "  status:                fail (${#FAILURES[@]} problem(s))"
    fi
fi

if [[ ${#FAILURES[@]} -ne 0 ]]; then
    exit 2
fi

exit 0
