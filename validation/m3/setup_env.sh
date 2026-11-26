#!/usr/bin/env bash
# Bootstrap and verify the M3 scenario evolution validation environment.
#
# Follows contracts/validation-evidence.md (T002):
# - Verifies Linux aarch64, Rust 1.96.0, DuckDB v1.5.5, Python 3.13.13, Docker,
#   and pinned extension-ci-tools commit
# - Reuses and verifies M0/M1 data & reference virtual environments
# - Reuses and verifies M2 isolated OM venv (requirements-om.lock: omfiles 1.2.0,
#   numpy 2.5.4, numcodecs 0.17.0)
# - Verifies SHA-256 hashes of dual DuckOMO releases (v0.3.5-rc.1 target and v0.3.4
#   baseline) in validation/m2/.cache/
# - Checks (and in non --check mode restores) M0 frozen sample payload
#   validation/m0/fixtures/real_ifs_ens_20200101_t2m_data.json for M1 regression
# - Supports --check and --json
# - Exits 0 on success, 2 on missing prerequisites/environment mismatch

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
M0_VALIDATION_DIR="$ROOT/validation/m0"
M2_VALIDATION_DIR="$ROOT/validation/m2"
M2_FIXTURES_DIR="$M2_VALIDATION_DIR/fixtures"

DATA_VENV="${ENSABC_M0_DATA_VENV:-/tmp/ensabc-m0-data/.venv}"
REFERENCE_VENV="${ENSABC_M0_REFERENCE_VENV:-/tmp/ensabc-m0-baseline}"
OM_VENV="${ENSABC_M2_OM_VENV:-$M2_VALIDATION_DIR/.venv}"
CACHE_DIR="${ENSABC_M2_CACHE_DIR:-$M2_VALIDATION_DIR/.cache}"
TARGET_DIR="${ENSABC_M3_TARGET_DIR:-/tmp/ensabc-m3-extension-target}"

REQUIRED_RUSTC="rustc 1.96.0"
REQUIRED_DUCKDB="v1.5.5"
REQUIRED_PYTHON="3.13.13"
REQUIRED_EXTENSION_CI_TOOLS_COMMIT="b777c70d30942cca5bef62d6d4fa23a13362f398"
EXTENSION_CI_TOOLS="${EXTENSION_CI_TOOLS:-/home/blizhan/repo/github/duckomo/extension-ci-tools}"

M0_SAMPLE_MANIFEST="$M0_VALIDATION_DIR/fixtures/real_ifs_ens_20200101_t2m_manifest.json"
M0_SAMPLE_DATA="$M0_VALIDATION_DIR/fixtures/real_ifs_ens_20200101_t2m_data.json"
M0_SPIKE_CSV="$ROOT/specs/001-m0-baseline-validation/spikes/data/sample.csv"

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --check               Only verify the environment; do not create, extract, or download
  --cache-dir DIR       Directory for cached DuckOMO archives (default: $CACHE_DIR)
  --om-venv DIR         Path for the isolated OM virtual environment (default: $OM_VENV)
  --json                Print the verification summary as JSON
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
    if [[ "$AS_JSON" == "0" ]]; then
        echo "FAIL: $1" >&2
    fi
}

# --- bootstrap environments and M0 sample if not in check-only mode -----------

if [[ "$CHECK_ONLY" == "0" ]]; then
    mkdir -p "$CACHE_DIR"
    if [[ ! -x "$DATA_VENV/bin/python" ]] || [[ ! -x "$REFERENCE_VENV/bin/python" ]]; then
        if ! command -v uv >/dev/null 2>&1; then
            record_failure "uv is required to bootstrap M0/M1 data and reference environments"
        else
            if [[ ! -x "$DATA_VENV/bin/python" ]]; then
                [[ "$AS_JSON" == "0" ]] && echo "creating M0 data environment in $DATA_VENV"
                uv venv --python "$REQUIRED_PYTHON" "$DATA_VENV" >/dev/null
                uv pip sync --python "$DATA_VENV/bin/python" \
                    "$M0_VALIDATION_DIR/requirements-data.lock" >/dev/null
            fi
            if [[ ! -x "$REFERENCE_VENV/bin/python" ]]; then
                [[ "$AS_JSON" == "0" ]] && echo "creating M0 reference environment in $REFERENCE_VENV"
                uv venv --python "$REQUIRED_PYTHON" "$REFERENCE_VENV" >/dev/null
                uv pip sync --python "$REFERENCE_VENV/bin/python" \
                    "$M0_VALIDATION_DIR/requirements-reference.lock" >/dev/null
            fi
        fi
    fi

    if [[ ! -x "$OM_VENV/bin/python" ]]; then
        if command -v uv >/dev/null 2>&1; then
            [[ "$AS_JSON" == "0" ]] && echo "creating M2 OM environment in $OM_VENV"
            uv venv --python "$REQUIRED_PYTHON" "$OM_VENV" >/dev/null
            uv pip sync --python "$OM_VENV/bin/python" --require-hashes \
                "$M2_VALIDATION_DIR/requirements-om.lock" >/dev/null
        elif command -v python3 >/dev/null 2>&1; then
            [[ "$AS_JSON" == "0" ]] && echo "creating M2 OM environment in $OM_VENV"
            python3 -m venv "$OM_VENV"
            "$OM_VENV/bin/pip" install --require-hashes \
                -r "$M2_VALIDATION_DIR/requirements-om.lock" >/dev/null
        else
            record_failure "uv or python3 is required to create the OM Python environment"
        fi
    fi

    if [[ ! -f "$M0_SAMPLE_DATA" ]] && [[ -x "$DATA_VENV/bin/python" ]]; then
        [[ "$AS_JSON" == "0" ]] && echo "restoring M0 frozen sample payload for M1 regression..."
        if [[ -f "$M0_SPIKE_CSV" ]]; then
            "$DATA_VENV/bin/python" "$M0_VALIDATION_DIR/extract_sample.py" --rebuild >/dev/null
        else
            "$DATA_VENV/bin/python" "$M0_VALIDATION_DIR/extract_sample.py" --rebuild --from-source >/dev/null
        fi
    fi
fi

# --- verify host toolchain and platform ---------------------------------------

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
    record_failure "the M3 target platform is linux aarch64, found '$UNAME_MACHINE'"
fi
if [[ "$DOCKER_VERSION" == "unavailable" ]]; then
    record_failure "docker is required for minimal runtime isolation verification"
fi
if [[ "$EXTENSION_CI_TOOLS_COMMIT" != "$REQUIRED_EXTENSION_CI_TOOLS_COMMIT" ]]; then
    record_failure "extension-ci-tools must be at $REQUIRED_EXTENSION_CI_TOOLS_COMMIT, found '$EXTENSION_CI_TOOLS_COMMIT'"
fi

# --- verify M0/M1 reference & data environments -------------------------------

REFERENCE_STACK="unavailable"
if [[ -x "$REFERENCE_VENV/bin/python" ]]; then
    REFERENCE_STACK="$("$REFERENCE_VENV/bin/python" -c '
import numpy, scipy, sklearn
print(f"{numpy.__version__} {scipy.__version__} {sklearn.__version__}")
' 2>/dev/null || echo 'unavailable')"
    if [[ "$REFERENCE_STACK" != "2.5.4 1.18.1 1.9.1" ]]; then
        record_failure "the M0/M1 reference stack must be numpy 2.5.4 / scipy 1.18.1 / scikit-learn 1.9.1, found '$REFERENCE_STACK'"
    fi
else
    record_failure "the M0/M1 reference environment is missing at $REFERENCE_VENV"
fi

DATA_STACK="unavailable"
if [[ -x "$DATA_VENV/bin/python" ]]; then
    DATA_STACK="$("$DATA_VENV/bin/python" -c '
import xarray, zarr, gcsfs
print(f"{xarray.__version__} {zarr.__version__} {gcsfs.__version__}")
' 2>/dev/null || echo 'unavailable')"
    if [[ "$DATA_STACK" == "unavailable" ]]; then
        record_failure "the M0/M1 data environment failed import checks at $DATA_VENV"
    fi
else
    record_failure "the M0/M1 data environment is missing at $DATA_VENV"
fi

# --- verify M2 OM environment (Python 3.13.13 + locked packages) --------------

OM_PYTHON_VERSION="unavailable"
OM_STACK="unavailable"
if [[ -x "$OM_VENV/bin/python" ]]; then
    OM_PYTHON_VERSION="$("$OM_VENV/bin/python" --version 2>&1 | awk '{print $2}' || echo 'unavailable')"
    if [[ "$OM_PYTHON_VERSION" != "$REQUIRED_PYTHON" ]]; then
        record_failure "the OM Python version must be $REQUIRED_PYTHON, found '$OM_PYTHON_VERSION'"
    fi
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
    record_failure "the OM environment is missing at $OM_VENV"
fi

# --- verify M0 sample payload for M1 regression -------------------------------

M0_SAMPLE_STATUS="missing"
if [[ -f "$M0_SAMPLE_MANIFEST" ]] && [[ -f "$M0_SAMPLE_DATA" ]] && [[ -x "$OM_VENV/bin/python" ]]; then
    if "$OM_VENV/bin/python" -B - "$M0_SAMPLE_MANIFEST" "$M0_SAMPLE_DATA" <<'PY'
import hashlib, json, sys
manifest_path, data_path = sys.argv[1], sys.argv[2]
with open(manifest_path, "r", encoding="utf-8") as mf:
    expected = json.load(mf)["data_sha256"]
h = hashlib.sha256()
with open(data_path, "rb") as df:
    while chunk := df.read(65536):
        h.update(chunk)
actual = h.hexdigest()
if actual != expected:
    print(f"M0 sample sha256 mismatch: expected {expected}, got {actual}", file=sys.stderr)
    sys.exit(1)
PY
    then
        M0_SAMPLE_STATUS="verified"
    else
        M0_SAMPLE_STATUS="mismatch"
        record_failure "M0 frozen sample payload hash mismatch at $M0_SAMPLE_DATA"
    fi
else
    record_failure "M0 frozen sample payload missing at $M0_SAMPLE_DATA (run setup_env.sh without --check)"
fi

# --- verify dual DuckOMO releases ---------------------------------------------

RELEASES_SPEC="$M2_FIXTURES_DIR/duckomo_releases.json"
PYTHON_BIN="python3"
if [[ -x "$OM_VENV/bin/python" ]]; then
    PYTHON_BIN="$OM_VENV/bin/python"
fi

if [[ ! -f "$RELEASES_SPEC" ]]; then
    record_failure "missing DuckOMO releases specification at $RELEASES_SPEC"
else
    DUCKOMO_OUT=""
    if DUCKOMO_OUT="$("$PYTHON_BIN" -B - "$RELEASES_SPEC" "$CACHE_DIR" "$CHECK_ONLY" <<'PY'
import hashlib, json, os, sys, urllib.request, zipfile

spec_path, cache_dir, check_only_str = sys.argv[1], sys.argv[2], sys.argv[3]
check_only = (check_only_str == "1")

with open(spec_path, "r", encoding="utf-8") as f:
    spec = json.load(f)

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(65536):
            h.update(chunk)
    return h.hexdigest()

for role, rel in spec.items():
    ver = rel["version"]
    zip_name = rel["zip_filename"]
    zip_path = os.path.join(cache_dir, zip_name)
    expected_zip_hash = rel["zip_sha256"]
    expected_ext_hash = rel["extension_sha256"]

    if not os.path.exists(zip_path):
        if check_only:
            print(f"missing archive {zip_name} in {cache_dir}", file=sys.stderr)
            sys.exit(1)
        os.makedirs(cache_dir, exist_ok=True)
        urllib.request.urlretrieve(rel["url"], zip_path)

    actual_zip_hash = sha256_file(zip_path)
    if actual_zip_hash != expected_zip_hash:
        print(f"{zip_name} sha256 mismatch: expected {expected_zip_hash}, got {actual_zip_hash}", file=sys.stderr)
        sys.exit(1)

    ext_dir = os.path.join(cache_dir, ver)
    ext_path = os.path.join(ext_dir, "duckomo.duckdb_extension")
    manifest_path = os.path.join(ext_dir, "manifest.json")

    if not check_only:
        os.makedirs(ext_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(ext_dir)
    elif not os.path.exists(ext_path) or not os.path.exists(manifest_path):
        print(f"extracted release files missing in {ext_dir} (run setup_env.sh without --check)", file=sys.stderr)
        sys.exit(1)

    actual_ext_hash = sha256_file(ext_path)
    if actual_ext_hash != expected_ext_hash:
        print(f"{ver} duckomo.duckdb_extension sha256 mismatch: expected {expected_ext_hash}, got {actual_ext_hash}", file=sys.stderr)
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
        print(f"{ver} extracted manifest.json identity mismatch: {pkg_manifest}", file=sys.stderr)
        sys.exit(1)

    print(f"Verified DuckOMO {role} ({ver}): ZIP, manifest, and extension hashes valid.")
PY
    )"; then
        if [[ "$AS_JSON" == "0" ]]; then
            printf '%s\n' "$DUCKOMO_OUT"
        fi
    else
        record_failure "DuckOMO releases verification failed"
    fi
fi

# --- report -------------------------------------------------------------------

STATUS="$([[ ${#FAILURES[@]} -eq 0 ]] && echo pass || echo fail)"

if [[ "$AS_JSON" == "1" ]]; then
    "$PYTHON_BIN" -B - "$STATUS" "$UNAME_MACHINE" "$RUSTC_VERSION" "$CARGO_VERSION" \
        "$DUCKDB_RAW_VERSION" "$DOCKER_VERSION" "$EXTENSION_CI_TOOLS_COMMIT" \
        "$OM_PYTHON_VERSION" "$OM_STACK" "$REFERENCE_STACK" "$DATA_STACK" \
        "$M0_SAMPLE_STATUS" "$CACHE_DIR" "$TARGET_DIR" "${FAILURES[@]:-}" <<'PY'
import json, sys
(
    status, platform, rustc_v, cargo_v, duckdb_v, docker_v, ext_commit,
    om_py, om_stack, ref_stack, data_stack, m0_sample, cache_dir, target_dir,
    *failures
) = sys.argv[1:]
failures = [f for f in failures if f]
print(json.dumps({
    "schema_version": "m3.environment.v1",
    "status": status,
    "platform": platform,
    "rustc": rustc_v,
    "cargo": cargo_v,
    "duckdb_cli": duckdb_v,
    "docker": docker_v,
    "extension_ci_tools_commit": ext_commit,
    "om_python": om_py,
    "om_stack": om_stack,
    "reference_stack": ref_stack,
    "data_stack": data_stack,
    "m0_sample_payload": m0_sample,
    "cache_dir": cache_dir,
    "target_dir": target_dir,
    "failures": failures,
}, indent=2))
PY
else
    echo
    echo "M3 environment"
    echo "  platform:              $UNAME_MACHINE"
    echo "  rustc:                 $RUSTC_VERSION"
    echo "  cargo:                 $CARGO_VERSION"
    echo "  duckdb CLI:            $DUCKDB_RAW_VERSION"
    echo "  docker:                $DOCKER_VERSION"
    echo "  extension-ci-tools:    $EXTENSION_CI_TOOLS_COMMIT"
    echo "  om python:             $OM_PYTHON_VERSION ($OM_STACK)"
    echo "  reference stack:       $REFERENCE_STACK"
    echo "  data stack:            $DATA_STACK"
    echo "  m0 sample payload:     $M0_SAMPLE_STATUS"
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
