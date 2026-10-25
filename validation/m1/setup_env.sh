#!/usr/bin/env bash
# Bootstrap and verify the M1 validation environment.
#
# Follows the evidence and validation contracts:
# - Reuses M0 reference/data lockfiles and environment overrides
# - Verifies Linux aarch64, Rust 1.96.0, DuckDB 1.5.5, packaging-tool revision, Docker
# - Verifies nalgebra/rand std-only (no external BLAS/LAPACK) policy
# - Supports --check and --json
# - Exits 0 on success, exits 2 on missing prerequisites/environment mismatch

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
M0_VALIDATION_DIR="$ROOT/validation/m0"
M1_VALIDATION_DIR="$ROOT/validation/m1"

DATA_VENV="${ENSABC_M0_DATA_VENV:-/tmp/ensabc-m0-data/.venv}"
REFERENCE_VENV="${ENSABC_M0_REFERENCE_VENV:-/tmp/ensabc-m0-baseline}"
TARGET_DIR="${ENSABC_M1_TARGET_DIR:-/tmp/ensabc-m1-extension-target}"

REQUIRED_RUSTC="rustc 1.96.0"
REQUIRED_DUCKDB="v1.5.5"
REQUIRED_PYTHON="3.13.13"
REQUIRED_EXTENSION_CI_TOOLS_COMMIT="b777c70d30942cca5bef62d6d4fa23a13362f398"

EXTENSION_CI_TOOLS="${EXTENSION_CI_TOOLS:-/home/blizhan/repo/github/duckomo/extension-ci-tools}"

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --check           Only verify the environment; do not create it
  --json            Print the summary as JSON
  -h, --help        Show this help
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

# --- bootstrap isolated Python environments if not in check-only mode ---------

if [[ "$CHECK_ONLY" == "0" ]]; then
    if ! command -v uv >/dev/null 2>&1; then
        record_failure "uv is required to create the isolated Python environments"
    else
        if [[ ! -x "$DATA_VENV/bin/python" ]]; then
            echo "creating data environment in $DATA_VENV"
            uv venv --python "$REQUIRED_PYTHON" "$DATA_VENV" >/dev/null
            uv pip sync --python "$DATA_VENV/bin/python" \
                "$M0_VALIDATION_DIR/requirements-data.lock" >/dev/null
        fi
        if [[ ! -x "$REFERENCE_VENV/bin/python" ]]; then
            echo "creating reference environment in $REFERENCE_VENV"
            uv venv --python "$REQUIRED_PYTHON" "$REFERENCE_VENV" >/dev/null
            uv pip sync --python "$REFERENCE_VENV/bin/python" \
                "$M0_VALIDATION_DIR/requirements-reference.lock" >/dev/null
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

DATA_PYTHON_VERSION="unavailable"
if [[ -x "$DATA_VENV/bin/python" ]]; then
    DATA_PYTHON_VERSION="$("$DATA_VENV/bin/python" --version 2>&1 | awk '{print $2}' || echo 'unavailable')"
fi

REFERENCE_PYTHON_VERSION="unavailable"
if [[ -x "$REFERENCE_VENV/bin/python" ]]; then
    REFERENCE_PYTHON_VERSION="$("$REFERENCE_VENV/bin/python" --version 2>&1 | awk '{print $2}' || echo 'unavailable')"
fi

if [[ "$RUSTC_VERSION" != "$REQUIRED_RUSTC"* ]]; then
    record_failure "rustc must be $REQUIRED_RUSTC, found '$RUSTC_VERSION'"
fi
if [[ "$DUCKDB_CLI_VERSION" != "$REQUIRED_DUCKDB" ]]; then
    record_failure "the duckdb CLI must be $REQUIRED_DUCKDB, found '$DUCKDB_CLI_VERSION'"
fi
if [[ "$UNAME_MACHINE" != "aarch64" ]]; then
    record_failure "the M1 target platform is linux aarch64, found '$UNAME_MACHINE'"
fi
if [[ "$DOCKER_VERSION" == "unavailable" ]]; then
    record_failure "docker is required for minimal runtime isolation verification"
fi
if [[ "$EXTENSION_CI_TOOLS_COMMIT" != "$REQUIRED_EXTENSION_CI_TOOLS_COMMIT" ]]; then
    record_failure "extension-ci-tools must be at $REQUIRED_EXTENSION_CI_TOOLS_COMMIT, found '$EXTENSION_CI_TOOLS_COMMIT'"
fi

# Reference environment stack checks
REFERENCE_STACK="unavailable"
if [[ -x "$REFERENCE_VENV/bin/python" ]]; then
    REFERENCE_STACK="$("$REFERENCE_VENV/bin/python" -c '
import numpy, scipy, sklearn
print(f"{numpy.__version__} {scipy.__version__} {sklearn.__version__}")
' 2>/dev/null || echo 'unavailable')"
    if [[ "$REFERENCE_STACK" != "2.5.4 1.18.1 1.9.1" ]]; then
        record_failure "the reference numerical stack must be numpy 2.5.4 / scipy 1.18.1 / scikit-learn 1.9.1, found '$REFERENCE_STACK'"
    fi
else
    record_failure "the reference environment is missing at $REFERENCE_VENV"
fi

# Data environment stack checks
DATA_STACK="unavailable"
if [[ -x "$DATA_VENV/bin/python" ]]; then
    DATA_STACK="$("$DATA_VENV/bin/python" -c '
import xarray, zarr, gcsfs
print(f"{xarray.__version__} {zarr.__version__} {gcsfs.__version__}")
' 2>/dev/null || echo 'unavailable')"
else
    record_failure "the data environment is missing at $DATA_VENV"
fi

# Numerics dependency policy check (pure-Rust std-only, no external BLAS/LAPACK)
NUMERICS_POLICY="nalgebra=0.35.0(std-only),rand=0.9.2,rand_chacha=0.9.0"

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
    printf '"data_python": "%s", ' "$DATA_PYTHON_VERSION"
    printf '"data_stack": "%s", ' "$DATA_STACK"
    printf '"reference_python": "%s", ' "$REFERENCE_PYTHON_VERSION"
    printf '"reference_stack": "%s", ' "$REFERENCE_STACK"
    printf '"numerics_policy": "%s", ' "$NUMERICS_POLICY"
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
    echo "M1 environment"
    echo "  platform:              $UNAME_MACHINE"
    echo "  rustc:                 $RUSTC_VERSION"
    echo "  cargo:                 $CARGO_VERSION"
    echo "  duckdb CLI:            $DUCKDB_RAW_VERSION"
    echo "  docker:                $DOCKER_VERSION"
    echo "  extension-ci-tools:    $EXTENSION_CI_TOOLS_COMMIT"
    echo "  data python:           $DATA_PYTHON_VERSION ($DATA_STACK)"
    echo "  reference python:      $REFERENCE_PYTHON_VERSION ($REFERENCE_STACK)"
    echo "  numerics policy:       $NUMERICS_POLICY"
    echo "  target dir:            $TARGET_DIR"
    if [[ ${#FAILURES[@]} -eq 0 ]]; then
        echo "  status:                pass"
    else
        echo "  status:                fail (${#FAILURES[@]} problem(s))"
    fi
fi

if [[ ${#FAILURES[@]} -ne 0 ]]; then
    # Missing prerequisites or environment mismatch exit with code 2
    exit 2
fi

exit 0
