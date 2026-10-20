#!/usr/bin/env bash
# Bootstrap and verify the M0 validation environment.
#
# The evidence contract requires that a verifier can rebuild the declared
# environment without depending on an undeclared personal global environment.
# This script therefore:
#
#   1. creates the two isolated Python environments from the committed lock
#      files (the data environment and the reference environment);
#   2. verifies the exact tool versions the M0 gate depends on;
#   3. prints a machine-readable summary that the gate records as the
#      environment evidence.
#
# The script never installs into the system Python and never writes outside
# the chosen virtual environment directories.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VALIDATION_DIR="$ROOT/validation/m0"

DATA_VENV="${ENSABC_M0_DATA_VENV:-/tmp/ensabc-m0-data/.venv}"
REFERENCE_VENV="${ENSABC_M0_REFERENCE_VENV:-/tmp/ensabc-m0-baseline}"

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

# --- create the isolated Python environments ----------------------------------

if [[ "$CHECK_ONLY" == "0" ]]; then
    if ! command -v uv >/dev/null 2>&1; then
        record_failure "uv is required to create the isolated Python environments"
    else
        echo "creating the data environment in $DATA_VENV"
        uv venv --python "$REQUIRED_PYTHON" "$DATA_VENV" >/dev/null
        uv pip sync --python "$DATA_VENV/bin/python" \
            "$VALIDATION_DIR/requirements-data.lock" >/dev/null

        echo "creating the reference environment in $REFERENCE_VENV"
        uv venv --python "$REQUIRED_PYTHON" "$REFERENCE_VENV" >/dev/null
        uv pip sync --python "$REFERENCE_VENV/bin/python" \
            "$VALIDATION_DIR/requirements-reference.lock" >/dev/null
    fi
fi

# --- verify the tool versions --------------------------------------------------

RUSTC_VERSION="$(rustc --version 2>/dev/null || echo 'unavailable')"
CARGO_VERSION="$(cargo --version 2>/dev/null || echo 'unavailable')"
DUCKDB_VERSION="$(duckdb --version 2>/dev/null || echo 'unavailable')"
# `duckdb --version` prints "v1.5.5 (Variegata) d8cdaa33fd"; the version is the
# first field.
DUCKDB_CLI_VERSION="$(duckdb --version 2>/dev/null | awk '{print $1}' | tr -d ',' || echo 'unavailable')"
DATA_PYTHON_VERSION="$("$DATA_VENV/bin/python" --version 2>&1 | awk '{print $2}' || echo 'unavailable')"
REFERENCE_PYTHON_VERSION="$("$REFERENCE_VENV/bin/python" --version 2>&1 | awk '{print $2}' || echo 'unavailable')"
EXTENSION_CI_TOOLS_COMMIT="$(git -C "$EXTENSION_CI_TOOLS" rev-parse HEAD 2>/dev/null || echo 'unavailable')"
UNAME_MACHINE="$(uname -m)"

if [[ "$RUSTC_VERSION" != "$REQUIRED_RUSTC"* ]]; then
    record_failure "rustc must be $REQUIRED_RUSTC, found '$RUSTC_VERSION'"
fi
if [[ "$DUCKDB_CLI_VERSION" != "$REQUIRED_DUCKDB" ]]; then
    record_failure "the duckdb CLI must be $REQUIRED_DUCKDB, found '$DUCKDB_CLI_VERSION'"
fi
if [[ "$DATA_PYTHON_VERSION" != "$REQUIRED_PYTHON" ]]; then
    record_failure "the data environment must run Python $REQUIRED_PYTHON, found '$DATA_PYTHON_VERSION'"
fi
if [[ "$REFERENCE_PYTHON_VERSION" != "$REQUIRED_PYTHON" ]]; then
    record_failure "the reference environment must run Python $REQUIRED_PYTHON, found '$REFERENCE_PYTHON_VERSION'"
fi
if [[ "$EXTENSION_CI_TOOLS_COMMIT" != "$REQUIRED_EXTENSION_CI_TOOLS_COMMIT" ]]; then
    record_failure "extension-ci-tools must be at $REQUIRED_EXTENSION_CI_TOOLS_COMMIT, found '$EXTENSION_CI_TOOLS_COMMIT'"
fi
if [[ "$UNAME_MACHINE" != "aarch64" ]]; then
    record_failure "the M0 target platform is linux aarch64, found '$UNAME_MACHINE'"
fi

# The reference environment must carry the locked numerical stack.
if [[ -x "$REFERENCE_VENV/bin/python" ]]; then
    REFERENCE_VERSIONS="$("$REFERENCE_VENV/bin/python" -c '
import numpy, scipy, sklearn
print(f"{numpy.__version__} {scipy.__version__} {sklearn.__version__}")
' 2>/dev/null || echo 'unavailable')"
    if [[ "$REFERENCE_VERSIONS" != "2.5.4 1.18.1 1.9.1" ]]; then
        record_failure "the reference numerical stack must be numpy 2.5.4 / scipy 1.18.1 / scikit-learn 1.9.1, found '$REFERENCE_VERSIONS'"
    fi
else
    REFERENCE_VERSIONS="unavailable"
    record_failure "the reference environment is missing at $REFERENCE_VENV"
fi

# The data environment must carry the locked reading stack.
if [[ -x "$DATA_VENV/bin/python" ]]; then
    DATA_VERSIONS="$("$DATA_VENV/bin/python" -c '
import xarray, zarr, gcsfs
print(f"{xarray.__version__} {zarr.__version__} {gcsfs.__version__}")
' 2>/dev/null || echo 'unavailable')"
else
    DATA_VERSIONS="unavailable"
    record_failure "the data environment is missing at $DATA_VENV"
fi

# --- report ---------------------------------------------------------------------

if [[ "$AS_JSON" == "1" ]]; then
    printf '{'
    printf '"status": "%s", ' "$([[ ${#FAILURES[@]} -eq 0 ]] && echo pass || echo fail)"
    printf '"rustc": "%s", ' "$RUSTC_VERSION"
    printf '"cargo": "%s", ' "$CARGO_VERSION"
    printf '"duckdb_cli": "%s", ' "$DUCKDB_VERSION"
    printf '"data_python": "%s", ' "$DATA_PYTHON_VERSION"
    printf '"reference_python": "%s", ' "$REFERENCE_PYTHON_VERSION"
    printf '"reference_stack": "%s", ' "$REFERENCE_VERSIONS"
    printf '"data_stack": "%s", ' "$DATA_VERSIONS"
    printf '"extension_ci_tools_commit": "%s", ' "$EXTENSION_CI_TOOLS_COMMIT"
    printf '"platform": "%s", ' "$UNAME_MACHINE"
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
    echo "M0 environment"
    echo "  platform:              $UNAME_MACHINE"
    echo "  rustc:                 $RUSTC_VERSION"
    echo "  cargo:                 $CARGO_VERSION"
    echo "  duckdb CLI:            $DUCKDB_VERSION"
    echo "  data python:           $DATA_PYTHON_VERSION ($DATA_VERSIONS)"
    echo "  reference python:      $REFERENCE_PYTHON_VERSION ($REFERENCE_VERSIONS)"
    echo "  extension-ci-tools:    $EXTENSION_CI_TOOLS_COMMIT"
    if [[ ${#FAILURES[@]} -eq 0 ]]; then
        echo "  status:                pass"
    else
        echo "  status:                fail (${#FAILURES[@]} problem(s))"
    fi
fi

[[ ${#FAILURES[@]} -eq 0 ]]
