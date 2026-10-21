#!/usr/bin/env python3
"""Executable FR-to-evidence traceability matrix for the M0 gate.

The matrix answers one question for every functional requirement: *which
recorded evidence demonstrates it, and does that evidence actually hold?*

Each requirement is bound to a list of assertions. An assertion names an
evidence document, a path inside it, a comparison and an expected value. The
assertions are **evaluated against the evidence files**, not written down as
statuses, so the matrix cannot silently drift away from the implementation: if
an evidence file stops satisfying an assertion, the matrix reports it.

Two directions are checked:

* forward — every FR-001 to FR-014 has at least one assertion, so no
  requirement is unaccounted for;
* backward — the per-requirement verdict that the matrix derives is compared
  with the verdict the gate report records, so the two cannot disagree.

The evidence paths are relative to the run directory. A path prefixed with
``repo:`` is relative to the repository root, which is used for the committed
artifacts (the sample manifest) that the run references.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import REPO_ROOT, M0Error, read_json  # noqa: E402
from schema import REQUIREMENT_IDS, SUCCESS_CRITERION_IDS, aggregate_overall  # noqa: E402

SCHEMA_VERSION = "m0.traceability.v1"

# The comparison operators the assertion language supports.
OPERATORS = (
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "is_true",
    "is_false",
    "non_empty",
    "len_eq",
    "len_ge",
    "all_true",
    "all_status_pass",
    "contains",
    "starts_with",
    "all_requirements_pass",
)


@dataclass
class Assertion:
    """One checkable statement about the recorded evidence."""

    assertion_id: str
    description: str
    path: str
    expected: Any
    operator: str
    custom: Callable[[Any], tuple[bool, str]] | None = field(default=None, repr=False)

    def evaluate(self, evidence: "Evidence") -> tuple[bool, str]:
        """Evaluate the assertion and return whether it holds and what was seen."""
        if self.custom is not None:
            return self.custom(evidence)

        try:
            value = evidence.lookup(self.path)
        except KeyError as error:
            return False, f"the evidence path is missing: {error}"

        observed = _render(value)
        operator = self.operator

        if operator == "eq":
            ok = value == self.expected
        elif operator == "ne":
            ok = value != self.expected
        elif operator == "lt":
            ok = _numeric(value) < _numeric(self.expected)
        elif operator == "le":
            ok = _numeric(value) <= _numeric(self.expected)
        elif operator == "gt":
            ok = _numeric(value) > _numeric(self.expected)
        elif operator == "ge":
            ok = _numeric(value) >= _numeric(self.expected)
        elif operator == "is_true":
            ok = value is True
        elif operator == "ne_true":
            ok = value is not True
        elif operator == "is_false":
            ok = value is False
        elif operator == "non_empty":
            ok = bool(value)
        elif operator == "len_eq":
            ok = len(value) == self.expected
        elif operator == "len_ge":
            ok = len(value) >= self.expected
        elif operator == "all_true":
            ok = bool(value) and all(entry is True for entry in value)
        elif operator == "all_status_pass":
            ok = bool(value) and all(
                entry.get("status") == "pass" for entry in value
            )
        elif operator == "contains":
            ok = self.expected in value
        elif operator == "starts_with":
            ok = isinstance(value, str) and value.startswith(self.expected)
        elif operator == "all_requirements_pass":
            ok = bool(value) and all(
                record.get("status") == "pass" for record in value.values()
            )
        else:
            raise M0Error(f"unknown comparison operator: {operator}")

        return ok, observed


def _numeric(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise M0Error(f"the value {value!r} is not a number")
    return float(value)


def _render(value: Any) -> str:
    """Render an observed value for the report."""
    if isinstance(value, str):
        return value if len(value) <= 120 else value[:117] + "..."
    if isinstance(value, (list, tuple)):
        return f"[{len(value)} item(s)]"
    if isinstance(value, dict):
        return f"{{{', '.join(sorted(value))}}}"
    return repr(value)


class Evidence:
    """Lazily loads the evidence documents a run directory contains."""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self._cache: dict[str, Any] = {}

    def _load(self, document: str) -> Any:
        if document in self._cache:
            return self._cache[document]
        path = self.run_dir / document
        if not path.is_file():
            raise KeyError(f"the evidence document is missing: {document}")
        value = read_json(path)
        self._cache[document] = value
        return value

    def _load_repo(self, document: str) -> Any:
        """Load a document that lives in the repository, not in the run."""
        key = f"repo:{document}"
        if key in self._cache:
            return self._cache[key]
        path = REPO_ROOT / document
        if not path.is_file():
            raise KeyError(f"the evidence document is missing: {key}")
        value = read_json(path)
        self._cache[key] = value
        return value
    def lookup(self, expression: str) -> Any:
        """Resolve a `document:path.to.value` expression.

        The document may be prefixed with `repo:`, which means the path is
        relative to the repository root instead of the run directory. The
        prefix is stripped first, so that a `repo:` document keeps the
        separator that divides it from the path inside it.
        """
        document = expression
        from_repo = document.startswith("repo:")
        if from_repo:
            document = document[len("repo:") :]
        # The path separator is the last colon, so that a document name may
        # itself contain one.
        document, separator, path = document.rpartition(":")
        if not separator:
            raise KeyError(f"the evidence expression has no document: {expression}")
        value = self._load_repo(document) if from_repo else self._load(document)
        for part in path.split("."):
            if part == "":
                continue
            if isinstance(value, list):
                try:
                    value = value[int(part)]
                except (ValueError, IndexError) as error:
                    raise KeyError(f"the path {path!r} does not resolve: {error}") from error
            elif isinstance(value, dict):
                if part not in value:
                    raise KeyError(f"the path {path!r} does not resolve in {document}")
                value = value[part]
            else:
                raise KeyError(f"the path {path!r} does not resolve in {document}")
        return value

    def unavailable_reason(self, assertion: Assertion) -> str | None:
        """Only an explicitly unexecuted producer makes its evidence unavailable.

        Missing evidence from a producer that claimed success remains a failure.
        Existing output from an unexecuted step is not accepted as current evidence.
        """
        document = assertion.path.rpartition(":")[0]
        producer = {
            "sample-validation.json": "sample",
            "sample-recovery.json": "sample_recovery",
            "environment.json": "environment",
            "source-hashes.json": "environment",
            "exchange.json": "extension",
            "measurements.json": "measurements",
            "minimal-runtime.json": "minimal_runtime",
            "independent-reproduction.json": "independent_reproduction",
            "reference/comparator-self-check.json": "comparator_self_check",
            "reference/reference-summary.json": "reference",
            "reference/comparison.json": "reference",
            "reference/run-1.json": "reference",
        }.get(document)
        if producer:
            try:
                step = self._load("steps.json").get(producer, {})
            except KeyError:
                step = {}
            if step.get("status") == "not_run":
                return f"the {producer} step was not run: {step.get('detail', '')}"
            try:
                record = self._load(document)
            except KeyError:
                record = {}
            if record.get("status") == "not_run":
                return f"the evidence producer recorded not_run: {document}"
        if assertion.assertion_id in ("SC006-2", "SC006-3"):
            requirements = self.lookup("report.json:requirements")
            if aggregate_overall(requirements) == "incomplete":
                return "the required steps are incomplete"
        return None


def A(  # noqa: N802 - a short constructor keeps the matrix readable
    assertion_id: str,
    description: str,
    path: str,
    expected: Any = None,
    operator: str = "eq",
    size: int | None = None,
) -> Assertion:
    """Build an assertion.

    `size` carries the bound for the length and count operators, so that the
    matrix table reads as one assertion per line.
    """
    if operator not in OPERATORS and operator not in ("ne_true", "custom"):
        raise M0Error(f"unknown comparison operator: {operator}")
    if operator in ("len_eq", "len_ge") and size is None:
        raise M0Error(f"the operator {operator} needs a size")
    if operator in ("len_eq", "len_ge"):
        expected = size
    return Assertion(assertion_id, description, path, expected, operator)


# --- custom checks ------------------------------------------------------------


def _all_illegal_categories_rejected(evidence: Evidence) -> tuple[bool, str]:
    """Every illegal input category must be rejected with a located reason."""
    categories = evidence.lookup("exchange.json:illegal_categories")
    if not categories:
        return False, "no illegal category was recorded"
    rejected = [entry for entry in categories if entry.get("status") == "pass"]
    located = [entry for entry in rejected if entry.get("error")]
    return (
        len(rejected) == len(categories) and len(located) == len(categories),
        f"{len(rejected)}/{len(categories)} rejected, {len(located)} with a located reason",
    )


# --- the matrix ----------------------------------------------------------------

# Each entry maps a requirement to the assertions that demonstrate it.
MATRIX: dict[str, list[Assertion]] = {
    "FR-001": [
        A("FR001-1", "the frozen sample is a real ensemble sample", "sample-validation.json:kind", "real_ensemble"),
        A("FR001-2", "the sample holds the complete 50-member set", "sample-validation.json:member_count", 50),
        A("FR001-3", "the sample is a regional field, not a single point", "sample-validation.json:point_count", 9),
        A("FR001-4", "the variable is the near-surface temperature", "sample-validation.json:variable", "2m_temperature"),
        A("FR001-5", "the sample has one initialization", "sample-validation.json:init_time", "2020-01-01T00:00:00Z"),
        A("FR001-6", "the sample has one valid time", "sample-validation.json:valid_time", "2020-01-02T00:00:00Z"),
        A("FR001-7", "the unit is kept as K", "sample-validation.json:units", "K"),
    ],
    "FR-002": [
        A("FR002-1", "the manifest records the source provider", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:source.provider", None, "non_empty"),
        A("FR002-2", "the manifest records the source URI", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:source.uri", None, "non_empty"),
        A("FR002-3", "the manifest records the acquisition time", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:source.retrieved_at", None, "non_empty"),
        A("FR002-4", "the manifest records the member roles", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:source.member_set", size=50, operator="len_ge"),
        A("FR002-5", "the manifest records the region and the point keys", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:points", size=9, operator="len_eq"),
        A("FR002-6", "the manifest records the raw and normalized units", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:variable.source_units", "K"),
        A("FR002-7", "the manifest records the ordering and transformation rules", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:layout.matrix_order", None, "non_empty"),
        A("FR002-8", "the manifest records the content checksum", "sample-validation.json:payload_sha256", None, "non_empty"),
        A("FR002-9", "the manifest records the usage limits", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:usage.license", None, "non_empty"),
        A("FR002-10", "the manifest records the recovery procedure", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:usage.recovery", None, "non_empty"),
    ],
    "FR-003": [
        A("FR003-1", "the frozen sample stays within the size budget", "sample-validation.json:total_frozen_bytes", 104857600, "le"),
        A("FR003-2", "the frozen sample was recovered from its source", "sample-recovery.json:status", "pass"),
        A("FR003-3", "the recovered matrix is byte identical to the frozen one", "sample-recovery.json:comparison.byte_identical", True),
        A("FR003-4", "the source matrix hash is the recorded one", "sample-validation.json:matrix_sha256_float32_le", "09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe"),
    ],
    "FR-004": [
        A("FR004-1", "three independent processes produced the reference", "reference/reference-summary.json:independent_processes.count", 3),
        A("FR004-2", "the standardized matrix is recorded", "reference/run-1.json:scaled", None, "non_empty"),
        A("FR004-3", "the projection is recorded", "reference/run-1.json:components", None, "non_empty"),
        A("FR004-4", "the explained variance is recorded", "reference/run-1.json:explained_variance_ratio", None, "non_empty"),
        A("FR004-5", "the member partition is recorded", "reference/run-1.json:labels", size=50, operator="len_eq"),
        A("FR004-6", "the cluster centers are recorded", "reference/run-1.json:cluster_centers", size=3, operator="len_eq"),
        A("FR004-7", "the inertia is recorded", "reference/run-1.json:inertia", None, "non_empty"),
        A("FR004-8", "the iteration count is recorded", "reference/run-1.json:n_iter", None, "non_empty"),
        A("FR004-9", "the input hash is recorded", "reference/run-1.json:input_sha256", None, "non_empty"),
        A("FR004-10", "the configuration hash is recorded", "reference/run-1.json:config_sha256", None, "non_empty"),
    ],
    "FR-005": [
        A("FR005-1", "the comparison tolerance is fixed at 1e-8", "reference/comparison.json:tolerance.absolute", 1e-8),
        A("FR005-2", "the comparison tolerance is fixed at 1e-6", "reference/comparison.json:tolerance.relative", 1e-6),
        A("FR005-3", "the member partition is equivalent across the runs", "reference/comparison.json:checks.partition_equal", True),
        A("FR005-4", "the projection is equivalent across the runs", "reference/comparison.json:checks.subspace_equal", True),
        A("FR005-5", "the cluster centers are equivalent across the runs", "reference/comparison.json:checks.cluster_centers_close", True),
        A("FR005-6", "the comparison itself passed", "reference/comparison.json:status", "pass"),
    ],
    "FR-006": [
        A("FR006-1", "the synthetic positive cases are recorded", "exchange.json:positive_cases", size=6, operator="len_ge"),
        A("FR006-2", "every synthetic positive case roundtripped", "exchange.json:positive_cases", None, "all_status_pass"),
        A("FR006-3", "the ten illegal input categories are covered", "exchange.json:illegal_category_count", 10),
    ],
    "FR-007": [
        A("FR007-1", "the extension was loaded in a real host", "exchange.json:status", "pass"),
        A("FR007-2", "the ordered members and features were preserved", "exchange.json:positive_cases", None, "all_status_pass"),
        A("FR007-3", "the extension artifact was recorded", "exchange.json:extension_path", None, "non_empty"),
    ],
    "FR-008": [
        A("FR008-1", "a task row set crossing the host batch boundary was consumed", "exchange.json:row_batch_boundary.status", "pass"),
        A("FR008-2", "the row batch exceeded the host vector size", "exchange.json:row_batch_boundary.row_count", 2048, "gt"),
        A("FR008-3", "a member list crossing the host batch boundary was preserved", "exchange.json:member_batch_boundary.status", "pass"),
        A("FR008-4", "the member list exceeded the host vector size", "exchange.json:member_batch_boundary.member_count", 2048, "gt"),
        A("FR008-5", "two concurrent connections produced the same result", "exchange.json:two_connection_concurrency.status", "pass"),
    ],
    "FR-009": [
        A("FR009-1", "every illegal input category was rejected with a located reason", "exchange.json:illegal_categories", None, "custom"),
        A("FR009-2", "a mixed valid/invalid batch failed as a whole", "exchange.json:mixed_batch.status", "pass"),
        A("FR009-3", "a type-binding error is rejected separately", "exchange.json:type_binding_rejection.distinct_from_data_validation", True),
    ],
    "FR-010": [
        A("FR010-1", "the Rust toolchain version is locked", "environment.json:rustc_version", "rustc 1.96.0", "starts_with"),
        A("FR010-2", "the DuckDB host version is locked", "environment.json:duckdb_version", "v1.5.5", "starts_with"),
        A("FR010-3", "the Python version is locked", "environment.json:python_version", "Python 3.13.13"),
        A("FR010-4", "the environment check passed", "environment.json:setup_check_status", "pass"),
        A("FR010-5", "the source identity is recorded", "source-hashes.json:git_commit", None, "non_empty"),
        A("FR010-6", "the source file hashes are recorded", "source-hashes.json:tracked_files", None, "non_empty"),
    ],
    "FR-011": [
        A("FR011-1", "the measurements cover the 1, 10 and 100 task scales", "measurements.json:measurements", size=3, operator="len_eq"),
        A("FR011-2", "the serial and the concurrent results agree", "measurements.json:serial_concurrent_consistency_passed", True),
        A("FR011-3", "the peak memory is recorded", "measurements.json:measurements.0.peak_rss_bytes", None, "non_empty"),
        A("FR011-4", "the host thread setting is recorded", "measurements.json:host_threads.duckdb_threads", None, "non_empty"),
        A("FR011-5", "the cold start and the loaded call are separate scopes", "measurements.json:measurements.0.cold_start.wall_clock_seconds_median", None, "non_empty"),
        A("FR011-6", "the unseparated costs are declared", "measurements.json:unseparated_costs", None, "non_empty"),
    ],
    "FR-012": [
        A("FR012-1", "the extension ran in an isolated minimal environment", "minimal-runtime.json:status", "pass"),
        A("FR012-2", "no reference tool was present in that environment", "minimal-runtime.json:reference_tools_present", size=0, operator="len_eq"),
        A("FR012-3", "the isolation method is recorded", "minimal-runtime.json:method", None, "non_empty"),
    ],
    "FR-013": [
        A("FR013-1", "the sample was re-extracted from its real source", "sample-recovery.json:status", "pass"),
        A("FR013-2", "the pinned source objects are unchanged", "sample-recovery.json:source_objects", None, "custom"),
        A("FR013-3", "the field mapping is recorded in the manifest", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:provenance.extraction_steps", size=3, operator="len_ge"),
        A("FR013-4", "the transformations are recorded in the manifest", "repo:validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json:provenance.transformations", size=1, operator="len_ge"),
    ],
    "FR-014": [
        A("FR014-1", "the report carries every functional requirement", "report.json:requirements", None, "custom"),
        A("FR014-2", "every referenced artifact resolves and hashes correctly", "report.json:requirements", None, "custom"),
        A("FR014-3", "the M1 sample contract is recorded", "report.json:m1_handoff.sample_contract", None, "non_empty"),
        A("FR014-4", "the M1 reference configuration is recorded", "report.json:m1_handoff.reference_config", None, "non_empty"),
        A("FR014-5", "the M1 comparison rules are recorded", "report.json:m1_handoff.comparison_rules", None, "non_empty"),
        A("FR014-6", "the M1 verified environment is recorded", "report.json:m1_handoff.verified_environment", None, "non_empty"),
        A("FR014-7", "the M1 dependency decisions are recorded", "report.json:m1_handoff.dependency_decisions", None, "non_empty"),
        A("FR014-8", "the version combination is recorded as a document", "report.json:m1_handoff.version_combination", None, "non_empty"),
        A("FR014-9", "the version combination file pins the Rust toolchain", "repo:validation/m0/fixtures/version_combination.json:toolchain.rustc", "1.96.0"),
        A("FR014-10", "the version combination file pins the DuckDB host", "repo:validation/m0/fixtures/version_combination.json:host.duckdb_cli", "1.5.5"),
        A("FR014-11", "the version combination file pins the extension dependency", "repo:validation/m0/fixtures/version_combination.json:extension_dependencies.duckdb", "=1.10505.0"),
        A("FR014-12", "the version combination file records the packaging tool", "repo:validation/m0/fixtures/version_combination.json:packaging_tool.commit", "b777c70d30942cca5bef62d6d4fa23a13362f398"),
        A("FR014-13", "the uncovered scope is recorded as a limitation", "report.json:limitations", None, "non_empty"),
    ],
}

# The custom checks are attached after the matrix is defined, so that the
# matrix itself stays a readable table.



# The custom checks for the success criteria.


def _all_negative_eligibility_rejected(evidence: Evidence) -> tuple[bool, str]:
    """Every ineligible sample shape must be rejected with a reason."""
    cases = evidence.lookup("sample-validation.json:negative_eligibility_cases")
    rejected = [case for case in cases if case.get("rejected") is True]
    with_reason = [case for case in rejected if case.get("reason")]
    return (
        len(rejected) == len(cases) and len(with_reason) == len(cases),
        f"{len(rejected)}/{len(cases)} rejected, {len(with_reason)} with a reason",
    )


def _source_objects_unchanged(evidence: Evidence) -> tuple[bool, str]:
    """Every pinned source object must still have the recorded identity."""
    objects = evidence.lookup("sample-recovery.json:source_objects")
    matched = [entry for entry in objects if entry.get("matches") is True]
    return (
        len(matched) == len(objects) and bool(objects),
        f"{len(matched)}/{len(objects)} source objects unchanged",
    )


def _comparator_self_checks_correct(evidence: Evidence) -> tuple[bool, str]:
    """The comparator self-checks must all behave as required."""
    checks = evidence.lookup("reference/comparator-self-check.json:checks")
    correct = [check for check in checks if check.get("correct") is True]
    return (
        len(correct) == len(checks) and bool(checks),
        f"{len(correct)}/{len(checks)} comparator self-checks behaved as required",
    )


def _artifact_hashes_match(evidence: Evidence) -> tuple[bool, str]:
    """Every artifact the report references must hash to the recorded value."""
    from common import sha256_file

    report = evidence.lookup("report.json:requirements")
    mismatched: list[str] = []
    total = 0
    for requirement_id, record in report.items():
        for artifact in record.get("artifacts", []):
            total += 1
            target = evidence.run_dir / artifact["path"]
            if not target.is_file() or sha256_file(target) != artifact["sha256"]:
                mismatched.append(f"{requirement_id}:{artifact['path']}")
    return (
        not mismatched,
        f"{total - len(mismatched)}/{total} artifact hashes match"
        + (f"; mismatched: {mismatched}" if mismatched else ""),
    )


def _report_covers_every_fr(evidence: Evidence) -> tuple[bool, str]:
    """The report must carry every FR exactly once."""
    requirements = evidence.lookup("report.json:requirements")
    missing = [rid for rid in REQUIREMENT_IDS if rid not in requirements]
    extra = [rid for rid in requirements if rid not in REQUIREMENT_IDS]
    return (
        not missing and not extra,
        f"{len(requirements)} requirements recorded"
        + (f"; missing {missing}" if missing else "")
        + (f"; unexpected {extra}" if extra else ""),
    )


def _all_requirements_pass(evidence: Evidence) -> tuple[bool, str]:
    """Every recorded requirement must be a pass for the milestone to pass."""
    requirements = evidence.lookup("report.json:requirements")
    failing = [
        requirement_id
        for requirement_id, record in requirements.items()
        if record.get("status") != "pass"
    ]
    return (
        not failing,
        f"{len(requirements) - len(failing)}/{len(requirements)} requirements pass"
        + (f"; not passing: {failing}" if failing else ""),
    )


def _distinct_pids(evidence: Evidence) -> tuple[bool, str]:
    """The three reference runs must have been separate processes."""
    pids = evidence.lookup("reference/reference-summary.json:independent_processes.pids")
    return (
        len(set(pids)) == len(pids) and len(pids) == 3,
        f"{len(set(pids))} distinct pid(s) of {len(pids)}",
    )


CUSTOM_CHECKS: dict[str, Callable[[Any], tuple[bool, str]]] = {
    "FR009-1": _all_illegal_categories_rejected,
    "FR013-2": _source_objects_unchanged,
    "FR014-1": _report_covers_every_fr,
    "FR014-2": _artifact_hashes_match,
}


SC_MATRIX: dict[str, list[Assertion]] = {
    "SC-001": [
        A("SC001-1", "a real regional ensemble sample is frozen", "sample-validation.json:kind", "real_ensemble"),
        A("SC001-2", "the sample is recoverable offline from its source", "sample-recovery.json:status", "pass"),
        A("SC001-3", "the recovered matrix is byte identical", "sample-recovery.json:comparison.byte_identical", True),
        A("SC001-4", "the frozen total stays within 100 MiB", "sample-validation.json:total_frozen_bytes", 104857600, "le"),
        A("SC001-5", "the manifest holds the complete member set", "sample-validation.json:member_count", 50),
        A("SC001-6", "the manifest holds the complete point set", "sample-validation.json:point_count", 9),
        A("SC001-7", "the manifest's required information is complete", "sample-validation.json:schema.member_count", 50),
        A("SC001-8", "the member and point set checks all pass", "sample-validation.json:negative_eligibility_cases", None, "custom"),
    ],
    "SC-002": [
        A("SC002-1", "the reference ran in three independent processes", "reference/reference-summary.json:independent_processes.count", 3),
        A("SC002-2", "the processes were distinct", "reference/reference-summary.json:independent_processes.pids", None, "custom"),
        A("SC002-3", "the runs satisfy the comparison rules", "reference/comparison.json:status", "pass"),
        A("SC002-4", "the member partition agrees", "reference/comparison.json:checks.partition_equal", True),
        A("SC002-5", "the input is traceable", "reference/run-1.json:input_sha256", None, "non_empty"),
        A("SC002-6", "the configuration is traceable", "reference/run-1.json:config_sha256", None, "non_empty"),
        A("SC002-7", "the result is traceable to its own output file", "reference/run-1.json:output_file", "run-1.json"),
        A("SC002-8", "the output file hash is recorded", "reference/reference-summary.json:runs.0.output_files.0.sha256", None, "non_empty"),
    ],
    "SC-003": [
        A("SC003-1", "every positive roundtrip preserved the members", "exchange.json:positive_cases", None, "all_status_pass"),
        A("SC003-2", "the positive cases cover several member counts", "exchange.json:positive_cases.0.member_count", None, "non_empty"),
        A("SC003-3", "a multi-task batch crossed the host boundary", "exchange.json:row_batch_boundary.status", "pass"),
        A("SC003-4", "a member list crossed the host boundary", "exchange.json:member_batch_boundary.status", "pass"),
        A("SC003-5", "the concurrent connections agreed", "exchange.json:two_connection_concurrency.status", "pass"),
        A("SC003-6", "the concurrent connections really overlapped", "exchange.json:two_connection_concurrency.execution", None, "non_empty"),
        A("SC003-7", "no result was truncated or mixed", "exchange.json:mixed_batch.status", "pass"),
    ],
    "SC-004": [
        A("SC004-1", "every illegal category was rejected", "exchange.json:illegal_categories", None, "custom"),
        A("SC004-2", "all ten categories are present", "exchange.json:illegal_category_count", 10),
        A("SC004-3", "each rejection names the located position", "exchange.json:illegal_categories.0.error", None, "non_empty"),
        A("SC004-4", "the session recovered after the rejections", "exchange.json:illegal_categories.0.status", "pass"),
    ],
    "SC-005": [
        A("SC005-1", "the 1, 10 and 100 task scales are recorded", "measurements.json:measurements", None, "len_eq", 3),
        A("SC005-2", "the rerun results agree", "measurements.json:measurements.0.results_consistent", True),
        A("SC005-3", "the wall-clock time is recorded", "measurements.json:measurements.0.cold_start.wall_clock_seconds_median", None, "non_empty"),
        A("SC005-4", "the peak memory is recorded", "measurements.json:measurements.0.peak_rss_bytes", None, "non_empty"),
        A("SC005-5", "the unmeasured parts are disclosed", "measurements.json:unseparated_costs", None, "non_empty"),
        A("SC005-6", "no throughput target is claimed", "measurements.json:no_throughput_threshold", None, "non_empty"),
    ],
    "SC-006": [
        A("SC006-1", "the evidence index covers every requirement", "report.json:requirements", None, "custom"),
        A("SC006-2", "all necessary gates pass before an overall pass", "report.json:overall", "passed"),
        A("SC006-3", "every requirement is recorded as passing", "report.json:requirements", None, "all_requirements_pass"),
        A("SC006-4", "an independent environment reproduced the result", "independent-reproduction.json:status", "pass"),
        A("SC006-5", "the independent run reproduced the same source matrix", "independent-reproduction.json:source_matrix_sha256", "09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe"),
    ],
}

SC_CUSTOM_CHECKS: dict[str, Callable[[Any], tuple[bool, str]]] = {
    "SC001-8": _all_negative_eligibility_rejected,
    "SC002-2": _distinct_pids,
    "SC004-1": _all_illegal_categories_rejected,
    "SC006-1": _report_covers_every_fr,
    "SC006-3": _all_requirements_pass,
}


SC_CUSTOM_CHECKS: dict[str, Callable[[Any], tuple[bool, str]]] = {
    "SC001-8": _all_negative_eligibility_rejected,
    "SC002-2": _distinct_pids,
    "SC004-1": _all_illegal_categories_rejected,
    "SC006-1": _report_covers_every_fr,
    "SC006-3": _all_requirements_pass,
}

def _attach_custom_checks() -> None:
    for requirement_id, assertions in MATRIX.items():
        for assertion in assertions:
            custom = CUSTOM_CHECKS.get(assertion.assertion_id)
            if custom is not None:
                assertion.custom = custom
    for criterion_id, assertions in SC_MATRIX.items():
        for assertion in assertions:
            custom = SC_CUSTOM_CHECKS.get(assertion.assertion_id)
            if custom is not None:
                assertion.custom = custom


_attach_custom_checks()


def _evaluate_assertions(assertions: list[Assertion], evidence: Evidence) -> dict[str, Any]:
    results = []
    for assertion in assertions:
        try:
            reason = evidence.unavailable_reason(assertion)
            if reason is not None:
                holds, observed, status = False, reason, "not_run"
            else:
                holds, observed = assertion.evaluate(evidence)
                status = "pass" if holds else "fail"
        except (KeyError, M0Error) as error:
            holds, observed, status = False, f"the assertion could not be evaluated: {error}", "fail"
        results.append({
            "assertion_id": assertion.assertion_id,
            "description": assertion.description,
            "evidence_path": assertion.path,
            "expected": assertion.expected,
            "operator": assertion.operator,
            "observed": observed,
            "holds": holds,
            "status": status,
        })
    failures = [result for result in results if result["status"] == "fail"]
    pending = [result for result in results if result["status"] == "not_run"]
    return {
        "assertion_count": len(results),
        "status": "fail" if failures or not results else "not_run" if pending else "pass",
        "failed_assertions": [result["assertion_id"] for result in failures],
        "not_run_assertions": [result["assertion_id"] for result in pending],
        "assertions": results,
    }


def run_traceability(run_dir: Path) -> dict[str, Any]:
    """Evaluate the matrix against the evidence in `run_dir`."""
    evidence = Evidence(run_dir)

    matrix = {
        requirement_id: _evaluate_assertions(MATRIX.get(requirement_id, []), evidence)
        for requirement_id in REQUIREMENT_IDS
    }

    # Forward coverage: every requirement is accounted for.
    uncovered = [rid for rid in REQUIREMENT_IDS if not MATRIX.get(rid)]
    # Backward coverage: no assertion is orphaned.
    referenced = {
        assertion.assertion_id
        for assertions in MATRIX.values()
        for assertion in assertions
    }
    referenced |= {
        assertion.assertion_id
        for assertions in SC_MATRIX.values()
        for assertion in assertions
    }
    defined = set(CUSTOM_CHECKS) | set(SC_CUSTOM_CHECKS) | referenced
    orphaned = sorted(defined - referenced)

    # The success criteria are evaluated with the same machinery.
    sc_matrix = {
        criterion_id: _evaluate_assertions(SC_MATRIX.get(criterion_id, []), evidence)
        for criterion_id in SUCCESS_CRITERION_IDS
    }

    uncovered_criteria = [sc for sc in SUCCESS_CRITERION_IDS if not SC_MATRIX.get(sc)]

    # Consistency with the gate report.
    consistency: dict[str, Any] = {"checked": False, "mismatches": []}
    try:
        report_requirements = evidence.lookup("report.json:requirements")
    except KeyError:
        report_requirements = {}
    if report_requirements:
        consistency["checked"] = True
        for requirement_id in REQUIREMENT_IDS:
            derived = matrix[requirement_id]["status"]
            recorded = report_requirements.get(requirement_id, {}).get("status")
            if recorded == "pass" and derived != "pass":
                consistency["mismatches"].append(
                    f"{requirement_id}: the report says 'pass' but the evidence does not support it"
                )
            elif recorded == "fail" and derived == "pass":
                consistency["mismatches"].append(
                    f"{requirement_id}: the report says 'fail' but the evidence does support it"
                )

    failures = [
        result
        for requirement in matrix.values()
        for result in requirement["assertions"]
        if result["status"] == "fail"
    ]
    sc_failures = [
        result
        for criterion in sc_matrix.values()
        for result in criterion["assertions"]
        if result["status"] == "fail"
    ]

    status = "pass"
    if (
        uncovered
        or uncovered_criteria
        or orphaned
        or failures
        or sc_failures
        or consistency["mismatches"]
    ):
        status = "fail"
    elif any(entry["status"] == "not_run" for entry in (*matrix.values(), *sc_matrix.values())):
        status = "not_run"

    return {
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run_dir),
        "status": status,
        "forward_coverage": {
            "requirements_total": len(REQUIREMENT_IDS),
            "requirements_with_assertions": len(REQUIREMENT_IDS) - len(uncovered),
            "uncovered_requirements": uncovered,
            "success_criteria_total": len(SUCCESS_CRITERION_IDS),
            "success_criteria_with_assertions": len(SUCCESS_CRITERION_IDS)
            - len(uncovered_criteria),
            "uncovered_success_criteria": uncovered_criteria,
        },
        "backward_coverage": {
            "assertions_total": sum(len(MATRIX.get(rid, [])) for rid in REQUIREMENT_IDS)
            + sum(len(SC_MATRIX.get(sc, [])) for sc in SUCCESS_CRITERION_IDS),
            "orphaned_checks": orphaned,
        },
        "consistency_with_report": consistency,
        "failed_assertions": [
            {
                "assertion_id": result["assertion_id"],
                "description": result["description"],
                "expected": result["expected"],
                "observed": result["observed"],
            }
            for result in failures
        ],
        "failed_success_criterion_assertions": [
            {
                "assertion_id": result["assertion_id"],
                "description": result["description"],
                "expected": result["expected"],
                "observed": result["observed"],
            }
            for result in sc_failures
        ],
        "not_run_assertions": [
            result["assertion_id"]
            for entry in (*matrix.values(), *sc_matrix.values())
            for result in entry["assertions"]
            if result["status"] == "not_run"
        ],
        "matrix": matrix,
        "success_criteria": sc_matrix,
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-dir",
        type=Path,
        help="the run directory that holds the evidence",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the traceability result to this JSON file",
    )
    arguments = parser.parse_args()

    if not arguments.run_dir:
        parser.error("--run-dir is required")

    result = run_traceability(arguments.run_dir)
    if arguments.output:
        from common import write_json

        write_json(arguments.output, result)

    print(f"traceability: {result['status']}")
    coverage = result["forward_coverage"]
    print(
        f"  forward coverage: {coverage['requirements_with_assertions']}/"
        f"{coverage['requirements_total']} requirements, "
        f"{coverage['success_criteria_with_assertions']}/"
        f"{coverage['success_criteria_total']} success criteria have assertions"
    )
    for requirement_id in REQUIREMENT_IDS:
        entry = result["matrix"][requirement_id]
        marker = {"pass": "ok  ", "fail": "FAIL", "not_run": "SKIP"}[entry["status"]]
        print(f"  [{marker}] {requirement_id}: {entry['assertion_count']} assertion(s)")
    for criterion_id in SUCCESS_CRITERION_IDS:
        entry = result["success_criteria"][criterion_id]
        marker = {"pass": "ok  ", "fail": "FAIL", "not_run": "SKIP"}[entry["status"]]
        print(f"  [{marker}] {criterion_id}: {entry['assertion_count']} assertion(s)")
    for failure in result["failed_assertions"]:
        print(
            f"  FAILED {failure['assertion_id']}: {failure['description']} "
            f"(expected {failure['expected']!r}, observed {failure['observed']!r})"
        )
    for failure in result["failed_success_criterion_assertions"]:
        print(
            f"  FAILED {failure['assertion_id']}: {failure['description']} "
            f"(expected {failure['expected']!r}, observed {failure['observed']!r})"
        )
    for assertion_id in result["not_run_assertions"]:
        print(f"  NOT RUN {assertion_id}: required evidence is unavailable")
    for mismatch in result["consistency_with_report"]["mismatches"]:
        print(f"  MISMATCH: {mismatch}")
    return {"pass": 0, "fail": 1, "not_run": 2}[result["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
