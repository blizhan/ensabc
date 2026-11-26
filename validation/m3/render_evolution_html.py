#!/usr/bin/env python3
"""Offline single-file interactive evolution visualization generator (T037).

Implements M3 visualization contract (`m3.visualization.v1` / `m3.disclaimer.v1`):
- Queries ONLY published DuckDB history tables (`evolution_runs`, `replay_batches`,
  `evolution_nodes`, `evolution_transitions`, `evolution_edges`, `evolution_events`,
  `evolution_branch_summaries`) via isolated `duckdb -json` CLI subprocess.
- Enforces strict pre-render provenance gate: exits 1 with `MISSING_PROVENANCE_FIELD`
  and writes 0 bytes to `--output` if `evolution_run_id` is missing, node count is 0,
  or any required provenance/margin/rule_basis column is NULL, empty, or non-finite.
- Emits a self-contained UTF-8 `.html` with strict offline CSP (`default-src 'none'`),
  prominent non-collapsible `#ensabc-disclaimer-banner`, strictly well-formed inline
  XML `<svg id="ensabc-evolution-svg">` with 0 untagged geometric primitives, canonical
  `<script id="ensabc-evolution-provenance" type="application/json">`, interactive ES6
  `#ensabc-inspector-drawer` (hover, click, Enter/Space), and static fallback tables
  in `<details id="ensabc-provenance-ledger">`.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "m3.visualization.v1"
DISCLAIMER_VERSION = "m3.disclaimer.v1"
NON_PHYSICAL_TOKEN = "algorithmic_attribution_only_not_physical_member_migration"

DISCLAIMER_CLAUSES: list[dict[str, str]] = [
    {
        "id": "non_member_migration",
        "title": "非物理成员迁移 / Non-Physical Member Migration",
        "text": (
            "分裂/合并连线与 attributed_flow 仅是基于情景物理均值场距离和成员计数的算法归因，"
            "不代表集合成员跨起报发生物理迁移 "
            "(Split/merge edges and attributed_flow are algorithmic attributions from cluster "
            "mean-field distances and member counts; they do NOT represent physical ensemble "
            "member migration across forecast initializations)."
        ),
    },
    {
        "id": "non_physical_cyclogenesis",
        "title": "非物理新生/消亡 / Non-Physical Cyclogenesis or Lysis",
        "text": (
            "emergence 与 disappearance 仅表示当前距离门槛下无可行前驱或后继，"
            "不代表天气系统的物理生成或消亡 "
            "(Emergence and disappearance indicate only the absence of feasible counterparts "
            "within the active distance threshold; they do NOT represent physical cyclogenesis "
            "or physical lysis/dissipation of weather systems)."
        ),
    },
    {
        "id": "non_calibrated_probability",
        "title": "非校准概率或业务技巧 / Non-Calibrated Probability, Skill, or Trading Value",
        "text": (
            "member_fraction 为样本内成员比例，未经概率校准，不构成预报修正技巧或交易决策依据 "
            "(member_fraction is an uncalibrated within-ensemble sample proportion; it is NOT "
            "a calibrated probability and does NOT establish forecast skill improvement or "
            "trading/financial decision value)."
        ),
    },
]

BRANCH_PALETTE: tuple[str, ...] = (
    "#2563eb",
    "#0d9488",
    "#7c3aed",
    "#db2777",
    "#ea580c",
    "#0284c7",
    "#4f46e5",
    "#059669",
    "#9333ea",
    "#be123c",
    "#b45309",
    "#0f766e",
)


class MissingProvenanceError(Exception):
    """Raised when required M3 provenance data is missing, NULL, or non-finite."""

    def __init__(self, detail: str) -> None:
        msg = detail if detail.startswith("MISSING_PROVENANCE_FIELD") else f"MISSING_PROVENANCE_FIELD: {detail}"
        super().__init__(msg)


def is_finite_number(value: Any) -> bool:
    """Return True iff value is an int (not bool) or finite float."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    return False


def normalize_float(value: float | int) -> float:
    """Validate finite number and normalize -0.0 to +0.0."""
    if not is_finite_number(value):
        raise MissingProvenanceError(f"Non-finite numeric value: {value!r}")
    fval = float(value)
    return 0.0 if fval == 0.0 else fval


def normalize_floats(obj: Any) -> Any:
    """Recursively validate finite floats and normalize -0.0 to +0.0."""
    if isinstance(obj, bool) or obj is None or isinstance(obj, (int, str)):
        return obj
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise MissingProvenanceError(f"Non-finite float encountered: {obj!r}")
        return 0.0 if obj == 0.0 else obj
    if isinstance(obj, dict):
        return {str(k): normalize_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [normalize_floats(v) for v in obj]
    raise MissingProvenanceError(f"Unsupported provenance type: {type(obj).__name__}")


def canonical_json_str(obj: Any, *, indent: int | None = 2) -> str:
    """Serialize object as canonical sorted-key JSON, rejecting NaN/Infinity."""
    normalized = normalize_floats(obj)
    separators = (",", ":") if indent is None else None
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        indent=indent,
        separators=separators,
        allow_nan=False,
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | Path) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as fh:
        while chunk := fh.read(65536):
            h.update(chunk)
    return h.hexdigest()


def atomic_write(path: str | Path, content: str | bytes) -> None:
    """Atomically write content to target path via sibling temporary file + os.replace."""
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    encoding = None if isinstance(content, bytes) else "utf-8"
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=".tmp_m3_viz_", dir=str(target.parent))
    try:
        with os.fdopen(tmp_fd, mode, encoding=encoding) as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, str(target))
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def atomic_write_json(path: str | Path, obj: Any, *, indent: int | None = 2) -> None:
    atomic_write(path, canonical_json_str(obj, indent=indent) + "\n")


def _reject_json_constant(const: str) -> Any:
    raise MissingProvenanceError(f"Non-finite JSON constant in DuckDB result: {const}")


def parse_duckdb_json(stdout: str) -> list[Any]:
    """Decode concatenated JSON arrays emitted by `duckdb -json`."""
    decoder = json.JSONDecoder(parse_constant=_reject_json_constant)
    results: list[Any] = []
    idx = 0
    length = len(stdout)
    while idx < length:
        if stdout[idx].isspace():
            idx += 1
            continue
        value, idx = decoder.raw_decode(stdout, idx)
        results.append(value)
    return results


def run_isolated_duckdb(
    sql: str,
    *,
    database: str | Path = ":memory:",
    readonly: bool = False,
    cli: str = "duckdb",
    timeout: float = 60.0,
    bail: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Execute SQL via DuckDB CLI in an isolated temporary HOME and extension_directory."""
    with tempfile.TemporaryDirectory(prefix="ensabc-m3-duckdb-") as tmp_home:
        env = os.environ.copy()
        env["HOME"] = tmp_home
        prefix = (
            f"SET extension_directory = '{tmp_home}/extensions';\n"
            "SET autoinstall_known_extensions = false;\n"
            "SET autoload_known_extensions = false;\n"
            "SET TimeZone = 'UTC';\n"
        )
        cmd = [cli]
        if readonly:
            cmd.append("-readonly")
        cmd.extend(["-unsigned", "-json"])
        if bail:
            cmd.append("-bail")
        cmd.append(str(database))
        return subprocess.run(
            cmd,
            input=prefix + sql,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            check=False,
        )


def _sql_quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _require_str(row: dict[str, Any], key: str, ctx: str, *, allow_empty: bool = False) -> str:
    if key not in row or row[key] is None or not isinstance(row[key], str):
        raise MissingProvenanceError(f"{ctx}.{key} is missing or NULL")
    val = row[key]
    if not allow_empty and len(val.strip()) == 0:
        raise MissingProvenanceError(f"{ctx}.{key} is empty")
    return val


def _require_opt_str(row: dict[str, Any], key: str, ctx: str) -> str | None:
    if key not in row:
        raise MissingProvenanceError(f"{ctx}.{key} column missing")
    val = row[key]
    if val is None:
        return None
    if not isinstance(val, str) or len(val.strip()) == 0:
        raise MissingProvenanceError(f"{ctx}.{key} must be NULL or non-empty string")
    return val


def _require_int(row: dict[str, Any], key: str, ctx: str, *, min_val: int | None = None) -> int:
    if key not in row or row[key] is None or isinstance(row[key], bool) or not isinstance(row[key], int):
        raise MissingProvenanceError(f"{ctx}.{key} is missing, NULL, or not an integer")
    val = row[key]
    if min_val is not None and val < min_val:
        raise MissingProvenanceError(f"{ctx}.{key}={val} < {min_val}")
    return val


def _require_bool(row: dict[str, Any], key: str, ctx: str) -> bool:
    if key not in row or row[key] is None or not isinstance(row[key], bool):
        raise MissingProvenanceError(f"{ctx}.{key} is missing, NULL, or not a boolean")
    return row[key]


def _require_float(
    row: dict[str, Any],
    key: str,
    ctx: str,
    *,
    min_val: float | None = None,
    max_val: float | None = None,
    strictly_positive: bool = False,
) -> float:
    if key not in row or row[key] is None or not is_finite_number(row[key]):
        raise MissingProvenanceError(f"{ctx}.{key} is missing, NULL, or non-finite")
    val = normalize_float(row[key])
    if strictly_positive and val <= 0.0:
        raise MissingProvenanceError(f"{ctx}.{key}={val} must be > 0.0")
    if min_val is not None and val < min_val:
        raise MissingProvenanceError(f"{ctx}.{key}={val} < {min_val}")
    if max_val is not None and val > max_val:
        raise MissingProvenanceError(f"{ctx}.{key}={val} > {max_val}")
    return val


def _require_float_list(row: dict[str, Any], key: str, ctx: str, *, non_empty: bool = True) -> list[float]:
    if key not in row or row[key] is None or not isinstance(row[key], list):
        raise MissingProvenanceError(f"{ctx}.{key} is missing, NULL, or not a list")
    items = row[key]
    if non_empty and len(items) == 0:
        raise MissingProvenanceError(f"{ctx}.{key} list is empty")
    out: list[float] = []
    for i, item in enumerate(items):
        if item is None or not is_finite_number(item):
            raise MissingProvenanceError(f"{ctx}.{key}[{i}] is NULL or non-finite")
        out.append(normalize_float(item))
    return out


def _require_int_list(row: dict[str, Any], key: str, ctx: str) -> list[int]:
    if key not in row or row[key] is None or not isinstance(row[key], list):
        raise MissingProvenanceError(f"{ctx}.{key} is missing, NULL, or not an integer list")
    out: list[int] = []
    for i, item in enumerate(row[key]):
        if item is None or isinstance(item, bool) or not isinstance(item, int):
            raise MissingProvenanceError(f"{ctx}.{key}[{i}] is NULL or not an int")
        out.append(item)
    return out


def _require_str_list(row: dict[str, Any], key: str, ctx: str) -> list[str]:
    if key not in row or row[key] is None or not isinstance(row[key], list):
        raise MissingProvenanceError(f"{ctx}.{key} is missing, NULL, or not a string list")
    out: list[str] = []
    for i, item in enumerate(row[key]):
        if item is None or not isinstance(item, str) or len(item.strip()) == 0:
            raise MissingProvenanceError(f"{ctx}.{key}[{i}] is NULL or empty string")
        out.append(item)
    return out


def fetch_evolution_history(db_path: Path, evolution_run_id: str) -> dict[str, Any]:
    """Query the published M3 6 tables + `replay_batches` from DuckDB in read-only mode."""
    if not evolution_run_id or not evolution_run_id.strip():
        raise MissingProvenanceError("evolution_run_id is empty")
    return _fetch_evolution_history_packed(db_path, evolution_run_id)


def _fetch_evolution_history_packed(db_path: Path, evolution_run_id: str) -> dict[str, Any]:
    """Fetch all 7 table slices in a single guaranteed 1-row query (safe even when a table has 0 rows)."""
    rid_lit = _sql_quote(evolution_run_id)
    sql = f"""
    SELECT
        coalesce((
            SELECT list(r) FROM (
                SELECT
                    evolution_run_id,
                    tracking_run_key,
                    evolution_binding,
                    weights,
                    planned_batch_count,
                    chain_summary,
                    non_physical_disclaimer,
                    content_sha256
                FROM evolution_runs
                WHERE evolution_run_id = {rid_lit}
            ) r
        ), []) AS runs,
        coalesce((
            SELECT list(b ORDER BY b.ordinal ASC) FROM (
                SELECT
                    rb.run_key,
                    rb.ordinal,
                    strftime(rb.init_time, '%Y-%m-%dT%H:%M:%SZ') AS init_time,
                    rb.lead_hours,
                    rb.status,
                    rb.status_reason,
                    rb.input_sha256,
                    rb.om_sha256,
                    rb.dispersion,
                    rb.cluster_result_sha256
                FROM replay_batches rb
                JOIN evolution_runs er ON er.tracking_run_key = rb.run_key
                WHERE er.evolution_run_id = {rid_lit}
            ) b
        ), []) AS batches,
        coalesce((
            SELECT list(n ORDER BY n.ordinal ASC, n.cluster_id ASC) FROM (
                SELECT
                    evolution_run_id,
                    ordinal,
                    cluster_id,
                    canonical_rank,
                    branch_id,
                    mean_features,
                    member_count,
                    member_fraction,
                    assignment_reason,
                    in_degree_primary,
                    in_degree_secondary,
                    out_degree_primary,
                    out_degree_secondary,
                    incoming_role,
                    outgoing_role,
                    primary_delta_fraction,
                    has_primary_delta_fraction,
                    unattributed_inflow_fraction,
                    unattributed_outflow_fraction,
                    high_uncertainty
                FROM evolution_nodes
                WHERE evolution_run_id = {rid_lit}
            ) n
        ), []) AS nodes,
        coalesce((
            SELECT list(t ORDER BY t.prev_ordinal ASC) FROM (
                SELECT
                    evolution_run_id,
                    prev_ordinal,
                    curr_ordinal,
                    transition_status,
                    chain_broken,
                    gap_reason,
                    pair_scale,
                    threshold,
                    primary_edge_count,
                    secondary_split_edge_count,
                    secondary_merge_edge_count,
                    transition_tv_redistribution,
                    min_threshold_margin,
                    ambiguous_tie,
                    high_uncertainty,
                    (evolve_result IS NOT NULL) AS has_evolve_result,
                    (CASE WHEN evolve_result IS NULL THEN NULL ELSE to_json(evolve_result)::VARCHAR END) AS evolve_result_json
                FROM evolution_transitions
                WHERE evolution_run_id = {rid_lit}
            ) t
        ), []) AS transitions,
        coalesce((
            SELECT list(e ORDER BY e.prev_ordinal ASC, e.prev_cluster_id ASC, e.curr_cluster_id ASC) FROM (
                SELECT
                    evolution_run_id,
                    prev_ordinal,
                    prev_cluster_id,
                    curr_ordinal,
                    curr_cluster_id,
                    prev_branch_id,
                    curr_branch_id,
                    edge_role,
                    event_type,
                    rule_basis,
                    distance,
                    distance_over_scale,
                    prev_member_fraction,
                    curr_member_fraction,
                    edge_delta_fraction,
                    attributed_flow,
                    edge_strength,
                    threshold_margin,
                    threshold_margin_ratio,
                    primary_alternative_status,
                    has_optimality_margin,
                    optimality_margin,
                    optimality_margin_ratio,
                    has_secondary_margin,
                    secondary_margin,
                    secondary_margin_ratio,
                    ambiguous_tie,
                    high_uncertainty,
                    uncertainty_reasons,
                    non_physical_disclaimer
                FROM evolution_edges
                WHERE evolution_run_id = {rid_lit}
            ) e
        ), []) AS edges,
        coalesce((
            SELECT list(ev ORDER BY ev.prev_ordinal ASC, ev.event_index ASC) FROM (
                SELECT
                    evolution_run_id,
                    prev_ordinal,
                    event_index,
                    event_type,
                    prev_cluster_ids,
                    curr_cluster_ids,
                    branch_ids,
                    primary_branch_id,
                    rule_basis,
                    max_morphology_distance,
                    max_morphology_over_scale,
                    prev_fraction_sum,
                    curr_fraction_sum,
                    net_delta_fraction,
                    attributed_flow_sum,
                    has_min_margin_ratio,
                    min_margin_ratio,
                    ambiguous_tie,
                    high_uncertainty,
                    non_physical_disclaimer
                FROM evolution_events
                WHERE evolution_run_id = {rid_lit}
            ) ev
        ), []) AS events,
        coalesce((
            SELECT list(br ORDER BY br.birth_ordinal ASC, br.branch_id ASC) FROM (
                SELECT
                    evolution_run_id,
                    branch_id,
                    birth_ordinal,
                    end_ordinal,
                    node_count,
                    step_count,
                    birth_reason,
                    termination_reason,
                    cumulative_morphology_distance,
                    cumulative_morphology_over_scale,
                    max_step_morphology_distance,
                    max_step_morphology_over_scale,
                    end_to_end_morphology_distance,
                    initial_member_fraction,
                    final_member_fraction,
                    min_member_fraction,
                    max_member_fraction,
                    net_delta_fraction,
                    cumulative_abs_delta_fraction,
                    split_out_event_count,
                    spawned_secondary_branch_ids,
                    split_from_parent_branch_id,
                    merge_in_event_count,
                    absorbed_secondary_branch_ids,
                    merged_into_branch_id,
                    ambiguous_step_count,
                    high_uncertainty_step_count,
                    high_uncertainty_step_ratio
                FROM evolution_branch_summaries
                WHERE evolution_run_id = {rid_lit}
            ) br
        ), []) AS branches;
    """
    proc = run_isolated_duckdb(sql, database=db_path, readonly=True)
    if proc.returncode != 0:
        raise MissingProvenanceError(
            f"Failed querying published M3 history tables in {db_path}: {proc.stderr.strip()}"
        )
    parsed = parse_duckdb_json(proc.stdout)
    if not parsed or not parsed[0]:
        raise MissingProvenanceError(f"Empty DuckDB response for evolution_run_id={evolution_run_id}")
    return parsed[0][0]


def _validate_chain_summary(cs: Any) -> dict[str, Any]:
    if not isinstance(cs, dict):
        raise MissingProvenanceError("evolution_runs.chain_summary is missing or not a STRUCT")
    int_keys = (
        "ok_batch_count",
        "missing_batch_count",
        "failed_batch_count",
        "ok_transition_count",
        "gap_transition_count",
        "branch_count",
        "primary_edge_count",
        "secondary_split_edge_count",
        "secondary_merge_edge_count",
        "continuation_count",
        "split_count",
        "merge_count",
        "emergence_count",
        "disappearance_count",
        "gap_interrupted_count",
        "gap_restarted_count",
        "ambiguous_transition_count",
        "high_uncertainty_transition_count",
        "high_uncertainty_edge_count",
    )
    float_keys = (
        "cumulative_primary_distance",
        "max_edge_distance",
        "max_edge_distance_over_scale",
        "mean_transition_tv_redistribution",
        "max_transition_tv_redistribution",
        "high_uncertainty_transition_ratio",
        "high_uncertainty_edge_ratio",
    )
    out: dict[str, Any] = {}
    for k in int_keys:
        out[k] = _require_int(cs, k, "chain_summary", min_val=0)
    for k in float_keys:
        out[k] = _require_float(cs, k, "chain_summary", min_val=0.0)
    return out


def _validate_evolution_binding(eb: Any) -> tuple[dict[str, Any], str, str, str, str]:
    if not isinstance(eb, dict):
        raise MissingProvenanceError("evolution_runs.evolution_binding is missing or not a STRUCT")
    secondary_rule = _require_str(eb, "secondary_rule", "evolution_binding")
    flow_rule = _require_str(eb, "flow_rule", "evolution_binding")
    uncertainty_rule = _require_str(eb, "uncertainty_rule", "evolution_binding")
    _require_str(eb, "tracking_run_key", "evolution_binding")
    _require_str(eb, "input_manifest_sha256", "evolution_binding")
    _require_str(eb, "scenario_config_sha256", "evolution_binding")
    _require_str(eb, "match_config_sha256", "evolution_binding")
    _require_str(eb, "evolve_config_sha256", "evolution_binding")
    _require_str(eb, "weights_version", "evolution_binding")
    _require_float(eb, "fraction_sum_tolerance", "evolution_binding", min_val=0.0)
    _require_float(eb, "high_uncertainty_threshold_ratio", "evolution_binding", min_val=0.0)
    _require_float(eb, "high_uncertainty_optimality_ratio", "evolution_binding", min_val=0.0)
    _require_float(eb, "high_uncertainty_secondary_ratio", "evolution_binding", min_val=0.0)

    sw = eb.get("software")
    if not isinstance(sw, dict):
        raise MissingProvenanceError("evolution_binding.software is missing or not a STRUCT")
    algo_ver = _require_str(sw, "evolution_algorithm_version", "evolution_binding.software")
    normalized_eb = normalize_floats(eb)
    for k, v in normalized_eb.items():
        if v is None:
            raise MissingProvenanceError(f"evolution_binding.{k} is NULL")
    for k, v in normalized_eb["software"].items():
        if v is None or (isinstance(v, str) and not v.strip()):
            raise MissingProvenanceError(f"evolution_binding.software.{k} is NULL or empty")
    return normalized_eb, algo_ver, secondary_rule, flow_rule, uncertainty_rule


def build_view_manifest(raw: dict[str, Any], expected_run_id: str) -> dict[str, Any]:
    """Validate all 6 M3 tables + replay_batches and construct TraceableEvolutionViewManifest."""
    runs = raw.get("runs") or []
    if len(runs) != 1:
        raise MissingProvenanceError(
            f"evolution_run_id '{expected_run_id}' not found in evolution_runs (matched {len(runs)})"
        )
    run_row = runs[0]
    run_id = _require_str(run_row, "evolution_run_id", "evolution_runs")
    if run_id != expected_run_id:
        raise MissingProvenanceError(f"Run ID mismatch: expected {expected_run_id}, got {run_id}")
    tracking_run_key = _require_str(run_row, "tracking_run_key", "evolution_runs")
    planned_batch_count = _require_int(run_row, "planned_batch_count", "evolution_runs", min_val=4)
    if planned_batch_count > 8:
        raise MissingProvenanceError(f"planned_batch_count={planned_batch_count} outside 4..8")
    weights = _require_float_list(run_row, "weights", "evolution_runs", non_empty=True)
    non_phys = _require_str(run_row, "non_physical_disclaimer", "evolution_runs")
    content_sha256 = _require_str(run_row, "content_sha256", "evolution_runs")
    chain_summary = _validate_chain_summary(run_row.get("chain_summary"))
    binding, algo_ver, secondary_rule, flow_rule, uncertainty_rule = _validate_evolution_binding(
        run_row.get("evolution_binding")
    )

    raw_nodes = raw.get("nodes") or []
    ok_batch_count = int(chain_summary["ok_batch_count"])
    if ok_batch_count > 0 and len(raw_nodes) == 0:
        raise MissingProvenanceError(
            f"Zero nodes in evolution_nodes when ok_batch_count={ok_batch_count} for evolution_run_id={run_id}"
        )
    if ok_batch_count == 0 and len(raw_nodes) > 0:
        raise MissingProvenanceError(
            f"Found {len(raw_nodes)} nodes in evolution_nodes when ok_batch_count=0 for evolution_run_id={run_id}"
        )

    raw_batches = raw.get("batches") or []
    if len(raw_batches) != planned_batch_count:
        raise MissingProvenanceError(
            f"replay_batches count ({len(raw_batches)}) != planned_batch_count ({planned_batch_count})"
        )

    batches: list[dict[str, Any]] = []
    batch_status_by_ord: dict[int, str] = {}
    for idx, b in enumerate(raw_batches):
        b_ctx = f"replay_batches[{idx}]"
        b_rk = _require_str(b, "run_key", b_ctx)
        if b_rk != tracking_run_key:
            raise MissingProvenanceError(f"{b_ctx}.run_key={b_rk} != {tracking_run_key}")
        ordinal = _require_int(b, "ordinal", b_ctx, min_val=0)
        if ordinal != idx:
            raise MissingProvenanceError(f"{b_ctx}.ordinal={ordinal} expected {idx}")
        init_time = _require_str(b, "init_time", b_ctx)
        lead_hours = _require_int(b, "lead_hours", b_ctx)
        status = _require_str(b, "status", b_ctx)
        if status not in ("ok", "missing", "failed"):
            raise MissingProvenanceError(f"{b_ctx}.status={status!r} invalid")
        status_reason = _require_opt_str(b, "status_reason", b_ctx)
        if status == "ok":
            dispersion: float | None = _require_float(b, "dispersion", b_ctx, strictly_positive=True)
        else:
            if status_reason is None:
                raise MissingProvenanceError(f"{b_ctx}.status_reason required when status={status!r}")
            if b.get("dispersion") is not None:
                raise MissingProvenanceError(f"{b_ctx}.dispersion must be NULL when status={status!r}")
            dispersion = None
        batch_status_by_ord[ordinal] = status
        batches.append(
            {
                "ordinal": ordinal,
                "run_key": b_rk,
                "init_time": init_time,
                "lead_hours": lead_hours,
                "status": status,
                "status_reason": status_reason,
                "dispersion": dispersion,
                "input_sha256": _require_opt_str(b, "input_sha256", b_ctx),
                "om_sha256": _require_opt_str(b, "om_sha256", b_ctx),
                "cluster_result_sha256": _require_opt_str(b, "cluster_result_sha256", b_ctx),
            }
        )

    actual_ok_batches = sum(1 for b in batches if b["status"] == "ok")
    if actual_ok_batches != ok_batch_count:
        raise MissingProvenanceError(
            f"replay_batches ok count ({actual_ok_batches}) != chain_summary.ok_batch_count ({ok_batch_count})"
        )

    nodes: dict[str, dict[str, Any]] = {}
    feature_dim = len(weights)
    for idx, n in enumerate(raw_nodes):
        n_ctx = f"evolution_nodes[{idx}]"
        if _require_str(n, "evolution_run_id", n_ctx) != run_id:
            raise MissingProvenanceError(f"{n_ctx}.evolution_run_id mismatch")
        ordinal = _require_int(n, "ordinal", n_ctx, min_val=0)
        if batch_status_by_ord.get(ordinal) != "ok":
            raise MissingProvenanceError(
                f"{n_ctx} exists on non-ok batch ordinal={ordinal} (status={batch_status_by_ord.get(ordinal)})"
            )
        cluster_id = _require_int(n, "cluster_id", n_ctx, min_val=0)
        node_id = f"b{ordinal}:c{cluster_id}"
        if node_id in nodes:
            raise MissingProvenanceError(f"Duplicate node_id {node_id}")
        canonical_rank = _require_int(n, "canonical_rank", n_ctx, min_val=0)
        branch_id = _require_str(n, "branch_id", n_ctx)
        mean_features = _require_float_list(n, "mean_features", n_ctx, non_empty=True)
        if len(mean_features) != feature_dim:
            raise MissingProvenanceError(
                f"{n_ctx}.mean_features length ({len(mean_features)}) != weights length ({feature_dim})"
            )
        member_count = _require_int(n, "member_count", n_ctx, min_val=1)
        member_fraction = _require_float(
            n, "member_fraction", n_ctx, strictly_positive=True, max_val=1.0
        )
        assignment_reason = _require_str(n, "assignment_reason", n_ctx)
        in_deg_p = _require_int(n, "in_degree_primary", n_ctx, min_val=0)
        in_deg_s = _require_int(n, "in_degree_secondary", n_ctx, min_val=0)
        out_deg_p = _require_int(n, "out_degree_primary", n_ctx, min_val=0)
        out_deg_s = _require_int(n, "out_degree_secondary", n_ctx, min_val=0)
        if in_deg_p not in (0, 1) or out_deg_p not in (0, 1):
            raise MissingProvenanceError(f"{n_ctx} primary degree outside {{0,1}}")
        incoming_role = _require_str(n, "incoming_role", n_ctx)
        outgoing_role = _require_str(n, "outgoing_role", n_ctx)
        primary_delta_fraction = _require_float(n, "primary_delta_fraction", n_ctx)
        has_primary_delta = _require_bool(n, "has_primary_delta_fraction", n_ctx)
        unattr_in = _require_float(n, "unattributed_inflow_fraction", n_ctx, min_val=0.0)
        unattr_out = _require_float(n, "unattributed_outflow_fraction", n_ctx, min_val=0.0)
        high_unc = _require_bool(n, "high_uncertainty", n_ctx)

        nodes[node_id] = {
            "node_id": node_id,
            "evolution_run_id": run_id,
            "ordinal": ordinal,
            "cluster_id": cluster_id,
            "canonical_rank": canonical_rank,
            "branch_id": branch_id,
            "mean_features": mean_features,
            "member_count": member_count,
            "member_fraction": member_fraction,
            "assignment_reason": assignment_reason,
            "in_degree_primary": in_deg_p,
            "in_degree_secondary": in_deg_s,
            "out_degree_primary": out_deg_p,
            "out_degree_secondary": out_deg_s,
            "incoming_role": incoming_role,
            "outgoing_role": outgoing_role,
            "primary_delta_fraction": primary_delta_fraction,
            "has_primary_delta_fraction": has_primary_delta,
            "unattributed_inflow_fraction": unattr_in,
            "unattributed_outflow_fraction": unattr_out,
            "high_uncertainty": high_unc,
        }

    raw_transitions = raw.get("transitions") or []
    if len(raw_transitions) != planned_batch_count - 1:
        raise MissingProvenanceError(
            f"evolution_transitions count ({len(raw_transitions)}) != {planned_batch_count - 1}"
        )

    raw_events = raw.get("events") or []
    events_by_prev: dict[int, list[dict[str, Any]]] = {i: [] for i in range(planned_batch_count - 1)}
    for idx, ev in enumerate(raw_events):
        ev_ctx = f"evolution_events[{idx}]"
        if _require_str(ev, "evolution_run_id", ev_ctx) != run_id:
            raise MissingProvenanceError(f"{ev_ctx}.evolution_run_id mismatch")
        prev_ord = _require_int(ev, "prev_ordinal", ev_ctx, min_val=0)
        if prev_ord not in events_by_prev:
            raise MissingProvenanceError(f"{ev_ctx}.prev_ordinal={prev_ord} out of bounds")
        ev_index = _require_int(ev, "event_index", ev_ctx, min_val=0)
        ev_type = _require_str(ev, "event_type", ev_ctx)
        if ev_type not in (
            "continuation",
            "split",
            "merge",
            "emergence",
            "disappearance",
            "gap_interrupted",
            "gap_restarted",
        ):
            raise MissingProvenanceError(f"{ev_ctx}.event_type={ev_type!r} invalid")
        events_by_prev[prev_ord].append(
            {
                "evolution_run_id": run_id,
                "prev_ordinal": prev_ord,
                "event_index": ev_index,
                "event_type": ev_type,
                "prev_cluster_ids": _require_int_list(ev, "prev_cluster_ids", ev_ctx),
                "curr_cluster_ids": _require_int_list(ev, "curr_cluster_ids", ev_ctx),
                "branch_ids": _require_str_list(ev, "branch_ids", ev_ctx),
                "primary_branch_id": _require_opt_str(ev, "primary_branch_id", ev_ctx),
                "rule_basis": _require_str(ev, "rule_basis", ev_ctx),
                "max_morphology_distance": _require_float(ev, "max_morphology_distance", ev_ctx, min_val=0.0),
                "max_morphology_over_scale": _require_float(
                    ev, "max_morphology_over_scale", ev_ctx, min_val=0.0
                ),
                "prev_fraction_sum": _require_float(ev, "prev_fraction_sum", ev_ctx, min_val=0.0),
                "curr_fraction_sum": _require_float(ev, "curr_fraction_sum", ev_ctx, min_val=0.0),
                "net_delta_fraction": _require_float(ev, "net_delta_fraction", ev_ctx),
                "attributed_flow_sum": _require_float(ev, "attributed_flow_sum", ev_ctx, min_val=0.0),
                "has_min_margin_ratio": _require_bool(ev, "has_min_margin_ratio", ev_ctx),
                "min_margin_ratio": _require_float(ev, "min_margin_ratio", ev_ctx, min_val=0.0),
                "ambiguous_tie": _require_bool(ev, "ambiguous_tie", ev_ctx),
                "high_uncertainty": _require_bool(ev, "high_uncertainty", ev_ctx),
                "non_physical_disclaimer": _require_str(ev, "non_physical_disclaimer", ev_ctx),
            }
        )

    transitions: list[dict[str, Any]] = []
    transition_status_by_prev: dict[int, str] = {}
    for idx, t in enumerate(raw_transitions):
        t_ctx = f"evolution_transitions[{idx}]"
        if _require_str(t, "evolution_run_id", t_ctx) != run_id:
            raise MissingProvenanceError(f"{t_ctx}.evolution_run_id mismatch")
        prev_ord = _require_int(t, "prev_ordinal", t_ctx, min_val=0)
        curr_ord = _require_int(t, "curr_ordinal", t_ctx, min_val=1)
        if prev_ord != idx or curr_ord != prev_ord + 1:
            raise MissingProvenanceError(f"{t_ctx} invalid ordinal pair ({prev_ord}, {curr_ord})")
        t_status = _require_str(t, "transition_status", t_ctx)
        if t_status not in ("ok", "gap"):
            raise MissingProvenanceError(f"{t_ctx}.transition_status={t_status!r} invalid")
        chain_broken = _require_bool(t, "chain_broken", t_ctx)
        has_evolve_result = _require_bool(t, "has_evolve_result", t_ctx)
        gap_reason = _require_opt_str(t, "gap_reason", t_ctx)

        expected_ok = batch_status_by_ord[prev_ord] == "ok" and batch_status_by_ord[curr_ord] == "ok"
        if (t_status == "ok") != expected_ok:
            raise MissingProvenanceError(
                f"{t_ctx}.transition_status={t_status!r} inconsistent with batch statuses"
            )
        if chain_broken != (t_status == "gap"):
            raise MissingProvenanceError(f"{t_ctx}.chain_broken inconsistent with transition_status")
        if has_evolve_result != (t_status == "ok"):
            raise MissingProvenanceError(f"{t_ctx}.evolve_result presence inconsistent with transition_status")

        if t_status == "ok":
            if gap_reason is not None:
                raise MissingProvenanceError(f"{t_ctx}.gap_reason must be NULL when ok")
            pair_scale: float | None = _require_float(t, "pair_scale", t_ctx, strictly_positive=True)
            threshold: float | None = _require_float(t, "threshold", t_ctx, min_val=0.0)
            tv_redist: float | None = _require_float(
                t, "transition_tv_redistribution", t_ctx, min_val=0.0
            )
            min_thr_margin: float | None = _require_float(
                t, "min_threshold_margin", t_ctx, min_val=0.0
            )
        else:
            if gap_reason is None:
                raise MissingProvenanceError(f"{t_ctx}.gap_reason required when transition_status='gap'")
            pair_scale = None
            threshold = None
            tv_redist = None
            min_thr_margin = None

        transition_status_by_prev[prev_ord] = t_status
        transitions.append(
            {
                "evolution_run_id": run_id,
                "prev_ordinal": prev_ord,
                "curr_ordinal": curr_ord,
                "transition_status": t_status,
                "chain_broken": chain_broken,
                "gap_reason": gap_reason,
                "pair_scale": pair_scale,
                "threshold": threshold,
                "primary_edge_count": _require_int(t, "primary_edge_count", t_ctx, min_val=0),
                "secondary_split_edge_count": _require_int(
                    t, "secondary_split_edge_count", t_ctx, min_val=0
                ),
                "secondary_merge_edge_count": _require_int(
                    t, "secondary_merge_edge_count", t_ctx, min_val=0
                ),
                "transition_tv_redistribution": tv_redist,
                "min_threshold_margin": min_thr_margin,
                "ambiguous_tie": _require_bool(t, "ambiguous_tie", t_ctx),
                "high_uncertainty": _require_bool(t, "high_uncertainty", t_ctx),
                "has_evolve_result": has_evolve_result,
                "events": events_by_prev[prev_ord],
            }
        )

    raw_edges = raw.get("edges") or []
    edges: dict[str, dict[str, Any]] = {}
    for idx, e in enumerate(raw_edges):
        e_ctx = f"evolution_edges[{idx}]"
        if _require_str(e, "evolution_run_id", e_ctx) != run_id:
            raise MissingProvenanceError(f"{e_ctx}.evolution_run_id mismatch")
        prev_ord = _require_int(e, "prev_ordinal", e_ctx, min_val=0)
        prev_cid = _require_int(e, "prev_cluster_id", e_ctx, min_val=0)
        curr_ord = _require_int(e, "curr_ordinal", e_ctx, min_val=1)
        curr_cid = _require_int(e, "curr_cluster_id", e_ctx, min_val=0)
        if curr_ord != prev_ord + 1:
            raise MissingProvenanceError(f"{e_ctx} non-adjacent ordinals {prev_ord}->{curr_ord}")
        if transition_status_by_prev.get(prev_ord) != "ok":
            raise MissingProvenanceError(f"{e_ctx} crosses broken/gap transition prev_ordinal={prev_ord}")
        prev_nid = f"b{prev_ord}:c{prev_cid}"
        curr_nid = f"b{curr_ord}:c{curr_cid}"
        if prev_nid not in nodes or curr_nid not in nodes:
            raise MissingProvenanceError(f"{e_ctx} references missing node ({prev_nid} -> {curr_nid})")
        edge_id = f"{prev_nid}->{curr_nid}"
        if edge_id in edges:
            raise MissingProvenanceError(f"Duplicate edge_id {edge_id}")

        edge_role = _require_str(e, "edge_role", e_ctx)
        if edge_role not in ("primary", "secondary_split", "secondary_merge"):
            raise MissingProvenanceError(f"{e_ctx}.edge_role={edge_role!r} invalid")
        event_type = _require_str(e, "event_type", e_ctx)
        if event_type not in ("continuation", "split", "merge"):
            raise MissingProvenanceError(f"{e_ctx}.event_type={event_type!r} invalid")

        edges[edge_id] = {
            "edge_id": edge_id,
            "evolution_run_id": run_id,
            "prev_node_id": prev_nid,
            "curr_node_id": curr_nid,
            "prev_ordinal": prev_ord,
            "prev_cluster_id": prev_cid,
            "curr_ordinal": curr_ord,
            "curr_cluster_id": curr_cid,
            "prev_branch_id": _require_str(e, "prev_branch_id", e_ctx),
            "curr_branch_id": _require_str(e, "curr_branch_id", e_ctx),
            "edge_role": edge_role,
            "event_type": event_type,
            "rule_basis": _require_str(e, "rule_basis", e_ctx),
            "distance": _require_float(e, "distance", e_ctx, min_val=0.0),
            "distance_over_scale": _require_float(e, "distance_over_scale", e_ctx, min_val=0.0),
            "prev_member_fraction": _require_float(
                e, "prev_member_fraction", e_ctx, strictly_positive=True, max_val=1.0
            ),
            "curr_member_fraction": _require_float(
                e, "curr_member_fraction", e_ctx, strictly_positive=True, max_val=1.0
            ),
            "edge_delta_fraction": _require_float(e, "edge_delta_fraction", e_ctx),
            "attributed_flow": _require_float(e, "attributed_flow", e_ctx, strictly_positive=True),
            "edge_strength": _require_float(e, "edge_strength", e_ctx, min_val=0.0, max_val=1.0),
            "threshold_margin": _require_float(e, "threshold_margin", e_ctx, min_val=0.0),
            "threshold_margin_ratio": _require_float(
                e, "threshold_margin_ratio", e_ctx, min_val=0.0
            ),
            "primary_alternative_status": _require_str(e, "primary_alternative_status", e_ctx),
            "has_optimality_margin": _require_bool(e, "has_optimality_margin", e_ctx),
            "optimality_margin": _require_float(e, "optimality_margin", e_ctx, min_val=0.0),
            "optimality_margin_ratio": _require_float(
                e, "optimality_margin_ratio", e_ctx, min_val=0.0
            ),
            "has_secondary_margin": _require_bool(e, "has_secondary_margin", e_ctx),
            "secondary_margin": _require_float(e, "secondary_margin", e_ctx, min_val=0.0),
            "secondary_margin_ratio": _require_float(
                e, "secondary_margin_ratio", e_ctx, min_val=0.0
            ),
            "ambiguous_tie": _require_bool(e, "ambiguous_tie", e_ctx),
            "high_uncertainty": _require_bool(e, "high_uncertainty", e_ctx),
            "uncertainty_reasons": _require_str_list(e, "uncertainty_reasons", e_ctx),
            "non_physical_disclaimer": _require_str(e, "non_physical_disclaimer", e_ctx),
        }

    raw_branches = raw.get("branches") or []
    node_branch_ids = {nd["branch_id"] for nd in nodes.values()}
    if ok_batch_count > 0 and len(raw_branches) == 0:
        raise MissingProvenanceError("evolution_branch_summaries is empty when ok_batch_count > 0")
    if ok_batch_count == 0 and len(raw_branches) > 0:
        raise MissingProvenanceError("evolution_branch_summaries must be empty when ok_batch_count == 0")

    branches: dict[str, dict[str, Any]] = {}
    for idx, br in enumerate(raw_branches):
        br_ctx = f"evolution_branch_summaries[{idx}]"
        if _require_str(br, "evolution_run_id", br_ctx) != run_id:
            raise MissingProvenanceError(f"{br_ctx}.evolution_run_id mismatch")
        bid = _require_str(br, "branch_id", br_ctx)
        if bid in branches:
            raise MissingProvenanceError(f"Duplicate branch_id {bid} in evolution_branch_summaries")
        birth_ord = _require_int(br, "birth_ordinal", br_ctx, min_val=0)
        end_ord = _require_int(br, "end_ordinal", br_ctx, min_val=birth_ord)
        node_cnt = _require_int(br, "node_count", br_ctx, min_val=1)
        step_cnt = _require_int(br, "step_count", br_ctx, min_val=0)
        if step_cnt != node_cnt - 1:
            raise MissingProvenanceError(f"{br_ctx}.step_count != node_count - 1")
        branches[bid] = {
            "evolution_run_id": run_id,
            "branch_id": bid,
            "birth_ordinal": birth_ord,
            "end_ordinal": end_ord,
            "node_count": node_cnt,
            "step_count": step_cnt,
            "birth_reason": _require_str(br, "birth_reason", br_ctx),
            "termination_reason": _require_str(br, "termination_reason", br_ctx),
            "cumulative_morphology_distance": _require_float(
                br, "cumulative_morphology_distance", br_ctx, min_val=0.0
            ),
            "cumulative_morphology_over_scale": _require_float(
                br, "cumulative_morphology_over_scale", br_ctx, min_val=0.0
            ),
            "max_step_morphology_distance": _require_float(
                br, "max_step_morphology_distance", br_ctx, min_val=0.0
            ),
            "max_step_morphology_over_scale": _require_float(
                br, "max_step_morphology_over_scale", br_ctx, min_val=0.0
            ),
            "end_to_end_morphology_distance": _require_float(
                br, "end_to_end_morphology_distance", br_ctx, min_val=0.0
            ),
            "initial_member_fraction": _require_float(
                br, "initial_member_fraction", br_ctx, strictly_positive=True, max_val=1.0
            ),
            "final_member_fraction": _require_float(
                br, "final_member_fraction", br_ctx, strictly_positive=True, max_val=1.0
            ),
            "min_member_fraction": _require_float(
                br, "min_member_fraction", br_ctx, strictly_positive=True, max_val=1.0
            ),
            "max_member_fraction": _require_float(
                br, "max_member_fraction", br_ctx, strictly_positive=True, max_val=1.0
            ),
            "net_delta_fraction": _require_float(br, "net_delta_fraction", br_ctx),
            "cumulative_abs_delta_fraction": _require_float(
                br, "cumulative_abs_delta_fraction", br_ctx, min_val=0.0
            ),
            "split_out_event_count": _require_int(br, "split_out_event_count", br_ctx, min_val=0),
            "spawned_secondary_branch_ids": _require_str_list(
                br, "spawned_secondary_branch_ids", br_ctx
            ),
            "split_from_parent_branch_id": _require_opt_str(
                br, "split_from_parent_branch_id", br_ctx
            ),
            "merge_in_event_count": _require_int(br, "merge_in_event_count", br_ctx, min_val=0),
            "absorbed_secondary_branch_ids": _require_str_list(
                br, "absorbed_secondary_branch_ids", br_ctx
            ),
            "merged_into_branch_id": _require_opt_str(br, "merged_into_branch_id", br_ctx),
            "ambiguous_step_count": _require_int(br, "ambiguous_step_count", br_ctx, min_val=0),
            "high_uncertainty_step_count": _require_int(
                br, "high_uncertainty_step_count", br_ctx, min_val=0
            ),
            "high_uncertainty_step_ratio": _require_float(
                br, "high_uncertainty_step_ratio", br_ctx, min_val=0.0, max_val=1.0
            ),
        }

    if set(branches.keys()) != node_branch_ids:
        raise MissingProvenanceError(
            "evolution_branch_summaries branch_id set does not match evolution_nodes branch_id set"
        )

    _verify_evolve_result_flat_parity(raw_transitions, transitions, edges)

    return normalize_floats(
        {
            "schema_version": SCHEMA_VERSION,
            "run": {
                "evolution_run_id": run_id,
                "tracking_run_key": tracking_run_key,
                "content_sha256": content_sha256,
                "algorithm_version": algo_ver,
                "secondary_rule": secondary_rule,
                "flow_rule": flow_rule,
                "uncertainty_rule": uncertainty_rule,
                "planned_batch_count": planned_batch_count,
                "weights": weights,
                "non_physical_disclaimer": non_phys,
                "evolution_binding": binding,
                "chain_summary": chain_summary,
            },
            "disclaimer": {
                "version": DISCLAIMER_VERSION,
                "non_physical_token": NON_PHYSICAL_TOKEN,
                "clauses": DISCLAIMER_CLAUSES,
            },
            "batches": batches,
            "nodes": nodes,
            "edges": edges,
            "transitions": transitions,
            "branches": branches,
        }
    )


def _floats_close_15(a: Any, b: Any) -> bool:
    if not (is_finite_number(a) and is_finite_number(b)):
        return False
    return math.isclose(float(a), float(b), rel_tol=1e-15, abs_tol=1e-15)


def _verify_evolve_result_flat_parity(
    raw_transitions: list[dict[str, Any]],
    transitions: list[dict[str, Any]],
    edges: dict[str, dict[str, Any]],
) -> None:
    """Cross-check evolution_transitions.evolve_result JSON against flat transitions, edges, and events."""
    edges_by_prev: dict[int, list[dict[str, Any]]] = {}
    for ed in edges.values():
        edges_by_prev.setdefault(ed["prev_ordinal"], []).append(ed)
    for elist in edges_by_prev.values():
        elist.sort(key=lambda item: (item["prev_cluster_id"], item["curr_cluster_id"]))

    for idx, (raw_t, tr) in enumerate(zip(raw_transitions, transitions)):
        prev_ord = tr["prev_ordinal"]
        ctx = f"evolution_transitions[{idx}](prev_ordinal={prev_ord}).evolve_result"
        er_json = raw_t.get("evolve_result_json")
        if tr["transition_status"] == "gap":
            if er_json is not None:
                raise MissingProvenanceError(f"{ctx} must be NULL when transition_status='gap'")
            continue

        if not isinstance(er_json, str) or not er_json.strip():
            raise MissingProvenanceError(f"{ctx} JSON missing on ok transition")
        try:
            er = json.loads(er_json, parse_constant=_reject_json_constant)
        except Exception as exc:
            raise MissingProvenanceError(f"{ctx} invalid JSON: {exc}") from exc
        if not isinstance(er, dict):
            raise MissingProvenanceError(f"{ctx} is not a JSON object")

        esum = er.get("summary")
        if not isinstance(esum, dict):
            raise MissingProvenanceError(f"{ctx}.summary is missing or not an object")
        for int_k in ("primary_edge_count", "secondary_split_edge_count", "secondary_merge_edge_count"):
            if _require_int(esum, int_k, f"{ctx}.summary", min_val=0) != tr[int_k]:
                raise MissingProvenanceError(
                    f"{ctx}.summary.{int_k}={esum.get(int_k)!r} != flat {tr[int_k]!r}"
                )
        for flt_k in ("transition_tv_redistribution", "min_threshold_margin"):
            s_val = _require_float(esum, flt_k, f"{ctx}.summary", min_val=0.0)
            if not _floats_close_15(s_val, tr[flt_k]):
                raise MissingProvenanceError(
                    f"{ctx}.summary.{flt_k}={s_val!r} != flat {tr[flt_k]!r}"
                )
        for bool_k in ("ambiguous_tie", "high_uncertainty"):
            if _require_bool(esum, bool_k, f"{ctx}.summary") != tr[bool_k]:
                raise MissingProvenanceError(
                    f"{ctx}.summary.{bool_k}={esum.get(bool_k)!r} != flat {tr[bool_k]!r}"
                )

        raw_er_edges = er.get("edges")
        if not isinstance(raw_er_edges, list):
            raise MissingProvenanceError(f"{ctx}.edges is missing or not a list")
        flat_edges = edges_by_prev.get(prev_ord, [])
        if len(raw_er_edges) != len(flat_edges):
            raise MissingProvenanceError(
                f"{ctx}.edges length ({len(raw_er_edges)}) != flat evolution_edges count ({len(flat_edges)})"
            )
        sorted_er_edges = sorted(
            raw_er_edges,
            key=lambda item: (
                _require_int(item, "prev_cluster_id", f"{ctx}.edges") if isinstance(item, dict) else -1,
                _require_int(item, "curr_cluster_id", f"{ctx}.edges") if isinstance(item, dict) else -1,
            ),
        )
        for e_i, (ee, fe) in enumerate(zip(sorted_er_edges, flat_edges)):
            e_ctx = f"{ctx}.edges[{e_i}]"
            if not isinstance(ee, dict):
                raise MissingProvenanceError(f"{e_ctx} is not an object")
            if (
                _require_int(ee, "prev_cluster_id", e_ctx, min_val=0) != fe["prev_cluster_id"]
                or _require_int(ee, "curr_cluster_id", e_ctx, min_val=0) != fe["curr_cluster_id"]
                or _require_str(ee, "edge_role", e_ctx) != fe["edge_role"]
                or _require_str(ee, "event_type", e_ctx) != fe["event_type"]
                or not _floats_close_15(_require_float(ee, "distance", e_ctx, min_val=0.0), fe["distance"])
                or not _floats_close_15(
                    _require_float(ee, "attributed_flow", e_ctx, strictly_positive=True),
                    fe["attributed_flow"],
                )
            ):
                raise MissingProvenanceError(
                    f"{e_ctx} mismatch against flat evolution_edges ({fe['edge_id']})"
                )

        raw_er_events = er.get("events")
        if not isinstance(raw_er_events, list):
            raise MissingProvenanceError(f"{ctx}.events is missing or not a list")
        flat_events = sorted(
            [
                ev
                for ev in tr["events"]
                if ev["event_type"] not in ("gap_interrupted", "gap_restarted")
            ],
            key=lambda item: item["event_index"],
        )
        if len(raw_er_events) != len(flat_events):
            raise MissingProvenanceError(
                f"{ctx}.events length ({len(raw_er_events)}) != flat non-gap evolution_events count ({len(flat_events)})"
            )
        sorted_er_events = sorted(
            raw_er_events,
            key=lambda item: _require_int(item, "event_index", f"{ctx}.events")
            if isinstance(item, dict)
            else -1,
        )
        for ev_i, (eev, fev) in enumerate(zip(sorted_er_events, flat_events)):
            ev_ctx = f"{ctx}.events[{ev_i}]"
            if not isinstance(eev, dict):
                raise MissingProvenanceError(f"{ev_ctx} is not an object")
            if (
                _require_int(eev, "event_index", ev_ctx, min_val=0) != fev["event_index"]
                or _require_str(eev, "event_type", ev_ctx) != fev["event_type"]
                or _require_int_list(eev, "prev_cluster_ids", ev_ctx) != fev["prev_cluster_ids"]
                or _require_int_list(eev, "curr_cluster_ids", ev_ctx) != fev["curr_cluster_ids"]
                or _require_str(eev, "rule_basis", ev_ctx) != fev["rule_basis"]
                or not _floats_close_15(
                    _require_float(eev, "net_delta_fraction", ev_ctx), fev["net_delta_fraction"]
                )
                or not _floats_close_15(
                    _require_float(eev, "attributed_flow_sum", ev_ctx, min_val=0.0),
                    fev["attributed_flow_sum"],
                )
            ):
                raise MissingProvenanceError(
                    f"{ev_ctx} mismatch against flat evolution_events (event_index={fev['event_index']})"
                )


def _esc(val: Any) -> str:
    return html.escape(str(val), quote=True)


def _bool_attr(val: bool) -> str:
    return "true" if val else "false"


def _float_attr(val: float) -> str:
    return repr(normalize_float(val))


def _node_badges(node: dict[str, Any], event_flags: set[str]) -> list[tuple[str, str, str]]:
    """Return list of `(badge_kind, short_label, fill_hex)` for node visual badges."""
    badges: list[tuple[str, str, str]] = []
    in_role = node["incoming_role"].lower()
    out_role = node["outgoing_role"].lower()

    if "gap_restarted" in in_role or "gap_restarted" in event_flags:
        badges.append(("gap_restarted", "GAP RESTART", "#0369a1"))
    elif "emerg" in in_role or "emergence" in event_flags:
        badges.append(("emerged", "EMERGED", "#0f766e"))

    if "gap_interrupted" in out_role or "gap_interrupted" in event_flags:
        badges.append(("gap_interrupted", "GAP BREAK", "#b45309"))
    elif "disappear" in out_role or "disappearance" in event_flags:
        badges.append(("disappeared", "DISAPPEARED", "#64748b"))

    if node["high_uncertainty"]:
        badges.append(("high_uncertainty", "HIGH UNCERT", "#dc2626"))
    return badges


def _render_svg(manifest: dict[str, Any]) -> str:
    batches: list[dict[str, Any]] = manifest["batches"]
    nodes: dict[str, dict[str, Any]] = manifest["nodes"]
    edges: dict[str, dict[str, Any]] = manifest["edges"]
    transitions: list[dict[str, Any]] = manifest["transitions"]
    branches: dict[str, dict[str, Any]] = manifest["branches"]

    sorted_bids = sorted(branches.keys(), key=lambda b: (branches[b]["birth_ordinal"], b))
    branch_color: dict[str, str] = {
        bid: BRANCH_PALETTE[idx % len(BRANCH_PALETTE)] for idx, bid in enumerate(sorted_bids)
    }

    node_event_flags: dict[str, set[str]] = {nid: set() for nid in nodes}
    for tr in transitions:
        p_ord = tr["prev_ordinal"]
        c_ord = tr["curr_ordinal"]
        for ev in tr["events"]:
            etype = ev["event_type"]
            for pc in ev["prev_cluster_ids"]:
                nid = f"b{p_ord}:c{pc}"
                if nid in node_event_flags:
                    node_event_flags[nid].add(etype)
            for cc in ev["curr_cluster_ids"]:
                nid = f"b{c_ord}:c{cc}"
                if nid in node_event_flags:
                    node_event_flags[nid].add(etype)

    batch_count = len(batches)
    col_x0 = 44.0
    col_step = 212.0
    node_width = 128.0
    svg_width = int(col_x0 * 2 + (batch_count - 1) * col_step + node_width)
    top_y = 112.0
    fraction_height_scale = 360.0
    node_vgap = 28.0

    nodes_by_ord: dict[int, list[dict[str, Any]]] = {b["ordinal"]: [] for b in batches}
    for nd in nodes.values():
        nodes_by_ord[nd["ordinal"]].append(nd)
    for ord_idx in nodes_by_ord:
        nodes_by_ord[ord_idx].sort(key=lambda item: (item["canonical_rank"], item["cluster_id"]))

    max_nodes_in_col = max((len(v) for v in nodes_by_ord.values()), default=1)
    svg_height = int(top_y + fraction_height_scale + max(1, max_nodes_in_col) * node_vgap + 84.0)

    node_geom: dict[str, tuple[float, float, float, float]] = {}
    for b in batches:
        ord_idx = b["ordinal"]
        x = col_x0 + ord_idx * col_step
        cur_y = top_y
        for nd in nodes_by_ord[ord_idx]:
            h = fraction_height_scale * nd["member_fraction"]
            node_geom[nd["node_id"]] = (x, cur_y, node_width, h)
            cur_y += h + node_vgap

    # Compute vertical port offsets on node outflow (right) and inflow (left) sides
    out_edges_by_node: dict[str, list[dict[str, Any]]] = {nid: [] for nid in nodes}
    in_edges_by_node: dict[str, list[dict[str, Any]]] = {nid: [] for nid in nodes}
    for ed in edges.values():
        out_edges_by_node[ed["prev_node_id"]].append(ed)
        in_edges_by_node[ed["curr_node_id"]].append(ed)

    for nid, elist in out_edges_by_node.items():
        elist.sort(
            key=lambda e: (
                0 if e["edge_role"] == "primary" else 1,
                nodes[e["curr_node_id"]]["canonical_rank"],
                e["curr_cluster_id"],
            )
        )
    for nid, elist in in_edges_by_node.items():
        elist.sort(
            key=lambda e: (
                0 if e["edge_role"] == "primary" else 1,
                nodes[e["prev_node_id"]]["canonical_rank"],
                e["prev_cluster_id"],
            )
        )

    out_port: dict[str, tuple[float, float]] = {}
    in_port: dict[str, tuple[float, float]] = {}
    for nid, elist in out_edges_by_node.items():
        _, ny, _, _ = node_geom[nid]
        cursor = ny
        for ed in elist:
            band_h = fraction_height_scale * ed["attributed_flow"]
            out_port[ed["edge_id"]] = (cursor, cursor + band_h)
            cursor += band_h
    for nid, elist in in_edges_by_node.items():
        _, ny, _, _ = node_geom[nid]
        cursor = ny
        for ed in elist:
            band_h = fraction_height_scale * ed["attributed_flow"]
            in_port[ed["edge_id"]] = (cursor, cursor + band_h)
            cursor += band_h

    lines: list[str] = []
    lines.append(
        f'<svg id="ensabc-evolution-svg" xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {svg_width} {svg_height}" width="{svg_width}" height="{svg_height}" '
        f'role="img" aria-label="M3 Traceable Multi-to-Multi Scenario Evolution Flow">'
    )
    lines.append("  <defs>")
    lines.append(
        '    <pattern id="ensabc-gap-hatch" width="12" height="12" patternUnits="userSpaceOnUse" '
        'patternTransform="rotate(45)">'
    )
    lines.append('      <rect x="0" y="0" width="12" height="12" fill="#fffbeb" />')
    lines.append('      <line x1="0" y1="0" x2="0" y2="12" stroke="#f59e0b" stroke-width="4" stroke-opacity="0.32" />')
    lines.append("    </pattern>")
    lines.append("  </defs>")

    # Whitelisted structural chrome with explicit fixed IDs (never generic data-decorative trust)
    lines.append(
        f'  <rect id="ensabc-svg-bg" x="0" y="0" width="{svg_width}" height="{svg_height}" rx="12" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1" />'
    )
    lines.append('  <g id="ensabc-legend" class="evo-legend">')
    lines.append(
        f'    <rect x="16" y="12" width="{svg_width - 32}" height="38" rx="8" fill="#ffffff" stroke="#e2e8f0" stroke-width="1" />'
    )
    lines.append('    <line x1="30" y1="31" x2="62" y2="31" stroke="#2563eb" stroke-width="5" />')
    lines.append('    <text x="68" y="35" font-size="11" fill="#1e293b" font-weight="600">Primary Backbone</text>')
    lines.append(
        '    <line x1="186" y1="31" x2="218" y2="31" stroke="#64748b" stroke-width="3" stroke-dasharray="6,4" />'
    )
    lines.append(
        '    <text x="224" y="35" font-size="11" fill="#1e293b" font-weight="600">Secondary Split / Merge</text>'
    )
    lines.append(
        '    <line x1="382" y1="29" x2="414" y2="29" stroke="#dc2626" stroke-width="2" />'
    )
    lines.append(
        '    <line x1="382" y1="34" x2="414" y2="34" stroke="#f59e0b" stroke-width="2" />'
    )
    lines.append(
        '    <text x="420" y="35" font-size="11" fill="#991b1b" font-weight="600">High Uncertainty / Ambiguous Tie</text>'
    )
    lines.append('    <rect x="628" y="23" width="18" height="16" rx="3" fill="url(#ensabc-gap-hatch)" stroke="#d97706" />')
    lines.append('    <text x="652" y="35" font-size="11" fill="#92400e" font-weight="600">Gap Chain-Break Barrier</text>')
    lines.append("  </g>")
    lines.append('  <g id="ensabc-lane-guides" class="evo-lane-guides">')
    for b in batches:
        bx = col_x0 + b["ordinal"] * col_step
        lines.append(
            f'    <line x1="{bx + node_width / 2:.1f}" y1="96" x2="{bx + node_width / 2:.1f}" '
            f'y2="{svg_height - 20}" stroke="#e2e8f0" stroke-width="1" stroke-dasharray="3,4" />'
        )
    lines.append("  </g>")

    # Directed cubic-Bezier evolution ribbons (.evo-edge)
    lines.append('  <g class="evo-edges-layer">')
    for edge_id in sorted(edges.keys(), key=lambda k: (edges[k]["prev_ordinal"], edges[k]["prev_cluster_id"], edges[k]["curr_cluster_id"])):
        ed = edges[edge_id]
        px, _, pw, _ = node_geom[ed["prev_node_id"]]
        cx, _, _, _ = node_geom[ed["curr_node_id"]]
        y0_top, y0_bot = out_port[edge_id]
        y1_top, y1_bot = in_port[edge_id]
        x0 = px + pw
        x1 = cx
        dx = (x1 - x0) * 0.46
        c0x = x0 + dx
        c1x = x1 - dx

        ribbon_d = (
            f"M {x0:.2f} {y0_top:.2f} "
            f"C {c0x:.2f} {y0_top:.2f}, {c1x:.2f} {y1_top:.2f}, {x1:.2f} {y1_top:.2f} "
            f"L {x1:.2f} {y1_bot:.2f} "
            f"C {c1x:.2f} {y1_bot:.2f}, {c0x:.2f} {y0_bot:.2f}, {x0:.2f} {y0_bot:.2f} Z"
        )
        ym0 = 0.5 * (y0_top + y0_bot)
        ym1 = 0.5 * (y1_top + y1_bot)
        center_d = f"M {x0:.2f} {ym0:.2f} C {c0x:.2f} {ym0:.2f}, {c1x:.2f} {ym1:.2f}, {x1:.2f} {ym1:.2f}"

        role = ed["edge_role"]
        is_primary = role == "primary"
        color = branch_color.get(
            ed["prev_branch_id"] if role != "secondary_merge" else ed["curr_branch_id"],
            "#2563eb",
        )
        fill_opacity = "0.34" if is_primary else "0.16"
        dash_attr = "" if is_primary else ' stroke-dasharray="6,4"'
        stroke_w = "2.2" if is_primary else "1.8"
        warn_flag = ed["high_uncertainty"] or ed["ambiguous_tie"]

        role_cls = f"evo-edge edge-role-{_esc(role)}"
        if warn_flag:
            role_cls += " edge-warning"

        lines.append(
            f'    <g class="{role_cls}" tabindex="0" role="button" '
            f'data-edge-id="{_esc(edge_id)}" '
            f'data-evolution-run-id="{_esc(ed["evolution_run_id"])}" '
            f'data-prev-node-id="{_esc(ed["prev_node_id"])}" '
            f'data-curr-node-id="{_esc(ed["curr_node_id"])}" '
            f'data-edge-role="{_esc(role)}" '
            f'data-event-type="{_esc(ed["event_type"])}" '
            f'data-distance="{_float_attr(ed["distance"])}" '
            f'data-distance-over-scale="{_float_attr(ed["distance_over_scale"])}" '
            f'data-attributed-flow="{_float_attr(ed["attributed_flow"])}" '
            f'data-ambiguous-tie="{_bool_attr(ed["ambiguous_tie"])}" '
            f'data-high-uncertainty="{_bool_attr(ed["high_uncertainty"])}">'
        )
        lines.append(
            f"      <title>{_esc(edge_id)} | {_esc(role)} ({_esc(ed['event_type'])}) | "
            f"d={ed['distance']:.4f} | flow={ed['attributed_flow']:.4f} (algorithmic attribution)</title>"
        )
        if warn_flag:
            lines.append(
                f'      <path class="evo-edge-warning-outer" d="{ribbon_d}" fill="none" '
                f'stroke="#dc2626" stroke-width="4.2" stroke-opacity="0.72" />'
            )
            lines.append(
                f'      <path class="evo-edge-warning-inner" d="{ribbon_d}" fill="none" '
                f'stroke="#fbbf24" stroke-width="2.0" stroke-dasharray="4,3" />'
            )
        lines.append(
            f'      <path class="evo-edge-ribbon" d="{ribbon_d}" fill="{color}" '
            f'fill-opacity="{fill_opacity}" stroke="{color}" stroke-width="1.0"{dash_attr} />'
        )
        lines.append(
            f'      <path class="evo-edge-centerline" d="{center_d}" fill="none" '
            f'stroke="{color}" stroke-width="{stroke_w}"{dash_attr} />'
        )
        # Small directional chevron at t=0.55 on centerline
        mx = 0.125 * x0 + 0.375 * c0x + 0.375 * c1x + 0.125 * x1
        my = 0.5 * (ym0 + ym1)
        lines.append(
            f'      <polygon class="evo-edge-arrow" '
            f'points="{mx - 4:.2f},{my - 3.5:.2f} {mx + 4.5:.2f},{my:.2f} {mx - 4:.2f},{my + 3.5:.2f}" '
            f'fill="{color}" />'
        )
        lines.append("    </g>")
    lines.append("  </g>")

    # Batch columns (.evo-batch-col), gap markers (.evo-gap-marker), and nodes (.evo-node)
    lines.append('  <g class="evo-batches-layer">')
    for b in batches:
        ord_idx = b["ordinal"]
        status = b["status"]
        bx = col_x0 + ord_idx * col_step
        lines.append(
            f'    <g class="evo-batch-col" '
            f'data-batch-ordinal="{ord_idx}" '
            f'data-batch-status="{_esc(status)}" '
            f'data-init-time="{_esc(b["init_time"])}" '
            f'data-lead-hours="{b["lead_hours"]}">'
        )
        # Batch column header owned by data-batch-ordinal
        hdr_stroke = "#94a3b8" if status == "ok" else "#d97706"
        hdr_fill = "#ffffff" if status == "ok" else "#fffbeb"
        lines.append(f'      <g id="ensabc-batch-header-{ord_idx}" class="evo-batch-header">')
        lines.append(
            f'        <rect x="{bx:.2f}" y="58" width="{node_width:.2f}" height="38" rx="6" '
            f'fill="{hdr_fill}" stroke="{hdr_stroke}" stroke-width="1.2" />'
        )
        lines.append(
            f'        <text x="{bx + 8:.2f}" y="74" font-size="11" font-weight="700" fill="#0f172a">'
            f'b{ord_idx} · +{b["lead_hours"]}h ({_esc(status.upper())})</text>'
        )
        lines.append(
            f'        <text x="{bx + 8:.2f}" y="89" font-size="9.5" fill="#475569">'
            f'{_esc(b["init_time"][:16].replace("T", " "))}Z</text>'
        )
        lines.append("      </g>")

        if status in ("missing", "failed"):
            reason = b["status_reason"] or status.upper()
            barrier_h = svg_height - top_y - 26.0
            lines.append(
                f'      <g class="evo-gap-marker" '
                f'data-batch-ordinal="{ord_idx}" '
                f'data-status="{_esc(status)}" '
                f'data-status-reason="{_esc(reason)}">'
            )
            lines.append(
                f"        <title>Chain Break at b{ord_idx}: status={_esc(status)} ({_esc(reason)}). "
                f"Zero cross-gap edges permitted.</title>"
            )
            lines.append(
                f'        <rect x="{bx + 10:.2f}" y="{top_y:.2f}" width="{node_width - 20:.2f}" '
                f'height="{barrier_h:.2f}" rx="8" fill="url(#ensabc-gap-hatch)" '
                f'stroke="#d97706" stroke-width="2" stroke-dasharray="7,4" />'
            )
            mid_x = bx + node_width / 2.0
            lines.append(
                f'        <line x1="{mid_x:.2f}" y1="{top_y + 12:.2f}" x2="{mid_x:.2f}" '
                f'y2="{top_y + barrier_h - 12:.2f}" stroke="#b45309" stroke-width="2" stroke-dasharray="4,4" />'
            )
            lines.append(
                f'        <polygon points="{mid_x:.2f},{top_y + 28:.2f} {mid_x + 12:.2f},{top_y + 48:.2f} '
                f'{mid_x - 12:.2f},{top_y + 48:.2f}" fill="#f59e0b" stroke="#92400e" stroke-width="1.2" />'
            )
            lines.append(
                f'        <text x="{mid_x:.2f}" y="{top_y + 68:.2f}" text-anchor="middle" '
                f'font-size="11" font-weight="700" fill="#78350f">CHAIN BROKEN</text>'
            )
            lines.append(
                f'        <text x="{mid_x:.2f}" y="{top_y + 84:.2f}" text-anchor="middle" '
                f'font-size="10" font-weight="600" fill="#92400e">{_esc(status.upper())}</text>'
            )
            lines.append(
                f'        <text x="{mid_x:.2f}" y="{top_y + 100:.2f}" text-anchor="middle" '
                f'font-size="9.5" fill="#78350f">{_esc(reason[:20])}</text>'
            )
            lines.append("      </g>")
        else:
            for nd in nodes_by_ord[ord_idx]:
                nid = nd["node_id"]
                nx, ny, nw, nh = node_geom[nid]
                bcolor = branch_color.get(nd["branch_id"], "#2563eb")
                hu = nd["high_uncertainty"]
                border_color = "#dc2626" if hu else "#334155"
                border_w = "2.2" if hu else "1.2"
                badges = _node_badges(nd, node_event_flags[nid])
                badge_classes = " ".join(f"node-badge-{bk}" for bk, _, _ in badges)
                node_cls = f"evo-node {badge_classes}".strip()

                lines.append(
                    f'      <g class="{node_cls}" tabindex="0" role="button" '
                    f'data-node-id="{_esc(nid)}" '
                    f'data-evolution-run-id="{_esc(nd["evolution_run_id"])}" '
                    f'data-batch-index="{nd["ordinal"]}" '
                    f'data-cluster-id="{nd["cluster_id"]}" '
                    f'data-canonical-rank="{nd["canonical_rank"]}" '
                    f'data-branch-id="{_esc(nd["branch_id"])}" '
                    f'data-incoming-role="{_esc(nd["incoming_role"])}" '
                    f'data-outgoing-role="{_esc(nd["outgoing_role"])}" '
                    f'data-member-count="{nd["member_count"]}" '
                    f'data-member-fraction="{_float_attr(nd["member_fraction"])}" '
                    f'data-high-uncertainty="{_bool_attr(hu)}">'
                )
                lines.append(
                    f"        <title>{_esc(nid)} | branch={_esc(nd['branch_id'])} | "
                    f"members={nd['member_count']} ({nd['member_fraction'] * 100:.1f}%) | "
                    f"in={_esc(nd['incoming_role'])}, out={_esc(nd['outgoing_role'])}</title>"
                )
                # Main node rectangle with height strictly proportional to member_fraction
                lines.append(
                    f'        <rect class="evo-node-body" x="{nx:.2f}" y="{ny:.2f}" '
                    f'width="{nw:.2f}" height="{nh:.4f}" rx="4" fill="#ffffff" '
                    f'stroke="{border_color}" stroke-width="{border_w}" />'
                )
                lines.append(
                    f'        <rect class="evo-node-branch-accent" x="{nx:.2f}" y="{ny:.2f}" '
                    f'width="7.0" height="{nh:.4f}" rx="2" fill="{bcolor}" />'
                )
                if hu:
                    lines.append(
                        f'        <rect class="evo-node-uncertainty-ring" x="{nx - 2.5:.2f}" '
                        f'y="{ny - 2.5:.2f}" width="{nw + 5.0:.2f}" height="{nh + 5.0:.4f}" '
                        f'rx="6" fill="none" stroke="#f59e0b" stroke-width="1.5" stroke-dasharray="4,3" />'
                    )
                short_branch = nd["branch_id"].split(":", 1)[-1]
                label_y = ny + min(max(13.0, nh * 0.42), 18.0)
                lines.append(
                    f'        <text x="{nx + 12:.2f}" y="{label_y:.2f}" font-size="10.5" '
                    f'font-weight="700" fill="#0f172a">{_esc(nid)} · {_esc(short_branch)}</text>'
                )
                if nh >= 30.0:
                    lines.append(
                        f'        <text x="{nx + 12:.2f}" y="{label_y + 13:.2f}" font-size="9.5" '
                        f'fill="#334155">n={nd["member_count"]} ({nd["member_fraction"] * 100:.1f}%)</text>'
                    )
                # Visual badges stacked cleanly at top-right of node card
                bx_cursor = nx + nw - 4.0
                for b_idx, (bkind, blabel, bhex) in enumerate(badges):
                    bw = float(len(blabel) * 5.4 + 8.0)
                    by_badge = ny - 13.0 if nh < 46.0 else ny + 4.0 + b_idx * 13.0
                    bx_pos = bx_cursor - bw if nh >= 46.0 else nx + b_idx * (bw + 3.0)
                    lines.append(
                        f'        <g class="evo-node-badge" data-badge-kind="{_esc(bkind)}">'
                    )
                    lines.append(
                        f'          <rect x="{bx_pos:.2f}" y="{by_badge:.2f}" width="{bw:.2f}" '
                        f'height="11.5" rx="3" fill="{bhex}" />'
                    )
                    lines.append(
                        f'          <text x="{bx_pos + bw / 2.0:.2f}" y="{by_badge + 8.5:.2f}" '
                        f'text-anchor="middle" font-size="7.5" font-weight="700" fill="#ffffff">'
                        f'{_esc(blabel)}</text>'
                    )
                    lines.append("        </g>")
                lines.append("      </g>")

        lines.append("    </g>")
    lines.append("  </g>")
    lines.append("</svg>")
    return "\n".join(lines)


def _render_ledger_tables(manifest: dict[str, Any]) -> str:
    run = manifest["run"]
    cs = run["chain_summary"]
    batches = manifest["batches"]
    nodes = manifest["nodes"]
    edges = manifest["edges"]
    transitions = manifest["transitions"]
    branches = manifest["branches"]

    parts: list[str] = []
    parts.append('<details id="ensabc-provenance-ledger" class="ledger-panel">')
    parts.append(
        "  <summary>Static Provenance Ledger (No-JS Complete Ground-Truth Audit Tables: "
        f"{len(batches)} Batches, {len(nodes)} Nodes, {len(edges)} Edges, "
        f"{len(transitions)} Transitions, {len(branches)} Branches)</summary>"
    )
    parts.append('  <div class="ledger-body">')

    # 1. Run Summary
    parts.append("    <h3>1. Run &amp; Chain Summary (`evolution_runs`)</h3>")
    parts.append('    <table class="ledger-table"><tbody>')
    for label, val in (
        ("evolution_run_id", run["evolution_run_id"]),
        ("tracking_run_key", run["tracking_run_key"]),
        ("content_sha256", run["content_sha256"]),
        ("evolution_algorithm_version", run["algorithm_version"]),
        ("secondary_rule", run["secondary_rule"]),
        ("flow_rule", run["flow_rule"]),
        ("uncertainty_rule", run["uncertainty_rule"]),
        ("non_physical_disclaimer", run["non_physical_disclaimer"]),
        (
            "batch_counts (planned / ok / missing / failed)",
            f"{run['planned_batch_count']} / {cs['ok_batch_count']} / {cs['missing_batch_count']} / {cs['failed_batch_count']}",
        ),
        (
            "edge_counts (primary / secondary_split / secondary_merge)",
            f"{cs['primary_edge_count']} / {cs['secondary_split_edge_count']} / {cs['secondary_merge_edge_count']}",
        ),
        (
            "events (cont / split / merge / emerg / disapp / gap_int / gap_rest)",
            f"{cs['continuation_count']} / {cs['split_count']} / {cs['merge_count']} / "
            f"{cs['emergence_count']} / {cs['disappearance_count']} / "
            f"{cs['gap_interrupted_count']} / {cs['gap_restarted_count']}",
        ),
    ):
        parts.append(f"      <tr><th>{_esc(label)}</th><td><code>{_esc(val)}</code></td></tr>")
    parts.append("    </tbody></table>")

    # 2. Batches
    parts.append("    <h3>2. Planned Forecast Batches (`replay_batches`)</h3>")
    parts.append(
        '    <table class="ledger-table"><thead><tr>'
        "<th>Ordinal</th><th>Init Time (UTC)</th><th>Lead (h)</th><th>Status</th>"
        "<th>Reason</th><th>Dispersion</th></tr></thead><tbody>"
    )
    for b in batches:
        disp_txt = "NULL" if b["dispersion"] is None else f"{b['dispersion']:.6g}"
        parts.append(
            f"      <tr><td>{b['ordinal']}</td><td><code>{_esc(b['init_time'])}</code></td>"
            f"<td>{b['lead_hours']}</td><td><code>{_esc(b['status'])}</code></td>"
            f"<td>{_esc(b['status_reason'] or '-')}</td><td><code>{_esc(disp_txt)}</code></td></tr>"
        )
    parts.append("    </tbody></table>")

    # 3. Nodes
    parts.append("    <h3>3. Scenario Evolution Nodes (`evolution_nodes`)</h3>")
    parts.append(
        '    <table class="ledger-table"><thead><tr>'
        "<th>Node ID</th><th>Rank</th><th>Branch ID</th><th>Members</th><th>Fraction</th>"
        "<th>Incoming Role</th><th>Outgoing Role</th><th>Primary Δf</th>"
        "<th>Unattr In / Out</th><th>High Unc</th></tr></thead><tbody>"
    )
    for nid in sorted(nodes.keys(), key=lambda k: (nodes[k]["ordinal"], nodes[k]["cluster_id"])):
        nd = nodes[nid]
        pdelta = (
            f"{nd['primary_delta_fraction']:+.6g}" if nd["has_primary_delta_fraction"] else "N/A (0.0)"
        )
        parts.append(
            f"      <tr><td><code>{_esc(nid)}</code></td><td>{nd['canonical_rank']}</td>"
            f"<td><code>{_esc(nd['branch_id'])}</code></td><td>{nd['member_count']}</td>"
            f"<td>{nd['member_fraction']:.6g}</td><td><code>{_esc(nd['incoming_role'])}</code></td>"
            f"<td><code>{_esc(nd['outgoing_role'])}</code></td><td><code>{_esc(pdelta)}</code></td>"
            f"<td>{nd['unattributed_inflow_fraction']:.4g} / {nd['unattributed_outflow_fraction']:.4g}</td>"
            f"<td><code>{_bool_attr(nd['high_uncertainty'])}</code></td></tr>"
        )
    parts.append("    </tbody></table>")

    # 4. Edges
    parts.append("    <h3>4. Multi-to-Multi Evolution Edges (`evolution_edges`)</h3>")
    parts.append(
        '    <table class="ledger-table"><thead><tr>'
        "<th>Edge ID</th><th>Role</th><th>Event</th><th>Rule Basis</th>"
        "<th>d (d/S)</th><th>Flow (Alg.)</th><th>Thr Margin</th><th>Opt Margin</th>"
        "<th>Sec Margin</th><th>Tie / High Unc</th><th>Reasons</th></tr></thead><tbody>"
    )
    for eid in sorted(edges.keys(), key=lambda k: (edges[k]["prev_ordinal"], edges[k]["prev_cluster_id"], edges[k]["curr_cluster_id"])):
        ed = edges[eid]
        opt_txt = f"{ed['optimality_margin']:.6g}" if ed["has_optimality_margin"] else "none"
        sec_txt = f"{ed['secondary_margin']:.6g}" if ed["has_secondary_margin"] else "none"
        reasons_txt = ", ".join(ed["uncertainty_reasons"]) if ed["uncertainty_reasons"] else "-"
        parts.append(
            f"      <tr><td><code>{_esc(eid)}</code></td><td><code>{_esc(ed['edge_role'])}</code></td>"
            f"<td><code>{_esc(ed['event_type'])}</code></td><td><code>{_esc(ed['rule_basis'])}</code></td>"
            f"<td>{ed['distance']:.6g} ({ed['distance_over_scale']:.4g})</td>"
            f"<td>{ed['attributed_flow']:.6g}</td><td>{ed['threshold_margin']:.6g}</td>"
            f"<td>{_esc(opt_txt)}</td><td>{_esc(sec_txt)}</td>"
            f"<td><code>{_bool_attr(ed['ambiguous_tie'])} / {_bool_attr(ed['high_uncertainty'])}</code></td>"
            f"<td><code>{_esc(reasons_txt)}</code></td></tr>"
        )
    parts.append("    </tbody></table>")

    # 5. Branch Summaries
    parts.append("    <h3>5. Branch Lifecycle Summaries (`evolution_branch_summaries`)</h3>")
    parts.append(
        '    <table class="ledger-table"><thead><tr>'
        "<th>Branch ID</th><th>Span</th><th>Nodes</th><th>Birth / Term</th>"
        "<th>Cumul d (End-to-End)</th><th>Init → Final f (Net Δf)</th>"
        "<th>Split Out / Merge In</th><th>High Unc Steps</th></tr></thead><tbody>"
    )
    for bid in sorted(branches.keys(), key=lambda k: (branches[k]["birth_ordinal"], k)):
        br = branches[bid]
        parts.append(
            f"      <tr><td><code>{_esc(bid)}</code></td>"
            f"<td>b{br['birth_ordinal']}..b{br['end_ordinal']}</td><td>{br['node_count']}</td>"
            f"<td><code>{_esc(br['birth_reason'])}</code> → <code>{_esc(br['termination_reason'])}</code></td>"
            f"<td>{br['cumulative_morphology_distance']:.6g} ({br['end_to_end_morphology_distance']:.6g})</td>"
            f"<td>{br['initial_member_fraction']:.4g} → {br['final_member_fraction']:.4g} ({br['net_delta_fraction']:+.4g})</td>"
            f"<td>{br['split_out_event_count']} / {br['merge_in_event_count']}</td>"
            f"<td>{br['high_uncertainty_step_count']} ({br['high_uncertainty_step_ratio']:.3g})</td></tr>"
        )
    parts.append("    </tbody></table>")

    parts.append("  </div>")
    parts.append("</details>")
    return "\n".join(parts)


def safe_embedded_json_str(obj: Any) -> str:
    """Serialize object as canonical compact JSON and escape &, <, > for safe <script> embedding."""
    normalized = normalize_floats(obj)
    raw_json = json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )
    return (
        raw_json.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )


def render_evolution_document(manifest: dict[str, Any]) -> str:
    """Render the complete offline single-file HTML document."""
    run = manifest["run"]
    cs = run["chain_summary"]
    provenance_json = safe_embedded_json_str(manifest)

    svg_markup = _render_svg(manifest)
    ledger_markup = _render_ledger_tables(manifest)

    clauses_html = "".join(
        f'<li data-clause-id="{_esc(c["id"])}"><strong>{_esc(c["title"])}：</strong>{_esc(c["text"])}</li>'
        for c in manifest["disclaimer"]["clauses"]
    )

    # Inline JS uses strictly block comments / clean statements with zero double-slash tokens.
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline';">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ENSABC M3 Scenario Evolution View — {_esc(run["evolution_run_id"][:16])}</title>
  <style>
    :root {{
      --bg-page: #f1f5f9;
      --bg-card: #ffffff;
      --text-main: #0f172a;
      --text-muted: #475569;
      --border: #cbd5e1;
      --accent: #2563eb;
      --warn-bg: #fffbeb;
      --warn-border: #d97706;
      --warn-text: #78350f;
      --danger: #dc2626;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      --sans: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 16px 20px 32px;
      font-family: var(--sans);
      background: var(--bg-page);
      color: var(--text-main);
      line-height: 1.45;
    }}
    #ensabc-disclaimer-banner {{
      display: block;
      background: var(--warn-bg);
      border: 1.5px solid var(--warn-border);
      border-left-width: 6px;
      border-radius: 10px;
      padding: 12px 16px;
      margin-bottom: 14px;
      color: var(--warn-text);
    }}
    #ensabc-disclaimer-banner h1 {{
      margin: 0 0 6px;
      font-size: 15px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }}
    #ensabc-disclaimer-banner ul {{
      margin: 0;
      padding-left: 20px;
      font-size: 12.5px;
    }}
    #ensabc-disclaimer-banner li {{ margin-bottom: 3px; }}
    .meta-strip {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 14px;
    }}
    .meta-pill {{
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 6px 10px;
      font-size: 12px;
    }}
    .meta-pill code {{
      font-family: var(--mono);
      font-size: 11.5px;
      color: #1e293b;
    }}
    .workspace {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) 410px;
      gap: 16px;
      align-items: start;
    }}
    @media (max-width: 1120px) {{
      .workspace {{ grid-template-columns: 1fr; }}
    }}
    .canvas-card {{
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      overflow-x: auto;
    }}
    #ensabc-inspector-drawer {{
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 16px;
      position: sticky;
      top: 12px;
      max-height: calc(100vh - 28px);
      overflow-y: auto;
    }}
    #ensabc-inspector-drawer h2 {{
      margin: 0 0 8px;
      font-size: 15px;
    }}
    .inspector-sec {{
      border-top: 1px solid #e2e8f0;
      padding-top: 8px;
      margin-top: 8px;
    }}
    .inspector-sec h3 {{
      margin: 0 0 5px;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.03em;
      color: var(--text-muted);
    }}
    .kv-grid {{
      display: grid;
      grid-template-columns: 152px minmax(0, 1fr);
      gap: 3px 8px;
      font-size: 12px;
    }}
    .kv-grid dt {{
      color: var(--text-muted);
      font-weight: 500;
    }}
    .kv-grid dd {{
      margin: 0;
      font-family: var(--mono);
      word-break: break-all;
    }}
    .evo-node, .evo-edge {{
      cursor: pointer;
      transition: opacity 0.14s ease;
    }}
    .evo-node:focus-visible, .evo-edge:focus-visible {{
      outline: 2.5px solid var(--accent);
      outline-offset: 2px;
    }}
    svg.has-highlight .evo-node.is-dimmed,
    svg.has-highlight .evo-edge.is-dimmed {{
      opacity: 0.2;
    }}
    svg.has-highlight .evo-node.is-active .evo-node-body {{
      stroke-width: 3px;
      stroke: #0f172a;
    }}
    svg.has-highlight .evo-edge.is-active .evo-edge-ribbon {{
      fill-opacity: 0.58;
    }}
    .pin-badge {{
      display: inline-block;
      font-size: 11px;
      padding: 2px 7px;
      border-radius: 999px;
      background: #dbeafe;
      color: #1e40af;
      font-weight: 600;
    }}
    .ledger-panel {{
      margin-top: 16px;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 16px;
    }}
    .ledger-panel summary {{
      cursor: pointer;
      font-weight: 700;
      font-size: 13.5px;
    }}
    .ledger-body {{
      margin-top: 10px;
      overflow-x: auto;
    }}
    .ledger-body h3 {{
      font-size: 13px;
      margin: 14px 0 6px;
    }}
    .ledger-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 11.5px;
    }}
    .ledger-table th, .ledger-table td {{
      border: 1px solid #e2e8f0;
      padding: 5px 8px;
      text-align: left;
      vertical-align: top;
    }}
    .ledger-table th {{
      background: #f8fafc;
      font-weight: 600;
    }}
  </style>
</head>
<body>
  <header id="ensabc-disclaimer-banner" data-disclaimer-version="{_esc(DISCLAIMER_VERSION)}" role="region" aria-label="Interpretation Boundary Disclaimer">
    <h1>
      <span>M3 情景演化分析 · 算法解释边界声明 (Algorithmic Attribution Boundary — Non-Physical &amp; Non-Calibrated)</span>
      <code>{_esc(NON_PHYSICAL_TOKEN)}</code>
    </h1>
    <ul>{clauses_html}</ul>
  </header>

  <section class="meta-strip" aria-label="Evolution Run Summary">
    <div class="meta-pill">Run ID: <code>{_esc(run["evolution_run_id"])}</code></div>
    <div class="meta-pill">M2 Tracking Key: <code>{_esc(run["tracking_run_key"])}</code></div>
    <div class="meta-pill">Content SHA-256: <code>{_esc(run["content_sha256"][:16])}…</code></div>
    <div class="meta-pill">Rules: <code>{_esc(run["secondary_rule"])}</code> · <code>{_esc(run["flow_rule"])}</code> · <code>{_esc(run["uncertainty_rule"])}</code></div>
    <div class="meta-pill">Batches (OK / Missing / Failed): <code>{cs["ok_batch_count"]} / {cs["missing_batch_count"]} / {cs["failed_batch_count"]}</code></div>
    <div class="meta-pill">Edges (Primary / Split / Merge): <code>{cs["primary_edge_count"]} / {cs["secondary_split_edge_count"]} / {cs["secondary_merge_edge_count"]}</code></div>
    <div class="meta-pill">High-Uncertainty Edges: <code>{cs["high_uncertainty_edge_count"]} ({cs["high_uncertainty_edge_ratio"]:.3f})</code></div>
  </section>

  <main class="workspace">
    <div class="canvas-card">
{svg_markup}
    </div>

    <aside id="ensabc-inspector-drawer" aria-live="polite" data-inspector-schema="{_esc(SCHEMA_VERSION)}">
      <h2>Traceability Inspector Drawer</h2>
      <p style="margin:0 0 8px;font-size:12.5px;color:#475569;">
        Hover, click, or focus + press <code>Enter</code> / <code>Space</code> on any scenario node (<code>.evo-node</code>)
        or evolution ribbon (<code>.evo-edge</code>) to highlight its full <code>branch_id</code> lineage and inspect
        all provenance &amp; 3-family metrics.
      </p>
      <div id="ensabc-inspector-content">
        <div class="inspector-sec">
          <h3>Active Run Overview</h3>
          <dl class="kv-grid">
            <dt>evolution_run_id</dt><dd>{_esc(run["evolution_run_id"])}</dd>
            <dt>tracking_run_key</dt><dd>{_esc(run["tracking_run_key"])}</dd>
            <dt>content_sha256</dt><dd>{_esc(run["content_sha256"])}</dd>
            <dt>algorithm_version</dt><dd>{_esc(run["algorithm_version"])}</dd>
            <dt>secondary_rule</dt><dd>{_esc(run["secondary_rule"])}</dd>
            <dt>flow_rule</dt><dd>{_esc(run["flow_rule"])}</dd>
            <dt>uncertainty_rule</dt><dd>{_esc(run["uncertainty_rule"])}</dd>
          </dl>
        </div>
      </div>
    </aside>
  </main>

{ledger_markup}

  <script id="ensabc-evolution-provenance" type="application/json">
{provenance_json}
  </script>

  <script>
  (function () {{
    "use strict";
    const provEl = document.getElementById("ensabc-evolution-provenance");
    const svgEl = document.getElementById("ensabc-evolution-svg");
    const contentEl = document.getElementById("ensabc-inspector-content");
    if (!provEl || !svgEl || !contentEl) {{
      return;
    }}
    const manifest = JSON.parse(provEl.textContent);
    const batchByOrd = new Map(manifest.batches.map((b) => [b.ordinal, b]));
    const transByPrev = new Map(manifest.transitions.map((t) => [t.prev_ordinal, t]));
    const nodeEls = Array.from(svgEl.querySelectorAll(".evo-node"));
    const edgeEls = Array.from(svgEl.querySelectorAll(".evo-edge"));
    let pinnedTarget = null;

    function escHtml(val) {{
      return String(val)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }}

    function fmtNum(val) {{
      if (val === null || val === undefined) {{
        return "NULL";
      }}
      if (typeof val === "number") {{
        return Number.isInteger(val) ? String(val) : val.toPrecision(8).replace(/\\.?0+$/, "");
      }}
      return String(val);
    }}

    function relatedLineageBranches(seedBranchId) {{
      const related = new Set([seedBranchId]);
      const br = manifest.branches[seedBranchId];
      if (br) {{
        if (br.split_from_parent_branch_id) {{
          related.add(br.split_from_parent_branch_id);
        }}
        if (br.merged_into_branch_id) {{
          related.add(br.merged_into_branch_id);
        }}
        (br.spawned_secondary_branch_ids || []).forEach((id) => related.add(id));
        (br.absorbed_secondary_branch_ids || []).forEach((id) => related.add(id));
      }}
      return related;
    }}

    function clearHighlight() {{
      svgEl.classList.remove("has-highlight");
      nodeEls.forEach((el) => el.classList.remove("is-active", "is-dimmed"));
      edgeEls.forEach((el) => el.classList.remove("is-active", "is-dimmed"));
    }}

    function highlightSelection(kind, key) {{
      svgEl.classList.add("has-highlight");
      let activeBranches = new Set();
      if (kind === "node") {{
        const nd = manifest.nodes[key];
        if (nd) {{
          activeBranches = relatedLineageBranches(nd.branch_id);
        }}
      }} else {{
        const ed = manifest.edges[key];
        if (ed) {{
          relatedLineageBranches(ed.prev_branch_id).forEach((b) => activeBranches.add(b));
          relatedLineageBranches(ed.curr_branch_id).forEach((b) => activeBranches.add(b));
        }}
      }}
      nodeEls.forEach((el) => {{
        const nid = el.getAttribute("data-node-id");
        const bid = el.getAttribute("data-branch-id");
        const hit = (kind === "node" && nid === key) || activeBranches.has(bid);
        el.classList.toggle("is-active", kind === "node" && nid === key);
        el.classList.toggle("is-dimmed", !hit);
      }});
      edgeEls.forEach((el) => {{
        const eid = el.getAttribute("data-edge-id");
        const ed = manifest.edges[eid];
        const hit =
          (kind === "edge" && eid === key) ||
          (ed && (activeBranches.has(ed.prev_branch_id) || activeBranches.has(ed.curr_branch_id)));
        el.classList.toggle("is-active", kind === "edge" && eid === key);
        el.classList.toggle("is-dimmed", !hit);
      }});
    }}

    function kvRows(pairs) {{
      return (
        '<dl class="kv-grid">' +
        pairs
          .map(([k, v]) => "<dt>" + escHtml(k) + "</dt><dd>" + escHtml(v) + "</dd>")
          .join("") +
        "</dl>"
      );
    }}

    function renderNodeInspector(nodeId, isPinned) {{
      const nd = manifest.nodes[nodeId];
      if (!nd) {{
        return;
      }}
      const batch = batchByOrd.get(nd.ordinal) || {{}};
      const br = manifest.branches[nd.branch_id] || {{}};
      const pinTag = isPinned ? '<span class="pin-badge">PINNED (Click again or Esc to unpin)</span>' : "";
      contentEl.innerHTML =
        "<div>" + pinTag + "</div>" +
        '<div class="inspector-sec"><h3>Node Provenance &amp; Identity</h3>' +
        kvRows([
          ["node_id", nd.node_id],
          ["evolution_run_id", nd.evolution_run_id],
          ["tracking_run_key", manifest.run.tracking_run_key],
          ["batch_ordinal", nd.ordinal + " (" + (batch.init_time || "") + ", +" + batch.lead_hours + "h)"],
          ["cluster_id / rank", nd.cluster_id + " / rank " + nd.canonical_rank],
          ["branch_id", nd.branch_id],
          ["assignment_reason", nd.assignment_reason],
          ["incoming_role", nd.incoming_role],
          ["outgoing_role", nd.outgoing_role],
          ["degrees (in P/S, out P/S)", nd.in_degree_primary + "/" + nd.in_degree_secondary + ", " + nd.out_degree_primary + "/" + nd.out_degree_secondary]
        ]) +
        "</div>" +
        '<div class="inspector-sec"><h3>Family 1 &amp; 2 · Morphology &amp; Member Fraction</h3>' +
        kvRows([
          ["member_count", nd.member_count],
          ["member_fraction", fmtNum(nd.member_fraction)],
          ["primary_delta_fraction", nd.has_primary_delta_fraction ? fmtNum(nd.primary_delta_fraction) : "none (0.0 sentinel)"],
          ["unattributed_inflow", fmtNum(nd.unattributed_inflow_fraction)],
          ["unattributed_outflow", fmtNum(nd.unattributed_outflow_fraction)],
          ["mean_features", JSON.stringify(nd.mean_features)],
          ["batch_dispersion", fmtNum(batch.dispersion)]
        ]) +
        "</div>" +
        '<div class="inspector-sec"><h3>Family 3 · Uncertainty &amp; Branch Lineage</h3>' +
        kvRows([
          ["high_uncertainty", String(nd.high_uncertainty)],
          ["branch_span", "b" + br.birth_ordinal + " .. b" + br.end_ordinal + " (" + br.node_count + " nodes)"],
          ["cumul_morphology_d", fmtNum(br.cumulative_morphology_distance) + " (d/S=" + fmtNum(br.cumulative_morphology_over_scale) + ")"],
          ["end_to_end_d", fmtNum(br.end_to_end_morphology_distance)],
          ["branch_net_delta_f", fmtNum(br.net_delta_fraction) + " (abs cumul=" + fmtNum(br.cumulative_abs_delta_fraction) + ")"],
          ["split_from_parent", br.split_from_parent_branch_id || "none"],
          ["spawned_children", (br.spawned_secondary_branch_ids || []).join(", ") || "none"],
          ["absorbed_branches", (br.absorbed_secondary_branch_ids || []).join(", ") || "none"],
          ["merged_into_branch", br.merged_into_branch_id || "none"]
        ]) +
        "</div>";
    }}

    function renderEdgeInspector(edgeId, isPinned) {{
      const ed = manifest.edges[edgeId];
      if (!ed) {{
        return;
      }}
      const tr = transByPrev.get(ed.prev_ordinal) || {{}};
      const pinTag = isPinned ? '<span class="pin-badge">PINNED (Click again or Esc to unpin)</span>' : "";
      contentEl.innerHTML =
        "<div>" + pinTag + "</div>" +
        '<div class="inspector-sec"><h3>Edge Provenance &amp; Rule Basis</h3>' +
        kvRows([
          ["edge_id", ed.edge_id],
          ["evolution_run_id", ed.evolution_run_id],
          ["tracking_run_key", manifest.run.tracking_run_key],
          ["edge_role", ed.edge_role],
          ["event_type", ed.event_type],
          ["rule_basis", ed.rule_basis],
          ["prev_branch_id", ed.prev_branch_id],
          ["curr_branch_id", ed.curr_branch_id],
          ["non_physical_disclaimer", ed.non_physical_disclaimer]
        ]) +
        "</div>" +
        '<div class="inspector-sec"><h3>Family 1 · Morphology Change</h3>' +
        kvRows([
          ["distance", fmtNum(ed.distance)],
          ["distance_over_scale", fmtNum(ed.distance_over_scale)],
          ["edge_strength", fmtNum(ed.edge_strength)],
          ["pair_scale / threshold", fmtNum(tr.pair_scale) + " / " + fmtNum(tr.threshold)]
        ]) +
        "</div>" +
        '<div class="inspector-sec"><h3>Family 2 · Member Fraction &amp; Algorithmic Flow</h3>' +
        kvRows([
          ["prev_member_fraction", fmtNum(ed.prev_member_fraction)],
          ["curr_member_fraction", fmtNum(ed.curr_member_fraction)],
          ["edge_delta_fraction", fmtNum(ed.edge_delta_fraction)],
          ["attributed_flow (alg.)", fmtNum(ed.attributed_flow)],
          ["transition_tv_redist", fmtNum(tr.transition_tv_redistribution)]
        ]) +
        "</div>" +
        '<div class="inspector-sec"><h3>Family 3 · Matching &amp; Evolution Uncertainty</h3>' +
        kvRows([
          ["threshold_margin", fmtNum(ed.threshold_margin) + " (ratio=" + fmtNum(ed.threshold_margin_ratio) + ")"],
          ["primary_alt_status", ed.primary_alternative_status],
          ["optimality_margin", ed.has_optimality_margin ? fmtNum(ed.optimality_margin) + " (ratio=" + fmtNum(ed.optimality_margin_ratio) + ")" : "none (count_drop)"],
          ["secondary_margin", ed.has_secondary_margin ? fmtNum(ed.secondary_margin) + " (ratio=" + fmtNum(ed.secondary_margin_ratio) + ")" : "none (single candidate / primary)"],
          ["ambiguous_tie", String(ed.ambiguous_tie)],
          ["high_uncertainty", String(ed.high_uncertainty)],
          ["uncertainty_reasons", (ed.uncertainty_reasons || []).join(", ") || "none"]
        ]) +
        "</div>";
    }}

    function activateItem(kind, key, togglePin) {{
      if (togglePin) {{
        if (pinnedTarget && pinnedTarget.kind === kind && pinnedTarget.key === key) {{
          pinnedTarget = null;
          clearHighlight();
          return;
        }}
        pinnedTarget = {{ kind: kind, key: key }};
      }} else if (pinnedTarget) {{
        return;
      }}
      highlightSelection(kind, key);
      const isPinned = Boolean(pinnedTarget && pinnedTarget.kind === kind && pinnedTarget.key === key);
      if (kind === "node") {{
        renderNodeInspector(key, isPinned);
      }} else {{
        renderEdgeInspector(key, isPinned);
      }}
    }}

    nodeEls.forEach((el) => {{
      const nid = el.getAttribute("data-node-id");
      el.addEventListener("mouseenter", () => activateItem("node", nid, false));
      el.addEventListener("mouseleave", () => {{ if (!pinnedTarget) clearHighlight(); }});
      el.addEventListener("click", (ev) => {{ ev.stopPropagation(); activateItem("node", nid, true); }});
      el.addEventListener("keydown", (ev) => {{
        if (ev.key === "Enter" || ev.key === " ") {{
          ev.preventDefault();
          activateItem("node", nid, true);
        }}
      }});
    }});

    edgeEls.forEach((el) => {{
      const eid = el.getAttribute("data-edge-id");
      el.addEventListener("mouseenter", () => activateItem("edge", eid, false));
      el.addEventListener("mouseleave", () => {{ if (!pinnedTarget) clearHighlight(); }});
      el.addEventListener("click", (ev) => {{ ev.stopPropagation(); activateItem("edge", eid, true); }});
      el.addEventListener("keydown", (ev) => {{
        if (ev.key === "Enter" || ev.key === " ") {{
          ev.preventDefault();
          activateItem("edge", eid, true);
        }}
      }});
    }});

    document.addEventListener("keydown", (ev) => {{
      if (ev.key === "Escape" && pinnedTarget) {{
        pinnedTarget = null;
        clearHighlight();
      }}
    }});
  }})();
  </script>
</body>
</html>
"""


def render_from_db(db_path: Path, evolution_run_id: str, output_path: Path) -> dict[str, Any]:
    """Query DuckDB, enforce pre-render provenance gate, and atomically write single-file HTML."""
    raw = _fetch_evolution_history_packed(db_path, evolution_run_id)
    manifest = build_view_manifest(raw, evolution_run_id)
    html_doc = render_evolution_document(manifest)
    atomic_write(output_path, html_doc)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Render M3 offline single-file interactive scenario evolution HTML from DuckDB."
    )
    parser.add_argument("--db", type=Path, required=True, help="Path to published DuckDB history database")
    parser.add_argument("--evolution-run-id", type=str, required=True, help="Target evolution_run_id")
    parser.add_argument("--output", type=Path, required=True, help="Output .html file path")
    args = parser.parse_args(argv)

    if not args.db.is_file():
        print(f"Error: DuckDB file not found: {args.db}", file=sys.stderr)
        return 2

    try:
        manifest = render_from_db(args.db, args.evolution_run_id, args.output)
    except MissingProvenanceError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"MISSING_PROVENANCE_FIELD: unexpected render failure: {exc}", file=sys.stderr)
        return 1

    print(
        f"Rendered {args.output} (run={args.evolution_run_id}, "
        f"nodes={len(manifest['nodes'])}, edges={len(manifest['edges'])})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
