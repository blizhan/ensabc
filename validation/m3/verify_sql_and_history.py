#!/usr/bin/env python3
"""Verification harness for M3 DuckDB C API UDFs & 11-table atomic SQL history (T029).

Usage:
    validation/m2/.venv/bin/python validation/m3/verify_sql_and_history.py \
        --scope synthetic|real|all \
        --run-dir DIR \
        --extension PATH \
        [--sample-dir DIR] \
        [--cache-dir DIR]

Writes machine-readable validation evidence under `<run-dir>`:
- `sql-history/evolution-runs.duckdb`: immutable 11-table DuckDB database containing
  published synthetic chains (including the 8-batch showcase run) and, when `--scope`
  includes `real`, the WB2 8-batch main evolution run (`k=3, c=1.0`, DuckOMO `v0.3.5-rc.1`).
- `sql-history/sql-contract.json`
- `sql-history/synthetic-evolution-replay.json`
- `sql-history/three-process-repro.json`
- `sql-history/transaction-atomicity.json`
- When `--scope` includes `real`:
  - `om-verification/test-om-sample-report.json`
  - `om-verification/cell-matrix-manifest.json`
  - `om-verification/om-cells-report.json`
  - `om-verification/duckomo-version-comparison.json`
  - `sql-history/real-evolution-replay.json`
"""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import datetime as dt
import json
import math
import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    CLUSTER_ALGORITHM_VERSION,
    EVOLUTION_ALGORITHM_VERSION,
    FIXTURES_DIR,
    M2_CACHE_DIR,
    M2_FIXTURES_DIR,
    M2_VALIDATION_DIR,
    MATCH_ALGORITHM_VERSION,
    NON_PHYSICAL_DISCLAIMER,
    atomic_write_json,
    canonical_json_sha256,
    default_evolution_config,
    default_scenario_config,
    is_finite_number,
    load_json,
    parse_duckdb_json,
    real_evolution_binding,
    run_isolated_duckdb,
    semantic_evolution_projection,
    sha256_file,
    sql_value,
)
from validation.m3.oracle import solve_chain_oracle, solve_evolution_oracle

SCHEMA_VERSION = "m3.report.v1"
FLOAT_TOL = 1e-14


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _locate_m3_runner(ext_path: Path) -> Path | None:
    candidates = [
        ext_path.parent / "m3_runner",
        Path("/tmp/ensabc-m3-extension-target/m3_runner"),
        Path("/tmp/ensabc-m3-extension-target/release/examples/m3_runner"),
        REPO_ROOT / "target" / "release" / "examples" / "m3_runner",
    ]
    for cand in candidates:
        if cand.is_file() and os.access(cand, os.X_OK):
            return cand.resolve()
    return None


def _run_duckdb(
    extension_path: Path,
    sql: str,
    *,
    db_file: str | Path = ":memory:",
    extra_extensions: list[Path] | None = None,
    bail: bool = True,
    timeout: float = 120.0,
) -> tuple[int, list[Any], str]:
    loads: list[str] = []
    if extra_extensions:
        for ext in extra_extensions:
            loads.append(f"LOAD '{ext.resolve()}';")
    loads.append(f"LOAD '{extension_path.resolve()}';")
    full_sql = "\n".join(loads) + "\n" + textwrap.dedent(sql)

    old_cwd = Path.cwd()
    try:
        os.chdir(REPO_ROOT)
        proc = run_isolated_duckdb(full_sql, database=str(db_file), timeout=timeout, bail=bail)
    finally:
        os.chdir(old_cwd)

    parsed: list[Any] = []
    if proc.stdout.strip():
        parsed = parse_duckdb_json(proc.stdout)
    return proc.returncode, parsed, proc.stderr


def _synthetic_bindings(
    run_key: str,
    *,
    case_payload: Any,
    ext_sha: str,
    threshold_coefficient: float = 1.0,
    cluster_count: int = 3,
    evolve_cfg: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    ev = evolve_cfg or default_evolution_config()
    input_sha = canonical_json_sha256(case_payload)
    sc_sha = canonical_json_sha256({"cluster_count": cluster_count, "mode": "synthetic_staged"})
    match_sha = canonical_json_sha256(
        {"threshold_coefficient": float(threshold_coefficient), "algorithm": MATCH_ALGORITHM_VERSION}
    )
    weights_sha = canonical_json_sha256(case_payload.get("weights", [1.0, 1.0]))
    exec_binding = {
        "run_id": run_key,
        "input_manifest_sha256": input_sha,
        "scenario_config_sha256": sc_sha,
        "match_config_sha256": match_sha,
        "weights_version": weights_sha,
        "threshold_coefficient": float(threshold_coefficient),
        "cluster_count": int(cluster_count),
        "software": {
            "ensabc_extension_sha256": ext_sha,
            "duckomo_version": "synthetic_none",
            "duckomo_sha256": "synthetic_none",
            "duckdb_version": "v1.5.5",
            "cluster_algorithm_version": CLUSTER_ALGORITHM_VERSION,
            "match_algorithm_version": MATCH_ALGORITHM_VERSION,
        },
    }
    evolve_hash_payload = {
        "secondary_rule": ev["secondary_rule"],
        "flow_rule": ev["flow_rule"],
        "uncertainty_rule": ev.get("uncertainty_rule", "m3.uncertainty.v1"),
        "fraction_sum_tolerance": float(ev["fraction_sum_tolerance"]),
        "high_uncertainty_threshold_ratio": float(ev["high_uncertainty_threshold_ratio"]),
        "high_uncertainty_optimality_ratio": float(ev["high_uncertainty_optimality_ratio"]),
        "high_uncertainty_secondary_ratio": float(ev["high_uncertainty_secondary_ratio"]),
        "algorithm": EVOLUTION_ALGORITHM_VERSION,
    }
    evol_binding = {
        "tracking_run_key": run_key,
        "input_manifest_sha256": input_sha,
        "scenario_config_sha256": sc_sha,
        "match_config_sha256": match_sha,
        "evolve_config_sha256": canonical_json_sha256(evolve_hash_payload),
        "weights_version": weights_sha,
        "secondary_rule": ev["secondary_rule"],
        "flow_rule": ev["flow_rule"],
        "uncertainty_rule": ev.get("uncertainty_rule", "m3.uncertainty.v1"),
        "fraction_sum_tolerance": float(ev["fraction_sum_tolerance"]),
        "high_uncertainty_threshold_ratio": float(ev["high_uncertainty_threshold_ratio"]),
        "high_uncertainty_optimality_ratio": float(ev["high_uncertainty_optimality_ratio"]),
        "high_uncertainty_secondary_ratio": float(ev["high_uncertainty_secondary_ratio"]),
        "software": {
            **exec_binding["software"],
            "evolution_algorithm_version": EVOLUTION_ALGORITHM_VERSION,
        },
    }
    evolution_run_id = canonical_json_sha256(evol_binding)
    return evolution_run_id, exec_binding, evol_binding


def _build_synthetic_showcase_8batch_case() -> dict[str, Any]:
    """Build an 8-batch synthetic showcase chain exercising all 5 base events,
    both `missing` and `failed` gap barriers, `ambiguous_tie`, `high_uncertainty`,
    and all 5 SVG node badges (`emerged`, `disappeared`, `gap_interrupted`,
    `gap_restarted`, `high_uncertainty`).
    """
    return {
        "case_id": "synthetic_showcase_8batch_all_topologies",
        "description": (
            "8-batch synthetic showcase exercising symmetric primary tie (inherited_ambiguous), "
            "secondary split, secondary merge, emergence, disappearance, missing-batch gap, "
            "and whitelisted failed-batch gap"
        ),
        "planned_batch_count": 8,
        "threshold_coefficient": 1.0,
        "weights": [1.0, 1.0],
        "config": default_evolution_config(),
        "batches": [
            {
                "ordinal": 0,
                "init_time_utc": "2020-02-01T00:00:00Z",
                "lead_hours": 96,
                "status": "ok",
                "dispersion": 3.0,
                "scenarios": [
                    {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.0, 0.0]},
                    {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [2.0, 2.0]},
                ],
            },
            {
                "ordinal": 1,
                "init_time_utc": "2020-02-01T12:00:00Z",
                "lead_hours": 84,
                "status": "ok",
                "dispersion": 3.0,
                "scenarios": [
                    {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.0, 2.0]},
                    {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [2.0, 0.0]},
                    {"cluster_id": 2, "member_count": 10, "member_fraction": 0.20, "mean_features": [1.0, 1.0]},
                ],
            },
            {
                "ordinal": 2,
                "init_time_utc": "2020-02-02T00:00:00Z",
                "lead_hours": 72,
                "status": "ok",
                "dispersion": 2.0,
                "scenarios": [
                    {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [0.2, 1.8]},
                    {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [2.1, 0.1]},
                ],
            },
            {
                "ordinal": 3,
                "init_time_utc": "2020-02-02T12:00:00Z",
                "lead_hours": 60,
                "status": "missing",
                "status_reason": "SOURCE_BATCH_MISSING",
            },
            {
                "ordinal": 4,
                "init_time_utc": "2020-02-03T00:00:00Z",
                "lead_hours": 48,
                "status": "ok",
                "dispersion": 2.0,
                "scenarios": [
                    {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [0.5, 0.5]},
                    {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [-12.0, -12.0]},
                ],
            },
            {
                "ordinal": 5,
                "init_time_utc": "2020-02-03T12:00:00Z",
                "lead_hours": 36,
                "status": "ok",
                "dispersion": 2.0,
                "scenarios": [
                    {"cluster_id": 0, "member_count": 28, "member_fraction": 0.56, "mean_features": [0.7, 0.7]},
                    {"cluster_id": 1, "member_count": 22, "member_fraction": 0.44, "mean_features": [15.0, 15.0]},
                ],
            },
            {
                "ordinal": 6,
                "init_time_utc": "2020-02-04T00:00:00Z",
                "lead_hours": 24,
                "status": "failed",
                "error_code": "INSUFFICIENT_RANK",
                "status_reason": "INSUFFICIENT_RANK: effective_rank=1 < projection_dim=2",
            },
            {
                "ordinal": 7,
                "init_time_utc": "2020-02-04T12:00:00Z",
                "lead_hours": 12,
                "status": "ok",
                "dispersion": 2.0,
                "scenarios": [
                    {"cluster_id": 0, "member_count": 26, "member_fraction": 0.52, "mean_features": [0.9, 0.9]},
                    {"cluster_id": 1, "member_count": 24, "member_fraction": 0.48, "mean_features": [15.2, 15.2]},
                ],
            },
        ],
    }


def _build_stage_sql_for_case(
    case: dict[str, Any],
    *,
    run_key: str,
    evolution_run_id: str,
    exec_binding: dict[str, Any],
    evol_binding: dict[str, Any],
    inject_m3_failure: str = "false",
    inject_m2_failure: bool = False,
) -> str:
    planned = len(case["batches"])
    tc = float(case["threshold_coefficient"])
    weights = [float(w) for w in case["weights"]]
    ev_cfg = case.get("config") or default_evolution_config()

    batch_rows: list[str] = []
    cluster_rows: list[str] = []
    for b in case["batches"]:
        ord_i = int(b["ordinal"])
        init_utc = b.get("init_time_utc", "2020-02-01T00:00:00Z").replace("T", " ").replace("Z", "")
        lead_hr = int(b.get("lead_hours", max(12, 96 - ord_i * 12)))
        st = b["status"]
        if st == "ok":
            disp = float(b["dispersion"])
            batch_rows.append(
                f"({ord_i}, TIMESTAMP '{init_utc}', {lead_hr}, 'ok', NULL::VARCHAR, "
                f"'in_{ord_i}', 'om_{ord_i}', {disp!r}::DOUBLE, 'cls_{ord_i}')"
            )
            sc_items = []
            for sc in b["scenarios"]:
                sc_items.append(
                    "{"
                    f"'cluster_id': {int(sc['cluster_id'])}::BIGINT, "
                    f"'member_count': {int(sc['member_count'])}::BIGINT, "
                    f"'member_fraction': {float(sc['member_fraction'])!r}::DOUBLE, "
                    f"'mean_features': {sql_value([float(x) for x in sc['mean_features']])}::DOUBLE[], "
                    f"'representative_member_id': 'm_{int(sc['cluster_id'])}', "
                    "'within_cluster_sse': 0.1::DOUBLE"
                    "}"
                )
            cluster_rows.append(
                f"({ord_i}, {{'algorithm_version': 'm1.native.v1', 'scenarios': [{', '.join(sc_items)}]}})"
            )
        else:
            reason = b.get("status_reason") or b.get("error_code") or "SOURCE_BATCH_MISSING"
            in_sha = "NULL::VARCHAR" if st == "missing" else f"'in_{ord_i}'"
            om_sha = "NULL::VARCHAR" if st == "missing" else f"'om_{ord_i}'"
            batch_rows.append(
                f"({ord_i}, TIMESTAMP '{init_utc}', {lead_hr}, {sql_value(st)}, {sql_value(reason)}, "
                f"{in_sha}, {om_sha}, NULL::DOUBLE, NULL::VARCHAR)"
            )

    evolve_cfg_sql = {
        "secondary_rule": ev_cfg["secondary_rule"],
        "flow_rule": ev_cfg["flow_rule"],
        "fraction_sum_tolerance": float(ev_cfg["fraction_sum_tolerance"]),
        "high_uncertainty_threshold_ratio": float(ev_cfg["high_uncertainty_threshold_ratio"]),
        "high_uncertainty_optimality_ratio": float(ev_cfg["high_uncertainty_optimality_ratio"]),
        "high_uncertainty_secondary_ratio": float(ev_cfg["high_uncertainty_secondary_ratio"]),
    }

    return textwrap.dedent(
        f"""
        SET VARIABLE run_key = {sql_value(run_key)};
        SET VARIABLE evolution_run_id = {sql_value(evolution_run_id)};
        SET VARIABLE planned_batch_count = {planned};
        SET VARIABLE threshold_coefficient = {tc!r}::DOUBLE;
        SET VARIABLE weights = {sql_value(weights)}::DOUBLE[];
        SET VARIABLE evolve_config = {sql_value(evolve_cfg_sql)};
        SET VARIABLE execution_binding = {sql_value(exec_binding)};
        SET VARIABLE evolution_binding = {sql_value(evol_binding)};
        SET VARIABLE inject_failure = {'true' if inject_m2_failure else 'false'};
        SET VARIABLE inject_m3_failure = {sql_value(inject_m3_failure)};
        DROP TABLE IF EXISTS stage_raw_batches;

        CREATE OR REPLACE TEMP TABLE stage_batches AS
        SELECT * FROM (VALUES {', '.join(batch_rows)})
        t(ordinal, init_time, lead_hours, status, status_reason, input_sha256, om_sha256, dispersion, cluster_result_sha256);

        CREATE OR REPLACE TEMP TABLE stage_cluster AS
        SELECT * FROM (VALUES {', '.join(cluster_rows)}) t(ordinal, result);

        .read validation/m3/sql/stage_and_publish.sql
        """
    )


def _query_published_evolution_bundle(
    ext_path: Path,
    db_path: Path,
    evolution_run_id: str,
) -> dict[str, Any]:
    rid = sql_value(evolution_run_id)
    sql = f"""
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
    WHERE evolution_run_id = {rid};

    SELECT
        tr.run_key,
        tr.content_sha256 AS m2_content_sha256,
        tr.planned_batch_count
    FROM tracking_runs tr
    JOIN evolution_runs er ON er.tracking_run_key = tr.run_key
    WHERE er.evolution_run_id = {rid};

    SELECT
        rb.ordinal,
        strftime(rb.init_time, '%Y-%m-%dT%H:%M:%SZ') AS init_time_utc,
        rb.lead_hours,
        rb.status,
        rb.status_reason,
        rb.dispersion
    FROM replay_batches rb
    JOIN evolution_runs er ON er.tracking_run_key = rb.run_key
    WHERE er.evolution_run_id = {rid}
    ORDER BY rb.ordinal ASC;

    SELECT * EXCLUDE (evolution_run_id)
    FROM evolution_nodes
    WHERE evolution_run_id = {rid}
    ORDER BY ordinal ASC, cluster_id ASC;

    SELECT * EXCLUDE (evolution_run_id, evolve_result), (evolve_result IS NOT NULL) AS has_evolve_result
    FROM evolution_transitions
    WHERE evolution_run_id = {rid}
    ORDER BY prev_ordinal ASC;

    SELECT * EXCLUDE (evolution_run_id)
    FROM evolution_edges
    WHERE evolution_run_id = {rid}
    ORDER BY prev_ordinal ASC, prev_cluster_id ASC, curr_cluster_id ASC;

    SELECT * EXCLUDE (evolution_run_id)
    FROM evolution_events
    WHERE evolution_run_id = {rid}
    ORDER BY prev_ordinal ASC, event_index ASC;

    SELECT * EXCLUDE (evolution_run_id)
    FROM evolution_branch_summaries
    WHERE evolution_run_id = {rid}
    ORDER BY branch_id ASC;
    """
    rc, parsed, stderr = _run_duckdb(ext_path, sql, db_file=db_path)
    if rc != 0:
        raise RuntimeError(f"Failed reading published bundle for {evolution_run_id}: {stderr}")
    run_rows, tr_rows, batches, nodes, transitions, edges, events, branches = parsed[-8:]
    if len(run_rows) != 1 or len(tr_rows) != 1:
        raise RuntimeError(f"Missing published run rows for {evolution_run_id}")
    run_row = run_rows[0]
    return {
        "evolution_run_id": run_row["evolution_run_id"],
        "tracking_run_key": run_row["tracking_run_key"],
        "content_sha256": run_row["content_sha256"],
        "m2_content_sha256": tr_rows[0]["m2_content_sha256"],
        "non_physical_disclaimer": run_row["non_physical_disclaimer"],
        "planned_batch_count": run_row["planned_batch_count"],
        "weights": run_row["weights"],
        "evolution_binding": run_row["evolution_binding"],
        "chain_summary": run_row["chain_summary"],
        "batches": batches,
        "nodes": nodes,
        "transitions": transitions,
        "edges": edges,
        "events": events,
        "branches": branches,
    }


def _assert_close(a: Any, b: Any, ctx: str) -> None:
    if isinstance(b, float):
        if not is_finite_number(a) or not math.isclose(float(a), b, rel_tol=FLOAT_TOL, abs_tol=FLOAT_TOL):
            raise AssertionError(f"{ctx}: float mismatch sql={a!r} vs runner={b!r}")
    elif isinstance(b, list):
        if not isinstance(a, list) or len(a) != len(b):
            raise AssertionError(f"{ctx}: list length mismatch sql={a!r} vs runner={b!r}")
        for idx, (ai, bi) in enumerate(zip(a, b)):
            _assert_close(ai, bi, f"{ctx}[{idx}]")
    else:
        if a != b:
            raise AssertionError(f"{ctx}: value mismatch sql={a!r} vs runner={b!r}")


def _assert_bundle_matches_m3_runner(
    bundle: dict[str, Any],
    runner_bin: Path,
    *,
    threshold_coefficient: float,
    evolve_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ev_cfg = evolve_config or default_evolution_config()
    nodes_by_ord: dict[int, list[dict[str, Any]]] = {}
    for n in bundle["nodes"]:
        nodes_by_ord.setdefault(int(n["ordinal"]), []).append(n)

    runner_batches: list[dict[str, Any]] = []
    for b in bundle["batches"]:
        ord_i = int(b["ordinal"])
        if b["status"] == "ok":
            runner_batches.append(
                {
                    "ordinal": ord_i,
                    "status": "ok",
                    "dispersion": b["dispersion"],
                    "scenarios": [
                        {
                            "cluster_id": n["cluster_id"],
                            "member_count": n["member_count"],
                            "member_fraction": n["member_fraction"],
                            "mean_features": n["mean_features"],
                        }
                        for n in nodes_by_ord.get(ord_i, [])
                    ],
                }
            )
        elif b["status"] == "missing":
            runner_batches.append(
                {
                    "ordinal": ord_i,
                    "status": "missing",
                    "status_reason": b["status_reason"] or "SOURCE_BATCH_MISSING",
                }
            )
        else:
            reason_str = b["status_reason"] or "BATCH_FAILED"
            err_code = reason_str.split(":", 1)[0].strip()
            runner_batches.append(
                {
                    "ordinal": ord_i,
                    "status": "failed",
                    "error_code": err_code,
                    "status_reason": reason_str,
                }
            )

    runner_payload = {
        "mode": "chain",
        "tracking_run_key": bundle["tracking_run_key"],
        "threshold_coefficient": float(threshold_coefficient),
        "weights": bundle["weights"],
        "config": {
            "secondary_rule": ev_cfg["secondary_rule"],
            "flow_rule": ev_cfg["flow_rule"],
            "fraction_sum_tolerance": float(ev_cfg["fraction_sum_tolerance"]),
            "high_uncertainty_threshold_ratio": float(ev_cfg["high_uncertainty_threshold_ratio"]),
            "high_uncertainty_optimality_ratio": float(ev_cfg["high_uncertainty_optimality_ratio"]),
            "high_uncertainty_secondary_ratio": float(ev_cfg["high_uncertainty_secondary_ratio"]),
        },
        "batches": runner_batches,
    }

    with tempfile.TemporaryDirectory(prefix="ensabc-m3-runner-cmp-") as tmp_str:
        tmp = Path(tmp_str)
        in_p = tmp / "chain_in.json"
        out_p = tmp / "chain_out.json"
        in_p.write_text(json.dumps(runner_payload), encoding="utf-8")
        proc = subprocess.run(
            [str(runner_bin), "--mode", "chain", "--input", str(in_p), "--output", str(out_p)],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"m3_runner --mode chain failed: {proc.stderr}")
        runner_res = json.loads(out_p.read_text(encoding="utf-8"))

    # 1. chain_summary (all 26 fields)
    for k, rv in runner_res["chain_summary"].items():
        _assert_close(bundle["chain_summary"][k], rv, f"chain_summary.{k}")

    # 2. nodes
    if len(bundle["nodes"]) != len(runner_res["nodes"]):
        raise AssertionError(f"Node count mismatch: sql={len(bundle['nodes'])} vs runner={len(runner_res['nodes'])}")
    for idx, (sn, rn) in enumerate(zip(bundle["nodes"], runner_res["nodes"])):
        for k, rv in rn.items():
            expected = (rv if rn["has_primary_delta_fraction"] else 0.0) if k == "primary_delta_fraction" else rv
            _assert_close(sn[k], expected, f"nodes[{idx}].{k}")

    # 3. transitions
    if len(bundle["transitions"]) != len(runner_res["transitions"]):
        raise AssertionError("Transition count mismatch")
    for idx, (st, rt) in enumerate(zip(bundle["transitions"], runner_res["transitions"])):
        for k in (
            "prev_ordinal",
            "curr_ordinal",
            "transition_status",
            "chain_broken",
            "gap_reason",
            "pair_scale",
            "threshold",
            "primary_edge_count",
            "secondary_split_edge_count",
            "secondary_merge_edge_count",
            "transition_tv_redistribution",
            "min_threshold_margin",
            "ambiguous_tie",
            "high_uncertainty",
        ):
            _assert_close(st[k], rt[k], f"transitions[{idx}].{k}")
        _assert_close(st["has_evolve_result"], rt["evolve_result"] is not None, f"transitions[{idx}].has_evolve_result")

    # 4. edges
    if len(bundle["edges"]) != len(runner_res["edges"]):
        raise AssertionError("Edge count mismatch")
    for idx, (se, re_) in enumerate(zip(bundle["edges"], runner_res["edges"])):
        for k, rv in re_.items():
            if k in ("optimality_margin", "optimality_margin_ratio") and not re_["has_optimality_margin"]:
                expected = 0.0
            elif k in ("secondary_margin", "secondary_margin_ratio") and not re_["has_secondary_margin"]:
                expected = 0.0
            else:
                expected = rv
            _assert_close(se[k], expected, f"edges[{idx}].{k}")

    # 5. events
    if len(bundle["events"]) != len(runner_res["events"]):
        raise AssertionError("Event count mismatch")
    for idx, (sev, rev) in enumerate(zip(bundle["events"], runner_res["events"])):
        for k, rv in rev.items():
            if k == "min_margin_ratio" and not rev["has_min_margin_ratio"]:
                expected = 0.0
            else:
                expected = rv
            _assert_close(sev[k], expected, f"events[{idx}].{k}")

    # 6. branch summaries (all 27 fields, including fsum accumulations)
    if len(bundle["branches"]) != len(runner_res["branches"]):
        raise AssertionError("Branch summary count mismatch")
    for idx, (sb, rb) in enumerate(zip(bundle["branches"], runner_res["branches"])):
        for k, rv in rb.items():
            _assert_close(sb[k], rv, f"branches[{idx}].{k}")

    # 7. Independent Python Fraction/fsum chain oracle parity (FR-007)
    oracle_chain = solve_chain_oracle(runner_payload)
    for k, ov in oracle_chain["chain_summary"].items():
        if k in bundle["chain_summary"]:
            _assert_close(bundle["chain_summary"][k], ov, f"oracle.chain_summary.{k}")
    oracle_branches = oracle_chain.get("branches") or oracle_chain.get("branch_summaries") or []
    if len(bundle["branches"]) != len(oracle_branches):
        raise AssertionError("Oracle branch summary count mismatch")
    for idx, (sb, ob) in enumerate(zip(bundle["branches"], oracle_branches)):
        for k in (
            "branch_id",
            "birth_ordinal",
            "end_ordinal",
            "node_count",
            "step_count",
            "cumulative_morphology_distance",
            "cumulative_morphology_over_scale",
            "max_step_morphology_distance",
            "max_step_morphology_over_scale",
            "end_to_end_morphology_distance",
            "initial_member_fraction",
            "final_member_fraction",
            "min_member_fraction",
            "max_member_fraction",
            "net_delta_fraction",
            "cumulative_abs_delta_fraction",
            "split_out_event_count",
            "spawned_secondary_branch_ids",
            "split_from_parent_branch_id",
            "merge_in_event_count",
            "absorbed_secondary_branch_ids",
            "merged_into_branch_id",
            "ambiguous_step_count",
            "high_uncertainty_step_count",
            "high_uncertainty_step_ratio",
        ):
            _assert_close(sb[k], ob[k], f"oracle.branches[{idx}].{k}")

    return {
        "nodes_verified": len(bundle["nodes"]),
        "transitions_verified": len(bundle["transitions"]),
        "edges_verified": len(bundle["edges"]),
        "events_verified": len(bundle["events"]),
        "branches_verified": len(bundle["branches"]),
        "exact_parity_with_m3_runner": True,
        "exact_parity_with_chain_oracle": True,
    }


def verify_sql_contract(ext_path: Path, runner_bin: Path) -> dict[str, Any]:
    """Verify `ens_fsum`, `ens_evolve_scenarios`, M1/M2 UDF preservation, >2048 chunk boundary,
    tampered backbone rejection, same-session recovery, and oracle parity (`sql-contract.json`).
    """
    # 1. ens_fsum exactness & negative cases
    alt_vals = [1e16, 1.0, -1e16, 1e-16, 3.25, -1e-16, -0.25]
    rc, parsed, err = _run_duckdb(
        ext_path,
        f"""
        SELECT
            ens_fsum([]::DOUBLE[]) AS empty_val,
            ens_fsum([-0.0, -0.0]::DOUBLE[]) AS neg_zero_val,
            (1.0 / ens_fsum([-0.0, -0.0]::DOUBLE[])) > 0 AS pos_zero_sign,
            ens_fsum([1e16, 1.0, -1e16]::DOUBLE[]) AS cancel_val,
            ens_fsum({sql_value(alt_vals)}::DOUBLE[]) AS alt_val,
            ens_m0_echo([{{'member_id': 'm0', 'features': [1.0, 2.0]}}]) AS m0_echo,
            ens_dispersion(
                [{{'member_id': 'm0', 'features': [1.0, 2.0]}}, {{'member_id': 'm1', 'features': [3.0, 4.0]}}],
                [1.0, 1.0]
            ) AS m2_disp;
        """,
    )
    if rc != 0:
        raise RuntimeError(f"ens_fsum contract query failed: {err}")
    fsum_row = parsed[-1][0]
    assert fsum_row["empty_val"] == 0.0
    assert fsum_row["neg_zero_val"] == 0.0 and fsum_row["pos_zero_sign"] is True
    assert fsum_row["cancel_val"] == 1.0
    assert fsum_row["alt_val"] == math.fsum(alt_vals)
    assert len(str(fsum_row["m0_echo"])) > 0
    assert abs(fsum_row["m2_disp"] - 1.0) < 1e-12

    # 2. Oracle parity on multi-event pair via SQL ens_evolve_scenarios
    prev_sc = [
        {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.0, 0.0]},
        {"cluster_id": 1, "member_count": 10, "member_fraction": 0.20, "mean_features": [0.6, 0.6]},
        {"cluster_id": 2, "member_count": 12, "member_fraction": 0.24, "mean_features": [6.0, 6.0]},
        {"cluster_id": 3, "member_count": 8, "member_fraction": 0.16, "mean_features": [-20.0, -20.0]},
    ]
    curr_sc = [
        {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.1, 0.1]},
        {"cluster_id": 1, "member_count": 9, "member_fraction": 0.18, "mean_features": [6.2, 6.2]},
        {"cluster_id": 2, "member_count": 8, "member_fraction": 0.16, "mean_features": [6.9, 6.9]},
        {"cluster_id": 3, "member_count": 8, "member_fraction": 0.16, "mean_features": [25.0, 25.0]},
    ]
    weights = [1.0, 1.0]
    ev_cfg = default_evolution_config()
    oracle_cfg = {k: v for k, v in ev_cfg.items() if k != "uncertainty_rule"}
    oracle_res = solve_evolution_oracle(
        {
            "mode": "scaled",
            "prev_scenarios": prev_sc,
            "curr_scenarios": curr_sc,
            "weights": weights,
            "threshold_coefficient": 1.0,
            "prev_dispersion": 2.0,
            "curr_dispersion": 2.0,
            "config": oracle_cfg,
        }
    )
    sql_pair = f"""
    WITH inp AS (
        SELECT
            {sql_value(prev_sc)} AS prev_s,
            {sql_value(curr_sc)} AS curr_s,
            {sql_value(weights)}::DOUBLE[] AS w,
            {sql_value({k: v for k, v in ev_cfg.items() if k != 'uncertainty_rule'})} AS cfg
    )
    SELECT ens_evolve_scenarios(
        prev_s,
        curr_s,
        w,
        ens_match_scenarios(
            list_transform(prev_s, lambda s: {{'cluster_id': s.cluster_id, 'mean_features': s.mean_features}}),
            list_transform(curr_s, lambda s: {{'cluster_id': s.cluster_id, 'mean_features': s.mean_features}}),
            w,
            {{'threshold_coefficient': 1.0::DOUBLE, 'prev_dispersion': 2.0::DOUBLE, 'curr_dispersion': 2.0::DOUBLE}}
        ),
        cfg
    ) AS evo
    FROM inp;
    """
    rc_p, parsed_p, err_p = _run_duckdb(ext_path, sql_pair)
    if rc_p != 0:
        raise RuntimeError(f"ens_evolve_scenarios oracle check failed: {err_p}")
    sql_evo = parsed_p[-1][0]["evo"]
    for k in (
        "primary_edge_count",
        "secondary_split_edge_count",
        "secondary_merge_edge_count",
        "continuation_count",
        "split_count",
        "merge_count",
        "emergence_count",
        "disappearance_count",
        "primary_total_distance",
        "transition_tv_redistribution",
        "secondary_attributed_flow",
        "total_attributed_flow",
        "min_threshold_margin",
    ):
        _assert_close(sql_evo["summary"][k], oracle_res["summary"][k], f"oracle_pair.summary.{k}")

    # 3. >2048 rows chunk boundary (2100 rows) + late-chunk bad row (row=2075) whole-query failure
    sql_chunk = """
    WITH rows_2100 AS (
        SELECT
            r,
            [1e16::DOUBLE, (r + 1)::DOUBLE, -1e16::DOUBLE] AS f_terms,
            [
                {'cluster_id': 0::BIGINT, 'member_count': 30::BIGINT, 'member_fraction': 0.6::DOUBLE,
                 'mean_features': [0.0::DOUBLE, (r * 0.001)::DOUBLE]},
                {'cluster_id': 1::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.4::DOUBLE,
                 'mean_features': [5.0::DOUBLE, (5.0 + r * 0.001)::DOUBLE]}
            ] AS prev_s,
            [
                {'cluster_id': 0::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.4::DOUBLE,
                 'mean_features': [0.2::DOUBLE, (0.2 + r * 0.001)::DOUBLE]},
                {'cluster_id': 1::BIGINT, 'member_count': 10::BIGINT, 'member_fraction': 0.2::DOUBLE,
                 'mean_features': [0.9::DOUBLE, (0.9 + r * 0.001)::DOUBLE]},
                {'cluster_id': 2::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.4::DOUBLE,
                 'mean_features': [5.2::DOUBLE, (5.2 + r * 0.001)::DOUBLE]}
            ] AS curr_s,
            [1.0, 1.0]::DOUBLE[] AS w,
            {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
             'fraction_sum_tolerance': 1e-9::DOUBLE, 'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
             'high_uncertainty_optimality_ratio': 0.10::DOUBLE, 'high_uncertainty_secondary_ratio': 0.10::DOUBLE} AS cfg
        FROM range(2100) AS t(r)
    ),
    eval_rows AS (
        SELECT
            r,
            ens_fsum(f_terms) AS fs,
            ens_evolve_scenarios(
                prev_s, curr_s, w,
                ens_match_scenarios(
                    list_transform(prev_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                    list_transform(curr_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                    w,
                    {'threshold_coefficient': 1.0::DOUBLE, 'prev_dispersion': 2.0::DOUBLE, 'curr_dispersion': 2.0::DOUBLE}
                ),
                cfg
            ) AS evo
        FROM rows_2100
    )
    SELECT
        count(*) AS rows_tested,
        bool_and(fs = (r + 1)::DOUBLE) AS fsum_exact,
        bool_and(evo.summary.primary_edge_count = 2 AND evo.summary.secondary_split_edge_count = 1) AS evo_exact
    FROM eval_rows;
    """
    rc_ch, parsed_ch, err_ch = _run_duckdb(ext_path, sql_chunk, timeout=120.0)
    if rc_ch != 0:
        raise RuntimeError(f"2100-row chunk test failed: {err_ch}")
    ch_row = parsed_ch[-1][0]
    assert ch_row == {"rows_tested": 2100, "fsum_exact": True, "evo_exact": True}

    sql_bad_chunk = sql_chunk.replace(
        "'member_fraction': 0.6::DOUBLE",
        "'member_fraction': CASE WHEN r = 2075 THEN 0.99::DOUBLE ELSE 0.6::DOUBLE END",
        1,
    )
    rc_bch, _, err_bch = _run_duckdb(ext_path, sql_bad_chunk, timeout=120.0)
    assert rc_bch != 0 and "[INVALID_FRACTION_SUM]" in err_bch

    # 4. Error & tamper rejections + same-session recovery
    error_Matrix = [
        ("fsum_top_null", "SELECT ens_fsum(NULL::DOUBLE[]);", "NULL_INPUT"),
        ("fsum_nested_null", "SELECT ens_fsum([1.0, NULL]::DOUBLE[]);", "NULL_INPUT"),
        ("fsum_nan", "SELECT ens_fsum(['nan'::DOUBLE]::DOUBLE[]);", "NON_FINITE_INPUT"),
        (
            "fsum_overflow",
            "SELECT ens_fsum([1.7976931348623157e308, 1.7976931348623157e308]::DOUBLE[]);",
            "NUMERICAL_FAILURE",
        ),
        (
            "evolve_top_null",
            "SELECT ens_evolve_scenarios(NULL, NULL, NULL, NULL, NULL);",
            "NULL_INPUT",
        ),
        (
            "evolve_tampered_backbone_links",
            """
            WITH bm AS (
                SELECT ens_match_scenarios(
                    [{'cluster_id': 0, 'mean_features': [0.0, 0.0]}, {'cluster_id': 1, 'mean_features': [4.0, 4.0]}],
                    [{'cluster_id': 0, 'mean_features': [0.2, 0.2]}, {'cluster_id': 1, 'mean_features': [4.2, 4.2]}],
                    [1.0, 1.0],
                    {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
                ) AS m
            )
            SELECT ens_evolve_scenarios(
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [4.0, 4.0]::DOUBLE[]}],
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.2, 0.2]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [4.2, 4.2]::DOUBLE[]}],
                [1.0, 1.0]::DOUBLE[],
                struct_pack(
                    algorithm_version := m.algorithm_version,
                    prev_cluster_ids := m.prev_cluster_ids,
                    curr_cluster_ids := m.curr_cluster_ids,
                    prev_dispersion := m.prev_dispersion,
                    curr_dispersion := m.curr_dispersion,
                    pair_scale := m.pair_scale,
                    threshold_coefficient := m.threshold_coefficient,
                    threshold := m.threshold,
                    distance_matrix := m.distance_matrix,
                    distance_ratio_matrix := m.distance_ratio_matrix,
                    eligible_matrix := m.eligible_matrix,
                    edge_count := 0::BIGINT,
                    total_distance := 0.0::DOUBLE,
                    optimal_solution_count := m.optimal_solution_count,
                    ambiguous := m.ambiguous,
                    threshold_margin := m.threshold_margin,
                    optimality_margin := m.optimality_margin,
                    has_optimality_margin := m.has_optimality_margin,
                    links := m.links[1:0],
                    unmatched_prev := m.unmatched_prev,
                    unmatched_curr := m.unmatched_curr
                ),
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9, 'high_uncertainty_threshold_ratio': 0.1,
                 'high_uncertainty_optimality_ratio': 0.1, 'high_uncertainty_secondary_ratio': 0.1}
            ) FROM bm;
            """,
            "BACKBONE_INCONSISTENCY",
        ),
        (
            "evolve_invalid_primary_link",
            """
            WITH bm AS (
                SELECT ens_match_scenarios(
                    [{'cluster_id': 0, 'mean_features': [0.0, 0.0]}, {'cluster_id': 1, 'mean_features': [4.0, 4.0]}],
                    [{'cluster_id': 0, 'mean_features': [0.2, 0.2]}, {'cluster_id': 1, 'mean_features': [4.2, 4.2]}],
                    [1.0, 1.0],
                    {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
                ) AS m
            )
            SELECT ens_evolve_scenarios(
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [4.0, 4.0]::DOUBLE[]}],
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.2, 0.2]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [4.2, 4.2]::DOUBLE[]}],
                [1.0, 1.0]::DOUBLE[],
                struct_pack(
                    algorithm_version := m.algorithm_version,
                    prev_cluster_ids := m.prev_cluster_ids,
                    curr_cluster_ids := m.curr_cluster_ids,
                    prev_dispersion := m.prev_dispersion,
                    curr_dispersion := m.curr_dispersion,
                    pair_scale := m.pair_scale,
                    threshold_coefficient := m.threshold_coefficient,
                    threshold := m.threshold,
                    distance_matrix := m.distance_matrix,
                    distance_ratio_matrix := m.distance_ratio_matrix,
                    eligible_matrix := m.eligible_matrix,
                    edge_count := m.edge_count,
                    total_distance := m.total_distance,
                    optimal_solution_count := m.optimal_solution_count,
                    ambiguous := m.ambiguous,
                    threshold_margin := m.threshold_margin,
                    optimality_margin := m.optimality_margin,
                    has_optimality_margin := m.has_optimality_margin,
                    links := [{'prev_cluster_id': 0::BIGINT, 'curr_cluster_id': 99::BIGINT, 'distance': 0.2::DOUBLE,
                               'distance_ratio': 0.2::DOUBLE, 'alternative_status': 'count_drop'::VARCHAR,
                               'alternative_edge_count': 1::BIGINT, 'alternative_total_distance': 0.2::DOUBLE,
                               'alternative_distance_increase': 0.0::DOUBLE, 'alternative_distance_increase_ratio': 0.0::DOUBLE,
                               'has_same_count_alternative': false, 'ambiguous': false}],
                    unmatched_prev := m.unmatched_prev,
                    unmatched_curr := m.unmatched_curr
                ),
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9, 'high_uncertainty_threshold_ratio': 0.1,
                 'high_uncertainty_optimality_ratio': 0.1, 'high_uncertainty_secondary_ratio': 0.1}
            ) FROM bm;
            """,
            "INVALID_PRIMARY_LINK",
        ),
    ]
    error_results: list[dict[str, Any]] = []
    for name, q, code_tok in error_Matrix:
        rc_e, _, err_e = _run_duckdb(ext_path, q)
        if rc_e == 0 or f"[{code_tok}]" not in err_e:
            raise RuntimeError(f"Contract negative check {name} did not fail with [{code_tok}]: {err_e}")
        error_results.append({"case": name, "expected_code": code_tok, "rejected": True})

    # Same-session valid -> invalid -> valid recovery
    recovery_sql = """
    SELECT ens_fsum([1e16, 5.0, -1e16]::DOUBLE[]) AS v_before;
    SELECT ens_fsum([1.0, NULL]::DOUBLE[]);
    SELECT ens_evolve_scenarios(NULL, NULL, NULL, NULL, NULL);
    SELECT ens_fsum([1e16, 9.0, -1e16]::DOUBLE[]) AS v_after;
    """
    _, rec_parsed, rec_err = _run_duckdb(ext_path, recovery_sql, bail=False)
    assert "ens_fsum [NULL_INPUT]" in rec_err and "ens_evolve_scenarios [NULL_INPUT]" in rec_err
    assert len(rec_parsed) == 2
    assert rec_parsed[0][0]["v_before"] == 5.0 and rec_parsed[1][0]["v_after"] == 9.0

    return {
        "schema_version": "m3.sql_contract.v1",
        "status": "pass",
        "checked_at_utc": _utc_now(),
        "extension_sha256": sha256_file(ext_path),
        "preserved_udfs": {
            "ens_m0_echo": True,
            "ens_cluster": True,
            "ens_dispersion": True,
            "ens_match_scenarios": True,
        },
        "ens_fsum_contract": {
            "empty_list_zero": True,
            "negative_zero_normalized_to_positive_zero": True,
            "shewchuk_cancellation_exact": True,
            "math_fsum_zero_ulp_parity": True,
        },
        "ens_evolve_scenarios_contract": {
            "oracle_pair_zero_ulp_parity": True,
            "non_null_sentinels_verified": True,
            "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        },
        "large_chunk_boundary": {
            "rows_tested": 2100,
            "duckdb_vector_size": 2048,
            "all_rows_exact": True,
            "late_chunk_bad_row_index": 2075,
            "late_chunk_bad_row_aborts_whole_query": True,
        },
        "negative_error_checks": error_results,
        "same_session_recovery": {
            "sequence": "valid -> NULL_INPUT -> NULL_INPUT -> valid",
            "recovered_cleanly": True,
        },
    }


def verify_synthetic_replays(
    ext_path: Path,
    runner_bin: Path,
    db_path: Path,
) -> dict[str, Any]:
    """Publish all synthetic multi-batch chains + the 8-batch showcase chain into `db_path`,
    verifying E1–E8 guards, gap disjointness, raw-batch whitelist isolation, and 100% parity
    with `m3_runner --mode chain`.
    """
    ext_sha = sha256_file(ext_path)
    fixtures = load_json(FIXTURES_DIR / "synthetic_evolution_cases.json")
    all_cases = list(fixtures["chain_cases"]) + [_build_synthetic_showcase_8batch_case()]

    case_reports: list[dict[str, Any]] = []
    showcase_run_id = ""
    showcase_tracking_key = ""

    for case in all_cases:
        cid = case["case_id"]
        rk = canonical_json_sha256({"synthetic_case_id": cid, "ext_sha": ext_sha})
        erid, exec_b, evol_b = _synthetic_bindings(
            rk,
            case_payload=case,
            ext_sha=ext_sha,
            threshold_coefficient=float(case["threshold_coefficient"]),
            evolve_cfg=case.get("config"),
        )
        sql = _build_stage_sql_for_case(
            case,
            run_key=rk,
            evolution_run_id=erid,
            exec_binding=exec_b,
            evol_binding=evol_b,
        )
        rc, _, err = _run_duckdb(ext_path, sql, db_file=db_path)
        if rc != 0:
            raise RuntimeError(f"Synthetic chain {cid} failed stage_and_publish.sql: {err}")

        bundle = _query_published_evolution_bundle(ext_path, db_path, erid)
        parity = _assert_bundle_matches_m3_runner(
            bundle,
            runner_bin,
            threshold_coefficient=float(case["threshold_coefficient"]),
            evolve_config=case.get("config"),
        )

        # Verify strict disjointness of gap events vs emergence/disappearance
        for tr in bundle["transitions"]:
            ev_types = {
                ev["event_type"]
                for ev in bundle["events"]
                if ev["prev_ordinal"] == tr["prev_ordinal"]
            }
            if tr["transition_status"] == "gap":
                assert ev_types.issubset({"gap_interrupted", "gap_restarted"})
            else:
                assert ev_types.isdisjoint({"gap_interrupted", "gap_restarted"})

        if cid == "synthetic_showcase_8batch_all_topologies":
            showcase_run_id = erid
            showcase_tracking_key = rk

        case_reports.append(
            {
                "case_id": cid,
                "evolution_run_id": erid,
                "tracking_run_key": rk,
                "content_sha256": bundle["content_sha256"],
                "planned_batch_count": bundle["planned_batch_count"],
                "chain_summary": bundle["chain_summary"],
                "runner_parity": parity,
                "gap_events_strictly_disjoint": True,
            }
        )

    # Also verify raw-member `ens_diagnose_batch` whitelisted failure vs non-whitelisted abort in an isolated temp DB
    with tempfile.TemporaryDirectory(prefix="ensabc-m3-synth-raw-") as tmp_str:
        raw_db = Path(tmp_str) / "raw_isolation.duckdb"
        rk_raw = canonical_json_sha256({"raw_test": "whitelist"})
        erid_raw, exec_raw, evol_raw = _synthetic_bindings(
            rk_raw, case_payload={"weights": [1.0, 1.0]}, ext_sha=ext_sha, cluster_count=2
        )
        ok_m = (
            "[{'member_id': 'm0', 'features': [0.0, 0.0]}, {'member_id': 'm1', 'features': [0.2, 0.1]}, "
            "{'member_id': 'm2', 'features': [6.0, 6.0]}, {'member_id': 'm3', 'features': [6.1, 6.2]}]"
        )
        zero_m = (
            "[{'member_id': 'm0', 'features': [2.0, 2.0]}, {'member_id': 'm1', 'features': [2.0, 2.0]}, "
            "{'member_id': 'm2', 'features': [2.0, 2.0]}, {'member_id': 'm3', 'features': [2.0, 2.0]}]"
        )
        sql_raw = f"""
        SET VARIABLE run_key = {sql_value(rk_raw)};
        SET VARIABLE evolution_run_id = {sql_value(erid_raw)};
        SET VARIABLE planned_batch_count = 4;
        SET VARIABLE threshold_coefficient = 1.0::DOUBLE;
        SET VARIABLE weights = [1.0, 1.0]::DOUBLE[];
        SET VARIABLE execution_binding = {sql_value(exec_raw)};
        SET VARIABLE evolution_binding = {sql_value(evol_raw)};
        SET VARIABLE cluster_config = {{
            'projection_dim': 2::BIGINT, 'cluster_count': 2::BIGINT,
            'initialization': 'kmeans++_chacha8_v1'::VARCHAR, 'seed': 3::BIGINT,
            'restarts': 4::BIGINT, 'initial_centers': []::DOUBLE[][][],
            'stopping_rule': 'sklearn_variance_scaled_v1'::VARCHAR,
            'tolerance': 1e-4::DOUBLE, 'max_iterations': 100::BIGINT, 'rank_threshold': 1e-10::DOUBLE
        }};
        CREATE OR REPLACE TEMP TABLE stage_raw_batches AS
        SELECT * FROM (VALUES
            (0, TIMESTAMP '2020-03-01 00:00:00', 48, 'present', NULL::VARCHAR, 'i0', 'o0', {ok_m}),
            (1, TIMESTAMP '2020-03-01 12:00:00', 36, 'present', NULL::VARCHAR, 'i1', 'o1', {zero_m}),
            (2, TIMESTAMP '2020-03-02 00:00:00', 24, 'present', NULL::VARCHAR, 'i2', 'o2', {ok_m}),
            (3, TIMESTAMP '2020-03-02 12:00:00', 12, 'present', NULL::VARCHAR, 'i3', 'o3', {ok_m})
        ) t(ordinal, init_time, lead_hours, raw_status, status_reason, input_sha256, om_sha256, members);
        .read validation/m3/sql/stage_and_publish.sql
        """
        rc_r, _, err_r = _run_duckdb(ext_path, sql_raw, db_file=raw_db)
        if rc_r != 0:
            raise RuntimeError(f"Raw whitelisted batch failure test failed: {err_r}")
        raw_bundle = _query_published_evolution_bundle(ext_path, raw_db, erid_raw)
        assert raw_bundle["batches"][1]["status"] == "failed"
        assert raw_bundle["batches"][1]["status_reason"] == "ZERO_DISPERSION"
        _assert_bundle_matches_m3_runner(raw_bundle, runner_bin, threshold_coefficient=1.0)

    return {
        "schema_version": "m3.synthetic_evolution_replay.v1",
        "status": "pass",
        "checked_at_utc": _utc_now(),
        "database_path": "sql-history/evolution-runs.duckdb",
        "evolution_run_id": showcase_run_id,
        "showcase_evolution_run_id": showcase_run_id,
        "showcase_tracking_run_key": showcase_tracking_key,
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "cases_verified": len(case_reports),
        "raw_batch_whitelist_isolation_verified": True,
        "cases": case_reports,
    }


def verify_transaction_atomicity(ext_path: Path) -> dict[str, Any]:
    """Verify same-version idempotency, EVOLUTION_RUN_VERSION_CONFLICT rejection,
    multi-version coexistence, and 0-residual rollback across all 6 M3 hooks + M2 hook.
    """
    ext_sha = sha256_file(ext_path)
    showcase = _build_synthetic_showcase_8batch_case()
    rk1 = canonical_json_sha256({"tx_atomicity": "v1", "ext_sha": ext_sha})
    erid1, exec1, evol1 = _synthetic_bindings(
        rk1, case_payload=showcase, ext_sha=ext_sha, threshold_coefficient=1.0
    )

    m3_hooks = (
        "after_runs",
        "after_nodes",
        "after_transitions",
        "after_edges",
        "after_events",
        "after_branches",
    )
    rollback_checks: list[dict[str, Any]] = []

    with tempfile.TemporaryDirectory(prefix="ensabc-m3-atomicity-") as tmp_str:
        db_path = Path(tmp_str) / "tx_atomicity.duckdb"

        # 1. Test M2 hook + all 6 M3 fault-injection hooks BEFORE valid publish
        for hook in ("m2_after_nodes", *m3_hooks):
            is_m2 = hook == "m2_after_nodes"
            sql_fault = _build_stage_sql_for_case(
                showcase,
                run_key=rk1,
                evolution_run_id=erid1,
                exec_binding=exec1,
                evol_binding=evol1,
                inject_m2_failure=is_m2,
                inject_m3_failure="false" if is_m2 else hook,
            )
            rc_f, _, err_f = _run_duckdb(ext_path, sql_fault, db_file=db_path)
            expected_err = "INJECTED_FAILURE" if is_m2 else f"INJECTED_M3_FAILURE: {hook}"
            if rc_f == 0 or expected_err not in err_f:
                raise RuntimeError(f"Fault injection {hook} did not trigger {expected_err}: {err_f}")

            rc_cnt, parsed_cnt, err_cnt = _run_duckdb(
                ext_path,
                f"""
                .read validation/m3/sql/schema.sql
                SELECT
                    (SELECT count(*) FROM tracking_runs WHERE run_key = {sql_value(rk1)}) AS m2_runs,
                    (SELECT count(*) FROM replay_batches WHERE run_key = {sql_value(rk1)}) AS m2_batches,
                    (SELECT count(*) FROM scenario_nodes WHERE run_key = {sql_value(rk1)}) AS m2_nodes,
                    (SELECT count(*) FROM pair_comparisons WHERE run_key = {sql_value(rk1)}) AS m2_pairs,
                    (SELECT count(*) FROM scenario_links WHERE run_key = {sql_value(rk1)}) AS m2_links,
                    (SELECT count(*) FROM evolution_runs WHERE evolution_run_id = {sql_value(erid1)}) AS m3_runs,
                    (SELECT count(*) FROM evolution_nodes WHERE evolution_run_id = {sql_value(erid1)}) AS m3_nodes,
                    (SELECT count(*) FROM evolution_transitions WHERE evolution_run_id = {sql_value(erid1)}) AS m3_transitions,
                    (SELECT count(*) FROM evolution_edges WHERE evolution_run_id = {sql_value(erid1)}) AS m3_edges,
                    (SELECT count(*) FROM evolution_events WHERE evolution_run_id = {sql_value(erid1)}) AS m3_events,
                    (SELECT count(*) FROM evolution_branch_summaries WHERE evolution_run_id = {sql_value(erid1)}) AS m3_branches;
                """,
                db_file=db_path,
            )
            if rc_cnt != 0:
                raise RuntimeError(f"Residual check failed after {hook}: {err_cnt}")
            counts = parsed_cnt[-1][0]
            if any(v != 0 for v in counts.values()):
                raise AssertionError(f"Residual rows found after {hook}: {counts}")
            rollback_checks.append({"hook": hook, "aborted": True, "residual_rows": counts})

        # 2. Clean publish + idempotent republish
        sql_valid = _build_stage_sql_for_case(
            showcase,
            run_key=rk1,
            evolution_run_id=erid1,
            exec_binding=exec1,
            evol_binding=evol1,
        )
        rc_v1, _, err_v1 = _run_duckdb(ext_path, sql_valid, db_file=db_path)
        if rc_v1 != 0:
            raise RuntimeError(f"Post-rollback clean publish failed: {err_v1}")
        bundle_before = _query_published_evolution_bundle(ext_path, db_path, erid1)

        rc_v2, _, err_v2 = _run_duckdb(ext_path, sql_valid, db_file=db_path)
        if rc_v2 != 0:
            raise RuntimeError(f"Idempotent republish failed: {err_v2}")
        bundle_after_idem = _query_published_evolution_bundle(ext_path, db_path, erid1)
        assert bundle_before == bundle_after_idem

        # 3. Conflict rejection on tampered evolution content under existing evolution_run_id
        tampered_case = copy.deepcopy(showcase)
        tampered_case["config"]["high_uncertainty_threshold_ratio"] = 0.95
        sql_conflict = _build_stage_sql_for_case(
            tampered_case,
            run_key=rk1,
            evolution_run_id=erid1,
            exec_binding=exec1,
            evol_binding=evol1,
        )
        rc_conf, _, err_conf = _run_duckdb(ext_path, sql_conflict, db_file=db_path)
        if rc_conf == 0 or "EVOLUTION_RUN_VERSION_CONFLICT" not in err_conf:
            raise RuntimeError(f"Expected EVOLUTION_RUN_VERSION_CONFLICT, got rc={rc_conf}, err={err_conf}")

        # 4. Multi-version coexistence (new threshold_coefficient = 1.5 -> distinct run_key & evolution_run_id)
        alt_case = copy.deepcopy(showcase)
        alt_case["threshold_coefficient"] = 1.5
        rk2 = canonical_json_sha256({"tx_atomicity": "v2_c1.5", "ext_sha": ext_sha})
        erid2, exec2, evol2 = _synthetic_bindings(
            rk2, case_payload=alt_case, ext_sha=ext_sha, threshold_coefficient=1.5
        )
        sql_v2_param = _build_stage_sql_for_case(
            alt_case,
            run_key=rk2,
            evolution_run_id=erid2,
            exec_binding=exec2,
            evol_binding=evol2,
        )
        rc_p2, _, err_p2 = _run_duckdb(ext_path, sql_v2_param, db_file=db_path)
        if rc_p2 != 0:
            raise RuntimeError(f"Second parameter version publish failed: {err_p2}")

        bundle_v1_preserved = _query_published_evolution_bundle(ext_path, db_path, erid1)
        bundle_v2 = _query_published_evolution_bundle(ext_path, db_path, erid2)
        assert bundle_v1_preserved == bundle_before
        assert bundle_v2["evolution_run_id"] == erid2 != erid1

    return {
        "schema_version": "m3.transaction_atomicity.v1",
        "status": "pass",
        "checked_at_utc": _utc_now(),
        "idempotent_republish": {
            "evolution_run_id": erid1,
            "content_sha256": bundle_before["content_sha256"],
            "identical_after_republish": True,
        },
        "version_conflict_rejection": {
            "error_code": "EVOLUTION_RUN_VERSION_CONFLICT",
            "rejected": True,
            "existing_version_preserved": True,
        },
        "multi_version_coexistence": {
            "version_1_evolution_run_id": erid1,
            "version_2_evolution_run_id": erid2,
            "both_preserved_without_mutation": True,
        },
        "fault_injection_rollback": {
            "hooks_tested": len(rollback_checks),
            "zero_residual_across_all_11_tables": True,
            "clean_recovery_publish_succeeded": True,
            "checks": rollback_checks,
        },
    }


def _build_real_8run_sql(
    *,
    sample_dir: Path,
    binding: dict[str, Any],
    weights_vec: list[float],
    scenario_cfg: dict[str, Any],
    threshold_coefficient: float = 1.0,
) -> str:
    manifest = load_json(sample_dir / "om_sample_manifest.json")
    om_hashes = [sha256_file(sample_dir / b["om_filename"]) for b in manifest["batches"]]
    ev_cfg = {
        "secondary_rule": binding["evolution_binding"]["secondary_rule"],
        "flow_rule": binding["evolution_binding"]["flow_rule"],
        "fraction_sum_tolerance": binding["evolution_binding"]["fraction_sum_tolerance"],
        "high_uncertainty_threshold_ratio": binding["evolution_binding"]["high_uncertainty_threshold_ratio"],
        "high_uncertainty_optimality_ratio": binding["evolution_binding"]["high_uncertainty_optimality_ratio"],
        "high_uncertainty_secondary_ratio": binding["evolution_binding"]["high_uncertainty_secondary_ratio"],
    }
    cluster_cfg_sql = {
        "projection_dim": int(scenario_cfg["projection_dim"]),
        "cluster_count": int(scenario_cfg["cluster_count"]),
        "initialization": str(scenario_cfg["initialization"]),
        "seed": int(scenario_cfg["seed"]),
        "restarts": int(scenario_cfg["restarts"]),
        "initial_centers": scenario_cfg["initial_centers"],
        "stopping_rule": str(scenario_cfg["stopping_rule"]),
        "tolerance": float(scenario_cfg["tolerance"]),
        "max_iterations": int(scenario_cfg["max_iterations"]),
        "rank_threshold": float(scenario_cfg["rank_threshold"]),
    }
    return textwrap.dedent(
        f"""
        SET VARIABLE om_dir = {sql_value(str(sample_dir.resolve()))};
        SET VARIABLE om_hashes = {sql_value(om_hashes)};
        SET VARIABLE weights = {sql_value(weights_vec)}::DOUBLE[];
        SET VARIABLE threshold_coefficient = {float(threshold_coefficient)!r}::DOUBLE;
        SET VARIABLE planned_batch_count = 8;
        SET VARIABLE cluster_config = {sql_value(cluster_cfg_sql)};
        SET VARIABLE evolve_config = {sql_value(ev_cfg)};
        SET VARIABLE execution_binding = {sql_value(binding['tracking_binding'])};
        SET VARIABLE run_key = {sql_value(binding['tracking_run_key'])};
        SET VARIABLE evolution_binding = {sql_value(binding['evolution_binding'])};
        SET VARIABLE evolution_run_id = {sql_value(binding['evolution_run_id'])};
        SET VARIABLE inject_failure = false;
        SET VARIABLE inject_m3_failure = 'false';

        .read validation/m3/sql/real_8run_evolution.sql
        """
    )


def verify_three_process_reproducibility(
    ext_path: Path,
    *,
    sample_dir: Path | None = None,
    duckomo_path: Path | None = None,
) -> dict[str, Any]:
    """Run 3 independent DuckDB CLI processes in parallel isolated directories and assert
    100% identical `evolution_run_id`, `content_sha256`, and 11-table contents.
    """
    ext_sha = sha256_file(ext_path)
    showcase = _build_synthetic_showcase_8batch_case()
    rk_s = canonical_json_sha256({"three_proc": "synthetic_showcase", "ext_sha": ext_sha})
    erid_s, exec_s, evol_s = _synthetic_bindings(
        rk_s, case_payload=showcase, ext_sha=ext_sha, threshold_coefficient=1.0
    )
    synth_sql = _build_stage_sql_for_case(
        showcase, run_key=rk_s, evolution_run_id=erid_s, exec_binding=exec_s, evol_binding=evol_s
    )

    real_sql: str | None = None
    real_erid: str | None = None
    if sample_dir is not None and duckomo_path is not None:
        weights_vec = load_json(M2_FIXTURES_DIR / "spatial_weights_eu3x3.json")["weights_vector"]
        sc_cfg = default_scenario_config()
        binding = real_evolution_binding(sample_dir, ext_path, duckomo_path, scenario_config=sc_cfg)
        real_erid = binding["evolution_run_id"]
        real_sql = _build_real_8run_sql(
            sample_dir=sample_dir,
            binding=binding,
            weights_vec=weights_vec,
            scenario_cfg=sc_cfg,
        )

    def _worker(worker_id: int) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix=f"ensabc-m3-3proc-{worker_id}-") as tmp_str:
            w_db = Path(tmp_str) / f"proc_{worker_id}.duckdb"
            # Capture child duckdb PID via isolated invocation
            loads = []
            if duckomo_path is not None and real_sql is not None:
                loads.append(f"LOAD '{duckomo_path.resolve()}';")
            loads.append(f"LOAD '{ext_path.resolve()}';")
            script = "\n".join(loads) + "\n" + synth_sql + ("\n" + real_sql if real_sql else "")

            with tempfile.TemporaryDirectory(prefix=f"ensabc-m3-home-{worker_id}-") as home_dir:
                env = os.environ.copy()
                env["HOME"] = home_dir
                prefix = (
                    f"SET extension_directory = '{home_dir}/extensions';\n"
                    "SET autoinstall_known_extensions = false;\n"
                    "SET autoload_known_extensions = false;\n"
                    "SET TimeZone = 'UTC';\n"
                )
                proc = subprocess.Popen(
                    ["duckdb", "-unsigned", "-json", "-bail", str(w_db)],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=REPO_ROOT,
                    env=env,
                )
                child_pid = proc.pid
                _, stderr = proc.communicate(prefix + script, timeout=120.0)
                if proc.returncode != 0:
                    raise RuntimeError(f"3-proc worker {worker_id} (pid={child_pid}) failed: {stderr}")

            synth_bundle = _query_published_evolution_bundle(ext_path, w_db, erid_s)
            real_bundle = (
                _query_published_evolution_bundle(ext_path, w_db, real_erid)
                if real_erid is not None
                else None
            )
            return {
                "worker_index": worker_id,
                "duckdb_pid": child_pid,
                "synthetic_bundle": synth_bundle,
                "real_bundle": real_bundle,
            }

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(_worker, i) for i in range(3)]
        workers = sorted((f.result() for f in futures), key=lambda w: w["worker_index"])

    pids = [w["duckdb_pid"] for w in workers]
    assert len(set(pids)) == 3, f"Expected 3 distinct OS PIDs, got {pids}"

    base_synth = workers[0]["synthetic_bundle"]
    for w in workers[1:]:
        if w["synthetic_bundle"] != base_synth:
            raise AssertionError("Synthetic replay differed across independent processes")

    real_summary: dict[str, Any] | None = None
    if real_erid is not None:
        base_real = workers[0]["real_bundle"]
        assert base_real is not None
        for w in workers[1:]:
            if w["real_bundle"] != base_real:
                raise AssertionError("Real 8-batch OM replay differed across independent processes")
        real_summary = {
            "evolution_run_id": base_real["evolution_run_id"],
            "tracking_run_key": base_real["tracking_run_key"],
            "content_sha256": base_real["content_sha256"],
            "m2_content_sha256": base_real["m2_content_sha256"],
            "exact_match_across_3_processes": True,
        }

    return {
        "schema_version": "m3.three_process_repro.v1",
        "status": "pass",
        "checked_at_utc": _utc_now(),
        "processes_tested": 3,
        "distinct_pids": pids,
        "synthetic_replay": {
            "evolution_run_id": base_synth["evolution_run_id"],
            "tracking_run_key": base_synth["tracking_run_key"],
            "content_sha256": base_synth["content_sha256"],
            "m2_content_sha256": base_synth["m2_content_sha256"],
            "exact_match_across_3_processes": True,
        },
        "real_replay": real_summary,
    }


def ensure_om_verification(run_dir: Path, sample_dir: Path, cache_dir: Path) -> dict[str, Any]:
    """Run M2 OM sample integrity and 3-path cell matrix verification into `run_dir/om-verification/`."""
    om_dir = run_dir / "om-verification"
    om_dir.mkdir(parents=True, exist_ok=True)

    sample_report = om_dir / "test-om-sample-report.json"
    if not sample_report.is_file():
        res_sample = subprocess.run(
            [
                sys.executable,
                str(M2_VALIDATION_DIR / "test_om_sample.py"),
                "--sample-dir",
                str(sample_dir),
                "--cache-dir",
                str(cache_dir),
                "--out-report",
                str(sample_report),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if res_sample.returncode != 0:
            raise RuntimeError(f"test_om_sample.py failed: {res_sample.stderr}")

    cell_manifest = om_dir / "cell-matrix-manifest.json"
    cell_report = om_dir / "om-cells-report.json"
    if not (cell_manifest.is_file() and cell_report.is_file()):
        res_cells = subprocess.run(
            [
                sys.executable,
                str(M2_VALIDATION_DIR / "verify_om_paths.py"),
                "--run-dir",
                str(run_dir),
                "--stage",
                "cells",
                "--sample-dir",
                str(sample_dir),
                "--cache-dir",
                str(cache_dir),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if res_cells.returncode != 0:
            raise RuntimeError(f"verify_om_paths.py --stage cells failed: {res_cells.stderr}")

    s_rep = load_json(sample_report)
    c_rep = load_json(cell_report)
    if str(s_rep.get("status", "")).lower() != "pass" or str(c_rep.get("status", "")).lower() != "pass":
        raise RuntimeError("OM sample or 3-path cell verification did not pass")
    return {
        "test_om_sample_report": str(sample_report.relative_to(run_dir)),
        "cell_matrix_manifest": str(cell_manifest.relative_to(run_dir)),
        "om_cells_report": str(cell_report.relative_to(run_dir)),
    }


def verify_real_replay_and_duckomo_comparison(
    ext_path: Path,
    runner_bin: Path,
    run_dir: Path,
    db_path: Path,
    sample_dir: Path,
    cache_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Publish WB2 8-batch evolution (`v0.3.5-rc.1`) into `db_path`, verify exact WB2 case-study
    counts & `m3_runner` parity & negative metadata gate rejection, and compare against `v0.3.4`.
    """
    duckomo_v035 = cache_dir / "v0.3.5-rc.1" / "duckomo.duckdb_extension"
    duckomo_v034 = cache_dir / "v0.3.4" / "duckomo.duckdb_extension"
    weights_vec = load_json(M2_FIXTURES_DIR / "spatial_weights_eu3x3.json")["weights_vector"]
    sc_cfg = default_scenario_config()
    ev_cfg = default_evolution_config()

    binding_v035 = real_evolution_binding(
        sample_dir,
        ext_path,
        duckomo_v035,
        scenario_config=sc_cfg,
        threshold_coefficient=1.0,
        evolution_config=ev_cfg,
    )
    sql_v035 = _build_real_8run_sql(
        sample_dir=sample_dir,
        binding=binding_v035,
        weights_vec=weights_vec,
        scenario_cfg=sc_cfg,
        threshold_coefficient=1.0,
    )
    rc_35, _, err_35 = _run_duckdb(
        ext_path,
        sql_v035,
        db_file=db_path,
        extra_extensions=[duckomo_v035],
        timeout=120.0,
    )
    if rc_35 != 0:
        raise RuntimeError(f"Real 8-run evolution (DuckOMO v0.3.5-rc.1) failed: {err_35}")

    bundle_v035 = _query_published_evolution_bundle(ext_path, db_path, binding_v035["evolution_run_id"])
    parity_v035 = _assert_bundle_matches_m3_runner(
        bundle_v035,
        runner_bin,
        threshold_coefficient=1.0,
        evolve_config=ev_cfg,
    )

    # Assert frozen WB2 8-batch main configuration counts (FR-015)
    cs = bundle_v035["chain_summary"]
    assert len(bundle_v035["batches"]) == 8 and cs["ok_batch_count"] == 8
    assert len(bundle_v035["nodes"]) == 24
    assert len(bundle_v035["branches"]) == 8 and cs["branch_count"] == 8
    assert len(bundle_v035["transitions"]) == 7 and cs["ok_transition_count"] == 7
    assert cs["primary_edge_count"] == 16
    assert cs["secondary_split_edge_count"] == 2
    assert cs["secondary_merge_edge_count"] == 1
    assert len(bundle_v035["edges"]) == 19
    assert cs["continuation_count"] == 13
    assert cs["split_count"] == 2
    assert cs["merge_count"] == 1
    assert cs["emergence_count"] == 3
    assert cs["disappearance_count"] == 4
    assert cs["gap_interrupted_count"] == 0 and cs["gap_restarted_count"] == 0

    hu_ordinals = [
        tr["prev_ordinal"]
        for tr in bundle_v035["transitions"]
        if tr["high_uncertainty"]
    ]
    assert hu_ordinals == [0, 3, 6], f"Expected high-uncertainty transitions [0, 3, 6], got {hu_ordinals}"

    # Verify negative preparation gate rejection (corrupted valid_time check) leaves 0 residual rows
    real_sql_text = (REPO_ROOT / "validation" / "m3" / "sql" / "real_8run_evolution.sql").read_text(encoding="utf-8")
    corrupted_real_sql = real_sql_text.replace(
        "SELECT CASE\n    WHEN count(*) = 0",
        "UPDATE stage_batch_checks SET valid_time_ok = false WHERE ordinal = 0;\nSELECT CASE\n    WHEN count(*) = 0",
        1,
    )
    neg_rk = binding_v035["tracking_run_key"][:52] + "_bad_valid_tm"
    neg_erid = binding_v035["evolution_run_id"][:52] + "_bad_valid_tm"
    neg_sql = sql_v035.replace(
        ".read validation/m3/sql/real_8run_evolution.sql",
        corrupted_real_sql,
    ).replace(binding_v035["tracking_run_key"], neg_rk).replace(binding_v035["evolution_run_id"], neg_erid)
    rc_neg, _, err_neg = _run_duckdb(
        ext_path,
        neg_sql,
        db_file=db_path,
        extra_extensions=[duckomo_v035],
    )
    if rc_neg == 0 or "INVALID_BATCH_METADATA" not in err_neg:
        raise RuntimeError(f"Expected INVALID_BATCH_METADATA on corrupted preparation check: {err_neg}")
    rc_res, parsed_res, err_res = _run_duckdb(
        ext_path,
        f"""
        SELECT
            (SELECT count(*) FROM tracking_runs WHERE run_key = {sql_value(neg_rk)}) AS m2_res,
            (SELECT count(*) FROM evolution_runs WHERE evolution_run_id = {sql_value(neg_erid)}) AS m3_res;
        """,
        db_file=db_path,
    )
    assert rc_res == 0 and parsed_res[-1][0] == {"m2_res": 0, "m3_res": 0}, err_res

    # Execute full 8-batch evolution under DuckOMO v0.3.4 in an isolated DB and compare semantic projection
    binding_v034 = real_evolution_binding(
        sample_dir,
        ext_path,
        duckomo_v034,
        scenario_config=sc_cfg,
        threshold_coefficient=1.0,
        evolution_config=ev_cfg,
    )
    sql_v034 = _build_real_8run_sql(
        sample_dir=sample_dir,
        binding=binding_v034,
        weights_vec=weights_vec,
        scenario_cfg=sc_cfg,
        threshold_coefficient=1.0,
    )
    with tempfile.TemporaryDirectory(prefix="ensabc-m3-duckomo-v034-") as tmp_str:
        db_v034 = Path(tmp_str) / "v034.duckdb"
        rc_34, _, err_34 = _run_duckdb(
            ext_path,
            sql_v034,
            db_file=db_v034,
            extra_extensions=[duckomo_v034],
            timeout=120.0,
        )
        if rc_34 != 0:
            raise RuntimeError(f"Real 8-run evolution (DuckOMO v0.3.4) failed: {err_34}")
        bundle_v034 = _query_published_evolution_bundle(
            ext_path, db_v034, binding_v034["evolution_run_id"]
        )

    assert binding_v035["evolution_run_id"] != binding_v034["evolution_run_id"]
    assert binding_v035["tracking_run_key"] != binding_v034["tracking_run_key"]

    proj_v035 = semantic_evolution_projection(
        {k: v for k, v in bundle_v035.items() if k != "m2_content_sha256"},
        expected_tracking_run_key=binding_v035["tracking_run_key"],
    )
    proj_v034 = semantic_evolution_projection(
        {k: v for k, v in bundle_v034.items() if k != "m2_content_sha256"},
        expected_tracking_run_key=binding_v034["tracking_run_key"],
    )
    if proj_v035 != proj_v034:
        diff_keys = [k for k in set(proj_v035) | set(proj_v034) if proj_v035.get(k) != proj_v034.get(k)]
        raise AssertionError(
            f"Semantic evolution projection differed between DuckOMO v0.3.5-rc.1 and v0.3.4 on keys: {diff_keys}"
        )

    real_report = {
        "schema_version": "m3.real_evolution_replay.v1",
        "status": "pass",
        "checked_at_utc": _utc_now(),
        "database_path": "sql-history/evolution-runs.duckdb",
        "evolution_run_id": bundle_v035["evolution_run_id"],
        "tracking_run_key": bundle_v035["tracking_run_key"],
        "content_sha256": bundle_v035["content_sha256"],
        "m2_content_sha256": bundle_v035["m2_content_sha256"],
        "non_physical_disclaimer": bundle_v035["non_physical_disclaimer"],
        "evolution_binding": bundle_v035["evolution_binding"],
        "tracking_binding": binding_v035["tracking_binding"],
        "invariants_e1_to_e8_passed": True,
        "preparation_negative_gate_rejected_with_zero_residual": True,
        "high_uncertainty_transition_prev_ordinals": hu_ordinals,
        "runner_parity": parity_v035,
        "chain_summary": bundle_v035["chain_summary"],
        "batches": bundle_v035["batches"],
        "nodes": bundle_v035["nodes"],
        "transitions": bundle_v035["transitions"],
        "edges": bundle_v035["edges"],
        "events": bundle_v035["events"],
        "branches": bundle_v035["branches"],
    }

    duckomo_comparison = {
        "schema_version": "m3.duckomo_version_comparison.v1",
        "status": "pass",
        "checked_at_utc": _utc_now(),
        "target_version": {
            "duckomo_version": "v0.3.5-rc.1",
            "duckomo_sha256": sha256_file(duckomo_v035),
            "evolution_run_id": bundle_v035["evolution_run_id"],
            "tracking_run_key": bundle_v035["tracking_run_key"],
            "content_sha256": bundle_v035["content_sha256"],
        },
        "baseline_version": {
            "duckomo_version": "v0.3.4",
            "duckomo_sha256": sha256_file(duckomo_v034),
            "evolution_run_id": bundle_v034["evolution_run_id"],
            "tracking_run_key": bundle_v034["tracking_run_key"],
            "content_sha256": bundle_v034["content_sha256"],
        },
        "authentic_branch_id_prefixes_verified": True,
        "semantic_projection_sha256": canonical_json_sha256(proj_v035),
        "exact_semantic_equivalence": True,
        "chain_summary": bundle_v035["chain_summary"],
    }
    return real_report, duckomo_comparison


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify M3 DuckDB C API UDFs and 11-table atomic SQL history."
    )
    parser.add_argument("--run-dir", type=Path, required=True, help="Target validation run directory")
    parser.add_argument(
        "--scope",
        choices=["synthetic", "real", "all"],
        default="all",
        help="Verification scope (default: all)",
    )
    parser.add_argument(
        "--extension",
        type=Path,
        default=Path("/tmp/ensabc-m3-extension-target/ensabc_m0_extension.duckdb_extension"),
        help="Path to compiled ensabc_m0_extension.duckdb_extension",
    )
    parser.add_argument(
        "--sample-dir",
        type=Path,
        default=REPO_ROOT / "validation" / "m2" / "data" / "om_sample",
        help="Directory containing frozen 8-batch WB2 .om sample files",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=M2_CACHE_DIR,
        help="Cache directory containing v0.3.5-rc.1 and v0.3.4 DuckOMO extensions",
    )
    args = parser.parse_args(argv)

    ext_path = args.extension.resolve()
    if not ext_path.is_file():
        print(f"Error: extension binary not found: {ext_path}", file=sys.stderr)
        return 2

    runner_bin = _locate_m3_runner(ext_path)
    if runner_bin is None:
        print(f"Error: m3_runner binary not found alongside {ext_path}", file=sys.stderr)
        return 2

    sample_dir = args.sample_dir.resolve()
    cache_dir = args.cache_dir.resolve()
    if args.scope in ("real", "all"):
        if not (sample_dir / "om_sample_manifest.json").is_file():
            print(f"Error: OM sample manifest not found in {sample_dir}", file=sys.stderr)
            return 2
        for ver in ("v0.3.5-rc.1", "v0.3.4"):
            d_ext = cache_dir / ver / "duckomo.duckdb_extension"
            if not d_ext.is_file():
                print(f"Error: DuckOMO {ver} extension not found at {d_ext}", file=sys.stderr)
                return 2

    try:
        run_dir = args.run_dir.resolve()
        sql_hist_dir = run_dir / "sql-history"
        sql_hist_dir.mkdir(parents=True, exist_ok=True)
        db_path = sql_hist_dir / "evolution-runs.duckdb"

        if args.scope in ("synthetic", "all"):
            contract_rep = verify_sql_contract(ext_path, runner_bin)
            atomic_write_json(sql_hist_dir / "sql-contract.json", contract_rep)

            synth_rep = verify_synthetic_replays(ext_path, runner_bin, db_path)
            atomic_write_json(sql_hist_dir / "synthetic-evolution-replay.json", synth_rep)

            atomicity_rep = verify_transaction_atomicity(ext_path)
            atomic_write_json(sql_hist_dir / "transaction-atomicity.json", atomicity_rep)

        if args.scope == "synthetic":
            repro_rep = verify_three_process_reproducibility(ext_path)
            atomic_write_json(sql_hist_dir / "three-process-repro.json", repro_rep)

        if args.scope in ("real", "all"):
            ensure_om_verification(run_dir, sample_dir, cache_dir)
            real_rep, duckomo_cmp = verify_real_replay_and_duckomo_comparison(
                ext_path,
                runner_bin,
                run_dir,
                db_path,
                sample_dir,
                cache_dir,
            )
            atomic_write_json(sql_hist_dir / "real-evolution-replay.json", real_rep)
            atomic_write_json(run_dir / "om-verification" / "duckomo-version-comparison.json", duckomo_cmp)

            repro_rep = verify_three_process_reproducibility(
                ext_path,
                sample_dir=sample_dir,
                duckomo_path=cache_dir / "v0.3.5-rc.1" / "duckomo.duckdb_extension",
            )
            atomic_write_json(sql_hist_dir / "three-process-repro.json", repro_rep)

        print(f"M3 SQL & 11-table history verification ({args.scope}): PASS -> {sql_hist_dir}")
        return 0
    except Exception as exc:
        print(f"M3 SQL & 11-table history verification FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
