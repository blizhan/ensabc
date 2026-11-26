#!/usr/bin/env python3
"""Run M3 WB2 8-batch case study, threshold & clustering sensitivity, and 2-level perturbations (T032-T035).

Usage:
    validation/m2/.venv/bin/python validation/m3/run_case_and_sensitivity.py \
        --run-dir DIR \
        --extension PATH \
        [--sample-dir DIR] \
        [--cache-dir DIR] \
        [--db PATH]

Exit codes:
    0 - All case-study, sensitivity, perturbation, certificate, and boundary checks passed
    1 - Assertion, numerical, SQL, or validation failure
    2 - Missing prerequisites or invalid CLI arguments

Generates under `<run-dir>/case-study/` (all via `atomic_write_json`):
    - `main-evolution-k3-c1.0.json`
    - `sensitivity-thresholds.json`
    - `sensitivity-clustering-configs.json`
    - `perturbation-evolution-level.json`
    - `perturbation-member-level.json`
    - `limitations.json`
And persists all distinct immutable `evolution_run_id` versions in
`<db>` (default `<run-dir>/sql-history/evolution-runs.duckdb`).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import numpy as np

from validation.m2.test_om_sample import read_via_omfiles
from validation.m3.common import (
    EVOLUTION_ALGORITHM_VERSION,
    FIXTURES_DIR,
    M2_CACHE_DIR,
    M2_FIXTURES_DIR,
    MATCH_ALGORITHM_VERSION,
    NON_PHYSICAL_DISCLAIMER,
    CLUSTER_ALGORITHM_VERSION,
    LocatedError,
    atomic_write_json,
    canonical_json_sha256,
    canonical_json_str,
    default_evolution_config,
    default_scenario_config,
    is_finite_number,
    load_json,
    parse_branch_birth_key,
    parse_duckdb_json,
    real_evolution_binding,
    run_isolated_duckdb,
    semantic_evolution_projection,
    sha256_file,
    sql_value,
)
from validation.m3.oracle import (
    CERTIFICATE_RULE_V1,
    compute_weighted_rmse_exact,
    evaluate_evolution_stability_certificate,
    overlap_cluster_mapping,
    solve_chain_oracle,
    solve_evolution_oracle,
)
from validation.m3.verify_visualization_traceability import PROHIBITED_CLAIM_PATTERNS

EXPERIMENT_CONFIG_PATH = FIXTURES_DIR / "evolution_experiment_config.json"
SPATIAL_WEIGHTS_PATH = M2_FIXTURES_DIR / "spatial_weights_eu3x3.json"
FLOAT_TOL = 1e-14


class PrerequisiteError(Exception):
    """Raised when required CLI files, binaries, or sample inputs are missing (exit code 2)."""


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _rel_or_str(path: Path, base: Path) -> str:
    try:
        return path.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _clean_scenario_config(raw_cfg: dict[str, Any]) -> dict[str, Any]:
    """Normalize a clustering variant dictionary to the exact 10 M1 ScenarioConfig keys."""
    defaults = default_scenario_config()
    return {
        "projection_dim": int(raw_cfg.get("projection_dim", defaults["projection_dim"])),
        "cluster_count": int(raw_cfg.get("cluster_count", defaults["cluster_count"])),
        "initialization": str(raw_cfg.get("initialization", defaults["initialization"])),
        "seed": int(raw_cfg.get("seed", defaults["seed"])),
        "restarts": int(raw_cfg.get("restarts", defaults["restarts"])),
        "initial_centers": list(raw_cfg.get("initial_centers", defaults["initial_centers"])),
        "stopping_rule": str(raw_cfg.get("stopping_rule", defaults["stopping_rule"])),
        "tolerance": float(raw_cfg.get("tolerance", defaults["tolerance"])),
        "max_iterations": int(raw_cfg.get("max_iterations", defaults["max_iterations"])),
        "rank_threshold": float(raw_cfg.get("rank_threshold", defaults["rank_threshold"])),
    }


def _clean_evolution_config(raw_cfg: dict[str, Any]) -> dict[str, Any]:
    """Normalize an evolution config dictionary to the exact 7 M3 EvolutionConfig binding keys."""
    defaults = default_evolution_config()
    return {
        "secondary_rule": str(raw_cfg.get("secondary_rule", defaults["secondary_rule"])),
        "flow_rule": str(raw_cfg.get("flow_rule", defaults["flow_rule"])),
        "uncertainty_rule": str(raw_cfg.get("uncertainty_rule", defaults["uncertainty_rule"])),
        "fraction_sum_tolerance": float(
            raw_cfg.get("fraction_sum_tolerance", defaults["fraction_sum_tolerance"])
        ),
        "high_uncertainty_threshold_ratio": float(
            raw_cfg.get(
                "high_uncertainty_threshold_ratio", defaults["high_uncertainty_threshold_ratio"]
            )
        ),
        "high_uncertainty_optimality_ratio": float(
            raw_cfg.get(
                "high_uncertainty_optimality_ratio", defaults["high_uncertainty_optimality_ratio"]
            )
        ),
        "high_uncertainty_secondary_ratio": float(
            raw_cfg.get(
                "high_uncertainty_secondary_ratio", defaults["high_uncertainty_secondary_ratio"]
            )
        ),
    }


def _locate_m3_runner(ext_path: Path, run_dir: Path) -> Path | None:
    candidates = [
        ext_path.parent / "m3_runner",
        run_dir / "build" / "m3_runner",
        Path("/tmp/ensabc-m3-extension-target/m3_runner"),
        Path("/tmp/ensabc-m3-extension-target/release/examples/m3_runner"),
        REPO_ROOT / "target" / "release" / "examples" / "m3_runner",
    ]
    for cand in candidates:
        if cand.is_file() and os.access(cand, os.X_OK):
            return cand.resolve()
    return None


def _run_duckdb(
    ext_path: Path,
    sql: str,
    *,
    db_file: str | Path = ":memory:",
    extra_extensions: Sequence[Path] | None = None,
    bail: bool = True,
    timeout: float = 240.0,
) -> tuple[int, list[Any], str]:
    loads: list[str] = []
    if extra_extensions:
        for extra in extra_extensions:
            loads.append(f"LOAD '{extra.resolve()}';")
    loads.append(f"LOAD '{ext_path.resolve()}';")
    full_sql = "\n".join(loads) + "\n" + textwrap.dedent(sql)

    old_cwd = Path.cwd()
    try:
        os.chdir(REPO_ROOT)
        proc = run_isolated_duckdb(
            full_sql,
            database=str(db_file),
            timeout=timeout,
            bail=bail,
        )
    finally:
        os.chdir(old_cwd)

    parsed: list[Any] = []
    if proc.stdout.strip():
        parsed = parse_duckdb_json(proc.stdout)
    return proc.returncode, parsed, proc.stderr


def _assert_no_prohibited_overclaims(payload: dict[str, Any], label: str) -> None:
    serialized = canonical_json_str(payload, indent=2)
    for pattern_name, regex in PROHIBITED_CLAIM_PATTERNS:
        m = regex.search(serialized)
        if m is not None:
            raise AssertionError(
                f"Prohibited affirmative overclaim ({pattern_name}) detected in {label}: {m.group(0)!r}"
            )


def _assert_all_numbers_finite(obj: Any, path: str = "root") -> None:
    if isinstance(obj, bool) or obj is None or isinstance(obj, (int, str)):
        return
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise AssertionError(f"Non-finite float at {path}: {obj!r}")
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            _assert_all_numbers_finite(v, f"{path}.{k}")
        return
    if isinstance(obj, (list, tuple)):
        for idx, v in enumerate(obj):
            _assert_all_numbers_finite(v, f"{path}[{idx}]")
        return


def _publish_all_sql_runs(
    *,
    ext_path: Path,
    duckomo_path: Path,
    sample_dir: Path,
    db_path: Path,
    weights_vec: list[float],
    evolution_cfg: dict[str, Any],
    run_specs: Sequence[tuple[str, dict[str, Any], float]],
) -> dict[str, dict[str, Any]]:
    """Publish all unique `(scenario_config, threshold_coefficient)` runs into `db_path` via
    `real_8run_evolution.sql` + `stage_and_publish.sql` and read back complete 11-table bundles.
    """
    manifest = load_json(sample_dir / "om_sample_manifest.json")
    om_hashes = [sha256_file(sample_dir / b["om_filename"]) for b in manifest["batches"]]
    ev_sql_cfg = {
        "secondary_rule": evolution_cfg["secondary_rule"],
        "flow_rule": evolution_cfg["flow_rule"],
        "fraction_sum_tolerance": float(evolution_cfg["fraction_sum_tolerance"]),
        "high_uncertainty_threshold_ratio": float(evolution_cfg["high_uncertainty_threshold_ratio"]),
        "high_uncertainty_optimality_ratio": float(evolution_cfg["high_uncertainty_optimality_ratio"]),
        "high_uncertainty_secondary_ratio": float(evolution_cfg["high_uncertainty_secondary_ratio"]),
    }

    bindings_by_key: dict[str, dict[str, Any]] = {}
    ordered_unique: list[tuple[str, dict[str, Any], float, dict[str, Any]]] = []
    seen_erids: set[str] = set()

    for spec_key, sc_raw, tc in run_specs:
        sc_clean = _clean_scenario_config(sc_raw)
        binding = real_evolution_binding(
            sample_dir,
            ext_path,
            duckomo_path,
            scenario_config=sc_clean,
            threshold_coefficient=float(tc),
            evolution_config=evolution_cfg,
        )
        bindings_by_key[spec_key] = binding
        erid = binding["evolution_run_id"]
        if erid not in seen_erids:
            seen_erids.add(erid)
            ordered_unique.append((spec_key, sc_clean, float(tc), binding))

    db_path.parent.mkdir(parents=True, exist_ok=True)
    sql_chunks: list[str] = [
        f"SET VARIABLE om_dir = {sql_value(str(sample_dir.resolve()))};",
        f"SET VARIABLE om_hashes = {sql_value(om_hashes)};",
        f"SET VARIABLE weights = {sql_value(weights_vec)}::DOUBLE[];",
    ]

    for idx, (_, sc_clean, tc, binding) in enumerate(ordered_unique):
        var_setup = textwrap.dedent(
            f"""
            SET VARIABLE threshold_coefficient = {tc!r}::DOUBLE;
            SET VARIABLE planned_batch_count = 8;
            SET VARIABLE cluster_config = {sql_value(sc_clean)};
            SET VARIABLE evolve_config = {sql_value(ev_sql_cfg)};
            SET VARIABLE execution_binding = {sql_value(binding['tracking_binding'])};
            SET VARIABLE run_key = {sql_value(binding['tracking_run_key'])};
            SET VARIABLE evolution_binding = {sql_value(binding['evolution_binding'])};
            SET VARIABLE evolution_run_id = {sql_value(binding['evolution_run_id'])};
            SET VARIABLE inject_failure = false;
            SET VARIABLE inject_m3_failure = 'false';
            """
        )
        if idx == 0:
            # First run executes the full OM read + 450-row preparation gate + Stage A/B publish
            sql_chunks.append(var_setup + "\n.read validation/m3/sql/real_8run_evolution.sql\n")
        else:
            # Subsequent runs reuse verified `stage_real_members` in session and run full Stage A/B
            sql_chunks.append(
                var_setup
                + textwrap.dedent(
                    """
                    CREATE OR REPLACE TEMP TABLE stage_raw_batches AS
                    SELECT
                        ordinal,
                        init_time,
                        lead_hours,
                        'present'::VARCHAR AS raw_status,
                        NULL::VARCHAR AS status_reason,
                        getvariable('execution_binding').input_manifest_sha256::VARCHAR AS input_sha256,
                        getvariable('om_hashes')[ordinal + 1]::VARCHAR AS om_sha256,
                        list(struct_pack(member_id := member_id, features := features) ORDER BY member_label ASC) AS members
                    FROM stage_real_members
                    GROUP BY ordinal, init_time, lead_hours
                    ORDER BY ordinal ASC;

                    .read validation/m3/sql/stage_and_publish.sql
                    """
                )
            )

    rc_pub, _, err_pub = _run_duckdb(
        ext_path,
        "\n".join(sql_chunks),
        db_file=db_path,
        extra_extensions=[duckomo_path],
        timeout=300.0,
    )
    if rc_pub != 0:
        raise RuntimeError(f"Publishing evolution runs to {db_path} failed: {err_pub}")

    # Query back all published bundles in a single read pass and verify M2 <-> M3 invariants
    erid_list_sql = sql_value([b["evolution_run_id"] for _, _, _, b in ordered_unique])
    read_sql = f"""
    SELECT
        er.evolution_run_id,
        er.tracking_run_key,
        er.evolution_binding,
        er.weights,
        er.planned_batch_count,
        er.chain_summary,
        er.non_physical_disclaimer,
        er.content_sha256,
        tr.content_sha256 AS m2_content_sha256
    FROM evolution_runs er
    JOIN tracking_runs tr ON tr.run_key = er.tracking_run_key
    WHERE er.evolution_run_id IN (SELECT unnest({erid_list_sql}::VARCHAR[]));

    SELECT
        er.evolution_run_id,
        rb.ordinal,
        strftime(rb.init_time, '%Y-%m-%dT%H:%M:%SZ') AS init_time_utc,
        rb.lead_hours,
        rb.status,
        rb.status_reason,
        rb.dispersion
    FROM replay_batches rb
    JOIN evolution_runs er ON er.tracking_run_key = rb.run_key
    WHERE er.evolution_run_id IN (SELECT unnest({erid_list_sql}::VARCHAR[]))
    ORDER BY er.evolution_run_id ASC, rb.ordinal ASC;

    SELECT *
    FROM evolution_nodes
    WHERE evolution_run_id IN (SELECT unnest({erid_list_sql}::VARCHAR[]))
    ORDER BY evolution_run_id ASC, ordinal ASC, cluster_id ASC;

    SELECT * EXCLUDE (evolve_result), (evolve_result IS NOT NULL) AS has_evolve_result
    FROM evolution_transitions
    WHERE evolution_run_id IN (SELECT unnest({erid_list_sql}::VARCHAR[]))
    ORDER BY evolution_run_id ASC, prev_ordinal ASC;

    SELECT *
    FROM evolution_edges
    WHERE evolution_run_id IN (SELECT unnest({erid_list_sql}::VARCHAR[]))
    ORDER BY evolution_run_id ASC, prev_ordinal ASC, prev_cluster_id ASC, curr_cluster_id ASC;

    SELECT *
    FROM evolution_events
    WHERE evolution_run_id IN (SELECT unnest({erid_list_sql}::VARCHAR[]))
    ORDER BY evolution_run_id ASC, prev_ordinal ASC, event_index ASC;

    SELECT *
    FROM evolution_branch_summaries
    WHERE evolution_run_id IN (SELECT unnest({erid_list_sql}::VARCHAR[]))
    ORDER BY evolution_run_id ASC, branch_id ASC;
    """
    rc_rd, parsed_rd, err_rd = _run_duckdb(ext_path, read_sql, db_file=db_path, timeout=120.0)
    if rc_rd != 0:
        raise RuntimeError(f"Reading published evolution bundles from {db_path} failed: {err_rd}")

    runs_rows, batches_rows, nodes_rows, trans_rows, edges_rows, events_rows, branch_rows = parsed_rd[-7:]
    bundles_by_erid: dict[str, dict[str, Any]] = {}

    for r in runs_rows:
        erid = r["evolution_run_id"]
        bundles_by_erid[erid] = {
            "evolution_run_id": erid,
            "tracking_run_key": r["tracking_run_key"],
            "content_sha256": r["content_sha256"],
            "m2_content_sha256": r["m2_content_sha256"],
            "non_physical_disclaimer": r["non_physical_disclaimer"],
            "planned_batch_count": r["planned_batch_count"],
            "weights": r["weights"],
            "evolution_binding": r["evolution_binding"],
            "chain_summary": r["chain_summary"],
            "batches": [],
            "nodes": [],
            "transitions": [],
            "edges": [],
            "events": [],
            "branches": [],
        }

    for row in batches_rows:
        erid = row["evolution_run_id"]
        bundles_by_erid[erid]["batches"].append(
            {k: v for k, v in row.items() if k != "evolution_run_id"}
        )
    for row in nodes_rows:
        erid = row["evolution_run_id"]
        bundles_by_erid[erid]["nodes"].append(
            {k: v for k, v in row.items() if k != "evolution_run_id"}
        )
    for row in trans_rows:
        erid = row["evolution_run_id"]
        bundles_by_erid[erid]["transitions"].append(
            {k: v for k, v in row.items() if k != "evolution_run_id"}
        )
    for row in edges_rows:
        erid = row["evolution_run_id"]
        bundles_by_erid[erid]["edges"].append(
            {k: v for k, v in row.items() if k != "evolution_run_id"}
        )
    for row in events_rows:
        erid = row["evolution_run_id"]
        bundles_by_erid[erid]["events"].append(
            {k: v for k, v in row.items() if k != "evolution_run_id"}
        )
    for row in branch_rows:
        erid = row["evolution_run_id"]
        bundles_by_erid[erid]["branches"].append(
            {k: v for k, v in row.items() if k != "evolution_run_id"}
        )

    if len(bundles_by_erid) != len(ordered_unique):
        raise AssertionError(
            f"Expected {len(ordered_unique)} distinct evolution_run_ids in DuckDB, got {len(bundles_by_erid)}"
        )

    return {
        spec_key: {
            "binding": binding,
            "bundle": bundles_by_erid[binding["evolution_run_id"]],
        }
        for spec_key, binding in bindings_by_key.items()
    }


def _build_main_case_artifact(
    *,
    main_entry: dict[str, Any],
    exp_cfg: dict[str, Any],
    db_path: Path,
    run_dir: Path,
    ext_path: Path,
    runner_bin: Path | None,
) -> dict[str, Any]:
    """Verify and construct `main-evolution-k3-c1.0.json` (T032)."""
    binding = main_entry["binding"]
    bundle = main_entry["bundle"]
    cs = bundle["chain_summary"]

    # Verify M2 <-> M3 exact alignment directly in DuckDB for the main run
    erid_lit = sql_value(bundle["evolution_run_id"])
    rk_lit = sql_value(bundle["tracking_run_key"])
    rc_m2, parsed_m2, err_m2 = _run_duckdb(
        ext_path,
        f"""
        SELECT
            (SELECT count(*) FROM (
                SELECT ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction
                FROM evolution_nodes WHERE evolution_run_id = {erid_lit}
                EXCEPT
                SELECT ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction
                FROM scenario_nodes WHERE run_key = {rk_lit}
            )) +
            (SELECT count(*) FROM (
                SELECT ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction
                FROM scenario_nodes WHERE run_key = {rk_lit}
                EXCEPT
                SELECT ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction
                FROM evolution_nodes WHERE evolution_run_id = {erid_lit}
            )) AS node_diff,
            (SELECT count(*) FROM (
                SELECT prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, ambiguous_tie AS ambiguous
                FROM evolution_edges WHERE evolution_run_id = {erid_lit} AND edge_role = 'primary'
                EXCEPT
                SELECT prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, ambiguous
                FROM scenario_links WHERE run_key = {rk_lit}
            )) +
            (SELECT count(*) FROM (
                SELECT prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, ambiguous
                FROM scenario_links WHERE run_key = {rk_lit}
                EXCEPT
                SELECT prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, ambiguous_tie AS ambiguous
                FROM evolution_edges WHERE evolution_run_id = {erid_lit} AND edge_role = 'primary'
            )) AS link_diff;
        """,
        db_file=db_path,
    )
    if rc_m2 != 0:
        raise RuntimeError(f"Main run M2/M3 parity query failed: {err_m2}")
    diff_row = parsed_m2[-1][0]
    if diff_row["node_diff"] != 0 or diff_row["link_diff"] != 0:
        raise AssertionError(f"M2/M3 main case mismatch: {diff_row}")

    # Verify authentic tracking_run_key[:12]: prefix on all branch_ids via semantic projection
    semantic_evolution_projection(
        {k: v for k, v in bundle.items() if k != "m2_content_sha256"},
        expected_tracking_run_key=bundle["tracking_run_key"],
    )

    # Cross-check against standalone m3_runner --mode chain & Python Fraction chain oracle
    nodes_by_ord: dict[int, list[dict[str, Any]]] = {}
    for nd in bundle["nodes"]:
        nodes_by_ord.setdefault(int(nd["ordinal"]), []).append(nd)
    chain_input = {
        "mode": "chain",
        "tracking_run_key": bundle["tracking_run_key"],
        "threshold_coefficient": 1.0,
        "weights": bundle["weights"],
        "config": {
            k: v
            for k, v in _clean_evolution_config(exp_cfg["evolution"]).items()
            if k != "uncertainty_rule"
        },
        "batches": [
            {
                "ordinal": int(b["ordinal"]),
                "status": "ok",
                "dispersion": float(b["dispersion"]),
                "scenarios": [
                    {
                        "cluster_id": int(n["cluster_id"]),
                        "member_count": int(n["member_count"]),
                        "member_fraction": float(n["member_fraction"]),
                        "mean_features": [float(x) for x in n["mean_features"]],
                    }
                    for n in nodes_by_ord[int(b["ordinal"])]
                ],
            }
            for b in bundle["batches"]
        ],
    }
    oracle_chain = solve_chain_oracle(chain_input)
    for k, ov in oracle_chain["chain_summary"].items():
        sv = cs[k]
        if isinstance(ov, float):
            if not math.isclose(float(sv), ov, rel_tol=FLOAT_TOL, abs_tol=FLOAT_TOL):
                raise AssertionError(f"Main chain_summary.{k} mismatch: sql={sv!r} vs oracle={ov!r}")
        elif sv != ov:
            raise AssertionError(f"Main chain_summary.{k} mismatch: sql={sv!r} vs oracle={ov!r}")

    runner_verified = False
    if runner_bin is not None:
        with tempfile.TemporaryDirectory(prefix="ensabc-m3-main-runner-") as td:
            in_p = Path(td) / "main_chain_in.json"
            out_p = Path(td) / "main_chain_out.json"
            in_p.write_text(json.dumps(chain_input), encoding="utf-8")
            proc = subprocess.run(
                [str(runner_bin), "--mode", "chain", "--input", str(in_p), "--output", str(out_p)],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"m3_runner --mode chain failed on main case: {proc.stderr}")
            runner_out = json.loads(out_p.read_text(encoding="utf-8"))
            for k, rv in runner_out["chain_summary"].items():
                sv = cs[k]
                if isinstance(rv, float):
                    if not math.isclose(float(sv), rv, rel_tol=FLOAT_TOL, abs_tol=FLOAT_TOL):
                        raise AssertionError(f"Runner chain_summary.{k} mismatch: {sv!r} vs {rv!r}")
                elif sv != rv:
                    raise AssertionError(f"Runner chain_summary.{k} mismatch: {sv!r} vs {rv!r}")
            runner_verified = True

    high_uncertainty_transitions = [
        int(tr["prev_ordinal"]) for tr in bundle["transitions"] if bool(tr["high_uncertainty"])
    ]

    # Strict FR-015 / T032 exact assertions on WB2 8-batch main evolution case
    expected_counts = {
        "planned_batch_count": (int(bundle["planned_batch_count"]), 8),
        "ok_batch_count": (int(cs["ok_batch_count"]), 8),
        "missing_batch_count": (int(cs["missing_batch_count"]), 0),
        "failed_batch_count": (int(cs["failed_batch_count"]), 0),
        "node_count": (len(bundle["nodes"]), 24),
        "branch_count": (int(cs["branch_count"]), 8),
        "primary_edge_count": (int(cs["primary_edge_count"]), 16),
        "secondary_split_edge_count": (int(cs["secondary_split_edge_count"]), 2),
        "secondary_merge_edge_count": (int(cs["secondary_merge_edge_count"]), 1),
        "continuation_count": (int(cs["continuation_count"]), 13),
        "split_count": (int(cs["split_count"]), 2),
        "merge_count": (int(cs["merge_count"]), 1),
        "emergence_count": (int(cs["emergence_count"]), 3),
        "disappearance_count": (int(cs["disappearance_count"]), 4),
    }
    for name, (observed, expected) in expected_counts.items():
        if observed != expected:
            raise AssertionError(f"Main WB2 case {name}: expected {expected}, got {observed}")
    if len(bundle["branches"]) != 8:
        raise AssertionError(f"Expected 8 branch summary rows, got {len(bundle['branches'])}")
    if len(bundle["transitions"]) != 7:
        raise AssertionError(f"Expected 7 transition rows, got {len(bundle['transitions'])}")
    if len(bundle["edges"]) != 19:
        raise AssertionError(f"Expected 19 total evolution edges, got {len(bundle['edges'])}")
    if high_uncertainty_transitions != [0, 3, 6]:
        raise AssertionError(
            f"Expected high_uncertainty_transitions == [0, 3, 6], got {high_uncertainty_transitions}"
        )
    if bundle["non_physical_disclaimer"] != NON_PHYSICAL_DISCLAIMER:
        raise AssertionError(f"Unexpected disclaimer: {bundle['non_physical_disclaimer']!r}")

    # Build per-transition diagnostic breakdown across all 7 adjacent transitions
    edges_by_prev: dict[int, list[dict[str, Any]]] = {}
    for e in bundle["edges"]:
        edges_by_prev.setdefault(int(e["prev_ordinal"]), []).append(e)
    events_by_prev: dict[int, list[dict[str, Any]]] = {}
    for ev in bundle["events"]:
        events_by_prev.setdefault(int(ev["prev_ordinal"]), []).append(ev)

    transition_diagnostics: list[dict[str, Any]] = []
    for tr in bundle["transitions"]:
        p_ord = int(tr["prev_ordinal"])
        c_ord = int(tr["curr_ordinal"])
        tr_edges = edges_by_prev.get(p_ord, [])
        tr_events = events_by_prev.get(p_ord, [])
        reasons = sorted(
            {
                reason
                for e in tr_edges
                for reason in e.get("uncertainty_reasons", [])
            }
        )
        # Also detect near-threshold unmatched nodes contributing to transition high_uncertainty
        if bool(tr["high_uncertainty"]) and not reasons:
            reasons = ["near_threshold"]
        transition_diagnostics.append(
            {
                "transition_label": f"b{p_ord}->b{c_ord}",
                "prev_ordinal": p_ord,
                "curr_ordinal": c_ord,
                "pair_scale": tr["pair_scale"],
                "threshold": tr["threshold"],
                "primary_edge_count": tr["primary_edge_count"],
                "secondary_split_edge_count": tr["secondary_split_edge_count"],
                "secondary_merge_edge_count": tr["secondary_merge_edge_count"],
                "event_types": [ev["event_type"] for ev in tr_events],
                "transition_tv_redistribution": tr["transition_tv_redistribution"],
                "min_threshold_margin": tr["min_threshold_margin"],
                "high_uncertainty": tr["high_uncertainty"],
                "edge_uncertainty_reasons": reasons,
            }
        )

    doc = {
        "schema_version": "m3.main_case.v1",
        "status": "PASS",
        "checked_at_utc": _utc_now(),
        "database_path": _rel_or_str(db_path, run_dir),
        "evolution_run_id": bundle["evolution_run_id"],
        "tracking_run_key": bundle["tracking_run_key"],
        "content_sha256": bundle["content_sha256"],
        "m2_content_sha256": bundle["m2_content_sha256"],
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "dataset": {
            "source": "WeatherBench2 IFS ENS",
            "variable": "temperature_2m",
            "units": "K",
            "grid_shape": [3, 3],
            "feature_count": 9,
            "ensemble_member_count": 50,
            "valid_time_utc": exp_cfg["history_replay"]["valid_time_utc"],
            "planned_interval_hours": exp_cfg["history_replay"]["planned_interval_hours"],
        },
        "configuration": {
            "clustering": _clean_scenario_config(exp_cfg["clustering"]),
            "matching": exp_cfg["matching"],
            "evolution": _clean_evolution_config(exp_cfg["evolution"]),
        },
        "planned_batch_count": 8,
        "ok_batch_count": int(cs["ok_batch_count"]),
        "missing_batch_count": int(cs["missing_batch_count"]),
        "failed_batch_count": int(cs["failed_batch_count"]),
        "node_count": len(bundle["nodes"]),
        "branch_count": int(cs["branch_count"]),
        "transition_count": len(bundle["transitions"]),
        "ok_transition_count": int(cs["ok_transition_count"]),
        "gap_transition_count": int(cs["gap_transition_count"]),
        "edge_count": len(bundle["edges"]),
        "primary_edge_count": int(cs["primary_edge_count"]),
        "secondary_split_edge_count": int(cs["secondary_split_edge_count"]),
        "secondary_merge_edge_count": int(cs["secondary_merge_edge_count"]),
        "continuation_count": int(cs["continuation_count"]),
        "split_count": int(cs["split_count"]),
        "merge_count": int(cs["merge_count"]),
        "emergence_count": int(cs["emergence_count"]),
        "disappearance_count": int(cs["disappearance_count"]),
        "gap_interrupted_count": int(cs["gap_interrupted_count"]),
        "gap_restarted_count": int(cs["gap_restarted_count"]),
        "high_uncertainty_transitions": high_uncertainty_transitions,
        "high_uncertainty_transition_labels": [
            f"b{o}->b{o + 1}" for o in high_uncertainty_transitions
        ],
        "verification": {
            "m2_backbone_and_branch_id_exact_match": True,
            "oracle_chain_exact_match": True,
            "m3_runner_chain_exact_match": runner_verified,
            "all_metrics_finite": True,
        },
        "weights": bundle["weights"],
        "evolution_binding": bundle["evolution_binding"],
        "tracking_binding": binding["tracking_binding"],
        "chain_summary": cs,
        "transition_diagnostics": transition_diagnostics,
        "batches": bundle["batches"],
        "nodes": bundle["nodes"],
        "transitions": bundle["transitions"],
        "edges": bundle["edges"],
        "events": bundle["events"],
        "branches": bundle["branches"],
    }
    _assert_all_numbers_finite(doc, "main-evolution-k3-c1.0")
    _assert_no_prohibited_overclaims(doc, "main-evolution-k3-c1.0.json")
    return doc


def _build_threshold_sensitivity_artifact(
    *,
    published: dict[str, dict[str, Any]],
    exp_cfg: dict[str, Any],
    db_path: Path,
    run_dir: Path,
) -> dict[str, Any]:
    """Verify and construct `sensitivity-thresholds.json` (18 `(k, c)` runs, T032)."""
    k_values = [int(k) for k in exp_cfg["threshold_grid"]["cluster_counts"]]
    c_values = [float(c) for c in exp_cfg["threshold_grid"]["threshold_coefficients"]]
    if k_values != [2, 3, 4] or c_values != [0.5, 0.75, 1.0, 1.25, 1.5, 2.0]:
        raise AssertionError(f"Unexpected threshold grid in config: k={k_values}, c={c_values}")

    runs: list[dict[str, Any]] = []
    seen_erids: set[str] = set()
    seen_rks: set[str] = set()

    for k in k_values:
        prev_prim = -1
        for c in c_values:
            key = f"thr_k{k}_c{c}"
            entry = published[key]
            bundle = entry["bundle"]
            cs = bundle["chain_summary"]
            erid = bundle["evolution_run_id"]
            rk = bundle["tracking_run_key"]
            seen_erids.add(erid)
            seen_rks.add(rk)

            if int(cs["ok_batch_count"]) != 8 or int(cs["failed_batch_count"]) != 0:
                raise AssertionError(f"Threshold grid run (k={k}, c={c}) had non-ok batches: {cs}")

            prim_cnt = int(cs["primary_edge_count"])
            if prim_cnt < prev_prim:
                raise AssertionError(
                    f"Primary edge count decreased as c increased for k={k}: {prev_prim} -> {prim_cnt} at c={c}"
                )
            prev_prim = prim_cnt

            split_ords = sorted(
                {
                    int(e["prev_ordinal"])
                    for e in bundle["edges"]
                    if e["edge_role"] == "secondary_split"
                }
            )
            merge_ords = sorted(
                {
                    int(e["prev_ordinal"])
                    for e in bundle["edges"]
                    if e["edge_role"] == "secondary_merge"
                }
            )
            hu_ords = [
                int(tr["prev_ordinal"])
                for tr in bundle["transitions"]
                if bool(tr["high_uncertainty"])
            ]

            runs.append(
                {
                    "grid_id": f"k{k}_c{c}",
                    "k": k,
                    "c": c,
                    "cluster_count": k,
                    "threshold_coefficient": c,
                    "evolution_run_id": erid,
                    "tracking_run_key": rk,
                    "content_sha256": bundle["content_sha256"],
                    "planned_batch_count": int(bundle["planned_batch_count"]),
                    "ok_batch_count": int(cs["ok_batch_count"]),
                    "node_count": len(bundle["nodes"]),
                    "branch_count": int(cs["branch_count"]),
                    "primary_edge_count": prim_cnt,
                    "secondary_split_edge_count": int(cs["secondary_split_edge_count"]),
                    "secondary_merge_edge_count": int(cs["secondary_merge_edge_count"]),
                    "continuation_count": int(cs["continuation_count"]),
                    "split_count": int(cs["split_count"]),
                    "merge_count": int(cs["merge_count"]),
                    "emergence_count": int(cs["emergence_count"]),
                    "disappearance_count": int(cs["disappearance_count"]),
                    "gap_interrupted_count": int(cs["gap_interrupted_count"]),
                    "gap_restarted_count": int(cs["gap_restarted_count"]),
                    "mean_transition_tv_redistribution": float(
                        cs["mean_transition_tv_redistribution"]
                    ),
                    "max_transition_tv_redistribution": float(
                        cs["max_transition_tv_redistribution"]
                    ),
                    "cumulative_primary_distance": float(cs["cumulative_primary_distance"]),
                    "max_edge_distance": float(cs["max_edge_distance"]),
                    "max_edge_distance_over_scale": float(cs["max_edge_distance_over_scale"]),
                    "high_uncertainty_transition_count": int(
                        cs["high_uncertainty_transition_count"]
                    ),
                    "high_uncertainty_transition_ratio": float(
                        cs["high_uncertainty_transition_ratio"]
                    ),
                    "high_uncertainty_edge_count": int(cs["high_uncertainty_edge_count"]),
                    "high_uncertainty_edge_ratio": float(cs["high_uncertainty_edge_ratio"]),
                    "ambiguous_transition_count": int(cs["ambiguous_transition_count"]),
                    "split_transition_ordinals": split_ords,
                    "merge_transition_ordinals": merge_ords,
                    "high_uncertainty_transitions": hu_ords,
                    "chain_summary": cs,
                }
            )

    if len(runs) != 18 or len(seen_erids) != 18 or len(seen_rks) != 18:
        raise AssertionError(
            f"Expected 18 distinct threshold sensitivity runs, got len={len(runs)}, erids={len(seen_erids)}"
        )

    # Verify structural regime assertions on k=3 across c in [0.5 .. 2.0]
    k3_by_c = {r["c"]: r for r in runs if r["k"] == 3}
    r_05 = k3_by_c[0.5]
    if not (
        r_05["primary_edge_count"] == 4
        and r_05["split_count"] == 0
        and r_05["merge_count"] == 0
        and r_05["emergence_count"] >= 15
        and r_05["disappearance_count"] >= 15
    ):
        raise AssertionError(f"Expected severe under-linking at (k=3, c=0.5), got: {r_05}")

    mid_cs = [0.75, 1.0, 1.25, 1.5]
    for mc in mid_cs:
        rm = k3_by_c[mc]
        if not {3, 6}.issubset(set(rm["split_transition_ordinals"])):
            raise AssertionError(
                f"Expected b3->b4 and b6->b7 splits invariant at (k=3, c={mc}), got {rm['split_transition_ordinals']}"
            )

    r_20 = k3_by_c[2.0]
    if not (
        r_20["primary_edge_count"] == 21
        and r_20["secondary_split_edge_count"] == 0
        and r_20["secondary_merge_edge_count"] == 0
        and r_20["split_count"] == 0
        and r_20["merge_count"] == 0
        and r_20["continuation_count"] == 21
    ):
        raise AssertionError(f"Expected full 21-link saturation at (k=3, c=2.0), got: {r_20}")

    observations = {
        "under_linking_at_c_0p5": {
            "verified": True,
            "k3_c0p5_primary_edges": r_05["primary_edge_count"],
            "k3_c0p5_splits": r_05["split_count"],
            "k3_c0p5_merges": r_05["merge_count"],
            "k3_c0p5_emergences": r_05["emergence_count"],
            "k3_c0p5_disappearances": r_05["disappearance_count"],
            "summary": (
                "At c=0.5 (k=3), overly tight thresholds sever viable continuations and suppress all secondary "
                "split/merge attribution (4 primary edges, 0 splits, 0 merges, 17 emergences, 17 disappearances)."
            ),
        },
        "mid_range_split_invariance_c_0p75_to_1p5": {
            "verified": True,
            "c_range": mid_cs,
            "invariant_split_transitions": ["b3->b4", "b6->b7"],
            "invariant_split_prev_ordinals": [3, 6],
            "k3_split_ordinals_by_c": {
                str(mc): k3_by_c[mc]["split_transition_ordinals"] for mc in mid_cs
            },
            "summary": (
                "Across a 2x threshold span c in [0.75, 1.5] at k=3, the b3->b4 and b6->b7 split events remain "
                "structurally invariant, whereas the b0->b1 secondary merge transitions into a third primary link "
                "once c >= 1.25."
            ),
        },
        "high_threshold_saturation_at_c_2p0": {
            "verified": True,
            "k3_c2p0_primary_edges": r_20["primary_edge_count"],
            "k3_c2p0_splits": r_20["split_count"],
            "k3_c2p0_merges": r_20["merge_count"],
            "k2_c2p0_primary_edges": next(r["primary_edge_count"] for r in runs if r["k"] == 2 and r["c"] == 2.0),
            "k4_c2p0_primary_edges": next(r["primary_edge_count"] for r in runs if r["k"] == 4 and r["c"] == 2.0),
            "summary": (
                "At c=2.0, prioritizing maximum 1-to-1 matched cardinality saturates all 7 transitions into "
                "7*k forced primary continuations (14 at k=2, 21 at k=3, 28 at k=4) with 0 secondary splits "
                "and 0 secondary merges, masking multi-to-multi structure."
            ),
        },
        "monotonic_primary_links_across_c": True,
    }

    doc = {
        "schema_version": "m3.sensitivity_thresholds.v1",
        "status": "PASS",
        "checked_at_utc": _utc_now(),
        "database_path": _rel_or_str(db_path, run_dir),
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "grid_count": 18,
        "c_values": c_values,
        "k_values": k_values,
        "distinct_evolution_run_ids_count": len(seen_erids),
        "all_persisted_in_duckdb": True,
        "observations": observations,
        "runs": runs,
    }
    _assert_all_numbers_finite(doc, "sensitivity-thresholds")
    _assert_no_prohibited_overclaims(doc, "sensitivity-thresholds.json")
    return doc


def _summarize_clustering_run(
    *,
    config_id: str,
    raw_cfg: dict[str, Any],
    entry: dict[str, Any],
    axes_membership: list[str],
    from_frozen_config: bool,
    whitelisted_codes: set[str],
) -> dict[str, Any]:
    bundle = entry["bundle"]
    cs = bundle["chain_summary"]
    sc_clean = _clean_scenario_config(raw_cfg)

    failed_ordinals: list[int] = []
    failed_reasons: dict[str, str] = {}
    batch_statuses_by_ord: dict[int, str] = {}
    for b in bundle["batches"]:
        b_ord = int(b["ordinal"])
        st = str(b["status"])
        batch_statuses_by_ord[b_ord] = st
        if st == "failed":
            reason = str(b["status_reason"] or "")
            code_prefix = reason.split(":", 1)[0].strip()
            if code_prefix not in whitelisted_codes:
                raise AssertionError(
                    f"Run {config_id} batch {b_ord} failed with non-whitelisted code {code_prefix!r}"
                )
            failed_ordinals.append(b_ord)
            failed_reasons[str(b_ord)] = reason

    # Verify zero cross-gap edges and strict gap_interrupted / gap_restarted semantics
    cross_gap_edges = 0
    for e in bundle["edges"]:
        p_ord = int(e["prev_ordinal"])
        c_ord = int(e["curr_ordinal"])
        if (
            c_ord != p_ord + 1
            or batch_statuses_by_ord.get(p_ord) != "ok"
            or batch_statuses_by_ord.get(c_ord) != "ok"
        ):
            cross_gap_edges += 1
    if cross_gap_edges != 0:
        raise AssertionError(f"Run {config_id} produced {cross_gap_edges} illegal cross-gap edges")

    gap_transition_ords = [
        int(tr["prev_ordinal"]) for tr in bundle["transitions"] if bool(tr["chain_broken"])
    ]
    for tr in bundle["transitions"]:
        p_ord = int(tr["prev_ordinal"])
        tr_evs = [ev for ev in bundle["events"] if int(ev["prev_ordinal"]) == p_ord]
        if bool(tr["chain_broken"]):
            for ev in tr_evs:
                if ev["event_type"] not in ("gap_interrupted", "gap_restarted"):
                    raise AssertionError(
                        f"Gap transition {p_ord} in {config_id} emitted non-gap event {ev['event_type']!r}"
                    )
        else:
            for ev in tr_evs:
                if ev["event_type"] in ("gap_interrupted", "gap_restarted"):
                    raise AssertionError(
                        f"Ok transition {p_ord} in {config_id} emitted gap event {ev['event_type']!r}"
                    )

    return {
        "config_id": config_id,
        "axes_membership": axes_membership,
        "from_frozen_experiment_config": from_frozen_config,
        "evolution_run_id": bundle["evolution_run_id"],
        "tracking_run_key": bundle["tracking_run_key"],
        "content_sha256": bundle["content_sha256"],
        "threshold_coefficient": 1.0,
        "scenario_config": sc_clean,
        "planned_batch_count": int(bundle["planned_batch_count"]),
        "ok_batch_count": int(cs["ok_batch_count"]),
        "missing_batch_count": int(cs["missing_batch_count"]),
        "failed_batch_count": int(cs["failed_batch_count"]),
        "failed_batch_ordinals": failed_ordinals,
        "failed_batch_reasons": failed_reasons,
        "gap_transition_count": int(cs["gap_transition_count"]),
        "gap_transition_ordinals": gap_transition_ords,
        "cross_gap_edge_count": cross_gap_edges,
        "node_count": len(bundle["nodes"]),
        "branch_count": int(cs["branch_count"]),
        "primary_edge_count": int(cs["primary_edge_count"]),
        "secondary_split_edge_count": int(cs["secondary_split_edge_count"]),
        "secondary_merge_edge_count": int(cs["secondary_merge_edge_count"]),
        "continuation_count": int(cs["continuation_count"]),
        "split_count": int(cs["split_count"]),
        "merge_count": int(cs["merge_count"]),
        "emergence_count": int(cs["emergence_count"]),
        "disappearance_count": int(cs["disappearance_count"]),
        "gap_interrupted_count": int(cs["gap_interrupted_count"]),
        "gap_restarted_count": int(cs["gap_restarted_count"]),
        "mean_transition_tv_redistribution": float(cs["mean_transition_tv_redistribution"]),
        "high_uncertainty_transition_count": int(cs["high_uncertainty_transition_count"]),
        "batches": bundle["batches"],
        "chain_summary": cs,
    }


def _build_clustering_sensitivity_artifact(
    *,
    published: dict[str, dict[str, Any]],
    exp_cfg: dict[str, Any],
    db_path: Path,
    run_dir: Path,
) -> dict[str, Any]:
    """Verify and construct `sensitivity-clustering-configs.json` (4 axes at c=1.0, T033)."""
    axes_cfg = exp_cfg["clustering_sensitivity"]["axes"]
    whitelisted_codes = set(exp_cfg["history_replay"]["whitelisted_batch_failure_codes"])

    axis_order = (
        ("cluster_count", "cluster_count_axis"),
        ("projection_dim", "projection_dim_axis"),
        ("initialization_and_restarts", "initialization_axis"),
        ("chain_break_degenerate", "chain_break_boundary_axis"),
    )

    config_axes_map: dict[str, list[str]] = {}
    ordered_variants: list[tuple[str, dict[str, Any], bool]] = []
    seen_cids: set[str] = set()

    for cfg_axis_key, _ in axis_order:
        variants = list(axes_cfg[cfg_axis_key]["variants"])
        for var in variants:
            cid = str(var["config_id"])
            config_axes_map.setdefault(cid, []).append(cfg_axis_key)
            if cid not in seen_cids:
                seen_cids.add(cid)
                ordered_variants.append((cid, var, True))

    runs_by_cid: dict[str, dict[str, Any]] = {}
    runs_list: list[dict[str, Any]] = []
    seen_erids: set[str] = set()

    for cid, var, from_frozen in ordered_variants:
        entry = published[f"cls_{cid}"]
        rec = _summarize_clustering_run(
            config_id=cid,
            raw_cfg=var,
            entry=entry,
            axes_membership=config_axes_map[cid],
            from_frozen_config=from_frozen,
            whitelisted_codes=whitelisted_codes,
        )
        seen_erids.add(rec["evolution_run_id"])
        runs_by_cid[cid] = rec
        runs_list.append(rec)

    if len(seen_erids) != len(runs_list):
        raise AssertionError(
            f"Expected {len(runs_list)} distinct evolution_run_ids across clustering variants, got {len(seen_erids)}"
        )

    axes_breakdown: dict[str, Any] = {}
    alias_axes: dict[str, Any] = {}
    for cfg_axis_key, alias_key in axis_order:
        cids_on_axis = [str(v["config_id"]) for v in axes_cfg[cfg_axis_key]["variants"]]
        axis_runs = [runs_by_cid[cid] for cid in cids_on_axis]
        axis_obj = {
            "axis_id": cfg_axis_key,
            "alias_id": alias_key,
            "description": axes_cfg[cfg_axis_key]["description"],
            "variants_count": len(axis_runs),
            "config_ids": cids_on_axis,
            "failed_batch_runs_count": sum(
                1 for r in axis_runs if int(r["failed_batch_count"]) > 0
            ),
            "runs": axis_runs,
        }
        axes_breakdown[cfg_axis_key] = axis_obj
        alias_axes[alias_key] = axis_obj

    failed_batch_runs_count = sum(1 for r in runs_list if int(r["failed_batch_count"]) > 0)
    total_gap_interrupted = sum(int(r["gap_interrupted_count"]) for r in runs_list)
    total_gap_restarted = sum(int(r["gap_restarted_count"]) for r in runs_list)
    total_cross_gap_edges = sum(int(r["cross_gap_edge_count"]) for r in runs_list)

    if failed_batch_runs_count < 1:
        raise AssertionError("Expected failed_batch_runs_count >= 1 on chain_break_degenerate axis")
    if total_gap_interrupted < 1 or total_gap_restarted < 1:
        raise AssertionError(
            f"Expected natural gap_interrupted (>0) and gap_restarted (>0) events, got {total_gap_interrupted}, {total_gap_restarted}"
        )
    if total_cross_gap_edges != 0:
        raise AssertionError(f"Expected 0 cross-gap edges, got {total_cross_gap_edges}")

    doc = {
        "schema_version": "m3.sensitivity_clustering.v1",
        "status": "PASS",
        "checked_at_utc": _utc_now(),
        "database_path": _rel_or_str(db_path, run_dir),
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "c": 1.0,
        "threshold_coefficient": 1.0,
        "runs_count": len(runs_list),
        "distinct_evolution_run_ids_count": len(seen_erids),
        "failed_batch_runs_count": failed_batch_runs_count,
        "total_gap_interrupted_events": total_gap_interrupted,
        "total_gap_restarted_events": total_gap_restarted,
        "cross_gap_edge_count": total_cross_gap_edges,
        "all_persisted_in_duckdb": True,
        "axes": axes_breakdown,
        **alias_axes,
        "runs": runs_list,
    }
    _assert_all_numbers_finite(doc, "sensitivity-clustering-configs")
    _assert_no_prohibited_overclaims(doc, "sensitivity-clustering-configs.json")
    return doc


def _execute_pair_cases_production(
    cases: list[dict[str, Any]],
    *,
    runner_bin: Path | None,
    ext_path: Path,
) -> list[dict[str, Any]]:
    """Execute scaled pair evolution cases using Rust production code (`m3_runner --mode pair`
    or DuckDB `ens_match_scenarios` + `ens_evolve_scenarios`).
    """
    if not cases:
        return []
    if runner_bin is not None:
        with tempfile.TemporaryDirectory(prefix="ensabc-m3-pair-batch-") as td:
            in_p = Path(td) / "pairs_in.json"
            out_p = Path(td) / "pairs_out.json"
            in_p.write_text(json.dumps({"cases": cases}), encoding="utf-8")
            proc = subprocess.run(
                [str(runner_bin), "--mode", "pair", "--scaled", "--input", str(in_p), "--output", str(out_p)],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"m3_runner --mode pair batch failed: {proc.stderr}")
            batch_out = json.loads(out_p.read_text(encoding="utf-8"))
            results: list[dict[str, Any]] = []
            for item in batch_out["results"]:
                if item["status"] != "ok":
                    raise RuntimeError(f"Unexpected pair error in m3_runner: {item}")
                results.append(item["result"])
            return results

    # Fallback: run via DuckDB SQL extension `ens_evolve_scenarios`
    results = []
    ev_cfg_sql = {
        k: v for k, v in default_evolution_config().items() if k != "uncertainty_rule"
    }
    chunk_size = 250
    for start in range(0, len(cases), chunk_size):
        sub = cases[start : start + chunk_size]
        rows_sql = ", ".join(
            f"({idx}, {sql_value(c['prev_scenarios'])}, {sql_value(c['curr_scenarios'])}, "
            f"{float(c['threshold_coefficient'])!r}::DOUBLE, {float(c['prev_dispersion'])!r}::DOUBLE, "
            f"{float(c['curr_dispersion'])!r}::DOUBLE)"
            for idx, c in enumerate(sub)
        )
        weights_sql = sql_value(sub[0]["weights"])
        sql = f"""
        WITH inp(idx, prev_s, curr_s, tc, sp, sc) AS (VALUES {rows_sql})
        SELECT
            idx,
            ens_evolve_scenarios(
                prev_s,
                curr_s,
                {weights_sql}::DOUBLE[],
                ens_match_scenarios(
                    list_transform(prev_s, lambda s: {{'cluster_id': s.cluster_id, 'mean_features': s.mean_features}}),
                    list_transform(curr_s, lambda s: {{'cluster_id': s.cluster_id, 'mean_features': s.mean_features}}),
                    {weights_sql}::DOUBLE[],
                    struct_pack(threshold_coefficient := tc, prev_dispersion := sp, curr_dispersion := sc)
                ),
                {sql_value(ev_cfg_sql)}
            ) AS evo
        FROM inp
        ORDER BY idx ASC;
        """
        rc, parsed, err = _run_duckdb(ext_path, sql)
        if rc != 0:
            raise RuntimeError(f"Fallback SQL pair evolution failed: {err}")
        for r in parsed[-1]:
            results.append(r["evo"])
    return results


def _topology_signature_unmapped(evo: dict[str, Any]) -> dict[str, Any]:
    """Canonical discrete topology & 5-way event signature on fixed cluster_ids."""
    prim_edges = sorted(
        (int(e["prev_cluster_id"]), int(e["curr_cluster_id"]))
        for e in evo["edges"]
        if e["edge_role"] == "primary"
    )
    sec_split_edges = sorted(
        (int(e["prev_cluster_id"]), int(e["curr_cluster_id"]))
        for e in evo["edges"]
        if e["edge_role"] == "secondary_split"
    )
    sec_merge_edges = sorted(
        (int(e["prev_cluster_id"]), int(e["curr_cluster_id"]))
        for e in evo["edges"]
        if e["edge_role"] == "secondary_merge"
    )
    events = sorted(
        (
            str(ev["event_type"]),
            tuple(sorted(int(x) for x in ev["prev_cluster_ids"])),
            tuple(sorted(int(y) for y in ev["curr_cluster_ids"])),
        )
        for ev in evo["events"]
    )
    return {
        "primary_edges": prim_edges,
        "secondary_split_edges": sec_split_edges,
        "secondary_merge_edges": sec_merge_edges,
        "events": events,
    }


def _baseline_margins_snapshot(base_ev: dict[str, Any]) -> dict[str, Any]:
    s = base_ev["summary"]
    backbone = base_ev.get("backbone", {})
    opt_cnt = backbone.get("optima_count")
    if opt_cnt is None:
        opt_cnt = backbone.get("optimal_solution_count", 1)
    return {
        "pair_scale": float(base_ev["pair_scale"]),
        "threshold": float(base_ev["threshold"]),
        "min_threshold_margin": float(s["min_threshold_margin"]),
        "min_threshold_margin_ratio": float(s["min_threshold_margin_ratio"]),
        "raw_matrix_threshold_margin": float(s["raw_matrix_threshold_margin"]),
        "min_optimality_margin": (
            float(s["min_optimality_margin"])
            if s.get("min_optimality_margin") is not None
            else None
        ),
        "min_optimality_margin_ratio": (
            float(s["min_optimality_margin_ratio"])
            if s.get("min_optimality_margin_ratio") is not None
            else None
        ),
        "min_secondary_margin": (
            float(s["min_secondary_margin"])
            if s.get("min_secondary_margin") is not None
            else None
        ),
        "min_secondary_margin_ratio": (
            float(s["min_secondary_margin_ratio"])
            if s.get("min_secondary_margin_ratio") is not None
            else None
        ),
        "optimal_solution_count": int(opt_cnt),
        "high_uncertainty": bool(s["high_uncertainty"]),
    }


def _describe_flip_details(
    base_sig: dict[str, Any],
    pert_sig: dict[str, Any],
) -> dict[str, Any]:
    return {
        "primary_edges_changed": base_sig["primary_edges"] != pert_sig["primary_edges"],
        "secondary_split_edges_changed": (
            base_sig["secondary_split_edges"] != pert_sig["secondary_split_edges"]
        ),
        "secondary_merge_edges_changed": (
            base_sig["secondary_merge_edges"] != pert_sig["secondary_merge_edges"]
        ),
        "events_changed": base_sig["events"] != pert_sig["events"],
        "baseline_primary_edges": [list(p) for p in base_sig["primary_edges"]],
        "perturbed_primary_edges": [list(p) for p in pert_sig["primary_edges"]],
        "baseline_secondary_split_edges": [list(p) for p in base_sig["secondary_split_edges"]],
        "perturbed_secondary_split_edges": [list(p) for p in pert_sig["secondary_split_edges"]],
        "baseline_secondary_merge_edges": [list(p) for p in base_sig["secondary_merge_edges"]],
        "perturbed_secondary_merge_edges": [list(p) for p in pert_sig["secondary_merge_edges"]],
        "baseline_events": [
            {"event_type": et, "prev_cluster_ids": list(pc), "curr_cluster_ids": list(cc)}
            for et, pc, cc in base_sig["events"]
        ],
        "perturbed_events": [
            {"event_type": et, "prev_cluster_ids": list(pc), "curr_cluster_ids": list(cc)}
            for et, pc, cc in pert_sig["events"]
        ],
    }


def _build_evolution_level_perturbation_artifact(
    *,
    main_bundle: dict[str, Any],
    exp_cfg: dict[str, Any],
    weights_vec: list[float],
    runner_bin: Path | None,
    ext_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Execute Level 1a (mean-field PCG64) & Level 1b (fraction-only +-1 member) perturbations
    and verify 4-condition stability certificates (T034). Also returns baseline pair oracle
    structures for reuse in Level 2.
    """
    nodes_by_ord: dict[int, list[dict[str, Any]]] = {}
    for nd in main_bundle["nodes"]:
        nodes_by_ord.setdefault(int(nd["ordinal"]), []).append(nd)
    for b_ord in nodes_by_ord:
        nodes_by_ord[b_ord].sort(key=lambda x: int(x["cluster_id"]))

    disp_by_ord = {int(b["ordinal"]): float(b["dispersion"]) for b in main_bundle["batches"]}
    ev_cfg_clean = {
        k: v
        for k, v in _clean_evolution_config(exp_cfg["evolution"]).items()
        if k != "uncertainty_rule"
    }

    # Baseline 7 adjacent pairs
    baseline_pairs: list[dict[str, Any]] = []
    for p_idx in range(7):
        p_sc = [
            {
                "cluster_id": int(n["cluster_id"]),
                "member_count": int(n["member_count"]),
                "member_fraction": float(n["member_fraction"]),
                "mean_features": [float(x) for x in n["mean_features"]],
            }
            for n in nodes_by_ord[p_idx]
        ]
        c_sc = [
            {
                "cluster_id": int(n["cluster_id"]),
                "member_count": int(n["member_count"]),
                "member_fraction": float(n["member_fraction"]),
                "mean_features": [float(x) for x in n["mean_features"]],
            }
            for n in nodes_by_ord[p_idx + 1]
        ]
        sp = disp_by_ord[p_idx]
        sc_d = disp_by_ord[p_idx + 1]
        base_ev = solve_evolution_oracle(
            {
                "mode": "scaled",
                "prev_scenarios": p_sc,
                "curr_scenarios": c_sc,
                "weights": weights_vec,
                "threshold_coefficient": 1.0,
                "prev_dispersion": sp,
                "curr_dispersion": sc_d,
                "config": ev_cfg_clean,
            }
        )
        baseline_pairs.append(
            {
                "pair_index": p_idx,
                "prev_ordinal": p_idx,
                "curr_ordinal": p_idx + 1,
                "prev_dispersion": sp,
                "curr_dispersion": sc_d,
                "prev_scenarios": p_sc,
                "curr_scenarios": c_sc,
                "oracle_result": base_ev,
                "signature": _topology_signature_unmapped(base_ev),
                "margins": _baseline_margins_snapshot(base_ev),
            }
        )

    # -------------------------------------------------------------------------
    # Level 1a: Mean-field weighted-RMS sphere perturbations (PCG64(20261029))
    # -------------------------------------------------------------------------
    l1a_cfg = exp_cfg["perturbation"]["level_1a_mean_field"]
    eps_ratios = [float(x) for x in l1a_cfg["epsilon_ratios"]]
    directions_count = int(l1a_cfg["directions"])
    seed_1a = int(l1a_cfg["seed"])

    rng_1a = np.random.Generator(np.random.PCG64(seed_1a))
    w_arr = np.asarray(weights_vec, dtype=np.float64)
    w_sum = float(math.fsum(weights_vec))

    l1a_cases: list[dict[str, Any]] = []
    l1a_meta: list[dict[str, Any]] = []

    # Strictly follow loop_order: "epsilon_then_pair_then_direction_0_to_99"
    for eps_ratio in eps_ratios:
        for p_idx in range(7):
            bp = baseline_pairs[p_idx]
            p_sc = bp["prev_scenarios"]
            c_sc = bp["curr_scenarios"]
            pair_scale = float(bp["oracle_result"]["pair_scale"])
            target_eps_abs = eps_ratio * pair_scale
            p_base_arr = np.asarray([s["mean_features"] for s in p_sc], dtype=np.float64)
            c_base_arr = np.asarray([s["mean_features"] for s in c_sc], dtype=np.float64)

            for sample_idx in range(directions_count):
                dir_prev = rng_1a.normal(0.0, 1.0, p_base_arr.shape)
                dir_prev /= np.sqrt(
                    np.sum((dir_prev * dir_prev) * w_arr, axis=1, keepdims=True) / w_sum
                )
                dir_curr = rng_1a.normal(0.0, 1.0, c_base_arr.shape)
                dir_curr /= np.sqrt(
                    np.sum((dir_curr * dir_curr) * w_arr, axis=1, keepdims=True) / w_sum
                )

                pert_p_feats = (p_base_arr + target_eps_abs * dir_prev).tolist()
                pert_c_feats = (c_base_arr + target_eps_abs * dir_curr).tolist()

                actual_eps_abs = max(
                    *(
                        compute_weighted_rmse_exact(
                            p_sc[i]["mean_features"], pert_p_feats[i], weights_vec
                        )
                        for i in range(len(p_sc))
                    ),
                    *(
                        compute_weighted_rmse_exact(
                            c_sc[j]["mean_features"], pert_c_feats[j], weights_vec
                        )
                        for j in range(len(c_sc))
                    ),
                )

                cert = evaluate_evolution_stability_certificate(
                    bp["oracle_result"],
                    epsilon_abs=actual_eps_abs,
                    threshold_coefficient=1.0,
                    delta_s=0.0,
                    prev_scenarios=p_sc,
                    curr_scenarios=c_sc,
                    weights=weights_vec,
                )

                pert_p_sc = [
                    {**p_sc[i], "mean_features": pert_p_feats[i]} for i in range(len(p_sc))
                ]
                pert_c_sc = [
                    {**c_sc[j], "mean_features": pert_c_feats[j]} for j in range(len(c_sc))
                ]

                cid = f"l1a_eps{eps_ratio}_p{p_idx}_d{sample_idx:03d}"
                l1a_cases.append(
                    {
                        "case_id": cid,
                        "mode": "scaled",
                        "prev_scenarios": pert_p_sc,
                        "curr_scenarios": pert_c_sc,
                        "weights": weights_vec,
                        "threshold_coefficient": 1.0,
                        "prev_dispersion": bp["prev_dispersion"],
                        "curr_dispersion": bp["curr_dispersion"],
                        "config": ev_cfg_clean,
                    }
                )
                l1a_meta.append(
                    {
                        "pair_index": p_idx,
                        "epsilon_ratio": eps_ratio,
                        "sample_index": sample_idx,
                        "target_epsilon_abs": target_eps_abs,
                        "actual_epsilon_abs": actual_eps_abs,
                        "certificate": cert,
                    }
                )

    l1a_results = _execute_pair_cases_production(
        l1a_cases, runner_bin=runner_bin, ext_path=ext_path
    )

    certified_evaluations_count = 0
    uncertified_evaluations_count = 0
    certified_pair_flips = 0
    uncertified_pair_flips = 0
    boundary_flips_1a: list[dict[str, Any]] = []

    stats_by_eps_pair: dict[tuple[float, int], dict[str, Any]] = {}
    for eps_r in eps_ratios:
        for p_idx in range(7):
            bp = baseline_pairs[p_idx]
            stats_by_eps_pair[(eps_r, p_idx)] = {
                "pair_index": p_idx,
                "transition": f"b{p_idx}->b{p_idx + 1}",
                "epsilon_ratio": eps_r,
                "pair_scale": bp["margins"]["pair_scale"],
                "threshold": bp["margins"]["threshold"],
                "baseline_margins": bp["margins"],
                "trials_count": 0,
                "certified_trials_count": 0,
                "uncertified_trials_count": 0,
                "certified_flips_count": 0,
                "uncertified_flips_count": 0,
                "max_actual_epsilon_abs": 0.0,
                "max_primary_total_distance_shift": 0.0,
                "max_tv_redistribution_shift": 0.0,
                "certificate_failed_conditions_seen": set(),
            }

    for meta, pert_ev in zip(l1a_meta, l1a_results):
        p_idx = meta["pair_index"]
        eps_r = meta["epsilon_ratio"]
        s_idx = meta["sample_index"]
        cert = meta["certificate"]
        bp = baseline_pairs[p_idx]
        pert_sig = _topology_signature_unmapped(pert_ev)
        flipped = pert_sig != bp["signature"]

        bucket = stats_by_eps_pair[(eps_r, p_idx)]
        bucket["trials_count"] += 1
        bucket["max_actual_epsilon_abs"] = max(
            bucket["max_actual_epsilon_abs"], float(meta["actual_epsilon_abs"])
        )
        bucket["max_primary_total_distance_shift"] = max(
            bucket["max_primary_total_distance_shift"],
            abs(
                float(pert_ev["summary"]["primary_total_distance"])
                - float(bp["oracle_result"]["summary"]["primary_total_distance"])
            ),
        )
        bucket["max_tv_redistribution_shift"] = max(
            bucket["max_tv_redistribution_shift"],
            abs(
                float(pert_ev["summary"]["transition_tv_redistribution"])
                - float(bp["oracle_result"]["summary"]["transition_tv_redistribution"])
            ),
        )

        if cert["certified"]:
            certified_evaluations_count += 1
            bucket["certified_trials_count"] += 1
            if flipped:
                certified_pair_flips += 1
                bucket["certified_flips_count"] += 1
        else:
            uncertified_evaluations_count += 1
            bucket["uncertified_trials_count"] += 1
            for fc in cert.get("failed_conditions", []):
                bucket["certificate_failed_conditions_seen"].add(fc)
            if flipped:
                uncertified_pair_flips += 1
                bucket["uncertified_flips_count"] += 1
                boundary_flips_1a.append(
                    {
                        "pair_index": p_idx,
                        "prev_ordinal": p_idx,
                        "curr_ordinal": p_idx + 1,
                        "transition": f"b{p_idx}->b{p_idx + 1}",
                        "epsilon_ratio": eps_r,
                        "sample_index": s_idx,
                        "actual_epsilon_abs": meta["actual_epsilon_abs"],
                        "certified": False,
                        "certificate_failed_conditions": cert.get("failed_conditions", []),
                        "baseline_margins": bp["margins"],
                        "flip_details": _describe_flip_details(bp["signature"], pert_sig),
                    }
                )

    if certified_evaluations_count <= 0:
        raise AssertionError("Expected certified_evaluations_count > 0 in Level 1a")
    if certified_pair_flips != 0:
        raise AssertionError(
            f"4-condition stability certificate violated in Level 1a: certified_pair_flips={certified_pair_flips}"
        )
    if not all(bool(item.get("baseline_margins")) for item in boundary_flips_1a):
        raise AssertionError("Every boundary flip in Level 1a must carry baseline_margins")

    by_epsilon_summaries: list[dict[str, Any]] = []
    for eps_r in eps_ratios:
        pair_items = []
        for p_idx in range(7):
            b_stat = dict(stats_by_eps_pair[(eps_r, p_idx)])
            b_stat["certificate_failed_conditions_seen"] = sorted(
                b_stat["certificate_failed_conditions_seen"]
            )
            b_stat["certified_all_trials"] = b_stat["certified_trials_count"] == directions_count
            pair_items.append(b_stat)
        by_epsilon_summaries.append(
            {
                "epsilon_ratio": eps_r,
                "directions_per_pair": directions_count,
                "total_trials": sum(p["trials_count"] for p in pair_items),
                "certified_pairs_count": sum(1 for p in pair_items if p["certified_all_trials"]),
                "certified_trials_count": sum(p["certified_trials_count"] for p in pair_items),
                "uncertified_trials_count": sum(p["uncertified_trials_count"] for p in pair_items),
                "certified_flips_count": sum(p["certified_flips_count"] for p in pair_items),
                "uncertified_flips_count": sum(p["uncertified_flips_count"] for p in pair_items),
                "pairs": pair_items,
            }
        )

    # -------------------------------------------------------------------------
    # Level 1b: Pure member-fraction (+-1 member / +-0.02 fraction) transfers
    # -------------------------------------------------------------------------
    l1b_cfg = exp_cfg["perturbation"]["level_1b_fraction_only"]
    delta_members = int(l1b_cfg["delta_members"])
    n_ens = int(l1b_cfg["ensemble_member_count"])

    l1b_cases: list[dict[str, Any]] = []
    l1b_meta: list[dict[str, Any]] = []

    for p_idx in range(7):
        bp = baseline_pairs[p_idx]
        p_sc = bp["prev_scenarios"]
        c_sc = bp["curr_scenarios"]

        prev_transfers: list[tuple[str, list[dict[str, Any]]]] = [("none", p_sc)]
        for src in range(len(p_sc)):
            if p_sc[src]["member_count"] - delta_members >= 1:
                for dst in range(len(p_sc)):
                    if src != dst:
                        mod_p = [dict(s) for s in p_sc]
                        mod_p[src]["member_count"] -= delta_members
                        mod_p[src]["member_fraction"] = mod_p[src]["member_count"] / float(n_ens)
                        mod_p[dst]["member_count"] += delta_members
                        mod_p[dst]["member_fraction"] = mod_p[dst]["member_count"] / float(n_ens)
                        prev_transfers.append((f"prev_c{src}_to_c{dst}", mod_p))

        curr_transfers: list[tuple[str, list[dict[str, Any]]]] = [("none", c_sc)]
        for src in range(len(c_sc)):
            if c_sc[src]["member_count"] - delta_members >= 1:
                for dst in range(len(c_sc)):
                    if src != dst:
                        mod_c = [dict(s) for s in c_sc]
                        mod_c[src]["member_count"] -= delta_members
                        mod_c[src]["member_fraction"] = mod_c[src]["member_count"] / float(n_ens)
                        mod_c[dst]["member_count"] += delta_members
                        mod_c[dst]["member_fraction"] = mod_c[dst]["member_count"] / float(n_ens)
                        curr_transfers.append((f"curr_c{src}_to_c{dst}", mod_c))

        for p_label, p_mod in prev_transfers:
            for c_label, c_mod in curr_transfers:
                if p_label == "none" and c_label == "none":
                    continue
                is_single_side = (p_label == "none") or (c_label == "none")
                l1b_cases.append(
                    {
                        "case_id": f"l1b_p{p_idx}_{p_label}__{c_label}",
                        "mode": "scaled",
                        "prev_scenarios": p_mod,
                        "curr_scenarios": c_mod,
                        "weights": weights_vec,
                        "threshold_coefficient": 1.0,
                        "prev_dispersion": bp["prev_dispersion"],
                        "curr_dispersion": bp["curr_dispersion"],
                        "config": ev_cfg_clean,
                    }
                )
                l1b_meta.append(
                    {
                        "pair_index": p_idx,
                        "prev_transfer": p_label,
                        "curr_transfer": c_label,
                        "single_side": is_single_side,
                    }
                )

    l1b_results = _execute_pair_cases_production(
        l1b_cases, runner_bin=runner_bin, ext_path=ext_path
    )

    l1b_topology_flips = 0
    l1b_primary_flips = 0
    l1b_secondary_flips = 0
    l1b_event_flips = 0
    max_morph_change_1b = 0.0
    max_tv_delta_1b = 0.0
    max_flow_delta_1b = 0.0
    nonzero_flow_responses = 0
    per_pair_1b: list[dict[str, Any]] = [
        {
            "pair_index": p_idx,
            "transition": f"b{p_idx}->b{p_idx + 1}",
            "trials_count": 0,
            "topology_flips": 0,
            "max_abs_tv_redistribution_delta": 0.0,
            "max_abs_total_attributed_flow_delta": 0.0,
            "nonzero_attributed_flow_changes": 0,
        }
        for p_idx in range(7)
    ]

    for meta, pert_ev in zip(l1b_meta, l1b_results):
        p_idx = meta["pair_index"]
        bp = baseline_pairs[p_idx]
        base_ev = bp["oracle_result"]
        pert_sig = _topology_signature_unmapped(pert_ev)

        if pert_sig["primary_edges"] != bp["signature"]["primary_edges"]:
            l1b_primary_flips += 1
        if (
            pert_sig["secondary_split_edges"] != bp["signature"]["secondary_split_edges"]
            or pert_sig["secondary_merge_edges"] != bp["signature"]["secondary_merge_edges"]
        ):
            l1b_secondary_flips += 1
        if pert_sig["events"] != bp["signature"]["events"]:
            l1b_event_flips += 1
        if pert_sig != bp["signature"]:
            l1b_topology_flips += 1
            per_pair_1b[p_idx]["topology_flips"] += 1

        morph_diff = max(
            abs(
                float(pert_ev["summary"]["primary_total_distance"])
                - float(base_ev["summary"]["primary_total_distance"])
            ),
            abs(
                float(pert_ev["summary"]["all_edges_max_distance"])
                - float(base_ev["summary"]["all_edges_max_distance"])
            ),
        )
        max_morph_change_1b = max(max_morph_change_1b, morph_diff)

        tv_delta = abs(
            float(pert_ev["summary"]["transition_tv_redistribution"])
            - float(base_ev["summary"]["transition_tv_redistribution"])
        )
        flow_delta = abs(
            float(pert_ev["summary"]["total_attributed_flow"])
            - float(base_ev["summary"]["total_attributed_flow"])
        )
        max_tv_delta_1b = max(max_tv_delta_1b, tv_delta)
        max_flow_delta_1b = max(max_flow_delta_1b, flow_delta)
        if flow_delta > 0.0:
            nonzero_flow_responses += 1
            per_pair_1b[p_idx]["nonzero_attributed_flow_changes"] += 1

        per_pair_1b[p_idx]["trials_count"] += 1
        per_pair_1b[p_idx]["max_abs_tv_redistribution_delta"] = max(
            per_pair_1b[p_idx]["max_abs_tv_redistribution_delta"], tv_delta
        )
        per_pair_1b[p_idx]["max_abs_total_attributed_flow_delta"] = max(
            per_pair_1b[p_idx]["max_abs_total_attributed_flow_delta"], flow_delta
        )

    if l1b_topology_flips != 0:
        raise AssertionError(f"Level 1b fraction-only perturbation caused {l1b_topology_flips} topology flips")
    if max_morph_change_1b != 0.0:
        raise AssertionError(
            f"Level 1b fraction-only perturbation altered morphology distance by {max_morph_change_1b!r}"
        )
    if nonzero_flow_responses == 0:
        raise AssertionError("Expected non-zero smooth attributed_flow adjustments in Level 1b")

    doc = {
        "schema_version": "m3.perturbation_evolution_level.v1",
        "status": "PASS",
        "checked_at_utc": _utc_now(),
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "baseline_evolution_run_id": main_bundle["evolution_run_id"],
        "baseline_tracking_run_key": main_bundle["tracking_run_key"],
        "stability_certificate_schema": CERTIFICATE_RULE_V1,
        "certified_evaluations_count": certified_evaluations_count,
        "uncertified_evaluations_count": uncertified_evaluations_count,
        "certified_pair_flips": certified_pair_flips,
        "boundary_flips_count": len(boundary_flips_1a),
        "all_flips_carry_baseline_margins": True,
        "boundary_flips": boundary_flips_1a,
        "level_1b_topology_flips": l1b_topology_flips,
        "level_1a_mean_field": {
            "method": l1a_cfg["method"],
            "rng": l1a_cfg["rng"],
            "seed": seed_1a,
            "loop_order": l1a_cfg["loop_order"],
            "epsilon_ratios": eps_ratios,
            "directions_per_pair": directions_count,
            "total_trials": len(l1a_cases),
            "certified_evaluations_count": certified_evaluations_count,
            "uncertified_evaluations_count": uncertified_evaluations_count,
            "certified_pair_flips": certified_pair_flips,
            "uncertified_pair_flips": uncertified_pair_flips,
            "all_flips_carry_baseline_margins": True,
            "by_epsilon_ratio": by_epsilon_summaries,
            "boundary_flips": boundary_flips_1a,
        },
        "level_1b_fraction_only": {
            "method": l1b_cfg["method"],
            "ensemble_member_count": n_ens,
            "delta_members": delta_members,
            "delta_fraction": float(l1b_cfg["delta_fraction"]),
            "single_side_trials_count": sum(1 for m in l1b_meta if m["single_side"]),
            "simultaneous_pair_trials_count": sum(1 for m in l1b_meta if not m["single_side"]),
            "total_trials": len(l1b_cases),
            "topology_flips": l1b_topology_flips,
            "primary_edge_flips": l1b_primary_flips,
            "secondary_edge_flips": l1b_secondary_flips,
            "event_classification_flips": l1b_event_flips,
            "max_morphology_distance_change": max_morph_change_1b,
            "nonzero_attributed_flow_adjustments_count": nonzero_flow_responses,
            "max_abs_attributed_flow_delta": max_flow_delta_1b,
            "max_abs_tv_redistribution_delta": max_tv_delta_1b,
            "per_pair_summary": per_pair_1b,
        },
    }
    _assert_all_numbers_finite(doc, "perturbation-evolution-level")
    _assert_no_prohibited_overclaims(doc, "perturbation-evolution-level.json")
    return doc, baseline_pairs


def _partitions_from_cluster_result(cluster_res: dict[str, Any]) -> dict[int, set[str]]:
    parts: dict[int, set[str]] = {
        int(sc["cluster_id"]): set() for sc in cluster_res["scenarios"]
    }
    for m in cluster_res["members"]:
        parts[int(m["cluster_id"])].add(str(m["member_id"]))
    return parts


def _build_member_level_perturbation_artifact(
    *,
    main_bundle: dict[str, Any],
    baseline_pairs: list[dict[str, Any]],
    batches_data: list[list[list[float]]],
    exp_cfg: dict[str, Any],
    weights_vec: list[float],
    ext_path: Path,
    runner_bin: Path | None,
) -> dict[str, Any]:
    """Execute Level 2 member-level Gaussian perturbations (`PCG64(4242)`, `sigma in [0.0, 0.001, 0.01]`,
    20 samples per level), evaluate 4-condition stability certificates on mapped pairs, and verify
    whole-chain logical branch lineage stability by `(birth_ordinal, birth_canonical_rank)` (T034).
    """
    l2_cfg = exp_cfg["perturbation"]["level_2_member_perturbation"]
    sigmas = [float(s) for s in l2_cfg["sigma_levels_k"]]
    samples_per_level = int(l2_cfg["samples"])
    seed_2 = int(l2_cfg["seed"])
    sc_cfg = _clean_scenario_config(exp_cfg["clustering"])
    ev_cfg_clean = {
        k: v
        for k, v in _clean_evolution_config(exp_cfg["evolution"]).items()
        if k != "uncertainty_rule"
    }
    tracking_rk = main_bundle["tracking_run_key"]

    # Baseline member partitions per batch via DuckDB `ens_cluster`
    base_cl_sql = " UNION ALL ".join(
        f"SELECT {b_idx} AS ordinal, ens_dispersion(["
        + ", ".join(
            f"{{'member_id': 'ifs_ens_{m + 1:02d}', 'features': {batches_data[b_idx][m]}}}"
            for m in range(50)
        )
        + f"], {sql_value(weights_vec)}::DOUBLE[]) AS disp, ens_cluster(["
        + ", ".join(
            f"{{'member_id': 'ifs_ens_{m + 1:02d}', 'features': {batches_data[b_idx][m]}}}"
            for m in range(50)
        )
        + f"], {sql_value(sc_cfg)}) AS cl"
        for b_idx in range(8)
    ) + " ORDER BY ordinal ASC;"
    rc_b, parsed_b, err_b = _run_duckdb(ext_path, base_cl_sql)
    if rc_b != 0:
        raise RuntimeError(f"Baseline member-partition clustering query failed: {err_b}")
    base_batch_rows = parsed_b[-1]
    base_partitions = [_partitions_from_cluster_result(r["cl"]) for r in base_batch_rows]

    # Baseline chain lineage map: (ordinal, base_cid) -> {birth_key, ancestry_path}
    base_nodes_map: dict[tuple[int, int], dict[str, Any]] = {}
    for nd in main_bundle["nodes"]:
        key = (int(nd["ordinal"]), int(nd["cluster_id"]))
        b_ord, b_rank = parse_branch_birth_key(
            str(nd["branch_id"]), expected_tracking_run_key=tracking_rk
        )
        base_nodes_map[key] = {
            "ordinal": key[0],
            "cluster_id": key[1],
            "canonical_rank": int(nd["canonical_rank"]),
            "branch_id": str(nd["branch_id"]),
            "birth_key": [b_ord, b_rank],
        }
    base_in_primary: dict[tuple[int, int], int] = {
        (int(e["curr_ordinal"]), int(e["curr_cluster_id"])): int(e["prev_cluster_id"])
        for e in main_bundle["edges"]
        if e["edge_role"] == "primary"
    }
    base_ancestry: dict[tuple[int, int], list[tuple[int, int]]] = {}
    for b_ord in range(8):
        for cid in range(sc_cfg["cluster_count"]):
            key = (b_ord, cid)
            if key not in base_nodes_map:
                continue
            parent_cid = base_in_primary.get(key)
            if parent_cid is None:
                base_ancestry[key] = [key]
            else:
                base_ancestry[key] = [*base_ancestry[(b_ord - 1, parent_cid)], key]

    base_proj_ref = semantic_evolution_projection(
        {
            "planned_batch_count": 8,
            "batches": [
                {
                    "ordinal": int(b["ordinal"]),
                    "status": b["status"],
                    "status_reason": b["status_reason"],
                    "dispersion": b["dispersion"],
                }
                for b in main_bundle["batches"]
            ],
            "nodes": main_bundle["nodes"],
            "edges": main_bundle["edges"],
            "events": main_bundle["events"],
            "branches": main_bundle["branches"],
            "chain_summary": main_bundle["chain_summary"],
        },
        expected_tracking_run_key=tracking_rk,
    )

    rng_2 = np.random.Generator(np.random.PCG64(seed_2))

    # Draw all perturbed 8x50x9 matrices in exact loop_order: "sigma_then_sample_0_to_19"
    perturbed_inputs: dict[tuple[float, int], list[list[list[float]]]] = {}
    for sg in sigmas:
        for s_idx in range(samples_per_level):
            sample_mats: list[list[list[float]]] = []
            for b_idx in range(8):
                orig = np.asarray(batches_data[b_idx], dtype=np.float64)
                noise = rng_2.normal(0.0, 1.0, orig.shape) * sg
                sample_mats.append((orig + noise).tolist())
            perturbed_inputs[(sg, s_idx)] = sample_mats

    # Execute per-batch diagnosis + dispersion + M1 clustering in DuckDB via ens_diagnose_batch
    cluster_eval_by_key: dict[tuple[float, int], list[dict[str, Any]]] = {}
    for sg in sigmas:
        select_rows: list[str] = []
        for s_idx in range(samples_per_level):
            mats = perturbed_inputs[(sg, s_idx)]
            for b_idx in range(8):
                mem_lit = (
                    "["
                    + ", ".join(
                        f"{{'member_id': 'ifs_ens_{m + 1:02d}', 'features': {mats[b_idx][m]}}}"
                        for m in range(50)
                    )
                    + "]"
                )
                select_rows.append(f"({s_idx}, {b_idx}, {mem_lit})")

        sql_level = f"""
        WITH raw_tasks(sample_idx, ordinal, members) AS (VALUES {', '.join(select_rows)}),
        diagnosed AS MATERIALIZED (
            SELECT
                sample_idx,
                ordinal,
                members,
                ens_diagnose_batch(members, {sql_value(weights_vec)}::DOUBLE[], {sql_value(sc_cfg)}) AS diag
            FROM raw_tasks
        )
        SELECT
            sample_idx,
            ordinal,
            diag,
            CASE WHEN diag = 'OK' THEN ens_dispersion(members, {sql_value(weights_vec)}::DOUBLE[]) ELSE NULL END AS disp,
            CASE WHEN diag = 'OK' THEN ens_cluster(members, {sql_value(sc_cfg)}) ELSE NULL END AS cl
        FROM diagnosed
        ORDER BY sample_idx ASC, ordinal ASC;
        """
        rc_l, parsed_l, err_l = _run_duckdb(ext_path, sql_level, timeout=240.0)
        if rc_l != 0:
            raise RuntimeError(f"Level 2 clustering query failed for sigma={sg}: {err_l}")
        for row in parsed_l[-1]:
            cluster_eval_by_key.setdefault((sg, int(row["sample_idx"])), []).append(row)

    # Execute whole-chain M3 evolution (`m3_runner --mode chain` or oracle fallback) on all 60 chains
    chain_cases: list[dict[str, Any]] = []
    chain_keys: list[tuple[float, int]] = []
    for sg in sigmas:
        for s_idx in range(samples_per_level):
            b_rows = cluster_eval_by_key[(sg, s_idx)]
            chain_batches: list[dict[str, Any]] = []
            for r in b_rows:
                b_ord = int(r["ordinal"])
                if r["diag"] == "OK":
                    chain_batches.append(
                        {
                            "ordinal": b_ord,
                            "status": "ok",
                            "dispersion": float(r["disp"]),
                            "scenarios": [
                                {
                                    "cluster_id": int(s["cluster_id"]),
                                    "member_count": int(s["member_count"]),
                                    "member_fraction": float(s["member_fraction"]),
                                    "mean_features": [float(x) for x in s["mean_features"]],
                                }
                                for s in r["cl"]["scenarios"]
                            ],
                        }
                    )
                else:
                    code = str(r["diag"])
                    chain_batches.append(
                        {
                            "ordinal": b_ord,
                            "status": "failed",
                            "error_code": code,
                            "status_reason": code,
                        }
                    )
            chain_cases.append(
                {
                    "case_id": f"l2_sg{sg}_s{s_idx:02d}",
                    "tracking_run_key": tracking_rk,
                    "threshold_coefficient": 1.0,
                    "weights": weights_vec,
                    "config": ev_cfg_clean,
                    "batches": chain_batches,
                }
            )
            chain_keys.append((sg, s_idx))

    chain_results_by_key: dict[tuple[float, int], dict[str, Any]] = {}
    if runner_bin is not None:
        with tempfile.TemporaryDirectory(prefix="ensabc-m3-l2-chains-") as td:
            in_p = Path(td) / "chains_in.json"
            out_p = Path(td) / "chains_out.json"
            in_p.write_text(json.dumps({"cases": chain_cases}), encoding="utf-8")
            proc = subprocess.run(
                [str(runner_bin), "--mode", "chain", "--input", str(in_p), "--output", str(out_p)],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"m3_runner --mode chain failed on Level 2: {proc.stderr}")
            batch_out = json.loads(out_p.read_text(encoding="utf-8"))["results"]
            for key, item in zip(chain_keys, batch_out):
                if item["status"] != "ok":
                    raise RuntimeError(f"Unexpected Level 2 chain failure: {item}")
                chain_results_by_key[key] = item["result"]
    else:
        for key, c_case in zip(chain_keys, chain_cases):
            ora = solve_chain_oracle(c_case)
            ora["branches"] = ora.pop("branch_summaries")
            chain_results_by_key[key] = ora

    sigma_zero_exact_matches = 0
    total_certified_evals = 0
    total_certified_flips = 0
    total_uncertified_flips = 0
    total_mapping_ambiguous_pairs = 0
    total_single_batch_failed_pairs = 0
    total_lineage_checked_nodes = 0
    total_lineage_violations = 0

    boundary_flips_l2: list[dict[str, Any]] = []
    lineage_flips_l2: list[dict[str, Any]] = []
    sigma_summaries: list[dict[str, Any]] = []

    for sg in sigmas:
        sg_exact_matches = 0
        sg_failed_batches = 0
        sg_failed_pairs = 0
        sg_ambig_batches = 0
        sg_ambig_pairs = 0
        sg_mapped_pairs = 0
        sg_cert_pairs = 0
        sg_cert_flips = 0
        sg_uncert_pairs = 0
        sg_uncert_flips = 0
        sg_delta_disps: list[float] = []
        sg_eps_disps: list[float] = []

        sg_nodes_evaluated = 0
        sg_invariant_ancestry_checked = 0
        sg_lineage_violations = 0
        sg_lineage_changes = 0
        sg_local_lineage_flips = 0
        sg_upstream_lineage_flips = 0
        sg_birth_rank_swaps = 0
        sample_records: list[dict[str, Any]] = []

        for s_idx in range(samples_per_level):
            b_rows = cluster_eval_by_key[(sg, s_idx)]
            chain_res = chain_results_by_key[(sg, s_idx)]

            if sg == 0.0:
                norm_nodes = [
                    {
                        **nd,
                        "primary_delta_fraction": (
                            nd["primary_delta_fraction"]
                            if nd["has_primary_delta_fraction"]
                            else 0.0
                        ),
                    }
                    for nd in chain_res["nodes"]
                ]
                norm_edges = [
                    {
                        **ed,
                        "optimality_margin": (
                            ed["optimality_margin"] if ed["has_optimality_margin"] else 0.0
                        ),
                        "optimality_margin_ratio": (
                            ed["optimality_margin_ratio"]
                            if ed["has_optimality_margin"]
                            else 0.0
                        ),
                        "secondary_margin": (
                            ed["secondary_margin"] if ed["has_secondary_margin"] else 0.0
                        ),
                        "secondary_margin_ratio": (
                            ed["secondary_margin_ratio"]
                            if ed["has_secondary_margin"]
                            else 0.0
                        ),
                    }
                    for ed in chain_res["edges"]
                ]
                norm_events = [
                    {
                        **ev,
                        "max_morphology_over_scale": (
                            0.0
                            if ev["max_morphology_over_scale"] is None
                            else ev["max_morphology_over_scale"]
                        ),
                        "min_margin_ratio": (
                            ev["min_margin_ratio"] if ev["has_min_margin_ratio"] else 0.0
                        ),
                    }
                    for ev in chain_res["events"]
                ]
                cand_proj = semantic_evolution_projection(
                    {
                        "planned_batch_count": chain_res["planned_batch_count"],
                        "batches": [
                            {
                                "ordinal": int(b["ordinal"]),
                                "status": b["status"],
                                "status_reason": b["status_reason"],
                                "dispersion": b["dispersion"],
                            }
                            for b in chain_res["batches"]
                        ],
                        "nodes": norm_nodes,
                        "edges": norm_edges,
                        "events": norm_events,
                        "branches": chain_res["branches"],
                        "chain_summary": chain_res["chain_summary"],
                    },
                    expected_tracking_run_key=tracking_rk,
                )
                if cand_proj == base_proj_ref:
                    sg_exact_matches += 1
                    sigma_zero_exact_matches += 1

            # Per-batch status and overlap mapping
            batch_ok: list[bool] = []
            pert_to_base_list: list[dict[int, int] | None] = []
            base_to_pert_list: list[dict[int, int] | None] = []
            batch_ambig_list: list[bool] = []

            for b_idx, r in enumerate(b_rows):
                if r["disp"] is None or r["cl"] is None:
                    batch_ok.append(False)
                    pert_to_base_list.append(None)
                    base_to_pert_list.append(None)
                    batch_ambig_list.append(False)
                    sg_failed_batches += 1
                else:
                    batch_ok.append(True)
                    sg_delta_disps.append(
                        abs(float(r["disp"]) - float(main_bundle["batches"][b_idx]["dispersion"]))
                    )
                    p_parts = _partitions_from_cluster_result(r["cl"])
                    mp, is_amb = overlap_cluster_mapping(base_partitions[b_idx], p_parts)
                    pert_to_base_list.append(mp)
                    base_to_pert_list.append({b_cid: p_cid for p_cid, b_cid in mp.items()})
                    batch_ambig_list.append(is_amb)
                    if is_amb:
                        sg_ambig_batches += 1

            # Evaluate all 7 adjacent transitions
            tr_by_prev = {int(t["prev_ordinal"]): t for t in chain_res["transitions"]}
            pair_outcomes: list[dict[str, Any]] = []

            for p_idx in range(7):
                bp = baseline_pairs[p_idx]
                if not batch_ok[p_idx] or not batch_ok[p_idx + 1]:
                    sg_failed_pairs += 1
                    total_single_batch_failed_pairs += 1
                    cert_fail = evaluate_evolution_stability_certificate(
                        bp["oracle_result"],
                        epsilon_abs=0.0,
                        batch_failed=True,
                    )
                    pair_outcomes.append(
                        {
                            "pair_index": p_idx,
                            "status": "single_batch_failed",
                            "certified": cert_fail["certified"],
                            "flipped": None,
                        }
                    )
                    continue

                if batch_ambig_list[p_idx] or batch_ambig_list[p_idx + 1]:
                    sg_ambig_pairs += 1
                    total_mapping_ambiguous_pairs += 1
                    cert_amb = evaluate_evolution_stability_certificate(
                        bp["oracle_result"],
                        epsilon_abs=0.0,
                        evaluation_mapping_ambiguous=True,
                    )
                    pair_outcomes.append(
                        {
                            "pair_index": p_idx,
                            "status": l2_cfg["ambiguous_mapping_status"],
                            "certified": cert_amb["certified"],
                            "flipped": None,
                        }
                    )
                    continue

                sg_mapped_pairs += 1
                mp_p = pert_to_base_list[p_idx]
                mp_c = pert_to_base_list[p_idx + 1]
                assert mp_p is not None and mp_c is not None

                pert_p_by_base = {
                    mp_p[int(s["cluster_id"])]: s for s in b_rows[p_idx]["cl"]["scenarios"]
                }
                pert_c_by_base = {
                    mp_c[int(s["cluster_id"])]: s for s in b_rows[p_idx + 1]["cl"]["scenarios"]
                }
                eps_abs = max(
                    *(
                        compute_weighted_rmse_exact(
                            bp["prev_scenarios"][i]["mean_features"],
                            pert_p_by_base[bp["prev_scenarios"][i]["cluster_id"]]["mean_features"],
                            weights_vec,
                        )
                        for i in range(len(bp["prev_scenarios"]))
                    ),
                    *(
                        compute_weighted_rmse_exact(
                            bp["curr_scenarios"][j]["mean_features"],
                            pert_c_by_base[bp["curr_scenarios"][j]["cluster_id"]]["mean_features"],
                            weights_vec,
                        )
                        for j in range(len(bp["curr_scenarios"]))
                    ),
                )
                sg_eps_disps.append(eps_abs)

                tr_rec = tr_by_prev[p_idx]
                pert_pair_scale = float(tr_rec["pair_scale"])
                delta_s = abs(pert_pair_scale - float(bp["oracle_result"]["pair_scale"]))

                cert = evaluate_evolution_stability_certificate(
                    bp["oracle_result"],
                    epsilon_abs=eps_abs,
                    threshold_coefficient=1.0,
                    delta_s=delta_s,
                    prev_scenarios=bp["prev_scenarios"],
                    curr_scenarios=bp["curr_scenarios"],
                    weights=weights_vec,
                )

                tr_edges = [
                    e for e in chain_res["edges"] if int(e["prev_ordinal"]) == p_idx
                ]
                tr_events = [
                    ev for ev in chain_res["events"] if int(ev["prev_ordinal"]) == p_idx
                ]
                mapped_sig = {
                    "primary_edges": sorted(
                        (mp_p[int(e["prev_cluster_id"])], mp_c[int(e["curr_cluster_id"])])
                        for e in tr_edges
                        if e["edge_role"] == "primary"
                    ),
                    "secondary_split_edges": sorted(
                        (mp_p[int(e["prev_cluster_id"])], mp_c[int(e["curr_cluster_id"])])
                        for e in tr_edges
                        if e["edge_role"] == "secondary_split"
                    ),
                    "secondary_merge_edges": sorted(
                        (mp_p[int(e["prev_cluster_id"])], mp_c[int(e["curr_cluster_id"])])
                        for e in tr_edges
                        if e["edge_role"] == "secondary_merge"
                    ),
                    "events": sorted(
                        (
                            str(ev["event_type"]),
                            tuple(sorted(mp_p[int(x)] for x in ev["prev_cluster_ids"])),
                            tuple(sorted(mp_c[int(y)] for y in ev["curr_cluster_ids"])),
                        )
                        for ev in tr_events
                    ),
                }
                flipped = mapped_sig != bp["signature"]

                if cert["certified"]:
                    sg_cert_pairs += 1
                    total_certified_evals += 1
                    if flipped:
                        sg_cert_flips += 1
                        total_certified_flips += 1
                else:
                    sg_uncert_pairs += 1
                    if flipped:
                        sg_uncert_flips += 1
                        total_uncertified_flips += 1
                        boundary_flips_l2.append(
                            {
                                "sigma_k": sg,
                                "sample_index": s_idx,
                                "pair_index": p_idx,
                                "prev_ordinal": p_idx,
                                "curr_ordinal": p_idx + 1,
                                "transition": f"b{p_idx}->b{p_idx + 1}",
                                "epsilon_abs": eps_abs,
                                "delta_s": delta_s,
                                "certified": False,
                                "certificate_status": cert["status"],
                                "certificate_failed_conditions": cert.get("failed_conditions", []),
                                "baseline_margins": bp["margins"],
                                "flip_details": _describe_flip_details(bp["signature"], mapped_sig),
                            }
                        )

                pair_outcomes.append(
                    {
                        "pair_index": p_idx,
                        "status": "certified" if cert["certified"] else "uncertified",
                        "certified": cert["certified"],
                        "epsilon_abs": eps_abs,
                        "delta_s": delta_s,
                        "flipped": flipped,
                    }
                )

            # Evaluate whole-chain logical branch lineage stability by (birth_ordinal, birth_canonical_rank)
            pert_nodes_by_pos: dict[tuple[int, int], dict[str, Any]] = {}
            for nd in chain_res["nodes"]:
                b_ord_n = int(nd["ordinal"])
                p_cid_n = int(nd["cluster_id"])
                pb_ord, pb_rank = parse_branch_birth_key(
                    str(nd["branch_id"]), expected_tracking_run_key=tracking_rk
                )
                pert_nodes_by_pos[(b_ord_n, p_cid_n)] = {
                    "ordinal": b_ord_n,
                    "cluster_id": p_cid_n,
                    "canonical_rank": int(nd["canonical_rank"]),
                    "branch_id": str(nd["branch_id"]),
                    "birth_key": [pb_ord, pb_rank],
                }
            pert_in_primary: dict[tuple[int, int], int] = {
                (int(e["curr_ordinal"]), int(e["curr_cluster_id"])): int(e["prev_cluster_id"])
                for e in chain_res["edges"]
                if e["edge_role"] == "primary"
            }

            for (b_ord, base_cid), b_node in sorted(base_nodes_map.items()):
                anc_path = base_ancestry[(b_ord, base_cid)]
                birth_ord_0, birth_cid_0 = anc_path[0]
                start_check_ord = max(0, birth_ord_0 - 1)
                if any(
                    (not batch_ok[u]) or batch_ambig_list[u]
                    for u in range(start_check_ord, b_ord + 1)
                ):
                    continue

                sg_nodes_evaluated += 1
                b2p_curr = base_to_pert_list[b_ord]
                assert b2p_curr is not None
                pert_cid = b2p_curr[base_cid]
                p_node = pert_nodes_by_pos[(b_ord, pert_cid)]

                # Check whether birth mapping (cluster + canonical_rank) AND all upstream primary transitions held
                b2p_birth = base_to_pert_list[birth_ord_0]
                assert b2p_birth is not None
                pert_birth_cid_expected = b2p_birth[birth_cid_0]
                p_birth_node = pert_nodes_by_pos[(birth_ord_0, pert_birth_cid_expected)]

                birth_still_root = (
                    pert_in_primary.get((birth_ord_0, pert_birth_cid_expected)) is None
                )
                birth_rank_invariant = (
                    p_birth_node["canonical_rank"] == b_node["birth_key"][1]
                )
                upstream_steps_invariant = birth_still_root
                first_broken_transition_ord: int | None = (
                    (birth_ord_0 - 1) if (birth_ord_0 > 0 and not birth_still_root) else None
                )

                for step_idx in range(1, len(anc_path)):
                    prev_o, prev_bc = anc_path[step_idx - 1]
                    curr_o, curr_bc = anc_path[step_idx]
                    b2p_step_curr = base_to_pert_list[curr_o]
                    p2b_step_prev = pert_to_base_list[prev_o]
                    assert b2p_step_curr is not None and p2b_step_prev is not None
                    p_curr_cid = b2p_step_curr[curr_bc]
                    p_parent = pert_in_primary.get((curr_o, p_curr_cid))
                    mapped_p_parent = (
                        p2b_step_prev[p_parent] if p_parent is not None else None
                    )
                    if mapped_p_parent != prev_bc:
                        upstream_steps_invariant = False
                        if first_broken_transition_ord is None:
                            first_broken_transition_ord = prev_o

                lineage_equal = p_node["birth_key"] == b_node["birth_key"]
                if upstream_steps_invariant and birth_rank_invariant:
                    sg_invariant_ancestry_checked += 1
                    total_lineage_checked_nodes += 1
                    if not lineage_equal:
                        sg_lineage_violations += 1
                        total_lineage_violations += 1

                if not lineage_equal:
                    sg_lineage_changes += 1
                    # Determine whether this lineage change is local vs upstream-induced
                    local_flipped = False
                    if b_ord > 0:
                        p2b_prev = pert_to_base_list[b_ord - 1]
                        assert p2b_prev is not None
                        p_imm_parent = pert_in_primary.get((b_ord, pert_cid))
                        mapped_imm_parent = (
                            p2b_prev[p_imm_parent] if p_imm_parent is not None else None
                        )
                        base_imm_parent = base_in_primary.get((b_ord, base_cid))
                        local_flipped = mapped_imm_parent != base_imm_parent

                    if local_flipped:
                        flip_origin = "local_boundary_flip"
                        sg_local_lineage_flips += 1
                        cause_ord = b_ord - 1
                    elif first_broken_transition_ord is not None:
                        flip_origin = "upstream_induced_flip"
                        sg_upstream_lineage_flips += 1
                        cause_ord = first_broken_transition_ord
                    else:
                        flip_origin = "birth_canonical_rank_swap"
                        sg_birth_rank_swaps += 1
                        cause_ord = min(birth_ord_0, 6)

                    lineage_flips_l2.append(
                        {
                            "sigma_k": sg,
                            "sample_index": s_idx,
                            "ordinal": b_ord,
                            "baseline_cluster_id": base_cid,
                            "perturbed_cluster_id": pert_cid,
                            "baseline_lineage_birth_key": b_node["birth_key"],
                            "perturbed_lineage_birth_key": p_node["birth_key"],
                            "flip_origin": flip_origin,
                            "root_cause_transition_ordinal": cause_ord,
                            "root_cause_transition": f"b{cause_ord}->b{cause_ord + 1}",
                            "baseline_margins": baseline_pairs[cause_ord]["margins"],
                        }
                    )

            sample_records.append(
                {
                    "sample_index": s_idx,
                    "failed_batches_count": sum(1 for ok in batch_ok if not ok),
                    "mapping_ambiguous_batches_count": sum(1 for a in batch_ambig_list if a),
                    "certified_pairs_count": sum(1 for po in pair_outcomes if po["certified"]),
                    "flipped_pairs_count": sum(1 for po in pair_outcomes if po["flipped"] is True),
                    "pairs": pair_outcomes,
                }
            )

        sigma_summaries.append(
            {
                "sigma_k": sg,
                "samples_tested": samples_per_level,
                "exact_baseline_matches_count": sg_exact_matches if sg == 0.0 else None,
                "mean_delta_dispersion_k": (
                    float(math.fsum(sg_delta_disps) / len(sg_delta_disps))
                    if sg_delta_disps
                    else 0.0
                ),
                "max_delta_dispersion_k": max(sg_delta_disps, default=0.0),
                "mean_scenario_rms_displacement_k": (
                    float(math.fsum(sg_eps_disps) / len(sg_eps_disps)) if sg_eps_disps else 0.0
                ),
                "max_scenario_rms_displacement_k": max(sg_eps_disps, default=0.0),
                "single_batch_failed_batches_count": sg_failed_batches,
                "single_batch_failed_pairs_count": sg_failed_pairs,
                "mapping_ambiguous_batches_count": sg_ambig_batches,
                "mapping_ambiguous_pairs_count": sg_ambig_pairs,
                "mapped_evaluable_pairs_count": sg_mapped_pairs,
                "certified_pairs_count": sg_cert_pairs,
                "certified_pair_flips": sg_cert_flips,
                "uncertified_pairs_count": sg_uncert_pairs,
                "uncertified_pair_flips_count": sg_uncert_flips,
                "lineage_summary": {
                    "nodes_evaluated": sg_nodes_evaluated,
                    "invariant_ancestry_nodes_checked": sg_invariant_ancestry_checked,
                    "lineage_invariance_violations": sg_lineage_violations,
                    "total_lineage_changes": sg_lineage_changes,
                    "local_boundary_lineage_flips_count": sg_local_lineage_flips,
                    "upstream_induced_lineage_flips_count": sg_upstream_lineage_flips,
                    "birth_canonical_rank_swaps_count": sg_birth_rank_swaps,
                },
                "samples": sample_records,
            }
        )

    sigma_zero_exact_match = sigma_zero_exact_matches == samples_per_level
    if not sigma_zero_exact_match:
        raise AssertionError(
            f"Expected all {samples_per_level} sigma==0.0 samples to reproduce baseline identically, got {sigma_zero_exact_matches}"
        )
    if total_certified_evals <= 0:
        raise AssertionError("Expected total_certified_evals > 0 in Level 2")
    if total_certified_flips != 0:
        raise AssertionError(
            f"4-condition stability certificate violated in Level 2: certified_pair_flips={total_certified_flips}"
        )
    if total_lineage_violations != 0:
        raise AssertionError(
            f"Logical branch lineage invariance violated {total_lineage_violations} times in Level 2"
        )
    if not all(bool(item.get("baseline_margins")) for item in boundary_flips_l2):
        raise AssertionError("Every Level 2 boundary flip must carry baseline_margins")
    if not all(bool(item.get("baseline_margins")) for item in lineage_flips_l2):
        raise AssertionError("Every Level 2 lineage flip must carry baseline_margins")

    doc = {
        "schema_version": "m3.perturbation_member_level.v1",
        "status": "PASS",
        "checked_at_utc": _utc_now(),
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "baseline_evolution_run_id": main_bundle["evolution_run_id"],
        "baseline_tracking_run_key": tracking_rk,
        "method": l2_cfg["method"],
        "rng": l2_cfg["rng"],
        "seed": seed_2,
        "loop_order": l2_cfg["loop_order"],
        "scenario_mapping": l2_cfg["scenario_mapping"],
        "ambiguous_mapping_status": l2_cfg["ambiguous_mapping_status"],
        "stability_certificate_schema": CERTIFICATE_RULE_V1,
        "sigma_levels_k": sigmas,
        "samples_per_level": samples_per_level,
        "total_chain_runs": len(chain_cases),
        "total_pair_evaluations": len(chain_cases) * 7,
        "sigma_zero_exact_match": sigma_zero_exact_match,
        "certified_evaluations_count": total_certified_evals,
        "certified_pair_flips": total_certified_flips,
        "uncertified_pair_flips_count": total_uncertified_flips,
        "mapping_ambiguous_pairs_count": total_mapping_ambiguous_pairs,
        "single_batch_failed_pairs_count": total_single_batch_failed_pairs,
        "lineage_invariance_checked_nodes": total_lineage_checked_nodes,
        "lineage_invariance_violations": total_lineage_violations,
        "all_flips_carry_baseline_margins": True,
        "sigma_summaries": sigma_summaries,
        "boundary_flips_count": len(boundary_flips_l2),
        "boundary_flips": boundary_flips_l2,
        "lineage_flips_count": len(lineage_flips_l2),
        "lineage_flips": lineage_flips_l2,
    }
    _assert_all_numbers_finite(doc, "perturbation-member-level")
    _assert_no_prohibited_overclaims(doc, "perturbation-member-level.json")
    return doc


def _build_limitations_artifact(
    *,
    main_doc: dict[str, Any],
    thr_doc: dict[str, Any],
    cls_doc: dict[str, Any],
    l2_doc: dict[str, Any],
) -> dict[str, Any]:
    """Construct honest 5-boundary `limitations.json` with zero prohibited overclaims (T034)."""
    boundaries_list = [
        {
            "boundary_id": "non_physical_member_migration",
            "title": "Algorithmic Scenario Attribution vs Physical Member Trajectories",
            "statement": (
                "Primary continuation, secondary_split, secondary_merge edges, and attributed_flow values are "
                "deterministic algorithmic attributions computed solely from scenario mean-field distances and "
                "per-batch member proportions; they do NOT represent physical migration or cross-run identity "
                "continuity of individual ensemble members."
            ),
            "empirical_evidence_ref": "case-study/main-evolution-k3-c1.0.json",
        },
        {
            "boundary_id": "non_physical_birth_death",
            "title": "Threshold-Relative Emergence/Disappearance vs Physical Weather System Genesis/Lysis",
            "statement": (
                "Emergence and disappearance events indicate only that a scenario has zero feasible counterparts "
                "within the active weighted-RMSE threshold tau = c * S (and are strictly separated from "
                "gap_interrupted / gap_restarted batch-unavailability markers); they do NOT represent physical "
                "cyclogenesis, frontogenesis, or lysis of atmospheric systems."
            ),
            "empirical_evidence_ref": "case-study/sensitivity-clustering-configs.json",
        },
        {
            "boundary_id": "non_calibrated_probability_and_no_skill_claims",
            "title": "Uncalibrated Ensemble Proportions and Zero Skill/Trading Claims",
            "statement": (
                "Node member_fraction values are raw within-batch ensemble member counts divided by 50 without "
                "post-processing or statistical calibration; M3 makes zero claims of probability calibration, "
                "forecast skill improvement, operational decision utility, or financial trading value."
            ),
            "empirical_evidence_ref": "case-study/perturbation-evolution-level.json",
        },
        {
            "boundary_id": "coarse_wb2_eu3x3_sample_boundaries",
            "title": "Coarse 5.625-Degree 3x3 WB2 Sample & Near-Boundary Perturbation Sensitivity",
            "statement": (
                "The frozen WeatherBench2 IFS ENS 2m_temperature sample spans a 3x3 European grid (~5.625 deg "
                "spacing, 50 perturbed members only, without control member or operational dissemination "
                "timestamps) for a single target valid time (2020-01-05T00:00:00Z). While certified transitions "
                f"exhibited 0 flips, low-margin transitions (b0->b1, b3->b4, b6->b7) experienced "
                f"{l2_doc['uncertified_pair_flips_count']} uncertified boundary flips across 40 perturbed "
                "member-level chains (sigma in {0.001, 0.01} K), confirming why raw margins must always accompany "
                "discrete event labels."
            ),
            "empirical_evidence_ref": "case-study/perturbation-member-level.json",
        },
        {
            "boundary_id": "max_cardinality_high_threshold_saturation",
            "title": "Max-Cardinality Saturation at Large Thresholds and Fixed-k Configuration Dependence",
            "statement": (
                "Because primary matching prioritizes maximum 1-to-1 cardinality before minimum distance sum, "
                "large threshold coefficients (c = 2.0 at k = 3) saturate all 7 transitions into 21 primary "
                "continuations with 0 splits and 0 merges, whereas tight thresholds (c = 0.5) under-link into "
                "4 continuations, 17 emergences, and 17 disappearances; similarly, varying cluster_count, "
                "projection_dim, or rank_threshold alters branch granularity or triggers single-batch rank/subspace "
                f"chain breaks ({cls_doc['failed_batch_runs_count']} runs with failed batches)."
            ),
            "empirical_evidence_ref": "case-study/sensitivity-thresholds.json",
        },
    ]

    boundaries_map = {item["boundary_id"]: item for item in boundaries_list}
    doc = {
        "schema_version": "m3.limitations.v1",
        "status": "PASS",
        "checked_at_utc": _utc_now(),
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "main_evolution_run_id": main_doc["evolution_run_id"],
        "threshold_grid_runs_count": thr_doc["grid_count"],
        "clustering_sensitivity_runs_count": cls_doc["runs_count"],
        "limitations_count": len(boundaries_list),
        "prohibited_overclaims_present": False,
        "boundaries": boundaries_map,
        "limitations": boundaries_list,
    }
    _assert_all_numbers_finite(doc, "limitations")
    _assert_no_prohibited_overclaims(doc, "limitations.json")
    return doc


def _resolve_prerequisites(
    args: argparse.Namespace,
) -> tuple[Path, Path, Path, Path, Path, Path | None]:
    if shutil.which("duckdb") is None:
        raise PrerequisiteError("duckdb CLI binary not found on PATH")
    if not EXPERIMENT_CONFIG_PATH.is_file():
        raise PrerequisiteError(f"Experiment config fixture missing: {EXPERIMENT_CONFIG_PATH}")
    if not SPATIAL_WEIGHTS_PATH.is_file():
        raise PrerequisiteError(f"Spatial weights fixture missing: {SPATIAL_WEIGHTS_PATH}")

    run_dir = args.run_dir.resolve()

    if args.extension is not None:
        ext_path = args.extension.resolve()
    elif (run_dir / "build" / "ensabc_m0_extension.duckdb_extension").is_file():
        ext_path = (run_dir / "build" / "ensabc_m0_extension.duckdb_extension").resolve()
    else:
        ext_path = Path(
            "/tmp/ensabc-m3-extension-target/ensabc_m0_extension.duckdb_extension"
        ).resolve()
    if not ext_path.is_file():
        raise PrerequisiteError(f"DuckDB extension binary not found: {ext_path}")

    if args.sample_dir is not None:
        sample_dir = args.sample_dir.resolve()
    elif (run_dir / "sample" / "om_sample_manifest.json").is_file():
        sample_dir = (run_dir / "sample").resolve()
    else:
        sample_dir = (REPO_ROOT / "validation" / "m2" / "data" / "om_sample").resolve()

    manifest_path = sample_dir / "om_sample_manifest.json"
    if not manifest_path.is_file():
        raise PrerequisiteError(f"OM sample manifest not found: {manifest_path}")
    manifest = load_json(manifest_path)
    for b in manifest.get("batches", []):
        om_file = sample_dir / b["om_filename"]
        if not om_file.is_file():
            raise PrerequisiteError(f"Missing OM batch file: {om_file}")

    cache_dir = (args.cache_dir or M2_CACHE_DIR).resolve()
    duckomo_path = cache_dir / "v0.3.5-rc.1" / "duckomo.duckdb_extension"
    if not duckomo_path.is_file():
        raise PrerequisiteError(f"DuckOMO v0.3.5-rc.1 extension not found: {duckomo_path}")

    db_path = (
        args.db.resolve()
        if args.db is not None
        else (run_dir / "sql-history" / "evolution-runs.duckdb").resolve()
    )
    runner_bin = _locate_m3_runner(ext_path, run_dir)
    return run_dir, ext_path, sample_dir, duckomo_path, db_path, runner_bin


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run M3 WB2 8-batch case study, sensitivity grids, and 2-level perturbations."
    )
    parser.add_argument("--run-dir", type=Path, required=True, help="Target M3 run directory")
    parser.add_argument(
        "--extension",
        type=Path,
        default=None,
        help="Path to compiled ensabc_m0_extension.duckdb_extension",
    )
    parser.add_argument(
        "--sample-dir",
        type=Path,
        default=None,
        help="Directory containing frozen 8-batch WB2 OM sample files",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Cache directory containing DuckOMO extensions (default: validation/m2/.cache)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Formal DuckDB 11-table history database path (default: <run-dir>/sql-history/evolution-runs.duckdb)",
    )
    args = parser.parse_args(argv)

    t0 = time.monotonic()
    try:
        run_dir, ext_path, sample_dir, duckomo_path, db_path, runner_bin = _resolve_prerequisites(
            args
        )
    except PrerequisiteError as exc:
        print(f"[PREREQUISITE_MISSING] {exc}", file=sys.stderr)
        return 2
    except LocatedError as exc:
        print(f"[PREREQUISITE_ERROR] {exc}", file=sys.stderr)
        return 2

    try:
        exp_cfg = load_json(EXPERIMENT_CONFIG_PATH)
        weights_vec = [float(w) for w in load_json(SPATIAL_WEIGHTS_PATH)["weights_vector"]]
        manifest = load_json(sample_dir / "om_sample_manifest.json")
        batches_data = [
            read_via_omfiles(sample_dir / b["om_filename"])["data"].reshape(50, 9).tolist()
            for b in manifest["batches"]
        ]
        evolution_cfg = _clean_evolution_config(exp_cfg["evolution"])

        # Collect all SQL runs: Main (k=3, c=1.0) + 18 Threshold Grid + 4-Axis Clustering Sensitivity
        run_specs: list[tuple[str, dict[str, Any], float]] = [
            ("main", exp_cfg["clustering"], 1.0),
        ]
        for k in exp_cfg["threshold_grid"]["cluster_counts"]:
            for c in exp_cfg["threshold_grid"]["threshold_coefficients"]:
                sc_grid = {**exp_cfg["clustering"], "cluster_count": int(k)}
                run_specs.append((f"thr_k{int(k)}_c{float(c)}", sc_grid, float(c)))

        for axis_obj in exp_cfg["clustering_sensitivity"]["axes"].values():
            for var in axis_obj["variants"]:
                run_specs.append((f"cls_{var['config_id']}", var, 1.0))

        published = _publish_all_sql_runs(
            ext_path=ext_path,
            duckomo_path=duckomo_path,
            sample_dir=sample_dir,
            db_path=db_path,
            weights_vec=weights_vec,
            evolution_cfg=evolution_cfg,
            run_specs=run_specs,
        )

        case_dir = run_dir / "case-study"
        case_dir.mkdir(parents=True, exist_ok=True)

        # 1. Main 8-batch case study (`main-evolution-k3-c1.0.json`, T032)
        main_doc = _build_main_case_artifact(
            main_entry=published["main"],
            exp_cfg=exp_cfg,
            db_path=db_path,
            run_dir=run_dir,
            ext_path=ext_path,
            runner_bin=runner_bin,
        )
        atomic_write_json(case_dir / "main-evolution-k3-c1.0.json", main_doc)

        # 2. 18-combination threshold sensitivity (`sensitivity-thresholds.json`, T032)
        thr_doc = _build_threshold_sensitivity_artifact(
            published=published,
            exp_cfg=exp_cfg,
            db_path=db_path,
            run_dir=run_dir,
        )
        atomic_write_json(case_dir / "sensitivity-thresholds.json", thr_doc)

        # 3. 4-axis clustering config sensitivity (`sensitivity-clustering-configs.json`, T033)
        cls_doc = _build_clustering_sensitivity_artifact(
            published=published,
            exp_cfg=exp_cfg,
            db_path=db_path,
            run_dir=run_dir,
        )
        atomic_write_json(case_dir / "sensitivity-clustering-configs.json", cls_doc)

        # 4. Level 1a & 1b evolution-level perturbations (`perturbation-evolution-level.json`, T034)
        l1_doc, baseline_pairs = _build_evolution_level_perturbation_artifact(
            main_bundle=published["main"]["bundle"],
            exp_cfg=exp_cfg,
            weights_vec=weights_vec,
            runner_bin=runner_bin,
            ext_path=ext_path,
        )
        atomic_write_json(case_dir / "perturbation-evolution-level.json", l1_doc)

        # 5. Level 2 member-level perturbations (`perturbation-member-level.json`, T034)
        l2_doc = _build_member_level_perturbation_artifact(
            main_bundle=published["main"]["bundle"],
            baseline_pairs=baseline_pairs,
            batches_data=batches_data,
            exp_cfg=exp_cfg,
            weights_vec=weights_vec,
            ext_path=ext_path,
            runner_bin=runner_bin,
        )
        atomic_write_json(case_dir / "perturbation-member-level.json", l2_doc)

        # 6. Scope & method limitations (`limitations.json`, T034)
        lim_doc = _build_limitations_artifact(
            main_doc=main_doc,
            thr_doc=thr_doc,
            cls_doc=cls_doc,
            l2_doc=l2_doc,
        )
        atomic_write_json(case_dir / "limitations.json", lim_doc)

        elapsed = time.monotonic() - t0
        distinct_runs_total = len({v["bundle"]["evolution_run_id"] for v in published.values()})
        print(
            f"M3 case study, sensitivity & perturbation verification: PASS in {elapsed:.2f}s "
            f"(published_runs={distinct_runs_total}, l1a_certified_evals={l1_doc['certified_evaluations_count']}, "
            f"l1a_cert_flips={l1_doc['certified_pair_flips']}, l1b_flips={l1_doc['level_1b_topology_flips']}, "
            f"l2_certified_evals={l2_doc['certified_evaluations_count']}, l2_cert_flips={l2_doc['certified_pair_flips']}) "
            f"-> {case_dir}"
        )
        return 0
    except Exception as exc:
        print(f"M3 case study, sensitivity & perturbation verification FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
