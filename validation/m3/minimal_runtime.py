#!/usr/bin/env python3
"""Host-side preparation and field-by-field parity verifier for M3 minimal runtime (T039).

Python runs ONLY on the host outside the airgapped container:
- `prepare`: verifies the pinned DuckOMO `v0.3.5-rc.1` SHA-256, executes a normal
  host DuckDB 11-table replay on the frozen 8-batch WB2 `.om` sample, extracts
  standalone `m3_runner` inputs (`--mode pair` across all 7 WB2 adjacent pairs and
  `--mode chain` across all 8 WB2 batches from raw 50x9 ensemble members), captures
  host `m3_runner` outputs, and emits container SQL scripts (`replay_11table.sql`
  and `recovery_session.sql`) into the read-only container staging directory.
- `verify`: validates container exit status, `--network none` routing table emptiness,
  absence of all 8 prohibited reference/scripting tools (`python3`, `python`, `cdo`,
  `wgrib`, `wgrib2`, `gfortran`, `R`, `Rscript`), 100% field-by-field parity of
  standalone `m3_runner` (`pair` + `chain`) and all 11 published DuckDB history
  tables (5 M2 + 6 M3), and same-session error recovery (`INVALID_FRACTION_SUM` +
  `BACKBONE_INCONSISTENCY` -> mid-transaction `inject_m3_failure` -> `ROLLBACK` ->
  `0` residual rows across all 6 M3 & 5 M2 tables -> subsequent valid publish
  `RECOVERED` with 100% 11-table parity), writing `<run_dir>/minimal-runtime.json`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
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
    M2_FIXTURES_DIR,
    NON_PHYSICAL_DISCLAIMER,
    SQL_DIR,
    atomic_write_json,
    atomic_write_text,
    default_evolution_config,
    default_scenario_config,
    is_finite_number,
    load_json,
    normalize_floats,
    parse_branch_birth_key,
    parse_duckdb_json,
    real_evolution_binding,
    run_isolated_duckdb,
    sha256_file,
    sql_value,
)

SCHEMA_VERSION = "m3.minimal_runtime.v1"
PROHIBITED_TOOLS: tuple[str, ...] = (
    "python3",
    "python",
    "cdo",
    "wgrib",
    "wgrib2",
    "gfortran",
    "R",
    "Rscript",
)

M2_TABLE_NAMES: tuple[str, ...] = (
    "tracking_runs",
    "replay_batches",
    "scenario_nodes",
    "pair_comparisons",
    "scenario_links",
)

M3_TABLE_NAMES: tuple[str, ...] = (
    "evolution_runs",
    "evolution_nodes",
    "evolution_transitions",
    "evolution_edges",
    "evolution_events",
    "evolution_branch_summaries",
)

ALL_11_TABLES: tuple[str, ...] = M2_TABLE_NAMES + M3_TABLE_NAMES


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _export_11tables_sql(run_key: str, evolution_run_id: str) -> str:
    """Return a single-row deterministic JSON-pack query exporting all 11 tables."""
    rk = sql_value(run_key)
    erid = sql_value(evolution_run_id)
    return textwrap.dedent(
        f"""
        SELECT
            coalesce((
                SELECT list(t ORDER BY t.run_key ASC) FROM (
                    SELECT
                        run_key,
                        execution_binding,
                        weights,
                        planned_batch_count,
                        content_sha256
                    FROM tracking_runs
                    WHERE run_key = {rk}
                ) t
            ), []) AS tracking_runs,
            coalesce((
                SELECT list(b ORDER BY b.ordinal ASC) FROM (
                    SELECT
                        run_key,
                        ordinal,
                        strftime(init_time, '%Y-%m-%dT%H:%M:%SZ') AS init_time_utc,
                        lead_hours,
                        status,
                        status_reason,
                        input_sha256,
                        om_sha256,
                        dispersion,
                        cluster_result_sha256
                    FROM replay_batches
                    WHERE run_key = {rk}
                ) b
            ), []) AS replay_batches,
            coalesce((
                SELECT list(sn ORDER BY sn.ordinal ASC, sn.cluster_id ASC) FROM (
                    SELECT
                        run_key,
                        ordinal,
                        cluster_id,
                        canonical_rank,
                        mean_features,
                        member_count,
                        member_fraction,
                        representative_member_id,
                        within_cluster_sse,
                        branch_id,
                        assignment_reason
                    FROM scenario_nodes
                    WHERE run_key = {rk}
                ) sn
            ), []) AS scenario_nodes,
            coalesce((
                SELECT list(pc ORDER BY pc.prev_ordinal ASC) FROM (
                    SELECT
                        run_key,
                        prev_ordinal,
                        curr_ordinal,
                        pair_scale,
                        threshold,
                        match_result
                    FROM pair_comparisons
                    WHERE run_key = {rk}
                ) pc
            ), []) AS pair_comparisons,
            coalesce((
                SELECT list(sl ORDER BY sl.prev_ordinal ASC, sl.prev_cluster_id ASC) FROM (
                    SELECT
                        run_key,
                        prev_ordinal,
                        prev_cluster_id,
                        curr_ordinal,
                        curr_cluster_id,
                        distance,
                        distance_ratio,
                        alternative_status,
                        ambiguous
                    FROM scenario_links
                    WHERE run_key = {rk}
                ) sl
            ), []) AS scenario_links,
            coalesce((
                SELECT list(er ORDER BY er.evolution_run_id ASC) FROM (
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
                    WHERE evolution_run_id = {erid}
                ) er
            ), []) AS evolution_runs,
            coalesce((
                SELECT list(en ORDER BY en.ordinal ASC, en.cluster_id ASC) FROM (
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
                    WHERE evolution_run_id = {erid}
                ) en
            ), []) AS evolution_nodes,
            coalesce((
                SELECT list(et ORDER BY et.prev_ordinal ASC) FROM (
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
                        evolve_result
                    FROM evolution_transitions
                    WHERE evolution_run_id = {erid}
                ) et
            ), []) AS evolution_transitions,
            coalesce((
                SELECT list(ee ORDER BY ee.prev_ordinal ASC, ee.prev_cluster_id ASC, ee.curr_cluster_id ASC) FROM (
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
                    WHERE evolution_run_id = {erid}
                ) ee
            ), []) AS evolution_edges,
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
                    WHERE evolution_run_id = {erid}
                ) ev
            ), []) AS evolution_events,
            coalesce((
                SELECT list(br ORDER BY br.branch_id ASC) FROM (
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
                    WHERE evolution_run_id = {erid}
                ) br
            ), []) AS evolution_branch_summaries;
        """
    )


def _session_vars_sql(
    *,
    om_dir_str: str,
    om_hashes: list[str],
    weights_vec: list[float],
    scenario_cfg: dict[str, Any],
    evolve_cfg: dict[str, Any],
    execution_binding: dict[str, Any],
    run_key: str,
    evolution_binding: dict[str, Any],
    evolution_run_id: str,
    inject_m3_failure: str = "false",
) -> str:
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
    evolve_cfg_sql = {
        "secondary_rule": str(evolve_cfg["secondary_rule"]),
        "flow_rule": str(evolve_cfg["flow_rule"]),
        "fraction_sum_tolerance": float(evolve_cfg["fraction_sum_tolerance"]),
        "high_uncertainty_threshold_ratio": float(evolve_cfg["high_uncertainty_threshold_ratio"]),
        "high_uncertainty_optimality_ratio": float(evolve_cfg["high_uncertainty_optimality_ratio"]),
        "high_uncertainty_secondary_ratio": float(evolve_cfg["high_uncertainty_secondary_ratio"]),
    }
    return textwrap.dedent(
        f"""
        SET VARIABLE om_dir = {sql_value(om_dir_str)};
        SET VARIABLE om_hashes = {sql_value(om_hashes)};
        SET VARIABLE weights = {sql_value(weights_vec)}::DOUBLE[];
        SET VARIABLE threshold_coefficient = 1.0::DOUBLE;
        SET VARIABLE planned_batch_count = 8;
        SET VARIABLE cluster_config = {sql_value(cluster_cfg_sql)};
        SET VARIABLE evolve_config = {sql_value(evolve_cfg_sql)};
        SET VARIABLE execution_binding = {sql_value(execution_binding)};
        SET VARIABLE run_key = {sql_value(run_key)};
        SET VARIABLE evolution_binding = {sql_value(evolution_binding)};
        SET VARIABLE evolution_run_id = {sql_value(evolution_run_id)};
        SET VARIABLE inject_failure = false;
        SET VARIABLE inject_m3_failure = {sql_value(inject_m3_failure)};
        """
    )


def prepare(
    *,
    staging_dir: Path,
    baseline_dir: Path,
    sample_dir: Path,
    extension_path: Path,
    duckomo_path: Path,
    runner_path: Path,
) -> None:
    """Run host baseline replay + m3_runner and stage container inputs & SQL."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    baseline_dir.mkdir(parents=True, exist_ok=True)

    releases = load_json(M2_FIXTURES_DIR / "duckomo_releases.json")
    target_release = releases["target"]
    actual_duckomo_sha = sha256_file(duckomo_path)
    if actual_duckomo_sha != target_release["extension_sha256"]:
        raise RuntimeError(
            f"DuckOMO target SHA-256 mismatch: expected {target_release['extension_sha256']}, "
            f"got {actual_duckomo_sha}"
        )

    manifest = load_json(sample_dir / "om_sample_manifest.json")
    om_hashes = [sha256_file(sample_dir / b["om_filename"]) for b in manifest["batches"]]
    weights_vec: list[float] = load_json(M2_FIXTURES_DIR / "spatial_weights_eu3x3.json")["weights_vector"]
    sc_cfg = default_scenario_config()
    ev_cfg = default_evolution_config()

    binding = real_evolution_binding(
        sample_dir,
        extension_path,
        duckomo_path,
        scenario_config=sc_cfg,
        threshold_coefficient=1.0,
        evolution_config=ev_cfg,
    )
    run_key = binding["tracking_run_key"]
    evolution_run_id = binding["evolution_run_id"]

    # 1. Execute normal host 11-table replay + extract raw 8x50x9 members
    host_vars = _session_vars_sql(
        om_dir_str=str(sample_dir.resolve()),
        om_hashes=om_hashes,
        weights_vec=weights_vec,
        scenario_cfg=sc_cfg,
        evolve_cfg=ev_cfg,
        execution_binding=binding["tracking_binding"],
        run_key=run_key,
        evolution_binding=binding["evolution_binding"],
        evolution_run_id=evolution_run_id,
    )
    host_sql = (
        f"LOAD {sql_value(str(duckomo_path.resolve()))};\n"
        f"LOAD {sql_value(str(extension_path.resolve()))};\n"
        + host_vars
        + "\n.read validation/m3/sql/schema.sql\n"
        + ".read validation/m3/sql/real_8run_evolution.sql\n"
        + _export_11tables_sql(run_key, evolution_run_id)
        + "\nSELECT ordinal, members FROM stage_raw_batches ORDER BY ordinal ASC;\n"
    )
    with tempfile.TemporaryDirectory(prefix="ensabc-m3-minrt-host-") as tmp_str:
        host_db = Path(tmp_str) / "host_baseline.duckdb"
        old_cwd = Path.cwd()
        try:
            import os
            os.chdir(REPO_ROOT)
            proc = run_isolated_duckdb(host_sql, database=str(host_db), timeout=120.0, bail=True)
        finally:
            os.chdir(old_cwd)
        if proc.returncode != 0:
            raise RuntimeError(f"Host 11-table baseline replay failed: {proc.stderr}")

    parsed = parse_duckdb_json(proc.stdout)
    if len(parsed) < 2 or not parsed[-2]:
        raise RuntimeError("Host DuckDB output missing 11-table export or raw member batches")
    host_11tables = normalize_floats(parsed[-2][0])
    raw_member_rows = parsed[-1]
    if len(raw_member_rows) != 8:
        raise RuntimeError(f"Expected 8 raw member batches from host replay, got {len(raw_member_rows)}")

    # 2. Build real 8-batch WB2 inputs for standalone m3_runner (--mode pair & --mode chain)
    nodes_by_ord: dict[int, list[dict[str, Any]]] = {}
    for node in host_11tables["evolution_nodes"]:
        nodes_by_ord.setdefault(int(node["ordinal"]), []).append(
            {
                "cluster_id": int(node["cluster_id"]),
                "member_count": int(node["member_count"]),
                "member_fraction": float(node["member_fraction"]),
                "mean_features": [float(x) for x in node["mean_features"]],
            }
        )
    disp_by_ord = {
        int(b["ordinal"]): float(b["dispersion"])
        for b in host_11tables["replay_batches"]
    }
    runner_cfg = {
        "secondary_rule": ev_cfg["secondary_rule"],
        "flow_rule": ev_cfg["flow_rule"],
        "fraction_sum_tolerance": float(ev_cfg["fraction_sum_tolerance"]),
        "high_uncertainty_threshold_ratio": float(ev_cfg["high_uncertainty_threshold_ratio"]),
        "high_uncertainty_optimality_ratio": float(ev_cfg["high_uncertainty_optimality_ratio"]),
        "high_uncertainty_secondary_ratio": float(ev_cfg["high_uncertainty_secondary_ratio"]),
    }

    pair_cases: list[dict[str, Any]] = []
    for ord_i in range(7):
        pair_cases.append(
            {
                "case_id": f"wb2_pair_b{ord_i}_to_b{ord_i + 1}",
                "mode": "scaled",
                "prev_scenarios": nodes_by_ord[ord_i],
                "curr_scenarios": nodes_by_ord[ord_i + 1],
                "weights": weights_vec,
                "threshold_coefficient": 1.0,
                "prev_dispersion": disp_by_ord[ord_i],
                "curr_dispersion": disp_by_ord[ord_i + 1],
                "config": runner_cfg,
            }
        )
    pair_input_payload = {
        "schema_version": "m3.pair-batch.v1",
        "cases": pair_cases,
    }

    chain_batches: list[dict[str, Any]] = []
    for row in raw_member_rows:
        ord_i = int(row["ordinal"])
        members_list = [
            {
                "member_index": idx,
                "member_id": str(m["member_id"]),
                "features": [float(x) for x in m["features"]],
            }
            for idx, m in enumerate(row["members"])
        ]
        chain_batches.append(
            {
                "ordinal": ord_i,
                "status": "ok",
                "members": members_list,
            }
        )
    chain_input_payload = {
        "mode": "chain",
        "tracking_run_key": run_key,
        "threshold_coefficient": 1.0,
        "weights": weights_vec,
        "config": runner_cfg,
        "cluster_config": sc_cfg,
        "batches": chain_batches,
    }

    pair_in_path = staging_dir / "runner_pair_input.json"
    chain_in_path = staging_dir / "runner_chain_input.json"
    atomic_write_json(pair_in_path, pair_input_payload)
    atomic_write_json(chain_in_path, chain_input_payload)

    # Run m3_runner on the host to establish host standalone outputs
    host_pair_out = baseline_dir / "host_runner_pair.json"
    host_chain_out = baseline_dir / "host_runner_chain.json"
    proc_p = subprocess.run(
        [str(runner_path), "--mode", "pair", "--input", str(pair_in_path), "--output", str(host_pair_out)],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc_p.returncode != 0:
        raise RuntimeError(f"Host m3_runner --mode pair failed: {proc_p.stderr}")

    proc_c = subprocess.run(
        [str(runner_path), "--mode", "chain", "--input", str(chain_in_path), "--output", str(host_chain_out)],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc_c.returncode != 0:
        raise RuntimeError(f"Host m3_runner --mode chain failed: {proc_c.stderr}")

    host_chain_data = load_json(host_chain_out)
    if host_chain_data.get("chain_summary") != host_11tables["evolution_runs"][0]["chain_summary"]:
        raise RuntimeError("Host m3_runner chain_summary disagreed with host SQL 11-table replay")

    # 3. Stage container SQL scripts (`replay_11table.sql` & `recovery_session.sql`)
    ext_guard_sql = textwrap.dedent(
        f"""
        SET extension_directory = '/results/extensions';
        SET autoinstall_known_extensions = false;
        SET autoload_known_extensions = false;
        SET TimeZone = 'UTC';
        LOAD '/runtime/opt/duckomo.duckdb_extension';
        LOAD '/runtime/opt/ensabc_m0_extension.duckdb_extension';

        CREATE OR REPLACE TEMP TABLE m3_container_ext_guard AS
        SELECT CASE
            WHEN (
                SELECT count(*)
                FROM duckdb_extensions()
                WHERE extension_name = 'duckomo'
                  AND loaded
                  AND extension_version = {sql_value(target_release['extension_version'])}
                  AND install_mode = 'NOT_INSTALLED'
            ) = 1
            AND (
                SELECT count(*)
                FROM duckdb_extensions()
                WHERE extension_name = 'ensabc_m0_extension'
                  AND loaded
                  AND install_mode = 'NOT_INSTALLED'
            ) = 1
            AND (
                SELECT count(*)
                FROM duckdb_extensions()
                WHERE loaded AND install_mode = 'NOT_INSTALLED'
            ) = 2
            THEN true
            ELSE error('EXTENSION_GUARD_FAILED: unexpected extension state in minimal container')
        END AS ok;
        """
    )

    container_vars = _session_vars_sql(
        om_dir_str="/runtime/om",
        om_hashes=om_hashes,
        weights_vec=weights_vec,
        scenario_cfg=sc_cfg,
        evolve_cfg=ev_cfg,
        execution_binding=binding["tracking_binding"],
        run_key=run_key,
        evolution_binding=binding["evolution_binding"],
        evolution_run_id=evolution_run_id,
    )

    replay_11table_sql = (
        ext_guard_sql
        + "\n"
        + container_vars
        + "\n.read validation/m3/sql/schema.sql\n"
        + ".read validation/m3/sql/real_8run_evolution.sql\n"
        + _export_11tables_sql(run_key, evolution_run_id)
    )
    atomic_write_text(staging_dir / "replay_11table.sql", replay_11table_sql)

    # Build same-session error recovery script:
    # (a) invalid ens_evolve_scenarios calls (INVALID_FRACTION_SUM + BACKBONE_INCONSISTENCY)
    # (b) mid-transaction inject_m3_failure = 'after_edges' -> ROLLBACK -> verify 0 residual rows
    # (c) subsequent valid 8-batch 11-table publish in the exact same DuckDB session -> RECOVERED
    fault_run_key = f"{run_key[:48]}_fault_rollback"
    fault_evolution_run_id = f"{evolution_run_id[:48]}_fault_rollback"
    fault_exec_binding = dict(binding["tracking_binding"])
    fault_exec_binding["run_id"] = fault_run_key
    fault_evol_binding = dict(binding["evolution_binding"])
    fault_evol_binding["tracking_run_key"] = fault_run_key

    fault_vars = _session_vars_sql(
        om_dir_str="/runtime/om",
        om_hashes=om_hashes,
        weights_vec=weights_vec,
        scenario_cfg=sc_cfg,
        evolve_cfg=ev_cfg,
        execution_binding=fault_exec_binding,
        run_key=fault_run_key,
        evolution_binding=fault_evol_binding,
        evolution_run_id=fault_evolution_run_id,
        inject_m3_failure="after_edges",
    )

    # Inline real_8run_evolution.sql + stage_and_publish.sql with `.bail off` so DuckDB CLI
    # stays in the same session across intentional errors (SQLite/DuckDB shell exits immediately
    # if `.bail on` is enabled when `errCnt > 0`). Slice step (b) right after `m3_fault_after_edges`
    # so `ROLLBACK;` rolls back the live mid-transaction state directly before `COMMIT;`.
    schema_sql_text = (SQL_DIR / "schema.sql").read_text(encoding="utf-8")
    stage_pub_no_bail_full = (
        (SQL_DIR / "stage_and_publish.sql")
        .read_text(encoding="utf-8")
        .replace(".read validation/m3/sql/schema.sql", schema_sql_text)
        .replace("\n.bail on\n", "\n.bail off\n")
    )
    after_edges_marker = "END AS m3_fault_after_edges;"
    if after_edges_marker not in stage_pub_no_bail_full:
        raise RuntimeError(f"Expected fault hook marker missing: {after_edges_marker}")
    stage_pub_until_fault = (
        stage_pub_no_bail_full.split(after_edges_marker, 1)[0] + after_edges_marker + "\n"
    )
    real_sql_raw = (SQL_DIR / "real_8run_evolution.sql").read_text(encoding="utf-8")
    real_before_pub = real_sql_raw.split(".read validation/m3/sql/stage_and_publish.sql", 1)[0]
    real_fault_inlined = real_before_pub + stage_pub_until_fault
    real_valid_inlined = real_sql_raw.replace(
        ".read validation/m3/sql/stage_and_publish.sql",
        stage_pub_no_bail_full,
    )

    fk_sql = sql_value(fault_run_key)
    fe_sql = sql_value(fault_evolution_run_id)
    rk_sql = sql_value(run_key)
    erid_sql = sql_value(evolution_run_id)

    recovery_session_sql = (
        ext_guard_sql.replace("'/results/extensions'", "'/results/extensions_rec'")
        + "\n.read validation/m3/sql/schema.sql\n"
        + ".bail off\n"
        + "SELECT ens_fsum([1e16, 4.0, -1e16]::DOUBLE[]) AS pre_fsum;\n"
        + textwrap.dedent(
            """
            -- (a.1) Invalid ens_evolve_scenarios call: fraction sum violates tolerance
            WITH bad_frac AS (
                SELECT ens_match_scenarios(
                    [{'cluster_id': 0, 'mean_features': [0.0, 0.0]}, {'cluster_id': 1, 'mean_features': [4.0, 4.0]}],
                    [{'cluster_id': 0, 'mean_features': [0.2, 0.2]}, {'cluster_id': 1, 'mean_features': [4.2, 4.2]}],
                    [1.0, 1.0],
                    {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
                ) AS bm
            )
            SELECT ens_evolve_scenarios(
                [{'cluster_id': 0::BIGINT, 'member_count': 35::BIGINT, 'member_fraction': 0.90::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [4.0, 4.0]::DOUBLE[]}],
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [0.2, 0.2]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [4.2, 4.2]::DOUBLE[]}],
                [1.0, 1.0]::DOUBLE[],
                bm,
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9, 'high_uncertainty_threshold_ratio': 0.1,
                 'high_uncertainty_optimality_ratio': 0.1, 'high_uncertainty_secondary_ratio': 0.1}
            ) FROM bad_frac;

            -- (a.2) Invalid ens_evolve_scenarios call: tampered backbone links
            WITH bad_bb AS (
                SELECT ens_match_scenarios(
                    [{'cluster_id': 0, 'mean_features': [0.0, 0.0]}, {'cluster_id': 1, 'mean_features': [4.0, 4.0]}],
                    [{'cluster_id': 0, 'mean_features': [0.2, 0.2]}, {'cluster_id': 1, 'mean_features': [4.2, 4.2]}],
                    [1.0, 1.0],
                    {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
                ) AS m
            )
            SELECT ens_evolve_scenarios(
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [4.0, 4.0]::DOUBLE[]}],
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [0.2, 0.2]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [4.2, 4.2]::DOUBLE[]}],
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
            ) FROM bad_bb;
            """
        )
        + "\n-- (b) Mid-transaction inject_m3_failure = 'after_edges' -> ROLLBACK -> verify 0 residual rows\n"
        + fault_vars
        + "\n"
        + real_fault_inlined
        + "\nROLLBACK;\n"
        + textwrap.dedent(
            f"""
            SELECT
                (SELECT count(*) FROM tracking_runs WHERE run_key = {fk_sql}) AS m2_tracking_runs,
                (SELECT count(*) FROM replay_batches WHERE run_key = {fk_sql}) AS m2_replay_batches,
                (SELECT count(*) FROM scenario_nodes WHERE run_key = {fk_sql}) AS m2_scenario_nodes,
                (SELECT count(*) FROM pair_comparisons WHERE run_key = {fk_sql}) AS m2_pair_comparisons,
                (SELECT count(*) FROM scenario_links WHERE run_key = {fk_sql}) AS m2_scenario_links,
                (SELECT count(*) FROM evolution_runs WHERE evolution_run_id = {fe_sql}) AS evolution_runs,
                (SELECT count(*) FROM evolution_nodes WHERE evolution_run_id = {fe_sql}) AS evolution_nodes,
                (SELECT count(*) FROM evolution_transitions WHERE evolution_run_id = {fe_sql}) AS evolution_transitions,
                (SELECT count(*) FROM evolution_edges WHERE evolution_run_id = {fe_sql}) AS evolution_edges,
                (SELECT count(*) FROM evolution_events WHERE evolution_run_id = {fe_sql}) AS evolution_events,
                (SELECT count(*) FROM evolution_branch_summaries WHERE evolution_run_id = {fe_sql}) AS evolution_branch_summaries;

            SELECT CASE
                WHEN (SELECT count(*) FROM tracking_runs WHERE run_key = {fk_sql}) = 0
                 AND (SELECT count(*) FROM replay_batches WHERE run_key = {fk_sql}) = 0
                 AND (SELECT count(*) FROM scenario_nodes WHERE run_key = {fk_sql}) = 0
                 AND (SELECT count(*) FROM pair_comparisons WHERE run_key = {fk_sql}) = 0
                 AND (SELECT count(*) FROM scenario_links WHERE run_key = {fk_sql}) = 0
                 AND (SELECT count(*) FROM evolution_runs WHERE evolution_run_id = {fe_sql}) = 0
                 AND (SELECT count(*) FROM evolution_nodes WHERE evolution_run_id = {fe_sql}) = 0
                 AND (SELECT count(*) FROM evolution_transitions WHERE evolution_run_id = {fe_sql}) = 0
                 AND (SELECT count(*) FROM evolution_edges WHERE evolution_run_id = {fe_sql}) = 0
                 AND (SELECT count(*) FROM evolution_events WHERE evolution_run_id = {fe_sql}) = 0
                 AND (SELECT count(*) FROM evolution_branch_summaries WHERE evolution_run_id = {fe_sql}) = 0
                THEN 'ZERO_RESIDUAL_OK'
                ELSE error('NON_ZERO_RESIDUAL_AFTER_ROLLBACK')
            END AS rollback_guard;
            """
        )
        + "\n-- (c) Subsequent valid 8-batch 11-table publish in the same DuckDB session\n"
        + container_vars
        + "\n"
        + real_valid_inlined
        + "\n"
        + _export_11tables_sql(run_key, evolution_run_id)
        + textwrap.dedent(
            f"""
            SELECT CASE
                WHEN (SELECT count(*) FROM evolution_runs WHERE evolution_run_id = {fe_sql}) = 0
                 AND (SELECT count(*) FROM evolution_nodes WHERE evolution_run_id = {erid_sql}) = 24
                 AND (SELECT count(*) FROM evolution_edges WHERE evolution_run_id = {erid_sql}) = 19
                 AND (SELECT count(*) FROM scenario_nodes WHERE run_key = {rk_sql}) = 24
                THEN 'RECOVERED'
                ELSE error('SAME_SESSION_RECOVERY_FAILED')
            END AS recovery_status;
            """
        )
    )
    atomic_write_text(staging_dir / "recovery_session.sql", recovery_session_sql)

    atomic_write_json(baseline_dir / "host_11tables.json", host_11tables)
    atomic_write_json(
        baseline_dir / "host_metadata.json",
        {
            "evolution_run_id": evolution_run_id,
            "tracking_run_key": run_key,
            "fault_evolution_run_id": fault_evolution_run_id,
            "fault_run_key": fault_run_key,
            "duckomo_version": target_release["extension_version"],
            "duckomo_sha256": actual_duckomo_sha,
            "extension_sha256": sha256_file(extension_path),
            "runner_sha256": sha256_file(runner_path),
        },
    )


def _deep_diff(actual: Any, expected: Any, path: str, diffs: list[str], *, limit: int = 20) -> None:
    if len(diffs) >= limit:
        return
    if expected is None or actual is None:
        if actual is not expected:
            diffs.append(f"{path}: expected {expected!r}, got {actual!r}")
        return
    if isinstance(expected, bool) or isinstance(actual, bool):
        if type(actual) is not bool or actual != expected:
            diffs.append(f"{path}: bool mismatch expected={expected!r}, got={actual!r}")
        return
    if isinstance(expected, int):
        if type(actual) is not int or actual != expected:
            diffs.append(f"{path}: int mismatch expected={expected!r}, got={actual!r}")
        return
    if isinstance(expected, float):
        if not is_finite_number(actual) or not math.isclose(float(actual), expected, rel_tol=0.0, abs_tol=0.0):
            # Allow 0.0 == 0.0 exact IEEE-754 parity; report even 1-ULP drift if any!
            if not (is_finite_number(actual) and float(actual) == expected):
                diffs.append(f"{path}: float mismatch expected={expected!r}, got={actual!r}")
        return
    if isinstance(expected, str):
        if not isinstance(actual, str) or actual != expected:
            diffs.append(f"{path}: str mismatch expected={expected!r}, got={actual!r}")
        return
    if isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            diffs.append(
                f"{path}: list length mismatch expected={len(expected)}, "
                f"got={len(actual) if isinstance(actual, list) else type(actual).__name__}"
            )
            return
        for idx, (av, ev) in enumerate(zip(actual, expected)):
            _deep_diff(av, ev, f"{path}[{idx}]", diffs, limit=limit)
        return
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            diffs.append(f"{path}: expected dict, got {type(actual).__name__}")
            return
        exp_k = set(expected.keys())
        act_k = set(actual.keys())
        for mk in sorted(exp_k - act_k):
            diffs.append(f"{path}: missing key {mk!r}")
        for ek in sorted(act_k - exp_k):
            diffs.append(f"{path}: extra key {ek!r}")
        for k in sorted(exp_k & act_k):
            _deep_diff(actual[k], expected[k], f"{path}.{k}" if path else k, diffs, limit=limit)
        return
    if actual != expected:
        diffs.append(f"{path}: mismatch expected={expected!r}, got={actual!r}")


def verify(
    *,
    run_dir: Path,
    baseline_dir: Path,
    results_dir: Path,
    container_log: Path,
    container_exit: int,
    image: str,
) -> int:
    """Compare container outputs against host baseline and write `<run_dir>/minimal-runtime.json`."""
    run_dir.mkdir(parents=True, exist_ok=True)
    meta = load_json(baseline_dir / "host_metadata.json")
    host_11tables = load_json(baseline_dir / "host_11tables.json")
    host_pair = normalize_floats(load_json(baseline_dir / "host_runner_pair.json"))
    host_chain = normalize_floats(load_json(baseline_dir / "host_runner_chain.json"))

    log_text = container_log.read_text(encoding="utf-8") if container_log.is_file() else ""
    reasons: list[str] = []

    if container_exit != 0:
        reasons.append(f"Container exited with non-zero code {container_exit}")

    # 1. Verify --network none inside container (0 non-header /proc/net/route entries)
    network_isolated = "NETWORK_NONE_NON_LOOPBACK_ROUTES: 0" in log_text
    if not network_isolated:
        reasons.append("Container network isolation check failed (expected 0 routes in /proc/net/route)")

    # 2. Verify all 8 prohibited tools are ABSENT
    absent_verified: list[str] = []
    present_tools: list[str] = []
    for tool in PROHIBITED_TOOLS:
        if f"ABSENT: {tool}" in log_text and f"PRESENT: {tool}" not in log_text:
            absent_verified.append(tool)
        else:
            present_tools.append(tool)
            reasons.append(f"Prohibited tool '{tool}' was not verified ABSENT in container")

    # 3. Verify standalone m3_runner (--mode pair AND --mode chain) 100% field-by-field parity
    pair_parity = False
    chain_parity = False
    cont_pair_path = results_dir / "runner_pair_output.json"
    cont_chain_path = results_dir / "runner_chain_output.json"

    if cont_pair_path.is_file():
        cont_pair = normalize_floats(load_json(cont_pair_path))
        p_diffs: list[str] = []
        _deep_diff(cont_pair, host_pair, "runner_pair", p_diffs)
        if p_diffs:
            reasons.append(f"Standalone m3_runner --mode pair parity mismatch: {p_diffs[:3]}")
        else:
            pair_parity = True
    else:
        reasons.append("Container runner_pair_output.json missing")

    if cont_chain_path.is_file():
        cont_chain = normalize_floats(load_json(cont_chain_path))
        c_diffs: list[str] = []
        _deep_diff(cont_chain, host_chain, "runner_chain", c_diffs)
        if c_diffs:
            reasons.append(f"Standalone m3_runner --mode chain parity mismatch: {c_diffs[:3]}")
        else:
            chain_parity = True
    else:
        reasons.append("Container runner_chain_output.json missing")

    standalone_runner_parity = pair_parity and chain_parity

    # 4. Verify 11-table DuckDB replay 100% field-by-field parity + authentic branch_id prefix
    sql_11table_parity = False
    table_counts: dict[str, int] = {}
    sql_out_path = results_dir / "sql_11table_output.json"
    if sql_out_path.is_file():
        sql_parsed = parse_duckdb_json(sql_out_path.read_text(encoding="utf-8"))
        if not sql_parsed or not sql_parsed[-1]:
            reasons.append("Container sql_11table_output.json contained no 11-table export row")
        else:
            cont_11tables = normalize_floats(sql_parsed[-1][0])
            s_diffs: list[str] = []
            _deep_diff(cont_11tables, host_11tables, "sql_11tables", s_diffs)
            if s_diffs:
                reasons.append(f"Container 11-table SQL replay parity mismatch: {s_diffs[:3]}")
            else:
                # Verify authentic tracking_run_key[:12]: prefix on every container branch_id
                for idx, n in enumerate(cont_11tables["evolution_nodes"]):
                    parse_branch_birth_key(
                        n["branch_id"],
                        expected_tracking_run_key=meta["tracking_run_key"],
                        location=f"evolution_nodes[{idx}].branch_id",
                    )
                table_counts = {tbl: len(cont_11tables[tbl]) for tbl in ALL_11_TABLES}
                sql_11table_parity = True
    else:
        reasons.append("Container sql_11table_output.json missing")

    # 5. Verify same-session error recovery + 0 residual rows across all 6 M3 & 5 M2 tables
    same_session_error_recovery = False
    zero_residual_after_rollback = False
    m3_residual_counts: dict[str, int] = {}
    m2_residual_counts: dict[str, int] = {}
    rec_out_path = results_dir / "recovery_output.json"
    rec_err_path = results_dir / "recovery_errors.txt"

    if rec_out_path.is_file() and rec_err_path.is_file():
        rec_err = rec_err_path.read_text(encoding="utf-8")
        rec_parsed = parse_duckdb_json(rec_out_path.read_text(encoding="utf-8"))

        missing_errs = [
            tok
            for tok in ("[INVALID_FRACTION_SUM]", "[BACKBONE_INCONSISTENCY]", "INJECTED_M3_FAILURE: after_edges")
            if tok not in rec_err
        ]
        if missing_errs:
            reasons.append(f"Same-session recovery stderr missing expected error tokens: {missing_errs}")

        # Expected stdout items in recovery_output.json:
        # [0]: [{"pre_fsum": 4.0}]
        # ... after ROLLBACK & .bail on:
        # [-4]: [<residual counts across 11 tables>]
        # [-3]: [{"rollback_guard": "ZERO_RESIDUAL_OK"}]
        # [-2]: [<9-column real_8run_evolution summary>] -> wait: real_8run_evolution emits 1 SELECT, then _export_11tables_sql emits 1 SELECT, then recovery_status emits 1 SELECT!
        # Let's locate residual counts, 11-table bundle, and recovery_status deterministically:
        residual_row: dict[str, Any] | None = None
        recovered_11tables: dict[str, Any] | None = None
        recovered_marker = False
        pre_fsum_ok = False

        for block in rec_parsed:
            if not isinstance(block, list) or len(block) != 1 or not isinstance(block[0], dict):
                continue
            row0 = block[0]
            if row0.get("pre_fsum") == 4.0:
                pre_fsum_ok = True
            if "m2_tracking_runs" in row0 and "evolution_branch_summaries" in row0 and len(row0) == 11:
                residual_row = row0
            if all(tbl in row0 for tbl in ALL_11_TABLES) and isinstance(row0.get("evolution_nodes"), list):
                recovered_11tables = normalize_floats(row0)
            if row0.get("recovery_status") == "RECOVERED":
                recovered_marker = True

        if residual_row is not None:
            m3_residual_counts = {tbl: int(residual_row[tbl]) for tbl in M3_TABLE_NAMES}
            m2_residual_counts = {
                tbl: int(residual_row[f"m2_{tbl}"]) for tbl in M2_TABLE_NAMES
            }
            if all(v == 0 for v in m3_residual_counts.values()) and all(
                v == 0 for v in m2_residual_counts.values()
            ):
                zero_residual_after_rollback = True
            else:
                reasons.append(
                    f"Non-zero residual rows after ROLLBACK: m3={m3_residual_counts}, m2={m2_residual_counts}"
                )
        else:
            reasons.append("Recovery output missing post-ROLLBACK 11-table residual count row")

        rec_parity = False
        if recovered_11tables is not None:
            r_diffs: list[str] = []
            _deep_diff(recovered_11tables, host_11tables, "recovered_11tables", r_diffs)
            if r_diffs:
                reasons.append(f"Recovered same-session 11-table replay differed from host: {r_diffs[:3]}")
            else:
                rec_parity = True
        else:
            reasons.append("Recovery output missing post-recovery 11-table export")

        if pre_fsum_ok and not missing_errs and zero_residual_after_rollback and recovered_marker and rec_parity:
            same_session_error_recovery = True
        else:
            if not pre_fsum_ok:
                reasons.append("Recovery session pre_fsum check missing")
            if not recovered_marker:
                reasons.append("Recovery session did not reach final RECOVERED marker")
    else:
        reasons.append("Container recovery_output.json or recovery_errors.txt missing")

    all_passed = (
        not reasons
        and container_exit == 0
        and network_isolated
        and len(absent_verified) == len(PROHIBITED_TOOLS)
        and standalone_runner_parity
        and sql_11table_parity
        and same_session_error_recovery
        and zero_residual_after_rollback
    )

    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "PASS" if all_passed else "FAIL",
        "checked_at_utc": _utc_now(),
        "method": "docker_isolated_container",
        "image": image,
        "network_mode": "none",
        "network_zero_routes_verified": network_isolated,
        "prohibited_tools_checked": list(PROHIBITED_TOOLS),
        "absent_tools_verified": absent_verified,
        "reference_tools_present": present_tools,
        "all_prohibited_tools_absent": len(present_tools) == 0 and len(absent_verified) == len(PROHIBITED_TOOLS),
        "standalone_runner_parity": standalone_runner_parity,
        "standalone_runner": {
            "pair_mode_cases_verified": len(host_pair.get("results", [])),
            "pair_mode_parity": pair_parity,
            "chain_mode_batches_verified": len(host_chain.get("batches", [])),
            "chain_mode_nodes_verified": len(host_chain.get("nodes", [])),
            "chain_mode_edges_verified": len(host_chain.get("edges", [])),
            "chain_mode_parity": chain_parity,
        },
        "sql_11table_replay_parity": sql_11table_parity,
        "sql_11table_counts": table_counts,
        "same_session_error_recovery": same_session_error_recovery,
        "zero_residual_after_rollback": zero_residual_after_rollback,
        "error_recovery_details": {
            "invalid_evolve_calls_rejected": ["INVALID_FRACTION_SUM", "BACKBONE_INCONSISTENCY"],
            "injected_failure_hook": "after_edges",
            "m3_residual_rows_after_rollback": m3_residual_counts,
            "m2_residual_rows_after_rollback": m2_residual_counts,
            "subsequent_publish_status": "RECOVERED" if same_session_error_recovery else "FAILED",
        },
        "evolution_run_id": meta["evolution_run_id"],
        "tracking_run_key": meta["tracking_run_key"],
        "content_sha256": host_11tables["evolution_runs"][0]["content_sha256"],
        "software_sha256s": {
            "duckomo_version": meta["duckomo_version"],
            "duckomo_sha256": meta["duckomo_sha256"],
            "ensabc_extension_sha256": meta["extension_sha256"],
            "m3_runner_sha256": meta["runner_sha256"],
        },
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
        "reasons": reasons,
    }

    out_path = run_dir / "minimal-runtime.json"
    atomic_write_json(out_path, report)
    if not all_passed:
        for r in reasons:
            print(f"  [FAIL] {r}", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Host helper for M3 airgapped minimal runtime verification.")
    sub = parser.add_subparsers(dest="subcommand", required=True)

    p_prep = sub.add_parser("prepare", help="Prepare host baseline and container inputs/SQL")
    p_prep.add_argument("--staging-dir", type=Path, required=True)
    p_prep.add_argument("--baseline-dir", type=Path, required=True)
    p_prep.add_argument("--sample-dir", type=Path, required=True)
    p_prep.add_argument("--extension", type=Path, required=True)
    p_prep.add_argument("--duckomo", type=Path, required=True)
    p_prep.add_argument("--runner", type=Path, required=True)

    p_ver = sub.add_parser("verify", help="Verify container outputs against host baseline")
    p_ver.add_argument("--run-dir", type=Path, required=True)
    p_ver.add_argument("--baseline-dir", type=Path, required=True)
    p_ver.add_argument("--results-dir", type=Path, required=True)
    p_ver.add_argument("--container-log", type=Path, required=True)
    p_ver.add_argument("--container-exit", type=int, required=True)
    p_ver.add_argument("--image", type=str, required=True)

    args = parser.parse_args(argv)
    if args.subcommand == "prepare":
        prepare(
            staging_dir=args.staging_dir.resolve(),
            baseline_dir=args.baseline_dir.resolve(),
            sample_dir=args.sample_dir.resolve(),
            extension_path=args.extension.resolve(),
            duckomo_path=args.duckomo.resolve(),
            runner_path=args.runner.resolve(),
        )
        return 0
    return verify(
        run_dir=args.run_dir.resolve(),
        baseline_dir=args.baseline_dir.resolve(),
        results_dir=args.results_dir.resolve(),
        container_log=args.container_log.resolve(),
        container_exit=args.container_exit,
        image=args.image,
    )


if __name__ == "__main__":
    sys.exit(main())
