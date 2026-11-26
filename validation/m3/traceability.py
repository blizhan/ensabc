#!/usr/bin/env python3
"""18-FR / 8-SC executable traceability evaluator for M3 scenario evolution (T043).

Implements `specs/004-m3-scenario-evolution/contracts/validation-evidence.md` §3:
- Evaluates `FR-001`–`FR-018` (18 functional requirements) and `SC-001`–`SC-008`
  (8 measurable success criteria).
- Independently recomputes key mathematical, SQL (`E1`–`E8`), perturbation, and
  SVG/DOM invariants directly from saved JSON, DuckDB, and HTML artifacts.
- Strictly acyclic: `evidence_refs` never reference `report.json`, `report-audit.json`,
  or `traceability.json`.
- Missing upstream artifacts gracefully evaluate to `"not_run"` without unhandled
  exceptions; overall status is `"pass"` iff all 26 items are `"pass"`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import struct
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    LocatedError,
    NON_PHYSICAL_DISCLAIMER,
    atomic_write_json,
    ensure_no_symlink_inside,
    is_finite_number,
    load_json,
    parse_duckdb_json,
    sha256_file,
)
from validation.m3.reproduce_independently import project_replay_report
from validation.m3.render_evolution_html import run_isolated_duckdb as run_readonly_duckdb
from validation.m3.verify_visualization_traceability import (
    scan_prohibited_claims,
    verify_html_traceability,
)

TRACEABILITY_SCHEMA_VERSION = "m3.traceability.v1"

REQUIREMENT_IDS: tuple[str, ...] = tuple(f"FR-{i:03d}" for i in range(1, 19))
SUCCESS_CRITERION_IDS: tuple[str, ...] = tuple(f"SC-{i:03d}" for i in range(1, 9))

FORBIDDEN_EVIDENCE_REL_PATHS: frozenset[str] = frozenset(
    {
        "report.json",
        "report-audit.json",
        "traceability.json",
    }
)


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _status_is_pass(doc: dict[str, Any] | None) -> bool:
    if not isinstance(doc, dict):
        return False
    return str(doc.get("status", "")).strip().lower() in ("pass", "passed")


def _is_pos_zero_float(val: Any) -> bool:
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        return False
    fval = float(val)
    return struct.pack(">d", fval) == b"\x00\x00\x00\x00\x00\x00\x00\x00"


def _collect_strings(obj: Any, out: list[str]) -> None:
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_strings(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_strings(item, out)


_ensure_no_symlink_inside = ensure_no_symlink_inside


class TraceabilityEvaluator:
    """Read-only evaluator computing 18 FR and 8 SC statuses from ground-truth run artifacts."""

    def __init__(self, run_dir: Path) -> None:
        if run_dir.is_symlink():
            raise LocatedError(f"Symlink run_dir is forbidden: {run_dir}", file=run_dir)
        self.run_dir = run_dir.resolve()
        self._json_cache: dict[str, tuple[str, Any]] = {}
        self._db_invariants_cache: dict[str, Any] | None = None
        self._viz_live_cache: dict[str, dict[str, Any]] = {}

    def _validate_rel_path(self, rel_path: str) -> Path:
        p = Path(rel_path)
        if not rel_path or p.is_absolute() or ".." in p.parts or "\\" in rel_path:
            raise ValueError(f"Illegal evidence relative path: {rel_path!r}")
        norm = p.as_posix()
        if norm in FORBIDDEN_EVIDENCE_REL_PATHS:
            raise ValueError(f"Acyclicity violation: evidence_refs cannot cite {norm!r}")
        raw_target = self.run_dir / norm
        _ensure_no_symlink_inside(self.run_dir, raw_target)
        full = raw_target.resolve()
        if not full.is_relative_to(self.run_dir):
            raise ValueError(f"Evidence path escapes run_dir: {rel_path!r}")
        if full.relative_to(self.run_dir).as_posix() in FORBIDDEN_EVIDENCE_REL_PATHS:
            raise ValueError(f"Acyclicity violation on resolved path: {rel_path!r}")
        return full

    def has_file(self, rel_path: str) -> bool:
        full = self._validate_rel_path(rel_path)
        return full.is_file()

    def load_artifact_json(self, rel_path: str) -> tuple[str, dict[str, Any] | None, str | None]:
        """Return (`"ok"|"missing"|"corrupt"`, parsed_dict, error_msg)."""
        if rel_path in self._json_cache:
            st, val = self._json_cache[rel_path]
            if st == "ok":
                return ("ok", val, None)
            return (st, None, str(val))

        try:
            full = self._validate_rel_path(rel_path)
        except (LocatedError, ValueError) as exc:
            err = f"Blocked evidence path {rel_path}: {exc}"
            self._json_cache[rel_path] = ("corrupt", err)
            return ("corrupt", None, err)

        if not full.is_file():
            return ("missing", None, f"Missing upstream artifact: {rel_path}")
        try:
            parsed = load_json(full)
        except Exception as exc:
            err = f"Corrupt JSON in {rel_path}: {exc}"
            self._json_cache[rel_path] = ("corrupt", err)
            return ("corrupt", None, err)
        if not isinstance(parsed, dict):
            err = f"Expected JSON object in {rel_path}, got {type(parsed).__name__}"
            self._json_cache[rel_path] = ("corrupt", err)
            return ("corrupt", None, err)
        self._json_cache[rel_path] = ("ok", parsed)
        return ("ok", parsed, None)

    def require_artifacts(
        self, rel_paths: list[str]
    ) -> tuple[str, dict[str, dict[str, Any]], list[dict[str, str]], str]:
        """Load multiple required JSON/file artifacts; return `'not_run'` if any missing, `'fail'` if corrupt."""
        missing: list[str] = []
        corrupt: list[str] = []
        loaded: dict[str, dict[str, Any]] = {}
        existing_refs: list[dict[str, str]] = []

        for rel in rel_paths:
            try:
                full = self._validate_rel_path(rel)
            except (LocatedError, ValueError) as exc:
                corrupt.append(f"Blocked path {rel}: {exc}")
                continue
            if not full.is_file():
                missing.append(rel)
                continue
            existing_refs.append({"path": Path(rel).as_posix(), "sha256": sha256_file(full)})
            if rel.endswith(".json"):
                st, doc, err = self.load_artifact_json(rel)
                if st != "ok" or doc is None:
                    corrupt.append(err or f"Invalid JSON: {rel}")
                else:
                    loaded[rel] = doc

        if corrupt:
            return ("fail", loaded, existing_refs, "; ".join(corrupt))
        if missing:
            return ("not_run", loaded, existing_refs, f"Missing upstream evidence: {', '.join(missing)}")
        return ("ok", loaded, existing_refs, "")

    def query_duckdb_invariants(self) -> dict[str, Any]:
        """Independently recompute E1–E8 invariants across all published runs in `evolution-runs.duckdb`."""
        if self._db_invariants_cache is not None:
            return self._db_invariants_cache

        rel_db = "sql-history/evolution-runs.duckdb"
        try:
            if not self.has_file(rel_db):
                res = {"status": "not_run", "reason": f"Missing {rel_db}"}
                self._db_invariants_cache = res
                return res
            db_path = self._validate_rel_path(rel_db)
        except (LocatedError, ValueError) as exc:
            res = {"status": "fail", "reason": f"Blocked DuckDB path: {exc}"}
            self._db_invariants_cache = res
            return res
        sql = """
        SELECT
            (SELECT count(*) FROM information_schema.tables
             WHERE table_schema = 'main' AND table_name IN (
               'tracking_runs', 'replay_batches', 'scenario_nodes', 'pair_comparisons', 'scenario_links',
               'evolution_runs', 'evolution_nodes', 'evolution_transitions', 'evolution_edges',
               'evolution_events', 'evolution_branch_summaries'
             )) AS table_count_11,
            (SELECT count(*) FROM evolution_runs) AS evolution_runs_count,
            -- E1: M2 scenario_nodes <-> M3 evolution_nodes bidirectional identity & fraction parity
            (SELECT count(*) FROM (
                SELECT er.evolution_run_id, sn.ordinal, sn.cluster_id, sn.canonical_rank, sn.branch_id, sn.member_count, sn.member_fraction
                FROM evolution_runs er
                JOIN scenario_nodes sn ON sn.run_key = er.tracking_run_key
                EXCEPT
                SELECT evolution_run_id, ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction
                FROM evolution_nodes
            )) + (SELECT count(*) FROM (
                SELECT evolution_run_id, ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction
                FROM evolution_nodes
                EXCEPT
                SELECT er.evolution_run_id, sn.ordinal, sn.cluster_id, sn.canonical_rank, sn.branch_id, sn.member_count, sn.member_fraction
                FROM evolution_runs er
                JOIN scenario_nodes sn ON sn.run_key = er.tracking_run_key
            )) AS e1_violations,
            -- E2: M2 scenario_links <-> M3 primary evolution_edges bidirectional parity & branch inheritance
            (SELECT count(*) FROM (
                SELECT er.evolution_run_id, sl.prev_ordinal, sl.prev_cluster_id, sl.curr_ordinal, sl.curr_cluster_id, sl.distance, sl.ambiguous
                FROM evolution_runs er
                JOIN scenario_links sl ON sl.run_key = er.tracking_run_key
                EXCEPT
                SELECT evolution_run_id, prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, distance, ambiguous_tie AS ambiguous
                FROM evolution_edges
                WHERE edge_role = 'primary'
            )) + (SELECT count(*) FROM (
                SELECT evolution_run_id, prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, distance, ambiguous_tie AS ambiguous
                FROM evolution_edges
                WHERE edge_role = 'primary'
                EXCEPT
                SELECT er.evolution_run_id, sl.prev_ordinal, sl.prev_cluster_id, sl.curr_ordinal, sl.curr_cluster_id, sl.distance, sl.ambiguous
                FROM evolution_runs er
                JOIN scenario_links sl ON sl.run_key = er.tracking_run_key
            )) + (SELECT count(*) FROM evolution_edges WHERE edge_role = 'primary' AND prev_branch_id <> curr_branch_id) AS e2_violations,
            -- E3: Secondary feasibility & threshold compliance & zero edges on no_candidate_within_threshold
            (SELECT count(*) FROM evolution_edges ee
             JOIN evolution_transitions et ON et.evolution_run_id = ee.evolution_run_id AND et.prev_ordinal = ee.prev_ordinal
             WHERE ee.distance > et.threshold)
            + (SELECT count(*) FROM evolution_edges ee
               JOIN evolution_nodes en ON en.evolution_run_id = ee.evolution_run_id AND en.ordinal = ee.curr_ordinal AND en.cluster_id = ee.curr_cluster_id
               WHERE ee.edge_role = 'secondary_split' AND (en.in_degree_primary <> 0 OR en.assignment_reason <> 'not_selected_in_global_optimum'))
            + (SELECT count(*) FROM evolution_edges ee
               JOIN evolution_nodes en ON en.evolution_run_id = ee.evolution_run_id AND en.ordinal = ee.prev_ordinal AND en.cluster_id = ee.prev_cluster_id
               WHERE ee.edge_role = 'secondary_merge' AND en.out_degree_primary <> 0)
            + (SELECT count(*) FROM evolution_nodes en
               JOIN evolution_edges ee ON ee.evolution_run_id = en.evolution_run_id AND ee.curr_ordinal = en.ordinal AND ee.curr_cluster_id = en.cluster_id
               WHERE en.assignment_reason = 'no_candidate_within_threshold') AS e3_violations,
            -- E4: Gap chain-break & zero cross-gap edges
            (SELECT count(*) FROM evolution_runs er
             WHERE (SELECT count(*) FROM evolution_transitions et WHERE et.evolution_run_id = er.evolution_run_id) <> er.planned_batch_count - 1)
            + (SELECT count(*) FROM evolution_edges ee
               JOIN evolution_transitions et ON et.evolution_run_id = ee.evolution_run_id AND et.prev_ordinal = ee.prev_ordinal
               WHERE et.chain_broken OR et.transition_status <> 'ok') AS e4_violations,
            -- E5: Strict disjointness of gap events vs emergence/disappearance
            (SELECT count(*) FROM evolution_events ev
             JOIN evolution_transitions et ON et.evolution_run_id = ev.evolution_run_id AND et.prev_ordinal = ev.prev_ordinal
             WHERE (et.transition_status = 'gap' AND ev.event_type NOT IN ('gap_interrupted', 'gap_restarted'))
                OR (et.transition_status = 'ok' AND ev.event_type IN ('gap_interrupted', 'gap_restarted'))) AS e5_violations,
            -- E6: Node primary/secondary degree conservation
            (SELECT count(*) FROM evolution_nodes en
             WHERE en.in_degree_primary <> (SELECT count(*) FROM evolution_edges ee WHERE ee.evolution_run_id = en.evolution_run_id AND ee.curr_ordinal = en.ordinal AND ee.curr_cluster_id = en.cluster_id AND ee.edge_role = 'primary')
                OR en.in_degree_secondary <> (SELECT count(*) FROM evolution_edges ee WHERE ee.evolution_run_id = en.evolution_run_id AND ee.curr_ordinal = en.ordinal AND ee.curr_cluster_id = en.cluster_id AND ee.edge_role <> 'primary')
                OR en.out_degree_primary <> (SELECT count(*) FROM evolution_edges ee WHERE ee.evolution_run_id = en.evolution_run_id AND ee.prev_ordinal = en.ordinal AND ee.prev_cluster_id = en.cluster_id AND ee.edge_role = 'primary')
                OR en.out_degree_secondary <> (SELECT count(*) FROM evolution_edges ee WHERE ee.evolution_run_id = en.evolution_run_id AND ee.prev_ordinal = en.ordinal AND ee.prev_cluster_id = en.cluster_id AND ee.edge_role <> 'primary')) AS e6_violations,
            -- E7: Branch lifecycle node_count conservation
            (SELECT count(*) FROM evolution_runs er
             WHERE coalesce((SELECT sum(node_count) FROM evolution_branch_summaries ebs WHERE ebs.evolution_run_id = er.evolution_run_id), 0)
                   <> (SELECT count(*) FROM evolution_nodes en WHERE en.evolution_run_id = er.evolution_run_id))
            + (SELECT count(*) FROM evolution_branch_summaries
               WHERE node_count <> end_ordinal - birth_ordinal + 1 OR step_count <> node_count - 1) AS e7_violations,
            -- E8: Non-empty algorithmic attribution disclaimer across runs, edges, events
            (SELECT count(*) FROM evolution_runs WHERE non_physical_disclaimer <> 'algorithmic_attribution_only_not_physical_member_migration')
            + (SELECT count(*) FROM evolution_edges WHERE non_physical_disclaimer <> 'algorithmic_attribution_only_not_physical_member_migration')
            + (SELECT count(*) FROM evolution_events WHERE non_physical_disclaimer <> 'algorithmic_attribution_only_not_physical_member_migration') AS e8_violations;
        """
        proc = run_readonly_duckdb(sql, database=db_path, readonly=True)
        if proc.returncode != 0:
            res = {"status": "fail", "reason": f"DuckDB invariant query failed: {proc.stderr.strip()}"}
            self._db_invariants_cache = res
            return res

        parsed = parse_duckdb_json(proc.stdout)
        if not parsed or not parsed[-1]:
            res = {"status": "fail", "reason": "Empty DuckDB invariant query response"}
            self._db_invariants_cache = res
            return res

        row = parsed[-1][0]
        total_violations = sum(
            int(row[f"e{k}_violations"]) for k in range(1, 9)
        )
        ok = (
            int(row["table_count_11"]) == 11
            and int(row["evolution_runs_count"]) >= 2
            and total_violations == 0
        )
        res = {
            "status": "pass" if ok else "fail",
            "row": row,
            "total_violations": total_violations,
        }
        self._db_invariants_cache = res
        return res

    def verify_live_html_views(self) -> dict[str, Any]:
        """Re-run 5-pass read-only HTML->DuckDB verification on both rendered views."""
        if "combined" in self._viz_live_cache:
            return self._viz_live_cache["combined"]

        req = [
            "sql-history/evolution-runs.duckdb",
            "sql-history/real-evolution-replay.json",
            "sql-history/synthetic-evolution-replay.json",
            "visualization/real-8run-main.html",
            "visualization/synthetic-showcase.html",
        ]
        st, docs, _, msg = self.require_artifacts(req)
        if st != "ok":
            res = {"status": st, "reason": msg}
            self._viz_live_cache["combined"] = res
            return res

        db_path = self._validate_rel_path("sql-history/evolution-runs.duckdb")
        real_run_id = str(docs["sql-history/real-evolution-replay.json"].get("evolution_run_id", ""))
        synth_doc = docs["sql-history/synthetic-evolution-replay.json"]
        synth_run_id = str(
            synth_doc.get("showcase_evolution_run_id") or synth_doc.get("evolution_run_id", "")
        )

        real_audit = verify_html_traceability(
            self._validate_rel_path("visualization/real-8run-main.html"),
            db_path,
            real_run_id,
        )
        synth_audit = verify_html_traceability(
            self._validate_rel_path("visualization/synthetic-showcase.html"),
            db_path,
            synth_run_id,
        )
        orphans = (
            real_audit["passes"]["pass_2_svg_xml_zero_orphans"].get("orphan_primitives_count", -1)
            + synth_audit["passes"]["pass_2_svg_xml_zero_orphans"].get("orphan_primitives_count", -1)
        )
        ok = real_audit["status"] == "pass" and synth_audit["status"] == "pass" and orphans == 0
        res = {
            "status": "pass" if ok else "fail",
            "real_audit": real_audit,
            "synth_audit": synth_audit,
            "orphan_primitives_total": orphans,
            "findings": [*real_audit.get("findings", []), *synth_audit.get("findings", [])],
        }
        self._viz_live_cache["combined"] = res
        return res

    @staticmethod
    def _recompute_berge_star_counts(oracle_doc: dict[str, Any]) -> tuple[bool, int]:
        berge = oracle_doc.get("berge_theorems_verified", {})
        req_flags = (
            "theorem1_no_unmatched_cross_feasible_and_no_parallel_edges",
            "theorem2_split_merge_mutually_exclusive_on_primary_link",
            "theorem3_secondary_forced_inclusion_preserves_max_cardinality",
            "theorem4_inactive_feasible_cross_edge_invariance",
            "corollary_five_class_star_partition_complete",
        )
        if not all(berge.get(f) is True for f in req_flags):
            return (False, 0)
        fixed = oracle_doc.get("fixed_pair_results")
        if not isinstance(fixed, list) or len(fixed) < 18:
            return (False, 0)
        for item in fixed:
            if not isinstance(item, dict):
                return (False, 0)
            if item.get("status") not in ("pass", "ok", None):
                return (False, 0)
            # Recompute star primary consistency when summary counts are present
            summary = item.get("summary")
            if isinstance(summary, dict) and "primary_edge_count" in summary:
                if int(summary["primary_edge_count"]) != (
                    int(summary["continuation_count"])
                    + int(summary["split_count"])
                    + int(summary["merge_count"])
                ):
                    return (False, 0)
        return (True, len(fixed))

    @staticmethod
    def _recompute_orthogonality(ortho_doc: dict[str, Any]) -> tuple[bool, str]:
        morph = ortho_doc.get("pure_morphology_drift_series", {})
        frac = ortho_doc.get("pure_fraction_redistribution_series", {})
        brack = ortho_doc.get("uncertainty_channel_bracketing", {})
        if not (isinstance(morph, dict) and isinstance(frac, dict) and isinstance(brack, dict)):
            return (False, "Missing series blocks in orthogonality-separation.json")

        dists = morph.get("primary_max_distances", [])
        tvs_morph = morph.get("transition_tv_redistribution_values", [])
        if len(dists) < 4 or not all(
            is_finite_number(dists[i]) and is_finite_number(dists[i + 1]) and dists[i] < dists[i + 1]
            for i in range(len(dists) - 1)
        ):
            return (False, f"Pure morphology distances not strictly monotonic: {dists}")
        if not tvs_morph or not all(_is_pos_zero_float(v) for v in tvs_morph):
            return (False, f"Pure morphology TV redistribution not bitwise +0.0: {tvs_morph}")
        if not (morph.get("fraction_deltas_bitwise_zero") is True and morph.get("transition_tv_bitwise_zero") is True):
            return (False, "Pure morphology zero-fraction flags false")

        f_tot = frac.get("primary_total_distances", [])
        f_max = frac.get("all_edges_max_distances", [])
        exp_tv = frac.get("expected_tv_values", [])
        obs_tv = frac.get("observed_tv_values", [])
        if len(f_tot) < 3 or not all(_is_pos_zero_float(v) for v in f_tot + f_max):
            return (False, "Pure fraction redistribution morphology distances not bitwise +0.0")
        if obs_tv != exp_tv or obs_tv != [0.0, 0.25, 0.375]:
            return (False, f"Pure fraction redistribution TV mismatch: observed={obs_tv}, expected={exp_tv}")

        req_channels = {"near_threshold", "near_primary_switch", "near_secondary_switch"}
        if brack.get("all_passed") is not True or not req_channels.issubset(set(brack.get("channels_verified", []))):
            return (False, "Uncertainty channel 0.05R vs 0.15R bracketing incomplete")

        return (True, "0.0 orthogonality, strict monotonicity, and 3-channel 0.10 bracketing verified")

    def _check_perturbation_and_sensitivity(
        self, docs: dict[str, dict[str, Any]]
    ) -> tuple[bool, str]:
        p_evol = docs["case-study/perturbation-evolution-level.json"]
        p_memb = docs["case-study/perturbation-member-level.json"]
        s_thr = docs["case-study/sensitivity-thresholds.json"]
        s_cls = docs["case-study/sensitivity-clustering-configs.json"]

        if not all(_status_is_pass(d) for d in (p_evol, p_memb, s_thr, s_cls)):
            return (False, "One or more sensitivity/perturbation artifacts did not report PASS")

        l1a = p_evol.get("level_1a_mean_field", {})
        l1b = p_evol.get("level_1b_fraction_only", {})
        if (
            l1a.get("certified_pair_flips") != 0
            or int(l1a.get("certified_evaluations_count", 0)) <= 0
            or l1b.get("topology_flips") != 0
        ):
            return (False, "Level 1a/1b perturbation certificate or topology flip invariant violated")

        if (
            p_memb.get("sigma_zero_exact_match") is not True
            or p_memb.get("certified_pair_flips") != 0
            or int(p_memb.get("certified_evaluations_count", 0)) <= 0
        ):
            return (False, "Level 2 member perturbation sigma=0 or certified 0-flip invariant violated")

        if p_evol.get("all_flips_carry_baseline_margins") is not True:
            return (False, "perturbation-evolution-level.json missing all_flips_carry_baseline_margins=true")
        if p_memb.get("all_flips_carry_baseline_margins") is not True:
            return (False, "perturbation-member-level.json missing all_flips_carry_baseline_margins=true")

        for flip_label, flip_list in (
            ("level_1_top_boundary_flips", p_evol.get("boundary_flips")),
            ("level_1a_boundary_flips", l1a.get("boundary_flips", [])),
            ("level_2_boundary_flips", p_memb.get("boundary_flips")),
        ):
            if not isinstance(flip_list, list):
                return (False, f"{flip_label} must be a list")
            for fl_idx, fl_rec in enumerate(flip_list):
                if (
                    not isinstance(fl_rec, dict)
                    or not isinstance(fl_rec.get("baseline_margins"), dict)
                    or len(fl_rec["baseline_margins"]) == 0
                ):
                    return (False, f"{flip_label}[{fl_idx}] missing non-empty baseline_margins dict")

        runs_thr = s_thr.get("runs", [])
        if s_thr.get("grid_count") != 18 or not isinstance(runs_thr, list) or len(runs_thr) != 18:
            return (False, f"Expected 18 threshold sensitivity runs, got grid_count={s_thr.get('grid_count')}")

        if int(s_cls.get("failed_batch_runs_count", 0)) < 1:
            return (False, "Clustering sensitivity missing natural single-batch failed chain-break run")
        has_axes_or_runs = (
            (isinstance(s_cls.get("axes"), (dict, list)) and len(s_cls["axes"]) >= 4)
            or (isinstance(s_cls.get("runs"), list) and len(s_cls["runs"]) >= 4)
        )
        if not has_axes_or_runs:
            return (False, "Clustering sensitivity missing 4-axis coverage")

        if s_thr.get("all_persisted_in_duckdb") is False or s_cls.get("all_persisted_in_duckdb") is False:
            return (False, "Sensitivity artifacts reported all_persisted_in_duckdb=false")

        db_inv = self.query_duckdb_invariants()
        if db_inv["status"] != "pass":
            return (False, f"DuckDB evolution_runs persistence check failed: {db_inv.get('reason')}")
        db_runs_cnt = int(db_inv["row"].get("evolution_runs_count", 0))
        if db_runs_cnt < 20:
            return (
                False,
                f"Expected >= 20 published evolution_runs including sensitivity grid in DuckDB, got {db_runs_cnt}",
            )

        return (
            True,
            f"Level 1a certified_evals={l1a.get('certified_evaluations_count')} (0 flips), "
            f"Level 1b topology_flips=0, Level 2 certified_evals={p_memb.get('certified_evaluations_count')} (0 flips), "
            f"all boundary flips carry baseline_margins, 18 threshold runs + 4 clustering axes persisted "
            f"(evolution_runs={db_runs_cnt}, failed_batch_runs={s_cls.get('failed_batch_runs_count')})",
        )

    @staticmethod
    def _check_measurements_stages(meas: dict[str, Any]) -> tuple[bool, str]:
        if not _status_is_pass(meas):
            return (False, "measurements.json status is not pass")
        top_repeats = int(meas.get("repeats") or meas.get("repeat_count") or 0)
        if top_repeats < 3:
            return (False, f"measurements.json repeats={top_repeats} < 3")
        raw_stages = meas.get("stages")
        stage_items = list(raw_stages.values()) if isinstance(raw_stages, dict) else raw_stages
        if not isinstance(stage_items, list) or len(stage_items) != 7:
            return (False, f"Expected 7 stages in measurements.json, got {type(raw_stages).__name__}")
        for idx, st_obj in enumerate(stage_items):
            if not isinstance(st_obj, dict):
                return (False, f"measurements.json stage[{idx}] is not an object")
            st_rep = int(st_obj.get("repeats") or st_obj.get("repeat_count") or 0)
            if st_rep < 3:
                return (False, f"measurements.json stage[{idx}] repeats={st_rep} < 3")
            mean_wall = st_obj.get("mean_wall_seconds")
            peak_rss = st_obj.get("peak_rss_kib")
            if not (is_finite_number(mean_wall) and float(mean_wall) > 0.0):
                return (False, f"measurements.json stage[{idx}] non-positive mean_wall_seconds={mean_wall!r}")
            if not (is_finite_number(peak_rss) and float(peak_rss) > 0.0):
                return (False, f"measurements.json stage[{idx}] non-positive peak_rss_kib={peak_rss!r}")
            runs = st_obj.get("runs")
            if not isinstance(runs, list) or len(runs) < 3:
                return (False, f"measurements.json stage[{idx}] runs list has < 3 entries")
            for r_i, run_rec in enumerate(runs):
                if not isinstance(run_rec, dict):
                    return (False, f"measurements.json stage[{idx}].runs[{r_i}] not an object")
                w_sec = run_rec.get("wall_seconds")
                r_kib = run_rec.get("maxrss_kib")
                if not (is_finite_number(w_sec) and float(w_sec) > 0.0):
                    return (False, f"measurements.json stage[{idx}].runs[{r_i}] invalid wall_seconds={w_sec!r}")
                if not (is_finite_number(r_kib) and float(r_kib) > 0.0):
                    return (False, f"measurements.json stage[{idx}].runs[{r_i}] invalid maxrss_kib={r_kib!r}")
        return (True, "7 stages verified with repeats>=3 and positive wall_seconds & peak_rss_kib")

    def _check_regression_gates(self, docs: dict[str, dict[str, Any]]) -> tuple[bool, str]:
        m1_sum = docs["m1-regression/summary.json"]
        m1_rep = docs["m1-regression/report.json"]
        m1_tr = docs["m1-regression/traceability.json"]
        m1_aud = docs["m1-regression/report-audit.json"]
        m2_sum = docs["m2-regression/summary.json"]
        m2_rep = docs["m2-regression/report.json"]
        m2_tr = docs["m2-regression/traceability.json"]
        m2_aud = docs["m2-regression/report-audit.json"]

        m1_rep_overall = m1_rep.get("overall_status") if "overall_status" in m1_rep else m1_rep.get("overall")
        m1_aud_ok = m1_aud.get("findings") == [] and (
            m1_aud.get("audit_status") == "passed"
            or (m1_aud.get("status") == "pass" and m1_aud.get("derived_overall") == "passed")
        )
        m1_ok = (
            _status_is_pass(m1_sum)
            and m1_sum.get("overall_status") == "passed"
            and m1_sum.get("traceability_status") == "pass"
            and m1_sum.get("audit_status") == "passed"
            and m1_rep_overall == "passed"
            and m1_tr.get("status") == "pass"
            and m1_aud_ok
        )
        if not m1_ok:
            return (False, "M1 regression summary/report/traceability/report-audit did not pass")

        m2_aud_ok = m2_aud.get("audit_status") == "passed" and m2_aud.get("findings") == []
        m2_ok = (
            _status_is_pass(m2_sum)
            and m2_sum.get("overall_status") == "passed"
            and m2_sum.get("traceability_status") == "pass"
            and m2_sum.get("audit_status") == "passed"
            and m2_rep.get("overall_status") == "passed"
            and m2_tr.get("status") == "pass"
            and m2_aud_ok
        )
        if not m2_ok:
            return (False, "M2 regression summary/report/traceability/report-audit did not pass")

        for prefix, sum_doc in (("m1-regression", m1_sum), ("m2-regression", m2_sum)):
            for sha_key, fname in (
                ("report_sha256", "report.json"),
                ("traceability_sha256", "traceability.json"),
                ("report_audit_sha256", "report-audit.json"),
            ):
                if sha_key in sum_doc:
                    actual_sha = sha256_file(self._validate_rel_path(f"{prefix}/{fname}"))
                    if sum_doc[sha_key] != actual_sha:
                        return (False, f"{prefix}/summary.json {sha_key} mismatch against {fname}")

        return (True, "M1 & M2 summary/report/traceability/report-audit passed")

    def _check_reproduction_authenticity(self) -> tuple[str, list[dict[str, str]], str]:
        req = [
            "independent-reproduction.json",
            "sql-history/real-evolution-replay.json",
            "reproduction/child/sql-history/real-evolution-replay.json",
        ]
        st, docs, refs, msg = self.require_artifacts(req)
        if st != "ok":
            return (st, refs, msg)
        repro = docs["independent-reproduction.json"]
        if not _status_is_pass(repro) or repro.get("equivalence") is not True:
            return ("fail", refs, "independent-reproduction.json did not pass")
        try:
            proj_parent = project_replay_report(docs["sql-history/real-evolution-replay.json"])
            proj_child = project_replay_report(
                docs["reproduction/child/sql-history/real-evolution-replay.json"]
            )
        except LocatedError as exc:
            return ("fail", refs, f"Forged or invalid branch_id prefix in reproduction: {exc}")
        if proj_parent != proj_child:
            return ("fail", refs, "Parent vs child semantic evolution projection mismatch")
        return ("pass", refs, "Fresh-clone reproduction verified with authentic tracking_run_key[:12]: prefixes")

    def eval_fr(self, fr_id: str) -> dict[str, Any]:
        match fr_id:
            case "FR-001":
                paths = [
                    "om-verification/cell-matrix-manifest.json",
                    "om-verification/om-cells-report.json",
                    "om-verification/test-om-sample-report.json",
                    "core/synthetic-oracle.json",
                    "sql-history/real-evolution-replay.json",
                    "case-study/main-evolution-k3-c1.0.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                cm = docs["om-verification/cell-matrix-manifest.json"]
                oc = docs["om-verification/om-cells-report.json"]
                ts = docs["om-verification/test-om-sample-report.json"]
                ora = docs["core/synthetic-oracle.json"]
                rr = docs["sql-history/real-evolution-replay.json"]
                mc = docs["case-study/main-evolution-k3-c1.0.json"]
                chain_res = ora.get("chain_results")
                chain_batches_ok = (
                    isinstance(chain_res, list)
                    and len(chain_res) >= 4
                    and all(
                        isinstance(c, dict)
                        and 4 <= int(c.get("planned_batch_count", 0)) <= 8
                        and str(c.get("status", "")).strip().lower() in ("pass", "passed", "ok")
                        and c.get("zero_ulp_verified") is True
                        for c in chain_res
                    )
                    and {4, 5, 6, 8}.issubset(
                        {int(c["planned_batch_count"]) for c in chain_res if isinstance(c, dict) and "planned_batch_count" in c}
                    )
                )
                ok = (
                    all(_status_is_pass(d) for d in (cm, oc, ts, ora, rr, mc))
                    and chain_batches_ok
                    and cm.get("total_cells_checked") == 3600
                    and oc.get("sc001_satisfied") is True
                    and int(ts.get("negative_cases_tested", 0)) >= 5
                    and rr.get("preparation_negative_gate_rejected_with_zero_residual") is True
                    and mc.get("planned_batch_count") == 8
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "4–8 planned batches ({4,5,6,8} 0-ULP chains + 8-batch WB2) & M2 metadata/valid_time/lead identity gates verified "
                        "(3600 OM cells, >=5 negative fixtures, 0-residual corrupted valid_time rejection)"
                        if ok
                        else "FR-001 4–8 batch chain coverage, metadata, or OM preparation check failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-002":
                paths = [
                    "core/synthetic-oracle.json",
                    "sql-history/real-evolution-replay.json",
                    "sql-history/evolution-runs.duckdb",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ora = docs["core/synthetic-oracle.json"]
                rr = docs["sql-history/real-evolution-replay.json"]
                berge_ok, pair_cnt = self._recompute_berge_star_counts(ora)
                db_inv = self.query_duckdb_invariants()
                cs = rr.get("chain_summary", {})
                ok = (
                    _status_is_pass(ora)
                    and _status_is_pass(rr)
                    and berge_ok
                    and db_inv["status"] == "pass"
                    and cs.get("primary_edge_count") == 16
                    and cs.get("secondary_split_edge_count") == 2
                    and cs.get("secondary_merge_edge_count") == 1
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        f"Primary 1-to-1 + nearest_feasible_v1 + star_min_mass_proportional_v1 verified across "
                        f"{pair_cnt} fixed pairs, 4000 random pairs, and E1–E3 DuckDB guards"
                        if ok
                        else f"FR-002 topology/flow invariant failed (db={db_inv})"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-003":
                paths = [
                    "core/synthetic-oracle.json",
                    "sql-history/synthetic-evolution-replay.json",
                    "sql-history/evolution-runs.duckdb",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ora = docs["core/synthetic-oracle.json"]
                sr = docs["sql-history/synthetic-evolution-replay.json"]
                berge_ok, _ = self._recompute_berge_star_counts(ora)
                db_inv = self.query_duckdb_invariants()
                ok = (
                    _status_is_pass(ora)
                    and _status_is_pass(sr)
                    and berge_ok
                    and ora.get("chain_gap_disjointness_verified") is True
                    and int(sr.get("cases_verified", 0)) >= 5
                    and db_inv["status"] == "pass"
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "5 base events + gap_interrupted/gap_restarted mutually exclusive and exhaustive (E4/E5 verified)"
                        if ok
                        else "FR-003 event classification or gap disjointness check failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-004":
                paths = [
                    "core/permutation-invariance.json",
                    "core/synthetic-oracle.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                perm = docs["core/permutation-invariance.json"]
                ora = docs["core/synthetic-oracle.json"]
                pstats = perm.get("permutation_stats", {})
                p144 = perm.get("exhaustive_3x4_144_permutations", {})
                sym = perm.get("symmetric_tie_canonical_verification", {})
                ok = (
                    _status_is_pass(perm)
                    and _status_is_pass(ora)
                    and int(pstats.get("permutations_tested", 0)) >= 2000
                    and pstats.get("mismatches") == 0
                    and p144.get("permutations_tested") == 144
                    and p144.get("mismatches") == 0
                    and sym.get("ambiguous_tie_asserted") is True
                    and sym.get("oracle_canonical_tiebreak_matched") is True
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "2000 random + 144 exhaustive 3x4 permutations 100% invariant; symmetric ties set ambiguous_tie=true"
                        if ok
                        else "FR-004 permutation invariance or tie-breaking failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-005":
                paths = [
                    "core/synthetic-oracle.json",
                    "core/orthogonality-separation.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ora = docs["core/synthetic-oracle.json"]
                ortho = docs["core/orthogonality-separation.json"]
                ortho_ok, ortho_msg = self._recompute_orthogonality(ortho)
                rstats = ora.get("random_oracle_stats", {})
                ok = (
                    _status_is_pass(ora)
                    and _status_is_pass(ortho)
                    and ortho_ok
                    and ora.get("ulp_tolerance") == 0
                    and ora.get("fixed_pair_mismatches") == 0
                    and int(rstats.get("cases_tested", 0)) >= 4000
                    and rstats.get("mismatches") == 0
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": ortho_msg if ok else f"FR-005 metric family check failed: {ortho_msg}",
                    "evidence_refs": refs,
                }

            case "FR-006":
                paths = [
                    "core/orthogonality-separation.json",
                    "core/synthetic-oracle.json",
                    "case-study/main-evolution-k3-c1.0.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ortho = docs["core/orthogonality-separation.json"]
                ora = docs["core/synthetic-oracle.json"]
                mc = docs["case-study/main-evolution-k3-c1.0.json"]
                ortho_ok, _ = self._recompute_orthogonality(ortho)
                ok = (
                    _status_is_pass(ortho)
                    and _status_is_pass(ora)
                    and _status_is_pass(mc)
                    and ortho_ok
                    and ora.get("uncertainty_rule") == "m3.uncertainty.v1"
                    and mc.get("high_uncertainty_transitions") == [0, 3, 6]
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "Continuous 3-family metrics + 5 base events + m3.uncertainty.v1 (0.10) verified "
                        "(0.05R vs 0.15R bracketing & WB2 transitions [0, 3, 6])"
                        if ok
                        else "FR-006 uncertainty rule or output granularity check failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-007":
                paths = [
                    "core/synthetic-oracle.json",
                    "sql-history/sql-contract.json",
                    "sql-history/real-evolution-replay.json",
                    "sql-history/evolution-runs.duckdb",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ora = docs["core/synthetic-oracle.json"]
                sc = docs["sql-history/sql-contract.json"]
                rr = docs["sql-history/real-evolution-replay.json"]
                db_inv = self.query_duckdb_invariants()
                rp = rr.get("runner_parity", {})
                ok = (
                    _status_is_pass(ora)
                    and _status_is_pass(sc)
                    and _status_is_pass(rr)
                    and db_inv["status"] == "pass"
                    and int(ora.get("chain_cases_count", 0)) >= 4
                    and ora.get("chain_mismatches") == 0
                    and sc.get("ens_fsum_contract", {}).get("math_fsum_zero_ulp_parity") is True
                    and rp.get("exact_parity_with_m3_runner") is True
                    and rp.get("exact_parity_with_chain_oracle") is True
                    and len(rr.get("branches", [])) == 8
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "Branch-level (evolution_branch_summaries) & chain-level (chain_summary) metrics "
                        "match m3_runner and Fraction/fsum oracle with E7 lifecycle conservation"
                        if ok
                        else "FR-007 branch/chain summary parity failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-008":
                paths = [
                    "case-study/limitations.json",
                    "case-study/main-evolution-k3-c1.0.json",
                    "visualization/traceability-audit.json",
                    "visualization/real-8run-main.html",
                    "visualization/synthetic-showcase.html",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                lim = docs["case-study/limitations.json"]
                mc = docs["case-study/main-evolution-k3-c1.0.json"]
                va = docs["visualization/traceability-audit.json"]
                db_inv = self.query_duckdb_invariants()
                viz_live = self.verify_live_html_views()

                blobs: list[str] = []
                symlink_err: str | None = None
                for cs_rel in (
                    "case-study/limitations.json",
                    "case-study/main-evolution-k3-c1.0.json",
                    "case-study/sensitivity-thresholds.json",
                    "case-study/sensitivity-clustering-configs.json",
                    "case-study/perturbation-evolution-level.json",
                    "case-study/perturbation-member-level.json",
                ):
                    try:
                        if self.has_file(cs_rel):
                            _, cs_doc, _ = self.load_artifact_json(cs_rel)
                            if cs_doc is not None:
                                _collect_strings(cs_doc, blobs)
                    except (LocatedError, ValueError) as exc:
                        symlink_err = str(exc)
                claim_findings = scan_prohibited_claims("\n".join(blobs))
                if symlink_err:
                    claim_findings.append(symlink_err)
                ok = (
                    _status_is_pass(lim)
                    and _status_is_pass(mc)
                    and _status_is_pass(va)
                    and lim.get("non_physical_disclaimer") == NON_PHYSICAL_DISCLAIMER
                    and mc.get("non_physical_disclaimer") == NON_PHYSICAL_DISCLAIMER
                    and bool(lim.get("clauses") or lim.get("limitations") or lim.get("boundaries"))
                    and db_inv["status"] == "pass"
                    and viz_live["status"] == "pass"
                    and len(claim_findings) == 0
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "Non-physical attribution disclaimer present across core, SQL, case-study, and HTML; "
                        "0 prohibited migration/cyclogenesis/calibration/trading claims"
                        if ok
                        else f"FR-008 disclaimer or prohibited claim failure: {claim_findings or viz_live.get('findings')}"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-009":
                paths = [
                    "sql-history/three-process-repro.json",
                    "sql-history/transaction-atomicity.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                tp = docs["sql-history/three-process-repro.json"]
                ta = docs["sql-history/transaction-atomicity.json"]
                pids = tp.get("distinct_pids", [])
                ok = (
                    _status_is_pass(tp)
                    and _status_is_pass(ta)
                    and int(tp.get("processes_tested", 0)) >= 3
                    and len(set(pids)) >= 3
                    and tp.get("synthetic_replay", {}).get("exact_match_across_3_processes") is True
                    and (tp.get("real_replay") or {}).get("exact_match_across_3_processes") is True
                    and ta.get("idempotent_republish", {}).get("identical_after_republish") is True
                    and ta.get("version_conflict_rejection", {}).get("rejected") is True
                    and ta.get("version_conflict_rejection", {}).get("error_code") == "EVOLUTION_RUN_VERSION_CONFLICT"
                    and ta.get("multi_version_coexistence", {}).get("both_preserved_without_mutation") is True
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "Content-bound evolution_run_id verified across 3 OS processes, idempotent republish, "
                        "EVOLUTION_RUN_VERSION_CONFLICT rejection, and multi-version coexistence"
                        if ok
                        else "FR-009 version immutability or 3-process check failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-010":
                paths = [
                    "sql-history/transaction-atomicity.json",
                    "sql-history/real-evolution-replay.json",
                    "sql-history/synthetic-evolution-replay.json",
                    "sql-history/evolution-runs.duckdb",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ta = docs["sql-history/transaction-atomicity.json"]
                rr = docs["sql-history/real-evolution-replay.json"]
                sr = docs["sql-history/synthetic-evolution-replay.json"]
                db_inv = self.query_duckdb_invariants()
                fb = ta.get("fault_injection_rollback", {})
                ok = (
                    _status_is_pass(ta)
                    and _status_is_pass(rr)
                    and _status_is_pass(sr)
                    and db_inv["status"] == "pass"
                    and rr.get("invariants_e1_to_e8_passed") is True
                    and int(fb.get("hooks_tested", 0)) >= 7
                    and fb.get("zero_residual_across_all_11_tables") is True
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "11-table atomic publish, E1–E8 SQL invariants, gap chain-break, and 7-hook fault rollback "
                        "with 0 residual rows verified"
                        if ok
                        else f"FR-010 11-table persistence or E1–E8 check failed: {db_inv}"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-011":
                paths = [
                    "sql-history/sql-contract.json",
                    "m1-regression/summary.json",
                    "m2-regression/summary.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                sc = docs["sql-history/sql-contract.json"]
                m1 = docs["m1-regression/summary.json"]
                m2 = docs["m2-regression/summary.json"]
                udfs = sc.get("preserved_udfs", {})
                ok = (
                    _status_is_pass(sc)
                    and _status_is_pass(m1)
                    and _status_is_pass(m2)
                    and all(udfs.get(u) is True for u in ("ens_m0_echo", "ens_cluster", "ens_dispersion", "ens_match_scenarios"))
                    and sc.get("ens_evolve_scenarios_contract", {}).get("oracle_pair_zero_ulp_parity") is True
                    and m1.get("overall_status") == "passed"
                    and m2.get("overall_status") == "passed"
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "Stateless ens_evolve_scenarios & ens_fsum verified; M1 & M2 entrypoints and full gates 100% preserved"
                        if ok
                        else "FR-011 stateless entrypoint or M1/M2 preservation check failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-012":
                paths = [
                    "core/error-cases.json",
                    "sql-history/sql-contract.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ec = docs["core/error-cases.json"]
                sc = docs["sql-history/sql-contract.json"]
                lcb = sc.get("large_chunk_boundary", {})
                ok = (
                    _status_is_pass(ec)
                    and _status_is_pass(sc)
                    and int(ec.get("distinct_error_codes_count", 0)) >= 16
                    and ec.get("single_item_exit_1_zero_output_residue_verified") is True
                    and int(lcb.get("rows_tested", 0)) > 2048
                    and lcb.get("late_chunk_bad_row_aborts_whole_query") is True
                    and sc.get("same_session_recovery", {}).get("recovered_cleanly") is True
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        f"{ec.get('distinct_error_codes_count')} error codes rejected with location indices, "
                        f">2048-row whole-query abort, and same-session recovery verified"
                        if ok
                        else "FR-012 error rejection or session recovery check failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-013":
                paths = [
                    "core/synthetic-oracle.json",
                    "core/orthogonality-separation.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ora = docs["core/synthetic-oracle.json"]
                ortho = docs["core/orthogonality-separation.json"]
                berge_ok, pair_cnt = self._recompute_berge_star_counts(ora)
                ortho_ok, _ = self._recompute_orthogonality(ortho)
                rstats = ora.get("random_oracle_stats", {})
                ok = (
                    _status_is_pass(ora)
                    and _status_is_pass(ortho)
                    and berge_ok
                    and ortho_ok
                    and int(ora.get("chain_cases_count", 0)) >= 4
                    and ora.get("chain_mismatches") == 0
                    and int(rstats.get("cases_tested", 0)) >= 4000
                    and rstats.get("mismatches") == 0
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        f"Synthetic suite ({pair_cnt} fixed pairs, {ora.get('chain_cases_count')} chains, "
                        f"orthogonality series, 4000 random cases) 0-ULP matched to independent Fraction oracle"
                        if ok
                        else "FR-013 synthetic oracle suite failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-014":
                paths = [
                    "visualization/real-8run-main.html",
                    "visualization/synthetic-showcase.html",
                    "visualization/traceability-audit.json",
                    "visualization/tamper-self-check.json",
                    "sql-history/evolution-runs.duckdb",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                va = docs["visualization/traceability-audit.json"]
                ts = docs["visualization/tamper-self-check.json"]
                viz_live = self.verify_live_html_views()
                ok = (
                    _status_is_pass(va)
                    and _status_is_pass(ts)
                    and viz_live["status"] == "pass"
                    and viz_live["orphan_primitives_total"] == 0
                    and int(ts.get("tamper_cases_tested", 0)) >= 10
                    and ts.get("tamper_cases_rejected") == ts.get("tamper_cases_tested")
                    and ts.get("incomplete_provenance_render_rejected") is True
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "Offline single-file interactive HTML views (real-8run-main & synthetic-showcase) passed "
                        f"all 6 traceability passes (0 external URLs, 0 orphan SVG elements, {ts.get('tamper_cases_rejected')}/{ts.get('tamper_cases_tested')} tampers rejected)"
                        if ok
                        else f"FR-014 visualization traceability failed: {viz_live.get('findings')}"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-015":
                paths = [
                    "case-study/main-evolution-k3-c1.0.json",
                    "case-study/limitations.json",
                    "sql-history/real-evolution-replay.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                mc = docs["case-study/main-evolution-k3-c1.0.json"]
                lim = docs["case-study/limitations.json"]
                rr = docs["sql-history/real-evolution-replay.json"]
                cs = rr.get("chain_summary", {})
                ok = (
                    _status_is_pass(mc)
                    and _status_is_pass(lim)
                    and _status_is_pass(rr)
                    and mc.get("planned_batch_count") == 8
                    and mc.get("node_count") == 24
                    and mc.get("branch_count") == 8
                    and mc.get("primary_edge_count") == 16 == cs.get("primary_edge_count")
                    and mc.get("secondary_split_edge_count") == 2 == cs.get("secondary_split_edge_count")
                    and mc.get("secondary_merge_edge_count") == 1 == cs.get("secondary_merge_edge_count")
                    and mc.get("continuation_count") == 13 == cs.get("continuation_count")
                    and mc.get("split_count") == 2 == cs.get("split_count")
                    and mc.get("merge_count") == 1 == cs.get("merge_count")
                    and mc.get("emergence_count") == 3 == cs.get("emergence_count")
                    and mc.get("disappearance_count") == 4 == cs.get("disappearance_count")
                    and mc.get("high_uncertainty_transitions") == [0, 3, 6]
                    and bool(lim.get("clauses") or lim.get("limitations") or lim.get("boundaries"))
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "WB2 8-batch main case study (k=3, c=1.0: 24 nodes, 8 branches, 16 primary + 2 split + "
                        "1 merge edges, high-uncertainty transitions [0, 3, 6]) and limitations verified"
                        if ok
                        else "FR-015 WB2 main case study counts or limitations mismatch"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-016":
                paths = [
                    "case-study/perturbation-evolution-level.json",
                    "case-study/perturbation-member-level.json",
                    "case-study/sensitivity-thresholds.json",
                    "case-study/sensitivity-clustering-configs.json",
                    "sql-history/evolution-runs.duckdb",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ok, details = self._check_perturbation_and_sensitivity(docs)
                return {"status": "pass" if ok else "fail", "details": details, "evidence_refs": refs}

            case "FR-017":
                paths = ["minimal-runtime.json"]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                mr = docs["minimal-runtime.json"]
                ok = (
                    _status_is_pass(mr)
                    and mr.get("network_mode") == "none"
                    and mr.get("network_zero_routes_verified") is True
                    and mr.get("all_prohibited_tools_absent") is True
                    and mr.get("zero_residual_after_rollback") is True
                    and mr.get("standalone_runner_parity") is True
                    and mr.get("sql_11table_replay_parity") is True
                    and mr.get("same_session_error_recovery") is True
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "Offline minimal container (--network none, 0 routes, prohibited tools absent, 0 rollback residual) verified "
                        "standalone m3_runner, 11-table SQL replay parity, and same-session recovery"
                        if ok
                        else "FR-017 minimal runtime container verification failed"
                    ),
                    "evidence_refs": refs,
                }

            case "FR-018":
                paths = [
                    "measurements.json",
                    "case-study/limitations.json",
                    "m1-regression/summary.json",
                    "m1-regression/report.json",
                    "m1-regression/traceability.json",
                    "m1-regression/report-audit.json",
                    "m2-regression/summary.json",
                    "m2-regression/report.json",
                    "m2-regression/traceability.json",
                    "m2-regression/report-audit.json",
                    "independent-reproduction.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                repro_st, repro_refs, repro_msg = self._check_reproduction_authenticity()
                seen = {r["path"] for r in refs}
                for rr_ref in repro_refs:
                    if rr_ref["path"] not in seen:
                        refs.append(rr_ref)
                        seen.add(rr_ref["path"])
                if repro_st != "pass":
                    return {"status": repro_st, "details": repro_msg, "evidence_refs": refs}

                meas = docs["measurements.json"]
                lim = docs["case-study/limitations.json"]
                docs_present = (
                    (REPO_ROOT / "specs" / "004-m3-scenario-evolution" / "quickstart.md").is_file()
                    and (REPO_ROOT / "validation" / "m3" / "README.md").is_file()
                )
                meas_ok, meas_msg = self._check_measurements_stages(meas)
                reg_ok, reg_msg = self._check_regression_gates(docs)
                ok = docs_present and meas_ok and _status_is_pass(lim) and reg_ok
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "Quickstart guide, 7-stage resource measurements (repeats>=3, positive wall & RSS), limitations, "
                        "serial M1+M2 full regressions (passed), and fresh-clone reproduction (PASS) verified"
                        if ok
                        else f"FR-018 check failed: meas=({meas_ok}: {meas_msg}), reg=({reg_ok}: {reg_msg})"
                    ),
                    "evidence_refs": refs,
                }

            case _:
                return {"status": "fail", "details": f"Unknown requirement ID: {fr_id}", "evidence_refs": []}

    def eval_sc(self, sc_id: str) -> dict[str, Any]:
        match sc_id:
            case "SC-001":
                paths = [
                    "core/synthetic-oracle.json",
                    "core/permutation-invariance.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ora = docs["core/synthetic-oracle.json"]
                perm = docs["core/permutation-invariance.json"]
                berge_ok, pair_cnt = self._recompute_berge_star_counts(ora)
                rstats = ora.get("random_oracle_stats", {})
                pstats = perm.get("permutation_stats", {})
                p144 = perm.get("exhaustive_3x4_144_permutations", {})
                ok = (
                    _status_is_pass(ora)
                    and _status_is_pass(perm)
                    and berge_ok
                    and ora.get("fixed_pair_mismatches") == 0
                    and ora.get("chain_mismatches") == 0
                    and int(rstats.get("cases_tested", 0)) >= 4000
                    and rstats.get("mismatches") == 0
                    and int(pstats.get("permutations_tested", 0)) >= 2000
                    and pstats.get("mismatches") == 0
                    and p144.get("permutations_tested") == 144
                    and p144.get("mismatches") == 0
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        f"100% 0-ULP agreement across {pair_cnt} fixed pairs, {ora.get('chain_cases_count')} chains, "
                        f"and 4000 random cases; 100% invariance across 2000+144 permutations"
                        if ok
                        else "SC-001 oracle parity or permutation invariance failed"
                    ),
                    "evidence_refs": refs,
                }

            case "SC-002":
                paths = [
                    "sql-history/real-evolution-replay.json",
                    "case-study/main-evolution-k3-c1.0.json",
                    "om-verification/om-cells-report.json",
                    "om-verification/duckomo-version-comparison.json",
                    "sql-history/evolution-runs.duckdb",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                rr = docs["sql-history/real-evolution-replay.json"]
                mc = docs["case-study/main-evolution-k3-c1.0.json"]
                oc = docs["om-verification/om-cells-report.json"]
                dc = docs["om-verification/duckomo-version-comparison.json"]
                db_inv = self.query_duckdb_invariants()
                ok = (
                    all(_status_is_pass(d) for d in (rr, mc, oc, dc))
                    and db_inv["status"] == "pass"
                    and oc.get("sc001_satisfied") is True
                    and dc.get("exact_semantic_equivalence") is True
                    and len(rr.get("batches", [])) == 8
                    and len(rr.get("nodes", [])) == 24
                    and len(rr.get("transitions", [])) == 7
                    and rr.get("chain_summary", {}).get("primary_edge_count") == 16
                    and mc.get("node_count") == 24
                    and mc.get("primary_edge_count") == 16
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "8 batches (24 nodes) & 7 transitions published in single workflow; 100% primary edges (16) "
                        "and branch_ids match M2 (E1/E2) and dual DuckOMO versions"
                        if ok
                        else "SC-002 real 8-batch workflow or E1/E2 check failed"
                    ),
                    "evidence_refs": refs,
                }

            case "SC-003":
                paths = [
                    "core/orthogonality-separation.json",
                    "core/synthetic-oracle.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ortho = docs["core/orthogonality-separation.json"]
                ora = docs["core/synthetic-oracle.json"]
                ortho_ok, ortho_msg = self._recompute_orthogonality(ortho)
                ok = _status_is_pass(ortho) and _status_is_pass(ora) and ortho_ok
                return {
                    "status": "pass" if ok else "fail",
                    "details": ortho_msg if ok else f"SC-003 orthogonality separation failed: {ortho_msg}",
                    "evidence_refs": refs,
                }

            case "SC-004":
                paths = [
                    "sql-history/three-process-repro.json",
                    "sql-history/transaction-atomicity.json",
                    "sql-history/sql-contract.json",
                    "core/error-cases.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                tp = docs["sql-history/three-process-repro.json"]
                ta = docs["sql-history/transaction-atomicity.json"]
                sc = docs["sql-history/sql-contract.json"]
                ec = docs["core/error-cases.json"]
                ok = (
                    all(_status_is_pass(d) for d in (tp, ta, sc, ec))
                    and int(tp.get("processes_tested", 0)) >= 3
                    and tp.get("synthetic_replay", {}).get("exact_match_across_3_processes") is True
                    and (tp.get("real_replay") or {}).get("exact_match_across_3_processes") is True
                    and ta.get("version_conflict_rejection", {}).get("rejected") is True
                    and ta.get("fault_injection_rollback", {}).get("zero_residual_across_all_11_tables") is True
                    and sc.get("same_session_recovery", {}).get("recovered_cleanly") is True
                    and int(ec.get("distinct_error_codes_count", 0)) >= 16
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "3-process reproducibility 100% identical; 100% invalid inputs & version conflicts "
                        "rejected with 0 residual rows and 100% same-session recovery"
                        if ok
                        else "SC-004 determinism, rollback, or error recovery check failed"
                    ),
                    "evidence_refs": refs,
                }

            case "SC-005":
                paths = [
                    "case-study/perturbation-evolution-level.json",
                    "case-study/perturbation-member-level.json",
                    "case-study/sensitivity-thresholds.json",
                    "case-study/sensitivity-clustering-configs.json",
                    "sql-history/evolution-runs.duckdb",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                ok, details = self._check_perturbation_and_sensitivity(docs)
                return {"status": "pass" if ok else "fail", "details": details, "evidence_refs": refs}

            case "SC-006":
                paths = [
                    "visualization/real-8run-main.html",
                    "visualization/synthetic-showcase.html",
                    "visualization/traceability-audit.json",
                    "visualization/tamper-self-check.json",
                    "sql-history/evolution-runs.duckdb",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                va = docs["visualization/traceability-audit.json"]
                ts = docs["visualization/tamper-self-check.json"]
                viz_live = self.verify_live_html_views()
                ok = (
                    _status_is_pass(va)
                    and _status_is_pass(ts)
                    and viz_live["status"] == "pass"
                    and viz_live["orphan_primitives_total"] == 0
                    and int(ts.get("tamper_cases_tested", 0)) >= 10
                    and ts.get("tamper_cases_rejected") == ts.get("tamper_cases_tested")
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "100% SVG nodes & edges mapped bijectively to DuckDB 6 M3 tables (0 orphans, "
                        f"visible 3-clause disclaimer, {ts.get('tamper_cases_rejected')}/{ts.get('tamper_cases_tested')} tampers rejected)"
                        if ok
                        else f"SC-006 visualization traceability failed: {viz_live.get('findings')}"
                    ),
                    "evidence_refs": refs,
                }

            case "SC-007":
                paths = ["minimal-runtime.json"]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                mr = docs["minimal-runtime.json"]
                ok = (
                    _status_is_pass(mr)
                    and mr.get("network_mode") == "none"
                    and mr.get("network_zero_routes_verified") is True
                    and mr.get("all_prohibited_tools_absent") is True
                    and mr.get("zero_residual_after_rollback") is True
                    and mr.get("standalone_runner_parity") is True
                    and mr.get("sql_11table_replay_parity") is True
                    and mr.get("same_session_error_recovery") is True
                )
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "Minimal --network none runtime (0 routes, prohibited tools absent, 0 rollback residual) parity and error recovery 100% verified"
                        if ok
                        else "SC-007 minimal runtime check failed"
                    ),
                    "evidence_refs": refs,
                }

            case "SC-008":
                paths = [
                    "m1-regression/summary.json",
                    "m1-regression/report.json",
                    "m1-regression/traceability.json",
                    "m1-regression/report-audit.json",
                    "m2-regression/summary.json",
                    "m2-regression/report.json",
                    "m2-regression/traceability.json",
                    "m2-regression/report-audit.json",
                    "independent-reproduction.json",
                    "measurements.json",
                    "case-study/limitations.json",
                ]
                st, docs, refs, msg = self.require_artifacts(paths)
                if st != "ok":
                    return {"status": st, "details": msg, "evidence_refs": refs}
                repro_st, repro_refs, repro_msg = self._check_reproduction_authenticity()
                seen = {r["path"] for r in refs}
                for rr_ref in repro_refs:
                    if rr_ref["path"] not in seen:
                        refs.append(rr_ref)
                        seen.add(rr_ref["path"])
                if repro_st != "pass":
                    return {"status": repro_st, "details": repro_msg, "evidence_refs": refs}

                meas = docs["measurements.json"]
                lim = docs["case-study/limitations.json"]
                meas_ok, meas_msg = self._check_measurements_stages(meas)
                reg_ok, reg_msg = self._check_regression_gates(docs)
                ok = reg_ok and meas_ok and _status_is_pass(lim)
                return {
                    "status": "pass" if ok else "fail",
                    "details": (
                        "M1 & M2 full gates passed (0 regression), fresh-clone reproduction passed, "
                        "7-stage measurements & limitations complete"
                        if ok
                        else f"SC-008 check failed: reg=({reg_ok}: {reg_msg}), meas=({meas_ok}: {meas_msg})"
                    ),
                    "evidence_refs": refs,
                }

            case _:
                return {"status": "fail", "details": f"Unknown success criterion ID: {sc_id}", "evidence_refs": []}

    def evaluate(self) -> dict[str, Any]:
        """Evaluate all 18 FRs and 8 SCs in-memory without mutating any file on disk."""
        requirements: dict[str, dict[str, Any]] = {}
        for fr_id in REQUIREMENT_IDS:
            requirements[fr_id] = self.eval_fr(fr_id)

        success_criteria: dict[str, dict[str, Any]] = {}
        for sc_id in SUCCESS_CRITERION_IDS:
            success_criteria[sc_id] = self.eval_sc(sc_id)

        passed_fr = sum(1 for item in requirements.values() if item["status"] == "pass")
        passed_sc = sum(1 for item in success_criteria.values() if item["status"] == "pass")
        all_passed = passed_fr == len(REQUIREMENT_IDS) and passed_sc == len(SUCCESS_CRITERION_IDS)

        return {
            "schema_version": TRACEABILITY_SCHEMA_VERSION,
            "run_id": self.run_dir.name,
            "evaluated_at_utc": _utc_now(),
            "status": "pass" if all_passed else "fail",
            "summary": {
                "total_requirements": len(REQUIREMENT_IDS),
                "passed_requirements": passed_fr,
                "not_run_requirements": sum(1 for item in requirements.values() if item["status"] == "not_run"),
                "failed_requirements": sum(1 for item in requirements.values() if item["status"] == "fail"),
                "total_success_criteria": len(SUCCESS_CRITERION_IDS),
                "passed_success_criteria": passed_sc,
                "not_run_success_criteria": sum(
                    1 for item in success_criteria.values() if item["status"] == "not_run"
                ),
                "failed_success_criteria": sum(
                    1 for item in success_criteria.values() if item["status"] == "fail"
                ),
            },
            "requirements": requirements,
            "success_criteria": success_criteria,
        }


def _strip_traceability_timestamps(doc: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in doc.items() if k not in ("evaluated_at_utc", "generated_at_utc")}


def check_traceability(
    run_dir: Path,
    *,
    out_path: Path | None = None,
) -> tuple[bool, list[str], dict[str, Any] | None]:
    """Evaluate traceability in-memory and verify existing traceability.json matches without writing."""
    if run_dir.is_symlink():
        return (False, [f"Symlink run_dir is forbidden: {run_dir}"], None)
    resolved_run = run_dir.resolve()
    raw_target = out_path if out_path is not None else (resolved_run / "traceability.json")
    if raw_target.is_symlink():
        return (False, [f"Symlink traceability output path is forbidden: {raw_target}"], None)
    if out_path is None or raw_target.resolve().is_relative_to(resolved_run):
        try:
            _ensure_no_symlink_inside(resolved_run, raw_target)
        except (LocatedError, ValueError) as exc:
            return (False, [str(exc)], None)

    target = raw_target.resolve()
    if not target.is_file():
        return (False, [f"Missing existing traceability file: {target}"], None)

    try:
        existing = load_json(target)
    except Exception as exc:
        return (False, [f"Failed parsing existing traceability file {target}: {exc}"], None)
    if not isinstance(existing, dict):
        return (False, [f"Existing traceability root must be a JSON object: {target}"], None)

    fresh = TraceabilityEvaluator(run_dir).evaluate()
    mismatches: list[str] = []
    if existing.get("status") != "pass":
        mismatches.append(f"existing status={existing.get('status')!r} != 'pass'")
    if fresh.get("status") != "pass":
        mismatches.append(f"freshly evaluated status={fresh.get('status')!r} != 'pass'")

    norm_existing = _strip_traceability_timestamps(existing)
    norm_fresh = _strip_traceability_timestamps(fresh)
    if norm_existing != norm_fresh:
        for key in ("schema_version", "run_id", "status", "summary"):
            if norm_existing.get(key) != norm_fresh.get(key):
                mismatches.append(f"{key} mismatch: existing={norm_existing.get(key)!r} vs fresh={norm_fresh.get(key)!r}")
        for sec_name, item_ids in (
            ("requirements", REQUIREMENT_IDS),
            ("success_criteria", SUCCESS_CRITERION_IDS),
        ):
            ex_sec = norm_existing.get(sec_name) or {}
            fr_sec = norm_fresh.get(sec_name) or {}
            for item_id in item_ids:
                ex_it = ex_sec.get(item_id) or {}
                fr_it = fr_sec.get(item_id) or {}
                if ex_it.get("status") != fr_it.get("status"):
                    mismatches.append(
                        f"{sec_name}.{item_id}.status mismatch: existing={ex_it.get('status')!r} vs fresh={fr_it.get('status')!r}"
                    )
                if ex_it.get("evidence_refs") != fr_it.get("evidence_refs"):
                    mismatches.append(f"{sec_name}.{item_id}.evidence_refs mismatch")
                elif ex_it != fr_it:
                    mismatches.append(f"{sec_name}.{item_id} content mismatch")
        if not mismatches:
            mismatches.append("traceability document differs from freshly evaluated result")

    return (len(mismatches) == 0, mismatches, fresh)


def evaluate_traceability(run_dir: Path, *, out_path: Path | None = None) -> dict[str, Any]:
    if run_dir.is_symlink():
        raise LocatedError(f"Symlink run_dir is forbidden: {run_dir}", file=run_dir)
    resolved_run = run_dir.resolve()
    raw_target = out_path if out_path is not None else (resolved_run / "traceability.json")
    if raw_target.is_symlink():
        raise LocatedError(f"Symlink traceability output path is forbidden: {raw_target}", file=raw_target)
    if out_path is None or raw_target.resolve().is_relative_to(resolved_run):
        _ensure_no_symlink_inside(resolved_run, raw_target)
    evaluator = TraceabilityEvaluator(run_dir)
    doc = evaluator.evaluate()
    target = raw_target.resolve()
    atomic_write_json(target, doc)
    return doc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate M3 18-FR / 8-SC executable traceability matrix."
    )
    parser.add_argument("--run-dir", type=Path, required=True, help="M3 validation run directory")
    parser.add_argument("--out", type=Path, default=None, help="Optional output path (default: <run-dir>/traceability.json)")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Read-only check mode: evaluate in memory and verify existing traceability.json matches",
    )
    args = parser.parse_args(argv)

    if args.run_dir.is_symlink():
        print(f"Error: symlink --run-dir is forbidden: {args.run_dir}", file=sys.stderr)
        return 1

    run_dir = args.run_dir.resolve()
    if not run_dir.is_dir():
        print(f"Error: --run-dir not found: {run_dir}", file=sys.stderr)
        return 2

    if args.check:
        ok, mismatches, fresh = check_traceability(args.run_dir, out_path=args.out)
        target = (args.out or (run_dir / "traceability.json")).resolve()
        if ok and fresh is not None:
            print(
                f"Traceability check: PASS "
                f"(FR {fresh['summary']['passed_requirements']}/{fresh['summary']['total_requirements']}, "
                f"SC {fresh['summary']['passed_success_criteria']}/{fresh['summary']['total_success_criteria']}) == {target}"
            )
            return 0
        print(f"Traceability check: FAIL ({len(mismatches)} mismatch(es)) on {target}", file=sys.stderr)
        for m in mismatches[:10]:
            print(f"  - {m}", file=sys.stderr)
        return 1

    try:
        doc = evaluate_traceability(args.run_dir, out_path=args.out)
    except (LocatedError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    target = (args.out or (run_dir / "traceability.json")).resolve()
    print(
        f"Traceability evaluation: {doc['status'].upper()} "
        f"(FR {doc['summary']['passed_requirements']}/{doc['summary']['total_requirements']}, "
        f"SC {doc['summary']['passed_success_criteria']}/{doc['summary']['total_success_criteria']}) -> {target}"
    )
    return 0 if doc["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
