#!/usr/bin/env python3
"""Contract and negative tamper test suite for M3 visualization & gate audit (T036).

Tests:
- Valid 8-batch 11-table DuckDB showcase (continuation, split, merge, emergence,
  disappearance, `missing` & `failed` gap barriers, `ambiguous_tie`, `high_uncertainty`)
  rendering cleanly and passing all 5 live verification passes + `--self-check`.
- Individual 100% rejection (`exit 1`) on all 10 tamper mutations (including
  simultaneous SVG + JSON distance mutation vs DuckDB ground truth, removed AND
  `display:none` CSS/inline hidden disclaimer, cross-gap edges, orphan `<path>`,
  secondary->primary swap, external URLs, stripped margins/rule_basis, wrong run_id).
- Missing/NULL/non-finite provenance pre-render gate (`MISSING_PROVENANCE_FIELD`,
  exit code `1`, `0` bytes at `--output`, zero `.tmp_*` residue).
- Prohibited affirmative claim interception (physical member migration, physical
  cyclogenesis, calibrated probability, trading signal/profitability) and gate
  evidence negative contracts (missing steps, `../` path traversal, SHA-256 mismatch,
  forged `run_key[:12]:` branch_id prefix).
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    atomic_write,
    collect_effective_source_manifest,
)
from validation.m3.render_evolution_html import (
    SCHEMA_VERSION,
    atomic_write_json,
    main as render_main,
    run_isolated_duckdb,
    sha256_file,
)
from validation.m3.traceability import (
    TraceabilityEvaluator,
    evaluate_traceability,
    main as traceability_main,
)
from validation.m3.verify_report import (
    _build_mock_passing_bundle,
    audit_report,
    compose_report_dict,
    main as verify_report_main,
)
from validation.m3.verify_visualization_traceability import (
    SHOWCASE_RUN_ID,
    SHOWCASE_TRACKING_KEY,
    TAMPER_MUTATIONS,
    _extract_svg_xml,
    _replace_provenance_json,
    audit_evidence_bundle_contract,
    main as verify_main,
    run_self_check,
    seed_showcase_duckdb,
    verify_branch_id_run_prefix,
    verify_html_traceability,
)


class TestM3VisualizationAndTamperSuite(unittest.TestCase):
    """Positive and negative visualization traceability & tamper tests."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp_ctx = tempfile.TemporaryDirectory(dir="/tmp", prefix="ensabc-m3-t036-")
        cls.work_dir = Path(cls._tmp_ctx.name)
        cls.db_path = cls.work_dir / "showcase.duckdb"
        seed_showcase_duckdb(cls.db_path)
        cls.valid_html = cls.work_dir / "showcase.html"
        rc = render_main(
            [
                "--db",
                str(cls.db_path),
                "--evolution-run-id",
                SHOWCASE_RUN_ID,
                "--output",
                str(cls.valid_html),
            ]
        )
        assert rc == 0, f"Valid showcase render failed with exit code {rc}"
        cls.baseline_html_text = cls.valid_html.read_text(encoding="utf-8")

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmp_ctx.cleanup()

    def test_valid_8batch_showcase_passes_all_passes(self) -> None:
        audit_out = self.work_dir / "valid_audit.json"
        rc = verify_main(
            [
                "--html",
                str(self.valid_html),
                "--db",
                str(self.db_path),
                "--evolution-run-id",
                SHOWCASE_RUN_ID,
                "--out-report",
                str(audit_out),
            ]
        )
        self.assertEqual(rc, 0)
        self.assertTrue(audit_out.is_file())
        report = json.loads(audit_out.read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["findings"], [])
        for pass_name, pass_res in report["passes"].items():
            self.assertEqual(pass_res["status"], "pass", msg=f"{pass_name} failed: {pass_res}")
        self.assertEqual(report["passes"]["pass_2_svg_xml_zero_orphans"]["orphan_primitives_count"], 0)
        self.assertEqual(report["passes"]["pass_2_svg_xml_zero_orphans"]["svg_batches_count"], 8)
        self.assertEqual(report["passes"]["pass_2_svg_xml_zero_orphans"]["svg_gap_markers_count"], 2)
        self.assertEqual(report["passes"]["pass_4_duckdb_ground_truth"]["db_nodes_verified"], 16)
        self.assertEqual(report["passes"]["pass_4_duckdb_ground_truth"]["db_edges_verified"], 9)
        self.assertEqual(report["passes"]["pass_4_duckdb_ground_truth"]["db_events_verified"], 18)
        self.assertEqual(report["passes"]["pass_4_duckdb_ground_truth"]["db_branches_verified"], 9)

    def test_svg_node_height_proportional_to_member_fraction_and_badges_present(self) -> None:
        svg_root = ET.fromstring(_extract_svg_xml(self.baseline_html_text))
        ratios: list[float] = []
        seen_badges: set[str] = set()
        for elem in svg_root.iter():
            classes = set(elem.attrib.get("class", "").split())
            if "evo-node" in classes:
                mf = float(elem.attrib["data-member-fraction"])
                body_rects = [
                    ch
                    for ch in elem
                    if "evo-node-body" in set(ch.attrib.get("class", "").split())
                ]
                self.assertEqual(len(body_rects), 1)
                height = float(body_rects[0].attrib["height"])
                ratios.append(height / mf)
            if "evo-node-badge" in classes:
                seen_badges.add(elem.attrib.get("data-badge-kind", ""))

        self.assertGreaterEqual(len(ratios), 16)
        for r in ratios[1:]:
            self.assertAlmostEqual(r, ratios[0], places=6)
        self.assertEqual(
            seen_badges,
            {"emerged", "disappeared", "gap_interrupted", "gap_restarted", "high_uncertainty"},
        )

    def test_all_10_mandatory_tamper_mutations_individually_exit_1(self) -> None:
        self.assertEqual(len(TAMPER_MUTATIONS), 18)
        for idx, (case_name, expected_pass, mutator) in enumerate(TAMPER_MUTATIONS, start=1):
            with self.subTest(idx=idx, case=case_name):
                mutated_html, run_id = mutator(self.baseline_html_text, SHOWCASE_RUN_ID)
                tamper_file = self.work_dir / f"unit_{case_name}.html"
                tamper_file.write_text(mutated_html, encoding="utf-8")

                rc = verify_main(
                    [
                        "--html",
                        str(tamper_file),
                        "--db",
                        str(self.db_path),
                        "--evolution-run-id",
                        run_id,
                    ]
                )
                self.assertEqual(rc, 1, msg=f"{case_name} did not return exit code 1")

                audit = verify_html_traceability(tamper_file, self.db_path, run_id)
                self.assertEqual(audit["status"], "fail")
                self.assertEqual(
                    audit["passes"][expected_pass]["status"],
                    "fail",
                    msg=f"{case_name} was not caught by {expected_pass}: {audit['passes']}",
                )
                # Specifically verify Tamper #4 passes internal SVG<->JSON checks and is caught by Pass 4!
                if case_name == "tamper_04_simultaneous_svg_and_json_distance_drift":
                    self.assertEqual(audit["passes"]["pass_1_offline_csp"]["status"], "pass")
                    self.assertEqual(audit["passes"]["pass_2_svg_xml_zero_orphans"]["status"], "pass")
                    self.assertEqual(audit["passes"]["pass_3_svg_json_bijection"]["status"], "pass")
                    self.assertEqual(audit["passes"]["pass_5_disclaimer_and_claims"]["status"], "pass")
                    self.assertEqual(audit["passes"]["pass_4_duckdb_ground_truth"]["status"], "fail")

    def test_disclaimer_removal_css_hide_and_details_collapse_all_rejected(self) -> None:
        variants = {
            "removed": re.sub(
                r'<header id="ensabc-disclaimer-banner".*?</header>',
                "",
                self.baseline_html_text,
                count=1,
                flags=re.DOTALL,
            ),
            "css_stylesheet_hidden": self.baseline_html_text.replace(
                "</style>",
                "    #ensabc-disclaimer-banner { display: none; }\n  </style>",
                1,
            ),
            "wrapped_in_details": self.baseline_html_text.replace(
                '<header id="ensabc-disclaimer-banner"',
                '<details><summary>Disclaimer</summary><header id="ensabc-disclaimer-banner"',
                1,
            ).replace("</header>", "</header></details>", 1),
        }
        for name, html_variant in variants.items():
            with self.subTest(variant=name):
                p = self.work_dir / f"disclaimer_{name}.html"
                p.write_text(html_variant, encoding="utf-8")
                rc = verify_main(
                    ["--html", str(p), "--db", str(self.db_path), "--evolution-run-id", SHOWCASE_RUN_ID]
                )
                self.assertEqual(rc, 1)
                audit = verify_html_traceability(p, self.db_path, SHOWCASE_RUN_ID)
                self.assertEqual(audit["passes"]["pass_5_disclaimer_and_claims"]["status"], "fail")

    def test_adjacent_into_gap_batch_edge_injection_rejected(self) -> None:
        """Even an adjacent ordinal edge `b2:c0->b3:c0` into a missing/failed batch must fail Pass 2."""
        injected = (
            f'    <g class="evo-edge edge-role-primary" tabindex="0" '
            f'data-edge-id="b2:c0-&gt;b3:c0" data-evolution-run-id="{SHOWCASE_RUN_ID}" '
            f'data-prev-node-id="b2:c0" data-curr-node-id="b3:c0" data-edge-role="primary" '
            f'data-event-type="continuation" data-distance="0.2" data-distance-over-scale="0.2" '
            f'data-attributed-flow="0.2" data-ambiguous-tie="false" data-high-uncertainty="false">'
            f'<path d="M 400 120 L 600 120" stroke="#000" /></g>\n'
        )
        bad_html = self.baseline_html_text.replace(
            '<g class="evo-edges-layer">\n',
            '<g class="evo-edges-layer">\n' + injected,
            1,
        )
        p = self.work_dir / "adjacent_gap_edge.html"
        p.write_text(bad_html, encoding="utf-8")
        audit = verify_html_traceability(p, self.db_path, SHOWCASE_RUN_ID)
        self.assertEqual(audit["status"], "fail")
        self.assertEqual(audit["passes"]["pass_2_svg_xml_zero_orphans"]["status"], "fail")

    def test_missing_provenance_renders_exit_1_with_zero_output_bytes(self) -> None:
        # Case A: Absent evolution_run_id
        out_absent = self.work_dir / "absent_run_out.html"
        err_buf = io.StringIO()
        with contextlib.redirect_stderr(err_buf):
            rc = render_main(
                [
                    "--db",
                    str(self.db_path),
                    "--evolution-run-id",
                    "0000_does_not_exist",
                    "--output",
                    str(out_absent),
                ]
            )
        self.assertEqual(rc, 1)
        self.assertIn("MISSING_PROVENANCE_FIELD", err_buf.getvalue())
        self.assertFalse(out_absent.exists())

        # Case B: Zero nodes for existing evolution_run_id
        empty_nodes_db = self.work_dir / "empty_nodes.duckdb"
        # Copy DB and delete nodes/edges/events/branches via unconstrained copy
        sql_zero_nodes = f"""
        ATTACH '{self.db_path}' AS src (READ_ONLY);
        CREATE TABLE evolution_runs AS SELECT * FROM src.evolution_runs;
        CREATE TABLE replay_batches AS SELECT * FROM src.replay_batches;
        CREATE TABLE evolution_nodes AS SELECT * FROM src.evolution_nodes WHERE false;
        CREATE TABLE evolution_transitions AS SELECT * FROM src.evolution_transitions;
        CREATE TABLE evolution_edges AS SELECT * FROM src.evolution_edges WHERE false;
        CREATE TABLE evolution_events AS SELECT * FROM src.evolution_events WHERE false;
        CREATE TABLE evolution_branch_summaries AS SELECT * FROM src.evolution_branch_summaries WHERE false;
        DETACH src;
        """
        proc = run_isolated_duckdb(sql_zero_nodes, database=empty_nodes_db)
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)

        out_zero = self.work_dir / "zero_nodes_out.html"
        err_zero = io.StringIO()
        with contextlib.redirect_stderr(err_zero):
            rc_zero = render_main(
                [
                    "--db",
                    str(empty_nodes_db),
                    "--evolution-run-id",
                    SHOWCASE_RUN_ID,
                    "--output",
                    str(out_zero),
                ]
            )
        self.assertEqual(rc_zero, 1)
        self.assertIn("MISSING_PROVENANCE_FIELD", err_zero.getvalue())
        self.assertFalse(out_zero.exists())

        # Case C: NULL in required provenance/margin column (optimality_margin NULL) & NaN in distance
        for col_case, edge_projection in (
            ("null_margin", "SELECT * REPLACE (NULL::DOUBLE AS optimality_margin) FROM src.evolution_edges"),
            ("null_rule_basis", "SELECT * REPLACE (NULL::VARCHAR AS rule_basis) FROM src.evolution_edges"),
            ("nan_distance", "SELECT * REPLACE ('nan'::DOUBLE AS distance) FROM src.evolution_edges"),
        ):
            with self.subTest(col_case=col_case):
                bad_db = self.work_dir / f"bad_{col_case}.duckdb"
                sql_bad = f"""
                ATTACH '{self.db_path}' AS src (READ_ONLY);
                CREATE TABLE evolution_runs AS SELECT * FROM src.evolution_runs;
                CREATE TABLE replay_batches AS SELECT * FROM src.replay_batches;
                CREATE TABLE evolution_nodes AS SELECT * FROM src.evolution_nodes;
                CREATE TABLE evolution_transitions AS SELECT * FROM src.evolution_transitions;
                CREATE TABLE evolution_edges AS {edge_projection};
                CREATE TABLE evolution_events AS SELECT * FROM src.evolution_events;
                CREATE TABLE evolution_branch_summaries AS SELECT * FROM src.evolution_branch_summaries;
                DETACH src;
                """
                p_bad = run_isolated_duckdb(sql_bad, database=bad_db)
                self.assertEqual(p_bad.returncode, 0, msg=p_bad.stderr)
                out_bad = self.work_dir / f"out_{col_case}.html"
                err_bad = io.StringIO()
                with contextlib.redirect_stderr(err_bad):
                    rc_bad = render_main(
                        [
                            "--db",
                            str(bad_db),
                            "--evolution-run-id",
                            SHOWCASE_RUN_ID,
                            "--output",
                            str(out_bad),
                        ]
                    )
                self.assertEqual(rc_bad, 1)
                self.assertIn("MISSING_PROVENANCE_FIELD", err_bad.getvalue())
                self.assertFalse(out_bad.exists())

        # Ensure zero .tmp_* files left behind anywhere in work_dir
        leftover_tmps = list(self.work_dir.glob(".tmp_*"))
        self.assertEqual(leftover_tmps, [])

    def test_prohibited_overclaims_intercepted_in_html_and_json(self) -> None:
        bad_claims = (
            "本结果证实集合成员发生物理迁移。",
            "本结果代表天气系统的物理生成。",
            "This ribbon represents physical ensemble member migration.",
            "该比例属于已校准预报概率，可直接用于交易信号与交易收益。",
            "Provides calibrated forecast probabilities and proven trading alpha.",
        )
        for idx, claim in enumerate(bad_claims):
            with self.subTest(idx=idx, claim=claim):
                injected = self.baseline_html_text.replace(
                    "</main>",
                    f"<p>{claim}</p></main>",
                    1,
                )
                p = self.work_dir / f"prohibited_claim_{idx}.html"
                p.write_text(injected, encoding="utf-8")
                rc = verify_main(
                    ["--html", str(p), "--db", str(self.db_path), "--evolution-run-id", SHOWCASE_RUN_ID]
                )
                self.assertEqual(rc, 1)
                audit = verify_html_traceability(p, self.db_path, SHOWCASE_RUN_ID)
                self.assertEqual(audit["passes"]["pass_5_disclaimer_and_claims"]["status"], "fail")

        # Also verify prohibited claim hidden inside embedded JSON string is caught
        def mut_json_claim(payload: dict) -> None:
            payload["run"]["non_physical_disclaimer"] = "This proves physical ensemble member migration."

        json_claim_html = _replace_provenance_json(self.baseline_html_text, mut_json_claim)
        p_json = self.work_dir / "prohibited_claim_in_json.html"
        p_json.write_text(json_claim_html, encoding="utf-8")
        audit_json = verify_html_traceability(p_json, self.db_path, SHOWCASE_RUN_ID)
        self.assertEqual(audit_json["passes"]["pass_5_disclaimer_and_claims"]["status"], "fail")

    def test_subprocess_cli_entrypoints_render_and_verify(self) -> None:
        cli_html = self.work_dir / "cli_subproc.html"
        cli_report = self.work_dir / "cli_subproc_audit.json"
        r1 = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "validation" / "m3" / "render_evolution_html.py"),
                "--db",
                str(self.db_path),
                "--evolution-run-id",
                SHOWCASE_RUN_ID,
                "--output",
                str(cli_html),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(r1.returncode, 0, msg=r1.stderr)
        r2 = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "validation" / "m3" / "verify_visualization_traceability.py"),
                "--html",
                str(cli_html),
                "--db",
                str(self.db_path),
                "--evolution-run-id",
                SHOWCASE_RUN_ID,
                "--out-report",
                str(cli_report),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(r2.returncode, 0, msg=r2.stderr)
        self.assertEqual(json.loads(cli_report.read_text(encoding="utf-8"))["status"], "pass")

    def test_gate_audit_and_identity_prefix_negative_contracts(self) -> None:
        # 1. Authentic vs forged run_key[:12]: prefix on branch_id
        verify_branch_id_run_prefix(f"{SHOWCASE_TRACKING_KEY[:12]}:b00:k01", SHOWCASE_TRACKING_KEY)
        with self.assertRaises(ValueError):
            verify_branch_id_run_prefix("forged000000:b00:k01", SHOWCASE_TRACKING_KEY)

        # 2. Gate report audit: valid vs missing step vs ../ path traversal vs SHA-256 mismatch
        outer = self.work_dir / "audit_sandbox"
        run_dir = outer / "run_001"
        run_dir.mkdir(parents=True, exist_ok=True)
        outside_secret = outer / "secret.json"
        outside_secret.write_text("{}", encoding="utf-8")
        ev_file = run_dir / "evidence.json"
        ev_file.write_text('{"ok": true}\n', encoding="utf-8")
        ev_sha = sha256_file(ev_file)

        req_steps = {"verify_evolution_core", "verify_sql_and_history", "verify_visualization_traceability"}
        valid_report = {
            "steps": [{"step": s, "status": "pass", "exit_code": 0} for s in sorted(req_steps)],
            "requirements": {
                "FR-014": {
                    "status": "pass",
                    "evidence_refs": [{"path": "evidence.json", "sha256": ev_sha}],
                }
            },
            "success_criteria": {
                "SC-006": {
                    "status": "pass",
                    "evidence_refs": [{"path": "evidence.json", "sha256": ev_sha}],
                }
            },
        }
        self.assertEqual(
            audit_evidence_bundle_contract(run_dir, valid_report, required_steps=req_steps),
            [],
        )

        # Missing required step
        missing_step_rep = json.loads(json.dumps(valid_report))
        missing_step_rep["steps"] = missing_step_rep["steps"][1:]
        findings_step = audit_evidence_bundle_contract(
            run_dir, missing_step_rep, required_steps=req_steps
        )
        self.assertTrue(any("Missing or failed required gate step" in f for f in findings_step))

        # ../ Path traversal escape
        traversal_rep = json.loads(json.dumps(valid_report))
        traversal_rep["requirements"]["FR-014"]["evidence_refs"] = [
            {"path": "../secret.json", "sha256": sha256_file(outside_secret)}
        ]
        findings_trav = audit_evidence_bundle_contract(
            run_dir, traversal_rep, required_steps=req_steps
        )
        self.assertTrue(any("traversal" in f or "escapes" in f for f in findings_trav))

        # SHA-256 mismatch
        sha_rep = json.loads(json.dumps(valid_report))
        sha_rep["success_criteria"]["SC-006"]["evidence_refs"] = [
            {"path": "evidence.json", "sha256": "0" * 64}
        ]
        findings_sha = audit_evidence_bundle_contract(run_dir, sha_rep, required_steps=req_steps)
        self.assertTrue(any("SHA-256 mismatch" in f for f in findings_sha))

    def test_cli_self_check_passes_and_cleans_up_tmp(self) -> None:
        report_path = self.work_dir / "self_check_report.json"
        before_entries = set(Path("/tmp").glob("ensabc-m3-viz-selfcheck-*"))
        summary = run_self_check(out_report=report_path)
        after_entries = set(Path("/tmp").glob("ensabc-m3-viz-selfcheck-*"))

        self.assertEqual(summary["status"], "pass")
        self.assertEqual(summary["tamper_cases_tested"], 18)
        self.assertEqual(summary["tamper_cases_rejected"], 18)
        self.assertTrue(summary["incomplete_provenance_render_rejected"])
        self.assertTrue(summary["script_escape_verified"])
        self.assertTrue(summary["evolve_result_mismatch_caught"])
        self.assertTrue(summary["zero_ok_batch_all_gap_verified"])
        self.assertTrue(report_path.is_file())
        self.assertEqual(after_entries - before_entries, set())

    def test_h5_script_escaping_and_raw_angle_bracket_rejection(self) -> None:
        xss_db = self.work_dir / "unit_xss_gap.duckdb"
        proc = run_isolated_duckdb(
            f"""
            ATTACH '{self.db_path}' AS src (READ_ONLY);
            CREATE TABLE evolution_runs AS SELECT * FROM src.evolution_runs;
            CREATE TABLE replay_batches AS
              SELECT * REPLACE (
                CASE WHEN ordinal = 3 THEN 'MISSING_</script><script>alert(1)</script>' ELSE status_reason END AS status_reason
              ) FROM src.replay_batches;
            CREATE TABLE evolution_nodes AS SELECT * FROM src.evolution_nodes;
            CREATE TABLE evolution_transitions AS
              SELECT * REPLACE (
                CASE WHEN prev_ordinal = 2 THEN '</script><script>alert(1)</script> & <!--' ELSE gap_reason END AS gap_reason
              ) FROM src.evolution_transitions;
            CREATE TABLE evolution_edges AS SELECT * FROM src.evolution_edges;
            CREATE TABLE evolution_events AS SELECT * FROM src.evolution_events;
            CREATE TABLE evolution_branch_summaries AS SELECT * FROM src.evolution_branch_summaries;
            DETACH src;
            """,
            database=xss_db,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        out_html = self.work_dir / "unit_xss_safe.html"
        self.assertEqual(
            render_main(["--db", str(xss_db), "--evolution-run-id", SHOWCASE_RUN_ID, "--output", str(out_html)]),
            0,
        )
        html_txt = out_html.read_text(encoding="utf-8")
        m = re.search(
            r'<script\s+id="ensabc-evolution-provenance"\s+type="application/json">\s*(.*?)\s*</script>',
            html_txt,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(m)
        raw_prov = m.group(1)
        self.assertNotIn("<", raw_prov)
        self.assertNotIn(">", raw_prov)
        self.assertIn("\\u003c/script\\u003e", raw_prov)
        self.assertIn("\\u0026", raw_prov)

        audit = verify_html_traceability(out_html, xss_db, SHOWCASE_RUN_ID)
        self.assertEqual(audit["status"], "pass", msg=audit["findings"])

    def test_m8_evolve_result_mismatch_against_edges_and_events_caught_by_pass4(self) -> None:
        for sub_case, mut_expr in (
            (
                "edge_distance_mismatch",
                "CASE WHEN prev_ordinal = 0 THEN {'algorithm_version': 'm3.evolve.v1', 'summary': evolve_result.summary, 'edges': [{'prev_cluster_id': 0, 'curr_cluster_id': 0, 'edge_role': 'primary', 'event_type': 'continuation', 'rule_basis': 'global_optimum_primary', 'distance': 0.999, 'attributed_flow': 0.30}, evolve_result.edges[2], evolve_result.edges[3]], 'events': evolve_result.events} ELSE evolve_result END",
            ),
            (
                "events_count_mismatch",
                "CASE WHEN prev_ordinal = 1 THEN {'algorithm_version': 'm3.evolve.v1', 'summary': evolve_result.summary, 'edges': evolve_result.edges, 'events': []} ELSE evolve_result END",
            ),
        ):
            with self.subTest(sub_case=sub_case):
                bad_db = self.work_dir / f"m8_{sub_case}.duckdb"
                proc = run_isolated_duckdb(
                    f"""
                    ATTACH '{self.db_path}' AS src (READ_ONLY);
                    CREATE TABLE evolution_runs AS SELECT * FROM src.evolution_runs;
                    CREATE TABLE replay_batches AS SELECT * FROM src.replay_batches;
                    CREATE TABLE evolution_nodes AS SELECT * FROM src.evolution_nodes;
                    CREATE TABLE evolution_transitions AS SELECT * REPLACE ({mut_expr} AS evolve_result) FROM src.evolution_transitions;
                    CREATE TABLE evolution_edges AS SELECT * FROM src.evolution_edges;
                    CREATE TABLE evolution_events AS SELECT * FROM src.evolution_events;
                    CREATE TABLE evolution_branch_summaries AS SELECT * FROM src.evolution_branch_summaries;
                    DETACH src;
                    """,
                    database=bad_db,
                )
                self.assertEqual(proc.returncode, 0, msg=proc.stderr)
                audit = verify_html_traceability(self.valid_html, bad_db, SHOWCASE_RUN_ID)
                self.assertEqual(audit["status"], "fail")
                self.assertEqual(audit["passes"]["pass_1_offline_csp"]["status"], "pass")
                self.assertEqual(audit["passes"]["pass_2_svg_xml_zero_orphans"]["status"], "pass")
                self.assertEqual(audit["passes"]["pass_3_svg_json_bijection"]["status"], "pass")
                self.assertEqual(audit["passes"]["pass_5_disclaimer_and_claims"]["status"], "pass")
                self.assertEqual(audit["passes"]["pass_4_duckdb_ground_truth"]["status"], "fail")

    def test_m9_zero_ok_batch_all_gap_run_renders_and_passes_all_5_passes(self) -> None:
        gap_db = self.work_dir / "unit_zero_ok.duckdb"
        gap_erid = "9999888877776666555544443333222211110000aaaabbbbccccddddeeeeffff"
        gap_trk = "zerooktrk123456789"
        proc = run_isolated_duckdb(
            f"""
            ATTACH '{self.db_path}' AS src (READ_ONLY);
            CREATE TABLE evolution_runs AS
              SELECT
                '{gap_erid}' AS evolution_run_id,
                '{gap_trk}' AS tracking_run_key,
                {{
                  'tracking_run_key': '{gap_trk}',
                  'input_manifest_sha256': '1111111111111111111111111111111111111111111111111111111111111111',
                  'scenario_config_sha256': '2222222222222222222222222222222222222222222222222222222222222222',
                  'match_config_sha256': '3333333333333333333333333333333333333333333333333333333333333333',
                  'evolve_config_sha256': '7777777777777777777777777777777777777777777777777777777777777777',
                  'weights_version': 'cos_lat_center_v1',
                  'secondary_rule': 'nearest_feasible_v1',
                  'flow_rule': 'star_min_mass_proportional_v1',
                  'uncertainty_rule': 'm3.uncertainty.v1',
                  'fraction_sum_tolerance': 1e-9::DOUBLE,
                  'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
                  'high_uncertainty_optimality_ratio': 0.10::DOUBLE,
                  'high_uncertainty_secondary_ratio': 0.10::DOUBLE,
                  'software': src.evolution_runs.evolution_binding.software
                }} AS evolution_binding,
                weights,
                8 AS planned_batch_count,
                {{
                  'ok_batch_count': 0,
                  'missing_batch_count': 0,
                  'failed_batch_count': 8,
                  'ok_transition_count': 0,
                  'gap_transition_count': 7,
                  'branch_count': 0,
                  'primary_edge_count': 0,
                  'secondary_split_edge_count': 0,
                  'secondary_merge_edge_count': 0,
                  'continuation_count': 0,
                  'split_count': 0,
                  'merge_count': 0,
                  'emergence_count': 0,
                  'disappearance_count': 0,
                  'gap_interrupted_count': 0,
                  'gap_restarted_count': 0,
                  'cumulative_primary_distance': 0.0::DOUBLE,
                  'max_edge_distance': 0.0::DOUBLE,
                  'max_edge_distance_over_scale': 0.0::DOUBLE,
                  'mean_transition_tv_redistribution': 0.0::DOUBLE,
                  'max_transition_tv_redistribution': 0.0::DOUBLE,
                  'ambiguous_transition_count': 0,
                  'high_uncertainty_transition_count': 0,
                  'high_uncertainty_transition_ratio': 0.0::DOUBLE,
                  'high_uncertainty_edge_count': 0,
                  'high_uncertainty_edge_ratio': 0.0::DOUBLE
                }} AS chain_summary,
                non_physical_disclaimer,
                'abababababababababababababababababababababababababababababababab' AS content_sha256
              FROM src.evolution_runs;
            CREATE TABLE replay_batches AS
              SELECT '{gap_trk}' AS run_key, ordinal, init_time, lead_hours,
                'failed' AS status, 'INSUFFICIENT_RANK' AS status_reason,
                'in' || ordinal::VARCHAR AS input_sha256, 'om' || ordinal::VARCHAR AS om_sha256,
                NULL::DOUBLE AS dispersion, NULL::VARCHAR AS cluster_result_sha256
              FROM src.replay_batches;
            CREATE TABLE evolution_nodes AS SELECT * FROM src.evolution_nodes WHERE false;
            CREATE TABLE evolution_transitions AS
              SELECT '{gap_erid}' AS evolution_run_id, prev_ordinal, curr_ordinal,
                'gap' AS transition_status, true AS chain_broken, 'both_unavailable' AS gap_reason,
                NULL::DOUBLE AS pair_scale, NULL::DOUBLE AS threshold,
                0 AS primary_edge_count, 0 AS secondary_split_edge_count, 0 AS secondary_merge_edge_count,
                NULL::DOUBLE AS transition_tv_redistribution, NULL::DOUBLE AS min_threshold_margin,
                false AS ambiguous_tie, false AS high_uncertainty, NULL AS evolve_result
              FROM src.evolution_transitions;
            CREATE TABLE evolution_edges AS SELECT * FROM src.evolution_edges WHERE false;
            CREATE TABLE evolution_events AS SELECT * FROM src.evolution_events WHERE false;
            CREATE TABLE evolution_branch_summaries AS SELECT * FROM src.evolution_branch_summaries WHERE false;
            DETACH src;
            """,
            database=gap_db,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        gap_html = self.work_dir / "unit_zero_ok.html"
        self.assertEqual(
            render_main(["--db", str(gap_db), "--evolution-run-id", gap_erid, "--output", str(gap_html)]),
            0,
        )
        audit = verify_html_traceability(gap_html, gap_db, gap_erid)
        self.assertEqual(audit["status"], "pass", msg=audit["findings"])
        self.assertEqual(audit["passes"]["pass_2_svg_xml_zero_orphans"]["svg_batches_count"], 8)
        self.assertEqual(audit["passes"]["pass_2_svg_xml_zero_orphans"]["svg_gap_markers_count"], 8)
        self.assertEqual(audit["passes"]["pass_2_svg_xml_zero_orphans"]["svg_nodes_count"], 0)
        self.assertEqual(audit["passes"]["pass_2_svg_xml_zero_orphans"]["svg_edges_count"], 0)

    def test_h4_m7_m10_l3_manifest_symlink_traceability_and_report_audit(self) -> None:
        # [H4] code_manifest_sha256 invariant to .md changes and sensitive to .rs / .py changes
        fake_repo = self.work_dir / "fake_repo_h4"
        (fake_repo / "crates").mkdir(parents=True)
        (fake_repo / "validation" / "m3").mkdir(parents=True)
        (fake_repo / "specs" / "004-m3-scenario-evolution").mkdir(parents=True)
        (fake_repo / "Cargo.toml").write_text("[workspace]\n", encoding="utf-8")
        (fake_repo / "crates" / "lib.rs").write_text("pub fn v() -> i32 { 1 }\n", encoding="utf-8")
        (fake_repo / "validation" / "m3" / "tool.py").write_text("A = 1\n", encoding="utf-8")
        (fake_repo / "specs" / "004-m3-scenario-evolution" / "spec.md").write_text("# v1\n", encoding="utf-8")
        (fake_repo / "validation" / "m3" / "README.md").write_text("# doc v1\n", encoding="utf-8")

        m_base = collect_effective_source_manifest(fake_repo)
        (fake_repo / "specs" / "004-m3-scenario-evolution" / "spec.md").write_text("# v2 updated\n", encoding="utf-8")
        (fake_repo / "validation" / "m3" / "README.md").write_text("# doc v2 updated\n", encoding="utf-8")
        m_md_edited = collect_effective_source_manifest(fake_repo)
        self.assertNotEqual(m_md_edited["manifest_sha256"], m_base["manifest_sha256"])
        self.assertEqual(m_md_edited["code_manifest_sha256"], m_base["code_manifest_sha256"])

        (fake_repo / "crates" / "lib.rs").write_text("pub fn v() -> i32 { 2 }\n", encoding="utf-8")
        m_rs_edited = collect_effective_source_manifest(fake_repo)
        self.assertNotEqual(m_rs_edited["code_manifest_sha256"], m_md_edited["code_manifest_sha256"])

        (fake_repo / "validation" / "m3" / "tool.py").write_text("A = 2\n", encoding="utf-8")
        m_py_edited = collect_effective_source_manifest(fake_repo)
        self.assertNotEqual(m_py_edited["code_manifest_sha256"], m_rs_edited["code_manifest_sha256"])

        # Synthesize full passing mock bundle in /tmp
        bundle_dir = self.work_dir / "mock_gate_bundle"
        _build_mock_passing_bundle(bundle_dir)

        # [L3] traceability.py --check leaves traceability.json mtime & SHA-256 untouched
        tr_file = bundle_dir / "traceability.json"
        sha_before = sha256_file(tr_file)
        mtime_ns_before = tr_file.stat().st_mtime_ns
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rc_chk = traceability_main(["--run-dir", str(bundle_dir), "--check"])
        self.assertEqual(rc_chk, 0)
        self.assertEqual(sha256_file(tr_file), sha_before)
        self.assertEqual(tr_file.stat().st_mtime_ns, mtime_ns_before)

        # [L3] tampered traceability.json fails --check without overwriting
        tampered_tr = self.work_dir / "tampered_traceability.json"
        tr_obj = json.loads(tr_file.read_text(encoding="utf-8"))
        tr_obj["requirements"]["FR-017"]["evidence_refs"][0]["sha256"] = "0" * 64
        atomic_write_json(tampered_tr, tr_obj)
        tampered_sha = sha256_file(tampered_tr)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rc_chk_bad = traceability_main(
                ["--run-dir", str(bundle_dir), "--out", str(tampered_tr), "--check"]
            )
        self.assertEqual(rc_chk_bad, 1)
        self.assertEqual(sha256_file(tampered_tr), tampered_sha)

        # [H4] verify_report catches tampered source-manifest.json, build_manifest.json, and traceability.json
        self.assertEqual(audit_report(bundle_dir, write_audit=False)["audit_status"], "passed")
        for label, mutator in (
            (
                "source_manifest",
                lambda d: atomic_write_json(
                    d / "source-manifest.json",
                    {**json.loads((d / "source-manifest.json").read_text(encoding="utf-8")), "code_manifest_sha256": "a" * 64},
                ),
            ),
            (
                "build_manifest",
                lambda d: atomic_write(d / "build" / "m3_runner", b"corrupt"),
            ),
            (
                "traceability_post_report",
                lambda d: atomic_write_json(
                    d / "traceability.json",
                    {**json.loads((d / "traceability.json").read_text(encoding="utf-8")), "evaluated_at_utc": "2099-01-01T00:00:00Z"},
                ),
            ),
        ):
            with self.subTest(tamper=label):
                import shutil

                clone_d = self.work_dir / f"h4_clone_{label}"
                shutil.copytree(bundle_dir, clone_d)
                mutator(clone_d)
                aud = audit_report(clone_d, write_audit=False)
                self.assertEqual(aud["audit_status"], "failed", msg=aud)

        # [M7] Symlink run_dir and symlinked evidence file rejected by both traceability.py and verify_report.py
        sym_run = self.work_dir / "sym_run_dir"
        sym_run.symlink_to(bundle_dir)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(traceability_main(["--run-dir", str(sym_run)]), 1)
            self.assertEqual(traceability_main(["--run-dir", str(sym_run), "--check"]), 1)
            self.assertEqual(verify_report_main(["--run-dir", str(sym_run)]), 1)

        import shutil

        sym_ev_dir = self.work_dir / "sym_evidence_bundle"
        shutil.copytree(bundle_dir, sym_ev_dir)
        mr_p = sym_ev_dir / "minimal-runtime.json"
        mr_real = sym_ev_dir / "minimal-runtime.real.json"
        mr_p.rename(mr_real)
        mr_p.symlink_to(mr_real.name)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(traceability_main(["--run-dir", str(sym_ev_dir), "--check"]), 1)
            self.assertEqual(verify_report_main(["--run-dir", str(sym_ev_dir)]), 1)

        # [M10] Deepened TraceabilityEvaluator catches empty flip baseline_margins & false runtime route check
        m10_dir = self.work_dir / "m10_negative_bundle"
        shutil.copytree(bundle_dir, m10_dir)
        pm = json.loads((m10_dir / "case-study" / "perturbation-member-level.json").read_text(encoding="utf-8"))
        pm["boundary_flips"][0]["baseline_margins"] = {}
        atomic_write_json(m10_dir / "case-study" / "perturbation-member-level.json", pm)
        mr_doc = json.loads((m10_dir / "minimal-runtime.json").read_text(encoding="utf-8"))
        mr_doc["network_zero_routes_verified"] = False
        atomic_write_json(m10_dir / "minimal-runtime.json", mr_doc)
        ev_m10 = TraceabilityEvaluator(m10_dir).evaluate()
        self.assertEqual(ev_m10["requirements"]["FR-016"]["status"], "fail")
        self.assertEqual(ev_m10["requirements"]["FR-017"]["status"], "fail")
        self.assertEqual(ev_m10["success_criteria"]["SC-005"]["status"], "fail")
        self.assertEqual(ev_m10["success_criteria"]["SC-007"]["status"], "fail")


if __name__ == "__main__":
    unittest.main()
