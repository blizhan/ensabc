#!/usr/bin/env python3
"""Verification harness for M2 matching core and m2_runner against Python Fraction oracle.

Executes:
1. Fixed synthetic positive and negative contract test cases
2. 4,000 randomized small test cases (1..5 scenarios per side) vs Python Fraction oracle
3. 2,000+ permutation invariance checks (cluster IDs and row ordering permuted)
4. Power-of-2 bitwise invariant and general positive weight scaling tests
5. Writes evidence records:
   - <run-dir>/core/synthetic-oracle.json
   - <run-dir>/core/permutation-invariance.json
   - <run-dir>/core/error-cases.json
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m2.common import (
    LocatedError,
    atomic_write_json,
    create_run_dir,
    load_json,
    sha256_file,
    sha256_json,
)
from validation.m2.oracle import (
    compute_ensemble_dispersion_exact,
    generate_random_oracle_cases,
    solve_oracle,
)


def clean_match_payload(case: dict[str, Any]) -> dict[str, Any]:
    """Strip fixture-only metadata keys before sending strict DTO to m2_runner."""
    allowed = {
        "schema_version",
        "case_id",
        "task_id",
        "mode",
        "prev_scenarios",
        "curr_scenarios",
        "weights",
        "threshold",
        "threshold_coefficient",
        "prev_dispersion",
        "curr_dispersion",
    }
    return {k: v for k, v in case.items() if k in allowed}


def assert_full_match_agreement(case_id: str, rust_res: dict[str, Any], oracle_res: dict[str, Any]) -> None:
    """Assert exact field-by-field agreement between Rust core and Fraction oracle."""
    if rust_res["objective"]["matched_count"] != oracle_res["objective"]["matched_count"]:
        raise AssertionError(
            f"{case_id} count mismatch: rust={rust_res['objective']['matched_count']} vs oracle={oracle_res['objective']['matched_count']}"
        )
    if rust_res["objective"]["weighted_distance_sum"] != oracle_res["objective"]["weighted_distance_sum"]:
        raise AssertionError(
            f"{case_id} sum mismatch: rust={rust_res['objective']['weighted_distance_sum']!r} vs oracle={oracle_res['objective']['weighted_distance_sum']!r}"
        )
    if rust_res["optima_count"] != oracle_res["optima_count"]:
        raise AssertionError(
            f"{case_id} optima_count mismatch: rust={rust_res['optima_count']} vs oracle={oracle_res['optima_count']}"
        )
    if rust_res["distance_matrix"] != oracle_res["distance_matrix"]:
        raise AssertionError(f"{case_id} distance_matrix mismatch")

    rust_links = [
        {
            "prev_cluster_id": l["prev_cluster_id"],
            "curr_cluster_id": l["curr_cluster_id"],
            "distance_k": l["distance_k"],
            "substitutability": l["substitutability"],
            "ambiguous": l["ambiguous"],
        }
        for l in rust_res["links"]
    ]
    if rust_links != oracle_res["links"]:
        raise AssertionError(f"{case_id} links mismatch:\n  rust={rust_links}\n  oracle={oracle_res['links']}")
    if rust_res["unmatched_prev"] != oracle_res["unmatched_prev"]:
        raise AssertionError(
            f"{case_id} unmatched_prev mismatch:\n  rust={rust_res['unmatched_prev']}\n  oracle={oracle_res['unmatched_prev']}"
        )
    if rust_res["unmatched_curr"] != oracle_res["unmatched_curr"]:
        raise AssertionError(
            f"{case_id} unmatched_curr mismatch:\n  rust={rust_res['unmatched_curr']}\n  oracle={oracle_res['unmatched_curr']}"
        )


def run_rust_runner(
    runner_bin: Path,
    mode: str,
    input_data: dict[str, Any],
    *,
    scaled: bool = False,
) -> tuple[int, dict[str, Any] | None, str]:
    """Invoke m2_runner binary via temp files."""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f_in, \
         tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f_out:
        in_path = Path(f_in.name)
        out_path = Path(f_out.name)
        json.dump(input_data, f_in)

    try:
        cmd = [str(runner_bin), "--mode", mode, "--input", str(in_path), "--output", str(out_path)]
        if scaled:
            cmd.append("--scaled")
        proc = subprocess.run(cmd, capture_output=True, text=True)
        out_data = None
        if proc.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0:
            with open(out_path, "r", encoding="utf-8") as f:
                out_data = json.load(f)
        return (proc.returncode, out_data, proc.stderr)
    finally:
        if in_path.exists():
            in_path.unlink()
        if out_path.exists():
            out_path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify matching core against independent oracle")
    parser.add_argument("--run-dir", type=Path, required=True, help="Exclusive run directory")
    parser.add_argument("--runner", type=Path, default=None, help="Path to compiled m2_runner executable")
    args = parser.parse_args()

    run_dir = args.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    core_dir = run_dir / "core"
    core_dir.mkdir(parents=True, exist_ok=True)

    # Locate or compile m2_runner
    runner_bin = args.runner
    if runner_bin is None or not runner_bin.is_file():
        built_runner = REPO_ROOT / "target" / "release" / "examples" / "m2_runner"
        if not built_runner.is_file():
            print("Compiling m2_runner in release mode...")
            res = subprocess.run(
                ["cargo", "build", "--locked", "--release", "-p", "ensabc-core", "--example", "m2_runner"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                print(f"Failed to build m2_runner: {res.stderr}", file=sys.stderr)
                return 2
        runner_bin = built_runner

    # 1. Fixed synthetic cases
    synth_path = REPO_ROOT / "validation" / "m2" / "fixtures" / "synthetic_match_cases.json"
    if not synth_path.is_file():
        print(f"Missing fixture: {synth_path}", file=sys.stderr)
        return 2
    synth_data = load_json(synth_path)

    pos_results = []
    for c in synth_data["positive_cases"]:
        cid = c["case_id"]
        oracle_res = solve_oracle(c)
        code, rust_res, stderr = run_rust_runner(runner_bin, "match", clean_match_payload(c))
        if code != 0 or rust_res is None:
            print(f"FAIL: {cid} failed in runner: {stderr}", file=sys.stderr)
            return 1
        assert_full_match_agreement(cid, rust_res, oracle_res)
        pos_results.append({
            "case_id": cid,
            "status": "pass",
            "matched_count": rust_res["objective"]["matched_count"],
            "weighted_distance_sum": rust_res["objective"]["weighted_distance_sum"],
            "optima_count": rust_res["optima_count"],
            "links_count": len(rust_res["links"]),
        })

    # Verify dispersion mode against exact flat-fsum Python oracle (including flat-vs-nested counterexample)
    d_small = math.sqrt(0.49 * math.ulp(2.0))
    disp_members = [
        {"member_index": i, "member_id": f"m{i}", "features": [1.0 if i % 2 == 0 else -1.0, d_small if i % 2 == 0 else -d_small]}
        for i in range(6)
    ]
    disp_weights = [2.0, 1.0]
    expected_disp = compute_ensemble_dispersion_exact([m["features"] for m in disp_members], disp_weights)
    nested_q = math.fsum(math.fsum([disp_weights[0] * (m["features"][0] ** 2), disp_weights[1] * (m["features"][1] ** 2)]) for m in disp_members)
    nested_disp = math.sqrt((nested_q / 6.0) / 3.0)
    assert expected_disp != nested_disp, "flat vs nested counterexample must differ"
    d_code, d_res, d_err = run_rust_runner(runner_bin, "dispersion", {"weights": disp_weights, "members": disp_members})
    if d_code != 0 or d_res is None or d_res["dispersion"] != expected_disp:
        print(f"FAIL: dispersion flat-fsum check failed: code={d_code} res={d_res} expected={expected_disp!r} err={d_err}", file=sys.stderr)
        return 1

    # Verify UTF-8 non-ASCII input does not panic in m2_runner
    utf8_case = {
        "case_id": "中文_°N_测试",
        "mode": "absolute",
        "weights": [1.0],
        "threshold": 1.0,
        "prev_scenarios": [{"cluster_id": 0, "mean_features": [280.0]}],
        "curr_scenarios": [{"cluster_id": 0, "mean_features": [280.5]}],
    }
    u_code, u_res, u_err = run_rust_runner(runner_bin, "match", utf8_case)
    if u_code != 0 or u_res is None or u_res["objective"]["matched_count"] != 1:
        print(f"FAIL: UTF-8 runner check failed: code={u_code} err={u_err}", file=sys.stderr)
        return 1

    # Negative error cases
    err_results = []
    for c in synth_data["invalid_cases"]:
        cid = c["case_id"]
        exp_code = c["expected_code"]
        # Generate invalid payload matching case
        bad_task = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 1.0,
            "prev_scenarios": [{"cluster_id": 0, "mean_features": [1.0]}],
            "curr_scenarios": [{"cluster_id": 0, "mean_features": [1.0]}],
        }
        if cid == "empty_prev_scenarios":
            bad_task["prev_scenarios"] = []
        elif cid == "empty_curr_scenarios":
            bad_task["curr_scenarios"] = []
        elif cid == "ragged_features":
            bad_task["curr_scenarios"].append({"cluster_id": 1, "mean_features": [1.0, 2.0]})
        elif cid == "feature_length_mismatch_weights":
            bad_task["weights"] = [1.0, 1.0]
        elif cid == "nan_feature":
            bad_task["prev_scenarios"][0]["mean_features"] = [float("nan")]
        elif cid == "inf_feature":
            bad_task["prev_scenarios"][0]["mean_features"] = [float("inf")]
        elif cid == "negative_weight":
            bad_task["weights"] = [-1.0]
        elif cid == "zero_weight_sum":
            bad_task["weights"] = [0.0]
        elif cid == "negative_threshold":
            bad_task["threshold"] = -1.0
        elif cid == "zero_dispersion":
            bad_task = {
                "mode": "scaled",
                "weights": [1.0],
                "threshold_coefficient": 1.0,
                "prev_dispersion": 0.0,
                "curr_dispersion": 1.0,
                "prev_scenarios": [{"cluster_id": 0, "mean_features": [1.0]}],
                "curr_scenarios": [{"cluster_id": 0, "mean_features": [1.0]}],
            }
        elif cid == "exceed_scenario_limit_13":
            bad_task["prev_scenarios"] = [{"cluster_id": i, "mean_features": [float(i)]} for i in range(13)]

        code, _, stderr = run_rust_runner(runner_bin, "match", bad_task)
        if code == 0:
            print(f"FAIL: invalid case {cid} unexpectedly succeeded", file=sys.stderr)
            return 1
        if exp_code not in stderr:
            print(f"FAIL: invalid case {cid} did not emit expected code {exp_code}, got: {stderr}", file=sys.stderr)
            return 1
        err_results.append({
            "case_id": cid,
            "expected_code": exp_code,
            "actual_stderr": stderr.strip(),
            "status": "pass",
        })

    # 2. 4,000 random oracle cases (generated deterministically in cache, never committed to fixtures/)
    rand_path = REPO_ROOT / "validation" / "m2" / ".cache" / "random_oracle_cases.json"
    if not rand_path.is_file():
        print("Generating 4,000 deterministic random oracle cases in cache...")
        cases = generate_random_oracle_cases(4000)
        atomic_write_json(rand_path, {"cases_count": len(cases), "cases": cases})
    else:
        cases = load_json(rand_path)["cases"]

    print(f"Testing {len(cases)} randomized oracle cases...")
    oracle_stats = {"cases_tested": len(cases), "matched": 0, "mismatches": 0}
    for c in cases:
        oracle_res = solve_oracle(c)
        code, rust_res, stderr = run_rust_runner(runner_bin, "match", clean_match_payload(c))
        if code != 0 or rust_res is None:
            print(f"FAIL: random case {c['case_id']} failed in runner: {stderr}", file=sys.stderr)
            return 1
        assert_full_match_agreement(c["case_id"], rust_res, oracle_res)
        oracle_stats["matched"] += 1

    # 3. Permutation invariance (2,000 checks comparing full physical links, substitutability & unmatched)
    print("Testing 2,000 permutation invariance checks...")
    perm_rnd = random.Random(424242)
    perm_stats = {"permutations_tested": 2000, "invariant_count": 0}
    for p_idx in range(2000):
        base_case = cases[p_idx % len(cases)]
        m = len(base_case["prev_scenarios"])
        n = len(base_case["curr_scenarios"])

        p_perm = list(range(m))
        c_perm = list(range(n))
        perm_rnd.shuffle(p_perm)
        perm_rnd.shuffle(c_perm)

        perm_case = {
            "mode": "absolute",
            "weights": list(base_case["weights"]),
            "threshold": base_case["threshold"],
            "prev_scenarios": [
                {
                    "cluster_id": base_case["prev_scenarios"][i]["cluster_id"] + 100,
                    "mean_features": list(base_case["prev_scenarios"][i]["mean_features"]),
                }
                for i in p_perm
            ],
            "curr_scenarios": [
                {
                    "cluster_id": base_case["curr_scenarios"][j]["cluster_id"] + 200,
                    "mean_features": list(base_case["curr_scenarios"][j]["mean_features"]),
                }
                for j in c_perm
            ],
        }

        code_base, res_base, _ = run_rust_runner(runner_bin, "match", clean_match_payload(base_case))
        code_perm, res_perm, _ = run_rust_runner(runner_bin, "match", perm_case)
        assert code_base == 0 and code_perm == 0
        assert res_base is not None and res_perm is not None

        if res_base["objective"] != res_perm["objective"] or res_base["optima_count"] != res_perm["optima_count"]:
            print(f"FAIL: Permutation changed objective/optima: base={res_base['objective']} perm={res_perm['objective']}", file=sys.stderr)
            return 1

        base_links = sorted(
            (l["prev_cluster_id"], l["curr_cluster_id"], l["distance_k"], l["substitutability"], l["ambiguous"])
            for l in res_base["links"]
        )
        perm_links = sorted(
            (l["prev_cluster_id"] - 100, l["curr_cluster_id"] - 200, l["distance_k"], l["substitutability"], l["ambiguous"])
            for l in res_perm["links"]
        )
        if base_links != perm_links:
            print(f"FAIL: Permutation changed physical links:\n  base={base_links}\n  perm={perm_links}", file=sys.stderr)
            return 1

        base_un_p = sorted((u["cluster_id"], u["reason"], u["matched_in_some_optimum"]) for u in res_base["unmatched_prev"])
        perm_un_p = sorted((u["cluster_id"] - 100, u["reason"], u["matched_in_some_optimum"]) for u in res_perm["unmatched_prev"])
        base_un_c = sorted((u["cluster_id"], u["reason"], u["matched_in_some_optimum"]) for u in res_base["unmatched_curr"])
        perm_un_c = sorted((u["cluster_id"] - 200, u["reason"], u["matched_in_some_optimum"]) for u in res_perm["unmatched_curr"])
        if base_un_p != perm_un_p or base_un_c != perm_un_c:
            print("FAIL: Permutation changed unmatched diagnosis", file=sys.stderr)
            return 1
        perm_stats["invariant_count"] += 1

    # 4. Weight scaling invariance (exact bitwise for 2^k; rtol <= 1e-15 on distances for general alpha)
    print("Testing weight scaling invariance...")
    scale_stats = {
        "power_of_two_tested": 100,
        "general_scaling_tested": 100,
        "max_general_rtol": 0.0,
        "flips_recorded": 0,
    }
    for s_idx in range(100):
        base_case = clean_match_payload(cases[s_idx])
        # 2^k scaling: bitwise identical distances, objective, and links
        scaled_2k = dict(base_case)
        scaled_2k["weights"] = [w * 1024.0 for w in base_case["weights"]]
        _, res_base, _ = run_rust_runner(runner_bin, "match", base_case)
        _, res_2k, _ = run_rust_runner(runner_bin, "match", scaled_2k)
        assert res_base is not None and res_2k is not None
        assert res_base["objective"] == res_2k["objective"]
        assert res_base["distance_matrix"] == res_2k["distance_matrix"]
        assert res_base["links"] == res_2k["links"]

        # General scaling alpha = 7.3: every distance cell must be within rtol = 1e-15
        scaled_gen = dict(base_case)
        scaled_gen["weights"] = [w * 7.3 for w in base_case["weights"]]
        _, res_gen, _ = run_rust_runner(runner_bin, "match", scaled_gen)
        assert res_gen is not None
        for row_b, row_g in zip(res_base["distance_matrix"], res_gen["distance_matrix"]):
            for db, dg in zip(row_b, row_g):
                if db == 0.0:
                    assert dg == 0.0, f"Zero distance changed under scaling: {dg}"
                else:
                    rtol = abs(db - dg) / abs(db)
                    scale_stats["max_general_rtol"] = max(scale_stats["max_general_rtol"], rtol)
                    assert rtol <= 1e-15, f"General scaling exceeded rtol 1e-15: {rtol} ({db} vs {dg})"
        if [(l["prev_cluster_id"], l["curr_cluster_id"]) for l in res_base["links"]] != [
            (l["prev_cluster_id"], l["curr_cluster_id"]) for l in res_gen["links"]
        ]:
            scale_stats["flips_recorded"] += 1

    # Write evidence files
    atomic_write_json(core_dir / "synthetic-oracle.json", {
        "status": "pass",
        "positive_cases_count": len(pos_results),
        "dispersion_flat_fsum_verified": True,
        "utf8_input_verified": True,
        "random_oracle_stats": oracle_stats,
        "positive_results": pos_results,
    })
    atomic_write_json(core_dir / "permutation-invariance.json", {
        "status": "pass",
        "permutation_stats": perm_stats,
        "weight_scaling_stats": scale_stats,
    })
    atomic_write_json(core_dir / "error-cases.json", {
        "status": "pass",
        "error_cases_count": len(err_results),
        "cases": err_results,
    })

    print(f"Verification complete: all checks passed. Evidence saved to {core_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
