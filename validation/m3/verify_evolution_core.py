#!/usr/bin/env python3
"""Cross-verification harness for M3 Rust evolution core (`m3_runner`) vs Python Fraction oracle.

Implements Tasks T015 and T022 (`specs/004-m3-scenario-evolution/contracts/validation-evidence.md`):
1. All 25 fixed `pair_cases` in `validation/m3/fixtures/synthetic_evolution_cases.json`:
   - 0-ULP IEEE-754 bitwise parity across backbone, edges, prev_nodes, curr_nodes, events, summary
   - Berge Structural Theorems 1-4 (including constructive inactive cross-edge removal invariance)
   - Scaled vs equivalent absolute threshold parity, UTF-8 non-ASCII safety, valid BackboneAssertion
   - Power-of-2 exact bitwise weight scaling and general positive constant weight scaling (`rtol <= 1e-15`)
2. Both `orthogonality_series`:
   - `pure_morphology_drift_series`: bitwise `+0.0` fraction deltas & TV redistribution + strictly monotonic distance
   - `pure_fraction_redistribution_series`: bitwise `+0.0` morphology distances + bitwise exact TV `[0.0, 0.25, 0.375]`
   - Isolated `0.05R` vs `0.15R` high-uncertainty bracketing across all 3 margin channels
3. All 4 multi-batch `chain_cases` (`m3_runner --mode chain` vs `solve_chain_oracle`):
   - 100% 0-ULP parity across batches, nodes, transitions, edges, events, `EvolutionBranchSummary`, and `EvolutionChainSummary`
   - Strict disjointness of `gap_interrupted` / `gap_restarted` from `disappearance` / `emergence` (`E2`-`E8`)
   - Raw `members` + `cluster_config` chain execution including whitelisted single-batch `failed` isolation
4. 4,000 deterministic seeded random pair cases (`generate_random_evolution_cases(count=4000, seed=20261125)`):
   - 0 differences (0-ULP bitwise float parity) across all 4,000 cases covering scaled & absolute modes
5. 2,000 random row/column/`cluster_id` permutations + all 144 (`3! * 4!`) permutations of `nontrivial_3x4_for_144_permutations`
   + explicit symmetric feature tie canonical tie-breaking check
6. All 19 fixture `negative_cases` + `DIMENSION_MISMATCH` + invalid chain/CLI checks covering all M2 + M3 error codes:
   - Single-item exit code `1` with `0` output file and `0` `.tmp_*` residue
   - Batch-mode structured error code & index parity against `OracleValidationError`
   - `std::mem::size_of::<CoreError>() <= 112` compile-time and unit-test verification

Writes canonical JSON evidence bundles under `<run-dir>/core/`:
  - `synthetic-oracle.json`
  - `orthogonality-separation.json`
  - `permutation-invariance.json`
  - `error-cases.json`
"""

from __future__ import annotations

import argparse
import copy
import itertools
import json
import math
import os
import random
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    EVOLUTION_ALGORITHM_VERSION,
    FIXTURES_DIR,
    NON_PHYSICAL_DISCLAIMER,
    atomic_write_json,
    load_json,
    semantic_evolution_projection,
    sha256_file,
)
from validation.m3.oracle import (
    FLOW_RULE_V1,
    OracleValidationError,
    SECONDARY_RULE_V1,
    UNCERTAINTY_RULE_V1,
    generate_random_evolution_cases,
    solve_chain_oracle,
    solve_evolution_oracle,
    verify_berge_and_star_invariants,
    verify_chain_invariants,
)

DEFAULT_VERIFY_TARGET_DIR = Path("/tmp/ensabc-m3-target-core-verify")
DEFAULT_EXT_TARGET_DIR = Path("/tmp/ensabc-m3-extension-target")

ALLOWED_CONFIG_KEYS = frozenset(
    {
        "secondary_rule",
        "flow_rule",
        "fraction_sum_tolerance",
        "high_uncertainty_threshold_ratio",
        "high_uncertainty_optimality_ratio",
        "high_uncertainty_secondary_ratio",
    }
)

ALLOWED_PAIR_KEYS = frozenset(
    {
        "schema_version",
        "case_id",
        "task_id",
        "description",
        "mode",
        "prev_scenarios",
        "curr_scenarios",
        "weights",
        "threshold",
        "threshold_coefficient",
        "prev_dispersion",
        "curr_dispersion",
        "config",
        "backbone",
    }
)

ALLOWED_CHAIN_TOP_KEYS = frozenset(
    {
        "schema_version",
        "case_id",
        "description",
        "mode",
        "run_key_prefix",
        "run_key",
        "tracking_run_key",
        "threshold_coefficient",
        "weights",
        "config",
        "cluster_config",
        "batches",
    }
)

ALLOWED_CHAIN_BATCH_KEYS = frozenset(
    {
        "ordinal",
        "status",
        "code",
        "error_code",
        "reason",
        "status_reason",
        "dispersion",
        "scenarios",
        "members",
        "init_time",
        "lead_hours",
    }
)


def float_bits(val: float) -> bytes:
    """Pack a finite Python float into big-endian IEEE-754 binary64 bytes (preserving sign of zero)."""
    if isinstance(val, bool) or not isinstance(val, float) or not math.isfinite(val):
        raise AssertionError(f"Expected finite float for 0-ULP bit check, got {val!r} ({type(val).__name__})")
    return struct.pack(">d", val)


def assert_bitwise_zero(val: Any, ctx: str) -> None:
    """Assert val is a float bitwise equal to +0.0 (not -0.0)."""
    if not isinstance(val, float) or float_bits(val) != b"\x00\x00\x00\x00\x00\x00\x00\x00":
        raise AssertionError(f"{ctx}: expected +0.0 bitwise, got {val!r}")


def assert_zero_ulp_equal(actual: Any, expected: Any, path: str) -> None:
    """Recursively assert 0-ULP IEEE-754 bitwise equality across nested JSON values."""
    if expected is None or actual is None:
        if actual is not expected:
            raise AssertionError(f"{path}: expected {expected!r}, got {actual!r}")
        return
    if isinstance(expected, bool) or isinstance(actual, bool):
        if type(actual) is not bool or type(expected) is not bool or actual != expected:
            raise AssertionError(f"{path}: bool mismatch actual={actual!r} vs expected={expected!r}")
        return
    if isinstance(expected, int):
        if type(actual) is not int or actual != expected:
            raise AssertionError(f"{path}: int mismatch actual={actual!r} vs expected={expected!r}")
        return
    if isinstance(expected, float):
        if type(actual) is not float:
            raise AssertionError(f"{path}: expected float {expected!r}, got {actual!r} ({type(actual).__name__})")
        if float_bits(actual) != float_bits(expected):
            ulp_diff = abs(
                int.from_bytes(float_bits(actual), "big", signed=False)
                - int.from_bytes(float_bits(expected), "big", signed=False)
            )
            raise AssertionError(
                f"{path}: 0-ULP float mismatch actual={actual!r} ({float_bits(actual).hex()}) "
                f"vs expected={expected!r} ({float_bits(expected).hex()}), raw_bit_delta={ulp_diff}"
            )
        return
    if isinstance(expected, str):
        if type(actual) is not str or actual != expected:
            raise AssertionError(f"{path}: str mismatch actual={actual!r} vs expected={expected!r}")
        return
    if isinstance(expected, (list, tuple)):
        if not isinstance(actual, (list, tuple)) or len(actual) != len(expected):
            raise AssertionError(
                f"{path}: sequence length mismatch actual={len(actual) if isinstance(actual, (list, tuple)) else type(actual).__name__} "
                f"vs expected={len(expected)}"
            )
        for idx, (av, ev) in enumerate(zip(actual, expected)):
            assert_zero_ulp_equal(av, ev, f"{path}[{idx}]")
        return
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            raise AssertionError(f"{path}: expected dict, got {type(actual).__name__}")
        act_keys = set(actual.keys())
        exp_keys = set(expected.keys())
        if act_keys != exp_keys:
            missing = sorted(exp_keys - act_keys)
            extra = sorted(act_keys - exp_keys)
            raise AssertionError(f"{path}: dict keys mismatch missing={missing}, extra={extra}")
        for k in sorted(exp_keys):
            assert_zero_ulp_equal(actual[k], expected[k], f"{path}.{k}")
        return
    if actual != expected:
        raise AssertionError(f"{path}: value mismatch actual={actual!r} vs expected={expected!r}")


def clean_config_dto(cfg: dict[str, Any] | None) -> dict[str, Any] | None:
    if cfg is None:
        return None
    return {k: copy.deepcopy(v) for k, v in cfg.items() if k in ALLOWED_CONFIG_KEYS}


def clean_pair_dto(case: dict[str, Any]) -> dict[str, Any]:
    """Strip fixture-only metadata keys before passing strict DTO to `m3_runner --mode pair`."""
    out = {k: copy.deepcopy(v) for k, v in case.items() if k in ALLOWED_PAIR_KEYS}
    if "config" in out and isinstance(out["config"], dict):
        out["config"] = clean_config_dto(out["config"])
    # In scaled cases where fixture also documents explicit `threshold`, omit redundant `threshold`
    # since ScaledEvolutionInput computes tau = c * S internally.
    if out.get("mode") == "scaled" or out.get("threshold_coefficient") is not None:
        out.pop("threshold", None)
    return out


def clean_chain_dto(case: dict[str, Any], *, default_run_key: str = "chain0000000") -> dict[str, Any]:
    """Strip fixture-only metadata keys before passing strict DTO to `m3_runner --mode chain`."""
    out = {k: copy.deepcopy(v) for k, v in case.items() if k in ALLOWED_CHAIN_TOP_KEYS}
    if "config" in out and isinstance(out["config"], dict):
        out["config"] = clean_config_dto(out["config"])
    if not any(out.get(k) for k in ("run_key_prefix", "run_key", "tracking_run_key")):
        out["tracking_run_key"] = default_run_key
    cleaned_batches: list[dict[str, Any]] = []
    for b in case["batches"]:
        cb = {k: copy.deepcopy(v) for k, v in b.items() if k in ALLOWED_CHAIN_BATCH_KEYS}
        if "init_time_utc" in b and "init_time" not in cb:
            cb["init_time"] = b["init_time_utc"]
        cleaned_batches.append(cb)
    out["batches"] = cleaned_batches
    return out


def run_m3_runner(
    runner_bin: Path,
    mode: str,
    payload: dict[str, Any] | str,
    *,
    scaled_flag: bool = False,
    verify_zero_output_on_error: bool = False,
) -> tuple[int, dict[str, Any] | None, str]:
    """Invoke `m3_runner` using an isolated temporary directory with non-existent initial output path."""
    with tempfile.TemporaryDirectory(prefix="ensabc-m3-runner-") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        in_path = tmp_dir / "input.json"
        out_path = tmp_dir / "output.json"

        if isinstance(payload, str):
            in_path.write_text(payload, encoding="utf-8")
        else:
            # Allow NaN/Infinity tokens when crafting raw non-finite test inputs
            in_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

        assert not out_path.exists()
        cmd = [str(runner_bin), "--mode", mode, "--input", str(in_path), "--output", str(out_path)]
        if scaled_flag:
            cmd.append("--scaled")

        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)

        leftover_tmps = list(tmp_dir.glob(".tmp_*"))
        if leftover_tmps:
            raise AssertionError(f"m3_runner left temporary file residue: {leftover_tmps}")

        if verify_zero_output_on_error and proc.returncode != 0:
            if out_path.exists():
                raise AssertionError(
                    f"m3_runner exited {proc.returncode} but left output file residue at {out_path} "
                    f"({out_path.stat().st_size} bytes)"
                )

        parsed_out: dict[str, Any] | None = None
        if proc.returncode == 0 and out_path.is_file():
            raw_out = out_path.read_text(encoding="utf-8")
            parsed_out = json.loads(
                raw_out,
                parse_constant=lambda c: (_ for _ in ()).throw(
                    AssertionError(f"m3_runner emitted non-finite JSON token: {c}")
                ),
            )
        return proc.returncode, parsed_out, proc.stderr


def assert_pair_zero_ulp_agreement(
    case_id: str,
    rust_res: dict[str, Any],
    oracle_res: dict[str, Any],
) -> dict[str, bool]:
    """Assert 100% field-by-field 0-ULP bitwise parity between `m3_runner` and `oracle.py`."""
    if rust_res.get("schema_version") != "m3.evolve-result.v1":
        raise AssertionError(f"{case_id}: unexpected schema_version {rust_res.get('schema_version')!r}")

    # 1. Top-level metadata and scale scalars
    for key in (
        "algorithm_version",
        "secondary_rule",
        "flow_rule",
        "uncertainty_rule",
        "non_physical_disclaimer",
        "prev_cluster_ids",
        "curr_cluster_ids",
        "threshold",
        "pair_scale",
        "threshold_coefficient",
        "prev_dispersion",
        "curr_dispersion",
        "reference_scale",
    ):
        assert_zero_ulp_equal(rust_res[key], oracle_res[key], f"{case_id}.{key}")

    # 2. Backbone (M2 global optimal 1-to-1 matching)
    rb = rust_res["backbone"]
    ob = oracle_res["backbone"]
    assert_zero_ulp_equal(rb["objective"], ob["objective"], f"{case_id}.backbone.objective")
    assert_zero_ulp_equal(rb["optima_count"], ob["optima_count"], f"{case_id}.backbone.optima_count")
    assert_zero_ulp_equal(rb["distance_matrix"], ob["distance_matrix"], f"{case_id}.backbone.distance_matrix")
    for opt_k in ("pair_scale", "threshold_coefficient", "prev_dispersion", "curr_dispersion"):
        assert_zero_ulp_equal(rb[opt_k], ob.get(opt_k), f"{case_id}.backbone.{opt_k}")

    if len(rb["links"]) != len(ob["links"]):
        raise AssertionError(f"{case_id}.backbone.links length mismatch: {len(rb['links'])} != {len(ob['links'])}")
    for idx, (rl, ol) in enumerate(zip(rb["links"], ob["links"])):
        lpath = f"{case_id}.backbone.links[{idx}]"
        for k in ("prev_cluster_id", "curr_cluster_id", "distance_k", "substitutability", "ambiguous"):
            assert_zero_ulp_equal(rl[k], ol[k], f"{lpath}.{k}")
        assert_zero_ulp_equal(rl["distance_ratio"], ol.get("distance_ratio"), f"{lpath}.distance_ratio")
        sub = rl["substitutability"]
        if sub == "same_count":
            assert_zero_ulp_equal(rl["substitutability_gap"], ol["substitutability_gap"], f"{lpath}.substitutability_gap")
            assert_zero_ulp_equal(rl["alt_objective"], ol["alt_objective"], f"{lpath}.alt_objective")
            if rl["alt_count"] is not None:
                raise AssertionError(f"{lpath}.alt_count expected None for same_count, got {rl['alt_count']!r}")
        elif sub == "count_drop":
            assert_zero_ulp_equal(rl["alt_count"], ol["alt_count"], f"{lpath}.alt_count")
            assert_zero_ulp_equal(rl["alt_objective"], ol["alt_objective"], f"{lpath}.alt_objective")
            if rl["substitutability_gap"] is not None:
                raise AssertionError(f"{lpath}.substitutability_gap expected None for count_drop")
        elif sub == "tied":
            if rl["substitutability_gap"] is not None or rl["alt_objective"] is not None or rl["alt_count"] is not None:
                raise AssertionError(f"{lpath}: tied link must carry None enum payload fields in BackboneMatchDto")
            assert_bitwise_zero(ol["substitutability_gap"], f"{lpath}.oracle_tied_gap")
        else:
            raise AssertionError(f"{lpath}: unknown substitutability {sub!r}")

    ora_unm_prev = [
        {
            "cluster_id": u["cluster_id"],
            "reason": u["reason"],
            "matched_in_some_optimum": u["matched_in_some_optimum"],
        }
        for u in ob["unmatched_prev"]
    ]
    ora_unm_curr = [
        {
            "cluster_id": u["cluster_id"],
            "reason": u["reason"],
            "matched_in_some_optimum": u["matched_in_some_optimum"],
        }
        for u in ob["unmatched_curr"]
    ]
    assert_zero_ulp_equal(rb["unmatched_prev"], ora_unm_prev, f"{case_id}.backbone.unmatched_prev")
    assert_zero_ulp_equal(rb["unmatched_curr"], ora_unm_curr, f"{case_id}.backbone.unmatched_curr")

    # 3. Many-to-many EvolutionEdges (all 22 fields)
    assert_zero_ulp_equal(rust_res["edges"], oracle_res["edges"], f"{case_id}.edges")

    # 4. NodeEvolutionSummary (all 19 core fields on prev_nodes and curr_nodes)
    ora_prev_nodes = []
    for n in oracle_res["prev_nodes"]:
        nc = dict(n)
        assert nc.pop("out_degree_primary") == nc["primary_degree"]
        assert nc.pop("out_degree_secondary") == nc["secondary_degree"]
        ora_prev_nodes.append(nc)
    assert_zero_ulp_equal(rust_res["prev_nodes"], ora_prev_nodes, f"{case_id}.prev_nodes")

    ora_curr_nodes = []
    for n in oracle_res["curr_nodes"]:
        nc = dict(n)
        assert nc.pop("in_degree_primary") == nc["primary_degree"]
        assert nc.pop("in_degree_secondary") == nc["secondary_degree"]
        ora_curr_nodes.append(nc)
    assert_zero_ulp_equal(rust_res["curr_nodes"], ora_curr_nodes, f"{case_id}.curr_nodes")

    # 5. EvolutionEvents (all 15 fields)
    ora_events = []
    for ev in oracle_res["events"]:
        evc = dict(ev)
        if evc["primary_link"] is not None:
            evc["primary_link"] = list(evc["primary_link"])
        ora_events.append(evc)
    assert_zero_ulp_equal(rust_res["events"], ora_events, f"{case_id}.events")

    # 6. TransitionEvolutionSummary (all 29 fields)
    assert_zero_ulp_equal(rust_res["summary"], oracle_res["summary"], f"{case_id}.summary")

    # 7. Berge Theorems 1-3 & 5-class star partition invariants on Rust output
    invariants = verify_berge_and_star_invariants(rust_res)
    if not invariants["all_passed"]:
        raise AssertionError(f"{case_id}: Berge/star invariants failed on Rust result: {invariants}")

    # Explicit Theorem 3 check: every secondary edge has finite non-negative promotion gap
    for idx, e in enumerate(rust_res["edges"]):
        if e["edge_role"] in ("secondary_split", "secondary_merge"):
            if e["primary_alternative_status"] != "promotion_gap":
                raise AssertionError(f"{case_id}.edges[{idx}]: secondary edge status != promotion_gap")
            gap = e["optimality_margin"]
            if gap is None or not isinstance(gap, float) or not math.isfinite(gap) or gap < 0.0:
                raise AssertionError(f"{case_id}.edges[{idx}]: Theorem 3 violated, optimality_margin={gap!r}")

    # Active threshold margin >= raw matrix threshold margin
    if rust_res["summary"]["min_threshold_margin"] < rust_res["summary"]["raw_matrix_threshold_margin"]:
        raise AssertionError(
            f"{case_id}: active min_threshold_margin {rust_res['summary']['min_threshold_margin']!r} "
            f"< raw_matrix_threshold_margin {rust_res['summary']['raw_matrix_threshold_margin']!r}"
        )

    return invariants


def assert_fixture_expected_subset(case_id: str, rust_res: dict[str, Any], expected: dict[str, Any]) -> None:
    """Verify every recorded expectation field in `synthetic_evolution_cases.json` matches 0-ULP."""
    if "threshold" in expected:
        assert_zero_ulp_equal(rust_res["threshold"], expected["threshold"], f"{case_id}.expected.threshold")
    if "pair_scale" in expected:
        assert_zero_ulp_equal(rust_res["pair_scale"], expected["pair_scale"], f"{case_id}.expected.pair_scale")
    if "reference_scale" in expected:
        assert_zero_ulp_equal(
            rust_res["reference_scale"], expected["reference_scale"], f"{case_id}.expected.reference_scale"
        )
    if "optima_count" in expected:
        assert_zero_ulp_equal(
            rust_res["backbone"]["optima_count"], expected["optima_count"], f"{case_id}.expected.optima_count"
        )
    if "edges" in expected:
        assert_zero_ulp_equal(rust_res["edges"], expected["edges"], f"{case_id}.expected.edges")
    if "events" in expected:
        if len(rust_res["events"]) != len(expected["events"]):
            raise AssertionError(f"{case_id}.expected.events length mismatch")
        for idx, (rev, eev) in enumerate(zip(rust_res["events"], expected["events"])):
            for k, v in eev.items():
                assert_zero_ulp_equal(rev[k], v, f"{case_id}.expected.events[{idx}].{k}")
    if "summary" in expected:
        for k, v in expected["summary"].items():
            assert_zero_ulp_equal(rust_res["summary"][k], v, f"{case_id}.expected.summary.{k}")


def assert_chain_fixture_expected(chain_case: dict[str, Any], rust_chain: dict[str, Any]) -> None:
    """Verify every recorded expectation field in `chain_cases[*].expected` matches 0-ULP."""
    if "expected" not in chain_case:
        return
    case_id = chain_case["case_id"]
    expected = chain_case["expected"]
    prefix = rust_chain["run_key_prefix"]

    def _strip_prefix(bid: str) -> str:
        expected_head = f"{prefix}:"
        if not bid.startswith(expected_head):
            raise AssertionError(f"{case_id}: branch_id {bid!r} does not start with {expected_head!r}")
        return bid[len(expected_head) :]

    if "chain_summary" in expected:
        for k, v in expected["chain_summary"].items():
            assert_zero_ulp_equal(
                rust_chain["chain_summary"][k], v, f"{case_id}.expected.chain_summary.{k}"
            )

    if "transitions" in expected:
        exp_trs = expected["transitions"]
        if len(rust_chain["transitions"]) != len(exp_trs):
            raise AssertionError(
                f"{case_id}.expected.transitions length mismatch: "
                f"{len(rust_chain['transitions'])} != {len(exp_trs)}"
            )
        for idx, (rtr, etr) in enumerate(zip(rust_chain["transitions"], exp_trs)):
            tpath = f"{case_id}.expected.transitions[{idx}]"
            for k, v in etr.items():
                if k == "event_types":
                    actual_ev_types = [ev["event_type"] for ev in rtr["events"]]
                    assert_zero_ulp_equal(actual_ev_types, v, f"{tpath}.event_types")
                else:
                    assert_zero_ulp_equal(rtr[k], v, f"{tpath}.{k}")

    if "branch_summaries" in expected:
        exp_brs = expected["branch_summaries"]
        if len(rust_chain["branches"]) != len(exp_brs):
            raise AssertionError(
                f"{case_id}.expected.branch_summaries length mismatch: "
                f"{len(rust_chain['branches'])} != {len(exp_brs)}"
            )
        for idx, (rbr, ebr) in enumerate(zip(rust_chain["branches"], exp_brs)):
            bpath = f"{case_id}.expected.branch_summaries[{idx}]"
            for k, v in ebr.items():
                if k == "branch_id_suffix":
                    actual = _strip_prefix(rbr["branch_id"])
                elif k == "spawned_secondary_branch_suffixes":
                    actual = [_strip_prefix(x) for x in rbr["spawned_secondary_branch_ids"]]
                elif k == "absorbed_secondary_branch_suffixes":
                    actual = [_strip_prefix(x) for x in rbr["absorbed_secondary_branch_ids"]]
                elif k == "split_from_parent_branch_suffix":
                    actual = (
                        _strip_prefix(rbr["split_from_parent_branch_id"])
                        if rbr["split_from_parent_branch_id"] is not None
                        else None
                    )
                elif k == "merged_into_branch_suffix":
                    actual = (
                        _strip_prefix(rbr["merged_into_branch_id"])
                        if rbr["merged_into_branch_id"] is not None
                        else None
                    )
                else:
                    actual = rbr[k]
                assert_zero_ulp_equal(actual, v, f"{bpath}.{k}")


def assert_chain_zero_ulp_agreement(
    case_id: str,
    rust_chain: dict[str, Any],
    oracle_chain: dict[str, Any],
    *,
    expected_tracking_run_key: str,
) -> dict[str, bool]:
    """Assert 100% 0-ULP parity across all multi-batch chain structures (`m3_runner --mode chain` vs oracle)."""
    if rust_chain.get("schema_version") != "m3.chain-result.v1":
        raise AssertionError(f"{case_id}: unexpected chain schema_version {rust_chain.get('schema_version')!r}")

    for key in (
        "algorithm_version",
        "secondary_rule",
        "flow_rule",
        "uncertainty_rule",
        "non_physical_disclaimer",
        "planned_batch_count",
    ):
        assert_zero_ulp_equal(rust_chain[key], oracle_chain[key], f"{case_id}.{key}")

    expected_prefix = expected_tracking_run_key[:12]
    assert_zero_ulp_equal(rust_chain["run_key_prefix"], expected_prefix, f"{case_id}.run_key_prefix")

    # 1. Resolved batches
    if len(rust_chain["batches"]) != len(oracle_chain["batches"]):
        raise AssertionError(f"{case_id}: batches length mismatch")
    for idx, (rb, ob) in enumerate(zip(rust_chain["batches"], oracle_chain["batches"])):
        bpath = f"{case_id}.batches[{idx}]"
        assert_zero_ulp_equal(rb["ordinal"], ob["ordinal"], f"{bpath}.ordinal")
        assert_zero_ulp_equal(rb["status"], ob["status"], f"{bpath}.status")
        assert_zero_ulp_equal(rb["dispersion"], ob["dispersion"], f"{bpath}.dispersion")
        if rb["status"] != "ok" and ob.get("status_reason") is not None:
            assert_zero_ulp_equal(rb["status_reason"], ob["status_reason"], f"{bpath}.status_reason")

    # 2. Chain nodes
    if len(rust_chain["nodes"]) != len(oracle_chain["nodes"]):
        raise AssertionError(f"{case_id}: nodes length mismatch")
    for idx, (rn, on) in enumerate(zip(rust_chain["nodes"], oracle_chain["nodes"])):
        npath = f"{case_id}.nodes[{idx}]"
        assert_zero_ulp_equal(
            rn["has_primary_delta_fraction"],
            on["has_primary_delta_fraction"],
            f"{npath}.has_primary_delta_fraction",
        )
        if rn["has_primary_delta_fraction"]:
            assert_zero_ulp_equal(
                rn["primary_delta_fraction"],
                on["primary_delta_fraction"],
                f"{npath}.primary_delta_fraction",
            )
        else:
            if rn["primary_delta_fraction"] is not None:
                raise AssertionError(f"{npath}: expected None primary_delta_fraction when has_primary_delta_fraction=False")
            assert_bitwise_zero(on["primary_delta_fraction"], f"{npath}.oracle_primary_delta_sentinel")

        for k in (
            "ordinal",
            "cluster_id",
            "canonical_rank",
            "branch_id",
            "mean_features",
            "member_count",
            "member_fraction",
            "assignment_reason",
            "in_degree_primary",
            "in_degree_secondary",
            "out_degree_primary",
            "out_degree_secondary",
            "incoming_role",
            "outgoing_role",
            "unattributed_inflow_fraction",
            "unattributed_outflow_fraction",
            "high_uncertainty",
        ):
            assert_zero_ulp_equal(rn[k], on[k], f"{npath}.{k}")

    # 3. Chain transitions & nested pair results + gap event disjointness
    if len(rust_chain["transitions"]) != len(oracle_chain["transitions"]):
        raise AssertionError(f"{case_id}: transitions length mismatch")
    gap_ordinals: set[int] = set()
    for idx, (rtr, otr) in enumerate(zip(rust_chain["transitions"], oracle_chain["transitions"])):
        tpath = f"{case_id}.transitions[{idx}]"
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
            assert_zero_ulp_equal(rtr[k], otr[k], f"{tpath}.{k}")

        if rtr["transition_status"] == "ok":
            if rtr["chain_broken"] or rtr["evolve_result"] is None:
                raise AssertionError(f"{tpath}: ok transition must have chain_broken=False and evolve_result")
            assert_pair_zero_ulp_agreement(f"{tpath}.evolve_result", rtr["evolve_result"], otr["evolve_result"])
            for ev in rtr["events"]:
                if ev["event_type"] in ("gap_interrupted", "gap_restarted"):
                    raise AssertionError(f"{tpath}: ok transition illegally emitted {ev['event_type']}")
        else:
            gap_ordinals.add(rtr["prev_ordinal"])
            if not rtr["chain_broken"] or rtr["evolve_result"] is not None:
                raise AssertionError(f"{tpath}: gap transition must have chain_broken=True and evolve_result=None")
            for ev in rtr["events"]:
                if ev["event_type"] not in ("gap_interrupted", "gap_restarted"):
                    raise AssertionError(
                        f"{tpath}: gap transition emitted non-gap event {ev['event_type']!r} "
                        "(must be strictly disjoint from disappearance/emergence)"
                    )

    # 4. Chain edges (all fields + zero cross-gap edges)
    if len(rust_chain["edges"]) != len(oracle_chain["edges"]):
        raise AssertionError(f"{case_id}: chain edges length mismatch")
    for idx, (re_edge, oe_edge) in enumerate(zip(rust_chain["edges"], oracle_chain["edges"])):
        epath = f"{case_id}.edges[{idx}]"
        if re_edge["prev_ordinal"] in gap_ordinals:
            raise AssertionError(f"{epath}: cross-gap edge detected at prev_ordinal={re_edge['prev_ordinal']}")
        if re_edge["has_optimality_margin"] != (re_edge["optimality_margin"] is not None):
            raise AssertionError(f"{epath}: has_optimality_margin inconsistent")
        if re_edge["has_secondary_margin"] != (re_edge["secondary_margin"] is not None):
            raise AssertionError(f"{epath}: has_secondary_margin inconsistent")
        re_stripped = {
            k: v for k, v in re_edge.items() if k not in ("has_optimality_margin", "has_secondary_margin")
        }
        assert_zero_ulp_equal(re_stripped, oe_edge, epath)

    # 5. Chain events (all fields, including 0.0 max_morphology_over_scale on gap events)
    if len(rust_chain["events"]) != len(oracle_chain["events"]):
        raise AssertionError(f"{case_id}: chain events length mismatch")
    for idx, (rev, oev) in enumerate(zip(rust_chain["events"], oracle_chain["events"])):
        evpath = f"{case_id}.events[{idx}]"
        if rev["has_min_margin_ratio"] != (rev["min_margin_ratio"] is not None):
            raise AssertionError(f"{evpath}: has_min_margin_ratio inconsistent")
        if rev["event_type"] in ("gap_interrupted", "gap_restarted"):
            assert_bitwise_zero(rev["max_morphology_over_scale"], f"{evpath}.gap_max_morphology_over_scale")
            assert_bitwise_zero(oev["max_morphology_over_scale"], f"{evpath}.oracle_gap_scale_sentinel")
        for k in (
            "prev_ordinal",
            "event_index",
            "event_type",
            "prev_cluster_ids",
            "curr_cluster_ids",
            "branch_ids",
            "primary_branch_id",
            "rule_basis",
            "max_morphology_distance",
            "max_morphology_over_scale",
            "prev_fraction_sum",
            "curr_fraction_sum",
            "net_delta_fraction",
            "attributed_flow_sum",
            "min_margin_ratio",
            "ambiguous_tie",
            "high_uncertainty",
            "non_physical_disclaimer",
        ):
            assert_zero_ulp_equal(rev[k], oev[k], f"{evpath}.{k}")

    # 6. EvolutionBranchSummary (all 26 fields, 0-ULP)
    assert_zero_ulp_equal(rust_chain["branches"], oracle_chain["branch_summaries"], f"{case_id}.branches")

    # 7. EvolutionChainSummary (all 26 fields, 0-ULP)
    assert_zero_ulp_equal(rust_chain["chain_summary"], oracle_chain["chain_summary"], f"{case_id}.chain_summary")

    # 8. Verify E2-E8 chain invariants and authentic run_key[:12]: prefix projection
    ora_inv = verify_chain_invariants(oracle_chain)
    if not ora_inv["all_passed"]:
        raise AssertionError(f"{case_id}: oracle chain invariants failed: {ora_inv}")

    # Convert rust_chain `branches` -> `branch_summaries` alias for E2-E8 checker
    rust_for_inv = {**rust_chain, "branch_summaries": rust_chain["branches"]}
    rust_inv = verify_chain_invariants(rust_for_inv)
    if not rust_inv["all_passed"]:
        raise AssertionError(f"{case_id}: Rust chain invariants failed: {rust_inv}")

    proj_rust = semantic_evolution_projection(
        {
            "tracking_run_key": expected_tracking_run_key,
            "nodes": [
                {k: v for k, v in n.items() if k != "primary_delta_fraction"}
                for n in rust_chain["nodes"]
            ],
            "edges": [
                {k: v for k, v in e.items() if k not in ("has_optimality_margin", "has_secondary_margin")}
                for e in rust_chain["edges"]
            ],
            "branch_summaries": rust_chain["branches"],
            "chain_summary": rust_chain["chain_summary"],
        },
        expected_tracking_run_key=expected_tracking_run_key,
    )
    proj_ora = semantic_evolution_projection(
        {
            "tracking_run_key": expected_tracking_run_key,
            "nodes": [
                {k: v for k, v in n.items() if k != "primary_delta_fraction"}
                for n in oracle_chain["nodes"]
            ],
            "edges": oracle_chain["edges"],
            "branch_summaries": oracle_chain["branch_summaries"],
            "chain_summary": oracle_chain["chain_summary"],
        },
        expected_tracking_run_key=expected_tracking_run_key,
    )
    assert_zero_ulp_equal(proj_rust, proj_ora, f"{case_id}.semantic_projection")

    return rust_inv


def map_result_to_base_ids(
    perm_res: dict[str, Any],
    p_new_to_base: dict[int, int],
    c_new_to_base: dict[int, int],
) -> dict[str, Any]:
    """Map permuted `cluster_id`s in an `EvolutionResultDto` back to base physical `cluster_id`s."""
    mapped_edges = sorted(
        [
            {
                **e,
                "prev_cluster_id": p_new_to_base[e["prev_cluster_id"]],
                "curr_cluster_id": c_new_to_base[e["curr_cluster_id"]],
            }
            for e in perm_res["edges"]
        ],
        key=lambda x: (x["prev_cluster_id"], x["curr_cluster_id"]),
    )

    mapped_prev_nodes = sorted(
        [
            {
                **n,
                "cluster_id": p_new_to_base[n["cluster_id"]],
                "primary_counterpart_cluster_id": (
                    c_new_to_base[n["primary_counterpart_cluster_id"]]
                    if n["primary_counterpart_cluster_id"] is not None
                    else None
                ),
            }
            for n in perm_res["prev_nodes"]
        ],
        key=lambda x: x["cluster_id"],
    )

    mapped_curr_nodes = sorted(
        [
            {
                **n,
                "cluster_id": c_new_to_base[n["cluster_id"]],
                "primary_counterpart_cluster_id": (
                    p_new_to_base[n["primary_counterpart_cluster_id"]]
                    if n["primary_counterpart_cluster_id"] is not None
                    else None
                ),
            }
            for n in perm_res["curr_nodes"]
        ],
        key=lambda x: x["cluster_id"],
    )

    mapped_events = [
        {
            **ev,
            "prev_cluster_ids": sorted(p_new_to_base[cid] for cid in ev["prev_cluster_ids"]),
            "curr_cluster_ids": sorted(c_new_to_base[cid] for cid in ev["curr_cluster_ids"]),
            "primary_link": (
                [p_new_to_base[ev["primary_link"][0]], c_new_to_base[ev["primary_link"][1]]]
                if ev["primary_link"] is not None
                else None
            ),
        }
        for ev in perm_res["events"]
    ]

    return {
        "edges": mapped_edges,
        "prev_nodes": mapped_prev_nodes,
        "curr_nodes": mapped_curr_nodes,
        "events": mapped_events,
        "summary": perm_res["summary"],
        "objective": perm_res["backbone"]["objective"],
        "optima_count": perm_res["backbone"]["optima_count"],
    }


def resolve_runner_binary(explicit_runner: Path | None) -> Path:
    """Resolve `m3_runner` executable or compile cleanly in isolated `/tmp/ensabc-m3-target-core-verify`."""
    if explicit_runner is not None:
        resolved = explicit_runner.resolve()
        if not resolved.is_file() or not os.access(resolved, os.X_OK):
            raise FileNotFoundError(f"Explicit --runner binary missing or not executable: {resolved}")
        return resolved

    candidates = (
        DEFAULT_VERIFY_TARGET_DIR / "release" / "examples" / "m3_runner",
        DEFAULT_EXT_TARGET_DIR / "release" / "examples" / "m3_runner",
        REPO_ROOT / "target" / "release" / "examples" / "m3_runner",
    )
    for cand in candidates:
        if cand.is_file() and os.access(cand, os.X_OK):
            return cand.resolve()

    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(DEFAULT_VERIFY_TARGET_DIR)
    proc = subprocess.run(
        ["cargo", "build", "--release", "--locked", "-p", "ensabc-core", "--example", "m3_runner"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    built = DEFAULT_VERIFY_TARGET_DIR / "release" / "examples" / "m3_runner"
    if proc.returncode != 0 or not built.is_file():
        raise RuntimeError(f"Failed compiling m3_runner: {proc.stderr}")
    return built.resolve()


def verify_core_error_byte_budget(runner_bin: Path) -> dict[str, Any]:
    """Verify `std::mem::size_of::<CoreError>() <= 112` compile-time guard and unit test execution."""
    contract_test_rs = REPO_ROOT / "crates" / "ensabc-core" / "tests" / "m3_evolution_contract.rs"
    if not contract_test_rs.is_file():
        raise AssertionError(f"Missing contract test file: {contract_test_rs}")
    src_text = contract_test_rs.read_text(encoding="utf-8")
    const_guard = "assert!(std::mem::size_of::<CoreError>() <= 112)" in src_text
    if not const_guard:
        raise AssertionError("Missing compile-time size_of::<CoreError>() <= 112 guard in m3_evolution_contract.rs")

    target_dir = DEFAULT_VERIFY_TARGET_DIR
    if runner_bin.parent.name == "examples" and runner_bin.parents[1].name in ("release", "debug"):
        target_dir = runner_bin.parents[2]

    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target_dir)
    proc = subprocess.run(
        [
            "cargo",
            "test",
            "--locked",
            "-p",
            "ensabc-core",
            "--test",
            "m3_evolution_contract",
            "core_error_byte_budget_and_m1_m2_exports_intact",
            "--",
            "--exact",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0 or "1 passed; 0 failed" not in proc.stdout:
        raise AssertionError(
            f"CoreError <= 112B unit test failed (rc={proc.returncode}):\n{proc.stdout}\n{proc.stderr}"
        )
    return {
        "max_allowed_bytes": 112,
        "compile_time_const_assert_present": True,
        "unit_test_name": "core_error_byte_budget_and_m1_m2_exports_intact",
        "unit_test_passed": True,
        "cargo_target_dir": str(target_dir),
        "verified": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Cross-verify M3 Rust scenario evolution core (m3_runner) against Python Fraction oracle"
    )
    parser.add_argument("--run-dir", type=Path, required=True, help="Output run directory")
    parser.add_argument("--runner", type=Path, default=None, help="Optional path to compiled m3_runner binary")
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return 0 if exc.code == 0 else 2

    t_start = time.perf_counter()

    fixture_path = FIXTURES_DIR / "synthetic_evolution_cases.json"
    if not fixture_path.is_file():
        print(f"[MISSING_PREREQUISITE] Fixture file not found: {fixture_path}", file=sys.stderr)
        return 2

    try:
        runner_bin = resolve_runner_binary(args.runner)
    except Exception as exc:
        print(f"[MISSING_PREREQUISITE] {exc}", file=sys.stderr)
        return 2

    try:
        run_dir = args.run_dir.resolve()
        core_dir = run_dir / "core"
        core_dir.mkdir(parents=True, exist_ok=True)

        fixtures = load_json(fixture_path)
        pair_cases: list[dict[str, Any]] = fixtures["pair_cases"]
        orth_series: dict[str, Any] = fixtures["orthogonality_series"]
        chain_cases: list[dict[str, Any]] = fixtures["chain_cases"]
        negative_cases: list[dict[str, Any]] = fixtures["negative_cases"]

        if len(pair_cases) != 25:
            raise AssertionError(f"Expected 25 fixed pair_cases, found {len(pair_cases)}")
        if len(chain_cases) != 4:
            raise AssertionError(f"Expected 4 fixed chain_cases, found {len(chain_cases)}")
        if len(negative_cases) != 19:
            raise AssertionError(f"Expected 19 fixed negative_cases, found {len(negative_cases)}")

        # =====================================================================
        # 1. Fixed 25 Pair Cases + Constructive Berge Theorem 4 + Backbone Check
        # =====================================================================
        rc_fixed, fixed_batch_out, err_fixed = run_m3_runner(
            runner_bin,
            "pair",
            {"cases": [clean_pair_dto(c) for c in pair_cases]},
        )
        if rc_fixed != 0 or fixed_batch_out is None:
            raise AssertionError(f"Fixed pair batch failed (rc={rc_fixed}): {err_fixed}")

        fixed_pair_records: list[dict[str, Any]] = []
        theorem4_inactive_cases_verified = 0
        bracketing_records: list[dict[str, Any]] = []

        for c, item in zip(pair_cases, fixed_batch_out["results"]):
            cid = c["case_id"]
            if item.get("status") != "ok":
                raise AssertionError(f"Fixed pair case {cid} failed in batch: {item}")
            rust_res = item["result"]
            oracle_res = solve_evolution_oracle(c)

            invariants = assert_pair_zero_ulp_agreement(cid, rust_res, oracle_res)
            if "expected" in c:
                assert_fixture_expected_subset(cid, rust_res, c["expected"])

            # Constructive Berge Theorem 4 verification whenever an inactive feasible cross-cell exists
            # strictly above max(d(E_evol)) and <= tau under unique primary & secondary choices
            dist_mat = rust_res["backbone"]["distance_matrix"]
            tau = rust_res["threshold"]
            p_ids = rust_res["prev_cluster_ids"]
            c_ids = rust_res["curr_cluster_ids"]
            p_map = {pid: i for i, pid in enumerate(p_ids)}
            c_map = {cid_v: j for j, cid_v in enumerate(c_ids)}
            evol_pairs = {(p_map[e["prev_cluster_id"]], c_map[e["curr_cluster_id"]]) for e in rust_res["edges"]}
            max_evol_d = rust_res["summary"]["all_edges_max_distance"]
            inactive_above_evol = [
                dist_mat[i][j]
                for i in range(len(p_ids))
                for j in range(len(c_ids))
                if (i, j) not in evol_pairs and max_evol_d < dist_mat[i][j] <= tau
            ]
            if inactive_above_evol and not rust_res["summary"]["ambiguous_tie"]:
                tighter_tau = 0.5 * (max_evol_d + min(inactive_above_evol))
                if max_evol_d <= tighter_tau < min(inactive_above_evol):
                    thm4_case = {
                        "mode": "absolute",
                        "prev_scenarios": copy.deepcopy(c["prev_scenarios"]),
                        "curr_scenarios": copy.deepcopy(c["curr_scenarios"]),
                        "weights": copy.deepcopy(c["weights"]),
                        "threshold": tighter_tau,
                        "config": clean_config_dto(c.get("config")),
                    }
                    rc_t4, res_t4, err_t4 = run_m3_runner(runner_bin, "pair", thm4_case)
                    if rc_t4 != 0 or res_t4 is None:
                        raise AssertionError(f"Theorem 4 reduced-tau run failed on {cid}: {err_t4}")
                    base_topo = [
                        (e["prev_cluster_id"], e["curr_cluster_id"], e["edge_role"], e["event_type"])
                        for e in rust_res["edges"]
                    ]
                    t4_topo = [
                        (e["prev_cluster_id"], e["curr_cluster_id"], e["edge_role"], e["event_type"])
                        for e in res_t4["edges"]
                    ]
                    if base_topo != t4_topo:
                        raise AssertionError(f"Berge Theorem 4 violated on {cid}: {base_topo} != {t4_topo}")
                    theorem4_inactive_cases_verified += 1

            # Scaled vs equivalent absolute threshold parity check
            if cid == "negative_zero_and_scaled_mode_parity":
                abs_equiv = {
                    "case_id": f"{cid}_abs_equiv",
                    "mode": "absolute",
                    "prev_scenarios": copy.deepcopy(c["prev_scenarios"]),
                    "curr_scenarios": copy.deepcopy(c["curr_scenarios"]),
                    "weights": copy.deepcopy(c["weights"]),
                    "threshold": c["equivalent_absolute_threshold"],
                    "config": clean_config_dto(c.get("config")),
                }
                rc_eq, res_eq, err_eq = run_m3_runner(runner_bin, "pair", abs_equiv)
                if rc_eq != 0 or res_eq is None:
                    raise AssertionError(f"Equivalent absolute run failed: {err_eq}")
                assert_pair_zero_ulp_agreement(abs_equiv["case_id"], res_eq, solve_evolution_oracle(abs_equiv))
                for strip_k in ("distance_over_scale", "threshold_margin_ratio", "optimality_margin_ratio", "secondary_margin_ratio"):
                    pass
                base_edges_topo = [
                    (e["prev_cluster_id"], e["curr_cluster_id"], e["edge_role"], e["event_type"], e["distance"], e["attributed_flow"])
                    for e in rust_res["edges"]
                ]
                eq_edges_topo = [
                    (e["prev_cluster_id"], e["curr_cluster_id"], e["edge_role"], e["event_type"], e["distance"], e["attributed_flow"])
                    for e in res_eq["edges"]
                ]
                assert_zero_ulp_equal(eq_edges_topo, base_edges_topo, f"{cid}.scaled_vs_abs_edges")

            if "bracketing" in cid:
                bracketing_records.append(
                    {
                        "case_id": cid,
                        "high_uncertainty": rust_res["summary"]["high_uncertainty"],
                        "high_uncertainty_edge_count": rust_res["summary"]["high_uncertainty_edge_count"],
                        "min_threshold_margin": rust_res["summary"]["min_threshold_margin"],
                        "min_optimality_margin": rust_res["summary"]["min_optimality_margin"],
                        "min_secondary_margin": rust_res["summary"]["min_secondary_margin"],
                    }
                )

            fixed_pair_records.append(
                {
                    "case_id": cid,
                    "status": "pass",
                    "mode": c.get("mode", "scaled" if c.get("threshold_coefficient") is not None else "absolute"),
                    "primary_edge_count": rust_res["summary"]["primary_edge_count"],
                    "secondary_split_edge_count": rust_res["summary"]["secondary_split_edge_count"],
                    "secondary_merge_edge_count": rust_res["summary"]["secondary_merge_edge_count"],
                    "continuation_count": rust_res["summary"]["continuation_count"],
                    "split_count": rust_res["summary"]["split_count"],
                    "merge_count": rust_res["summary"]["merge_count"],
                    "emergence_count": rust_res["summary"]["emergence_count"],
                    "disappearance_count": rust_res["summary"]["disappearance_count"],
                    "optima_count": rust_res["backbone"]["optima_count"],
                    "ambiguous_tie": rust_res["summary"]["ambiguous_tie"],
                    "high_uncertainty": rust_res["summary"]["high_uncertainty"],
                    "berge_invariants_passed": invariants["all_passed"],
                    "zero_ulp_verified": True,
                }
            )

        # Also verify single-item valid BackboneAssertion + UTF-8 non-ASCII case_id + 4-cycle primary-tie split edge case
        sample_base = clean_pair_dto(pair_cases[3])  # rect_2_to_3_split_continuation
        rc_sb, sb_res, err_sb = run_m3_runner(runner_bin, "pair", sample_base)
        if rc_sb != 0 or sb_res is None:
            raise AssertionError(f"Single-item pair execution failed: {err_sb}")
        with_backbone = copy.deepcopy(sample_base)
        with_backbone["case_id"] = "中文_°N_主干核验_valid_backbone"
        with_backbone["backbone"] = {
            "prev_cluster_ids": sb_res["prev_cluster_ids"],
            "curr_cluster_ids": sb_res["curr_cluster_ids"],
            "threshold": sb_res["threshold"],
            "distance_matrix": sb_res["backbone"]["distance_matrix"],
            "eligible_matrix": [
                [d <= sb_res["threshold"] for d in row] for row in sb_res["backbone"]["distance_matrix"]
            ],
            "primary_links": [
                [l["prev_cluster_id"], l["curr_cluster_id"]] for l in sb_res["backbone"]["links"]
            ],
        }
        rc_wb, wb_res, err_wb = run_m3_runner(runner_bin, "pair", with_backbone)
        if rc_wb != 0 or wb_res is None:
            raise AssertionError(f"Valid BackboneAssertion + UTF-8 check failed: {err_wb}")
        assert_zero_ulp_equal(wb_res, sb_res, "valid_backbone_assertion_equivalence")

        # Verify subtle 4-cycle primary tie with non-tied nearest secondary split parent
        subtle_tie_case = {
            "case_id": "cross_component_primary_tie_unique_secondary_split",
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 1.0,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 25, "member_fraction": 0.5, "mean_features": [0.0]},
                {"cluster_id": 1, "member_count": 25, "member_fraction": 0.5, "mean_features": [1.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 20, "member_fraction": 0.4, "mean_features": [-0.6]},
                {"cluster_id": 1, "member_count": 20, "member_fraction": 0.4, "mean_features": [1.3]},
                {"cluster_id": 2, "member_count": 10, "member_fraction": 0.2, "mean_features": [0.6]},
            ],
        }
        rc_st, st_res, err_st = run_m3_runner(runner_bin, "pair", subtle_tie_case)
        if rc_st != 0 or st_res is None:
            raise AssertionError(f"Subtle cross-component tie case failed: {err_st}")
        assert_pair_zero_ulp_agreement(subtle_tie_case["case_id"], st_res, solve_evolution_oracle(subtle_tie_case))

        # =====================================================================
        # 2. Both Orthogonality Series (`orthogonality-separation.json`)
        # =====================================================================
        morph_series = orth_series["pure_morphology_drift_series"]
        frac_series = orth_series["pure_fraction_redistribution_series"]

        rc_ms, ms_batch_out, err_ms = run_m3_runner(
            runner_bin,
            "pair",
            {
                "cases": [
                    clean_pair_dto({**step, "case_id": f"morph_step_{step['step_index']}"})
                    for step in morph_series["steps"]
                ]
            },
        )
        if rc_ms != 0 or ms_batch_out is None:
            raise AssertionError(f"pure_morphology_drift_series failed: {err_ms}")

        morph_max_dists: list[float] = []
        morph_tot_dists: list[float] = []
        morph_tv_vals: list[float] = []
        morph_step_records: list[dict[str, Any]] = []
        baseline_flows: list[float] | None = None

        for step, item in zip(morph_series["steps"], ms_batch_out["results"]):
            s_idx = step["step_index"]
            scid = f"pure_morphology_drift_step_{s_idx}"
            if item.get("status") != "ok":
                raise AssertionError(f"{scid} failed: {item}")
            r_step = item["result"]
            o_step = solve_evolution_oracle(step)
            assert_pair_zero_ulp_agreement(scid, r_step, o_step)
            assert_fixture_expected_subset(scid, r_step, step["expected"])

            # Assert all fraction change metrics are bitwise +0.0
            assert_bitwise_zero(r_step["summary"]["transition_tv_redistribution"], f"{scid}.tv_redistribution")
            assert_bitwise_zero(r_step["summary"]["component_tv_residual"], f"{scid}.component_tv_residual")
            assert_bitwise_zero(r_step["summary"]["secondary_attributed_flow"], f"{scid}.secondary_flow")
            assert_bitwise_zero(r_step["summary"]["disappeared_fraction_sum"], f"{scid}.disappeared_sum")
            assert_bitwise_zero(r_step["summary"]["emerged_fraction_sum"], f"{scid}.emerged_sum")
            for e in r_step["edges"]:
                assert_bitwise_zero(e["edge_delta_fraction"], f"{scid}.edge_delta_fraction")
            for n in (*r_step["prev_nodes"], *r_step["curr_nodes"]):
                assert_bitwise_zero(n["primary_delta_fraction"], f"{scid}.node.primary_delta_fraction")
                assert_bitwise_zero(n["unattributed_fraction"], f"{scid}.node.unattributed_fraction")
            for ev in r_step["events"]:
                assert_bitwise_zero(ev["net_delta_fraction"], f"{scid}.event.net_delta_fraction")

            step_flows = [e["attributed_flow"] for e in r_step["edges"]]
            if baseline_flows is None:
                baseline_flows = step_flows
            else:
                assert_zero_ulp_equal(step_flows, baseline_flows, f"{scid}.constant_attributed_flows")

            morph_max_dists.append(r_step["summary"]["primary_max_distance"])
            morph_tot_dists.append(r_step["summary"]["primary_total_distance"])
            morph_tv_vals.append(r_step["summary"]["transition_tv_redistribution"])
            morph_step_records.append(
                {
                    "step_index": s_idx,
                    "theoretical_shift_k": step["theoretical_shift_k"],
                    "primary_max_distance": r_step["summary"]["primary_max_distance"],
                    "primary_total_distance": r_step["summary"]["primary_total_distance"],
                    "transition_tv_redistribution": r_step["summary"]["transition_tv_redistribution"],
                    "attributed_flows": step_flows,
                }
            )

        if not all(morph_max_dists[i] < morph_max_dists[i + 1] for i in range(len(morph_max_dists) - 1)):
            raise AssertionError(f"pure_morphology_drift_series distances not strictly monotonic: {morph_max_dists}")
        if not all(morph_tot_dists[i] < morph_tot_dists[i + 1] for i in range(len(morph_tot_dists) - 1)):
            raise AssertionError(f"pure_morphology_drift_series total distances not strictly monotonic: {morph_tot_dists}")

        rc_fs, fs_batch_out, err_fs = run_m3_runner(
            runner_bin,
            "pair",
            {
                "cases": [
                    clean_pair_dto({**step, "case_id": f"frac_step_{step['step_index']}"})
                    for step in frac_series["steps"]
                ]
            },
        )
        if rc_fs != 0 or fs_batch_out is None:
            raise AssertionError(f"pure_fraction_redistribution_series failed: {err_fs}")

        expected_tv_sequence = [0.0, 0.25, 0.375]
        observed_tv_sequence: list[float] = []
        frac_tot_dists: list[float] = []
        frac_max_dists: list[float] = []
        frac_step_records: list[dict[str, Any]] = []

        for step, item, exp_tv in zip(frac_series["steps"], fs_batch_out["results"], expected_tv_sequence):
            s_idx = step["step_index"]
            scid = f"pure_fraction_redistribution_step_{s_idx}"
            if item.get("status") != "ok":
                raise AssertionError(f"{scid} failed: {item}")
            r_step = item["result"]
            o_step = solve_evolution_oracle(step)
            assert_pair_zero_ulp_agreement(scid, r_step, o_step)
            assert_fixture_expected_subset(scid, r_step, step["expected"])

            # Assert all morphology distances are bitwise +0.0
            assert_bitwise_zero(r_step["summary"]["primary_total_distance"], f"{scid}.primary_total_distance")
            assert_bitwise_zero(r_step["summary"]["primary_max_distance"], f"{scid}.primary_max_distance")
            assert_bitwise_zero(
                r_step["summary"]["primary_max_distance_over_scale"], f"{scid}.primary_max_distance_over_scale"
            )
            assert_bitwise_zero(r_step["summary"]["all_edges_max_distance"], f"{scid}.all_edges_max_distance")
            assert_bitwise_zero(
                r_step["summary"]["all_edges_max_distance_over_scale"], f"{scid}.all_edges_max_distance_over_scale"
            )
            for e in r_step["edges"]:
                assert_bitwise_zero(e["distance"], f"{scid}.edge.distance")
                assert_bitwise_zero(e["distance_over_scale"], f"{scid}.edge.distance_over_scale")
                assert_zero_ulp_equal(e["edge_strength"], 1.0, f"{scid}.edge.edge_strength")
            for ev in r_step["events"]:
                assert_bitwise_zero(ev["max_morphology_distance"], f"{scid}.event.max_morphology_distance")
                assert_bitwise_zero(ev["max_morphology_over_scale"], f"{scid}.event.max_morphology_over_scale")

            assert_zero_ulp_equal(r_step["summary"]["transition_tv_redistribution"], exp_tv, f"{scid}.tv")
            assert_zero_ulp_equal(
                r_step["summary"]["transition_tv_redistribution"],
                step["theoretical_tv_redistribution"],
                f"{scid}.theoretical_tv",
            )
            observed_tv_sequence.append(r_step["summary"]["transition_tv_redistribution"])
            frac_tot_dists.append(r_step["summary"]["primary_total_distance"])
            frac_max_dists.append(r_step["summary"]["all_edges_max_distance"])
            frac_step_records.append(
                {
                    "step_index": s_idx,
                    "label": step.get("label", f"step_{s_idx}"),
                    "theoretical_tv_redistribution": exp_tv,
                    "observed_tv_redistribution": r_step["summary"]["transition_tv_redistribution"],
                    "primary_total_distance": r_step["summary"]["primary_total_distance"],
                    "all_edges_max_distance": r_step["summary"]["all_edges_max_distance"],
                }
            )

        assert_zero_ulp_equal(observed_tv_sequence, expected_tv_sequence, "pure_fraction_tv_sequence")

        # =====================================================================
        # 3. All 4 Multi-Batch Chain Cases (`m3_runner --mode chain` vs Oracle)
        # =====================================================================
        chain_run_key = "chain0000000fedcba987654"
        rc_ch, chain_batch_out, err_ch = run_m3_runner(
            runner_bin,
            "chain",
            {"cases": [clean_chain_dto(c, default_run_key=chain_run_key) for c in chain_cases]},
        )
        if rc_ch != 0 or chain_batch_out is None:
            raise AssertionError(f"Chain batch execution failed (rc={rc_ch}): {err_ch}")

        chain_records: list[dict[str, Any]] = []
        for idx, (c, item) in enumerate(zip(chain_cases, chain_batch_out["results"])):
            cid = c["case_id"]
            if item.get("status") != "ok":
                raise AssertionError(f"Chain case {cid} failed in batch: {item}")
            rust_ch = item["result"]
            ora_in = copy.deepcopy(c)
            ora_in["tracking_run_key"] = chain_run_key
            ora_ch = solve_chain_oracle(ora_in)
            ch_inv = assert_chain_zero_ulp_agreement(
                cid,
                rust_ch,
                ora_ch,
                expected_tracking_run_key=chain_run_key,
            )

            # Also verify single-case `--mode chain` matches batch-case 0-ULP on first chain case
            if idx == 0:
                rc_sc, single_ch_out, err_sc = run_m3_runner(
                    runner_bin, "chain", clean_chain_dto(c, default_run_key=chain_run_key)
                )
                if rc_sc != 0 or single_ch_out is None:
                    raise AssertionError(f"Single-item chain run failed: {err_sc}")
                assert_zero_ulp_equal(single_ch_out, rust_ch, f"{cid}.single_vs_batch")

            # Verify all fixture expected fields (chain_summary, transitions, branch_summaries)
            assert_chain_fixture_expected(c, rust_ch)

            chain_records.append(
                {
                    "case_id": cid,
                    "status": "pass",
                    "planned_batch_count": rust_ch["planned_batch_count"],
                    "branch_count": rust_ch["chain_summary"]["branch_count"],
                    "nodes_count": len(rust_ch["nodes"]),
                    "edges_count": len(rust_ch["edges"]),
                    "events_count": len(rust_ch["events"]),
                    "chain_summary": rust_ch["chain_summary"],
                    "invariants": ch_inv,
                    "zero_ulp_verified": True,
                }
            )

        # Also verify `--mode chain` end-to-end from raw `members` + `cluster_config` including whitelisted single-batch failure
        raw_members_chain = {
            "case_id": "chain_4batch_raw_members_with_rank_failure_gap",
            "tracking_run_key": "rawmem1234567890",
            "threshold_coefficient": 1.2,
            "weights": [0.5, 0.5],
            "cluster_config": {
                "projection_dim": 2,
                "cluster_count": 2,
                "initialization": "kmeans++_chacha8_v1",
                "seed": 3,
                "restarts": 5,
                "initial_centers": [],
                "stopping_rule": "sklearn_variance_scaled_v1",
                "tolerance": 0.0001,
                "max_iterations": 100,
                "rank_threshold": 1e-8,
            },
            "batches": [
                {
                    "ordinal": 0,
                    "status": "ok",
                    "members": [
                        {"member_index": 0, "member_id": "m0", "features": [275.0, 276.0]},
                        {"member_index": 1, "member_id": "m1", "features": [275.4, 275.8]},
                        {"member_index": 2, "member_id": "m2", "features": [285.0, 286.0]},
                        {"member_index": 3, "member_id": "m3", "features": [285.5, 285.7]},
                    ],
                },
                {
                    "ordinal": 1,
                    "status": "ok",
                    "members": [
                        {"member_index": 0, "member_id": "m0", "features": [275.2, 276.1]},
                        {"member_index": 1, "member_id": "m1", "features": [275.5, 275.9]},
                        {"member_index": 2, "member_id": "m2", "features": [285.2, 286.1]},
                        {"member_index": 3, "member_id": "m3", "features": [285.6, 285.8]},
                    ],
                },
                {
                    "ordinal": 2,
                    "status": "ok",
                    # Collinear 1D variation triggers whitelisted INSUFFICIENT_RANK (rank=1 < projection_dim=2) -> status="failed"!
                    "members": [
                        {"member_index": 0, "member_id": "m0", "features": [270.0, 270.0]},
                        {"member_index": 1, "member_id": "m1", "features": [272.0, 272.0]},
                        {"member_index": 2, "member_id": "m2", "features": [274.0, 274.0]},
                        {"member_index": 3, "member_id": "m3", "features": [276.0, 276.0]},
                    ],
                },
                {
                    "ordinal": 3,
                    "status": "ok",
                    "members": [
                        {"member_index": 0, "member_id": "m0", "features": [276.0, 277.0]},
                        {"member_index": 1, "member_id": "m1", "features": [276.3, 276.8]},
                        {"member_index": 2, "member_id": "m2", "features": [286.0, 287.0]},
                        {"member_index": 3, "member_id": "m3", "features": [286.4, 286.7]},
                    ],
                },
            ],
        }
        rc_rm, rm_ch_out, err_rm = run_m3_runner(runner_bin, "chain", raw_members_chain)
        if rc_rm != 0 or rm_ch_out is None:
            raise AssertionError(f"Raw-members chain run failed: {err_rm}")
        if rm_ch_out["batches"][2]["status"] != "failed" or rm_ch_out["batches"][2]["status_code"] != "INSUFFICIENT_RANK":
            raise AssertionError(f"Expected ordinal 2 whitelisted INSUFFICIENT_RANK failed status, got {rm_ch_out['batches'][2]}")
        if rm_ch_out["chain_summary"]["gap_transition_count"] != 2:
            raise AssertionError(f"Expected 2 gap transitions around failed batch 2, got {rm_ch_out['chain_summary']}")

        # Verify canonical (prev_ordinal, branch_id) ordering for absorbed_secondary_branch_ids
        # when prev_ordinal=0 absorbs `:b00:k02` (lexicographically larger) and prev_ordinal=1 absorbs `:b00:k01`
        multi_step_absorb_key = "absorb0123456789"
        multi_step_absorb_chain = {
            "case_id": "chain_4batch_multistep_secondary_absorption_order",
            "tracking_run_key": multi_step_absorb_key,
            "threshold_coefficient": 1.0,
            "weights": [1.0, 1.0],
            "batches": [
                {
                    "ordinal": 0,
                    "status": "ok",
                    "dispersion": 2.0,
                    "scenarios": [
                        {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.0, 0.0]},
                        {"cluster_id": 1, "member_count": 15, "member_fraction": 0.30, "mean_features": [0.0, 1.5]},
                        {"cluster_id": 2, "member_count": 15, "member_fraction": 0.30, "mean_features": [0.5, 0.0]},
                    ],
                },
                {
                    "ordinal": 1,
                    "status": "ok",
                    "dispersion": 2.0,
                    "scenarios": [
                        {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [0.0, 0.0]},
                        {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.0, 1.5]},
                    ],
                },
                {
                    "ordinal": 2,
                    "status": "ok",
                    "dispersion": 2.0,
                    "scenarios": [
                        {"cluster_id": 0, "member_count": 50, "member_fraction": 1.00, "mean_features": [0.0, 0.0]},
                    ],
                },
                {
                    "ordinal": 3,
                    "status": "ok",
                    "dispersion": 2.0,
                    "scenarios": [
                        {"cluster_id": 0, "member_count": 50, "member_fraction": 1.00, "mean_features": [0.0, 0.0]},
                    ],
                },
            ],
        }
        rc_msa, msa_rust, err_msa = run_m3_runner(runner_bin, "chain", multi_step_absorb_chain)
        if rc_msa != 0 or msa_rust is None:
            raise AssertionError(f"Multi-step absorption order chain failed: {err_msa}")
        msa_ora = solve_chain_oracle(multi_step_absorb_chain)
        assert_chain_zero_ulp_agreement(
            multi_step_absorb_chain["case_id"],
            msa_rust,
            msa_ora,
            expected_tracking_run_key=multi_step_absorb_key,
        )
        msa_main_branch = next(
            b for b in msa_rust["branches"] if b["branch_id"] == f"{multi_step_absorb_key[:12]}:b00:k00"
        )
        expected_absorbed_order = [
            f"{multi_step_absorb_key[:12]}:b00:k02",
            f"{multi_step_absorb_key[:12]}:b00:k01",
        ]
        if msa_main_branch["absorbed_secondary_branch_ids"] != expected_absorbed_order:
            raise AssertionError(
                f"Expected canonical (prev_ordinal, branch_id) absorbed order {expected_absorbed_order}, "
                f"got {msa_main_branch['absorbed_secondary_branch_ids']}"
            )

        # =====================================================================
        # 4. 4,000 Seeded Random Pair Cases (`seed=20261125`, 0-ULP vs Oracle)
        # =====================================================================
        random_cases = generate_random_evolution_cases(count=4000, seed=20261125)
        if len(random_cases) != 4000:
            raise AssertionError(f"Expected 4000 random cases, got {len(random_cases)}")

        rc_rand, rand_batch_out, err_rand = run_m3_runner(
            runner_bin,
            "pair",
            {"cases": [clean_pair_dto(c) for c in random_cases]},
        )
        if rc_rand != 0 or rand_batch_out is None:
            raise AssertionError(f"4000 random pair batch failed (rc={rc_rand}): {err_rand}")

        event_totals = {"continuation": 0, "split": 0, "merge": 0, "emergence": 0, "disappearance": 0}
        edge_role_totals = {"primary": 0, "secondary_split": 0, "secondary_merge": 0}
        scaled_count = 0
        absolute_count = 0
        covered_1_to_5_count = 0
        high_unc_transitions = 0
        ambig_transitions = 0

        for c, item in zip(random_cases, rand_batch_out["results"]):
            cid = c["case_id"]
            if item.get("status") != "ok":
                raise AssertionError(f"Random case {cid} failed in runner: {item}")
            r_res = item["result"]
            o_res = solve_evolution_oracle(c)
            assert_pair_zero_ulp_agreement(cid, r_res, o_res)

            if c.get("mode") == "scaled":
                scaled_count += 1
            else:
                absolute_count += 1
            m_len = len(c["prev_scenarios"])
            n_len = len(c["curr_scenarios"])
            if 1 <= m_len <= 5 and 1 <= n_len <= 5:
                covered_1_to_5_count += 1

            s = r_res["summary"]
            event_totals["continuation"] += s["continuation_count"]
            event_totals["split"] += s["split_count"]
            event_totals["merge"] += s["merge_count"]
            event_totals["emergence"] += s["emergence_count"]
            event_totals["disappearance"] += s["disappearance_count"]
            edge_role_totals["primary"] += s["primary_edge_count"]
            edge_role_totals["secondary_split"] += s["secondary_split_edge_count"]
            edge_role_totals["secondary_merge"] += s["secondary_merge_edge_count"]
            if s["high_uncertainty"]:
                high_unc_transitions += 1
            if s["ambiguous_tie"]:
                ambig_transitions += 1

        # =====================================================================
        # 5. Permutation Invariance (2,000 Random + All 144 of 3x4 + Symmetric)
        #    + Weight Scaling Invariance (2^k Bitwise + General 7.3x <= 1e-15)
        # =====================================================================
        perm_rng = random.Random(20261126)
        perm_payloads: list[dict[str, Any]] = []
        perm_mappings: list[tuple[dict[int, int], dict[int, int]]] = []

        for p_idx in range(2000):
            base_c = random_cases[p_idx]
            m = len(base_c["prev_scenarios"])
            n = len(base_c["curr_scenarios"])
            p_order = list(range(m))
            c_order = list(range(n))
            perm_rng.shuffle(p_order)
            perm_rng.shuffle(c_order)

            # Assign non-monotonic scrambled cluster_ids so relative cluster_id ordering is permuted too
            new_p_cids = perm_rng.sample(range(10, 500), m)
            new_c_cids = perm_rng.sample(range(500, 990), n)

            p_new_to_base = {
                new_p_cids[slot]: int(base_c["prev_scenarios"][orig_i]["cluster_id"])
                for slot, orig_i in enumerate(p_order)
            }
            c_new_to_base = {
                new_c_cids[slot]: int(base_c["curr_scenarios"][orig_j]["cluster_id"])
                for slot, orig_j in enumerate(c_order)
            }
            perm_mappings.append((p_new_to_base, c_new_to_base))

            p_case = clean_pair_dto(base_c)
            p_case["case_id"] = f"perm_rand_{p_idx:04d}"
            p_case["prev_scenarios"] = [
                {**copy.deepcopy(base_c["prev_scenarios"][orig_i]), "cluster_id": new_p_cids[slot]}
                for slot, orig_i in enumerate(p_order)
            ]
            p_case["curr_scenarios"] = [
                {**copy.deepcopy(base_c["curr_scenarios"][orig_j]), "cluster_id": new_c_cids[slot]}
                for slot, orig_j in enumerate(c_order)
            ]
            perm_payloads.append(p_case)

        rc_perm, perm_batch_out, err_perm = run_m3_runner(
            runner_bin, "pair", {"cases": perm_payloads}
        )
        if rc_perm != 0 or perm_batch_out is None:
            raise AssertionError(f"2000 random permutation batch failed: {err_perm}")

        perm_invariant_count = 0
        perm_symmetric_tie_count = 0
        for p_idx, (perm_item, (p_map, c_map)) in enumerate(zip(perm_batch_out["results"], perm_mappings)):
            if perm_item.get("status") != "ok":
                raise AssertionError(f"Permutation {p_idx} failed: {perm_item}")
            base_c = random_cases[p_idx]
            base_r = rand_batch_out["results"][p_idx]["result"]
            perm_r = perm_item["result"]

            p_feats = [tuple(0.0 if x == 0.0 else x for x in s["mean_features"]) for s in base_c["prev_scenarios"]]
            c_feats = [tuple(0.0 if x == 0.0 else x for x in s["mean_features"]) for s in base_c["curr_scenarios"]]
            has_bit_identical_twin = (len(set(p_feats)) < len(p_feats)) or (len(set(c_feats)) < len(c_feats))

            base_identity_map_p = {int(s["cluster_id"]): int(s["cluster_id"]) for s in base_c["prev_scenarios"]}
            base_identity_map_c = {int(s["cluster_id"]): int(s["cluster_id"]) for s in base_c["curr_scenarios"]}
            base_proj = map_result_to_base_ids(base_r, base_identity_map_p, base_identity_map_c)
            perm_proj = map_result_to_base_ids(perm_r, p_map, c_map)

            if has_bit_identical_twin and base_proj != perm_proj:
                if not (base_r["summary"]["ambiguous_tie"] and perm_r["summary"]["ambiguous_tie"]):
                    raise AssertionError(
                        f"Permutation {p_idx} changed output on twin features without ambiguous_tie=True"
                    )
                assert_pair_zero_ulp_agreement(f"perm_sym_{p_idx}", perm_r, solve_evolution_oracle(perm_payloads[p_idx]))
                perm_symmetric_tie_count += 1
            else:
                assert_zero_ulp_equal(perm_proj, base_proj, f"perm_rand_{p_idx:04d}")
                perm_invariant_count += 1

        # Exhaustive 3! * 4! = 144 permutations of `nontrivial_3x4_for_144_permutations`
        case_3x4 = next(c for c in pair_cases if c["case_id"] == "nontrivial_3x4_for_144_permutations")
        idx_3x4 = next(i for i, c in enumerate(pair_cases) if c["case_id"] == "nontrivial_3x4_for_144_permutations")
        ref_3x4_res = fixed_batch_out["results"][idx_3x4]["result"]
        ref_3x4_proj = map_result_to_base_ids(
            ref_3x4_res,
            {i: i for i in range(3)},
            {j: j for j in range(4)},
        )

        perm_144_cases: list[dict[str, Any]] = []
        perm_144_maps: list[tuple[dict[int, int], dict[int, int]]] = []
        for p_idx_3, p_perm in enumerate(itertools.permutations(range(3))):
            for c_idx_4, c_perm in enumerate(itertools.permutations(range(4))):
                # Reverse-ordered & strided cluster_ids to exercise full label + order independence
                p_cids = [70 - slot * 11 for slot in range(3)]
                c_cids = [190 - slot * 13 for slot in range(4)]
                p_map_144 = {
                    p_cids[slot]: int(case_3x4["prev_scenarios"][orig_i]["cluster_id"])
                    for slot, orig_i in enumerate(p_perm)
                }
                c_map_144 = {
                    c_cids[slot]: int(case_3x4["curr_scenarios"][orig_j]["cluster_id"])
                    for slot, orig_j in enumerate(c_perm)
                }
                perm_144_maps.append((p_map_144, c_map_144))
                pc144 = clean_pair_dto(case_3x4)
                pc144["case_id"] = f"perm144_{p_idx_3}_{c_idx_4}"
                pc144["prev_scenarios"] = [
                    {**copy.deepcopy(case_3x4["prev_scenarios"][orig_i]), "cluster_id": p_cids[slot]}
                    for slot, orig_i in enumerate(p_perm)
                ]
                pc144["curr_scenarios"] = [
                    {**copy.deepcopy(case_3x4["curr_scenarios"][orig_j]), "cluster_id": c_cids[slot]}
                    for slot, orig_j in enumerate(c_perm)
                ]
                perm_144_cases.append(pc144)

        if len(perm_144_cases) != 144:
            raise AssertionError(f"Expected 144 3x4 permutations, built {len(perm_144_cases)}")

        rc_144, out_144, err_144 = run_m3_runner(runner_bin, "pair", {"cases": perm_144_cases})
        if rc_144 != 0 or out_144 is None:
            raise AssertionError(f"144 exhaustive 3x4 permutations failed: {err_144}")

        for idx144, (item144, (pm, cm)) in enumerate(zip(out_144["results"], perm_144_maps)):
            if item144.get("status") != "ok":
                raise AssertionError(f"3x4 permutation {idx144} failed: {item144}")
            proj144 = map_result_to_base_ids(item144["result"], pm, cm)
            assert_zero_ulp_equal(proj144, ref_3x4_proj, f"perm144[{idx144}]")

        # Explicit bit-identical symmetric feature vector permutation check (asserting `ambiguous_tie == True`)
        sym_twin_case_a = {
            "case_id": "sym_twin_order_a",
            "mode": "absolute",
            "weights": [1.0, 1.0],
            "threshold": 2.0,
            "prev_scenarios": [
                {"cluster_id": 10, "member_count": 25, "member_fraction": 0.5, "mean_features": [1.0, 1.0]},
                {"cluster_id": 20, "member_count": 25, "member_fraction": 0.5, "mean_features": [1.0, 1.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 5, "member_count": 50, "member_fraction": 1.0, "mean_features": [1.2, 1.2]},
            ],
        }
        sym_twin_case_b = {
            **copy.deepcopy(sym_twin_case_a),
            "case_id": "sym_twin_order_b",
            "prev_scenarios": [
                {"cluster_id": 30, "member_count": 25, "member_fraction": 0.5, "mean_features": [1.0, 1.0]},
                {"cluster_id": 5, "member_count": 25, "member_fraction": 0.5, "mean_features": [1.0, 1.0]},
            ],
        }
        rc_sym, out_sym, err_sym = run_m3_runner(runner_bin, "pair", {"cases": [sym_twin_case_a, sym_twin_case_b]})
        if rc_sym != 0 or out_sym is None:
            raise AssertionError(f"Symmetric twin tie check failed: {err_sym}")
        for sc_in, sym_item in zip((sym_twin_case_a, sym_twin_case_b), out_sym["results"]):
            sym_r = sym_item["result"]
            if not sym_r["summary"]["ambiguous_tie"]:
                raise AssertionError("Bit-identical symmetric twin case did not set ambiguous_tie=True")
            assert_pair_zero_ulp_agreement(sc_in["case_id"], sym_r, solve_evolution_oracle(sc_in))

        # Weight scaling invariance: 25 fixed pair_cases + 100 random cases = 125 cases
        scale_targets = [
            *[(c, fixed_batch_out["results"][i]["result"]) for i, c in enumerate(pair_cases)],
            *[(random_cases[i], rand_batch_out["results"][i]["result"]) for i in range(100)],
        ]
        pow2_cases: list[dict[str, Any]] = []
        gen_cases: list[dict[str, Any]] = []
        for idx_s, (base_c, _) in enumerate(scale_targets):
            c_pow2 = clean_pair_dto(base_c)
            c_pow2["case_id"] = f"scale_2k_{idx_s}"
            c_pow2["weights"] = [w * 1024.0 for w in c_pow2["weights"]]
            pow2_cases.append(c_pow2)

            c_gen = clean_pair_dto(base_c)
            c_gen["case_id"] = f"scale_7p3_{idx_s}"
            c_gen["weights"] = [w * 7.3 for w in c_gen["weights"]]
            gen_cases.append(c_gen)

        rc_p2, out_p2, err_p2 = run_m3_runner(runner_bin, "pair", {"cases": pow2_cases})
        rc_gn, out_gn, err_gn = run_m3_runner(runner_bin, "pair", {"cases": gen_cases})
        if rc_p2 != 0 or out_p2 is None or rc_gn != 0 or out_gn is None:
            raise AssertionError(f"Weight scaling batch failed: p2={err_p2}, gn={err_gn}")

        max_general_rtol = 0.0
        general_flips = 0
        for idx_s, ((base_c, base_r), item_p2, item_gn) in enumerate(
            zip(scale_targets, out_p2["results"], out_gn["results"])
        ):
            if item_p2.get("status") != "ok" or item_gn.get("status") != "ok":
                raise AssertionError(f"Weight scaling case {idx_s} failed")
            # 2^10 = 1024.0 scaling must be 0-ULP bitwise identical across the ENTIRE EvolutionResult!
            assert_zero_ulp_equal(item_p2["result"], base_r, f"scale_2k[{idx_s}]")

            res_gn = item_gn["result"]
            for row_b, row_g in zip(base_r["backbone"]["distance_matrix"], res_gn["backbone"]["distance_matrix"]):
                for db, dg in zip(row_b, row_g):
                    if db == 0.0:
                        assert_bitwise_zero(dg, f"scale_7p3[{idx_s}].zero_dist")
                    else:
                        rtol = abs(db - dg) / abs(db)
                        max_general_rtol = max(max_general_rtol, rtol)
                        if rtol > 1e-15:
                            raise AssertionError(
                                f"General weight scaling 7.3x exceeded rtol 1e-15 on case {idx_s}: rtol={rtol!r}"
                            )
            base_edge_pairs = [
                (e["prev_cluster_id"], e["curr_cluster_id"], e["edge_role"]) for e in base_r["edges"]
            ]
            gen_edge_pairs = [
                (e["prev_cluster_id"], e["curr_cluster_id"], e["edge_role"]) for e in res_gn["edges"]
            ]
            if base_edge_pairs != gen_edge_pairs:
                general_flips += 1

        # =====================================================================
        # 6. All 19 Fixture Negative Cases + DIMENSION_MISMATCH + Chain/CLI
        #    + CoreError <= 112B Budget (`error-cases.json`)
        # =====================================================================
        fixture_neg_ids = {c["case_id"] for c in negative_cases}
        extra_negative_pair_cases: list[dict[str, Any]] = [
            {
                "case_id": "dimension_mismatch_weights_vs_features",
                "mode": "absolute",
                "weights": [0.5, 0.25, 0.25],
                "threshold": 2.0,
                "prev_scenarios": [
                    {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [1.0, 2.0]}
                ],
                "curr_scenarios": [
                    {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [1.1, 2.1]}
                ],
                "expected_code": "DIMENSION_MISMATCH",
                "expected_location": {},
            },
            {
                "case_id": "subnormal_pair_scale_ratio_overflow",
                "mode": "scaled",
                "weights": [1.0],
                "threshold_coefficient": 1e308,
                "prev_dispersion": 1e-308,
                "curr_dispersion": 1e-308,
                "prev_scenarios": [
                    {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}
                ],
                "curr_scenarios": [
                    {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [10.0]}
                ],
                "expected_code": "NUMERICAL_FAILURE",
                "expected_location": {},
            },
        ]
        all_negative_pair_cases = [*negative_cases, *extra_negative_pair_cases]

        # Batch-mode error reporting across all negative pair cases
        rc_nb, neg_batch_out, err_nb = run_m3_runner(
            runner_bin,
            "pair",
            {"cases": [clean_pair_dto(c) for c in all_negative_pair_cases]},
        )
        if rc_nb != 0 or neg_batch_out is None:
            raise AssertionError(f"Negative batch execution failed (rc={rc_nb}): {err_nb}")

        error_case_records: list[dict[str, Any]] = []
        distinct_error_codes: set[str] = set()

        for c, b_item in zip(all_negative_pair_cases, neg_batch_out["results"]):
            cid = c["case_id"]
            exp_code = c["expected_code"]
            exp_loc = c.get("expected_location", {})

            # 1) Verify batch item error reporting
            if b_item.get("status") != "error":
                raise AssertionError(f"Negative case {cid} unexpectedly succeeded in batch mode: {b_item}")
            if b_item.get("code") != exp_code:
                raise AssertionError(f"Negative case {cid}: batch code {b_item.get('code')!r} != {exp_code!r}")
            for loc_k, loc_v in exp_loc.items():
                if b_item.get(loc_k) != loc_v:
                    raise AssertionError(
                        f"Negative case {cid}: batch {loc_k}={b_item.get(loc_k)!r} != expected {loc_v!r}"
                    )

            # 2) Verify OracleValidationError parity (code + location indices)
            try:
                solve_evolution_oracle(c)
            except OracleValidationError as ora_err:
                if ora_err.code != exp_code:
                    raise AssertionError(f"Negative case {cid}: oracle code {ora_err.code!r} != {exp_code!r}")
                if (
                    ora_err.prev_index != b_item.get("prev_index")
                    or ora_err.curr_index != b_item.get("curr_index")
                    or ora_err.feature_index != b_item.get("feature_index")
                ):
                    raise AssertionError(
                        f"Negative case {cid}: oracle indices ({ora_err.prev_index}, {ora_err.curr_index}, {ora_err.feature_index}) "
                        f"!= Rust indices ({b_item.get('prev_index')}, {b_item.get('curr_index')}, {b_item.get('feature_index')})"
                    )
            else:
                raise AssertionError(f"Negative case {cid} unexpectedly succeeded in Python oracle")

            # 3) Verify single-item execution exits 1 with 0 output file residue and 0 .tmp_* residue
            rc_single, out_single, stderr_single = run_m3_runner(
                runner_bin,
                "pair",
                clean_pair_dto(c),
                verify_zero_output_on_error=True,
            )
            if rc_single != 1 or out_single is not None:
                raise AssertionError(
                    f"Negative single case {cid}: expected exit code 1 and None output, got rc={rc_single}"
                )
            if exp_code not in stderr_single:
                raise AssertionError(
                    f"Negative single case {cid}: expected {exp_code!r} in stderr, got {stderr_single!r}"
                )

            distinct_error_codes.add(exp_code)
            error_case_records.append(
                {
                    "case_id": cid,
                    "source": "fixture" if cid in fixture_neg_ids else "supplementary",
                    "expected_code": exp_code,
                    "observed_code": b_item["code"],
                    "prev_index": b_item.get("prev_index"),
                    "curr_index": b_item.get("curr_index"),
                    "feature_index": b_item.get("feature_index"),
                    "single_exit_code": rc_single,
                    "zero_output_residue": True,
                    "oracle_parity": True,
                    "status": "pass",
                }
            )

        # Also verify bare unquoted NaN/Infinity JSON sanitization in m3_runner, chain <4 batches rejection, and CLI exit 2
        bare_nan_raw_json = (
            '{"mode":"absolute","weights":[1.0],"threshold":1.0,'
            '"prev_scenarios":[{"cluster_id":0,"member_count":50,"member_fraction":1.0,"mean_features":[NaN]}],'
            '"curr_scenarios":[{"cluster_id":0,"member_count":50,"member_fraction":1.0,"mean_features":[1.0]}]}'
        )
        rc_bnan, _, err_bnan = run_m3_runner(
            runner_bin, "pair", bare_nan_raw_json, verify_zero_output_on_error=True
        )
        if rc_bnan != 1 or "NON_FINITE_INPUT" not in err_bnan:
            raise AssertionError(f"Bare NaN sanitization check failed: rc={rc_bnan}, stderr={err_bnan!r}")

        bad_short_chain = {
            "tracking_run_key": "shortchain0000",
            "threshold_coefficient": 1.0,
            "weights": [1.0],
            "batches": [
                {
                    "ordinal": i,
                    "status": "ok",
                    "dispersion": 1.0,
                    "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
                }
                for i in range(3)
            ],
        }
        rc_sch, _, err_sch = run_m3_runner(
            runner_bin, "chain", bad_short_chain, verify_zero_output_on_error=True
        )
        if rc_sch != 1 or "INVALID_EVOLUTION_CONFIG" not in err_sch:
            raise AssertionError(f"Short 3-batch chain check failed: rc={rc_sch}, stderr={err_sch!r}")

        bad_non_whitelisted_chain = {
            "tracking_run_key": "badfailcode0000",
            "threshold_coefficient": 1.0,
            "weights": [1.0],
            "batches": [
                {
                    "ordinal": 0,
                    "status": "ok",
                    "dispersion": 1.0,
                    "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
                },
                {
                    "ordinal": 1,
                    "status": "failed",
                    "error_code": "NUMERICAL_FAILURE",
                    "status_reason": "NUMERICAL_FAILURE: non-whitelisted failure",
                },
                {
                    "ordinal": 2,
                    "status": "ok",
                    "dispersion": 1.0,
                    "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.1]}],
                },
                {
                    "ordinal": 3,
                    "status": "ok",
                    "dispersion": 1.0,
                    "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.2]}],
                },
            ],
        }
        rc_nwc, _, err_nwc = run_m3_runner(
            runner_bin, "chain", bad_non_whitelisted_chain, verify_zero_output_on_error=True
        )
        if rc_nwc != 1 or "INVALID_EVOLUTION_CONFIG" not in err_nwc:
            raise AssertionError(
                f"Non-whitelisted failed batch chain check failed: rc={rc_nwc}, stderr={err_nwc!r}"
            )
        try:
            solve_chain_oracle(bad_non_whitelisted_chain)
        except OracleValidationError as ora_nwc_err:
            if ora_nwc_err.code != "INVALID_EVOLUTION_CONFIG" or ora_nwc_err.prev_index != 1:
                raise AssertionError(
                    f"Oracle non-whitelisted chain check mismatch: code={ora_nwc_err.code!r}, prev_index={ora_nwc_err.prev_index!r}"
                )
        else:
            raise AssertionError("solve_chain_oracle unexpectedly accepted status=failed with NUMERICAL_FAILURE")

        proc_bad_flag = subprocess.run(
            [str(runner_bin), "--unknown-flag"], capture_output=True, text=True, check=False
        )
        if proc_bad_flag.returncode != 2:
            raise AssertionError(f"Expected exit code 2 for unknown CLI flag, got {proc_bad_flag.returncode}")

        required_17_codes = {
            "EMPTY_SCENARIOS",
            "TOO_MANY_SCENARIOS",
            "DUPLICATE_SCENARIO_ID",
            "EMPTY_FEATURES",
            "RAGGED_FEATURES",
            "DIMENSION_MISMATCH",
            "NON_FINITE_INPUT",
            "INVALID_WEIGHTS",
            "INVALID_THRESHOLD",
            "ZERO_DISPERSION",
            "NUMERICAL_FAILURE",
            "ZERO_MEMBER_SCENARIO",
            "INVALID_MEMBER_FRACTION",
            "INVALID_FRACTION_SUM",
            "INVALID_EVOLUTION_CONFIG",
            "INVALID_PRIMARY_LINK",
            "BACKBONE_INCONSISTENCY",
        }
        if not required_17_codes.issubset(distinct_error_codes):
            missing_codes = sorted(required_17_codes - distinct_error_codes)
            raise AssertionError(f"Missing required error code coverage: {missing_codes}")

        core_error_budget = verify_core_error_byte_budget(runner_bin)
        elapsed_s = round(time.perf_counter() - t_start, 4)

        # =====================================================================
        # Write Canonical Evidence Bundles under `<run-dir>/core/`
        # =====================================================================
        atomic_write_json(
            core_dir / "synthetic-oracle.json",
            {
                "schema_version": "m3.core-synthetic-oracle.v1",
                "status": "pass",
                "algorithm_version": EVOLUTION_ALGORITHM_VERSION,
                "secondary_rule": SECONDARY_RULE_V1,
                "flow_rule": FLOW_RULE_V1,
                "uncertainty_rule": UNCERTAINTY_RULE_V1,
                "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
                "runner_binary": str(runner_bin),
                "runner_sha256": sha256_file(runner_bin),
                "fixture_path": "validation/m3/fixtures/synthetic_evolution_cases.json",
                "fixture_sha256": sha256_file(fixture_path),
                "ulp_tolerance": 0,
                "fixed_pair_cases_count": len(fixed_pair_records),
                "positive_cases_count": len(fixed_pair_records),
                "fixed_pair_mismatches": 0,
                "scaled_vs_absolute_parity_verified": True,
                "valid_backbone_assertion_verified": True,
                "utf8_input_verified": True,
                "berge_theorems_verified": {
                    "theorem1_no_unmatched_cross_feasible_and_no_parallel_edges": True,
                    "theorem2_split_merge_mutually_exclusive_on_primary_link": True,
                    "theorem3_secondary_forced_inclusion_preserves_max_cardinality": True,
                    "theorem4_inactive_feasible_cross_edge_invariance": True,
                    "theorem4_constructive_reduced_tau_cases": theorem4_inactive_cases_verified,
                    "corollary_five_class_star_partition_complete": True,
                    "total_pair_cases_checked": len(fixed_pair_records) + len(random_cases) + 2,
                },
                "chain_cases_count": len(chain_records),
                "chain_mismatches": 0,
                "chain_gap_disjointness_verified": True,
                "chain_raw_members_mode_verified": True,
                "chain_results": chain_records,
                "random_oracle_stats": {
                    "seed": 20261125,
                    "cases_tested": len(random_cases),
                    "matched": len(random_cases),
                    "mismatches": 0,
                    "scaled_mode_cases": scaled_count,
                    "absolute_mode_cases": absolute_count,
                    "scenario_count_min": 1,
                    "scenario_count_max": 6,
                    "covered_1_to_5_per_side_count": covered_1_to_5_count,
                    "event_totals": event_totals,
                    "edge_role_totals": edge_role_totals,
                    "high_uncertainty_transitions": high_unc_transitions,
                    "ambiguous_tie_transitions": ambig_transitions,
                },
                "fixed_pair_results": fixed_pair_records,
                "elapsed_seconds": elapsed_s,
            },
        )

        atomic_write_json(
            core_dir / "orthogonality-separation.json",
            {
                "schema_version": "m3.core-orthogonality-separation.v1",
                "status": "pass",
                "algorithm_version": EVOLUTION_ALGORITHM_VERSION,
                "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
                "ulp_tolerance": 0,
                "pure_morphology_drift_series": {
                    "series_id": "pure_morphology_drift_series",
                    "steps_count": len(morph_step_records),
                    "primary_max_distances": morph_max_dists,
                    "primary_total_distances": morph_tot_dists,
                    "strictly_monotonic_distance": True,
                    "fraction_deltas_bitwise_zero": True,
                    "transition_tv_redistribution_values": morph_tv_vals,
                    "transition_tv_bitwise_zero": True,
                    "component_tv_residual_bitwise_zero": True,
                    "constant_attributed_flows_verified": True,
                    "oracle_zero_ulp_verified": True,
                    "steps": morph_step_records,
                },
                "pure_fraction_redistribution_series": {
                    "series_id": "pure_fraction_redistribution_series",
                    "steps_count": len(frac_step_records),
                    "morphology_distances_bitwise_zero": True,
                    "primary_total_distances": frac_tot_dists,
                    "all_edges_max_distances": frac_max_dists,
                    "expected_tv_values": expected_tv_sequence,
                    "observed_tv_values": observed_tv_sequence,
                    "tv_values_bitwise_exact": True,
                    "strictly_monotonic_tv": True,
                    "oracle_zero_ulp_verified": True,
                    "steps": frac_step_records,
                },
                "uncertainty_channel_bracketing": {
                    "uncertainty_rule": UNCERTAINTY_RULE_V1,
                    "default_ratio": 0.10,
                    "channels_verified": [
                        "near_threshold",
                        "near_primary_switch",
                        "near_secondary_switch",
                    ],
                    "cases_verified_count": len(bracketing_records),
                    "all_passed": True,
                    "cases": bracketing_records,
                },
            },
        )

        atomic_write_json(
            core_dir / "permutation-invariance.json",
            {
                "schema_version": "m3.core-permutation-invariance.v1",
                "status": "pass",
                "algorithm_version": EVOLUTION_ALGORITHM_VERSION,
                "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
                "permutation_stats": {
                    "seed": 20261126,
                    "permutations_tested": 2000,
                    "invariant_count": perm_invariant_count,
                    "symmetric_tie_count": perm_symmetric_tie_count,
                    "mismatches": 0,
                },
                "exhaustive_3x4_144_permutations": {
                    "case_id": "nontrivial_3x4_for_144_permutations",
                    "prev_permutations": 6,
                    "curr_permutations": 24,
                    "permutations_tested": 144,
                    "invariant_count": 144,
                    "mismatches": 0,
                },
                "symmetric_tie_canonical_verification": {
                    "tested": True,
                    "ambiguous_tie_asserted": True,
                    "oracle_canonical_tiebreak_matched": True,
                },
                "weight_scaling_stats": {
                    "fixed_cases_power_of_two_tested": len(pair_cases),
                    "random_cases_power_of_two_tested": 100,
                    "power_of_two_tested": len(scale_targets),
                    "power_of_two_bitwise_identical": len(scale_targets),
                    "fixed_cases_general_scaling_tested": len(pair_cases),
                    "random_cases_general_scaling_tested": 100,
                    "general_scaling_tested": len(scale_targets),
                    "general_scale_factor": 7.3,
                    "max_general_rtol": max_general_rtol,
                    "rtol_ceiling": 1e-15,
                    "flips_recorded": general_flips,
                },
            },
        )

        atomic_write_json(
            core_dir / "error-cases.json",
            {
                "schema_version": "m3.core-error-cases.v1",
                "status": "pass",
                "algorithm_version": EVOLUTION_ALGORITHM_VERSION,
                "fixture_negative_cases_count": len(negative_cases),
                "supplementary_negative_cases_count": len(extra_negative_pair_cases),
                "error_cases_count": len(error_case_records),
                "distinct_error_codes_count": len(distinct_error_codes),
                "distinct_error_codes_verified": sorted(distinct_error_codes),
                "single_item_exit_1_zero_output_residue_verified": True,
                "bare_nan_token_sanitization_verified": True,
                "short_chain_rejection_verified": True,
                "unknown_cli_flag_exit_2_verified": True,
                "batch_mode_error_reporting_verified": True,
                "oracle_error_parity_verified": True,
                "core_error_byte_budget": core_error_budget,
                "cases": error_case_records,
            },
        )

        print(
            f"PASS: M3 core vs Fraction oracle verification succeeded in {elapsed_s:.2f}s "
            f"(25 fixed pairs, 2 orthogonality series, 4 chains + raw-member chain, "
            f"4000 random pairs, 2000+144 permutations, {len(error_case_records)} negative cases, "
            f"CoreError <= 112B). Evidence written to {core_dir}"
        )
        return 0
    except Exception as exc:
        print(f"FAIL: M3 core verification failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
