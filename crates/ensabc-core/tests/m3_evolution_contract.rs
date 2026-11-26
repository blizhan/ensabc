//! M3 scenario evolution contract tests (`T008`, `T011`, `T019`).
//!
//! Covers:
//! - `CoreError <= 112` bytes budget & M1/M2 zero-breakage compatibility (`T008`)
//! - All M3 + M2 error codes and position reporting (`T008`, `T012`)
//! - Berge 4 structural theorems, canonical tie-breaking, `star_min_mass_proportional_v1`
//!   flow attribution, `BackboneAssertion` tamper rejection, and all 144 row/col permutations
//!   of nontrivial `3x4` (plus `4x4` split+merge+continuation) graphs (`T011`, `T013`)
//! - Pure morphology drift vs pure fraction redistribution orthogonality, `0.05R` vs `0.15R`
//!   bracketing on all 3 uncertainty channels, and 6-batch + 8-batch `evolve_chain` with
//!   `Missing` / `Failed` gaps (`T019`, `T020`, `T021`)

use std::collections::{HashMap, HashSet};

use ensabc_core::fsum::fsum;
use ensabc_core::{
    ALGORITHM_VERSION, BackboneAssertion, ChainBatchInput, ChainBatchStatus, ChainTransitionStatus,
    CoreError, CoreErrorCode, DEFAULT_CHAIN_RUN_KEY_PREFIX, EVOLUTION_ALGORITHM_VERSION,
    EdgeAlternativeStatus, EdgeRole, EvolutionChainInput, EvolutionConfig, EvolutionInput,
    EvolutionResult, FLOW_RULE_V1, FlowRule, MATCH_ALGORITHM_VERSION, MatchInput,
    NON_PHYSICAL_DISCLAIMER, NodeRole, SECONDARY_RULE_V1, ScaledEvolutionInput, ScenarioNodeInput,
    SecondaryRule, TransitionEventKind, UNCERTAINTY_RULE_V1, UncertaintyReason, UnmatchedReason,
    evolve_chain, evolve_scenarios, evolve_scenarios_scaled, match_scenarios,
};

// Compile-time byte budget guard for `clippy::result_large_err`
const _: () = assert!(std::mem::size_of::<CoreError>() <= 112);

fn make_sc(cid: usize, count: usize, frac: f64, feats: &[f64]) -> ScenarioNodeInput {
    ScenarioNodeInput {
        cluster_id: cid,
        mean_features: feats.to_vec(),
        member_count: count,
        member_fraction: frac,
    }
}

/// Assert Berge's 4 structural invariants and accounting identities on any valid `EvolutionResult`.
fn assert_structural_invariants(res: &EvolutionResult) {
    assert_eq!(res.algorithm_version, EVOLUTION_ALGORITHM_VERSION);
    assert_eq!(res.secondary_rule, SECONDARY_RULE_V1);
    assert_eq!(res.flow_rule, FLOW_RULE_V1);
    assert_eq!(res.uncertainty_rule, UNCERTAINTY_RULE_V1);
    assert_eq!(res.non_physical_disclaimer, NON_PHYSICAL_DISCLAIMER);

    let m = res.prev_cluster_ids.len();
    let n = res.curr_cluster_ids.len();
    let prev_id_to_idx: HashMap<usize, usize> = res
        .prev_cluster_ids
        .iter()
        .enumerate()
        .map(|(i, &cid)| (cid, i))
        .collect();
    let curr_id_to_idx: HashMap<usize, usize> = res
        .curr_cluster_ids
        .iter()
        .enumerate()
        .map(|(j, &cid)| (cid, j))
        .collect();

    let prim_prev: HashSet<usize> = res
        .edges
        .iter()
        .filter(|e| e.edge_role == EdgeRole::Primary)
        .map(|e| e.prev_cluster_id)
        .collect();
    let prim_curr: HashSet<usize> = res
        .edges
        .iter()
        .filter(|e| e.edge_role == EdgeRole::Primary)
        .map(|e| e.curr_cluster_id)
        .collect();

    // Theorem 1a: Any pair (i not in V_prev(M*), j not in V_curr(M*)) has d(i, j) > tau
    for (&p_cid, &p_idx) in &prev_id_to_idx {
        if !prim_prev.contains(&p_cid) {
            for (&c_cid, &c_idx) in &curr_id_to_idx {
                if !prim_curr.contains(&c_cid) {
                    assert!(
                        res.backbone.distance_matrix[p_idx][c_idx] > res.threshold,
                        "Berge Theorem 1 violated: unmatched pair ({p_cid}, {c_cid}) is feasible"
                    );
                }
            }
        }
    }

    // Theorem 1b: No parallel edges, sorted order, valid endpoint memberships, d <= tau
    let mut seen_pairs = HashSet::new();
    for (idx, e) in res.edges.iter().enumerate() {
        if idx > 0 {
            let prev = &res.edges[idx - 1];
            assert!(
                (prev.prev_cluster_id, prev.curr_cluster_id)
                    < (e.prev_cluster_id, e.curr_cluster_id),
                "Edges must be strictly sorted without parallel edges"
            );
        }
        assert!(seen_pairs.insert((e.prev_cluster_id, e.curr_cluster_id)));
        assert!(e.distance <= res.threshold);
        assert!(e.attributed_flow > 0.0);
        assert!(e.attributed_flow <= e.prev_member_fraction.min(e.curr_member_fraction) + 1e-15);
        assert!((0.0..=1.0).contains(&e.edge_strength));

        match e.edge_role {
            EdgeRole::Primary => {
                assert!(prim_prev.contains(&e.prev_cluster_id));
                assert!(prim_curr.contains(&e.curr_cluster_id));
                assert!(e.secondary_margin.is_none());
            }
            EdgeRole::SecondarySplit => {
                assert!(prim_prev.contains(&e.prev_cluster_id));
                assert!(!prim_curr.contains(&e.curr_cluster_id));
                assert_eq!(e.event_type, TransitionEventKind::Split);
                assert_eq!(
                    e.primary_alternative_status,
                    EdgeAlternativeStatus::PromotionGap
                );
                // Theorem 3: secondary promotion optimality margin is finite and >= 0
                let gap = e
                    .optimality_margin
                    .expect("Theorem 3 requires finite optimality_margin on secondary_split");
                assert!(gap.is_finite() && gap >= 0.0);
            }
            EdgeRole::SecondaryMerge => {
                assert!(!prim_prev.contains(&e.prev_cluster_id));
                assert!(prim_curr.contains(&e.curr_cluster_id));
                assert_eq!(e.event_type, TransitionEventKind::Merge);
                assert_eq!(
                    e.primary_alternative_status,
                    EdgeAlternativeStatus::PromotionGap
                );
                // Theorem 3: secondary promotion optimality margin is finite and >= 0
                let gap = e
                    .optimality_margin
                    .expect("Theorem 3 requires finite optimality_margin on secondary_merge");
                assert!(gap.is_finite() && gap >= 0.0);
            }
        }
    }

    // Theorem 2: On every primary edge (i, j), split on i and merge on j are mutually exclusive
    for e in &res.edges {
        if e.edge_role == EdgeRole::Primary {
            let has_split_on_i = res.edges.iter().any(|s| {
                s.edge_role == EdgeRole::SecondarySplit && s.prev_cluster_id == e.prev_cluster_id
            });
            let has_merge_on_j = res.edges.iter().any(|s| {
                s.edge_role == EdgeRole::SecondaryMerge && s.curr_cluster_id == e.curr_cluster_id
            });
            assert!(
                !(has_split_on_i && has_merge_on_j),
                "Berge Theorem 2 violated on primary edge ({}, {})",
                e.prev_cluster_id,
                e.curr_cluster_id
            );
        }
    }

    // Node degree conservation and split/merge mutual exclusion
    assert_eq!(res.prev_nodes.len(), m);
    for pn in &res.prev_nodes {
        assert!(!(pn.participates_in_split && pn.participates_in_merge));
        let p_deg = res
            .edges
            .iter()
            .filter(|e| e.prev_cluster_id == pn.cluster_id && e.edge_role == EdgeRole::Primary)
            .count();
        let s_deg = res
            .edges
            .iter()
            .filter(|e| e.prev_cluster_id == pn.cluster_id && e.edge_role != EdgeRole::Primary)
            .count();
        assert_eq!(pn.primary_degree, p_deg);
        assert_eq!(pn.secondary_degree, s_deg);
        let sum_recon = fsum([pn.attributed_flow_fraction, pn.unattributed_fraction]).unwrap();
        assert!((sum_recon - pn.member_fraction).abs() <= 1e-15);
    }

    assert_eq!(res.curr_nodes.len(), n);
    for cn in &res.curr_nodes {
        assert!(!(cn.participates_in_split && cn.participates_in_merge));
        let p_deg = res
            .edges
            .iter()
            .filter(|e| e.curr_cluster_id == cn.cluster_id && e.edge_role == EdgeRole::Primary)
            .count();
        let s_deg = res
            .edges
            .iter()
            .filter(|e| e.curr_cluster_id == cn.cluster_id && e.edge_role != EdgeRole::Primary)
            .count();
        assert_eq!(cn.primary_degree, p_deg);
        assert_eq!(cn.secondary_degree, s_deg);
        let sum_recon = fsum([cn.attributed_flow_fraction, cn.unattributed_fraction]).unwrap();
        assert!((sum_recon - cn.member_fraction).abs() <= 1e-15);
    }

    // Star 5-class component partition completeness (every prev and curr node appears in exactly 1 event)
    let mut covered_prev = Vec::new();
    let mut covered_curr = Vec::new();
    for (idx, ev) in res.events.iter().enumerate() {
        assert_eq!(ev.event_index, idx);
        match ev.event_type {
            TransitionEventKind::Continuation => {
                assert_eq!(ev.prev_cluster_ids.len(), 1);
                assert_eq!(ev.curr_cluster_ids.len(), 1);
                assert!(ev.primary_link.is_some());
            }
            TransitionEventKind::Split => {
                assert_eq!(ev.prev_cluster_ids.len(), 1);
                assert!(ev.curr_cluster_ids.len() >= 2);
                assert!(ev.primary_link.is_some());
            }
            TransitionEventKind::Merge => {
                assert!(ev.prev_cluster_ids.len() >= 2);
                assert_eq!(ev.curr_cluster_ids.len(), 1);
                assert!(ev.primary_link.is_some());
            }
            TransitionEventKind::Disappearance => {
                assert_eq!(ev.prev_cluster_ids.len(), 1);
                assert!(ev.curr_cluster_ids.is_empty());
                assert!(ev.primary_link.is_none());
            }
            TransitionEventKind::Emergence => {
                assert!(ev.prev_cluster_ids.is_empty());
                assert_eq!(ev.curr_cluster_ids.len(), 1);
                assert!(ev.primary_link.is_none());
            }
            other => panic!("Unexpected gap event in pair result: {other:?}"),
        }
        covered_prev.extend_from_slice(&ev.prev_cluster_ids);
        covered_curr.extend_from_slice(&ev.curr_cluster_ids);
    }
    covered_prev.sort_unstable();
    covered_curr.sort_unstable();
    let mut exp_prev = res.prev_cluster_ids.clone();
    exp_prev.sort_unstable();
    let mut exp_curr = res.curr_cluster_ids.clone();
    exp_curr.sort_unstable();
    assert_eq!(covered_prev, exp_prev);
    assert_eq!(covered_curr, exp_curr);
}

// ---------------------------------------------------------------------------
// T008: CoreError Byte Budget, M1/M2 Compatibility, and Error Code Contracts
// ---------------------------------------------------------------------------

#[test]
fn core_error_byte_budget_and_m1_m2_exports_intact() {
    assert!(
        std::mem::size_of::<CoreError>() <= 112,
        "CoreError size {} exceeded 112-byte budget",
        std::mem::size_of::<CoreError>()
    );
    assert_eq!(ALGORITHM_VERSION, "m1.native.v1");
    assert_eq!(MATCH_ALGORITHM_VERSION, "m2.match.v1");
    assert_eq!(EVOLUTION_ALGORITHM_VERSION, "m3.evolve.v1");

    // Verify M2 `match_scenarios` still ignores `member_count = 0` and `member_fraction = 0.0`
    let m2_res = match_scenarios(&MatchInput {
        prev_scenarios: vec![make_sc(0, 0, 0.0, &[1.0, 2.0])],
        curr_scenarios: vec![make_sc(0, 0, -5.0, &[1.0, 2.5])],
        weights: vec![1.0, 1.0],
        threshold: 2.0,
    })
    .expect("M2 match_scenarios must continue ignoring member_count and member_fraction");
    assert_eq!(m2_res.objective.matched_count, 1);
}

#[test]
fn all_m3_and_m2_error_codes_and_indices_verified() {
    assert_eq!(
        CoreErrorCode::ZeroMemberScenario.as_str(),
        "ZERO_MEMBER_SCENARIO"
    );
    assert_eq!(
        CoreErrorCode::InvalidMemberFraction.as_str(),
        "INVALID_MEMBER_FRACTION"
    );
    assert_eq!(
        CoreErrorCode::InvalidFractionSum.as_str(),
        "INVALID_FRACTION_SUM"
    );
    assert_eq!(
        CoreErrorCode::InvalidEvolutionConfig.as_str(),
        "INVALID_EVOLUTION_CONFIG"
    );
    assert_eq!(
        CoreErrorCode::InvalidPrimaryLink.as_str(),
        "INVALID_PRIMARY_LINK"
    );
    assert_eq!(
        CoreErrorCode::BackboneInconsistency.as_str(),
        "BACKBONE_INCONSISTENCY"
    );

    let valid_prev = vec![make_sc(0, 25, 0.5, &[0.0]), make_sc(1, 25, 0.5, &[2.0])];
    let valid_curr = vec![make_sc(0, 20, 0.4, &[0.1]), make_sc(1, 30, 0.6, &[2.1])];

    // 1. ZeroMemberScenario (prev & curr)
    let err = evolve_scenarios(&EvolutionInput {
        prev_scenarios: vec![make_sc(0, 25, 0.5, &[0.0]), make_sc(1, 0, 0.5, &[2.0])],
        curr_scenarios: valid_curr.clone(),
        weights: vec![1.0],
        threshold: 1.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap_err();
    assert_eq!(err.code, CoreErrorCode::ZeroMemberScenario);
    assert_eq!(err.prev_index, Some(1));

    let err = evolve_scenarios(&EvolutionInput {
        prev_scenarios: valid_prev.clone(),
        curr_scenarios: vec![make_sc(0, 0, 0.4, &[0.1]), make_sc(1, 30, 0.6, &[2.1])],
        weights: vec![1.0],
        threshold: 1.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap_err();
    assert_eq!(err.code, CoreErrorCode::ZeroMemberScenario);
    assert_eq!(err.curr_index, Some(0));

    // 2. InvalidMemberFraction (<= 0, > 1, NaN)
    for bad_f in [0.0, -0.2, 1.05, f64::NAN, f64::INFINITY] {
        let err = evolve_scenarios(&EvolutionInput {
            prev_scenarios: valid_prev.clone(),
            curr_scenarios: vec![make_sc(0, 20, 0.4, &[0.1]), make_sc(1, 30, bad_f, &[2.1])],
            weights: vec![1.0],
            threshold: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap_err();
        assert_eq!(err.code, CoreErrorCode::InvalidMemberFraction);
        assert_eq!(err.curr_index, Some(1));
    }

    // 3. InvalidFractionSum
    let err = evolve_scenarios(&EvolutionInput {
        prev_scenarios: vec![make_sc(0, 25, 0.4, &[0.0]), make_sc(1, 25, 0.4, &[2.0])],
        curr_scenarios: valid_curr.clone(),
        weights: vec![1.0],
        threshold: 1.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap_err();
    assert_eq!(err.code, CoreErrorCode::InvalidFractionSum);
    assert_eq!(err.prev_index, Some(0));

    // 4. InvalidEvolutionConfig
    for bad_cfg in [
        EvolutionConfig {
            fraction_sum_tolerance: -1e-9,
            ..Default::default()
        },
        EvolutionConfig {
            fraction_sum_tolerance: 2e-3,
            ..Default::default()
        },
        EvolutionConfig {
            fraction_sum_tolerance: f64::NAN,
            ..Default::default()
        },
        EvolutionConfig {
            high_uncertainty_threshold_ratio: -0.01,
            ..Default::default()
        },
        EvolutionConfig {
            high_uncertainty_optimality_ratio: f64::INFINITY,
            ..Default::default()
        },
        EvolutionConfig {
            high_uncertainty_secondary_ratio: f64::NAN,
            ..Default::default()
        },
    ] {
        let err = evolve_scenarios(&EvolutionInput {
            prev_scenarios: valid_prev.clone(),
            curr_scenarios: valid_curr.clone(),
            weights: vec![1.0],
            threshold: 1.0,
            config: bad_cfg,
            backbone: None,
        })
        .unwrap_err();
        assert_eq!(err.code, CoreErrorCode::InvalidEvolutionConfig);
    }

    // 5. Propagated M2 error codes
    assert_eq!(
        evolve_scenarios(&EvolutionInput {
            prev_scenarios: vec![],
            curr_scenarios: valid_curr.clone(),
            weights: vec![1.0],
            threshold: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap_err()
        .code,
        CoreErrorCode::EmptyScenarios
    );
    assert_eq!(
        evolve_scenarios(&EvolutionInput {
            prev_scenarios: (0..13)
                .map(|i| make_sc(i, 1, 1.0 / 13.0, &[i as f64]))
                .collect(),
            curr_scenarios: valid_curr.clone(),
            weights: vec![1.0],
            threshold: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap_err()
        .code,
        CoreErrorCode::TooManyScenarios
    );
    assert_eq!(
        evolve_scenarios(&EvolutionInput {
            prev_scenarios: vec![make_sc(7, 25, 0.5, &[0.0]), make_sc(7, 25, 0.5, &[1.0])],
            curr_scenarios: valid_curr.clone(),
            weights: vec![1.0],
            threshold: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap_err()
        .code,
        CoreErrorCode::DuplicateScenarioId
    );
    assert_eq!(
        evolve_scenarios(&EvolutionInput {
            prev_scenarios: vec![make_sc(0, 50, 1.0, &[0.0, 1.0])],
            curr_scenarios: valid_curr.clone(),
            weights: vec![1.0],
            threshold: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap_err()
        .code,
        CoreErrorCode::DimensionMismatch
    );
    assert_eq!(
        evolve_scenarios(&EvolutionInput {
            prev_scenarios: valid_prev.clone(),
            curr_scenarios: valid_curr.clone(),
            weights: vec![-1.0],
            threshold: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap_err()
        .code,
        CoreErrorCode::InvalidWeights
    );
    assert_eq!(
        evolve_scenarios(&EvolutionInput {
            prev_scenarios: valid_prev.clone(),
            curr_scenarios: valid_curr.clone(),
            weights: vec![1.0],
            threshold: -0.1,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap_err()
        .code,
        CoreErrorCode::InvalidThreshold
    );
    assert_eq!(
        evolve_scenarios_scaled(&ScaledEvolutionInput {
            prev_scenarios: valid_prev,
            curr_scenarios: valid_curr,
            weights: vec![1.0],
            threshold_coefficient: 1.0,
            prev_dispersion: 0.0,
            curr_dispersion: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap_err()
        .code,
        CoreErrorCode::ZeroDispersion
    );
}

// ---------------------------------------------------------------------------
// T011: Berge Structural Theorems, Canonical Tie-Break, Flow Attribution,
//       BackboneAssertion Tamper Rejection, and 3x4 All-144 Permutations
// ---------------------------------------------------------------------------

#[test]
fn berge_theorems_and_five_event_topologies() {
    // 1. Pure Split (1 -> 3) with proportional attribution (p_0 = 0.6 < q_sum = 1.0, wait single prev must sum to 1.0!)
    // Use 2 prev (P0=0.40 splits into C0=0.30, C1=0.20, C2=0.10 -> sum_q=0.60 > p_0=0.40; P1=0.60 disappears; C3=0.40 emerges)
    let split_res = evolve_scenarios_scaled(&ScaledEvolutionInput {
        prev_scenarios: vec![make_sc(0, 20, 0.40, &[0.0]), make_sc(1, 30, 0.60, &[20.0])],
        curr_scenarios: vec![
            make_sc(0, 15, 0.30, &[0.2]),
            make_sc(1, 10, 0.20, &[0.5]),
            make_sc(2, 5, 0.10, &[0.8]),
            make_sc(3, 20, 0.40, &[-20.0]),
        ],
        weights: vec![1.0],
        threshold_coefficient: 1.0,
        prev_dispersion: 1.0,
        curr_dispersion: 1.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap();
    assert_structural_invariants(&split_res);
    assert_eq!(split_res.summary.split_count, 1);
    assert_eq!(split_res.summary.disappearance_count, 1);
    assert_eq!(split_res.summary.emergence_count, 1);
    assert_eq!(split_res.summary.primary_edge_count, 1);
    assert_eq!(split_res.summary.secondary_split_edge_count, 2);

    // Check proportional split flow: p_0 = 0.40 < Q = 0.60 => f(0, j) = (0.40 * q_j) / 0.60
    for e in &split_res.edges {
        let expected_f = (0.40_f64 * e.curr_member_fraction) / fsum([0.30, 0.20, 0.10]).unwrap();
        assert!((e.attributed_flow - expected_f).abs() < 1e-15);
    }

    // 2. Pure Merge (3 -> 1) + Continuation (1 -> 1): 4 -> 2
    let merge_res = evolve_scenarios_scaled(&ScaledEvolutionInput {
        prev_scenarios: vec![
            make_sc(0, 15, 0.30, &[0.1]),
            make_sc(1, 10, 0.20, &[0.4]),
            make_sc(2, 10, 0.20, &[0.7]),
            make_sc(3, 15, 0.30, &[10.0]),
        ],
        curr_scenarios: vec![make_sc(0, 25, 0.50, &[0.0]), make_sc(1, 25, 0.50, &[10.1])],
        weights: vec![1.0],
        threshold_coefficient: 1.0,
        prev_dispersion: 1.0,
        curr_dispersion: 1.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap();
    assert_structural_invariants(&merge_res);
    assert_eq!(merge_res.summary.merge_count, 1);
    assert_eq!(merge_res.summary.continuation_count, 1);
    assert_eq!(merge_res.summary.secondary_merge_edge_count, 2);
    // Proportional merge flow: q_0 = 0.50 < P = 0.70 => f(i, 0) = (0.50 * p_i) / 0.70
    for e in merge_res.edges.iter().filter(|e| e.curr_cluster_id == 0) {
        let expected_f = (0.50_f64 * e.prev_member_fraction) / fsum([0.30, 0.20, 0.20]).unwrap();
        assert!((e.attributed_flow - expected_f).abs() < 1e-15);
    }

    // 3. All exceed threshold -> 0 edges, all disappearance + emergence, no NaN
    let zero_res = evolve_scenarios(&EvolutionInput {
        prev_scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
        curr_scenarios: vec![make_sc(0, 50, 1.0, &[5.0])],
        weights: vec![1.0],
        threshold: 1.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap();
    assert_structural_invariants(&zero_res);
    assert!(zero_res.edges.is_empty());
    assert_eq!(zero_res.summary.disappearance_count, 1);
    assert_eq!(zero_res.summary.emergence_count, 1);
    assert_eq!(zero_res.summary.transition_tv_redistribution, 1.0);
    assert_eq!(zero_res.prev_nodes[0].node_role, NodeRole::Disappeared);
    assert_eq!(
        zero_res.prev_nodes[0].unmatched_reason,
        Some(UnmatchedReason::NoCandidateWithinThreshold)
    );
    assert_eq!(zero_res.prev_nodes[0].min_candidate_distance, Some(5.0));
    assert_eq!(zero_res.prev_nodes[0].threshold_margin, Some(4.0));

    // 4. Exact boundary d == tau and tau == 0.0 (including -0.0 feature normalization)
    let tau_zero = evolve_scenarios(&EvolutionInput {
        prev_scenarios: vec![make_sc(0, 25, 0.5, &[-0.0]), make_sc(1, 25, 0.5, &[2.0])],
        curr_scenarios: vec![make_sc(0, 25, 0.5, &[0.0]), make_sc(1, 25, 0.5, &[2.5])],
        weights: vec![1.0],
        threshold: 0.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap();
    assert_structural_invariants(&tau_zero);
    assert!(tau_zero.reference_scale.is_none());
    assert_eq!(tau_zero.edges.len(), 1);
    assert_eq!(tau_zero.edges[0].edge_strength, 1.0);
    assert!(tau_zero.edges[0].high_uncertainty);
    assert!(
        tau_zero.edges[0]
            .uncertainty_reasons
            .contains(&UncertaintyReason::NearThreshold)
    );
}

#[test]
fn canonical_tiebreak_and_inactive_cross_edge_invariance() {
    // Secondary split exact distance tie: C2 at 1.0 is equidistant to P0 (0.0) and P1 (2.0) with d=1.0
    // P0 matches C0 (d=0.1), P1 matches C1 (d=0.1). C2 is unmatched_curr in U_curr^comp with d(P0,C2)=1.0 == d(P1,C2)=1.0!
    // Canonical order of prev puts P0 (feature 0.0) at rank 0 and P1 (feature 2.0) at rank 1,
    // even if P0 has larger cluster_id (9) than P1 (2)!
    let res = evolve_scenarios(&EvolutionInput {
        prev_scenarios: vec![make_sc(2, 25, 0.5, &[2.0]), make_sc(9, 25, 0.5, &[0.0])],
        curr_scenarios: vec![
            make_sc(0, 20, 0.4, &[0.1]),
            make_sc(1, 20, 0.4, &[2.1]),
            make_sc(2, 10, 0.2, &[1.0]),
        ],
        weights: vec![1.0],
        threshold: 1.5,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap();
    assert_structural_invariants(&res);
    let sec_edge = res
        .edges
        .iter()
        .find(|e| e.edge_role == EdgeRole::SecondarySplit)
        .expect("secondary_split edge exists");
    assert_eq!(sec_edge.prev_cluster_id, 9); // canonical rank 0 (feature 0.0) wins!
    assert_eq!(sec_edge.curr_cluster_id, 2);
    assert_eq!(
        sec_edge.rule_basis,
        "nearest_feasible_split_canonical_tiebreak"
    );
    assert!(sec_edge.ambiguous_tie);
    assert_eq!(sec_edge.secondary_margin, Some(0.0));
    assert!(
        sec_edge
            .uncertainty_reasons
            .contains(&UncertaintyReason::AmbiguousTie)
    );
    assert!(
        sec_edge
            .uncertainty_reasons
            .contains(&UncertaintyReason::NearSecondarySwitch)
    );

    // Theorem 4: Active threshold margin ignores inactive feasible cross-cell (i, j) in E_tau \ E_evol
    // P0=[0.0], P1=[0.99], C0=[0.0], C1=[0.99], tau = 1.0.
    // Primary links: (0,0) d=0.0, (1,1) d=0.0. Cross cells (0,1) and (1,0) have d=0.99 <= 1.0 (inactive feasible!).
    // raw_matrix_threshold_margin = 0.01, whereas active min_threshold_margin = 1.0!
    let thm4 = evolve_scenarios_scaled(&ScaledEvolutionInput {
        prev_scenarios: vec![make_sc(0, 25, 0.5, &[0.0]), make_sc(1, 25, 0.5, &[0.99])],
        curr_scenarios: vec![make_sc(0, 25, 0.5, &[0.0]), make_sc(1, 25, 0.5, &[0.99])],
        weights: vec![1.0],
        threshold_coefficient: 1.0,
        prev_dispersion: 1.0,
        curr_dispersion: 1.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap();
    assert!((thm4.summary.raw_matrix_threshold_margin - 0.01).abs() < 1e-12);
    assert!((thm4.summary.min_threshold_margin - 1.0).abs() < 1e-12);
    assert!(!thm4.summary.high_uncertainty);
}

#[test]
fn backbone_assertion_strict_verification_and_tamper_rejection() {
    let prev = vec![make_sc(0, 25, 0.5, &[0.0]), make_sc(1, 25, 0.5, &[2.0])];
    let curr = vec![make_sc(0, 25, 0.5, &[0.2]), make_sc(1, 25, 0.5, &[2.2])];
    let base = evolve_scenarios(&EvolutionInput {
        prev_scenarios: prev.clone(),
        curr_scenarios: curr.clone(),
        weights: vec![1.0],
        threshold: 2.5,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap();

    let valid_assertion = BackboneAssertion {
        prev_cluster_ids: vec![0, 1],
        curr_cluster_ids: vec![0, 1],
        threshold: 2.5,
        distance_matrix: Some(base.backbone.distance_matrix.clone()),
        eligible_matrix: Some(vec![vec![true, true], vec![true, true]]),
        primary_links: vec![(0, 0), (1, 1)],
    };

    // Valid assertion succeeds identically
    let verified = evolve_scenarios(&EvolutionInput {
        prev_scenarios: prev.clone(),
        curr_scenarios: curr.clone(),
        weights: vec![1.0],
        threshold: 2.5,
        config: EvolutionConfig::default(),
        backbone: Some(valid_assertion.clone()),
    })
    .unwrap();
    assert_eq!(verified, base);

    // Tamper 1: unknown cluster_id -> InvalidPrimaryLink
    let mut t1 = valid_assertion.clone();
    t1.primary_links = vec![(0, 0), (99, 1)];
    assert_eq!(
        evolve_scenarios(&EvolutionInput {
            prev_scenarios: prev.clone(),
            curr_scenarios: curr.clone(),
            weights: vec![1.0],
            threshold: 2.5,
            config: EvolutionConfig::default(),
            backbone: Some(t1),
        })
        .unwrap_err()
        .code,
        CoreErrorCode::InvalidPrimaryLink
    );

    // Tamper 2: duplicate prev_cluster_id -> InvalidPrimaryLink
    let mut t2 = valid_assertion.clone();
    t2.primary_links = vec![(0, 0), (0, 1)];
    assert_eq!(
        evolve_scenarios(&EvolutionInput {
            prev_scenarios: prev.clone(),
            curr_scenarios: curr.clone(),
            weights: vec![1.0],
            threshold: 2.5,
            config: EvolutionConfig::default(),
            backbone: Some(t2),
        })
        .unwrap_err()
        .code,
        CoreErrorCode::InvalidPrimaryLink
    );

    // Tamper 3: link exceeds threshold -> InvalidPrimaryLink
    let err_gt = evolve_scenarios(&EvolutionInput {
        prev_scenarios: prev.clone(),
        curr_scenarios: curr.clone(),
        weights: vec![1.0],
        threshold: 1.0,
        config: EvolutionConfig::default(),
        backbone: Some(BackboneAssertion {
            prev_cluster_ids: vec![0, 1],
            curr_cluster_ids: vec![0, 1],
            threshold: 1.0,
            distance_matrix: None,
            eligible_matrix: None,
            primary_links: vec![(0, 1)], // d(0, 1) = 2.2 > 1.0
        }),
    })
    .unwrap_err();
    assert_eq!(err_gt.code, CoreErrorCode::InvalidPrimaryLink);
    assert_eq!(err_gt.prev_index, Some(0));
    assert_eq!(err_gt.curr_index, Some(1));

    // Tamper 4: feasible 1-to-1 links (0->1, 1->0) but suboptimal sum (2.2 + 1.8 = 4.0 > 0.4) -> BackboneInconsistency
    let mut t4 = valid_assertion.clone();
    t4.primary_links = vec![(0, 1), (1, 0)];
    assert_eq!(
        evolve_scenarios(&EvolutionInput {
            prev_scenarios: prev.clone(),
            curr_scenarios: curr.clone(),
            weights: vec![1.0],
            threshold: 2.5,
            config: EvolutionConfig::default(),
            backbone: Some(t4),
        })
        .unwrap_err()
        .code,
        CoreErrorCode::BackboneInconsistency
    );

    // Tamper 5: tampered distance_matrix[1][0] -> BackboneInconsistency with (1, 0)
    let mut t5 = valid_assertion.clone();
    t5.distance_matrix.as_mut().unwrap()[1][0] += 0.01;
    let err_dm = evolve_scenarios(&EvolutionInput {
        prev_scenarios: prev.clone(),
        curr_scenarios: curr.clone(),
        weights: vec![1.0],
        threshold: 2.5,
        config: EvolutionConfig::default(),
        backbone: Some(t5),
    })
    .unwrap_err();
    assert_eq!(err_dm.code, CoreErrorCode::BackboneInconsistency);
    assert_eq!(err_dm.prev_index, Some(1));
    assert_eq!(err_dm.curr_index, Some(0));

    // Tamper 6: tampered eligible_matrix[0][1] -> BackboneInconsistency with (0, 1)
    let mut t6 = valid_assertion;
    t6.eligible_matrix.as_mut().unwrap()[0][1] = false;
    let err_em = evolve_scenarios(&EvolutionInput {
        prev_scenarios: prev,
        curr_scenarios: curr,
        weights: vec![1.0],
        threshold: 2.5,
        config: EvolutionConfig::default(),
        backbone: Some(t6),
    })
    .unwrap_err();
    assert_eq!(err_em.code, CoreErrorCode::BackboneInconsistency);
    assert_eq!(err_em.prev_index, Some(0));
    assert_eq!(err_em.curr_index, Some(1));
}

fn permutations(k: usize) -> Vec<Vec<usize>> {
    fn backtrack(k: usize, cur: &mut Vec<usize>, used: &mut [bool], out: &mut Vec<Vec<usize>>) {
        if cur.len() == k {
            out.push(cur.clone());
            return;
        }
        for i in 0..k {
            if !used[i] {
                used[i] = true;
                cur.push(i);
                backtrack(k, cur, used, out);
                cur.pop();
                used[i] = false;
            }
        }
    }
    let mut out = Vec::new();
    let mut cur = Vec::new();
    let mut used = vec![false; k];
    backtrack(k, &mut cur, &mut used, &mut out);
    out
}

#[test]
fn all_144_permutations_of_3x4_composite_graph_and_4x4_split_merge_continuation() {
    // Nontrivial 3x4 graph with primary continuations + merge (2->1) + split (1->2) + emergence (0->1)
    // AND an inactive feasible cross-edge!
    // Base prev (3):
    //   P0: [0.0, 0.0], frac 0.35
    //   P1: [0.8, 0.0], frac 0.25  -> merges into C0 (d=0.7 <= 1.2)
    //   P2: [3.0, 0.0], frac 0.40  -> splits to C1 (primary, d=0.2) and C2 (secondary, d=0.6)
    // Base curr (4):
    //   C0: [0.1, 0.0], frac 0.45
    //   C1: [3.2, 0.0], frac 0.25
    //   C2: [3.6, 0.0], frac 0.15
    //   C3: [9.0, 0.0], frac 0.15  -> emerges (d > 1.2)
    let base_prev = [
        make_sc(0, 17, 0.35, &[0.0, 0.0]),
        make_sc(1, 13, 0.25, &[0.8, 0.0]),
        make_sc(2, 20, 0.40, &[3.0, 0.0]),
    ];
    let base_curr = [
        make_sc(0, 22, 0.45, &[0.1, 0.0]),
        make_sc(1, 13, 0.25, &[3.2, 0.0]),
        make_sc(2, 8, 0.15, &[3.6, 0.0]),
        make_sc(3, 7, 0.15, &[9.0, 0.0]),
    ];
    let weights = vec![0.7, 0.3];

    let ref_res = evolve_scenarios_scaled(&ScaledEvolutionInput {
        prev_scenarios: base_prev.to_vec(),
        curr_scenarios: base_curr.to_vec(),
        weights: weights.clone(),
        threshold_coefficient: 1.2,
        prev_dispersion: 1.0,
        curr_dispersion: 1.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap();
    assert_structural_invariants(&ref_res);
    assert_eq!(ref_res.summary.merge_count, 1);
    assert_eq!(ref_res.summary.split_count, 1);
    assert_eq!(ref_res.summary.emergence_count, 1);

    let perms3 = permutations(3);
    let perms4 = permutations(4);
    assert_eq!(perms3.len() * perms4.len(), 144);

    #[derive(Debug, PartialEq, Eq)]
    struct EdgeFingerprint {
        base_prev: usize,
        base_curr: usize,
        edge_role: EdgeRole,
        event_type: TransitionEventKind,
        rule_basis: String,
        distance_bits: u64,
        attributed_flow_bits: u64,
        edge_strength_bits: u64,
        threshold_margin_bits: u64,
        optimality_margin_bits: Option<u64>,
        secondary_margin_bits: Option<u64>,
        ambiguous_tie: bool,
        high_uncertainty: bool,
        uncertainty_reasons: Vec<UncertaintyReason>,
    }

    // Map base_id -> canonical key for semantic comparison across arbitrary cluster_id relabelings
    let ref_edges_by_base: Vec<EdgeFingerprint> = ref_res
        .edges
        .iter()
        .map(|e| EdgeFingerprint {
            base_prev: e.prev_cluster_id,
            base_curr: e.curr_cluster_id,
            edge_role: e.edge_role,
            event_type: e.event_type,
            rule_basis: e.rule_basis.clone(),
            distance_bits: e.distance.to_bits(),
            attributed_flow_bits: e.attributed_flow.to_bits(),
            edge_strength_bits: e.edge_strength.to_bits(),
            threshold_margin_bits: e.threshold_margin.to_bits(),
            optimality_margin_bits: e.optimality_margin.map(f64::to_bits),
            secondary_margin_bits: e.secondary_margin.map(f64::to_bits),
            ambiguous_tie: e.ambiguous_tie,
            high_uncertainty: e.high_uncertainty,
            uncertainty_reasons: e.uncertainty_reasons.clone(),
        })
        .collect();

    for p_perm in &perms3 {
        for c_perm in &perms4 {
            // Reorder array AND assign permuted cluster_ids (e.g. 10 + slot)
            let mut new_to_base_p = HashMap::new();
            let prev_permuted: Vec<ScenarioNodeInput> = p_perm
                .iter()
                .enumerate()
                .map(|(slot, &base_i)| {
                    let new_cid = 10 + slot * 3;
                    new_to_base_p.insert(new_cid, base_i);
                    let mut sc = base_prev[base_i].clone();
                    sc.cluster_id = new_cid;
                    sc
                })
                .collect();

            let mut new_to_base_c = HashMap::new();
            let curr_permuted: Vec<ScenarioNodeInput> = c_perm
                .iter()
                .enumerate()
                .map(|(slot, &base_j)| {
                    let new_cid = 20 + slot * 5;
                    new_to_base_c.insert(new_cid, base_j);
                    let mut sc = base_curr[base_j].clone();
                    sc.cluster_id = new_cid;
                    sc
                })
                .collect();

            let perm_res = evolve_scenarios_scaled(&ScaledEvolutionInput {
                prev_scenarios: prev_permuted,
                curr_scenarios: curr_permuted,
                weights: weights.clone(),
                threshold_coefficient: 1.2,
                prev_dispersion: 1.0,
                curr_dispersion: 1.0,
                config: EvolutionConfig::default(),
                backbone: None,
            })
            .unwrap();
            assert_structural_invariants(&perm_res);
            assert_eq!(perm_res.summary, ref_res.summary);

            let mut mapped_edges: Vec<EdgeFingerprint> = perm_res
                .edges
                .iter()
                .map(|e| EdgeFingerprint {
                    base_prev: new_to_base_p[&e.prev_cluster_id],
                    base_curr: new_to_base_c[&e.curr_cluster_id],
                    edge_role: e.edge_role,
                    event_type: e.event_type,
                    rule_basis: e.rule_basis.clone(),
                    distance_bits: e.distance.to_bits(),
                    attributed_flow_bits: e.attributed_flow.to_bits(),
                    edge_strength_bits: e.edge_strength.to_bits(),
                    threshold_margin_bits: e.threshold_margin.to_bits(),
                    optimality_margin_bits: e.optimality_margin.map(f64::to_bits),
                    secondary_margin_bits: e.secondary_margin.map(f64::to_bits),
                    ambiguous_tie: e.ambiguous_tie,
                    high_uncertainty: e.high_uncertainty,
                    uncertainty_reasons: e.uncertainty_reasons.clone(),
                })
                .collect();
            mapped_edges.sort_by_key(|fp| (fp.base_prev, fp.base_curr));
            assert_eq!(mapped_edges, ref_edges_by_base);

            // Event ordering is canonical_rank-based, so mapped event sequence must match identically!
            assert_eq!(perm_res.events.len(), ref_res.events.len());
            for (pe, re) in perm_res.events.iter().zip(&ref_res.events) {
                assert_eq!(pe.event_type, re.event_type);
                assert_eq!(
                    pe.max_morphology_distance.to_bits(),
                    re.max_morphology_distance.to_bits()
                );
                assert_eq!(
                    pe.net_delta_fraction.to_bits(),
                    re.net_delta_fraction.to_bits()
                );
                assert_eq!(
                    pe.attributed_flow_sum.to_bits(),
                    re.attributed_flow_sum.to_bits()
                );
            }
        }
    }

    // Also verify a 4x4 graph simultaneously containing Continuation + Split + Merge!
    let res_4x4 = evolve_scenarios_scaled(&ScaledEvolutionInput {
        prev_scenarios: vec![
            make_sc(0, 12, 0.24, &[0.0]),
            make_sc(1, 13, 0.26, &[0.5]),
            make_sc(2, 15, 0.30, &[5.0]),
            make_sc(3, 10, 0.20, &[10.0]),
        ],
        curr_scenarios: vec![
            make_sc(0, 20, 0.40, &[0.1]),
            make_sc(1, 10, 0.20, &[5.1]),
            make_sc(2, 10, 0.20, &[5.5]),
            make_sc(3, 10, 0.20, &[10.2]),
        ],
        weights: vec![1.0],
        threshold_coefficient: 1.0,
        prev_dispersion: 1.0,
        curr_dispersion: 1.0,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap();
    assert_structural_invariants(&res_4x4);
    assert_eq!(res_4x4.summary.merge_count, 1);
    assert_eq!(res_4x4.summary.split_count, 1);
    assert_eq!(res_4x4.summary.continuation_count, 1);
}

// ---------------------------------------------------------------------------
// T019: Orthogonality Separation, 0.05R vs 0.15R Uncertainty Bracketing,
//       and 6-Batch / 8-Batch `evolve_chain` Contract Tests
// ---------------------------------------------------------------------------

#[test]
fn pure_morphology_drift_vs_pure_fraction_redistribution_orthogonality() {
    // 1. Pure Morphology Drift (4 shifts: 0.1, 0.25, 0.45, 0.70 with identical fractions [0.6, 0.4])
    let shifts = [0.10, 0.25, 0.45, 0.70];
    let mut prev_dist = 0.0;
    for shift in shifts {
        let res = evolve_scenarios_scaled(&ScaledEvolutionInput {
            prev_scenarios: vec![
                make_sc(0, 30, 0.60, &[0.0, 1.0]),
                make_sc(1, 20, 0.40, &[5.0, 6.0]),
            ],
            curr_scenarios: vec![
                make_sc(0, 30, 0.60, &[shift, 1.0 + shift]),
                make_sc(1, 20, 0.40, &[5.0 + shift, 6.0 + shift]),
            ],
            weights: vec![0.5, 0.5],
            threshold_coefficient: 1.5,
            prev_dispersion: 1.0,
            curr_dispersion: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap();
        assert_eq!(res.summary.transition_tv_redistribution, 0.0);
        assert_eq!(res.summary.component_tv_residual, 0.0);
        for e in &res.edges {
            assert_eq!(e.edge_delta_fraction, 0.0);
        }
        for n in res.prev_nodes.iter().chain(&res.curr_nodes) {
            assert_eq!(n.primary_delta_fraction, Some(0.0));
            assert_eq!(n.unattributed_fraction, 0.0);
        }
        assert!(
            res.summary.primary_max_distance > prev_dist,
            "Morphology distance must strictly increase with spatial shift"
        );
        prev_dist = res.summary.primary_max_distance;
    }

    // 2. Pure Fraction Redistribution (3 exact dyadic transfers: 0.125, 0.25, 0.375 with 0 morphology shift)
    for delta in [0.125_f64, 0.25_f64, 0.375_f64] {
        let res = evolve_scenarios_scaled(&ScaledEvolutionInput {
            prev_scenarios: vec![
                make_sc(0, 25, 0.5, &[0.0, 1.0]),
                make_sc(1, 25, 0.5, &[5.0, 6.0]),
            ],
            curr_scenarios: vec![
                make_sc(0, 25, 0.5 + delta, &[0.0, 1.0]),
                make_sc(1, 25, 0.5 - delta, &[5.0, 6.0]),
            ],
            weights: vec![0.5, 0.5],
            threshold_coefficient: 1.0,
            prev_dispersion: 1.0,
            curr_dispersion: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap();
        assert_eq!(res.summary.primary_total_distance, 0.0);
        assert_eq!(res.summary.primary_max_distance, 0.0);
        assert_eq!(res.summary.all_edges_max_distance, 0.0);
        for e in &res.edges {
            assert_eq!(e.distance, 0.0);
            assert_eq!(e.distance_over_scale, Some(0.0));
        }
        assert_eq!(
            res.summary.transition_tv_redistribution.to_bits(),
            delta.to_bits()
        );
    }
}

#[test]
fn uncertainty_bracketing_005r_vs_015r_across_all_three_channels() {
    // Reference scale R = S = 1.0, c = 1.0 => tau = 1.0, default ratio = 0.10.
    // Channel 1: threshold_margin = 0.05R (d = 0.95) vs 0.15R (d = 0.85)
    for (d, expect_hu) in [(0.95, true), (0.85, false)] {
        let res = evolve_scenarios_scaled(&ScaledEvolutionInput {
            prev_scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
            curr_scenarios: vec![make_sc(0, 50, 1.0, &[d])],
            weights: vec![1.0],
            threshold_coefficient: 1.0,
            prev_dispersion: 1.0,
            curr_dispersion: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap();
        assert_eq!(res.edges[0].high_uncertainty, expect_hu);
        assert_eq!(
            res.edges[0]
                .uncertainty_reasons
                .contains(&UncertaintyReason::NearThreshold),
            expect_hu
        );
    }

    // Channel 2: optimality_margin = 0.05R vs 0.15R (1 prev at 0.0 -> 2 curr at 0.20 and 0.20 + gap)
    for (gap, expect_hu) in [(0.05, true), (0.15, false)] {
        let res = evolve_scenarios_scaled(&ScaledEvolutionInput {
            prev_scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
            curr_scenarios: vec![
                make_sc(0, 30, 0.6, &[0.20]),
                make_sc(1, 20, 0.4, &[0.20 + gap]),
            ],
            weights: vec![1.0],
            threshold_coefficient: 1.0,
            prev_dispersion: 1.0,
            curr_dispersion: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap();
        for e in &res.edges {
            assert_eq!(e.high_uncertainty, expect_hu);
            assert_eq!(
                e.uncertainty_reasons
                    .contains(&UncertaintyReason::NearPrimarySwitch),
                expect_hu
            );
        }
    }

    // Channel 3: secondary_margin = 0.05R vs 0.15R
    // P0=[0.0], P1=[0.50], C0=[0.0], C1=[0.50]; unmatched prev P2 at x where d(P2,C0) < d(P2,C1) <= 1.0
    // Pick P0=[0.0], P1=[0.60], C0=[0.0], C1=[0.60]; for sec_gap = 0.05: P2 at (0.60 - 0.05)/2 = 0.275
    // => d(P2, C0) = 0.275, d(P2, C1) = 0.325 => secondary_margin = 0.05!
    // And promotion gap of P2 = 0.275 > 0.10, threshold_margin = 0.725 > 0.10 => ONLY channel 3 fires!
    for (p2_pos, expect_hu) in [(0.275, true), (0.225, false)] {
        // For 0.225: d(P2,C0)=0.225, d(P2,C1)=0.375 => secondary_margin = 0.15!
        let res = evolve_scenarios_scaled(&ScaledEvolutionInput {
            prev_scenarios: vec![
                make_sc(0, 20, 0.4, &[0.0]),
                make_sc(1, 20, 0.4, &[0.60]),
                make_sc(2, 10, 0.2, &[p2_pos]),
            ],
            curr_scenarios: vec![make_sc(0, 25, 0.5, &[0.0]), make_sc(1, 25, 0.5, &[0.60])],
            weights: vec![1.0],
            threshold_coefficient: 1.0,
            prev_dispersion: 1.0,
            curr_dispersion: 1.0,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap();
        let sec = res
            .edges
            .iter()
            .find(|e| e.edge_role == EdgeRole::SecondaryMerge)
            .expect("secondary_merge edge exists");
        assert_eq!(sec.high_uncertainty, expect_hu);
        assert_eq!(
            sec.uncertainty_reasons
                .contains(&UncertaintyReason::NearSecondarySwitch),
            expect_hu
        );
        assert!(
            !sec.uncertainty_reasons
                .contains(&UncertaintyReason::NearThreshold)
        );
        assert!(
            !sec.uncertainty_reasons
                .contains(&UncertaintyReason::NearPrimarySwitch)
        );
    }
}

#[test]
fn evolve_chain_6batch_all_events_and_8batch_missing_failed_gaps() {
    // Batch count out of 4..=8 rejected with InvalidEvolutionConfig
    let err_short = evolve_chain(&EvolutionChainInput {
        run_key_prefix: None,
        batches: (0..3)
            .map(|ord| ChainBatchInput {
                ordinal: ord,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
                    dispersion: 1.0,
                },
            })
            .collect(),
        weights: vec![1.0],
        threshold_coefficient: 1.0,
        config: EvolutionConfig::default(),
    })
    .unwrap_err();
    assert_eq!(err_short.code, CoreErrorCode::InvalidEvolutionConfig);

    // 1. 6-batch all-Ok chain exercising continuation, split, merge, disappearance, emergence
    let chain6 = evolve_chain(&EvolutionChainInput {
        run_key_prefix: None, // defaults to "chain0000000"
        batches: vec![
            // b0: 2 scenarios
            ChainBatchInput {
                ordinal: 0,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 30, 0.6, &[0.0]), make_sc(1, 20, 0.4, &[5.0])],
                    dispersion: 1.0,
                },
            },
            // b1 (b0->b1): P0 splits into C0(0.1), C2(0.5); P1 continues to C1(5.1)
            ChainBatchInput {
                ordinal: 1,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![
                        make_sc(0, 20, 0.4, &[0.1]),
                        make_sc(1, 20, 0.4, &[5.1]),
                        make_sc(2, 10, 0.2, &[0.5]),
                    ],
                    dispersion: 1.0,
                },
            },
            // b2 (b1->b2): C0 & C2 merge back into [0.2]; C1 continues to [5.2]
            ChainBatchInput {
                ordinal: 2,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 30, 0.6, &[0.2]), make_sc(1, 20, 0.4, &[5.2])],
                    dispersion: 1.0,
                },
            },
            // b3 (b2->b3): [0.2] continues to [0.3]; [5.2] disappears; [15.0] emerges
            ChainBatchInput {
                ordinal: 3,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 30, 0.6, &[0.3]), make_sc(1, 20, 0.4, &[15.0])],
                    dispersion: 1.0,
                },
            },
            // b4 (b3->b4): pure continuation
            ChainBatchInput {
                ordinal: 4,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 30, 0.6, &[0.4]), make_sc(1, 20, 0.4, &[15.1])],
                    dispersion: 1.0,
                },
            },
            // b5 (b4->b5): pure continuation
            ChainBatchInput {
                ordinal: 5,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 35, 0.7, &[0.5]), make_sc(1, 15, 0.3, &[15.2])],
                    dispersion: 1.0,
                },
            },
        ],
        weights: vec![1.0],
        threshold_coefficient: 1.0,
        config: EvolutionConfig {
            secondary_rule: SecondaryRule::NearestFeasibleV1,
            flow_rule: FlowRule::StarMinMassProportionalV1,
            ..Default::default()
        },
    })
    .unwrap();

    assert_eq!(chain6.run_key_prefix, DEFAULT_CHAIN_RUN_KEY_PREFIX);
    assert_eq!(chain6.transitions.len(), 5);
    assert_eq!(chain6.chain_summary.ok_batch_count, 6);
    assert_eq!(chain6.chain_summary.ok_transition_count, 5);
    assert_eq!(chain6.chain_summary.gap_transition_count, 0);
    assert!(chain6.chain_summary.split_count >= 1);
    assert!(chain6.chain_summary.merge_count >= 1);
    assert!(chain6.chain_summary.disappearance_count >= 1);
    assert!(chain6.chain_summary.emergence_count >= 1);
    assert_eq!(chain6.chain_summary.gap_interrupted_count, 0);
    assert_eq!(chain6.chain_summary.gap_restarted_count, 0);

    // E7: branch lifecycle conservation
    let sum_branch_nodes: usize = chain6.branches.iter().map(|b| b.node_count).sum();
    assert_eq!(sum_branch_nodes, chain6.nodes.len());
    for b in &chain6.branches {
        assert!(b.branch_id.starts_with("chain0000000:b"));
        assert_eq!(b.node_count, b.end_ordinal - b.birth_ordinal + 1);
        assert_eq!(b.step_count, b.node_count - 1);
    }
    let main_branch = chain6
        .branches
        .iter()
        .find(|b| b.branch_id == "chain0000000:b00:k00")
        .expect("b00:k00 branch exists");
    assert_eq!(main_branch.birth_ordinal, 0);
    assert_eq!(main_branch.end_ordinal, 5);
    assert_eq!(main_branch.split_out_event_count, 1);
    assert_eq!(main_branch.merge_in_event_count, 1);
    assert_eq!(main_branch.spawned_secondary_branch_ids.len(), 1);
    assert_eq!(main_branch.absorbed_secondary_branch_ids.len(), 1);
    assert!((main_branch.end_to_end_morphology_distance - 0.5).abs() < 1e-12);

    // 2. 8-batch chain with Missing (b2), Failed (b3), and Failed (b6)
    let chain8 = evolve_chain(&EvolutionChainInput {
        run_key_prefix: Some("abcdef1234567890".to_string()),
        batches: vec![
            ChainBatchInput {
                ordinal: 0,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
                    dispersion: 1.0,
                },
            },
            ChainBatchInput {
                ordinal: 1,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 50, 1.0, &[0.2])],
                    dispersion: 1.0,
                },
            },
            ChainBatchInput {
                ordinal: 2,
                status: ChainBatchStatus::Missing {
                    reason: "upstream_file_absent".to_string(),
                },
            },
            ChainBatchInput {
                ordinal: 3,
                status: ChainBatchStatus::Failed {
                    code: "INSUFFICIENT_RANK".to_string(),
                    reason: "effective_rank < projection_dim".to_string(),
                },
            },
            ChainBatchInput {
                ordinal: 4,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 30, 0.6, &[0.3]), make_sc(1, 20, 0.4, &[5.0])],
                    dispersion: 1.0,
                },
            },
            ChainBatchInput {
                ordinal: 5,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 30, 0.6, &[0.4]), make_sc(1, 20, 0.4, &[5.1])],
                    dispersion: 1.0,
                },
            },
            ChainBatchInput {
                ordinal: 6,
                status: ChainBatchStatus::Failed {
                    code: "EMPTY_CLUSTER".to_string(),
                    reason: "degenerate_cluster".to_string(),
                },
            },
            ChainBatchInput {
                ordinal: 7,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 50, 1.0, &[0.5])],
                    dispersion: 1.0,
                },
            },
        ],
        weights: vec![1.0],
        threshold_coefficient: 1.0,
        config: EvolutionConfig::default(),
    })
    .unwrap();

    assert_eq!(chain8.run_key_prefix, "abcdef123456");
    assert_eq!(chain8.transitions.len(), 7);
    assert_eq!(chain8.chain_summary.ok_batch_count, 5);
    assert_eq!(chain8.chain_summary.missing_batch_count, 1);
    assert_eq!(chain8.chain_summary.failed_batch_count, 2);
    assert_eq!(chain8.chain_summary.ok_transition_count, 2); // (0->1) and (4->5)
    assert_eq!(chain8.chain_summary.gap_transition_count, 5); // (1->2), (2->3), (3->4), (5->6), (6->7)

    // E4 & E5: gap transitions have 0 edges, exact gap_reason, and strictly disjoint gap_* events
    assert_eq!(
        chain8.transitions[1].gap_reason.as_deref(),
        Some("curr_unavailable")
    );
    assert_eq!(
        chain8.transitions[2].gap_reason.as_deref(),
        Some("both_unavailable")
    );
    assert!(chain8.transitions[2].events.is_empty());
    assert_eq!(
        chain8.transitions[3].gap_reason.as_deref(),
        Some("prev_unavailable")
    );

    for t in &chain8.transitions {
        if t.transition_status == ChainTransitionStatus::Gap {
            assert!(t.chain_broken);
            assert!(t.evolve_result.is_none());
            assert_eq!(t.primary_edge_count, 0);
            for ev in &t.events {
                assert!(matches!(
                    ev.event_type,
                    TransitionEventKind::GapInterrupted | TransitionEventKind::GapRestarted
                ));
            }
        } else {
            assert!(!t.chain_broken);
            assert!(t.evolve_result.is_some());
            for ev in &t.events {
                assert!(!matches!(
                    ev.event_type,
                    TransitionEventKind::GapInterrupted | TransitionEventKind::GapRestarted
                ));
            }
        }
    }

    // Verify no branch resurrects across gaps (b04 starts fresh `abcdef123456:b04:k00`)
    for b in &chain8.branches {
        assert_eq!(b.node_count, b.end_ordinal - b.birth_ordinal + 1);
    }
    assert!(
        chain8
            .branches
            .iter()
            .any(|b| b.branch_id == "abcdef123456:b04:k00" && b.birth_reason == "gap_restarted")
    );
    assert!(chain8.branches.iter().any(
        |b| b.branch_id == "abcdef123456:b00:k00" && b.termination_reason == "gap_interrupted"
    ));

    // Gap events carry max_morphology_over_scale == Some(0.0)
    for ev in &chain8.events {
        if matches!(
            ev.event_type,
            TransitionEventKind::GapInterrupted | TransitionEventKind::GapRestarted
        ) {
            assert_eq!(ev.max_morphology_over_scale, Some(0.0));
        }
    }
}

// ---------------------------------------------------------------------------
// M3 Review Regression Contract Tests (`[H2]`, `[M1]`, `[M2]`, `[M3]`, `[L1]`)
// ---------------------------------------------------------------------------

#[test]
fn h2_subnormal_pair_scale_and_reference_scale_return_numerical_failure() {
    // 1. Matched scenarios (d = 0.0) where SameCount optimality_margin = 2.8 overflows 2.8 / 1e-308 -> +inf
    let err_matched = evolve_scenarios_scaled(&ScaledEvolutionInput {
        prev_scenarios: vec![make_sc(0, 25, 0.5, &[0.0]), make_sc(1, 25, 0.5, &[1.4])],
        curr_scenarios: vec![make_sc(0, 25, 0.5, &[0.0]), make_sc(1, 25, 0.5, &[1.4])],
        weights: vec![1.0],
        threshold_coefficient: 1.5e308,
        prev_dispersion: 1e-308,
        curr_dispersion: 1e-308,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap_err();
    assert_eq!(err_matched.code, CoreErrorCode::NumericalFailure);

    // 2. Unmatched scenario with subnormal pair_scale (1e-308) and distance 5.0 > tau = 1.0 (5.0 / 1e-308 -> +inf)
    let err_unmatched = evolve_scenarios_scaled(&ScaledEvolutionInput {
        prev_scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
        curr_scenarios: vec![make_sc(0, 50, 1.0, &[5.0])],
        weights: vec![1.0],
        threshold_coefficient: 1e308,
        prev_dispersion: 1e-308,
        curr_dispersion: 1e-308,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap_err();
    assert_eq!(err_unmatched.code, CoreErrorCode::NumericalFailure);

    // 3. Absolute mode with subnormal tau = 1e-308 and unmatched distance 5.0 > tau
    let err_abs = evolve_scenarios(&EvolutionInput {
        prev_scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
        curr_scenarios: vec![make_sc(0, 50, 1.0, &[5.0])],
        weights: vec![1.0],
        threshold: 1e-308,
        config: EvolutionConfig::default(),
        backbone: None,
    })
    .unwrap_err();
    assert_eq!(err_abs.code, CoreErrorCode::NumericalFailure);

    // 4. Chain mode with subnormal dispersions and distance 5.0 propagates NumericalFailure
    let err_chain = evolve_chain(&EvolutionChainInput {
        run_key_prefix: None,
        batches: vec![
            ChainBatchInput {
                ordinal: 0,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
                    dispersion: 1e-308,
                },
            },
            ChainBatchInput {
                ordinal: 1,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 50, 1.0, &[5.0])],
                    dispersion: 1e-308,
                },
            },
            ChainBatchInput {
                ordinal: 2,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 50, 1.0, &[5.0])],
                    dispersion: 1.0,
                },
            },
            ChainBatchInput {
                ordinal: 3,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 50, 1.0, &[5.0])],
                    dispersion: 1.0,
                },
            },
        ],
        weights: vec![1.0],
        threshold_coefficient: 1e308,
        config: EvolutionConfig::default(),
    })
    .unwrap_err();
    assert_eq!(err_chain.code, CoreErrorCode::NumericalFailure);
}

#[test]
fn m1_chronological_and_canonical_secondary_branch_id_ordering() {
    // Construct a 4-batch chain where primary branch `test00000000:b00:k00`:
    // - at prev_ordinal = 0 (b0 -> b1): absorbs higher-named `test00000000:b00:k02`
    //   while `test00000000:b00:k01` continues independently to b1!
    // - at prev_ordinal = 1 (b1 -> b2): absorbs lower-named `test00000000:b00:k01`!
    // - at prev_ordinal = 2 (b2 -> b3): splits into two secondary children whose
    //   `cluster_id` order is inverted relative to their canonical `branch_id` order!
    //
    // Positions (tau = 1.0):
    // b0:
    //   cid=0: [0.0]  -> k00 (`b00:k00`)
    //   cid=1: [2.0]  -> k01 (`b00:k01`)
    //   cid=2: [2.5]  -> wait: canonical order sorts by mean_features ascending!
    //   Let's place:
    //   cid=0: [0.0]  -> k00 (`b00:k00`)
    //   cid=1: [1.5]  -> k01 (`b00:k01`)
    //   cid=2: [2.3]  -> k02 (`b00:k02`) -- too far from 0.0 unless 2D!
    // In 2D (weights = [0.5, 0.5], tau = 1.0):
    //   b0:
    //     cid=0: [0.0, 0.0]  -> k00 (`b00:k00`)
    //     cid=1: [0.5, 1.2]  -> k01 (`b00:k01`)
    //     cid=2: [0.6, 0.0]  -> k02 (`b00:k02`)  (since [0.5, 1.2] < [0.6, 0.0] lexicographically!)
    //   b1:
    //     cid=0: [0.0, 0.0]  -> primary match from b0 cid=0 (d=0.0); b0 cid=2 [0.6, 0.0] has d=sqrt(0.18)~0.424 <= 1.0 and merges into b1 cid=0!
    //     cid=1: [0.5, 1.1]  -> primary match from b0 cid=1 (d=sqrt(0.005)~0.071 <= 1.0), so `b00:k01` survives to b1!
    //   b2:
    //     cid=0: [0.2, 0.5]  -> primary match from b1 cid=0 (d=sqrt(0.145)~0.381), AND b1 cid=1 [0.5, 1.1] has d=sqrt(0.225)~0.474 <= 1.0, so `b00:k01` merges into `b00:k00` at prev_ordinal=1!
    //   b3:
    //     cid=0: [0.2, 0.5]  -> primary match from b2 cid=0 (d=0.0)
    //     cid=1: [0.6, 0.5]  -> secondary split child, feature [0.6, 0.5] -> k02 (`b03:k02`)
    //     cid=2: [0.4, 0.5]  -> secondary split child, feature [0.4, 0.5] -> k01 (`b03:k01`), given higher cluster_id=2!
    let chain = evolve_chain(&EvolutionChainInput {
        run_key_prefix: Some("test00000000".to_string()),
        batches: vec![
            ChainBatchInput {
                ordinal: 0,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![
                        make_sc(0, 20, 0.40, &[0.0, 0.0]),
                        make_sc(1, 15, 0.30, &[0.5, 1.2]),
                        make_sc(2, 15, 0.30, &[0.6, 0.0]),
                    ],
                    dispersion: 1.0,
                },
            },
            ChainBatchInput {
                ordinal: 1,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![
                        make_sc(0, 35, 0.70, &[0.0, 0.0]),
                        make_sc(1, 15, 0.30, &[0.5, 1.1]),
                    ],
                    dispersion: 1.0,
                },
            },
            ChainBatchInput {
                ordinal: 2,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 50, 1.00, &[0.2, 0.5])],
                    dispersion: 1.0,
                },
            },
            ChainBatchInput {
                ordinal: 3,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![
                        make_sc(0, 30, 0.60, &[0.2, 0.5]),
                        make_sc(1, 10, 0.20, &[0.6, 0.5]),
                        make_sc(2, 10, 0.20, &[0.4, 0.5]),
                    ],
                    dispersion: 1.0,
                },
            },
        ],
        weights: vec![0.5, 0.5],
        threshold_coefficient: 1.0,
        config: EvolutionConfig::default(),
    })
    .unwrap();

    let main_branch = chain
        .branches
        .iter()
        .find(|b| b.branch_id == "test00000000:b00:k00")
        .expect("primary branch test00000000:b00:k00 exists");

    // Chronological (prev_ordinal=0 -> b00:k02, then prev_ordinal=1 -> b00:k01), NOT lexicographical!
    assert_eq!(
        main_branch.absorbed_secondary_branch_ids,
        vec![
            "test00000000:b00:k02".to_string(),
            "test00000000:b00:k01".to_string(),
        ]
    );

    // Within prev_ordinal=2, ordered by curr_branch_id ascending (k01 before k02) regardless of cluster_id!
    assert_eq!(
        main_branch.spawned_secondary_branch_ids,
        vec![
            "test00000000:b03:k01".to_string(),
            "test00000000:b03:k02".to_string(),
        ]
    );
}

#[test]
fn m2_m3_l1_error_whitelist_isolated_batch_message_and_invalid_scenario_id() {
    assert_eq!(
        CoreErrorCode::InvalidScenarioId.as_str(),
        "INVALID_SCENARIO_ID"
    );
    assert!(std::mem::size_of::<CoreError>() <= 112);

    // [M3] Non-whitelisted error_code in ChainBatchStatus::Failed must fail with InvalidEvolutionConfig at batch ordinal
    for bad_code in [
        "",
        "NUMERICAL_FAILURE",
        "INVALID_CONFIG",
        "NULL_INPUT",
        "BATCH_FAILED",
    ] {
        let err = evolve_chain(&EvolutionChainInput {
            run_key_prefix: None,
            batches: vec![
                ChainBatchInput {
                    ordinal: 0,
                    status: ChainBatchStatus::Ok {
                        scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
                        dispersion: 1.0,
                    },
                },
                ChainBatchInput {
                    ordinal: 1,
                    status: ChainBatchStatus::Ok {
                        scenarios: vec![make_sc(0, 50, 1.0, &[0.1])],
                        dispersion: 1.0,
                    },
                },
                ChainBatchInput {
                    ordinal: 2,
                    status: ChainBatchStatus::Failed {
                        code: bad_code.to_string(),
                        reason: "bad".to_string(),
                    },
                },
                ChainBatchInput {
                    ordinal: 3,
                    status: ChainBatchStatus::Ok {
                        scenarios: vec![make_sc(0, 50, 1.0, &[0.2])],
                        dispersion: 1.0,
                    },
                },
            ],
            weights: vec![1.0],
            threshold_coefficient: 1.0,
            config: EvolutionConfig::default(),
        })
        .unwrap_err();
        assert_eq!(err.code, CoreErrorCode::InvalidEvolutionConfig);
        assert_eq!(err.prev_index, Some(2));
        assert!(err.message.contains("batch 2"));
    }

    // All 8 whitelisted single-batch failure codes must succeed in evolve_chain
    for good_code in [
        "INSUFFICIENT_RANK",
        "AMBIGUOUS_SUBSPACE",
        "EMPTY_CLUSTER",
        "NOT_CONVERGED",
        "INSUFFICIENT_DISTINCT_VECTORS",
        "INSUFFICIENT_PROJECTED_VECTORS",
        "SVD_NOT_CONVERGED",
        "ZERO_DISPERSION",
    ] {
        let ok = evolve_chain(&EvolutionChainInput {
            run_key_prefix: None,
            batches: vec![
                ChainBatchInput {
                    ordinal: 0,
                    status: ChainBatchStatus::Ok {
                        scenarios: vec![make_sc(0, 50, 1.0, &[0.0])],
                        dispersion: 1.0,
                    },
                },
                ChainBatchInput {
                    ordinal: 1,
                    status: ChainBatchStatus::Failed {
                        code: good_code.to_string(),
                        reason: "whitelisted".to_string(),
                    },
                },
                ChainBatchInput {
                    ordinal: 2,
                    status: ChainBatchStatus::Ok {
                        scenarios: vec![make_sc(0, 50, 1.0, &[0.1])],
                        dispersion: 1.0,
                    },
                },
                ChainBatchInput {
                    ordinal: 3,
                    status: ChainBatchStatus::Ok {
                        scenarios: vec![make_sc(0, 50, 1.0, &[0.2])],
                        dispersion: 1.0,
                    },
                },
            ],
            weights: vec![1.0],
            threshold_coefficient: 1.0,
            config: EvolutionConfig::default(),
        })
        .unwrap();
        assert_eq!(ok.batches[1].status_code.as_deref(), Some(good_code));
    }

    // [L1] Isolated `ok` batch (surrounded by Missing batches so no pair transition runs)
    // with invalid fraction sum must include `batch 2` in err.message!
    let err_iso = evolve_chain(&EvolutionChainInput {
        run_key_prefix: None,
        batches: vec![
            ChainBatchInput {
                ordinal: 0,
                status: ChainBatchStatus::Missing {
                    reason: "m0".into(),
                },
            },
            ChainBatchInput {
                ordinal: 1,
                status: ChainBatchStatus::Missing {
                    reason: "m1".into(),
                },
            },
            ChainBatchInput {
                ordinal: 2,
                status: ChainBatchStatus::Ok {
                    scenarios: vec![make_sc(0, 25, 0.4, &[0.0]), make_sc(1, 25, 0.4, &[1.0])],
                    dispersion: 1.0,
                },
            },
            ChainBatchInput {
                ordinal: 3,
                status: ChainBatchStatus::Missing {
                    reason: "m3".into(),
                },
            },
        ],
        weights: vec![1.0],
        threshold_coefficient: 1.0,
        config: EvolutionConfig::default(),
    })
    .unwrap_err();
    assert_eq!(err_iso.code, CoreErrorCode::InvalidFractionSum);
    assert!(
        err_iso.message.contains("batch 2"),
        "Expected 'batch 2' in isolated batch error message, got: {}",
        err_iso.message
    );
}
