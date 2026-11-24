use ensabc_core::{
    CoreErrorCode, MatchInput, MemberInput, ScaledMatchInput, ScenarioNodeInput, Substitutability,
    ensemble_dispersion, match_scenarios, match_scenarios_scaled,
};

#[test]
fn test_threshold_counterexample_2x2() {
    let p1_0 = (-1.0 + 63.0_f64.sqrt()) / 2.0;
    let p1_1 = -1.0 - p1_0;

    let input = MatchInput {
        weights: vec![1.0, 1.0],
        threshold: 5.0,
        prev_scenarios: vec![
            ScenarioNodeInput {
                cluster_id: 0,
                mean_features: vec![0.0, 0.0],
                member_count: 25,
                member_fraction: 0.5,
            },
            ScenarioNodeInput {
                cluster_id: 1,
                mean_features: vec![p1_0, p1_1],
                member_count: 25,
                member_fraction: 0.5,
            },
        ],
        curr_scenarios: vec![
            ScenarioNodeInput {
                cluster_id: 0,
                mean_features: vec![0.0, 0.0],
                member_count: 25,
                member_fraction: 0.5,
            },
            ScenarioNodeInput {
                cluster_id: 1,
                mean_features: vec![4.0, 4.0],
                member_count: 25,
                member_fraction: 0.5,
            },
        ],
    };

    let result = match_scenarios(&input).expect("Matching should succeed");
    assert_eq!(result.objective.matched_count, 2);
    assert!((result.objective.weighted_distance_sum - 8.0).abs() < 1e-12);
    assert_eq!(result.links.len(), 2);

    // Links should be (0 -> 1, d=4.0) and (1 -> 0, d=4.0)
    assert_eq!(result.links[0].prev_cluster_id, 0);
    assert_eq!(result.links[0].curr_cluster_id, 1);
    assert!((result.links[0].distance_k - 4.0).abs() < 1e-12);

    assert_eq!(result.links[1].prev_cluster_id, 1);
    assert_eq!(result.links[1].curr_cluster_id, 0);
    assert!((result.links[1].distance_k - 4.0).abs() < 1e-12);
}

#[test]
fn test_scenario_ceiling_12_and_13() {
    let make_scenarios = |count: usize| -> Vec<ScenarioNodeInput> {
        (0..count)
            .map(|i| ScenarioNodeInput {
                cluster_id: i,
                mean_features: vec![i as f64],
                member_count: 10,
                member_fraction: 0.1,
            })
            .collect()
    };

    // 12 scenarios should succeed
    let input12 = MatchInput {
        weights: vec![1.0],
        threshold: 20.0,
        prev_scenarios: make_scenarios(12),
        curr_scenarios: make_scenarios(12),
    };
    let res12 = match_scenarios(&input12);
    assert!(res12.is_ok(), "12 scenarios must be supported");

    // 13 scenarios should be rejected with EXCEEDED_SCENARIO_LIMIT
    let input13 = MatchInput {
        weights: vec![1.0],
        threshold: 20.0,
        prev_scenarios: make_scenarios(13),
        curr_scenarios: make_scenarios(12),
    };
    let err13 = match_scenarios(&input13).expect_err("13 scenarios must be rejected");
    assert_eq!(err13.code, CoreErrorCode::TooManyScenarios);
}

#[test]
fn test_all_exceed_threshold() {
    let input = MatchInput {
        weights: vec![1.0],
        threshold: 0.5,
        prev_scenarios: vec![ScenarioNodeInput {
            cluster_id: 0,
            mean_features: vec![0.0],
            member_count: 50,
            member_fraction: 1.0,
        }],
        curr_scenarios: vec![ScenarioNodeInput {
            cluster_id: 0,
            mean_features: vec![10.0],
            member_count: 50,
            member_fraction: 1.0,
        }],
    };
    let res = match_scenarios(&input).unwrap();
    assert_eq!(res.objective.matched_count, 0);
    assert_eq!(res.links.len(), 0);
    assert_eq!(res.unmatched_prev.len(), 1);
    assert_eq!(
        res.unmatched_prev[0].reason,
        "no_candidate_within_threshold"
    );
}

#[test]
fn test_substitutability_types() {
    // 2 prev vs 3 curr: P0 at 0, P1 at 10. C0 at 0.5, C1 at 9.5, C2 at 5.0.
    let input = MatchInput {
        weights: vec![1.0],
        threshold: 10.0,
        prev_scenarios: vec![
            ScenarioNodeInput {
                cluster_id: 0,
                mean_features: vec![0.0],
                member_count: 25,
                member_fraction: 0.5,
            },
            ScenarioNodeInput {
                cluster_id: 1,
                mean_features: vec![10.0],
                member_count: 25,
                member_fraction: 0.5,
            },
        ],
        curr_scenarios: vec![
            ScenarioNodeInput {
                cluster_id: 0,
                mean_features: vec![0.5],
                member_count: 15,
                member_fraction: 0.3,
            },
            ScenarioNodeInput {
                cluster_id: 1,
                mean_features: vec![9.5],
                member_count: 15,
                member_fraction: 0.3,
            },
            ScenarioNodeInput {
                cluster_id: 2,
                mean_features: vec![5.0],
                member_count: 20,
                member_fraction: 0.4,
            },
        ],
    };
    let res = match_scenarios(&input).unwrap();
    assert_eq!(res.objective.matched_count, 2);
    // Links have alternative same_count matchings via C2
    for link in &res.links {
        match &link.substitutability {
            Substitutability::SameCount { gap, .. } => assert!(*gap > 0.0),
            other => panic!("Expected SameCount, got {:?}", other),
        }
    }
}

#[test]
fn test_symmetric_ambiguity() {
    let input = MatchInput {
        weights: vec![1.0, 1.0],
        threshold: 5.0,
        prev_scenarios: vec![
            ScenarioNodeInput {
                cluster_id: 0,
                mean_features: vec![0.0, 0.0],
                member_count: 25,
                member_fraction: 0.5,
            },
            ScenarioNodeInput {
                cluster_id: 1,
                mean_features: vec![2.0, 2.0],
                member_count: 25,
                member_fraction: 0.5,
            },
        ],
        curr_scenarios: vec![
            ScenarioNodeInput {
                cluster_id: 0,
                mean_features: vec![0.0, 2.0],
                member_count: 25,
                member_fraction: 0.5,
            },
            ScenarioNodeInput {
                cluster_id: 1,
                mean_features: vec![2.0, 0.0],
                member_count: 25,
                member_fraction: 0.5,
            },
        ],
    };
    let res = match_scenarios(&input).unwrap();
    assert_eq!(res.objective.matched_count, 2);
    assert_eq!(res.optima_count, 2);
    for link in &res.links {
        assert!(link.ambiguous);
        assert_eq!(link.substitutability, Substitutability::Tied);
    }
}

#[test]
fn test_scaled_match_scenarios() {
    let input = ScaledMatchInput {
        weights: vec![1.0],
        threshold_coefficient: 1.0,
        prev_dispersion: 2.0,
        curr_dispersion: 2.0,
        prev_scenarios: vec![ScenarioNodeInput {
            cluster_id: 0,
            mean_features: vec![0.0],
            member_count: 50,
            member_fraction: 1.0,
        }],
        curr_scenarios: vec![ScenarioNodeInput {
            cluster_id: 0,
            mean_features: vec![1.5],
            member_count: 50,
            member_fraction: 1.0,
        }],
    };
    let res = match_scenarios_scaled(&input).unwrap();
    assert_eq!(res.pair_scale, Some(2.0));
    assert_eq!(res.threshold_coefficient, Some(1.0));
    assert_eq!(res.links.len(), 1);
    assert!((res.links[0].distance_k - 1.5).abs() < 1e-12);
    assert!((res.links[0].distance_ratio.unwrap() - 0.75).abs() < 1e-12);
}

#[test]
fn test_dispersion_flat_fsum() {
    let members = vec![
        MemberInput {
            member_index: 0,
            member_id: "m0".into(),
            features: vec![1.0, 2.0],
        },
        MemberInput {
            member_index: 1,
            member_id: "m1".into(),
            features: vec![3.0, 4.0],
        },
    ];
    let weights = vec![1.0, 1.0];
    let s = ensemble_dispersion(&members, &weights).unwrap();
    assert!(s > 0.0);
}

fn scalar_scenarios(values: &[f64]) -> Vec<ScenarioNodeInput> {
    values
        .iter()
        .enumerate()
        .map(|(cluster_id, &value)| ScenarioNodeInput {
            cluster_id,
            mean_features: vec![value],
            member_count: 1,
            member_fraction: 1.0,
        })
        .collect()
}

#[test]
fn ambiguity_is_local_to_replaceable_edges() {
    let result = match_scenarios(&MatchInput {
        prev_scenarios: scalar_scenarios(&[0.0, 0.0, 100.0]),
        curr_scenarios: scalar_scenarios(&[0.0, 0.0, 100.0]),
        weights: vec![1.0],
        threshold: 0.0,
    })
    .unwrap();
    assert_eq!(result.optima_count, 2);
    for link in result.links {
        assert_eq!(link.ambiguous, link.prev_cluster_id != 2);
    }
}

#[test]
fn unmatched_optimum_membership_requires_equal_objective() {
    for (values, expected) in [([0.0, 1.0], false), ([0.0, 0.0], true)] {
        let prev = scalar_scenarios(&values);
        let curr = scalar_scenarios(&[0.0]);
        for (prev_scenarios, curr_scenarios) in [(prev.clone(), curr.clone()), (curr, prev)] {
            let result = match_scenarios(&MatchInput {
                prev_scenarios,
                curr_scenarios,
                weights: vec![1.0],
                threshold: 2.0,
            })
            .unwrap();
            let unmatched = result
                .unmatched_prev
                .iter()
                .chain(&result.unmatched_curr)
                .next()
                .unwrap();
            assert_eq!(unmatched.matched_in_some_optimum, expected);
        }
    }
}

#[test]
fn dispersion_validates_member_identity_in_core() {
    for (ids, expected) in [
        (["", "m1"], CoreErrorCode::EmptyMemberId),
        (["m0", "m0"], CoreErrorCode::DuplicateMemberId),
    ] {
        let members: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| MemberInput {
                member_index: i,
                member_id: id.to_string(),
                features: vec![i as f64],
            })
            .collect();
        assert_eq!(
            ensemble_dispersion(&members, &[1.0]).unwrap_err().code,
            expected
        );
    }
}

#[test]
fn scaled_match_allows_zero_threshold_coefficient() {
    let input = ScaledMatchInput {
        weights: vec![1.0],
        threshold_coefficient: 0.0,
        prev_dispersion: 1.0,
        curr_dispersion: 1.0,
        prev_scenarios: scalar_scenarios(&[0.0, 2.0]),
        curr_scenarios: scalar_scenarios(&[0.0, 2.5]),
    };
    let res = match_scenarios_scaled(&input).expect("c=0.0 must be accepted");
    assert_eq!(res.objective.matched_count, 1);
    assert_eq!(res.links[0].prev_cluster_id, 0);
    assert_eq!(res.links[0].curr_cluster_id, 0);
    assert_eq!(res.links[0].distance_k, 0.0);
    assert_eq!(res.links[0].distance_ratio, Some(0.0));
}

#[test]
fn rmse_division_overflow_fails_instead_of_unmatched() {
    let input = MatchInput {
        weights: vec![1e-308],
        threshold: 1.0,
        prev_scenarios: scalar_scenarios(&[0.0]),
        curr_scenarios: scalar_scenarios(&[1e155]),
    };
    let err = match_scenarios(&input).expect_err("overflow must return NumericalFailure");
    assert_eq!(err.code, CoreErrorCode::NumericalFailure);
}

#[test]
fn error_code_names_and_indices_match_contract() {
    assert_eq!(CoreErrorCode::TooManyScenarios.as_str(), "TOO_MANY_SCENARIOS");
    let err = match_scenarios(&MatchInput {
        weights: vec![1.0],
        threshold: 1.0,
        prev_scenarios: scalar_scenarios(&[0.0, f64::NAN]),
        curr_scenarios: scalar_scenarios(&[0.0]),
    })
    .unwrap_err();
    assert_eq!(err.code, CoreErrorCode::NonFiniteInput);
    assert_eq!(err.prev_index, Some(1));
    assert_eq!(err.feature_index, Some(0));
}
