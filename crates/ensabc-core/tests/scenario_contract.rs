use ensabc_core::{
    ALGORITHM_VERSION, CoreErrorCode, InitMethod, MemberInput, ScenarioConfig, StopRule, TaskInput,
    cluster,
};

fn default_test_config() -> ScenarioConfig {
    ScenarioConfig {
        projection_dim: 2,
        cluster_count: 2,
        initialization: InitMethod::KMeansPlusPlusChaCha8V1,
        seed: 42,
        restarts: 3,
        initial_centers: Vec::new(),
        stopping_rule: StopRule::SklearnVarianceScaledV1,
        tolerance: 1e-4,
        max_iterations: 300,
        rank_threshold: 1e-10,
    }
}

fn make_members(data: &[(&str, &[f64])]) -> Vec<MemberInput> {
    data.iter()
        .enumerate()
        .map(|(i, (id, feats))| MemberInput {
            member_index: i,
            member_id: id.to_string(),
            features: feats.to_vec(),
        })
        .collect()
}

#[test]
fn random_restarts_consume_one_seeded_stream() {
    use rand::{RngCore, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    let mut cfg = default_test_config();
    cfg.projection_dim = 1;
    cfg.cluster_count = 2;
    let seed = cfg.seed;
    let result = cluster(&TaskInput {
        config: cfg,
        members: make_members(&[("a", &[0.0]), ("b", &[1.0]), ("c", &[4.0])]),
    })
    .unwrap();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    for restart in result.diagnostics.restarts {
        let u = (rng.next_u64() >> 11) as f64 / ((1u64 << 53) as f64);
        assert_eq!(
            restart.initial_member_indices[0],
            (u * 3.0).floor() as usize
        );
        rng.next_u64(); // The second D² center consumes the next draw.
    }
}

#[test]
fn tolerance_stop_reports_changed_final_assignment() {
    let mut cfg = default_test_config();
    cfg.projection_dim = 1;
    cfg.initialization = InitMethod::ExplicitCentersV1;
    cfg.seed = 0;
    cfg.restarts = 1;
    cfg.tolerance = 100.0;
    cfg.initial_centers = vec![vec![vec![0.0], vec![1.0]]];
    let mut task = TaskInput {
        config: cfg,
        members: make_members(&[("a", &[0.0]), ("b", &[2.0]), ("c", &[3.0]), ("d", &[10.0])]),
    };
    let projected = ensabc_core::preprocess::preprocess_and_project(&task).unwrap();
    task.config.initial_centers = vec![vec![
        projected.scores[0].clone(),
        projected.scores[1].clone(),
    ]];
    let result = cluster(&task).unwrap();
    assert_eq!(result.diagnostics.stop_reason, "center_shift");
    assert!(result.diagnostics.final_assignment_changed);
}

#[test]
fn test_negative_empty_members() {
    let task = TaskInput {
        config: default_test_config(),
        members: Vec::new(),
    };
    let err = cluster(&task).unwrap_err();
    assert_eq!(err.code, CoreErrorCode::EmptyMembers);
}

#[test]
fn test_negative_empty_member_id() {
    let members = make_members(&[
        ("", &[1.0, 2.0, 3.0]),
        ("m2", &[4.0, 5.0, 6.0]),
        ("m3", &[7.0, 8.0, 9.0]),
    ]);
    let task = TaskInput {
        config: default_test_config(),
        members,
    };
    let err = cluster(&task).unwrap_err();
    assert_eq!(err.code, CoreErrorCode::EmptyMemberId);
    assert_eq!(err.member_index, Some(0));
}

#[test]
fn test_negative_duplicate_member_id() {
    let members = make_members(&[
        ("m1", &[1.0, 2.0, 3.0]),
        ("m1", &[4.0, 5.0, 6.0]),
        ("m3", &[7.0, 8.0, 9.0]),
    ]);
    let task = TaskInput {
        config: default_test_config(),
        members,
    };
    let err = cluster(&task).unwrap_err();
    assert_eq!(err.code, CoreErrorCode::DuplicateMemberId);
    assert_eq!(err.member_index, Some(1));
}

#[test]
fn test_negative_empty_features() {
    let members = make_members(&[("m1", &[]), ("m2", &[]), ("m3", &[])]);
    let task = TaskInput {
        config: default_test_config(),
        members,
    };
    let err = cluster(&task).unwrap_err();
    assert_eq!(err.code, CoreErrorCode::EmptyFeatures);
    assert_eq!(err.member_index, Some(0));
}

#[test]
fn test_negative_ragged_features() {
    let members = make_members(&[
        ("m1", &[1.0, 2.0, 3.0]),
        ("m2", &[4.0, 5.0]),
        ("m3", &[7.0, 8.0, 9.0]),
    ]);
    let task = TaskInput {
        config: default_test_config(),
        members,
    };
    let err = cluster(&task).unwrap_err();
    assert_eq!(err.code, CoreErrorCode::RaggedFeatures);
    assert_eq!(err.member_index, Some(1));
}

#[test]
fn test_negative_non_finite_input() {
    let members = make_members(&[
        ("m1", &[1.0, 2.0, 3.0]),
        ("m2", &[4.0, f64::NAN, 6.0]),
        ("m3", &[7.0, 8.0, 9.0]),
    ]);
    let task = TaskInput {
        config: default_test_config(),
        members,
    };
    let err = cluster(&task).unwrap_err();
    assert_eq!(err.code, CoreErrorCode::NonFiniteInput);
    assert_eq!(err.member_index, Some(1));
    assert_eq!(err.feature_index, Some(1));
}

#[test]
fn test_negative_invalid_config() {
    let members = make_members(&[
        ("m1", &[1.0, 2.0, 3.0]),
        ("m2", &[4.0, 5.0, 6.0]),
        ("m3", &[7.0, 8.0, 9.0]),
    ]);

    // k > n
    let mut cfg = default_test_config();
    cfg.cluster_count = 5;
    let err = cluster(&TaskInput {
        config: cfg,
        members: members.clone(),
    })
    .unwrap_err();
    assert_eq!(err.code, CoreErrorCode::InvalidConfig);

    // d = 0
    let mut cfg = default_test_config();
    cfg.projection_dim = 0;
    let err = cluster(&TaskInput {
        config: cfg,
        members: members.clone(),
    })
    .unwrap_err();
    assert_eq!(err.code, CoreErrorCode::InvalidConfig);

    // max_iter = 0
    let mut cfg = default_test_config();
    cfg.max_iterations = 0;
    let err = cluster(&TaskInput {
        config: cfg,
        members: members.clone(),
    })
    .unwrap_err();
    assert_eq!(err.code, CoreErrorCode::InvalidConfig);

    // rank_threshold <= 0
    let mut cfg = default_test_config();
    cfg.rank_threshold = -1.0;
    let err = cluster(&TaskInput {
        config: cfg,
        members,
    })
    .unwrap_err();
    assert_eq!(err.code, CoreErrorCode::InvalidConfig);
}

#[test]
fn test_positive_contract_and_diagnostics() {
    // 6 members, 4 features
    let members = make_members(&[
        ("m1", &[10.0, 1.0, 0.5, 100.0]),
        ("m2", &[11.0, 1.2, 0.6, 100.0]),
        ("m3", &[10.5, 0.9, 0.4, 100.0]),
        ("m4", &[50.0, 9.0, 8.0, 100.0]),
        ("m5", &[51.0, 9.2, 8.5, 100.0]),
        ("m6", &[49.5, 8.8, 7.9, 100.0]),
    ]);
    let mut cfg = default_test_config();
    cfg.projection_dim = 2;
    cfg.cluster_count = 2;

    let res = cluster(&TaskInput {
        config: cfg,
        members,
    })
    .unwrap();
    assert_eq!(res.algorithm_version, ALGORITHM_VERSION);
    assert_eq!(res.members.len(), 6);
    assert_eq!(res.scenarios.len(), 2);

    // Check sum of member counts and fractions
    let total_count: usize = res.scenarios.iter().map(|s| s.member_count).sum();
    let total_frac: f64 = res.scenarios.iter().map(|s| s.member_fraction).sum();
    assert_eq!(total_count, 6);
    assert!((total_frac - 1.0).abs() <= 1e-12);

    // Check diagnostics
    assert!(res.diagnostics.rank_after_scaling >= 2);
    assert_eq!(res.diagnostics.explained_variance_ratio.len(), 2);
    for (&s, &variance) in res
        .diagnostics
        .singular_values
        .iter()
        .zip(&res.diagnostics.explained_variance)
    {
        assert!((variance - s * s / 5.0).abs() < 1e-12);
    }
    for j in 0..4 {
        let mean = res.diagnostics.scaled.iter().map(|row| row[j]).sum::<f64>() / 6.0;
        assert_eq!(res.diagnostics.pca_mean[j], mean);
    }
    assert_eq!(res.diagnostics.restarts.len(), 3);
    for r in &res.diagnostics.restarts {
        assert!(r.converged);
        assert_eq!(r.initial_member_indices.len(), 2);
    }
}
