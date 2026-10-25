use std::collections::HashSet;

use nalgebra::DMatrix;
use nalgebra::linalg::SVD;

use crate::{CoreError, CoreErrorCode, InitMethod, TaskInput};

#[cfg(test)]
thread_local! {
    static INJECT_SVD_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub fn with_injected_svd_failure<R>(f: impl FnOnce() -> R) -> R {
    INJECT_SVD_FAILURE.with(|flag| flag.set(true));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    INJECT_SVD_FAILURE.with(|flag| flag.set(false));
    match result {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[derive(Debug, Clone)]
pub struct PreprocessedData {
    pub n_members: usize,
    pub n_features: usize,
    pub effective_rank: usize,
    pub normalization_mean: Vec<f64>,
    pub normalization_scale: Vec<f64>,
    pub pca_mean: Vec<f64>,
    pub scaled: Vec<Vec<f64>>,              // n x p
    pub singular_values: Vec<f64>,          // d
    pub components: Vec<Vec<f64>>,          // d x p
    pub explained_variance: Vec<f64>,       // d
    pub scores: Vec<Vec<f64>>,              // n x d
    pub explained_variance_ratio: Vec<f64>, // d
    pub original_x: Vec<Vec<f64>>,          // n x p
}

pub fn preprocess_and_project(task: &TaskInput) -> Result<PreprocessedData, CoreError> {
    // 1. Semantic configuration checks
    let cfg = &task.config;
    let n = task.members.len();

    if cfg.cluster_count < 2 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidConfig,
            format!("cluster_count must be >= 2, got {}", cfg.cluster_count),
        ));
    }
    if cfg.projection_dim < 1 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidConfig,
            format!("projection_dim must be >= 1, got {}", cfg.projection_dim),
        ));
    }
    if cfg.tolerance < 0.0 || !cfg.tolerance.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::InvalidConfig,
            format!("tolerance must be finite and >= 0, got {}", cfg.tolerance),
        ));
    }
    if cfg.max_iterations == 0 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidConfig,
            "max_iterations must be > 0",
        ));
    }
    if cfg.rank_threshold <= 0.0 || !cfg.rank_threshold.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::InvalidConfig,
            format!(
                "rank_threshold must be finite and > 0, got {}",
                cfg.rank_threshold
            ),
        ));
    }
    if cfg.restarts == 0 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidConfig,
            "restarts must be > 0",
        ));
    }

    // 2. Member validation
    if n == 0 {
        return Err(CoreError::new(
            CoreErrorCode::EmptyMembers,
            "members list is empty",
        ));
    }
    if cfg.cluster_count > n {
        return Err(CoreError::new(
            CoreErrorCode::InvalidConfig,
            format!(
                "cluster_count ({}) cannot exceed member count ({})",
                cfg.cluster_count, n
            ),
        ));
    }

    let mut seen_ids = HashSet::with_capacity(n);
    let mut p_len: Option<usize> = None;
    let mut original_x = Vec::with_capacity(n);

    for (m_idx, member) in task.members.iter().enumerate() {
        if member.member_id.is_empty() {
            return Err(
                CoreError::new(CoreErrorCode::EmptyMemberId, "member_id cannot be empty")
                    .with_member_index(m_idx),
            );
        }
        if !seen_ids.insert(&member.member_id) {
            return Err(CoreError::new(
                CoreErrorCode::DuplicateMemberId,
                format!("duplicate member_id: {}", member.member_id),
            )
            .with_member_index(m_idx));
        }
        if member.features.is_empty() {
            return Err(
                CoreError::new(CoreErrorCode::EmptyFeatures, "features cannot be empty")
                    .with_member_index(m_idx),
            );
        }
        if let Some(expected_p) = p_len {
            if member.features.len() != expected_p {
                return Err(CoreError::new(
                    CoreErrorCode::RaggedFeatures,
                    format!(
                        "feature length mismatch: expected {}, got {}",
                        expected_p,
                        member.features.len()
                    ),
                )
                .with_member_index(m_idx));
            }
        } else {
            p_len = Some(member.features.len());
        }

        for (feat_idx, &val) in member.features.iter().enumerate() {
            if !val.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NonFiniteInput,
                    format!(
                        "non-finite feature value ({}) in member {}",
                        val, member.member_id
                    ),
                )
                .with_member_index(m_idx)
                .with_feature_index(feat_idx));
            }
        }
        original_x.push(member.features.clone());
    }

    let p = p_len.unwrap();

    // Explicit centers check
    match cfg.initialization {
        InitMethod::ExplicitCentersV1 => {
            if cfg.seed != 0 {
                return Err(CoreError::new(
                    CoreErrorCode::InvalidConfig,
                    format!("seed must be 0 for explicit_centers_v1, got {}", cfg.seed),
                ));
            }
            if cfg.initial_centers.len() != cfg.restarts {
                return Err(CoreError::new(
                    CoreErrorCode::InvalidInitialCenters,
                    format!(
                        "explicit initial_centers must have {} sets for {} restarts",
                        cfg.restarts, cfg.restarts
                    ),
                ));
            }
            for (r_idx, r_centers) in cfg.initial_centers.iter().enumerate() {
                if r_centers.len() != cfg.cluster_count {
                    return Err(CoreError::new(
                        CoreErrorCode::InvalidInitialCenters,
                        format!(
                            "restart {} centers count {} != cluster_count {}",
                            r_idx,
                            r_centers.len(),
                            cfg.cluster_count
                        ),
                    )
                    .with_restart_index(r_idx));
                }
                for (c_idx, center) in r_centers.iter().enumerate() {
                    if center.len() != cfg.projection_dim {
                        return Err(CoreError::new(
                            CoreErrorCode::InvalidInitialCenters,
                            format!(
                                "center {} in restart {} dimension {} != projection_dim {}",
                                c_idx,
                                r_idx,
                                center.len(),
                                cfg.projection_dim
                            ),
                        )
                        .with_restart_index(r_idx));
                    }
                    for &coord in center {
                        if !coord.is_finite() {
                            return Err(CoreError::new(
                                CoreErrorCode::InvalidInitialCenters,
                                "non-finite coordinate in explicit initial center",
                            )
                            .with_restart_index(r_idx));
                        }
                    }
                }
            }
        }
        InitMethod::KMeansPlusPlusChaCha8V1 => {
            if !cfg.initial_centers.is_empty() {
                return Err(CoreError::new(
                    CoreErrorCode::InvalidConfig,
                    "initial_centers must be empty when initialization is kmeans++_chacha8_v1",
                ));
            }
        }
    }

    // 3. Check distinct original member vectors
    let mut distinct_rows = HashSet::with_capacity(n);
    for row in &original_x {
        let bits: Vec<u64> = row
            .iter()
            .map(|&x| {
                if x == 0.0 {
                    0.0f64.to_bits()
                } else {
                    x.to_bits()
                }
            })
            .collect();
        distinct_rows.insert(bits);
    }
    if distinct_rows.len() < cfg.cluster_count {
        return Err(CoreError::new(
            CoreErrorCode::InsufficientDistinctVectors,
            format!(
                "distinct member vectors ({}) < cluster_count ({})",
                distinct_rows.len(),
                cfg.cluster_count
            ),
        ));
    }

    // 4. Two-pass population standardization
    let n_f = n as f64;
    let eps = f64::EPSILON;
    let mut means = vec![0.0; p];
    let mut scales = vec![1.0; p];

    // Pass 1: means
    for j in 0..p {
        let mut sum = 0.0;
        for i in 0..n {
            sum += original_x[i][j];
            if !sum.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    format!("non-finite mean accumulator in column {}", j),
                )
                .with_member_index(i)
                .with_feature_index(j));
            }
        }
        means[j] = sum / n_f;
        if !means[j].is_finite() {
            return Err(CoreError::new(
                CoreErrorCode::NumericalFailure,
                format!("non-finite mean in column {}", j),
            ));
        }
    }

    // Pass 2: deviations and corrected variance
    for j in 0..p {
        let mut sum_delta = 0.0;
        let mut sum_delta_sq = 0.0;
        for i in 0..n {
            let delta = original_x[i][j] - means[j];
            sum_delta += delta;
            sum_delta_sq += delta * delta;
            if !delta.is_finite() || !sum_delta.is_finite() || !sum_delta_sq.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    format!("non-finite variance accumulator in column {}", j),
                )
                .with_member_index(i)
                .with_feature_index(j));
            }
        }
        let mut var = (sum_delta_sq - (sum_delta * sum_delta) / n_f) / n_f;

        if !var.is_finite() {
            return Err(CoreError::new(
                CoreErrorCode::NumericalFailure,
                format!("non-finite variance in column {}", j),
            ));
        }

        if var < 0.0 {
            let bound = -64.0 * eps * sum_delta_sq.max(1.0) / n_f;
            if var < bound {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    format!(
                        "negative variance {} exceeds rounding error bound in column {}",
                        var, j
                    ),
                ));
            }
            var = 0.0;
        }

        // Constant feature check
        let is_constant = var <= n_f * eps * var + (n_f * means[j] * eps).powi(2);
        if is_constant || var == 0.0 {
            scales[j] = 1.0;
        } else {
            scales[j] = var.sqrt();
        }
    }

    // Standardized matrix Z (n x p)
    let required_matrix_len = n.checked_mul(p).ok_or_else(|| {
        CoreError::new(
            CoreErrorCode::SizeOverflow,
            "member × feature dimensions overflow usize",
        )
    })?;
    // The public Vec-backed inputs already exist, but reserve derived arrays fallibly
    // before allocation so allocation failure can be reported as a recoverable error.
    let mut standardized = Vec::new();
    standardized
        .try_reserve_exact(required_matrix_len)
        .map_err(|_| {
            CoreError::new(
                CoreErrorCode::AllocationFailure,
                "unable to reserve standardized matrix",
            )
        })?;
    let mut z_data = standardized;
    z_data.resize(required_matrix_len, 0.0);
    for i in 0..n {
        for j in 0..p {
            z_data[i * p + j] = (original_x[i][j] - means[j]) / scales[j];
            if !z_data[i * p + j].is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    format!(
                        "non-finite standardized value at member {}, feature {}",
                        i, j
                    ),
                )
                .with_member_index(i)
                .with_feature_index(j));
            }
        }
    }
    let z_mat = DMatrix::from_row_slice(n, p, &z_data);

    // Rank of Z
    let svd_z = SVD::try_new(z_mat.clone(), false, false, eps, 10000).ok_or_else(|| {
        CoreError::new(
            CoreErrorCode::SvdNotConverged,
            "SVD failed on standardized matrix",
        )
    })?;
    let s_max_z = svd_z.singular_values[0].max(1.0);
    let rank_z = svd_z
        .singular_values
        .iter()
        .filter(|&&s| s > cfg.rank_threshold * s_max_z)
        .count();

    if rank_z < cfg.projection_dim {
        return Err(CoreError::new(
            CoreErrorCode::InsufficientRank,
            format!(
                "effective rank of standardized matrix ({}) is less than projection_dim ({})",
                rank_z, cfg.projection_dim
            ),
        ));
    }

    // 5. PCA: Center Z to produce matrix A
    let mut z_col_means = vec![0.0; p];
    for j in 0..p {
        let col_sum: f64 = (0..n).map(|i| z_data[i * p + j]).sum();
        z_col_means[j] = col_sum / n_f;
    }

    let mut centered_data = Vec::new();
    centered_data
        .try_reserve_exact(required_matrix_len)
        .map_err(|_| {
            CoreError::new(
                CoreErrorCode::AllocationFailure,
                "unable to reserve PCA-centered matrix",
            )
        })?;
    centered_data.resize(required_matrix_len, 0.0);
    let mut a_data = centered_data;
    for i in 0..n {
        for j in 0..p {
            a_data[i * p + j] = z_data[i * p + j] - z_col_means[j];
            if !a_data[i * p + j].is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    format!("non-finite centered value at member {}, feature {}", i, j),
                )
                .with_member_index(i)
                .with_feature_index(j));
            }
        }
    }
    let a_mat = DMatrix::from_row_slice(n, p, &a_data);

    // SVD of A
    #[cfg(test)]
    let inject_svd = INJECT_SVD_FAILURE.with(|flag| flag.get());
    #[cfg(not(test))]
    let inject_svd = false;

    let svd_a = if inject_svd {
        None
    } else {
        SVD::try_new(a_mat.clone(), false, true, eps, 10000)
    };

    let svd_a = svd_a.ok_or_else(|| {
        CoreError::new(
            CoreErrorCode::SvdNotConverged,
            "SVD failed to converge on centered matrix A",
        )
    })?;

    let s_a = &svd_a.singular_values;
    let s_max_a = s_a[0].max(1.0);
    let rank_a = s_a
        .iter()
        .filter(|&&s| s > cfg.rank_threshold * s_max_a)
        .count();

    if rank_a < cfg.projection_dim {
        return Err(CoreError::new(
            CoreErrorCode::InsufficientRank,
            format!(
                "effective rank of centered matrix ({}) is less than projection_dim ({})",
                rank_a, cfg.projection_dim
            ),
        ));
    }

    let d = cfg.projection_dim;

    // Ambiguous subspace check
    if s_a.len() > d {
        let gap = (s_a[d - 1] - s_a[d]).abs();
        if gap <= cfg.rank_threshold * s_max_a {
            return Err(CoreError::new(
                CoreErrorCode::AmbiguousSubspace,
                format!(
                    "singular value gap between component {} and {} ({}) is <= rank_threshold * s_max",
                    d - 1,
                    d,
                    gap
                ),
            ));
        }
    }

    let v_t = svd_a.v_t.as_ref().ok_or_else(|| {
        CoreError::new(CoreErrorCode::NumericalFailure, "V^T not computed in SVD")
    })?;

    // Extract first d components and sign-normalize
    let mut components = vec![vec![0.0; p]; d];
    for c in 0..d {
        for j in 0..p {
            components[c][j] = v_t[(c, j)];
        }
        // Sign normalization: find maximum absolute loading
        let mut max_abs = 0.0;
        let mut max_idx = 0;
        for (j, &val) in components[c].iter().enumerate() {
            let abs_val = val.abs();
            if abs_val > max_abs {
                max_abs = abs_val;
                max_idx = j;
            }
        }
        if components[c][max_idx] < 0.0 {
            for j in 0..p {
                components[c][j] = -components[c][j];
            }
        }
    }

    // Compute scores = A * components^T
    let score_len = n.checked_mul(d).ok_or_else(|| {
        CoreError::new(
            CoreErrorCode::SizeOverflow,
            "member × projection dimensions overflow usize",
        )
    })?;
    let mut scores = Vec::new();
    scores.try_reserve_exact(n).map_err(|_| {
        CoreError::new(
            CoreErrorCode::AllocationFailure,
            "unable to reserve projected score rows",
        )
    })?;
    for _ in 0..n {
        let mut row = Vec::new();
        row.try_reserve_exact(d).map_err(|_| {
            CoreError::new(
                CoreErrorCode::AllocationFailure,
                "unable to reserve projected score row",
            )
        })?;
        row.resize(d, 0.0);
        scores.push(row);
    }
    debug_assert_eq!(score_len, scores.iter().map(Vec::len).sum::<usize>());
    for i in 0..n {
        for c in 0..d {
            let mut dot = 0.0;
            for j in 0..p {
                dot += a_data[i * p + j] * components[c][j];
            }
            if !dot.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite projected score",
                )
                .with_member_index(i));
            }
            scores[i][c] = dot;
        }
    }

    // Explained variance ratio
    let total_variance: f64 = s_a.iter().map(|&s| s * s).sum();
    let mut evr = vec![0.0; d];
    for c in 0..d {
        evr[c] = (s_a[c] * s_a[c]) / total_variance;
    }

    // 6. Distinct projected vectors check
    let mut distinct_proj = HashSet::with_capacity(n);
    for row in &scores {
        let bits: Vec<u64> = row
            .iter()
            .map(|&x| {
                if x == 0.0 {
                    0.0f64.to_bits()
                } else {
                    x.to_bits()
                }
            })
            .collect();
        distinct_proj.insert(bits);
    }
    if distinct_proj.len() < cfg.cluster_count {
        return Err(CoreError::new(
            CoreErrorCode::InsufficientProjectedVectors,
            format!(
                "distinct projected vectors ({}) < cluster_count ({})",
                distinct_proj.len(),
                cfg.cluster_count
            ),
        ));
    }

    let singular_values: Vec<f64> = s_a.as_slice()[0..d].to_vec();
    let explained_variance: Vec<f64> = singular_values
        .iter()
        .map(|&s| s * s / (n_f - 1.0))
        .collect();
    let pca_mean = z_col_means;
    let mut scaled = Vec::with_capacity(n);
    for i in 0..n {
        scaled.push(z_data[i * p..(i + 1) * p].to_vec());
    }

    Ok(PreprocessedData {
        n_members: n,
        n_features: p,
        effective_rank: rank_a,
        normalization_mean: means,
        normalization_scale: scales,
        pca_mean,
        scaled,
        singular_values,
        components,
        explained_variance,
        scores,
        explained_variance_ratio: evr,
        original_x,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InitMethod, MemberInput, ScenarioConfig, StopRule};

    fn make_test_task(data: &[(&str, &[f64])], d: usize, k: usize) -> TaskInput {
        let members: Vec<MemberInput> = data
            .iter()
            .enumerate()
            .map(|(i, (id, feats))| MemberInput {
                member_index: i,
                member_id: id.to_string(),
                features: feats.to_vec(),
            })
            .collect();
        TaskInput {
            config: ScenarioConfig {
                projection_dim: d,
                cluster_count: k,
                initialization: InitMethod::KMeansPlusPlusChaCha8V1,
                seed: 0,
                restarts: 1,
                initial_centers: Vec::new(),
                stopping_rule: StopRule::SklearnVarianceScaledV1,
                tolerance: 1e-4,
                max_iterations: 300,
                rank_threshold: 1e-10,
            },
            members,
        }
    }

    #[test]
    fn test_constant_feature_handling() {
        // Column 1 is strictly constant (999.0), Column 0 and Column 2 are independent
        let task = make_test_task(
            &[
                ("m1", &[1.0, 999.0, 10.0]),
                ("m2", &[2.0, 999.0, 25.0]),
                ("m3", &[3.0, 999.0, 15.0]),
                ("m4", &[4.0, 999.0, 40.0]),
            ],
            2,
            2,
        );
        let res = preprocess_and_project(&task).unwrap();
        assert_eq!(res.effective_rank, 2);
        assert_eq!(res.components.len(), 2);
    }

    #[test]
    fn test_insufficient_rank() {
        // Col0 and Col1 are proportional, Col2 constant => effective rank is 1, but d=2
        let task = make_test_task(
            &[
                ("m1", &[1.0, 2.0, 5.0]),
                ("m2", &[2.0, 4.0, 5.0]),
                ("m3", &[3.0, 6.0, 5.0]),
                ("m4", &[4.0, 8.0, 5.0]),
            ],
            2,
            2,
        );
        let err = preprocess_and_project(&task).unwrap_err();
        assert_eq!(err.code, CoreErrorCode::InsufficientRank);
    }

    #[test]
    fn test_ambiguous_subspace() {
        // Perfectly symmetric cross where singular values s[0] and s[1] are identical
        // If d=1, s[0] == s[1], gap is 0 <= rank_threshold * s_max => AMBIGUOUS_SUBSPACE
        let task = make_test_task(
            &[
                ("m1", &[1.0, 0.0]),
                ("m2", &[-1.0, 0.0]),
                ("m3", &[0.0, 1.0]),
                ("m4", &[0.0, -1.0]),
            ],
            1,
            2,
        );
        let err = preprocess_and_project(&task).unwrap_err();
        assert_eq!(err.code, CoreErrorCode::AmbiguousSubspace);
    }

    #[test]
    fn test_svd_failure_injection() {
        let task = make_test_task(
            &[
                ("m1", &[10.0, 1.0]),
                ("m2", &[20.0, 2.0]),
                ("m3", &[30.0, 4.0]),
                ("m4", &[40.0, 5.0]),
            ],
            2,
            2,
        );
        let err = with_injected_svd_failure(|| preprocess_and_project(&task).unwrap_err());
        assert_eq!(err.code, CoreErrorCode::SvdNotConverged);
    }
}
