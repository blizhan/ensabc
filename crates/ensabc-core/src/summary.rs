use std::collections::HashSet;

use crate::kmeans::KMeansExecutionResult;
use crate::preprocess::PreprocessedData;
use crate::{
    ALGORITHM_VERSION, ClusterScenario, CoreError, CoreErrorCode, MemberAssignment,
    ScenarioDiagnostics, ScenarioResult, TaskInput,
};

pub fn assemble_result(
    task: &TaskInput,
    preprocessed: &PreprocessedData,
    kmeans: KMeansExecutionResult,
) -> Result<ScenarioResult, CoreError> {
    let n = preprocessed.n_members;
    let p = preprocessed.n_features;
    let k = task.config.cluster_count;
    let d = task.config.projection_dim;

    // Canonicalize cluster numbering by first member occurrence. Preserve the
    // initialization order in restart summaries; only selected fit outputs move.
    let mut remap = vec![usize::MAX; k];
    let mut next = 0usize;
    for &label in &kmeans.labels {
        if label >= k {
            return Err(CoreError::new(
                CoreErrorCode::NumericalFailure,
                "label outside configured cluster range",
            ));
        }
        if remap[label] == usize::MAX {
            remap[label] = next;
            next += 1;
        }
    }
    if next != k {
        return Err(CoreError::new(
            CoreErrorCode::EmptyCluster,
            "one or more clusters are empty in final labels",
        ));
    }

    let mut labels = Vec::with_capacity(n);
    let mut centers = vec![vec![0.0; d]; k];
    for old in 0..k {
        let new = remap[old];
        centers[new] = kmeans.centers[old].clone();
    }
    for &old_label in &kmeans.labels {
        labels.push(remap[old_label]);
    }

    let mut member_assignments = Vec::with_capacity(n);
    let mut counts = vec![0usize; k];
    let mut members_by_cluster: Vec<Vec<usize>> = vec![Vec::new(); k];
    for (i, member) in task.members.iter().enumerate() {
        let cluster_id = labels[i];
        counts[cluster_id] += 1;
        members_by_cluster[cluster_id].push(i);
        member_assignments.push(MemberAssignment {
            member_index: member.member_index,
            member_id: member.member_id.clone(),
            cluster_id,
        });
    }

    let mut scenarios = Vec::with_capacity(k);
    let mut total_sse = 0.0;
    for cluster_id in 0..k {
        if counts[cluster_id] == 0 {
            return Err(CoreError::new(
                CoreErrorCode::EmptyCluster,
                format!("cluster {} is empty after canonicalization", cluster_id),
            ));
        }
        let members = &members_by_cluster[cluster_id];
        let mut mean_features = vec![0.0; p];
        for &member_idx in members {
            for feature_idx in 0..p {
                mean_features[feature_idx] += preprocessed.original_x[member_idx][feature_idx];
            }
        }
        for value in &mut mean_features {
            *value /= counts[cluster_id] as f64;
            if !value.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite physical mean",
                ));
            }
        }

        let mut cluster_sse = 0.0;
        let mut representative_idx = members[0];
        let mut representative_distance = f64::INFINITY;
        for &member_idx in members {
            let distance = preprocessed.scores[member_idx]
                .iter()
                .zip(&centers[cluster_id])
                .map(|(score, center)| {
                    let delta = score - center;
                    delta * delta
                })
                .sum::<f64>();
            if !distance.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite member-to-center distance",
                )
                .with_member_index(member_idx));
            }
            cluster_sse += distance;
            if !cluster_sse.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite cluster SSE",
                )
                .with_member_index(member_idx));
            }
            if distance < representative_distance {
                representative_distance = distance;
                representative_idx = member_idx;
            }
        }
        total_sse += cluster_sse;
        if !total_sse.is_finite() {
            return Err(CoreError::new(
                CoreErrorCode::NumericalFailure,
                "non-finite total SSE",
            ));
        }

        scenarios.push(ClusterScenario {
            cluster_id,
            member_count: counts[cluster_id],
            member_fraction: counts[cluster_id] as f64 / n as f64,
            representative_member_id: task.members[representative_idx].member_id.clone(),
            cluster_sse,
            center: centers[cluster_id].clone(),
            mean_features,
        });
    }

    let mut restarts = kmeans.restarts;
    for restart in &mut restarts {
        // Only the selected restart's centers are in final canonical order.
        // For every restart, preserve its exact initial order and source indices.
        // Initialization provenance retains the exact sampled center ordering,
        // paired with initial_member_indices, independently of final label normalization.
        let _preserve_initial_order = (restart.restart_index == kmeans.best_restart_index, d, k);
    }

    let diagnostics = ScenarioDiagnostics {
        input_member_count: n,
        feature_count: p,
        projection_dim: d,
        cluster_count: k,
        effective_rank: preprocessed.effective_rank,
        rank_after_scaling: preprocessed.effective_rank,
        normalization_mean: preprocessed.normalization_mean.clone(),
        normalization_scale: preprocessed.normalization_scale.clone(),
        pca_mean: preprocessed.pca_mean.clone(),
        scaled: preprocessed.scaled.clone(),
        singular_values: preprocessed.singular_values.clone(),
        components: preprocessed.components.clone(),
        explained_variance: preprocessed.explained_variance.clone(),
        explained_variance_ratio: preprocessed.explained_variance_ratio.clone(),
        scores: preprocessed.scores.clone(),
        centers: centers.clone(),
        total_sse,
        iterations: kmeans.iterations,
        converged: true,
        stop_reason: kmeans.stop_reason.clone(),
        final_assignment_changed: kmeans.final_assignment_changed,
        restarts,
        selected_restart: kmeans.best_restart_index,
        distance_space: "projected_scores_v1".to_string(),
    };

    let res = ScenarioResult {
        algorithm_version: ALGORITHM_VERSION.to_string(),
        config: task.config.clone(),
        members: member_assignments,
        scenarios,
        diagnostics,
    };

    // Make sure output member identity remains unique and one row per input.
    let ids: HashSet<&str> = res.members.iter().map(|m| m.member_id.as_str()).collect();
    if ids.len() != n || res.members.len() != n {
        return Err(CoreError::new(
            CoreErrorCode::NumericalFailure,
            "result member identities are not one-to-one",
        ));
    }
    Ok(res)
}
