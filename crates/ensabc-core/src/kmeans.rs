use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::preprocess::PreprocessedData;
use crate::{CoreError, CoreErrorCode, InitMethod, RestartSummary, ScenarioConfig, TaskInput};

#[derive(Debug, Clone)]
pub struct KMeansExecutionResult {
    pub best_restart_index: usize,
    pub labels: Vec<usize>,     // n
    pub centers: Vec<Vec<f64>>, // k x d
    pub restarts: Vec<RestartSummary>,
    pub total_sse: f64,
    pub iterations: usize,
    pub stop_reason: String,
    pub final_assignment_changed: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct LloydTraceEvent {
    pub iteration: usize,
    pub event_type: &'static str,
    pub counts_as_iteration: bool,
    pub labels: Vec<usize>,
    pub centers: Vec<Vec<f64>>,
    pub sse: f64,
    pub shift: Option<f64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct LloydRun {
    pub labels: Vec<usize>,
    pub centers: Vec<Vec<f64>>,
    pub iterations: usize,
    pub total_sse: f64,
    pub stop_reason: &'static str,
    pub final_assignment_changed: bool,
    pub events: Vec<LloydTraceEvent>,
}

fn distance_squared(a: &[f64], b: &[f64]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| {
            let delta = x - y;
            delta * delta
        })
        .sum()
}

fn assign_labels(
    scores: &[Vec<f64>],
    centers: &[Vec<f64>],
) -> Result<(Vec<usize>, Vec<usize>, f64), CoreError> {
    let k = centers.len();
    let mut labels = Vec::with_capacity(scores.len());
    let mut counts = vec![0usize; k];
    let mut inertia = 0.0;

    for (member_index, point) in scores.iter().enumerate() {
        let mut best_cluster = 0;
        let mut best_distance = distance_squared(point, &centers[0]);
        if !best_distance.is_finite() {
            return Err(CoreError::new(
                CoreErrorCode::NumericalFailure,
                "non-finite squared distance",
            )
            .with_member_index(member_index));
        }
        for cluster_index in 1..k {
            let distance = distance_squared(point, &centers[cluster_index]);
            if !distance.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite squared distance",
                )
                .with_member_index(member_index));
            }
            // Strictly less preserves the smallest center index on exact ties.
            if distance < best_distance {
                best_distance = distance;
                best_cluster = cluster_index;
            }
        }
        labels.push(best_cluster);
        counts[best_cluster] = counts[best_cluster].checked_add(1).ok_or_else(|| {
            CoreError::new(CoreErrorCode::SizeOverflow, "cluster member count overflow")
        })?;
        inertia += best_distance;
        if !inertia.is_finite() {
            return Err(CoreError::new(
                CoreErrorCode::NumericalFailure,
                "non-finite assignment SSE",
            )
            .with_member_index(member_index));
        }
    }
    Ok((labels, counts, inertia))
}

fn update_centers(
    scores: &[Vec<f64>],
    labels: &[usize],
    counts: &[usize],
    k: usize,
    d: usize,
    restart_index: usize,
    iteration: usize,
) -> Result<Vec<Vec<f64>>, CoreError> {
    let mut centers = vec![vec![0.0; d]; k];
    for (i, point) in scores.iter().enumerate() {
        let label = labels[i];
        for j in 0..d {
            centers[label][j] += point[j];
            if !centers[label][j].is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite center accumulator",
                )
                .with_member_index(i)
                .with_restart_index(restart_index)
                .with_iteration(iteration));
            }
        }
    }
    for cluster_index in 0..k {
        if counts[cluster_index] == 0 {
            return Err(CoreError::new(
                CoreErrorCode::EmptyCluster,
                format!("cluster {} is empty", cluster_index),
            )
            .with_restart_index(restart_index)
            .with_iteration(iteration));
        }
        for coordinate in 0..d {
            centers[cluster_index][coordinate] /= counts[cluster_index] as f64;
            if !centers[cluster_index][coordinate].is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite updated center",
                )
                .with_restart_index(restart_index)
                .with_iteration(iteration));
            }
        }
    }
    Ok(centers)
}

fn initialize_kmeans_plus_plus(
    scores: &[Vec<f64>],
    config: &ScenarioConfig,
    rng: &mut ChaCha8Rng,
    restart_index: usize,
) -> Result<(Vec<Vec<f64>>, Vec<usize>), CoreError> {
    let n = scores.len();
    let k = config.cluster_count;
    if n < k {
        return Err(CoreError::new(
            CoreErrorCode::InvalidConfig,
            "cluster_count exceeds number of scores",
        )
        .with_restart_index(restart_index));
    }

    let u = (rng.next_u64() >> 11) as f64 / ((1u64 << 53) as f64);
    let first_index = ((u * n as f64).floor() as usize).min(n - 1);
    let mut indices = vec![first_index];

    while indices.len() < k {
        let mut weights = Vec::with_capacity(n);
        let mut total_weight = 0.0;
        for point in scores {
            let mut min_distance = f64::INFINITY;
            for &selected_index in &indices {
                let dist = distance_squared(point, &scores[selected_index]);
                if dist < min_distance {
                    min_distance = dist;
                }
            }
            if !min_distance.is_finite() || min_distance < 0.0 {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "invalid kmeans++ D² weight",
                )
                .with_restart_index(restart_index));
            }
            total_weight += min_distance;
            if !total_weight.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite sum of kmeans++ D² weights",
                )
                .with_restart_index(restart_index));
            }
            weights.push(min_distance);
        }

        if total_weight <= 0.0 {
            return Err(CoreError::new(
                CoreErrorCode::InsufficientDistinctVectors,
                "kmeans++ has no positive remaining D² weight",
            )
            .with_restart_index(restart_index));
        }
        let u = (rng.next_u64() >> 11) as f64 / ((1u64 << 53) as f64);
        let target = u * total_weight;
        let mut cumulative = 0.0;
        let mut choice = None;
        let mut last_positive = None;
        for (index, weight) in weights.iter().copied().enumerate() {
            if weight > 0.0 {
                last_positive = Some(index);
            }
            cumulative += weight;
            if cumulative > target && weight > 0.0 {
                choice = Some(index);
                break;
            }
        }
        let chosen = choice.or(last_positive).ok_or_else(|| {
            CoreError::new(
                CoreErrorCode::InsufficientDistinctVectors,
                "kmeans++ could not select a positive-weight member",
            )
            .with_restart_index(restart_index)
        })?;
        if indices.contains(&chosen) {
            return Err(CoreError::new(
                CoreErrorCode::NumericalFailure,
                "kmeans++ selected an already chosen zero-distance member",
            )
            .with_restart_index(restart_index));
        }
        indices.push(chosen);
    }

    let centers = indices.iter().map(|&index| scores[index].clone()).collect();
    Ok((centers, indices))
}

pub(crate) fn run_lloyd(
    scores: &[Vec<f64>],
    initial_centers: Vec<Vec<f64>>,
    config: &ScenarioConfig,
    restart_index: usize,
    collect_trace: bool,
) -> Result<LloydRun, CoreError> {
    let n = scores.len();
    let k = config.cluster_count;
    let d = config.projection_dim;
    let mut centers = initial_centers;
    let mut coordinate_variances = Vec::with_capacity(d);
    for coordinate in 0..d {
        let mut sum = 0.0;
        for row in scores {
            sum += row[coordinate];
            if !sum.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite score mean accumulator",
                )
                .with_restart_index(restart_index));
            }
        }
        let mean = sum / n as f64;
        let mut variance_sum = 0.0;
        for row in scores {
            let delta = row[coordinate] - mean;
            variance_sum += delta * delta;
            if !variance_sum.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "non-finite score variance accumulator",
                )
                .with_restart_index(restart_index));
            }
        }
        coordinate_variances.push(variance_sum / n as f64);
    }
    let population_variance_mean = coordinate_variances.iter().sum::<f64>() / d as f64;
    if !population_variance_mean.is_finite() {
        return Err(
            CoreError::new(CoreErrorCode::NumericalFailure, "non-finite score variance")
                .with_restart_index(restart_index),
        );
    }
    let tolerance_threshold = config.tolerance * population_variance_mean;
    if !tolerance_threshold.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::NumericalFailure,
            "non-finite variance-scaled tolerance",
        )
        .with_restart_index(restart_index));
    }

    let mut previous_labels: Option<Vec<usize>> = None;
    let mut events = Vec::new();
    let mut converged = false;
    let mut stop_reason = "iteration_limit";
    let mut iterations = 0;

    for iteration in 1..=config.max_iterations {
        let (labels, counts, _assignment_sse) = assign_labels(scores, &centers).map_err(|e| {
            e.with_restart_index(restart_index)
                .with_iteration(iteration)
        })?;
        let new_centers = update_centers(scores, &labels, &counts, k, d, restart_index, iteration)?;

        let mut shift = 0.0;
        for cluster_index in 0..k {
            shift += distance_squared(&centers[cluster_index], &new_centers[cluster_index]);
        }
        if !shift.is_finite() {
            return Err(
                CoreError::new(CoreErrorCode::NumericalFailure, "non-finite center shift")
                    .with_restart_index(restart_index)
                    .with_iteration(iteration),
            );
        }

        let stable = previous_labels
            .as_ref()
            .map(|previous| previous == &labels)
            .unwrap_or(false);
        centers = new_centers;
        iterations = iteration;

        // The stop order is labels_stable first, then center_shift. A final
        // assignment is performed below only after the stopping decision.
        let (labels_after_update, counts_after_update, _sse_after_update) =
            assign_labels(scores, &centers).map_err(|e| {
                e.with_restart_index(restart_index)
                    .with_iteration(iteration)
            })?;
        if counts_after_update.iter().any(|&count| count == 0) {
            let empty = counts_after_update
                .iter()
                .position(|&count| count == 0)
                .unwrap();
            return Err(CoreError::new(
                CoreErrorCode::EmptyCluster,
                format!("cluster {} is empty after center update", empty),
            )
            .with_restart_index(restart_index)
            .with_iteration(iteration));
        }
        if collect_trace {
            events.push(LloydTraceEvent {
                iteration,
                event_type: "update",
                counts_as_iteration: true,
                labels: labels.clone(),
                centers: centers.clone(),
                sse: scores
                    .iter()
                    .zip(&labels)
                    .map(|(point, &label)| distance_squared(point, &centers[label]))
                    .sum(),
                shift: Some(shift),
            });
        }
        previous_labels = Some(labels);

        // sklearn's loop compares the current labels with the previous E-step,
        // then uses center shift if labels did not stabilize.
        if stable {
            converged = true;
            stop_reason = "labels_stable";
            break;
        }
        if shift <= tolerance_threshold {
            converged = true;
            stop_reason = "center_shift";
            break;
        }
        let _ = labels_after_update;
    }

    if !converged {
        return Err(CoreError::new(
            CoreErrorCode::NotConverged,
            "reached max_iterations without convergence",
        )
        .with_restart_index(restart_index)
        .with_iteration(config.max_iterations));
    }

    // Final E-step: returned labels correspond to returned updated centers.
    let (final_labels, final_counts, total_sse) = assign_labels(scores, &centers).map_err(|e| {
        e.with_restart_index(restart_index)
            .with_iteration(iterations + 1)
    })?;
    if final_counts.iter().any(|&count| count == 0) {
        let empty = final_counts.iter().position(|&count| count == 0).unwrap();
        return Err(CoreError::new(
            CoreErrorCode::EmptyCluster,
            format!("cluster {} is empty in final assignment", empty),
        )
        .with_restart_index(restart_index)
        .with_iteration(iterations + 1));
    }
    let final_assignment_changed = previous_labels.as_ref() != Some(&final_labels);
    if collect_trace && stop_reason != "labels_stable" {
        events.push(LloydTraceEvent {
            iteration: iterations + 1,
            event_type: "assignment_only",
            counts_as_iteration: false,
            labels: final_labels.clone(),
            centers: centers.clone(),
            sse: total_sse,
            shift: None,
        });
    }

    Ok(LloydRun {
        labels: final_labels,
        centers,
        iterations,
        total_sse,
        stop_reason,
        final_assignment_changed,
        events,
    })
}

fn run_restart(
    scores: &[Vec<f64>],
    config: &ScenarioConfig,
    rng: &mut ChaCha8Rng,
    restart_index: usize,
) -> Result<(LloydRun, Vec<usize>, Vec<Vec<f64>>), CoreError> {
    let (initial_centers, initial_member_indices) = match config.initialization {
        InitMethod::KMeansPlusPlusChaCha8V1 => {
            initialize_kmeans_plus_plus(scores, config, rng, restart_index)?
        }
        InitMethod::ExplicitCentersV1 => {
            (config.initial_centers[restart_index].clone(), Vec::new())
        }
    };
    let recorded_initial_centers = initial_centers.clone();
    let run = run_lloyd(scores, initial_centers, config, restart_index, false)?;
    Ok((run, initial_member_indices, recorded_initial_centers))
}

pub fn run_kmeans(
    task: &TaskInput,
    preprocessed: &PreprocessedData,
) -> Result<KMeansExecutionResult, CoreError> {
    let cfg = &task.config;
    let mut summaries = Vec::with_capacity(cfg.restarts);
    let mut successful_runs: Vec<(usize, LloydRun)> = Vec::with_capacity(cfg.restarts);

    let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);
    for restart_index in 0..cfg.restarts {
        let (run, initial_member_indices, initial_centers) =
            run_restart(&preprocessed.scores, cfg, &mut rng, restart_index)?;
        summaries.push(RestartSummary {
            restart_index,
            converged: true,
            iterations: run.iterations,
            total_sse: run.total_sse,
            initial_centers,
            initial_member_indices,
            stop_reason: run.stop_reason.to_string(),
        });
        successful_runs.push((restart_index, run));
    }

    let (best_restart_index, best_run) = successful_runs
        .into_iter()
        .min_by(|(left_index, left), (right_index, right)| {
            left.total_sse
                .total_cmp(&right.total_sse)
                .then_with(|| left_index.cmp(right_index))
        })
        .ok_or_else(|| {
            CoreError::new(CoreErrorCode::InvalidConfig, "no restarts were configured")
        })?;

    Ok(KMeansExecutionResult {
        best_restart_index,
        labels: best_run.labels,
        centers: best_run.centers,
        restarts: summaries,
        total_sse: best_run.total_sse,
        iterations: best_run.iterations,
        stop_reason: best_run.stop_reason.to_string(),
        final_assignment_changed: best_run.final_assignment_changed,
    })
}
