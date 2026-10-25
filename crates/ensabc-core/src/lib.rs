//! Pure Rust computational core for ensemble scenario generation (PCA + K-means).
//!
//! Exposes the single public entry point `cluster(&TaskInput) -> Result<ScenarioResult, CoreError>`.
//! Does not depend on DuckDB, Arrow, Python, external BLAS/LAPACK, or filesystem I/O.

pub mod kmeans;
pub mod preprocess;
pub mod summary;

pub const ALGORITHM_VERSION: &str = "m1.native.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitMethod {
    KMeansPlusPlusChaCha8V1,
    ExplicitCentersV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopRule {
    SklearnVarianceScaledV1,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioConfig {
    pub projection_dim: usize,
    pub cluster_count: usize,
    pub initialization: InitMethod,
    pub seed: u64,
    pub restarts: usize,
    pub initial_centers: Vec<Vec<Vec<f64>>>,
    pub stopping_rule: StopRule,
    pub tolerance: f64,
    pub max_iterations: usize,
    pub rank_threshold: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemberInput {
    pub member_index: usize,
    pub member_id: String,
    pub features: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskInput {
    pub config: ScenarioConfig,
    pub members: Vec<MemberInput>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemberAssignment {
    pub member_index: usize,
    pub member_id: String,
    pub cluster_id: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterScenario {
    pub cluster_id: usize,
    pub member_count: usize,
    pub member_fraction: f64,
    pub representative_member_id: String,
    pub cluster_sse: f64,
    pub center: Vec<f64>,
    pub mean_features: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RestartSummary {
    pub restart_index: usize,
    pub converged: bool,
    pub iterations: usize,
    pub total_sse: f64,
    pub initial_centers: Vec<Vec<f64>>,
    pub initial_member_indices: Vec<usize>,
    pub stop_reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioDiagnostics {
    pub input_member_count: usize,
    pub feature_count: usize,
    pub projection_dim: usize,
    pub cluster_count: usize,
    pub effective_rank: usize,
    pub rank_after_scaling: usize,
    pub normalization_mean: Vec<f64>,
    pub normalization_scale: Vec<f64>,
    pub pca_mean: Vec<f64>,
    pub scaled: Vec<Vec<f64>>,
    pub singular_values: Vec<f64>,
    pub components: Vec<Vec<f64>>,
    pub explained_variance: Vec<f64>,
    pub explained_variance_ratio: Vec<f64>,
    pub scores: Vec<Vec<f64>>,
    pub centers: Vec<Vec<f64>>,
    pub total_sse: f64,
    pub iterations: usize,
    pub converged: bool,
    pub stop_reason: String,
    pub final_assignment_changed: bool,
    pub restarts: Vec<RestartSummary>,
    pub selected_restart: usize,
    pub distance_space: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioResult {
    pub algorithm_version: String,
    pub config: ScenarioConfig,
    pub members: Vec<MemberAssignment>,
    pub scenarios: Vec<ClusterScenario>,
    pub diagnostics: ScenarioDiagnostics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreErrorCode {
    EmptyMembers,
    EmptyMemberId,
    DuplicateMemberId,
    EmptyFeatures,
    RaggedFeatures,
    NonFiniteInput,
    InvalidConfig,
    InvalidInitialCenters,
    InsufficientDistinctVectors,
    InsufficientProjectedVectors,
    InsufficientRank,
    AmbiguousSubspace,
    SvdNotConverged,
    EmptyCluster,
    NotConverged,
    NumericalFailure,
    SizeOverflow,
    AllocationFailure,
}

impl CoreErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EmptyMembers => "EMPTY_MEMBERS",
            Self::EmptyMemberId => "EMPTY_MEMBER_ID",
            Self::DuplicateMemberId => "DUPLICATE_MEMBER_ID",
            Self::EmptyFeatures => "EMPTY_FEATURES",
            Self::RaggedFeatures => "RAGGED_FEATURES",
            Self::NonFiniteInput => "NON_FINITE_INPUT",
            Self::InvalidConfig => "INVALID_CONFIG",
            Self::InvalidInitialCenters => "INVALID_INITIAL_CENTERS",
            Self::InsufficientDistinctVectors => "INSUFFICIENT_DISTINCT_VECTORS",
            Self::InsufficientProjectedVectors => "INSUFFICIENT_PROJECTED_VECTORS",
            Self::InsufficientRank => "INSUFFICIENT_RANK",
            Self::AmbiguousSubspace => "AMBIGUOUS_SUBSPACE",
            Self::SvdNotConverged => "SVD_NOT_CONVERGED",
            Self::EmptyCluster => "EMPTY_CLUSTER",
            Self::NotConverged => "NOT_CONVERGED",
            Self::NumericalFailure => "NUMERICAL_FAILURE",
            Self::SizeOverflow => "SIZE_OVERFLOW",
            Self::AllocationFailure => "ALLOCATION_FAILURE",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoreError {
    pub code: CoreErrorCode,
    pub message: String,
    pub member_index: Option<usize>,
    pub feature_index: Option<usize>,
    pub restart_index: Option<usize>,
    pub iteration: Option<usize>,
}

impl CoreError {
    pub fn new(code: CoreErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            member_index: None,
            feature_index: None,
            restart_index: None,
            iteration: None,
        }
    }

    pub fn with_member_index(mut self, index: usize) -> Self {
        self.member_index = Some(index);
        self
    }

    pub fn with_feature_index(mut self, index: usize) -> Self {
        self.feature_index = Some(index);
        self
    }

    pub fn with_restart_index(mut self, index: usize) -> Self {
        self.restart_index = Some(index);
        self
    }

    pub fn with_iteration(mut self, iteration: usize) -> Self {
        self.iteration = Some(iteration);
        self
    }
}

impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code.as_str(), self.message)?;
        if let Some(m) = self.member_index {
            write!(f, " (member_index={})", m)?;
        }
        if let Some(feat) = self.feature_index {
            write!(f, " (feature_index={})", feat)?;
        }
        if let Some(r) = self.restart_index {
            write!(f, " (restart_index={})", r)?;
        }
        if let Some(it) = self.iteration {
            write!(f, " (iteration={})", it)?;
        }
        Ok(())
    }
}

impl std::error::Error for CoreError {}

/// Main entry point for single-batch scenario computation.
pub fn cluster(task: &TaskInput) -> Result<ScenarioResult, CoreError> {
    let preprocessed = preprocess::preprocess_and_project(task)?;
    let kmeans_result = kmeans::run_kmeans(task, &preprocessed)?;
    summary::assemble_result(task, &preprocessed, kmeans_result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::path::Path;

    #[derive(serde::Deserialize)]
    struct TraceInput {
        scores: Vec<Vec<f64>>,
        initial_centers: Vec<Vec<f64>>,
        #[serde(default = "default_tol")]
        tolerance: f64,
        #[serde(default = "default_max_iter")]
        max_iterations: usize,
        cluster_count: usize,
        projection_dim: usize,
    }

    fn default_tol() -> f64 {
        1e-4
    }
    fn default_max_iter() -> usize {
        300
    }

    #[derive(serde::Serialize)]
    struct TraceOutput {
        schema_version: String,
        events: Vec<TraceEventDto>,
        final_iteration_count: usize,
        stop_reason: String,
    }

    #[derive(serde::Serialize)]
    struct TraceEventDto {
        iteration: usize,
        event_type: String,
        counts_as_iteration: bool,
        labels: Vec<usize>,
        centers: Vec<Vec<f64>>,
        sse: f64,
        shift: Option<f64>,
    }

    #[test]
    #[ignore]
    fn m1_shared_trace() {
        let input_path = env::var("ENSABC_M1_TRACE_INPUT")
            .expect("ENSABC_M1_TRACE_INPUT environment variable must be set");
        let output_path = env::var("ENSABC_M1_TRACE_OUTPUT")
            .expect("ENSABC_M1_TRACE_OUTPUT environment variable must be set");

        let input_bytes = fs::read(&input_path)
            .unwrap_or_else(|e| panic!("Failed reading trace input from {}: {}", input_path, e));
        let trace_in: TraceInput = serde_json::from_slice(&input_bytes)
            .unwrap_or_else(|e| panic!("Failed parsing trace input JSON: {}", e));

        let config = ScenarioConfig {
            projection_dim: trace_in.projection_dim,
            cluster_count: trace_in.cluster_count,
            initialization: InitMethod::ExplicitCentersV1,
            seed: 0,
            restarts: 1,
            initial_centers: vec![trace_in.initial_centers.clone()],
            stopping_rule: StopRule::SklearnVarianceScaledV1,
            tolerance: trace_in.tolerance,
            max_iterations: trace_in.max_iterations,
            rank_threshold: 1e-10,
        };

        let plain = kmeans::run_lloyd(
            &trace_in.scores,
            trace_in.initial_centers.clone(),
            &config,
            0,
            false,
        )
        .expect("run_lloyd failed without observer");
        let run = kmeans::run_lloyd(&trace_in.scores, trace_in.initial_centers, &config, 0, true)
            .expect("run_lloyd failed during shared trace extraction");
        assert!(plain.events.is_empty());
        assert_eq!(plain.labels, run.labels);
        assert_eq!(plain.centers, run.centers);
        assert_eq!(plain.total_sse, run.total_sse);
        assert_eq!(plain.iterations, run.iterations);
        assert_eq!(plain.stop_reason, run.stop_reason);
        assert_eq!(plain.final_assignment_changed, run.final_assignment_changed);

        let output_dto = TraceOutput {
            schema_version: "m1.trace.v1".to_string(),
            events: run
                .events
                .iter()
                .map(|e| TraceEventDto {
                    iteration: e.iteration,
                    event_type: e.event_type.to_string(),
                    counts_as_iteration: e.counts_as_iteration,
                    labels: e.labels.clone(),
                    centers: e.centers.clone(),
                    sse: e.sse,
                    shift: e.shift,
                })
                .collect(),
            final_iteration_count: run.iterations,
            stop_reason: run.stop_reason.to_string(),
        };

        let out_json = serde_json::to_string_pretty(&output_dto)
            .expect("Serialization of trace output failed");
        let out_p = Path::new(&output_path);
        if let Some(parent) = out_p.parent() {
            fs::create_dir_all(parent).expect("Failed to create parent directory for trace output");
        }
        fs::write(out_p, format!("{}\n", out_json)).expect("Failed to write trace output file");
    }
}
