//! Development and verification runner for M1 scenario computation.
//!
//! Usage:
//!     cargo run --locked --release -p ensabc-core --example m1_runner -- \
//!         --input /path/task.json --output /path/result.json

use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;

use serde::{Deserialize, Serialize};

use ensabc_core::{
    ALGORITHM_VERSION, CoreError, InitMethod, MemberInput, ScenarioConfig, StopRule, TaskInput,
    cluster,
};

#[derive(Debug, Deserialize)]
struct TaskEnvelope {
    schema_version: String,
    #[serde(default)]
    #[allow(dead_code)]
    task_context: Option<serde_json::Value>,
    config: ConfigDto,
    members: Vec<MemberInputDto>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ConfigDto {
    schema_version: String,
    algorithm_version: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    comparison_rules_path: Option<String>,
    projection_dim: usize,
    cluster_count: usize,
    initialization: String,
    seed: u64,
    restarts: usize,
    initial_centers: Vec<Vec<Vec<f64>>>,
    stopping_rule: String,
    tolerance: f64,
    max_iterations: usize,
    rank_threshold: f64,
}

#[derive(Debug, Deserialize)]
struct MemberInputDto {
    member_index: usize,
    member_id: String,
    features: Vec<f64>,
}

#[derive(Debug, Serialize)]
struct ResultEnvelope {
    schema_version: String,
    algorithm_version: String,
    config: ConfigDto,
    members: Vec<MemberAssignmentDto>,
    scenarios: Vec<ClusterScenarioDto>,
    diagnostics: DiagnosticsDto,
}

#[derive(Debug, Serialize)]
struct MemberAssignmentDto {
    member_index: usize,
    member_id: String,
    cluster_id: usize,
}

#[derive(Debug, Serialize)]
struct ClusterScenarioDto {
    cluster_id: usize,
    member_count: usize,
    member_fraction: f64,
    representative_member_id: String,
    cluster_sse: f64,
    center: Vec<f64>,
    mean_features: Vec<f64>,
}

#[derive(Debug, Serialize)]
struct DiagnosticsDto {
    effective_rank: usize,
    rank_after_scaling: usize,
    normalization_mean: Vec<f64>,
    normalization_scale: Vec<f64>,
    components: Vec<Vec<f64>>,
    explained_variance_ratio: Vec<f64>,
    scores: Vec<Vec<f64>>,
    total_sse: f64,
    restarts: Vec<RestartSummaryDto>,
    selected_restart: usize,
}

#[derive(Debug, Serialize)]
struct RestartSummaryDto {
    restart_index: usize,
    converged: bool,
    iterations: usize,
    total_sse: f64,
    initial_centers: Vec<Vec<f64>>,
    initial_member_indices: Vec<usize>,
    stop_reason: String,
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!(
        "Usage: m1_runner --input <PATH> --output <PATH>\n\
         Both arguments are required. Unknown flags are rejected."
    );
    process::exit(code);
}

fn parse_args() -> (PathBuf, PathBuf) {
    let mut args = env::args().skip(1);
    let mut input_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => {
                let val = args.next().unwrap_or_else(|| print_usage_and_exit(1));
                input_path = Some(PathBuf::from(val));
            }
            "--output" => {
                let val = args.next().unwrap_or_else(|| print_usage_and_exit(1));
                output_path = Some(PathBuf::from(val));
            }
            "-h" | "--help" => print_usage_and_exit(0),
            unknown => {
                eprintln!("[UNKNOWN_ARG] Unknown flag: {}", unknown);
                process::exit(1);
            }
        }
    }

    match (input_path, output_path) {
        (Some(i), Some(o)) => (i, o),
        _ => print_usage_and_exit(1),
    }
}

fn run() -> Result<(), CoreError> {
    let (input_path, output_path) = parse_args();

    let input_bytes = fs::read(&input_path).map_err(|e| {
        CoreError::new(
            ensabc_core::CoreErrorCode::NumericalFailure,
            format!("Cannot read input file {:?}: {}", input_path, e),
        )
    })?;

    let envelope: TaskEnvelope = serde_json::from_slice(&input_bytes).map_err(|e| {
        CoreError::new(
            ensabc_core::CoreErrorCode::NumericalFailure,
            format!("Invalid JSON in task envelope: {}", e),
        )
    })?;

    if envelope.schema_version != "m1.task.v1" {
        return Err(CoreError::new(
            ensabc_core::CoreErrorCode::InvalidConfig,
            format!(
                "Unsupported schema_version: expected m1.task.v1, got {}",
                envelope.schema_version
            ),
        ));
    }

    let init_method = match envelope.config.initialization.as_str() {
        "kmeans++_chacha8_v1" => InitMethod::KMeansPlusPlusChaCha8V1,
        "explicit_centers_v1" => InitMethod::ExplicitCentersV1,
        other => {
            return Err(CoreError::new(
                ensabc_core::CoreErrorCode::InvalidConfig,
                format!("Unsupported initialization: {}", other),
            ));
        }
    };

    let stop_rule = match envelope.config.stopping_rule.as_str() {
        "sklearn_variance_scaled_v1" => StopRule::SklearnVarianceScaledV1,
        other => {
            return Err(CoreError::new(
                ensabc_core::CoreErrorCode::InvalidConfig,
                format!("Unsupported stopping_rule: {}", other),
            ));
        }
    };

    let config = ScenarioConfig {
        projection_dim: envelope.config.projection_dim,
        cluster_count: envelope.config.cluster_count,
        initialization: init_method,
        seed: envelope.config.seed,
        restarts: envelope.config.restarts,
        initial_centers: envelope.config.initial_centers.clone(),
        stopping_rule: stop_rule,
        tolerance: envelope.config.tolerance,
        max_iterations: envelope.config.max_iterations,
        rank_threshold: envelope.config.rank_threshold,
    };

    let members = envelope
        .members
        .into_iter()
        .map(|m| MemberInput {
            member_index: m.member_index,
            member_id: m.member_id,
            features: m.features,
        })
        .collect();

    let task = TaskInput { config, members };
    let result = cluster(&task)?;

    // Map result to ResultEnvelope
    let result_envelope = ResultEnvelope {
        schema_version: "m1.result.v1".to_string(),
        algorithm_version: ALGORITHM_VERSION.to_string(),
        config: envelope.config,
        members: result
            .members
            .into_iter()
            .map(|m| MemberAssignmentDto {
                member_index: m.member_index,
                member_id: m.member_id,
                cluster_id: m.cluster_id,
            })
            .collect(),
        scenarios: result
            .scenarios
            .into_iter()
            .map(|s| ClusterScenarioDto {
                cluster_id: s.cluster_id,
                member_count: s.member_count,
                member_fraction: s.member_fraction,
                representative_member_id: s.representative_member_id,
                cluster_sse: s.cluster_sse,
                center: s.center,
                mean_features: s.mean_features,
            })
            .collect(),
        diagnostics: DiagnosticsDto {
            effective_rank: result.diagnostics.effective_rank,
            rank_after_scaling: result.diagnostics.rank_after_scaling,
            normalization_mean: result.diagnostics.normalization_mean,
            normalization_scale: result.diagnostics.normalization_scale,
            components: result.diagnostics.components,
            explained_variance_ratio: result.diagnostics.explained_variance_ratio,
            scores: result.diagnostics.scores,
            total_sse: result.diagnostics.total_sse,
            restarts: result
                .diagnostics
                .restarts
                .into_iter()
                .map(|r| RestartSummaryDto {
                    restart_index: r.restart_index,
                    converged: r.converged,
                    iterations: r.iterations,
                    total_sse: r.total_sse,
                    initial_centers: r.initial_centers,
                    initial_member_indices: r.initial_member_indices,
                    stop_reason: r.stop_reason,
                })
                .collect(),
            selected_restart: result.diagnostics.selected_restart,
        },
    };

    let serialized = serde_json::to_string_pretty(&result_envelope).map_err(|e| {
        CoreError::new(
            ensabc_core::CoreErrorCode::NumericalFailure,
            format!("Serialization error: {}", e),
        )
    })?;

    // Atomic write to output_path via temporary file in parent directory
    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|e| {
        CoreError::new(
            ensabc_core::CoreErrorCode::NumericalFailure,
            format!("Failed to create output directory {:?}: {}", parent, e),
        )
    })?;

    let tmp_path = parent.join(format!(".tmp_{}.json", process::id()));
    let mut tmp_file = File::create(&tmp_path).map_err(|e| {
        CoreError::new(
            ensabc_core::CoreErrorCode::NumericalFailure,
            format!("Failed to create temporary file {:?}: {}", tmp_path, e),
        )
    })?;

    tmp_file
        .write_all(serialized.as_bytes())
        .and_then(|_| tmp_file.write_all(b"\n"))
        .map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            CoreError::new(
                ensabc_core::CoreErrorCode::NumericalFailure,
                format!("Failed writing result: {}", e),
            )
        })?;

    fs::rename(&tmp_path, &output_path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        CoreError::new(
            ensabc_core::CoreErrorCode::NumericalFailure,
            format!("Failed renaming temporary file to {:?}: {}", output_path, e),
        )
    })?;

    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", err);
        process::exit(1);
    }
}
