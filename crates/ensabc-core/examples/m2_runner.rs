//! Development and verification runner for M2 scenario matching and dispersion.
//!
//! Usage:
//!     cargo run --locked --release -p ensabc-core --example m2_runner -- \
//!         --mode match --input /path/task.json --output /path/result.json [--scaled]
//!
//!     cargo run --locked --release -p ensabc-core --example m2_runner -- \
//!         --mode dispersion --input /path/disp_task.json --output /path/disp_result.json

use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;

use serde::{Deserialize, Serialize};

use ensabc_core::{
    CoreError, InitMethod, MatchInput, MemberInput, ScaledMatchInput, ScenarioConfig,
    ScenarioNodeInput, StopRule, Substitutability, TaskInput, cluster, ensemble_dispersion,
    match_scenarios, match_scenarios_scaled,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScenarioNodeDto {
    cluster_id: usize,
    #[serde(deserialize_with = "deserialize_vec_f64_lenient")]
    mean_features: Vec<f64>,
    #[serde(default = "default_member_count")]
    member_count: usize,
    #[serde(default = "default_member_fraction")]
    member_fraction: f64,
}

fn deserialize_vec_f64_lenient<'de, D>(deserializer: D) -> Result<Vec<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct LenientVecF64Visitor;
    impl<'de> serde::de::Visitor<'de> for LenientVecF64Visitor {
        type Value = Vec<f64>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of floats or float strings")
        }
        fn visit_seq<A>(self, mut seq: A) -> Result<Vec<f64>, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(elem) = seq.next_element::<serde_json::Value>()? {
                let val = match elem {
                    serde_json::Value::Number(n) => n
                        .as_f64()
                        .ok_or_else(|| serde::de::Error::custom("invalid number"))?,
                    serde_json::Value::String(s) => match s.as_str() {
                        "NaN" | "nan" | "NAN" => f64::NAN,
                        "Infinity" | "+Infinity" | "inf" | "+inf" => f64::INFINITY,
                        "-Infinity" | "-inf" => f64::NEG_INFINITY,
                        other => other.parse::<f64>().map_err(serde::de::Error::custom)?,
                    },
                    _ => return Err(serde::de::Error::custom("expected float or float string")),
                };
                vec.push(val);
            }
            Ok(vec)
        }
    }
    deserializer.deserialize_seq(LenientVecF64Visitor)
}

fn default_member_count() -> usize {
    1
}
fn default_member_fraction() -> f64 {
    1.0
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MatchEnvelopeDto {
    #[serde(default)]
    #[allow(dead_code)]
    schema_version: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    case_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    task_id: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    prev_scenarios: Vec<ScenarioNodeDto>,
    curr_scenarios: Vec<ScenarioNodeDto>,
    #[serde(deserialize_with = "deserialize_vec_f64_lenient")]
    weights: Vec<f64>,
    #[serde(default)]
    threshold: Option<f64>,
    #[serde(default)]
    threshold_coefficient: Option<f64>,
    #[serde(default)]
    prev_dispersion: Option<f64>,
    #[serde(default)]
    curr_dispersion: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispersionMemberDto {
    #[serde(default)]
    member_index: Option<usize>,
    #[serde(default)]
    member_id: Option<String>,
    #[serde(deserialize_with = "deserialize_vec_f64_lenient")]
    features: Vec<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispersionEnvelopeDto {
    #[serde(default)]
    #[allow(dead_code)]
    schema_version: Option<String>,
    #[serde(deserialize_with = "deserialize_vec_f64_lenient")]
    weights: Vec<f64>,
    members: Vec<DispersionMemberDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterConfigDto {
    projection_dim: usize,
    cluster_count: usize,
    initialization: String,
    seed: u64,
    restarts: usize,
    #[serde(default)]
    initial_centers: Vec<Vec<Vec<f64>>>,
    stopping_rule: String,
    tolerance: f64,
    max_iterations: usize,
    rank_threshold: f64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChainBatchDto {
    ordinal: usize,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    dispersion: Option<f64>,
    #[serde(default)]
    scenarios: Vec<ScenarioNodeDto>,
    #[serde(default)]
    members: Option<Vec<DispersionMemberDto>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChainEnvelopeDto {
    #[serde(default)]
    #[allow(dead_code)]
    schema_version: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    mode: Option<String>,
    #[serde(default)]
    run_key: Option<String>,
    threshold_coefficient: f64,
    #[serde(deserialize_with = "deserialize_vec_f64_lenient")]
    weights: Vec<f64>,
    #[serde(default)]
    cluster_config: Option<ClusterConfigDto>,
    batches: Vec<ChainBatchDto>,
}

#[derive(Debug, Serialize)]
struct ChainNodeResultDto {
    ordinal: usize,
    cluster_id: usize,
    canonical_rank: usize,
    branch_id: String,
    assignment_reason: String,
}

#[derive(Debug, Serialize)]
struct ChainLinkResultDto {
    prev_ordinal: usize,
    prev_cluster_id: usize,
    curr_ordinal: usize,
    curr_cluster_id: usize,
    distance: f64,
    ambiguous: bool,
}

#[derive(Debug, Serialize)]
struct ChainResultDto {
    schema_version: String,
    run_key: String,
    nodes: Vec<ChainNodeResultDto>,
    links: Vec<ChainLinkResultDto>,
}

#[derive(Debug, Serialize)]
struct MatchedLinkDto {
    prev_cluster_id: usize,
    curr_cluster_id: usize,
    distance_k: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    distance_ratio: Option<f64>,
    substitutability: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    substitutability_gap: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    alt_objective: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    alt_count: Option<usize>,
    ambiguous: bool,
}

#[derive(Debug, Serialize)]
struct UnmatchedScenarioDto {
    cluster_id: usize,
    reason: String,
    matched_in_some_optimum: bool,
}

#[derive(Debug, Serialize)]
struct MatchObjectiveDto {
    matched_count: usize,
    weighted_distance_sum: f64,
}

#[derive(Debug, Serialize)]
struct MatchResultDto {
    schema_version: String,
    objective: MatchObjectiveDto,
    links: Vec<MatchedLinkDto>,
    unmatched_prev: Vec<UnmatchedScenarioDto>,
    unmatched_curr: Vec<UnmatchedScenarioDto>,
    distance_matrix: Vec<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair_scale: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    threshold_coefficient: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev_dispersion: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    curr_dispersion: Option<f64>,
    optima_count: usize,
}

#[derive(Debug, Serialize)]
struct DispersionResultDto {
    schema_version: String,
    dispersion: f64,
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!(
        "Usage: m2_runner --mode match|dispersion --input <PATH> --output <PATH> [--scaled]\n\
         Unknown flags are rejected."
    );
    process::exit(code);
}

struct ParsedArgs {
    mode: String,
    input_path: PathBuf,
    output_path: PathBuf,
    scaled: bool,
}

fn parse_args() -> ParsedArgs {
    let mut args = env::args().skip(1);
    let mut mode: Option<String> = None;
    let mut input_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut scaled = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                mode = Some(args.next().unwrap_or_else(|| print_usage_and_exit(2)));
            }
            "--input" => {
                input_path = Some(PathBuf::from(
                    args.next().unwrap_or_else(|| print_usage_and_exit(2)),
                ));
            }
            "--output" => {
                output_path = Some(PathBuf::from(
                    args.next().unwrap_or_else(|| print_usage_and_exit(2)),
                ));
            }
            "--scaled" => {
                scaled = true;
            }
            "-h" | "--help" => print_usage_and_exit(0),
            unknown => {
                eprintln!("[UNKNOWN_ARG] Unknown flag: {}", unknown);
                process::exit(2);
            }
        }
    }

    match (mode, input_path, output_path) {
        (Some(m), Some(i), Some(o)) => ParsedArgs {
            mode: m,
            input_path: i,
            output_path: o,
            scaled,
        },
        _ => print_usage_and_exit(2),
    }
}

enum RunnerError {
    Usage(String),
    Core(CoreError),
}

impl From<CoreError> for RunnerError {
    fn from(e: CoreError) -> Self {
        Self::Core(e)
    }
}

fn atomic_write_json(path: &Path, content: &str) -> Result<(), CoreError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|e| {
        CoreError::new(
            ensabc_core::CoreErrorCode::NumericalFailure,
            format!("Failed to create parent dir {:?}: {}", parent, e),
        )
    })?;

    let tmp_path = parent.join(format!(".tmp_m2_{}_{}.json", process::id(), rand_suffix()));
    let mut tmp_file = File::create(&tmp_path).map_err(|e| {
        CoreError::new(
            ensabc_core::CoreErrorCode::NumericalFailure,
            format!("Failed to create temporary file {:?}: {}", tmp_path, e),
        )
    })?;

    tmp_file
        .write_all(content.as_bytes())
        .and_then(|_| tmp_file.write_all(b"\n"))
        .map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            CoreError::new(
                ensabc_core::CoreErrorCode::NumericalFailure,
                format!("Failed writing content: {}", e),
            )
        })?;

    fs::rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        CoreError::new(
            ensabc_core::CoreErrorCode::NumericalFailure,
            format!("Failed renaming tmp file to {:?}: {}", path, e),
        )
    })?;

    Ok(())
}

fn rand_suffix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn sanitize_json_non_finite(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_string = false;
    let mut escape = false;
    let mut byte_idx = 0usize;
    let bytes = text.as_bytes();
    while byte_idx < bytes.len() {
        let ch = text[byte_idx..].chars().next().unwrap();
        let ch_len = ch.len_utf8();
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            out.push(ch);
            byte_idx += ch_len;
        } else if ch == '"' {
            in_string = true;
            out.push(ch);
            byte_idx += ch_len;
        } else if text[byte_idx..].starts_with("NaN") {
            out.push_str("\"NaN\"");
            byte_idx += 3;
        } else if text[byte_idx..].starts_with("-Infinity") {
            out.push_str("\"-Infinity\"");
            byte_idx += 9;
        } else if text[byte_idx..].starts_with("Infinity") {
            out.push_str("\"Infinity\"");
            byte_idx += 8;
        } else {
            out.push(ch);
            byte_idx += ch_len;
        }
    }
    out
}

fn run() -> Result<(), RunnerError> {
    let parsed = parse_args();

    let input_bytes = fs::read(&parsed.input_path)
        .map_err(|e| RunnerError::Usage(format!("Cannot read input file {:?}: {}", parsed.input_path, e)))?;
    let raw_text = String::from_utf8_lossy(&input_bytes);
    let sanitized_text = sanitize_json_non_finite(&raw_text);

    match parsed.mode.as_str() {
        "match" => {
            let env: MatchEnvelopeDto = serde_json::from_str(&sanitized_text)
                .map_err(|e| RunnerError::Usage(format!("Invalid JSON in match envelope: {}", e)))?;

            let prev_scens: Vec<ScenarioNodeInput> = env
                .prev_scenarios
                .into_iter()
                .map(|s| ScenarioNodeInput {
                    cluster_id: s.cluster_id,
                    mean_features: s.mean_features,
                    member_count: s.member_count,
                    member_fraction: s.member_fraction,
                })
                .collect();

            let curr_scens: Vec<ScenarioNodeInput> = env
                .curr_scenarios
                .into_iter()
                .map(|s| ScenarioNodeInput {
                    cluster_id: s.cluster_id,
                    mean_features: s.mean_features,
                    member_count: s.member_count,
                    member_fraction: s.member_fraction,
                })
                .collect();

            let is_scaled = parsed.scaled
                || env.mode.as_deref() == Some("scaled")
                || (env.threshold_coefficient.is_some()
                    && env.prev_dispersion.is_some()
                    && env.curr_dispersion.is_some());

            let result = if is_scaled {
                let c = env.threshold_coefficient.ok_or_else(|| {
                    CoreError::new(
                        ensabc_core::CoreErrorCode::InvalidThreshold,
                        "threshold_coefficient missing for scaled mode",
                    )
                })?;
                let sp = env.prev_dispersion.ok_or_else(|| {
                    CoreError::new(
                        ensabc_core::CoreErrorCode::ZeroDispersion,
                        "prev_dispersion missing for scaled mode",
                    )
                })?;
                let sc = env.curr_dispersion.ok_or_else(|| {
                    CoreError::new(
                        ensabc_core::CoreErrorCode::ZeroDispersion,
                        "curr_dispersion missing for scaled mode",
                    )
                })?;

                let scaled_input = ScaledMatchInput {
                    prev_scenarios: prev_scens,
                    curr_scenarios: curr_scens,
                    weights: env.weights,
                    threshold_coefficient: c,
                    prev_dispersion: sp,
                    curr_dispersion: sc,
                };
                match_scenarios_scaled(&scaled_input)?
            } else {
                let tau = env.threshold.ok_or_else(|| {
                    CoreError::new(
                        ensabc_core::CoreErrorCode::InvalidThreshold,
                        "threshold missing for absolute match mode",
                    )
                })?;
                let match_input = MatchInput {
                    prev_scenarios: prev_scens,
                    curr_scenarios: curr_scens,
                    weights: env.weights,
                    threshold: tau,
                };
                match_scenarios(&match_input)?
            };

            let out_dto = MatchResultDto {
                schema_version: "m2.match-result.v1".to_string(),
                objective: MatchObjectiveDto {
                    matched_count: result.objective.matched_count,
                    weighted_distance_sum: result.objective.weighted_distance_sum,
                },
                links: result
                    .links
                    .into_iter()
                    .map(|l| {
                        let (sub_str, gap, alt_obj, alt_cnt) = match l.substitutability {
                            Substitutability::Tied => ("tied".to_string(), None, None, None),
                            Substitutability::SameCount { gap, alt_objective } => (
                                "same_count".to_string(),
                                Some(gap),
                                Some(alt_objective),
                                None,
                            ),
                            Substitutability::CountDrop {
                                alt_count,
                                alt_objective,
                            } => (
                                "count_drop".to_string(),
                                None,
                                Some(alt_objective),
                                Some(alt_count),
                            ),
                        };
                        MatchedLinkDto {
                            prev_cluster_id: l.prev_cluster_id,
                            curr_cluster_id: l.curr_cluster_id,
                            distance_k: l.distance_k,
                            distance_ratio: l.distance_ratio,
                            substitutability: sub_str,
                            substitutability_gap: gap,
                            alt_objective: alt_obj,
                            alt_count: alt_cnt,
                            ambiguous: l.ambiguous,
                        }
                    })
                    .collect(),
                unmatched_prev: result
                    .unmatched_prev
                    .into_iter()
                    .map(|u| UnmatchedScenarioDto {
                        cluster_id: u.cluster_id,
                        reason: u.reason,
                        matched_in_some_optimum: u.matched_in_some_optimum,
                    })
                    .collect(),
                unmatched_curr: result
                    .unmatched_curr
                    .into_iter()
                    .map(|u| UnmatchedScenarioDto {
                        cluster_id: u.cluster_id,
                        reason: u.reason,
                        matched_in_some_optimum: u.matched_in_some_optimum,
                    })
                    .collect(),
                distance_matrix: result.distance_matrix,
                pair_scale: result.pair_scale,
                threshold_coefficient: result.threshold_coefficient,
                prev_dispersion: result.prev_dispersion,
                curr_dispersion: result.curr_dispersion,
                optima_count: result.optima_count,
            };

            let json_str = serde_json::to_string_pretty(&out_dto).map_err(|e| {
                CoreError::new(
                    ensabc_core::CoreErrorCode::NumericalFailure,
                    format!("Serialization failure: {}", e),
                )
            })?;
            atomic_write_json(&parsed.output_path, &json_str)?;
        }
        "dispersion" => {
            let env: DispersionEnvelopeDto = serde_json::from_str(&sanitized_text).map_err(|e| {
                RunnerError::Usage(format!("Invalid JSON in dispersion envelope: {}", e))
            })?;

            let members: Vec<MemberInput> = env
                .members
                .into_iter()
                .enumerate()
                .map(|(idx, m)| MemberInput {
                    member_index: m.member_index.unwrap_or(idx),
                    member_id: m.member_id.unwrap_or_else(|| format!("m_{idx}")),
                    features: m.features,
                })
                .collect();

            let s = ensemble_dispersion(&members, &env.weights)?;
            let out_dto = DispersionResultDto {
                schema_version: "m2.dispersion-result.v1".to_string(),
                dispersion: s,
            };
            let json_str = serde_json::to_string_pretty(&out_dto).map_err(|e| {
                CoreError::new(
                    ensabc_core::CoreErrorCode::NumericalFailure,
                    format!("Serialization failure: {}", e),
                )
            })?;
            atomic_write_json(&parsed.output_path, &json_str)?;
        }
        "chain" => {
            let env: ChainEnvelopeDto = serde_json::from_str(&sanitized_text)
                .map_err(|e| RunnerError::Usage(format!("Invalid JSON in chain envelope: {}", e)))?;

            let cluster_cfg: Option<ScenarioConfig> = match &env.cluster_config {
                Some(c) => {
                    let init = match c.initialization.as_str() {
                        "kmeans++_chacha8_v1" => InitMethod::KMeansPlusPlusChaCha8V1,
                        "explicit_centers_v1" => InitMethod::ExplicitCentersV1,
                        other => {
                            return Err(RunnerError::Usage(format!(
                                "Unsupported initialization: {other}"
                            )));
                        }
                    };
                    let stop = match c.stopping_rule.as_str() {
                        "sklearn_variance_scaled_v1" => StopRule::SklearnVarianceScaledV1,
                        other => {
                            return Err(RunnerError::Usage(format!(
                                "Unsupported stopping_rule: {other}"
                            )));
                        }
                    };
                    Some(ScenarioConfig {
                        projection_dim: c.projection_dim,
                        cluster_count: c.cluster_count,
                        initialization: init,
                        seed: c.seed,
                        restarts: c.restarts,
                        initial_centers: c.initial_centers.clone(),
                        stopping_rule: stop,
                        tolerance: c.tolerance,
                        max_iterations: c.max_iterations,
                        rank_threshold: c.rank_threshold,
                    })
                }
                None => None,
            };

            let run_key = env
                .run_key
                .unwrap_or_else(|| "default_synthetic_run_key_123456789".to_string());
            let prefix = if run_key.len() >= 12 {
                &run_key[0..12]
            } else {
                &run_key
            };

            struct RankedNode {
                #[allow(dead_code)]
                ordinal: usize,
                cluster_id: usize,
                canonical_rank: usize,
                mean_features: Vec<f64>,
            }

            let mut resolved_statuses: Vec<String> = Vec::with_capacity(env.batches.len());
            let mut resolved_dispersions: Vec<Option<f64>> = Vec::with_capacity(env.batches.len());
            let mut batch_nodes: Vec<Vec<RankedNode>> = Vec::with_capacity(env.batches.len());

            for b in &env.batches {
                let status = b.status.as_deref().unwrap_or("ok");
                if status == "missing" || status == "failed" {
                    resolved_statuses.push(status.to_string());
                    resolved_dispersions.push(None);
                    batch_nodes.push(Vec::new());
                    continue;
                }

                let (raw_scenarios, disp): (Vec<ScenarioNodeInput>, f64) =
                    if let Some(member_dtos) = &b.members {
                        let cfg = cluster_cfg.as_ref().ok_or_else(|| {
                            RunnerError::Usage(
                                "cluster_config is required when chain batches supply members"
                                    .to_string(),
                            )
                        })?;
                        let members: Vec<MemberInput> = member_dtos
                            .iter()
                            .enumerate()
                            .map(|(idx, m)| MemberInput {
                                member_index: m.member_index.unwrap_or(idx),
                                member_id: m
                                    .member_id
                                    .clone()
                                    .unwrap_or_else(|| format!("m_{idx}")),
                                features: m.features.clone(),
                            })
                            .collect();
                        let task = TaskInput {
                            config: cfg.clone(),
                            members: members.clone(),
                        };
                        let c_res = cluster(&task)?;
                        let s = ensemble_dispersion(&members, &env.weights)?;
                        let scs = c_res
                            .scenarios
                            .into_iter()
                            .map(|sc| ScenarioNodeInput {
                                cluster_id: sc.cluster_id,
                                mean_features: sc.mean_features,
                                member_count: sc.member_count,
                                member_fraction: sc.member_fraction,
                            })
                            .collect();
                        (scs, s)
                    } else {
                        let s = b.dispersion.ok_or_else(|| {
                            RunnerError::Usage(format!(
                                "Batch ordinal {} with status='ok' requires explicit dispersion",
                                b.ordinal
                            ))
                        })?;
                        let scs = b
                            .scenarios
                            .iter()
                            .map(|sc| ScenarioNodeInput {
                                cluster_id: sc.cluster_id,
                                mean_features: sc.mean_features.clone(),
                                member_count: sc.member_count,
                                member_fraction: sc.member_fraction,
                            })
                            .collect();
                        (scs, s)
                    };

                let mut order: Vec<usize> = (0..raw_scenarios.len()).collect();
                let norm = |x: f64| if x == 0.0 { 0.0 } else { x };
                order.sort_by(|&i, &j| {
                    let sc_a = &raw_scenarios[i];
                    let sc_b = &raw_scenarios[j];
                    sc_a.mean_features
                        .iter()
                        .zip(&sc_b.mean_features)
                        .map(|(&x, &y)| norm(x).total_cmp(&norm(y)))
                        .find(|&o| o != std::cmp::Ordering::Equal)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| sc_a.cluster_id.cmp(&sc_b.cluster_id))
                });

                let mut ranked = Vec::new();
                for (rank, &sc_idx) in order.iter().enumerate() {
                    let sc = &raw_scenarios[sc_idx];
                    ranked.push(RankedNode {
                        ordinal: b.ordinal,
                        cluster_id: sc.cluster_id,
                        canonical_rank: rank,
                        mean_features: sc.mean_features.clone(),
                    });
                }
                ranked.sort_by_key(|r| r.cluster_id);
                resolved_statuses.push("ok".to_string());
                resolved_dispersions.push(Some(disp));
                batch_nodes.push(ranked);
            }

            // Match adjacent batches
            let mut links_out: Vec<ChainLinkResultDto> = Vec::new();
            let mut incoming_map: std::collections::HashMap<(usize, usize), (usize, usize, bool)> =
                std::collections::HashMap::new();
            let mut unmatched_curr_reasons: std::collections::HashMap<(usize, usize), String> =
                std::collections::HashMap::new();

            for idx in 0..env.batches.len().saturating_sub(1) {
                let b_prev = &env.batches[idx];
                let b_curr = &env.batches[idx + 1];
                if b_curr.ordinal != b_prev.ordinal + 1 {
                    continue;
                }
                if resolved_statuses[idx] != "ok" || resolved_statuses[idx + 1] != "ok" {
                    continue;
                }

                let sp = resolved_dispersions[idx].expect("ok batch has dispersion");
                let sc = resolved_dispersions[idx + 1].expect("ok batch has dispersion");

                let p_nodes = &batch_nodes[idx];
                let c_nodes = &batch_nodes[idx + 1];

                let match_in = ScaledMatchInput {
                    prev_scenarios: p_nodes
                        .iter()
                        .map(|n| ScenarioNodeInput {
                            cluster_id: n.cluster_id,
                            mean_features: n.mean_features.clone(),
                            member_count: 1,
                            member_fraction: 1.0,
                        })
                        .collect(),
                    curr_scenarios: c_nodes
                        .iter()
                        .map(|n| ScenarioNodeInput {
                            cluster_id: n.cluster_id,
                            mean_features: n.mean_features.clone(),
                            member_count: 1,
                            member_fraction: 1.0,
                        })
                        .collect(),
                    weights: env.weights.clone(),
                    threshold_coefficient: env.threshold_coefficient,
                    prev_dispersion: sp,
                    curr_dispersion: sc,
                };

                let res = match_scenarios_scaled(&match_in)?;

                for l in &res.links {
                    links_out.push(ChainLinkResultDto {
                        prev_ordinal: b_prev.ordinal,
                        prev_cluster_id: l.prev_cluster_id,
                        curr_ordinal: b_curr.ordinal,
                        curr_cluster_id: l.curr_cluster_id,
                        distance: l.distance_k,
                        ambiguous: l.ambiguous,
                    });
                    incoming_map.insert(
                        (b_curr.ordinal, l.curr_cluster_id),
                        (b_prev.ordinal, l.prev_cluster_id, l.ambiguous),
                    );
                }
                for u in &res.unmatched_curr {
                    unmatched_curr_reasons.insert((b_curr.ordinal, u.cluster_id), u.reason.clone());
                }
            }

            let mut identity_map: std::collections::HashMap<(usize, usize), (String, String)> =
                std::collections::HashMap::new();

            for (idx, b) in env.batches.iter().enumerate() {
                let r_nodes = &batch_nodes[idx];
                for n in r_nodes {
                    if let Some(&(p_ord, p_cid, amb)) = incoming_map.get(&(b.ordinal, n.cluster_id))
                    {
                        let (p_branch, _) = identity_map
                            .get(&(p_ord, p_cid))
                            .expect("parent must be processed first");
                        let reason = if amb {
                            "inherited_ambiguous"
                        } else {
                            "inherited"
                        };
                        identity_map.insert(
                            (b.ordinal, n.cluster_id),
                            (p_branch.clone(), reason.to_string()),
                        );
                    } else {
                        let branch_id =
                            format!("{prefix}:b{:02}:k{:02}", b.ordinal, n.canonical_rank);
                        let reason = if b.ordinal == 0 {
                            "first_batch".to_string()
                        } else if idx > 0 && resolved_statuses[idx - 1] != "ok" {
                            "previous_planned_batch_unavailable".to_string()
                        } else if let Some(r) =
                            unmatched_curr_reasons.get(&(b.ordinal, n.cluster_id))
                        {
                            r.clone()
                        } else {
                            "not_selected_in_global_optimum".to_string()
                        };
                        identity_map.insert((b.ordinal, n.cluster_id), (branch_id, reason));
                    }
                }
            }

            let mut nodes_out: Vec<ChainNodeResultDto> = Vec::new();
            for (idx, b) in env.batches.iter().enumerate() {
                for n in &batch_nodes[idx] {
                    let (b_id, reason) = identity_map.get(&(b.ordinal, n.cluster_id)).unwrap();
                    nodes_out.push(ChainNodeResultDto {
                        ordinal: b.ordinal,
                        cluster_id: n.cluster_id,
                        canonical_rank: n.canonical_rank,
                        branch_id: b_id.clone(),
                        assignment_reason: reason.clone(),
                    });
                }
            }

            let out_dto = ChainResultDto {
                schema_version: "m2.chain-result.v1".to_string(),
                run_key,
                nodes: nodes_out,
                links: links_out,
            };

            let json_str = serde_json::to_string_pretty(&out_dto).map_err(|e| {
                CoreError::new(
                    ensabc_core::CoreErrorCode::NumericalFailure,
                    format!("Serialization failure: {}", e),
                )
            })?;
            atomic_write_json(&parsed.output_path, &json_str)?;
        }
        unknown => {
            return Err(RunnerError::Usage(format!(
                "Unknown mode: {}, expected 'match', 'dispersion', or 'chain'",
                unknown
            )));
        }
    }

    Ok(())
}

fn main() {
    match run() {
        Ok(()) => {}
        Err(RunnerError::Core(err)) => {
            eprintln!("[ERROR] {}: {}", err.code.as_str(), err.message);
            process::exit(1);
        }
        Err(RunnerError::Usage(msg)) => {
            eprintln!("[USAGE] {}", msg);
            process::exit(2);
        }
    }
}
