//! Standalone CLI verification runner for M3 scenario evolution (`pair` and `chain` modes).
//!
//! Usage:
//!     cargo run --locked --release -p ensabc-core --example m3_runner -- \
//!         --mode pair --input /path/input.json --output /path/output.json
//!
//!     cargo run --locked --release -p ensabc-core --example m3_runner -- \
//!         --mode chain --input /path/chain_input.json --output /path/chain_output.json

use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;

use serde::{Deserialize, Serialize};

use ensabc_core::{
    BackboneAssertion, ChainBatchInput, ChainBatchStatus, ChainEvolutionEdge, ChainEvolutionEvent,
    ChainNodeSummary, ChainResolvedBatch, ChainTransitionRecord, CoreError, CoreErrorCode,
    EvolutionBranchSummary, EvolutionChainInput, EvolutionChainResult, EvolutionChainSummary,
    EvolutionConfig, EvolutionEdge, EvolutionEvent, EvolutionInput, EvolutionResult, FlowRule,
    InitMethod, MatchResult, MemberInput, NodeEvolutionSummary, ScaledEvolutionInput,
    ScenarioConfig, ScenarioNodeInput, SecondaryRule, StopRule, Substitutability, TaskInput,
    TransitionEvolutionSummary, cluster, ensemble_dispersion, evolve_chain, evolve_scenarios,
    evolve_scenarios_scaled,
};

// ---------------------------------------------------------------------------
// Lenient Float Parsing (supports `"NaN"`, `"Infinity"`, `"-Infinity"` tokens)
// ---------------------------------------------------------------------------

fn parse_lenient_f64_value(val: &serde_json::Value) -> Result<f64, String> {
    match val {
        serde_json::Value::Number(n) => {
            n.as_f64().ok_or_else(|| "invalid float number".to_string())
        }
        serde_json::Value::String(s) => match s.as_str() {
            "NaN" | "nan" | "NAN" | "+NaN" | "-NaN" => Ok(f64::NAN),
            "Infinity" | "+Infinity" | "inf" | "+inf" | "INF" | "+INF" => Ok(f64::INFINITY),
            "-Infinity" | "-inf" | "-INF" => Ok(f64::NEG_INFINITY),
            other => other
                .parse::<f64>()
                .map_err(|e| format!("invalid float string '{other}': {e}")),
        },
        _ => Err("expected float number or float string".to_string()),
    }
}

fn deserialize_f64_lenient<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = serde_json::Value::deserialize(deserializer)?;
    parse_lenient_f64_value(&v).map_err(serde::de::Error::custom)
}

fn deserialize_opt_f64_lenient<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = Option::<serde_json::Value>::deserialize(deserializer)?;
    match v {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(other) => parse_lenient_f64_value(&other)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

fn deserialize_vec_f64_lenient<'de, D>(deserializer: D) -> Result<Vec<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let seq = Vec::<serde_json::Value>::deserialize(deserializer)?;
    seq.iter()
        .map(parse_lenient_f64_value)
        .collect::<Result<Vec<f64>, _>>()
        .map_err(serde::de::Error::custom)
}

fn deserialize_opt_mat_f64_lenient<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<Vec<f64>>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<Vec<Vec<serde_json::Value>>>::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(rows) => {
            let mut mat = Vec::with_capacity(rows.len());
            for row in &rows {
                let parsed = row
                    .iter()
                    .map(parse_lenient_f64_value)
                    .collect::<Result<Vec<f64>, _>>()
                    .map_err(serde::de::Error::custom)?;
                mat.push(parsed);
            }
            Ok(Some(mat))
        }
    }
}

#[inline]
fn norm_f64(x: f64) -> f64 {
    if x == 0.0 { 0.0 } else { x }
}

#[inline]
fn checked_finite(x: f64, label: &str) -> Result<f64, CoreError> {
    let v = norm_f64(x);
    if !v.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::NumericalFailure,
            format!("{label} is non-finite ({x})"),
        ));
    }
    Ok(v)
}

#[inline]
fn checked_opt_finite(x: Option<f64>, label: &str) -> Result<Option<f64>, CoreError> {
    match x {
        Some(v) => checked_finite(v, label).map(Some),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Input DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScenarioNodeDto {
    cluster_id: i64,
    member_count: i64,
    #[serde(deserialize_with = "deserialize_f64_lenient")]
    member_fraction: f64,
    #[serde(deserialize_with = "deserialize_vec_f64_lenient")]
    mean_features: Vec<f64>,
    #[serde(default)]
    #[allow(dead_code)]
    representative_member_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    cluster_sse: Option<f64>,
    #[serde(default)]
    #[allow(dead_code)]
    within_cluster_sse: Option<f64>,
    #[serde(default)]
    #[allow(dead_code)]
    center: Option<Vec<f64>>,
}

impl ScenarioNodeDto {
    fn into_prev_core(self, idx: usize) -> Result<ScenarioNodeInput, CoreError> {
        if self.cluster_id < 0 {
            return Err(CoreError::new(
                CoreErrorCode::InvalidScenarioId,
                format!(
                    "prev_scenarios[{idx}] cluster_id must be non-negative, got {}",
                    self.cluster_id
                ),
            )
            .with_prev_index(idx));
        }
        let cluster_id = usize::try_from(self.cluster_id).map_err(|_| {
            CoreError::new(
                CoreErrorCode::SizeOverflow,
                format!("prev_scenarios[{idx}] cluster_id overflows usize"),
            )
            .with_prev_index(idx)
        })?;
        if self.member_count <= 0 {
            return Err(CoreError::new(
                CoreErrorCode::ZeroMemberScenario,
                format!(
                    "prev_scenarios[{idx}] member_count must be >= 1, got {}",
                    self.member_count
                ),
            )
            .with_prev_index(idx));
        }
        let member_count = usize::try_from(self.member_count).map_err(|_| {
            CoreError::new(
                CoreErrorCode::SizeOverflow,
                format!("prev_scenarios[{idx}] member_count overflows usize"),
            )
            .with_prev_index(idx)
        })?;
        Ok(ScenarioNodeInput {
            cluster_id,
            mean_features: self.mean_features,
            member_count,
            member_fraction: self.member_fraction,
        })
    }

    fn into_curr_core(self, idx: usize) -> Result<ScenarioNodeInput, CoreError> {
        if self.cluster_id < 0 {
            return Err(CoreError::new(
                CoreErrorCode::InvalidScenarioId,
                format!(
                    "curr_scenarios[{idx}] cluster_id must be non-negative, got {}",
                    self.cluster_id
                ),
            )
            .with_curr_index(idx));
        }
        let cluster_id = usize::try_from(self.cluster_id).map_err(|_| {
            CoreError::new(
                CoreErrorCode::SizeOverflow,
                format!("curr_scenarios[{idx}] cluster_id overflows usize"),
            )
            .with_curr_index(idx)
        })?;
        if self.member_count <= 0 {
            return Err(CoreError::new(
                CoreErrorCode::ZeroMemberScenario,
                format!(
                    "curr_scenarios[{idx}] member_count must be >= 1, got {}",
                    self.member_count
                ),
            )
            .with_curr_index(idx));
        }
        let member_count = usize::try_from(self.member_count).map_err(|_| {
            CoreError::new(
                CoreErrorCode::SizeOverflow,
                format!("curr_scenarios[{idx}] member_count overflows usize"),
            )
            .with_curr_index(idx)
        })?;
        Ok(ScenarioNodeInput {
            cluster_id,
            mean_features: self.mean_features,
            member_count,
            member_fraction: self.member_fraction,
        })
    }

    fn into_batch_core(
        self,
        batch_ord: usize,
        sc_idx: usize,
    ) -> Result<ScenarioNodeInput, CoreError> {
        if self.cluster_id < 0 {
            return Err(CoreError::new(
                CoreErrorCode::InvalidScenarioId,
                format!(
                    "batch {batch_ord}: scenarios[{sc_idx}] cluster_id must be non-negative, got {}",
                    self.cluster_id
                ),
            )
            .with_prev_index(batch_ord));
        }
        let cluster_id = usize::try_from(self.cluster_id).map_err(|_| {
            CoreError::new(
                CoreErrorCode::SizeOverflow,
                format!("batch {batch_ord}: scenarios[{sc_idx}] cluster_id overflows usize"),
            )
            .with_prev_index(batch_ord)
        })?;
        if self.member_count <= 0 {
            return Err(CoreError::new(
                CoreErrorCode::ZeroMemberScenario,
                format!(
                    "batch {batch_ord}: scenarios[{sc_idx}] member_count must be >= 1, got {}",
                    self.member_count
                ),
            )
            .with_prev_index(batch_ord));
        }
        let member_count = usize::try_from(self.member_count).map_err(|_| {
            CoreError::new(
                CoreErrorCode::SizeOverflow,
                format!("batch {batch_ord}: scenarios[{sc_idx}] member_count overflows usize"),
            )
            .with_prev_index(batch_ord)
        })?;
        Ok(ScenarioNodeInput {
            cluster_id,
            mean_features: self.mean_features,
            member_count,
            member_fraction: self.member_fraction,
        })
    }
}

fn default_secondary_rule() -> String {
    "nearest_feasible_v1".to_string()
}
fn default_flow_rule() -> String {
    "star_min_mass_proportional_v1".to_string()
}
fn default_fraction_tol() -> f64 {
    1e-9
}
fn default_uncertainty_ratio() -> f64 {
    0.10
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvolutionConfigDto {
    #[serde(default = "default_secondary_rule")]
    secondary_rule: String,
    #[serde(default = "default_flow_rule")]
    flow_rule: String,
    #[serde(
        default = "default_fraction_tol",
        deserialize_with = "deserialize_f64_lenient"
    )]
    fraction_sum_tolerance: f64,
    #[serde(
        default = "default_uncertainty_ratio",
        deserialize_with = "deserialize_f64_lenient"
    )]
    high_uncertainty_threshold_ratio: f64,
    #[serde(
        default = "default_uncertainty_ratio",
        deserialize_with = "deserialize_f64_lenient"
    )]
    high_uncertainty_optimality_ratio: f64,
    #[serde(
        default = "default_uncertainty_ratio",
        deserialize_with = "deserialize_f64_lenient"
    )]
    high_uncertainty_secondary_ratio: f64,
}

impl EvolutionConfigDto {
    fn into_core(self) -> Result<EvolutionConfig, CoreError> {
        let secondary_rule = match self.secondary_rule.as_str() {
            "nearest_feasible_v1" => SecondaryRule::NearestFeasibleV1,
            other => {
                return Err(CoreError::new(
                    CoreErrorCode::InvalidEvolutionConfig,
                    format!("Unsupported secondary_rule: {other}"),
                ));
            }
        };
        let flow_rule = match self.flow_rule.as_str() {
            "star_min_mass_proportional_v1" => FlowRule::StarMinMassProportionalV1,
            other => {
                return Err(CoreError::new(
                    CoreErrorCode::InvalidEvolutionConfig,
                    format!("Unsupported flow_rule: {other}"),
                ));
            }
        };
        Ok(EvolutionConfig {
            secondary_rule,
            flow_rule,
            fraction_sum_tolerance: self.fraction_sum_tolerance,
            high_uncertainty_threshold_ratio: self.high_uncertainty_threshold_ratio,
            high_uncertainty_optimality_ratio: self.high_uncertainty_optimality_ratio,
            high_uncertainty_secondary_ratio: self.high_uncertainty_secondary_ratio,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PrimaryLinkDto {
    Tuple((i64, i64)),
    Struct {
        prev_cluster_id: i64,
        curr_cluster_id: i64,
    },
}

impl PrimaryLinkDto {
    fn into_pair(self, pos: usize) -> Result<(usize, usize), CoreError> {
        let (p, c) = match self {
            Self::Tuple(pair) => pair,
            Self::Struct {
                prev_cluster_id,
                curr_cluster_id,
            } => (prev_cluster_id, curr_cluster_id),
        };
        if p < 0 || c < 0 {
            return Err(CoreError::new(
                CoreErrorCode::InvalidPrimaryLink,
                format!(
                    "BackboneAssertion primary_links[{pos}] contains negative cluster_id ({p}, {c})"
                ),
            )
            .with_prev_index(pos));
        }
        let pu = usize::try_from(p).map_err(|_| {
            CoreError::new(
                CoreErrorCode::SizeOverflow,
                "primary_links prev_cluster_id overflows usize",
            )
            .with_prev_index(pos)
        })?;
        let cu = usize::try_from(c).map_err(|_| {
            CoreError::new(
                CoreErrorCode::SizeOverflow,
                "primary_links curr_cluster_id overflows usize",
            )
            .with_prev_index(pos)
        })?;
        Ok((pu, cu))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackboneAssertionDto {
    prev_cluster_ids: Vec<i64>,
    curr_cluster_ids: Vec<i64>,
    #[serde(deserialize_with = "deserialize_f64_lenient")]
    threshold: f64,
    #[serde(default, deserialize_with = "deserialize_opt_mat_f64_lenient")]
    distance_matrix: Option<Vec<Vec<f64>>>,
    #[serde(default)]
    eligible_matrix: Option<Vec<Vec<bool>>>,
    primary_links: Vec<PrimaryLinkDto>,
}

impl BackboneAssertionDto {
    fn into_core(self) -> Result<BackboneAssertion, CoreError> {
        let mut prev_cluster_ids = Vec::with_capacity(self.prev_cluster_ids.len());
        for id in self.prev_cluster_ids {
            if id < 0 {
                return Err(CoreError::new(
                    CoreErrorCode::BackboneInconsistency,
                    format!("BackboneAssertion prev_cluster_ids contains negative id {id}"),
                ));
            }
            prev_cluster_ids.push(usize::try_from(id).map_err(|_| {
                CoreError::new(
                    CoreErrorCode::SizeOverflow,
                    "BackboneAssertion prev_cluster_ids overflows usize",
                )
            })?);
        }

        let mut curr_cluster_ids = Vec::with_capacity(self.curr_cluster_ids.len());
        for id in self.curr_cluster_ids {
            if id < 0 {
                return Err(CoreError::new(
                    CoreErrorCode::BackboneInconsistency,
                    format!("BackboneAssertion curr_cluster_ids contains negative id {id}"),
                ));
            }
            curr_cluster_ids.push(usize::try_from(id).map_err(|_| {
                CoreError::new(
                    CoreErrorCode::SizeOverflow,
                    "BackboneAssertion curr_cluster_ids overflows usize",
                )
            })?);
        }

        let mut primary_links = Vec::with_capacity(self.primary_links.len());
        for (pos, pl) in self.primary_links.into_iter().enumerate() {
            primary_links.push(pl.into_pair(pos)?);
        }

        Ok(BackboneAssertion {
            prev_cluster_ids,
            curr_cluster_ids,
            threshold: self.threshold,
            distance_matrix: self.distance_matrix,
            eligible_matrix: self.eligible_matrix,
            primary_links,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairCaseDto {
    #[serde(default)]
    #[allow(dead_code)]
    schema_version: Option<String>,
    #[serde(default)]
    case_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    task_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    description: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    prev_scenarios: Vec<ScenarioNodeDto>,
    curr_scenarios: Vec<ScenarioNodeDto>,
    #[serde(deserialize_with = "deserialize_vec_f64_lenient")]
    weights: Vec<f64>,
    #[serde(default, deserialize_with = "deserialize_opt_f64_lenient")]
    threshold: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_opt_f64_lenient")]
    threshold_coefficient: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_opt_f64_lenient")]
    prev_dispersion: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_opt_f64_lenient")]
    curr_dispersion: Option<f64>,
    #[serde(default)]
    config: Option<EvolutionConfigDto>,
    #[serde(default)]
    backbone: Option<BackboneAssertionDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairBatchEnvelopeDto {
    #[serde(default)]
    #[allow(dead_code)]
    schema_version: Option<String>,
    cases: Vec<PairCaseDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberDto {
    #[serde(default)]
    member_index: Option<i64>,
    #[serde(default)]
    member_id: Option<String>,
    #[serde(deserialize_with = "deserialize_vec_f64_lenient")]
    features: Vec<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterConfigDto {
    projection_dim: i64,
    cluster_count: i64,
    initialization: String,
    seed: i64,
    restarts: i64,
    #[serde(default)]
    initial_centers: Vec<Vec<Vec<f64>>>,
    stopping_rule: String,
    #[serde(deserialize_with = "deserialize_f64_lenient")]
    tolerance: f64,
    max_iterations: i64,
    #[serde(deserialize_with = "deserialize_f64_lenient")]
    rank_threshold: f64,
}

impl ClusterConfigDto {
    fn to_core(&self) -> Result<ScenarioConfig, CoreError> {
        if self.projection_dim <= 0
            || self.cluster_count <= 0
            || self.restarts <= 0
            || self.max_iterations <= 0
            || self.seed < 0
        {
            return Err(CoreError::new(
                CoreErrorCode::InvalidConfig,
                "ClusterConfigDto integer parameters must be positive (seed >= 0)",
            ));
        }
        let initialization = match self.initialization.as_str() {
            "kmeans++_chacha8_v1" => InitMethod::KMeansPlusPlusChaCha8V1,
            "explicit_centers_v1" => InitMethod::ExplicitCentersV1,
            other => {
                return Err(CoreError::new(
                    CoreErrorCode::InvalidConfig,
                    format!("Unsupported initialization: {other}"),
                ));
            }
        };
        let stopping_rule = match self.stopping_rule.as_str() {
            "sklearn_variance_scaled_v1" => StopRule::SklearnVarianceScaledV1,
            other => {
                return Err(CoreError::new(
                    CoreErrorCode::InvalidConfig,
                    format!("Unsupported stopping_rule: {other}"),
                ));
            }
        };
        Ok(ScenarioConfig {
            projection_dim: self.projection_dim as usize,
            cluster_count: self.cluster_count as usize,
            initialization,
            seed: self.seed as u64,
            restarts: self.restarts as usize,
            initial_centers: self.initial_centers.clone(),
            stopping_rule,
            tolerance: self.tolerance,
            max_iterations: self.max_iterations as usize,
            rank_threshold: self.rank_threshold,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChainBatchDto {
    ordinal: i64,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    error_code: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    status_reason: Option<String>,
    #[serde(default, deserialize_with = "deserialize_opt_f64_lenient")]
    dispersion: Option<f64>,
    #[serde(default)]
    scenarios: Option<Vec<ScenarioNodeDto>>,
    #[serde(default)]
    members: Option<Vec<MemberDto>>,
    #[serde(default)]
    #[allow(dead_code)]
    init_time: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    lead_hours: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChainCaseDto {
    #[serde(default)]
    #[allow(dead_code)]
    schema_version: Option<String>,
    #[serde(default)]
    case_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    description: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    mode: Option<String>,
    #[serde(default)]
    run_key_prefix: Option<String>,
    #[serde(default)]
    run_key: Option<String>,
    #[serde(default)]
    tracking_run_key: Option<String>,
    #[serde(deserialize_with = "deserialize_f64_lenient")]
    threshold_coefficient: f64,
    #[serde(deserialize_with = "deserialize_vec_f64_lenient")]
    weights: Vec<f64>,
    #[serde(default)]
    config: Option<EvolutionConfigDto>,
    #[serde(default)]
    cluster_config: Option<ClusterConfigDto>,
    batches: Vec<ChainBatchDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChainBatchEnvelopeDto {
    #[serde(default)]
    #[allow(dead_code)]
    schema_version: Option<String>,
    cases: Vec<ChainCaseDto>,
}

// ---------------------------------------------------------------------------
// Output DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct MatchedLinkDto {
    prev_cluster_id: usize,
    curr_cluster_id: usize,
    distance_k: f64,
    distance_ratio: Option<f64>,
    substitutability: String,
    substitutability_gap: Option<f64>,
    alt_objective: Option<f64>,
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
struct BackboneMatchDto {
    objective: MatchObjectiveDto,
    links: Vec<MatchedLinkDto>,
    unmatched_prev: Vec<UnmatchedScenarioDto>,
    unmatched_curr: Vec<UnmatchedScenarioDto>,
    distance_matrix: Vec<Vec<f64>>,
    pair_scale: Option<f64>,
    threshold_coefficient: Option<f64>,
    prev_dispersion: Option<f64>,
    curr_dispersion: Option<f64>,
    optima_count: usize,
}

impl TryFrom<MatchResult> for BackboneMatchDto {
    type Error = CoreError;

    fn try_from(m: MatchResult) -> Result<Self, CoreError> {
        let weighted_distance_sum = checked_finite(
            m.objective.weighted_distance_sum,
            "backbone.weighted_distance_sum",
        )?;
        let pair_scale = checked_opt_finite(m.pair_scale, "backbone.pair_scale")?;
        let threshold_coefficient =
            checked_opt_finite(m.threshold_coefficient, "backbone.threshold_coefficient")?;
        let prev_dispersion = checked_opt_finite(m.prev_dispersion, "backbone.prev_dispersion")?;
        let curr_dispersion = checked_opt_finite(m.curr_dispersion, "backbone.curr_dispersion")?;

        let mut links = Vec::with_capacity(m.links.len());
        for l in m.links {
            let (sub_str, gap, alt_obj, alt_cnt) = match l.substitutability {
                Substitutability::Tied => ("tied".to_string(), None, None, None),
                Substitutability::SameCount { gap, alt_objective } => (
                    "same_count".to_string(),
                    Some(checked_finite(gap, "backbone.links.substitutability_gap")?),
                    Some(checked_finite(
                        alt_objective,
                        "backbone.links.alt_objective",
                    )?),
                    None,
                ),
                Substitutability::CountDrop {
                    alt_count,
                    alt_objective,
                } => (
                    "count_drop".to_string(),
                    None,
                    Some(checked_finite(
                        alt_objective,
                        "backbone.links.alt_objective",
                    )?),
                    Some(alt_count),
                ),
            };
            let distance_k = checked_finite(l.distance_k, "backbone.links.distance_k")?;
            let distance_ratio = match (l.distance_ratio, pair_scale) {
                (Some(r), _) => Some(checked_finite(r, "backbone.links.distance_ratio")?),
                (None, Some(s)) => Some(checked_finite(
                    distance_k / s,
                    "backbone.links.distance_ratio",
                )?),
                (None, None) => None,
            };
            links.push(MatchedLinkDto {
                prev_cluster_id: l.prev_cluster_id,
                curr_cluster_id: l.curr_cluster_id,
                distance_k,
                distance_ratio,
                substitutability: sub_str,
                substitutability_gap: gap,
                alt_objective: alt_obj,
                alt_count: alt_cnt,
                ambiguous: l.ambiguous,
            });
        }

        let mut distance_matrix = Vec::with_capacity(m.distance_matrix.len());
        for r in m.distance_matrix {
            let mut row = Vec::with_capacity(r.len());
            for cell in r {
                row.push(checked_finite(cell, "backbone.distance_matrix")?);
            }
            distance_matrix.push(row);
        }

        Ok(Self {
            objective: MatchObjectiveDto {
                matched_count: m.objective.matched_count,
                weighted_distance_sum,
            },
            links,
            unmatched_prev: m
                .unmatched_prev
                .into_iter()
                .map(|u| UnmatchedScenarioDto {
                    cluster_id: u.cluster_id,
                    reason: u.reason,
                    matched_in_some_optimum: u.matched_in_some_optimum,
                })
                .collect(),
            unmatched_curr: m
                .unmatched_curr
                .into_iter()
                .map(|u| UnmatchedScenarioDto {
                    cluster_id: u.cluster_id,
                    reason: u.reason,
                    matched_in_some_optimum: u.matched_in_some_optimum,
                })
                .collect(),
            distance_matrix,
            pair_scale,
            threshold_coefficient,
            prev_dispersion,
            curr_dispersion,
            optima_count: m.optima_count,
        })
    }
}

#[derive(Debug, Serialize)]
struct EvolutionEdgeDto {
    prev_cluster_id: usize,
    curr_cluster_id: usize,
    edge_role: String,
    event_type: String,
    rule_basis: String,
    distance: f64,
    distance_over_scale: Option<f64>,
    prev_member_fraction: f64,
    curr_member_fraction: f64,
    edge_delta_fraction: f64,
    attributed_flow: f64,
    edge_strength: f64,
    threshold_margin: f64,
    threshold_margin_ratio: Option<f64>,
    primary_alternative_status: String,
    optimality_margin: Option<f64>,
    optimality_margin_ratio: Option<f64>,
    secondary_margin: Option<f64>,
    secondary_margin_ratio: Option<f64>,
    ambiguous_tie: bool,
    high_uncertainty: bool,
    uncertainty_reasons: Vec<String>,
}

impl From<EvolutionEdge> for EvolutionEdgeDto {
    fn from(e: EvolutionEdge) -> Self {
        Self {
            prev_cluster_id: e.prev_cluster_id,
            curr_cluster_id: e.curr_cluster_id,
            edge_role: e.edge_role.as_str().to_string(),
            event_type: e.event_type.as_str().to_string(),
            rule_basis: e.rule_basis,
            distance: norm_f64(e.distance),
            distance_over_scale: e.distance_over_scale.map(norm_f64),
            prev_member_fraction: norm_f64(e.prev_member_fraction),
            curr_member_fraction: norm_f64(e.curr_member_fraction),
            edge_delta_fraction: norm_f64(e.edge_delta_fraction),
            attributed_flow: norm_f64(e.attributed_flow),
            edge_strength: norm_f64(e.edge_strength),
            threshold_margin: norm_f64(e.threshold_margin),
            threshold_margin_ratio: e.threshold_margin_ratio.map(norm_f64),
            primary_alternative_status: e.primary_alternative_status.as_str().to_string(),
            optimality_margin: e.optimality_margin.map(norm_f64),
            optimality_margin_ratio: e.optimality_margin_ratio.map(norm_f64),
            secondary_margin: e.secondary_margin.map(norm_f64),
            secondary_margin_ratio: e.secondary_margin_ratio.map(norm_f64),
            ambiguous_tie: e.ambiguous_tie,
            high_uncertainty: e.high_uncertainty,
            uncertainty_reasons: e
                .uncertainty_reasons
                .into_iter()
                .map(|r| r.as_str().to_string())
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
struct NodeEvolutionSummaryDto {
    cluster_id: usize,
    canonical_rank: usize,
    member_count: usize,
    member_fraction: f64,
    primary_degree: usize,
    secondary_degree: usize,
    node_role: String,
    participates_in_split: bool,
    participates_in_merge: bool,
    unmatched_reason: Option<String>,
    min_candidate_distance: Option<f64>,
    min_candidate_distance_over_scale: Option<f64>,
    primary_counterpart_cluster_id: Option<usize>,
    primary_delta_fraction: Option<f64>,
    attributed_flow_fraction: f64,
    unattributed_fraction: f64,
    threshold_margin: Option<f64>,
    ambiguous_tie: bool,
    high_uncertainty: bool,
}

impl From<NodeEvolutionSummary> for NodeEvolutionSummaryDto {
    fn from(n: NodeEvolutionSummary) -> Self {
        Self {
            cluster_id: n.cluster_id,
            canonical_rank: n.canonical_rank,
            member_count: n.member_count,
            member_fraction: norm_f64(n.member_fraction),
            primary_degree: n.primary_degree,
            secondary_degree: n.secondary_degree,
            node_role: n.node_role.as_str().to_string(),
            participates_in_split: n.participates_in_split,
            participates_in_merge: n.participates_in_merge,
            unmatched_reason: n.unmatched_reason.map(|r| r.as_str().to_string()),
            min_candidate_distance: n.min_candidate_distance.map(norm_f64),
            min_candidate_distance_over_scale: n.min_candidate_distance_over_scale.map(norm_f64),
            primary_counterpart_cluster_id: n.primary_counterpart_cluster_id,
            primary_delta_fraction: n.primary_delta_fraction.map(norm_f64),
            attributed_flow_fraction: norm_f64(n.attributed_flow_fraction),
            unattributed_fraction: norm_f64(n.unattributed_fraction),
            threshold_margin: n.threshold_margin.map(norm_f64),
            ambiguous_tie: n.ambiguous_tie,
            high_uncertainty: n.high_uncertainty,
        }
    }
}

#[derive(Debug, Serialize)]
struct EvolutionEventDto {
    event_index: usize,
    event_type: String,
    prev_cluster_ids: Vec<usize>,
    curr_cluster_ids: Vec<usize>,
    primary_link: Option<(usize, usize)>,
    rule_basis: String,
    max_morphology_distance: f64,
    max_morphology_over_scale: Option<f64>,
    prev_fraction_sum: f64,
    curr_fraction_sum: f64,
    net_delta_fraction: f64,
    attributed_flow_sum: f64,
    min_margin_ratio: Option<f64>,
    ambiguous_tie: bool,
    high_uncertainty: bool,
}

impl From<EvolutionEvent> for EvolutionEventDto {
    fn from(ev: EvolutionEvent) -> Self {
        Self {
            event_index: ev.event_index,
            event_type: ev.event_type.as_str().to_string(),
            prev_cluster_ids: ev.prev_cluster_ids,
            curr_cluster_ids: ev.curr_cluster_ids,
            primary_link: ev.primary_link,
            rule_basis: ev.rule_basis,
            max_morphology_distance: norm_f64(ev.max_morphology_distance),
            max_morphology_over_scale: ev.max_morphology_over_scale.map(norm_f64),
            prev_fraction_sum: norm_f64(ev.prev_fraction_sum),
            curr_fraction_sum: norm_f64(ev.curr_fraction_sum),
            net_delta_fraction: norm_f64(ev.net_delta_fraction),
            attributed_flow_sum: norm_f64(ev.attributed_flow_sum),
            min_margin_ratio: ev.min_margin_ratio.map(norm_f64),
            ambiguous_tie: ev.ambiguous_tie,
            high_uncertainty: ev.high_uncertainty,
        }
    }
}

#[derive(Debug, Serialize)]
struct TransitionEvolutionSummaryDto {
    primary_edge_count: usize,
    secondary_split_edge_count: usize,
    secondary_merge_edge_count: usize,
    continuation_count: usize,
    split_count: usize,
    merge_count: usize,
    emergence_count: usize,
    disappearance_count: usize,
    primary_total_distance: f64,
    primary_max_distance: f64,
    primary_max_distance_over_scale: Option<f64>,
    all_edges_max_distance: f64,
    all_edges_max_distance_over_scale: Option<f64>,
    transition_tv_redistribution: f64,
    secondary_attributed_flow: f64,
    total_attributed_flow: f64,
    disappeared_fraction_sum: f64,
    emerged_fraction_sum: f64,
    component_tv_residual: f64,
    min_threshold_margin: f64,
    min_threshold_margin_ratio: Option<f64>,
    raw_matrix_threshold_margin: f64,
    min_optimality_margin: Option<f64>,
    min_optimality_margin_ratio: Option<f64>,
    min_secondary_margin: Option<f64>,
    min_secondary_margin_ratio: Option<f64>,
    ambiguous_tie: bool,
    high_uncertainty_edge_count: usize,
    high_uncertainty: bool,
}

impl From<TransitionEvolutionSummary> for TransitionEvolutionSummaryDto {
    fn from(s: TransitionEvolutionSummary) -> Self {
        Self {
            primary_edge_count: s.primary_edge_count,
            secondary_split_edge_count: s.secondary_split_edge_count,
            secondary_merge_edge_count: s.secondary_merge_edge_count,
            continuation_count: s.continuation_count,
            split_count: s.split_count,
            merge_count: s.merge_count,
            emergence_count: s.emergence_count,
            disappearance_count: s.disappearance_count,
            primary_total_distance: norm_f64(s.primary_total_distance),
            primary_max_distance: norm_f64(s.primary_max_distance),
            primary_max_distance_over_scale: s.primary_max_distance_over_scale.map(norm_f64),
            all_edges_max_distance: norm_f64(s.all_edges_max_distance),
            all_edges_max_distance_over_scale: s.all_edges_max_distance_over_scale.map(norm_f64),
            transition_tv_redistribution: norm_f64(s.transition_tv_redistribution),
            secondary_attributed_flow: norm_f64(s.secondary_attributed_flow),
            total_attributed_flow: norm_f64(s.total_attributed_flow),
            disappeared_fraction_sum: norm_f64(s.disappeared_fraction_sum),
            emerged_fraction_sum: norm_f64(s.emerged_fraction_sum),
            component_tv_residual: norm_f64(s.component_tv_residual),
            min_threshold_margin: norm_f64(s.min_threshold_margin),
            min_threshold_margin_ratio: s.min_threshold_margin_ratio.map(norm_f64),
            raw_matrix_threshold_margin: norm_f64(s.raw_matrix_threshold_margin),
            min_optimality_margin: s.min_optimality_margin.map(norm_f64),
            min_optimality_margin_ratio: s.min_optimality_margin_ratio.map(norm_f64),
            min_secondary_margin: s.min_secondary_margin.map(norm_f64),
            min_secondary_margin_ratio: s.min_secondary_margin_ratio.map(norm_f64),
            ambiguous_tie: s.ambiguous_tie,
            high_uncertainty_edge_count: s.high_uncertainty_edge_count,
            high_uncertainty: s.high_uncertainty,
        }
    }
}

#[derive(Debug, Serialize)]
struct EvolutionResultDto {
    schema_version: String,
    algorithm_version: String,
    secondary_rule: String,
    flow_rule: String,
    uncertainty_rule: String,
    non_physical_disclaimer: String,
    prev_cluster_ids: Vec<usize>,
    curr_cluster_ids: Vec<usize>,
    threshold: f64,
    pair_scale: Option<f64>,
    threshold_coefficient: Option<f64>,
    prev_dispersion: Option<f64>,
    curr_dispersion: Option<f64>,
    reference_scale: Option<f64>,
    backbone: BackboneMatchDto,
    edges: Vec<EvolutionEdgeDto>,
    prev_nodes: Vec<NodeEvolutionSummaryDto>,
    curr_nodes: Vec<NodeEvolutionSummaryDto>,
    events: Vec<EvolutionEventDto>,
    summary: TransitionEvolutionSummaryDto,
}

impl TryFrom<EvolutionResult> for EvolutionResultDto {
    type Error = CoreError;

    fn try_from(r: EvolutionResult) -> Result<Self, CoreError> {
        Ok(Self {
            schema_version: "m3.evolve-result.v1".to_string(),
            algorithm_version: r.algorithm_version,
            secondary_rule: r.secondary_rule,
            flow_rule: r.flow_rule,
            uncertainty_rule: r.uncertainty_rule,
            non_physical_disclaimer: r.non_physical_disclaimer,
            prev_cluster_ids: r.prev_cluster_ids,
            curr_cluster_ids: r.curr_cluster_ids,
            threshold: checked_finite(r.threshold, "evolution.threshold")?,
            pair_scale: checked_opt_finite(r.pair_scale, "evolution.pair_scale")?,
            threshold_coefficient: checked_opt_finite(
                r.threshold_coefficient,
                "evolution.threshold_coefficient",
            )?,
            prev_dispersion: checked_opt_finite(r.prev_dispersion, "evolution.prev_dispersion")?,
            curr_dispersion: checked_opt_finite(r.curr_dispersion, "evolution.curr_dispersion")?,
            reference_scale: checked_opt_finite(r.reference_scale, "evolution.reference_scale")?,
            backbone: r.backbone.try_into()?,
            edges: r.edges.into_iter().map(Into::into).collect(),
            prev_nodes: r.prev_nodes.into_iter().map(Into::into).collect(),
            curr_nodes: r.curr_nodes.into_iter().map(Into::into).collect(),
            events: r.events.into_iter().map(Into::into).collect(),
            summary: r.summary.into(),
        })
    }
}

// ---------------------------------------------------------------------------
// Chain Output DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ChainResolvedBatchDto {
    ordinal: usize,
    status: String,
    status_code: Option<String>,
    status_reason: Option<String>,
    dispersion: Option<f64>,
}

impl From<ChainResolvedBatch> for ChainResolvedBatchDto {
    fn from(b: ChainResolvedBatch) -> Self {
        Self {
            ordinal: b.ordinal,
            status: b.status,
            status_code: b.status_code,
            status_reason: b.status_reason,
            dispersion: b.dispersion.map(norm_f64),
        }
    }
}

#[derive(Debug, Serialize)]
struct ChainNodeSummaryDto {
    ordinal: usize,
    cluster_id: usize,
    canonical_rank: usize,
    branch_id: String,
    mean_features: Vec<f64>,
    member_count: usize,
    member_fraction: f64,
    assignment_reason: String,
    in_degree_primary: usize,
    in_degree_secondary: usize,
    out_degree_primary: usize,
    out_degree_secondary: usize,
    incoming_role: String,
    outgoing_role: String,
    primary_delta_fraction: Option<f64>,
    has_primary_delta_fraction: bool,
    unattributed_inflow_fraction: f64,
    unattributed_outflow_fraction: f64,
    high_uncertainty: bool,
}

impl From<ChainNodeSummary> for ChainNodeSummaryDto {
    fn from(n: ChainNodeSummary) -> Self {
        Self {
            ordinal: n.ordinal,
            cluster_id: n.cluster_id,
            canonical_rank: n.canonical_rank,
            branch_id: n.branch_id,
            mean_features: n.mean_features.into_iter().map(norm_f64).collect(),
            member_count: n.member_count,
            member_fraction: norm_f64(n.member_fraction),
            assignment_reason: n.assignment_reason,
            in_degree_primary: n.in_degree_primary,
            in_degree_secondary: n.in_degree_secondary,
            out_degree_primary: n.out_degree_primary,
            out_degree_secondary: n.out_degree_secondary,
            incoming_role: n.incoming_role,
            outgoing_role: n.outgoing_role,
            has_primary_delta_fraction: n.primary_delta_fraction.is_some(),
            primary_delta_fraction: n.primary_delta_fraction.map(norm_f64),
            unattributed_inflow_fraction: norm_f64(n.unattributed_inflow_fraction),
            unattributed_outflow_fraction: norm_f64(n.unattributed_outflow_fraction),
            high_uncertainty: n.high_uncertainty,
        }
    }
}

#[derive(Debug, Serialize)]
struct ChainEvolutionEdgeDto {
    prev_ordinal: usize,
    prev_cluster_id: usize,
    curr_ordinal: usize,
    curr_cluster_id: usize,
    prev_branch_id: String,
    curr_branch_id: String,
    edge_role: String,
    event_type: String,
    rule_basis: String,
    distance: f64,
    distance_over_scale: Option<f64>,
    prev_member_fraction: f64,
    curr_member_fraction: f64,
    edge_delta_fraction: f64,
    attributed_flow: f64,
    edge_strength: f64,
    threshold_margin: f64,
    threshold_margin_ratio: Option<f64>,
    primary_alternative_status: String,
    has_optimality_margin: bool,
    optimality_margin: Option<f64>,
    optimality_margin_ratio: Option<f64>,
    has_secondary_margin: bool,
    secondary_margin: Option<f64>,
    secondary_margin_ratio: Option<f64>,
    ambiguous_tie: bool,
    high_uncertainty: bool,
    uncertainty_reasons: Vec<String>,
    non_physical_disclaimer: String,
}

impl From<ChainEvolutionEdge> for ChainEvolutionEdgeDto {
    fn from(ce: ChainEvolutionEdge) -> Self {
        let e = ce.edge;
        Self {
            prev_ordinal: ce.prev_ordinal,
            prev_cluster_id: e.prev_cluster_id,
            curr_ordinal: ce.curr_ordinal,
            curr_cluster_id: e.curr_cluster_id,
            prev_branch_id: ce.prev_branch_id,
            curr_branch_id: ce.curr_branch_id,
            edge_role: e.edge_role.as_str().to_string(),
            event_type: e.event_type.as_str().to_string(),
            rule_basis: e.rule_basis,
            distance: norm_f64(e.distance),
            distance_over_scale: e.distance_over_scale.map(norm_f64),
            prev_member_fraction: norm_f64(e.prev_member_fraction),
            curr_member_fraction: norm_f64(e.curr_member_fraction),
            edge_delta_fraction: norm_f64(e.edge_delta_fraction),
            attributed_flow: norm_f64(e.attributed_flow),
            edge_strength: norm_f64(e.edge_strength),
            threshold_margin: norm_f64(e.threshold_margin),
            threshold_margin_ratio: e.threshold_margin_ratio.map(norm_f64),
            primary_alternative_status: e.primary_alternative_status.as_str().to_string(),
            has_optimality_margin: e.optimality_margin.is_some(),
            optimality_margin: e.optimality_margin.map(norm_f64),
            optimality_margin_ratio: e.optimality_margin_ratio.map(norm_f64),
            has_secondary_margin: e.secondary_margin.is_some(),
            secondary_margin: e.secondary_margin.map(norm_f64),
            secondary_margin_ratio: e.secondary_margin_ratio.map(norm_f64),
            ambiguous_tie: e.ambiguous_tie,
            high_uncertainty: e.high_uncertainty,
            uncertainty_reasons: e
                .uncertainty_reasons
                .into_iter()
                .map(|r| r.as_str().to_string())
                .collect(),
            non_physical_disclaimer: ce.non_physical_disclaimer,
        }
    }
}

#[derive(Debug, Serialize)]
struct ChainEvolutionEventDto {
    prev_ordinal: usize,
    event_index: usize,
    event_type: String,
    prev_cluster_ids: Vec<usize>,
    curr_cluster_ids: Vec<usize>,
    branch_ids: Vec<String>,
    primary_branch_id: Option<String>,
    rule_basis: String,
    max_morphology_distance: f64,
    max_morphology_over_scale: Option<f64>,
    prev_fraction_sum: f64,
    curr_fraction_sum: f64,
    net_delta_fraction: f64,
    attributed_flow_sum: f64,
    has_min_margin_ratio: bool,
    min_margin_ratio: Option<f64>,
    ambiguous_tie: bool,
    high_uncertainty: bool,
    non_physical_disclaimer: String,
}

impl From<ChainEvolutionEvent> for ChainEvolutionEventDto {
    fn from(ev: ChainEvolutionEvent) -> Self {
        Self {
            prev_ordinal: ev.prev_ordinal,
            event_index: ev.event_index,
            event_type: ev.event_type.as_str().to_string(),
            prev_cluster_ids: ev.prev_cluster_ids,
            curr_cluster_ids: ev.curr_cluster_ids,
            branch_ids: ev.branch_ids,
            primary_branch_id: ev.primary_branch_id,
            rule_basis: ev.rule_basis,
            max_morphology_distance: norm_f64(ev.max_morphology_distance),
            max_morphology_over_scale: ev.max_morphology_over_scale.map(norm_f64),
            prev_fraction_sum: norm_f64(ev.prev_fraction_sum),
            curr_fraction_sum: norm_f64(ev.curr_fraction_sum),
            net_delta_fraction: norm_f64(ev.net_delta_fraction),
            attributed_flow_sum: norm_f64(ev.attributed_flow_sum),
            has_min_margin_ratio: ev.min_margin_ratio.is_some(),
            min_margin_ratio: ev.min_margin_ratio.map(norm_f64),
            ambiguous_tie: ev.ambiguous_tie,
            high_uncertainty: ev.high_uncertainty,
            non_physical_disclaimer: ev.non_physical_disclaimer,
        }
    }
}

#[derive(Debug, Serialize)]
struct ChainTransitionRecordDto {
    prev_ordinal: usize,
    curr_ordinal: usize,
    transition_status: String,
    chain_broken: bool,
    gap_reason: Option<String>,
    pair_scale: Option<f64>,
    threshold: Option<f64>,
    primary_edge_count: usize,
    secondary_split_edge_count: usize,
    secondary_merge_edge_count: usize,
    transition_tv_redistribution: Option<f64>,
    min_threshold_margin: Option<f64>,
    ambiguous_tie: bool,
    high_uncertainty: bool,
    evolve_result: Option<EvolutionResultDto>,
    events: Vec<ChainEvolutionEventDto>,
}

impl TryFrom<ChainTransitionRecord> for ChainTransitionRecordDto {
    type Error = CoreError;

    fn try_from(t: ChainTransitionRecord) -> Result<Self, CoreError> {
        let evolve_result = match t.evolve_result {
            Some(er) => Some(er.try_into()?),
            None => None,
        };
        Ok(Self {
            prev_ordinal: t.prev_ordinal,
            curr_ordinal: t.curr_ordinal,
            transition_status: t.transition_status.as_str().to_string(),
            chain_broken: t.chain_broken,
            gap_reason: t.gap_reason,
            pair_scale: checked_opt_finite(t.pair_scale, "transition.pair_scale")?,
            threshold: checked_opt_finite(t.threshold, "transition.threshold")?,
            primary_edge_count: t.primary_edge_count,
            secondary_split_edge_count: t.secondary_split_edge_count,
            secondary_merge_edge_count: t.secondary_merge_edge_count,
            transition_tv_redistribution: checked_opt_finite(
                t.transition_tv_redistribution,
                "transition.transition_tv_redistribution",
            )?,
            min_threshold_margin: checked_opt_finite(
                t.min_threshold_margin,
                "transition.min_threshold_margin",
            )?,
            ambiguous_tie: t.ambiguous_tie,
            high_uncertainty: t.high_uncertainty,
            evolve_result,
            events: t.events.into_iter().map(Into::into).collect(),
        })
    }
}

#[derive(Debug, Serialize)]
struct EvolutionBranchSummaryDto {
    branch_id: String,
    birth_ordinal: usize,
    end_ordinal: usize,
    node_count: usize,
    step_count: usize,
    birth_reason: String,
    termination_reason: String,
    cumulative_morphology_distance: f64,
    cumulative_morphology_over_scale: f64,
    max_step_morphology_distance: f64,
    max_step_morphology_over_scale: f64,
    end_to_end_morphology_distance: f64,
    initial_member_fraction: f64,
    final_member_fraction: f64,
    min_member_fraction: f64,
    max_member_fraction: f64,
    net_delta_fraction: f64,
    cumulative_abs_delta_fraction: f64,
    split_out_event_count: usize,
    spawned_secondary_branch_ids: Vec<String>,
    split_from_parent_branch_id: Option<String>,
    merge_in_event_count: usize,
    absorbed_secondary_branch_ids: Vec<String>,
    merged_into_branch_id: Option<String>,
    ambiguous_step_count: usize,
    high_uncertainty_step_count: usize,
    high_uncertainty_step_ratio: f64,
}

impl From<EvolutionBranchSummary> for EvolutionBranchSummaryDto {
    fn from(b: EvolutionBranchSummary) -> Self {
        Self {
            branch_id: b.branch_id,
            birth_ordinal: b.birth_ordinal,
            end_ordinal: b.end_ordinal,
            node_count: b.node_count,
            step_count: b.step_count,
            birth_reason: b.birth_reason,
            termination_reason: b.termination_reason,
            cumulative_morphology_distance: norm_f64(b.cumulative_morphology_distance),
            cumulative_morphology_over_scale: norm_f64(b.cumulative_morphology_over_scale),
            max_step_morphology_distance: norm_f64(b.max_step_morphology_distance),
            max_step_morphology_over_scale: norm_f64(b.max_step_morphology_over_scale),
            end_to_end_morphology_distance: norm_f64(b.end_to_end_morphology_distance),
            initial_member_fraction: norm_f64(b.initial_member_fraction),
            final_member_fraction: norm_f64(b.final_member_fraction),
            min_member_fraction: norm_f64(b.min_member_fraction),
            max_member_fraction: norm_f64(b.max_member_fraction),
            net_delta_fraction: norm_f64(b.net_delta_fraction),
            cumulative_abs_delta_fraction: norm_f64(b.cumulative_abs_delta_fraction),
            split_out_event_count: b.split_out_event_count,
            spawned_secondary_branch_ids: b.spawned_secondary_branch_ids,
            split_from_parent_branch_id: b.split_from_parent_branch_id,
            merge_in_event_count: b.merge_in_event_count,
            absorbed_secondary_branch_ids: b.absorbed_secondary_branch_ids,
            merged_into_branch_id: b.merged_into_branch_id,
            ambiguous_step_count: b.ambiguous_step_count,
            high_uncertainty_step_count: b.high_uncertainty_step_count,
            high_uncertainty_step_ratio: norm_f64(b.high_uncertainty_step_ratio),
        }
    }
}

#[derive(Debug, Serialize)]
struct EvolutionChainSummaryDto {
    ok_batch_count: usize,
    missing_batch_count: usize,
    failed_batch_count: usize,
    ok_transition_count: usize,
    gap_transition_count: usize,
    branch_count: usize,
    primary_edge_count: usize,
    secondary_split_edge_count: usize,
    secondary_merge_edge_count: usize,
    continuation_count: usize,
    split_count: usize,
    merge_count: usize,
    emergence_count: usize,
    disappearance_count: usize,
    gap_interrupted_count: usize,
    gap_restarted_count: usize,
    cumulative_primary_distance: f64,
    max_edge_distance: f64,
    max_edge_distance_over_scale: f64,
    mean_transition_tv_redistribution: f64,
    max_transition_tv_redistribution: f64,
    ambiguous_transition_count: usize,
    high_uncertainty_transition_count: usize,
    high_uncertainty_transition_ratio: f64,
    high_uncertainty_edge_count: usize,
    high_uncertainty_edge_ratio: f64,
}

impl From<EvolutionChainSummary> for EvolutionChainSummaryDto {
    fn from(s: EvolutionChainSummary) -> Self {
        Self {
            ok_batch_count: s.ok_batch_count,
            missing_batch_count: s.missing_batch_count,
            failed_batch_count: s.failed_batch_count,
            ok_transition_count: s.ok_transition_count,
            gap_transition_count: s.gap_transition_count,
            branch_count: s.branch_count,
            primary_edge_count: s.primary_edge_count,
            secondary_split_edge_count: s.secondary_split_edge_count,
            secondary_merge_edge_count: s.secondary_merge_edge_count,
            continuation_count: s.continuation_count,
            split_count: s.split_count,
            merge_count: s.merge_count,
            emergence_count: s.emergence_count,
            disappearance_count: s.disappearance_count,
            gap_interrupted_count: s.gap_interrupted_count,
            gap_restarted_count: s.gap_restarted_count,
            cumulative_primary_distance: norm_f64(s.cumulative_primary_distance),
            max_edge_distance: norm_f64(s.max_edge_distance),
            max_edge_distance_over_scale: norm_f64(s.max_edge_distance_over_scale),
            mean_transition_tv_redistribution: norm_f64(s.mean_transition_tv_redistribution),
            max_transition_tv_redistribution: norm_f64(s.max_transition_tv_redistribution),
            ambiguous_transition_count: s.ambiguous_transition_count,
            high_uncertainty_transition_count: s.high_uncertainty_transition_count,
            high_uncertainty_transition_ratio: norm_f64(s.high_uncertainty_transition_ratio),
            high_uncertainty_edge_count: s.high_uncertainty_edge_count,
            high_uncertainty_edge_ratio: norm_f64(s.high_uncertainty_edge_ratio),
        }
    }
}

#[derive(Debug, Serialize)]
struct EvolutionChainResultDto {
    schema_version: String,
    algorithm_version: String,
    secondary_rule: String,
    flow_rule: String,
    uncertainty_rule: String,
    non_physical_disclaimer: String,
    run_key_prefix: String,
    planned_batch_count: usize,
    threshold_coefficient: f64,
    batches: Vec<ChainResolvedBatchDto>,
    nodes: Vec<ChainNodeSummaryDto>,
    transitions: Vec<ChainTransitionRecordDto>,
    edges: Vec<ChainEvolutionEdgeDto>,
    events: Vec<ChainEvolutionEventDto>,
    branches: Vec<EvolutionBranchSummaryDto>,
    chain_summary: EvolutionChainSummaryDto,
}

impl TryFrom<EvolutionChainResult> for EvolutionChainResultDto {
    type Error = CoreError;

    fn try_from(r: EvolutionChainResult) -> Result<Self, CoreError> {
        let transitions = r
            .transitions
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, CoreError>>()?;
        Ok(Self {
            schema_version: "m3.chain-result.v1".to_string(),
            algorithm_version: r.algorithm_version,
            secondary_rule: r.secondary_rule,
            flow_rule: r.flow_rule,
            uncertainty_rule: r.uncertainty_rule,
            non_physical_disclaimer: r.non_physical_disclaimer,
            run_key_prefix: r.run_key_prefix,
            planned_batch_count: r.planned_batch_count,
            threshold_coefficient: checked_finite(
                r.threshold_coefficient,
                "chain.threshold_coefficient",
            )?,
            batches: r.batches.into_iter().map(Into::into).collect(),
            nodes: r.nodes.into_iter().map(Into::into).collect(),
            transitions,
            edges: r.edges.into_iter().map(Into::into).collect(),
            events: r.events.into_iter().map(Into::into).collect(),
            branches: r.branches.into_iter().map(Into::into).collect(),
            chain_summary: r.chain_summary.into(),
        })
    }
}

// ---------------------------------------------------------------------------
// Batch Mode Item Output Envelope
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum BatchItemResultDto<T> {
    Ok {
        #[serde(skip_serializing_if = "Option::is_none")]
        case_id: Option<String>,
        result: T,
    },
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        case_id: Option<String>,
        code: String,
        message: String,
        prev_index: Option<u32>,
        curr_index: Option<u32>,
        feature_index: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        member_index: Option<usize>,
    },
}

impl<T> BatchItemResultDto<T> {
    fn from_outcome(case_id: Option<String>, outcome: Result<T, CoreError>) -> Self {
        match outcome {
            Ok(result) => Self::Ok { case_id, result },
            Err(err) => Self::Error {
                case_id,
                code: err.code.as_str().to_string(),
                message: err.message,
                prev_index: err.prev_index,
                curr_index: err.curr_index,
                feature_index: err.feature_index,
                member_index: err.member_index,
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct BatchOutputEnvelopeDto<T> {
    schema_version: String,
    mode: String,
    results: Vec<BatchItemResultDto<T>>,
}

// ---------------------------------------------------------------------------
// Execution Helpers & CLI Parsing
// ---------------------------------------------------------------------------

enum RunnerError {
    Usage(String),
    Core(CoreError),
}

impl From<CoreError> for RunnerError {
    fn from(e: CoreError) -> Self {
        Self::Core(e)
    }
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!(
        "Usage: m3_runner --mode pair|chain --input <PATH> --output <PATH> [--scaled]\n\
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
                eprintln!("[UNKNOWN_ARG] Unknown flag: {unknown}");
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
        } else if text[byte_idx..].starts_with("+Infinity") {
            out.push_str("\"+Infinity\"");
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

fn rand_suffix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn atomic_write_json(path: &Path, content: &str) -> Result<(), RunnerError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|e| {
        RunnerError::Usage(format!("Failed to create parent dir {:?}: {e}", parent))
    })?;

    let tmp_path = parent.join(format!(".tmp_m3_{}_{}.json", process::id(), rand_suffix()));
    let mut tmp_file = File::create(&tmp_path).map_err(|e| {
        RunnerError::Usage(format!(
            "Failed to create temporary file {:?}: {e}",
            tmp_path
        ))
    })?;

    tmp_file
        .write_all(content.as_bytes())
        .and_then(|_| tmp_file.write_all(b"\n"))
        .map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            RunnerError::Usage(format!("Failed writing output content: {e}"))
        })?;

    fs::rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        RunnerError::Usage(format!("Failed renaming tmp file to {:?}: {e}", path))
    })?;

    Ok(())
}

fn execute_pair_case(
    case: PairCaseDto,
    force_scaled: bool,
) -> Result<EvolutionResultDto, CoreError> {
    let config = match case.config {
        Some(c) => c.into_core()?,
        None => EvolutionConfig::default(),
    };
    let prev_scenarios: Vec<ScenarioNodeInput> = case
        .prev_scenarios
        .into_iter()
        .enumerate()
        .map(|(i, sc)| sc.into_prev_core(i))
        .collect::<Result<Vec<_>, CoreError>>()?;
    let curr_scenarios: Vec<ScenarioNodeInput> = case
        .curr_scenarios
        .into_iter()
        .enumerate()
        .map(|(j, sc)| sc.into_curr_core(j))
        .collect::<Result<Vec<_>, CoreError>>()?;
    let backbone = match case.backbone {
        Some(b) => Some(b.into_core()?),
        None => None,
    };

    let is_scaled = force_scaled
        || case.mode.as_deref() == Some("scaled")
        || case.threshold_coefficient.is_some()
        || case.prev_dispersion.is_some()
        || case.curr_dispersion.is_some();

    let res = if is_scaled {
        let c = case.threshold_coefficient.ok_or_else(|| {
            CoreError::new(
                CoreErrorCode::InvalidThreshold,
                "threshold_coefficient missing for scaled evolution mode",
            )
        })?;
        let sp = case.prev_dispersion.ok_or_else(|| {
            CoreError::new(
                CoreErrorCode::ZeroDispersion,
                "prev_dispersion missing for scaled evolution mode",
            )
        })?;
        let sc = case.curr_dispersion.ok_or_else(|| {
            CoreError::new(
                CoreErrorCode::ZeroDispersion,
                "curr_dispersion missing for scaled evolution mode",
            )
        })?;
        evolve_scenarios_scaled(&ScaledEvolutionInput {
            prev_scenarios,
            curr_scenarios,
            weights: case.weights,
            threshold_coefficient: c,
            prev_dispersion: sp,
            curr_dispersion: sc,
            config,
            backbone,
        })?
    } else {
        let tau = case.threshold.ok_or_else(|| {
            CoreError::new(
                CoreErrorCode::InvalidThreshold,
                "threshold missing for absolute evolution mode",
            )
        })?;
        evolve_scenarios(&EvolutionInput {
            prev_scenarios,
            curr_scenarios,
            weights: case.weights,
            threshold: tau,
            config,
            backbone,
        })?
    };

    res.try_into()
}

fn is_whitelisted_single_batch_failure(code: CoreErrorCode) -> bool {
    matches!(
        code,
        CoreErrorCode::InsufficientRank
            | CoreErrorCode::AmbiguousSubspace
            | CoreErrorCode::EmptyCluster
            | CoreErrorCode::NotConverged
            | CoreErrorCode::InsufficientDistinctVectors
            | CoreErrorCode::InsufficientProjectedVectors
            | CoreErrorCode::SvdNotConverged
            | CoreErrorCode::ZeroDispersion
    )
}

fn execute_chain_case(case: ChainCaseDto) -> Result<EvolutionChainResultDto, CoreError> {
    let config = match case.config {
        Some(c) => c.into_core()?,
        None => EvolutionConfig::default(),
    };
    let cluster_cfg = match &case.cluster_config {
        Some(c) => Some(c.to_core()?),
        None => None,
    };

    let run_key_prefix = case
        .run_key_prefix
        .or(case.run_key)
        .or(case.tracking_run_key);

    let mut batches_in = Vec::with_capacity(case.batches.len());
    for (b_pos, b) in case.batches.into_iter().enumerate() {
        if b.ordinal < 0 {
            return Err(CoreError::new(
                CoreErrorCode::InvalidEvolutionConfig,
                format!(
                    "batches[{b_pos}].ordinal must be non-negative, got {}",
                    b.ordinal
                ),
            )
            .with_prev_index(b_pos));
        }
        let ord = usize::try_from(b.ordinal).map_err(|_| {
            CoreError::new(
                CoreErrorCode::SizeOverflow,
                format!("batches[{b_pos}].ordinal overflows usize"),
            )
            .with_prev_index(b_pos)
        })?;

        let status_str = b.status.as_deref().unwrap_or("ok");
        match status_str {
            "missing" => {
                let reason = b
                    .reason
                    .or(b.status_reason)
                    .unwrap_or_else(|| "batch_missing".to_string());
                batches_in.push(ChainBatchInput {
                    ordinal: ord,
                    status: ChainBatchStatus::Missing { reason },
                });
            }
            "failed" => {
                let code = b.code.or(b.error_code).unwrap_or_default();
                let reason = b
                    .reason
                    .or(b.status_reason)
                    .unwrap_or_else(|| "batch_failed".to_string());
                batches_in.push(ChainBatchInput {
                    ordinal: ord,
                    status: ChainBatchStatus::Failed { code, reason },
                });
            }
            "ok" => {
                if let Some(member_dtos) = b.members {
                    let cfg = cluster_cfg.as_ref().ok_or_else(|| {
                        CoreError::new(
                            CoreErrorCode::InvalidEvolutionConfig,
                            "cluster_config is required when chain batches supply raw members",
                        )
                    })?;
                    let mut members = Vec::with_capacity(member_dtos.len());
                    for (idx, m) in member_dtos.into_iter().enumerate() {
                        let member_index = match m.member_index {
                            Some(raw_idx) if raw_idx < 0 => {
                                return Err(CoreError::new(
                                    CoreErrorCode::InvalidConfig,
                                    format!(
                                        "batch {ord}: members[{idx}].member_index must be >= 0, got {raw_idx}"
                                    ),
                                )
                                .with_member_index(idx));
                            }
                            Some(raw_idx) => raw_idx as usize,
                            None => idx,
                        };
                        members.push(MemberInput {
                            member_index,
                            member_id: m.member_id.unwrap_or_else(|| format!("m_{idx}")),
                            features: m.features,
                        });
                    }

                    let disp_res = ensemble_dispersion(&members, &case.weights);
                    let disp = match disp_res {
                        Ok(s) => s,
                        Err(err) if is_whitelisted_single_batch_failure(err.code) => {
                            batches_in.push(ChainBatchInput {
                                ordinal: ord,
                                status: ChainBatchStatus::Failed {
                                    code: err.code.as_str().to_string(),
                                    reason: err.message,
                                },
                            });
                            continue;
                        }
                        Err(err) => return Err(err),
                    };

                    let task = TaskInput {
                        config: cfg.clone(),
                        members,
                    };
                    match cluster(&task) {
                        Ok(c_res) => {
                            let scs: Vec<ScenarioNodeInput> = c_res
                                .scenarios
                                .into_iter()
                                .map(|sc| ScenarioNodeInput {
                                    cluster_id: sc.cluster_id,
                                    mean_features: sc.mean_features,
                                    member_count: sc.member_count,
                                    member_fraction: sc.member_fraction,
                                })
                                .collect();
                            batches_in.push(ChainBatchInput {
                                ordinal: ord,
                                status: ChainBatchStatus::Ok {
                                    scenarios: scs,
                                    dispersion: disp,
                                },
                            });
                        }
                        Err(err) if is_whitelisted_single_batch_failure(err.code) => {
                            batches_in.push(ChainBatchInput {
                                ordinal: ord,
                                status: ChainBatchStatus::Failed {
                                    code: err.code.as_str().to_string(),
                                    reason: err.message,
                                },
                            });
                        }
                        Err(err) => return Err(err),
                    }
                } else {
                    let disp = b.dispersion.ok_or_else(|| {
                        CoreError::new(
                            CoreErrorCode::ZeroDispersion,
                            format!("Batch ordinal {ord} with status='ok' requires dispersion"),
                        )
                    })?;
                    let sc_dtos = b.scenarios.unwrap_or_default();
                    let scs: Vec<ScenarioNodeInput> = sc_dtos
                        .into_iter()
                        .enumerate()
                        .map(|(s_idx, sc)| sc.into_batch_core(ord, s_idx))
                        .collect::<Result<Vec<_>, CoreError>>()?;
                    batches_in.push(ChainBatchInput {
                        ordinal: ord,
                        status: ChainBatchStatus::Ok {
                            scenarios: scs,
                            dispersion: disp,
                        },
                    });
                }
            }
            other => {
                return Err(CoreError::new(
                    CoreErrorCode::InvalidEvolutionConfig,
                    format!("Unsupported batch status '{other}' at ordinal {ord}"),
                ));
            }
        }
    }

    let chain_res = evolve_chain(&EvolutionChainInput {
        run_key_prefix,
        batches: batches_in,
        weights: case.weights,
        threshold_coefficient: case.threshold_coefficient,
        config,
    })?;

    chain_res.try_into()
}

fn is_batch_envelope(sanitized_text: &str) -> Result<bool, RunnerError> {
    let raw_val: serde_json::Value = serde_json::from_str(sanitized_text)
        .map_err(|e| RunnerError::Usage(format!("Invalid JSON input: {e}")))?;
    let Some(obj) = raw_val.as_object() else {
        return Err(RunnerError::Usage(
            "Top-level JSON must be an object".to_string(),
        ));
    };
    Ok(obj.contains_key("cases"))
}

fn run() -> Result<(), RunnerError> {
    let parsed = parse_args();

    let input_bytes = fs::read(&parsed.input_path).map_err(|e| {
        RunnerError::Usage(format!(
            "Cannot read input file {:?}: {e}",
            parsed.input_path
        ))
    })?;
    let raw_text = String::from_utf8_lossy(&input_bytes);
    let sanitized_text = sanitize_json_non_finite(&raw_text);
    let batch_mode = is_batch_envelope(&sanitized_text)?;

    let json_out = match parsed.mode.as_str() {
        "pair" => {
            if batch_mode {
                let batch_env: PairBatchEnvelopeDto = serde_json::from_str(&sanitized_text)
                    .map_err(|e| RunnerError::Usage(format!("Invalid JSON in pair batch: {e}")))?;
                let mut results = Vec::with_capacity(batch_env.cases.len());
                for mut c in batch_env.cases {
                    let cid = c.case_id.take();
                    let out = execute_pair_case(c, parsed.scaled);
                    results.push(BatchItemResultDto::from_outcome(cid, out));
                }
                let out_env = BatchOutputEnvelopeDto {
                    schema_version: "m3.batch-result.v1".to_string(),
                    mode: "pair".to_string(),
                    results,
                };
                serde_json::to_string_pretty(&out_env)
                    .map_err(|e| RunnerError::Usage(format!("JSON serialization failed: {e}")))?
            } else {
                let single: PairCaseDto = serde_json::from_str(&sanitized_text)
                    .map_err(|e| RunnerError::Usage(format!("Invalid JSON in pair input: {e}")))?;
                let dto = execute_pair_case(single, parsed.scaled)?;
                serde_json::to_string_pretty(&dto)
                    .map_err(|e| RunnerError::Usage(format!("JSON serialization failed: {e}")))?
            }
        }
        "chain" => {
            if batch_mode {
                let batch_env: ChainBatchEnvelopeDto = serde_json::from_str(&sanitized_text)
                    .map_err(|e| RunnerError::Usage(format!("Invalid JSON in chain batch: {e}")))?;
                let mut results = Vec::with_capacity(batch_env.cases.len());
                for mut c in batch_env.cases {
                    let cid = c.case_id.take();
                    let out = execute_chain_case(c);
                    results.push(BatchItemResultDto::from_outcome(cid, out));
                }
                let out_env = BatchOutputEnvelopeDto {
                    schema_version: "m3.batch-result.v1".to_string(),
                    mode: "chain".to_string(),
                    results,
                };
                serde_json::to_string_pretty(&out_env)
                    .map_err(|e| RunnerError::Usage(format!("JSON serialization failed: {e}")))?
            } else {
                let single: ChainCaseDto = serde_json::from_str(&sanitized_text)
                    .map_err(|e| RunnerError::Usage(format!("Invalid JSON in chain input: {e}")))?;
                let dto = execute_chain_case(single)?;
                serde_json::to_string_pretty(&dto)
                    .map_err(|e| RunnerError::Usage(format!("JSON serialization failed: {e}")))?
            }
        }
        other => {
            return Err(RunnerError::Usage(format!(
                "Unknown --mode '{other}', expected 'pair' or 'chain'"
            )));
        }
    };

    atomic_write_json(&parsed.output_path, &json_out)?;
    Ok(())
}

fn main() {
    match run() {
        Ok(()) => {}
        Err(RunnerError::Core(err)) => {
            eprintln!("[ERROR] {err}");
            process::exit(1);
        }
        Err(RunnerError::Usage(msg)) => {
            eprintln!("[USAGE] {msg}");
            process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_and_chain_batches_isolate_negative_ids_counts_and_subnormal_overflow() {
        let pair_batch_json = r#"{
            "cases": [
                {
                    "case_id": "valid_before",
                    "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
                    "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.1]}],
                    "weights": [1.0],
                    "threshold": 1.0
                },
                {
                    "case_id": "neg_member_count",
                    "prev_scenarios": [{"cluster_id": 0, "member_count": -3, "member_fraction": 1.0, "mean_features": [0.0]}],
                    "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.1]}],
                    "weights": [1.0],
                    "threshold": 1.0
                },
                {
                    "case_id": "neg_curr_cluster_id",
                    "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
                    "curr_scenarios": [{"cluster_id": -2, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.1]}],
                    "weights": [1.0],
                    "threshold": 1.0
                },
                {
                    "case_id": "subnormal_scale_overflow",
                    "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
                    "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [5.0]}],
                    "weights": [1.0],
                    "threshold_coefficient": 1e308,
                    "prev_dispersion": 1e-308,
                    "curr_dispersion": 1e-308
                },
                {
                    "case_id": "valid_after",
                    "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [1.0]}],
                    "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [1.2]}],
                    "weights": [1.0],
                    "threshold": 1.0
                }
            ]
        }"#;

        let sanitized = sanitize_json_non_finite(pair_batch_json);
        let batch_env: PairBatchEnvelopeDto = serde_json::from_str(&sanitized)
            .expect("pair batch must deserialize with negative ints");
        let mut results = Vec::new();
        for mut c in batch_env.cases {
            let cid = c.case_id.take();
            results.push(BatchItemResultDto::from_outcome(
                cid,
                execute_pair_case(c, false),
            ));
        }
        let out_json = serde_json::to_string_pretty(&BatchOutputEnvelopeDto {
            schema_version: "m3.batch-result.v1".to_string(),
            mode: "pair".to_string(),
            results,
        })
        .expect("pair batch output must serialize cleanly without Infinity");
        let v: serde_json::Value = serde_json::from_str(&out_json).unwrap();
        let arr = v["results"].as_array().unwrap();
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[0]["status"], "ok");
        assert_eq!(arr[1]["status"], "error");
        assert_eq!(arr[1]["code"], "ZERO_MEMBER_SCENARIO");
        assert_eq!(arr[1]["prev_index"], 0);
        assert_eq!(arr[2]["status"], "error");
        assert_eq!(arr[2]["code"], "INVALID_SCENARIO_ID");
        assert_eq!(arr[2]["curr_index"], 0);
        assert_eq!(arr[3]["status"], "error");
        assert_eq!(arr[3]["code"], "NUMERICAL_FAILURE");
        assert_eq!(arr[4]["status"], "ok");

        let chain_batch_json = r#"{
            "cases": [
                {
                    "case_id": "chain_neg_id",
                    "threshold_coefficient": 1.0,
                    "weights": [1.0],
                    "batches": [
                        {"ordinal": 0, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}]},
                        {"ordinal": 1, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": -1, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.1]}]},
                        {"ordinal": 2, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.2]}]},
                        {"ordinal": 3, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.3]}]}
                    ]
                },
                {
                    "case_id": "chain_neg_mcnt",
                    "threshold_coefficient": 1.0,
                    "weights": [1.0],
                    "batches": [
                        {"ordinal": 0, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}]},
                        {"ordinal": 1, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.1]}]},
                        {"ordinal": 2, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": -5, "member_fraction": 1.0, "mean_features": [0.2]}]},
                        {"ordinal": 3, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.3]}]}
                    ]
                },
                {
                    "case_id": "chain_valid_sibling",
                    "threshold_coefficient": 1.0,
                    "weights": [1.0],
                    "batches": [
                        {"ordinal": 0, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}]},
                        {"ordinal": 1, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.1]}]},
                        {"ordinal": 2, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.2]}]},
                        {"ordinal": 3, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.3]}]}
                    ]
                }
            ]
        }"#;

        let chain_env: ChainBatchEnvelopeDto =
            serde_json::from_str(chain_batch_json).expect("chain batch must deserialize");
        let mut c_results = Vec::new();
        for mut c in chain_env.cases {
            let cid = c.case_id.take();
            c_results.push(BatchItemResultDto::from_outcome(cid, execute_chain_case(c)));
        }
        let c_json = serde_json::to_string_pretty(&BatchOutputEnvelopeDto {
            schema_version: "m3.batch-result.v1".to_string(),
            mode: "chain".to_string(),
            results: c_results,
        })
        .unwrap();
        let cv: serde_json::Value = serde_json::from_str(&c_json).unwrap();
        let carr = cv["results"].as_array().unwrap();
        assert_eq!(carr[0]["status"], "error");
        assert_eq!(carr[0]["code"], "INVALID_SCENARIO_ID");
        assert_eq!(carr[0]["prev_index"], 1);
        assert_eq!(carr[1]["status"], "error");
        assert_eq!(carr[1]["code"], "ZERO_MEMBER_SCENARIO");
        assert_eq!(carr[1]["prev_index"], 2);
        assert_eq!(carr[2]["status"], "ok");
    }
}
