//! Many-to-many scenario evolution core (`m3.evolve.v1`).
//!
//! Preserves the M2 global-optimum 1-to-1 primary backbone and persistent `branch_id`
//! verbatim, adds `nearest_feasible_v1` secondary split/merge edges, decomposes each
//! transition into mutually exclusive star components, attributes algorithmic mass flow
//! via `star_min_mass_proportional_v1`, and computes three orthogonal metric families
//! (morphology change, member fraction change, and matching/evolution uncertainty).

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::fsum::{Expansion, fsum};
use crate::matching::{
    CanonContext, Constraint, MAX_SCENARIOS, MatchInput, MatchResult, ScaledMatchInput,
    ScenarioNodeInput, SolvedMatching, Substitutability, canonical_scenario_order,
    compute_weighted_rmse, match_scenarios, match_scenarios_scaled, solve_dp,
};
use crate::{CoreError, CoreErrorCode};

pub const EVOLUTION_ALGORITHM_VERSION: &str = "m3.evolve.v1";
pub const SECONDARY_RULE_V1: &str = "nearest_feasible_v1";
pub const FLOW_RULE_V1: &str = "star_min_mass_proportional_v1";
pub const UNCERTAINTY_RULE_V1: &str = "m3.uncertainty.v1";
pub const NON_PHYSICAL_DISCLAIMER: &str =
    "algorithmic_attribution_only_not_physical_member_migration";

pub const MIN_CHAIN_BATCHES: usize = 4;
pub const MAX_CHAIN_BATCHES: usize = 8;
pub const DEFAULT_CHAIN_RUN_KEY_PREFIX: &str = "chain0000000";

const MAX_FRACTION_SUM_TOLERANCE: f64 = 1e-3;

const WHITELISTED_SINGLE_BATCH_FAILURE_CODES: [&str; 8] = [
    "INSUFFICIENT_RANK",
    "AMBIGUOUS_SUBSPACE",
    "EMPTY_CLUSTER",
    "NOT_CONVERGED",
    "INSUFFICIENT_DISTINCT_VECTORS",
    "INSUFFICIENT_PROJECTED_VECTORS",
    "SVD_NOT_CONVERGED",
    "ZERO_DISPERSION",
];

#[inline]
fn norm_f64(x: f64) -> f64 {
    if x == 0.0 { 0.0 } else { x }
}

#[inline]
fn checked_ratio(num: f64, den: f64, label: &str) -> Result<f64, CoreError> {
    let q = norm_f64(num / den);
    if !q.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::NumericalFailure,
            format!("{label} overflowed to non-finite value ({num} / {den})"),
        ));
    }
    Ok(q)
}

#[inline]
fn checked_opt_ratio(num: f64, den: Option<f64>, label: &str) -> Result<Option<f64>, CoreError> {
    match den {
        Some(d) => checked_ratio(num, d, label).map(Some),
        None => Ok(None),
    }
}

#[inline]
fn checked_fsum_finite<I: IntoIterator<Item = f64>>(
    vals: I,
    label: &str,
) -> Result<f64, CoreError> {
    let s = norm_f64(fsum(vals).map_err(|_| {
        CoreError::new(CoreErrorCode::NumericalFailure, format!("{label} overflow"))
    })?);
    if !s.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::NumericalFailure,
            format!("{label} rounded to non-finite value ({s})"),
        ));
    }
    Ok(s)
}

// ---------------------------------------------------------------------------
// Configuration & Input Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecondaryRule {
    NearestFeasibleV1,
}

impl SecondaryRule {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NearestFeasibleV1 => SECONDARY_RULE_V1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowRule {
    StarMinMassProportionalV1,
}

impl FlowRule {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::StarMinMassProportionalV1 => FLOW_RULE_V1,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionConfig {
    pub secondary_rule: SecondaryRule,
    pub flow_rule: FlowRule,
    pub fraction_sum_tolerance: f64,
    pub high_uncertainty_threshold_ratio: f64,
    pub high_uncertainty_optimality_ratio: f64,
    pub high_uncertainty_secondary_ratio: f64,
}

impl Default for EvolutionConfig {
    fn default() -> Self {
        Self {
            secondary_rule: SecondaryRule::NearestFeasibleV1,
            flow_rule: FlowRule::StarMinMassProportionalV1,
            fraction_sum_tolerance: 1e-9,
            high_uncertainty_threshold_ratio: 0.10,
            high_uncertainty_optimality_ratio: 0.10,
            high_uncertainty_secondary_ratio: 0.10,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackboneAssertion {
    pub prev_cluster_ids: Vec<usize>,
    pub curr_cluster_ids: Vec<usize>,
    pub threshold: f64,
    pub distance_matrix: Option<Vec<Vec<f64>>>,
    pub eligible_matrix: Option<Vec<Vec<bool>>>,
    pub primary_links: Vec<(usize, usize)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionInput {
    pub prev_scenarios: Vec<ScenarioNodeInput>,
    pub curr_scenarios: Vec<ScenarioNodeInput>,
    pub weights: Vec<f64>,
    pub threshold: f64,
    pub config: EvolutionConfig,
    pub backbone: Option<BackboneAssertion>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScaledEvolutionInput {
    pub prev_scenarios: Vec<ScenarioNodeInput>,
    pub curr_scenarios: Vec<ScenarioNodeInput>,
    pub weights: Vec<f64>,
    pub threshold_coefficient: f64,
    pub prev_dispersion: f64,
    pub curr_dispersion: f64,
    pub config: EvolutionConfig,
    pub backbone: Option<BackboneAssertion>,
}

// ---------------------------------------------------------------------------
// Output Enums & Structs (Pair Evolution)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeRole {
    Primary,
    SecondarySplit,
    SecondaryMerge,
}

impl EdgeRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::SecondarySplit => "secondary_split",
            Self::SecondaryMerge => "secondary_merge",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionEventKind {
    Continuation,
    Split,
    Merge,
    Disappearance,
    Emergence,
    GapInterrupted,
    GapRestarted,
}

impl TransitionEventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Continuation => "continuation",
            Self::Split => "split",
            Self::Merge => "merge",
            Self::Disappearance => "disappearance",
            Self::Emergence => "emergence",
            Self::GapInterrupted => "gap_interrupted",
            Self::GapRestarted => "gap_restarted",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeAlternativeStatus {
    Tied,
    SameCount,
    CountDrop,
    PromotionGap,
}

impl EdgeAlternativeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tied => "tied",
            Self::SameCount => "same_count",
            Self::CountDrop => "count_drop",
            Self::PromotionGap => "promotion_gap",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UncertaintyReason {
    AmbiguousTie,
    NearThreshold,
    NearPrimarySwitch,
    NearSecondarySwitch,
}

impl UncertaintyReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AmbiguousTie => "ambiguous_tie",
            Self::NearThreshold => "near_threshold",
            Self::NearPrimarySwitch => "near_primary_switch",
            Self::NearSecondarySwitch => "near_secondary_switch",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    ContinuationSource,
    SplitParent,
    MergePrimaryParent,
    MergeSecondaryParent,
    Disappeared,
    ContinuationTarget,
    SplitPrimaryChild,
    SplitSecondaryChild,
    MergeTarget,
    Emerged,
}

impl NodeRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ContinuationSource => "continuation_source",
            Self::SplitParent => "split_parent",
            Self::MergePrimaryParent => "merge_primary_parent",
            Self::MergeSecondaryParent => "merge_secondary_parent",
            Self::Disappeared => "disappeared",
            Self::ContinuationTarget => "continuation_target",
            Self::SplitPrimaryChild => "split_primary_child",
            Self::SplitSecondaryChild => "split_secondary_child",
            Self::MergeTarget => "merge_target",
            Self::Emerged => "emerged",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnmatchedReason {
    NotSelectedInGlobalOptimum,
    NoCandidateWithinThreshold,
}

impl UnmatchedReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotSelectedInGlobalOptimum => "not_selected_in_global_optimum",
            Self::NoCandidateWithinThreshold => "no_candidate_within_threshold",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionEdge {
    pub prev_cluster_id: usize,
    pub curr_cluster_id: usize,
    pub edge_role: EdgeRole,
    pub event_type: TransitionEventKind,
    pub rule_basis: String,
    pub distance: f64,
    pub distance_over_scale: Option<f64>,
    pub prev_member_fraction: f64,
    pub curr_member_fraction: f64,
    pub edge_delta_fraction: f64,
    pub attributed_flow: f64,
    pub edge_strength: f64,
    pub threshold_margin: f64,
    pub threshold_margin_ratio: Option<f64>,
    pub primary_alternative_status: EdgeAlternativeStatus,
    pub optimality_margin: Option<f64>,
    pub optimality_margin_ratio: Option<f64>,
    pub secondary_margin: Option<f64>,
    pub secondary_margin_ratio: Option<f64>,
    pub ambiguous_tie: bool,
    pub high_uncertainty: bool,
    pub uncertainty_reasons: Vec<UncertaintyReason>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeEvolutionSummary {
    pub cluster_id: usize,
    pub canonical_rank: usize,
    pub member_count: usize,
    pub member_fraction: f64,
    pub primary_degree: usize,
    pub secondary_degree: usize,
    pub node_role: NodeRole,
    pub participates_in_split: bool,
    pub participates_in_merge: bool,
    pub unmatched_reason: Option<UnmatchedReason>,
    pub min_candidate_distance: Option<f64>,
    pub min_candidate_distance_over_scale: Option<f64>,
    pub primary_counterpart_cluster_id: Option<usize>,
    pub primary_delta_fraction: Option<f64>,
    pub attributed_flow_fraction: f64,
    pub unattributed_fraction: f64,
    pub threshold_margin: Option<f64>,
    pub ambiguous_tie: bool,
    pub high_uncertainty: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionEvent {
    pub event_index: usize,
    pub event_type: TransitionEventKind,
    pub prev_cluster_ids: Vec<usize>,
    pub curr_cluster_ids: Vec<usize>,
    pub primary_link: Option<(usize, usize)>,
    pub rule_basis: String,
    pub max_morphology_distance: f64,
    pub max_morphology_over_scale: Option<f64>,
    pub prev_fraction_sum: f64,
    pub curr_fraction_sum: f64,
    pub net_delta_fraction: f64,
    pub attributed_flow_sum: f64,
    pub min_margin_ratio: Option<f64>,
    pub ambiguous_tie: bool,
    pub high_uncertainty: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransitionEvolutionSummary {
    pub primary_edge_count: usize,
    pub secondary_split_edge_count: usize,
    pub secondary_merge_edge_count: usize,
    pub continuation_count: usize,
    pub split_count: usize,
    pub merge_count: usize,
    pub emergence_count: usize,
    pub disappearance_count: usize,
    pub primary_total_distance: f64,
    pub primary_max_distance: f64,
    pub primary_max_distance_over_scale: Option<f64>,
    pub all_edges_max_distance: f64,
    pub all_edges_max_distance_over_scale: Option<f64>,
    pub transition_tv_redistribution: f64,
    pub secondary_attributed_flow: f64,
    pub total_attributed_flow: f64,
    pub disappeared_fraction_sum: f64,
    pub emerged_fraction_sum: f64,
    pub component_tv_residual: f64,
    pub min_threshold_margin: f64,
    pub min_threshold_margin_ratio: Option<f64>,
    pub raw_matrix_threshold_margin: f64,
    pub min_optimality_margin: Option<f64>,
    pub min_optimality_margin_ratio: Option<f64>,
    pub min_secondary_margin: Option<f64>,
    pub min_secondary_margin_ratio: Option<f64>,
    pub ambiguous_tie: bool,
    pub high_uncertainty_edge_count: usize,
    pub high_uncertainty: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionResult {
    pub algorithm_version: String,
    pub secondary_rule: String,
    pub flow_rule: String,
    pub uncertainty_rule: String,
    pub non_physical_disclaimer: String,
    pub prev_cluster_ids: Vec<usize>,
    pub curr_cluster_ids: Vec<usize>,
    pub threshold: f64,
    pub pair_scale: Option<f64>,
    pub threshold_coefficient: Option<f64>,
    pub prev_dispersion: Option<f64>,
    pub curr_dispersion: Option<f64>,
    pub reference_scale: Option<f64>,
    pub backbone: MatchResult,
    pub edges: Vec<EvolutionEdge>,
    pub prev_nodes: Vec<NodeEvolutionSummary>,
    pub curr_nodes: Vec<NodeEvolutionSummary>,
    pub events: Vec<EvolutionEvent>,
    pub summary: TransitionEvolutionSummary,
}

// ---------------------------------------------------------------------------
// Multi-Batch Chain Types (`evolve_chain`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum ChainBatchStatus {
    Ok {
        scenarios: Vec<ScenarioNodeInput>,
        dispersion: f64,
    },
    Missing {
        reason: String,
    },
    Failed {
        code: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChainBatchInput {
    pub ordinal: usize,
    pub status: ChainBatchStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionChainInput {
    pub run_key_prefix: Option<String>,
    pub batches: Vec<ChainBatchInput>,
    pub weights: Vec<f64>,
    pub threshold_coefficient: f64,
    pub config: EvolutionConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainTransitionStatus {
    Ok,
    Gap,
}

impl ChainTransitionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Gap => "gap",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChainResolvedBatch {
    pub ordinal: usize,
    pub status: String,
    pub status_code: Option<String>,
    pub status_reason: Option<String>,
    pub dispersion: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChainNodeSummary {
    pub ordinal: usize,
    pub cluster_id: usize,
    pub canonical_rank: usize,
    pub branch_id: String,
    pub mean_features: Vec<f64>,
    pub member_count: usize,
    pub member_fraction: f64,
    pub assignment_reason: String,
    pub in_degree_primary: usize,
    pub in_degree_secondary: usize,
    pub out_degree_primary: usize,
    pub out_degree_secondary: usize,
    pub incoming_role: String,
    pub outgoing_role: String,
    pub primary_delta_fraction: Option<f64>,
    pub unattributed_inflow_fraction: f64,
    pub unattributed_outflow_fraction: f64,
    pub high_uncertainty: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChainEvolutionEdge {
    pub prev_ordinal: usize,
    pub curr_ordinal: usize,
    pub prev_branch_id: String,
    pub curr_branch_id: String,
    pub edge: EvolutionEdge,
    pub non_physical_disclaimer: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChainEvolutionEvent {
    pub prev_ordinal: usize,
    pub event_index: usize,
    pub event_type: TransitionEventKind,
    pub prev_cluster_ids: Vec<usize>,
    pub curr_cluster_ids: Vec<usize>,
    pub branch_ids: Vec<String>,
    pub primary_branch_id: Option<String>,
    pub rule_basis: String,
    pub max_morphology_distance: f64,
    pub max_morphology_over_scale: Option<f64>,
    pub prev_fraction_sum: f64,
    pub curr_fraction_sum: f64,
    pub net_delta_fraction: f64,
    pub attributed_flow_sum: f64,
    pub min_margin_ratio: Option<f64>,
    pub ambiguous_tie: bool,
    pub high_uncertainty: bool,
    pub non_physical_disclaimer: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChainTransitionRecord {
    pub prev_ordinal: usize,
    pub curr_ordinal: usize,
    pub transition_status: ChainTransitionStatus,
    pub chain_broken: bool,
    pub gap_reason: Option<String>,
    pub pair_scale: Option<f64>,
    pub threshold: Option<f64>,
    pub primary_edge_count: usize,
    pub secondary_split_edge_count: usize,
    pub secondary_merge_edge_count: usize,
    pub transition_tv_redistribution: Option<f64>,
    pub min_threshold_margin: Option<f64>,
    pub ambiguous_tie: bool,
    pub high_uncertainty: bool,
    pub evolve_result: Option<EvolutionResult>,
    pub events: Vec<ChainEvolutionEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionBranchSummary {
    pub branch_id: String,
    pub birth_ordinal: usize,
    pub end_ordinal: usize,
    pub node_count: usize,
    pub step_count: usize,
    pub birth_reason: String,
    pub termination_reason: String,
    pub cumulative_morphology_distance: f64,
    pub cumulative_morphology_over_scale: f64,
    pub max_step_morphology_distance: f64,
    pub max_step_morphology_over_scale: f64,
    pub end_to_end_morphology_distance: f64,
    pub initial_member_fraction: f64,
    pub final_member_fraction: f64,
    pub min_member_fraction: f64,
    pub max_member_fraction: f64,
    pub net_delta_fraction: f64,
    pub cumulative_abs_delta_fraction: f64,
    pub split_out_event_count: usize,
    pub spawned_secondary_branch_ids: Vec<String>,
    pub split_from_parent_branch_id: Option<String>,
    pub merge_in_event_count: usize,
    pub absorbed_secondary_branch_ids: Vec<String>,
    pub merged_into_branch_id: Option<String>,
    pub ambiguous_step_count: usize,
    pub high_uncertainty_step_count: usize,
    pub high_uncertainty_step_ratio: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionChainSummary {
    pub ok_batch_count: usize,
    pub missing_batch_count: usize,
    pub failed_batch_count: usize,
    pub ok_transition_count: usize,
    pub gap_transition_count: usize,
    pub branch_count: usize,
    pub primary_edge_count: usize,
    pub secondary_split_edge_count: usize,
    pub secondary_merge_edge_count: usize,
    pub continuation_count: usize,
    pub split_count: usize,
    pub merge_count: usize,
    pub emergence_count: usize,
    pub disappearance_count: usize,
    pub gap_interrupted_count: usize,
    pub gap_restarted_count: usize,
    pub cumulative_primary_distance: f64,
    pub max_edge_distance: f64,
    pub max_edge_distance_over_scale: f64,
    pub mean_transition_tv_redistribution: f64,
    pub max_transition_tv_redistribution: f64,
    pub ambiguous_transition_count: usize,
    pub high_uncertainty_transition_count: usize,
    pub high_uncertainty_transition_ratio: f64,
    pub high_uncertainty_edge_count: usize,
    pub high_uncertainty_edge_ratio: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionChainResult {
    pub algorithm_version: String,
    pub secondary_rule: String,
    pub flow_rule: String,
    pub uncertainty_rule: String,
    pub non_physical_disclaimer: String,
    pub run_key_prefix: String,
    pub planned_batch_count: usize,
    pub threshold_coefficient: f64,
    pub batches: Vec<ChainResolvedBatch>,
    pub nodes: Vec<ChainNodeSummary>,
    pub transitions: Vec<ChainTransitionRecord>,
    pub edges: Vec<ChainEvolutionEdge>,
    pub events: Vec<ChainEvolutionEvent>,
    pub branches: Vec<EvolutionBranchSummary>,
    pub chain_summary: EvolutionChainSummary,
}

// ---------------------------------------------------------------------------
// Input & Backbone Validation Helpers
// ---------------------------------------------------------------------------

fn validate_evolution_config(config: &EvolutionConfig) -> Result<(), CoreError> {
    if !config.fraction_sum_tolerance.is_finite()
        || config.fraction_sum_tolerance < 0.0
        || config.fraction_sum_tolerance > MAX_FRACTION_SUM_TOLERANCE
    {
        return Err(CoreError::new(
            CoreErrorCode::InvalidEvolutionConfig,
            format!(
                "fraction_sum_tolerance must be finite and in [0.0, 1e-3], got {}",
                config.fraction_sum_tolerance
            ),
        ));
    }
    if !config.high_uncertainty_threshold_ratio.is_finite()
        || config.high_uncertainty_threshold_ratio < 0.0
    {
        return Err(CoreError::new(
            CoreErrorCode::InvalidEvolutionConfig,
            format!(
                "high_uncertainty_threshold_ratio must be finite and >= 0.0, got {}",
                config.high_uncertainty_threshold_ratio
            ),
        ));
    }
    if !config.high_uncertainty_optimality_ratio.is_finite()
        || config.high_uncertainty_optimality_ratio < 0.0
    {
        return Err(CoreError::new(
            CoreErrorCode::InvalidEvolutionConfig,
            format!(
                "high_uncertainty_optimality_ratio must be finite and >= 0.0, got {}",
                config.high_uncertainty_optimality_ratio
            ),
        ));
    }
    if !config.high_uncertainty_secondary_ratio.is_finite()
        || config.high_uncertainty_secondary_ratio < 0.0
    {
        return Err(CoreError::new(
            CoreErrorCode::InvalidEvolutionConfig,
            format!(
                "high_uncertainty_secondary_ratio must be finite and >= 0.0, got {}",
                config.high_uncertainty_secondary_ratio
            ),
        ));
    }
    Ok(())
}

fn validate_weights(weights: &[f64]) -> Result<f64, CoreError> {
    if weights.is_empty() {
        return Err(CoreError::new(
            CoreErrorCode::EmptyFeatures,
            "Spatial weights must not be empty",
        ));
    }
    let mut w_exp = Expansion::new();
    for (g, &w) in weights.iter().enumerate() {
        if !w.is_finite() || w < 0.0 {
            return Err(CoreError::new(
                CoreErrorCode::InvalidWeights,
                format!("Weight at {g} must be finite and >= 0.0, got {w}"),
            )
            .with_feature_index(g));
        }
        w_exp.push(w).map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "Weights summation overflow",
            )
        })?;
    }
    let w_sum = w_exp.round();
    if w_sum <= 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidWeights,
            "Sum of spatial weights must be strictly positive",
        ));
    }
    Ok(w_sum)
}

#[derive(Clone, Copy)]
enum ScenarioSide {
    Prev,
    Curr,
}

impl ScenarioSide {
    fn attach_index(self, err: CoreError, idx: usize) -> CoreError {
        match self {
            Self::Prev => err.with_prev_index(idx),
            Self::Curr => err.with_curr_index(idx),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Prev => "prev_scenarios",
            Self::Curr => "curr_scenarios",
        }
    }
}

fn validate_scenario_side_fractions(
    scenarios: &[ScenarioNodeInput],
    tol: f64,
    side: ScenarioSide,
) -> Result<(), CoreError> {
    let label = side.label();
    for (idx, sc) in scenarios.iter().enumerate() {
        if sc.member_count == 0 {
            return Err(side.attach_index(
                CoreError::new(
                    CoreErrorCode::ZeroMemberScenario,
                    format!("{label}[{idx}] has member_count == 0"),
                ),
                idx,
            ));
        }
        if !sc.member_fraction.is_finite() || sc.member_fraction <= 0.0 || sc.member_fraction > 1.0
        {
            return Err(side.attach_index(
                CoreError::new(
                    CoreErrorCode::InvalidMemberFraction,
                    format!(
                        "{label}[{idx}] member_fraction must be finite and in (0.0, 1.0], got {}",
                        sc.member_fraction
                    ),
                ),
                idx,
            ));
        }
    }

    let frac_sum = fsum(scenarios.iter().map(|s| s.member_fraction)).map_err(|_| {
        CoreError::new(
            CoreErrorCode::NumericalFailure,
            format!("{label} member_fraction summation overflow"),
        )
    })?;
    let diff = (frac_sum - 1.0).abs();
    if !diff.is_finite() || diff > tol {
        return Err(side.attach_index(
            CoreError::new(
                CoreErrorCode::InvalidFractionSum,
                format!(
                    "{label} fsum(member_fraction) = {frac_sum} deviates from 1.0 by {diff} > {tol}"
                ),
            ),
            0,
        ));
    }
    Ok(())
}

fn validate_standalone_batch_scenarios(
    scenarios: &[ScenarioNodeInput],
    n_points: usize,
    tol: f64,
    batch_ordinal: Option<usize>,
) -> Result<(), CoreError> {
    let decorate = |mut err: CoreError| -> CoreError {
        if let Some(ord) = batch_ordinal {
            err.message = format!("batch {ord}: {}", err.message);
            if err.prev_index.is_none() {
                err = err.with_prev_index(ord);
            }
        }
        err
    };

    if scenarios.is_empty() {
        return Err(decorate(CoreError::new(
            CoreErrorCode::EmptyScenarios,
            "Batch scenarios must not be empty",
        )));
    }
    if scenarios.len() > MAX_SCENARIOS {
        return Err(decorate(CoreError::new(
            CoreErrorCode::TooManyScenarios,
            format!(
                "Batch scenario count {} exceeds maximum supported limit of {MAX_SCENARIOS}",
                scenarios.len()
            ),
        )));
    }
    let mut ids = HashSet::with_capacity(scenarios.len());
    let first_len = scenarios[0].mean_features.len();
    for (idx, sc) in scenarios.iter().enumerate() {
        if !ids.insert(sc.cluster_id) {
            return Err(decorate(
                CoreError::new(
                    CoreErrorCode::DuplicateScenarioId,
                    format!("Duplicate cluster_id {} in batch scenarios", sc.cluster_id),
                )
                .with_prev_index(idx),
            ));
        }
        if sc.mean_features.is_empty() {
            return Err(decorate(
                CoreError::new(
                    CoreErrorCode::EmptyFeatures,
                    format!("scenarios[{idx}] mean_features is empty"),
                )
                .with_prev_index(idx),
            ));
        }
        if sc.mean_features.len() != first_len {
            return Err(decorate(
                CoreError::new(
                    CoreErrorCode::RaggedFeatures,
                    format!(
                        "scenarios[{idx}] features length {} != scenarios[0] length {first_len}",
                        sc.mean_features.len()
                    ),
                )
                .with_prev_index(idx),
            ));
        }
        for (g, &val) in sc.mean_features.iter().enumerate() {
            if !val.is_finite() {
                return Err(decorate(
                    CoreError::new(
                        CoreErrorCode::NonFiniteInput,
                        format!("scenarios[{idx}] feature {g} is non-finite: {val}"),
                    )
                    .with_prev_index(idx)
                    .with_feature_index(g),
                ));
            }
        }
    }
    if first_len != n_points {
        return Err(decorate(CoreError::new(
            CoreErrorCode::DimensionMismatch,
            format!(
                "Scenario features length {first_len} does not match weights length {n_points}"
            ),
        )));
    }
    validate_scenario_side_fractions(scenarios, tol, ScenarioSide::Prev).map_err(decorate)
}

struct BackboneCheckContext<'a> {
    prev_scenarios: &'a [ScenarioNodeInput],
    curr_scenarios: &'a [ScenarioNodeInput],
    tau: f64,
    dist_matrix: &'a [Vec<f64>],
    base: &'a SolvedMatching,
    backbone_match: &'a MatchResult,
}

fn verify_backbone_assertion(
    assertion: &BackboneAssertion,
    ctx: &BackboneCheckContext<'_>,
) -> Result<(), CoreError> {
    let m = ctx.prev_scenarios.len();
    let n = ctx.curr_scenarios.len();

    let expected_prev_ids: Vec<usize> = ctx.prev_scenarios.iter().map(|s| s.cluster_id).collect();
    let expected_curr_ids: Vec<usize> = ctx.curr_scenarios.iter().map(|s| s.cluster_id).collect();

    if assertion.prev_cluster_ids != expected_prev_ids
        || assertion.curr_cluster_ids != expected_curr_ids
    {
        return Err(CoreError::new(
            CoreErrorCode::BackboneInconsistency,
            "BackboneAssertion cluster_id axis sequence does not match input scenarios",
        ));
    }

    if !assertion.threshold.is_finite()
        || norm_f64(assertion.threshold).to_bits() != norm_f64(ctx.tau).to_bits()
    {
        return Err(CoreError::new(
            CoreErrorCode::BackboneInconsistency,
            format!(
                "BackboneAssertion threshold {} does not match effective threshold {}",
                assertion.threshold, ctx.tau
            ),
        ));
    }

    if let Some(dm) = &assertion.distance_matrix {
        if dm.len() != m {
            return Err(CoreError::new(
                CoreErrorCode::BackboneInconsistency,
                format!(
                    "BackboneAssertion distance_matrix row count {} != {m}",
                    dm.len()
                ),
            ));
        }
        for i in 0..m {
            if dm[i].len() != n {
                return Err(CoreError::new(
                    CoreErrorCode::BackboneInconsistency,
                    format!(
                        "BackboneAssertion distance_matrix[{i}] col count {} != {n}",
                        dm[i].len()
                    ),
                ));
            }
            for j in 0..n {
                let got = dm[i][j];
                let exp = ctx.dist_matrix[i][j];
                if !got.is_finite() || norm_f64(got).to_bits() != norm_f64(exp).to_bits() {
                    return Err(CoreError::new(
                        CoreErrorCode::BackboneInconsistency,
                        format!(
                            "BackboneAssertion distance_matrix[{i}][{j}] = {got} != expected {exp}"
                        ),
                    )
                    .with_prev_index(i)
                    .with_curr_index(j));
                }
            }
        }
    }

    if let Some(em) = &assertion.eligible_matrix {
        if em.len() != m {
            return Err(CoreError::new(
                CoreErrorCode::BackboneInconsistency,
                format!(
                    "BackboneAssertion eligible_matrix row count {} != {m}",
                    em.len()
                ),
            ));
        }
        for i in 0..m {
            if em[i].len() != n {
                return Err(CoreError::new(
                    CoreErrorCode::BackboneInconsistency,
                    format!(
                        "BackboneAssertion eligible_matrix[{i}] col count {} != {n}",
                        em[i].len()
                    ),
                ));
            }
            for j in 0..n {
                let exp_elig = ctx.dist_matrix[i][j] <= ctx.tau;
                if em[i][j] != exp_elig {
                    return Err(CoreError::new(
                        CoreErrorCode::BackboneInconsistency,
                        format!(
                            "BackboneAssertion eligible_matrix[{i}][{j}] = {} != expected {exp_elig}",
                            em[i][j]
                        ),
                    )
                    .with_prev_index(i)
                    .with_curr_index(j));
                }
            }
        }
    }

    let prev_id_to_idx: HashMap<usize, usize> = ctx
        .prev_scenarios
        .iter()
        .enumerate()
        .map(|(i, s)| (s.cluster_id, i))
        .collect();
    let curr_id_to_idx: HashMap<usize, usize> = ctx
        .curr_scenarios
        .iter()
        .enumerate()
        .map(|(j, s)| (s.cluster_id, j))
        .collect();

    let mut used_prev = vec![false; m];
    let mut used_curr = vec![false; n];
    let mut asserted_sum = Expansion::new();

    for (link_pos, &(p_cid, c_cid)) in assertion.primary_links.iter().enumerate() {
        let Some(&p_idx) = prev_id_to_idx.get(&p_cid) else {
            let mut err = CoreError::new(
                CoreErrorCode::InvalidPrimaryLink,
                format!("BackboneAssertion primary_links[{link_pos}] references unknown prev_cluster_id {p_cid}"),
            )
            .with_prev_index(link_pos);
            if let Some(&c_idx) = curr_id_to_idx.get(&c_cid) {
                err = err.with_curr_index(c_idx);
            }
            return Err(err);
        };
        let Some(&c_idx) = curr_id_to_idx.get(&c_cid) else {
            return Err(CoreError::new(
                CoreErrorCode::InvalidPrimaryLink,
                format!("BackboneAssertion primary_links[{link_pos}] references unknown curr_cluster_id {c_cid}"),
            )
            .with_prev_index(p_idx)
            .with_curr_index(link_pos));
        };

        if used_prev[p_idx] {
            return Err(CoreError::new(
                CoreErrorCode::InvalidPrimaryLink,
                format!("BackboneAssertion primary_links duplicates prev_cluster_id {p_cid}"),
            )
            .with_prev_index(p_idx)
            .with_curr_index(c_idx));
        }
        if used_curr[c_idx] {
            return Err(CoreError::new(
                CoreErrorCode::InvalidPrimaryLink,
                format!("BackboneAssertion primary_links duplicates curr_cluster_id {c_cid}"),
            )
            .with_prev_index(p_idx)
            .with_curr_index(c_idx));
        }
        used_prev[p_idx] = true;
        used_curr[c_idx] = true;

        let d = ctx.dist_matrix[p_idx][c_idx];
        if d > ctx.tau {
            return Err(CoreError::new(
                CoreErrorCode::InvalidPrimaryLink,
                format!(
                    "BackboneAssertion primary link ({p_cid}, {c_cid}) distance {d} exceeds threshold {}",
                    ctx.tau
                ),
            )
            .with_prev_index(p_idx)
            .with_curr_index(c_idx));
        }

        asserted_sum.push(d).map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "BackboneAssertion primary distance sum overflow",
            )
        })?;
    }

    if assertion.primary_links.len() != ctx.base.val.count
        || asserted_sum.cmp_exact(&ctx.base.val.sum) != Ordering::Equal
    {
        return Err(CoreError::new(
            CoreErrorCode::BackboneInconsistency,
            format!(
                "BackboneAssertion primary_links objective (count={}, sum={}) does not match global optimum (count={}, sum={})",
                assertion.primary_links.len(),
                asserted_sum.round(),
                ctx.base.val.count,
                ctx.base.val.sum.round()
            ),
        ));
    }

    let mut asserted_sorted = assertion.primary_links.clone();
    asserted_sorted.sort_unstable();
    let mut expected_sorted: Vec<(usize, usize)> = ctx
        .backbone_match
        .links
        .iter()
        .map(|l| (l.prev_cluster_id, l.curr_cluster_id))
        .collect();
    expected_sorted.sort_unstable();

    if asserted_sorted != expected_sorted {
        return Err(CoreError::new(
            CoreErrorCode::BackboneInconsistency,
            "BackboneAssertion primary_links does not match canonical M2 global optimum",
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Core Pair Evolution Engine
// ---------------------------------------------------------------------------

struct RawEdgeSpec {
    p_orig: usize,
    c_orig: usize,
    edge_role: EdgeRole,
    rule_basis: String,
    distance: f64,
    primary_alternative_status: EdgeAlternativeStatus,
    optimality_margin: Option<f64>,
    secondary_margin: Option<f64>,
    ambiguous_tie: bool,
}

struct PairScaleMeta {
    pair_scale: Option<f64>,
    threshold_coefficient: Option<f64>,
    prev_dispersion: Option<f64>,
    curr_dispersion: Option<f64>,
}

/// Evolve scenarios across a pair of adjacent batches using an absolute threshold `tau >= 0`.
pub fn evolve_scenarios(input: &EvolutionInput) -> Result<EvolutionResult, CoreError> {
    validate_evolution_config(&input.config)?;

    let match_in = MatchInput {
        prev_scenarios: input.prev_scenarios.clone(),
        curr_scenarios: input.curr_scenarios.clone(),
        weights: input.weights.clone(),
        threshold: input.threshold,
    };
    let backbone = match_scenarios(&match_in)?;

    validate_scenario_side_fractions(
        &input.prev_scenarios,
        input.config.fraction_sum_tolerance,
        ScenarioSide::Prev,
    )?;
    validate_scenario_side_fractions(
        &input.curr_scenarios,
        input.config.fraction_sum_tolerance,
        ScenarioSide::Curr,
    )?;

    let tau = norm_f64(input.threshold);
    let reference_scale = if tau > 0.0 { Some(tau) } else { None };
    let meta = PairScaleMeta {
        pair_scale: None,
        threshold_coefficient: None,
        prev_dispersion: None,
        curr_dispersion: None,
    };

    build_evolution_result(
        &input.prev_scenarios,
        &input.curr_scenarios,
        tau,
        reference_scale,
        &meta,
        &input.config,
        input.backbone.as_ref(),
        backbone,
    )
}

/// Evolve scenarios across a pair of adjacent batches using pair dispersion scale `S` and coefficient `c`.
pub fn evolve_scenarios_scaled(input: &ScaledEvolutionInput) -> Result<EvolutionResult, CoreError> {
    validate_evolution_config(&input.config)?;

    let scaled_match_in = ScaledMatchInput {
        prev_scenarios: input.prev_scenarios.clone(),
        curr_scenarios: input.curr_scenarios.clone(),
        weights: input.weights.clone(),
        threshold_coefficient: input.threshold_coefficient,
        prev_dispersion: input.prev_dispersion,
        curr_dispersion: input.curr_dispersion,
    };
    let backbone = match_scenarios_scaled(&scaled_match_in)?;

    validate_scenario_side_fractions(
        &input.prev_scenarios,
        input.config.fraction_sum_tolerance,
        ScenarioSide::Prev,
    )?;
    validate_scenario_side_fractions(
        &input.curr_scenarios,
        input.config.fraction_sum_tolerance,
        ScenarioSide::Curr,
    )?;

    let pair_scale = norm_f64(backbone.pair_scale.expect("scaled match sets pair_scale"));
    let tau = norm_f64(input.threshold_coefficient * pair_scale);
    let meta = PairScaleMeta {
        pair_scale: Some(pair_scale),
        threshold_coefficient: Some(norm_f64(input.threshold_coefficient)),
        prev_dispersion: Some(norm_f64(input.prev_dispersion)),
        curr_dispersion: Some(norm_f64(input.curr_dispersion)),
    };

    build_evolution_result(
        &input.prev_scenarios,
        &input.curr_scenarios,
        tau,
        Some(pair_scale),
        &meta,
        &input.config,
        input.backbone.as_ref(),
        backbone,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_evolution_result(
    prev_scenarios: &[ScenarioNodeInput],
    curr_scenarios: &[ScenarioNodeInput],
    tau: f64,
    reference_scale: Option<f64>,
    meta: &PairScaleMeta,
    config: &EvolutionConfig,
    backbone_assertion: Option<&BackboneAssertion>,
    mut backbone: MatchResult,
) -> Result<EvolutionResult, CoreError> {
    let m = prev_scenarios.len();
    let n = curr_scenarios.len();

    // Normalize -0.0 in distance_matrix and verify backbone link distance_ratio finiteness
    for row in &mut backbone.distance_matrix {
        for cell in row {
            *cell = norm_f64(*cell);
        }
    }
    backbone.objective.weighted_distance_sum = norm_f64(backbone.objective.weighted_distance_sum);
    if let Some(s) = meta.pair_scale {
        for link in &mut backbone.links {
            link.distance_ratio = Some(checked_ratio(
                norm_f64(link.distance_k),
                s,
                "backbone.links.distance_ratio",
            )?);
        }
    }

    let prev_canon = canonical_scenario_order(prev_scenarios);
    let curr_canon = canonical_scenario_order(curr_scenarios);

    let mut prev_rank_of_orig = vec![0usize; m];
    for (rank, &orig) in prev_canon.iter().enumerate() {
        prev_rank_of_orig[orig] = rank;
    }
    let mut curr_rank_of_orig = vec![0usize; n];
    for (rank, &orig) in curr_canon.iter().enumerate() {
        curr_rank_of_orig[orig] = rank;
    }

    let dp_ctx = CanonContext {
        m,
        n,
        dist: &backbone.distance_matrix,
        tau,
        rows: prev_canon,
        cols: curr_canon,
    };

    let base = solve_dp(&dp_ctx, &Constraint::default())?.expect("empty matching is feasible");

    if let Some(assertion) = backbone_assertion {
        let check_ctx = BackboneCheckContext {
            prev_scenarios,
            curr_scenarios,
            tau,
            dist_matrix: &backbone.distance_matrix,
            base: &base,
            backbone_match: &backbone,
        };
        verify_backbone_assertion(assertion, &check_ctx)?;
    }

    let prev_id_to_orig: HashMap<usize, usize> = prev_scenarios
        .iter()
        .enumerate()
        .map(|(i, s)| (s.cluster_id, i))
        .collect();
    let curr_id_to_orig: HashMap<usize, usize> = curr_scenarios
        .iter()
        .enumerate()
        .map(|(j, s)| (s.cluster_id, j))
        .collect();

    let mut prev_primary_partner: Vec<Option<usize>> = vec![None; m];
    let mut curr_primary_partner: Vec<Option<usize>> = vec![None; n];

    let mut raw_edges: Vec<RawEdgeSpec> = Vec::new();

    // 1. Primary edges from M2 global optimum
    for link in &backbone.links {
        let p_orig = prev_id_to_orig[&link.prev_cluster_id];
        let c_orig = curr_id_to_orig[&link.curr_cluster_id];
        prev_primary_partner[p_orig] = Some(c_orig);
        curr_primary_partner[c_orig] = Some(p_orig);

        let (alt_status, opt_margin) = match &link.substitutability {
            Substitutability::Tied => (EdgeAlternativeStatus::Tied, Some(0.0)),
            Substitutability::SameCount { gap, .. } => {
                (EdgeAlternativeStatus::SameCount, Some(norm_f64(*gap)))
            }
            Substitutability::CountDrop { .. } => (EdgeAlternativeStatus::CountDrop, None),
        };

        let rule_basis = if link.ambiguous {
            "global_optimum_primary_canonical_tiebreak".to_string()
        } else {
            "global_optimum_primary".to_string()
        };

        raw_edges.push(RawEdgeSpec {
            p_orig,
            c_orig,
            edge_role: EdgeRole::Primary,
            rule_basis,
            distance: norm_f64(link.distance_k),
            primary_alternative_status: alt_status,
            optimality_margin: opt_margin,
            secondary_margin: None,
            ambiguous_tie: link.ambiguous,
        });
    }

    // 2. Secondary split edges (`nearest_feasible_v1` for unmatched_curr in U_curr^comp)
    match config.secondary_rule {
        SecondaryRule::NearestFeasibleV1 => {
            for un in &backbone.unmatched_curr {
                if un.reason != "not_selected_in_global_optimum" {
                    continue;
                }
                let c_orig = curr_id_to_orig[&un.cluster_id];
                let c_canon = curr_rank_of_orig[c_orig];

                let feasible_prev: Vec<usize> = (0..m)
                    .filter(|&p_orig| backbone.distance_matrix[p_orig][c_orig] <= tau)
                    .collect();
                debug_assert!(!feasible_prev.is_empty());

                let d1 = feasible_prev
                    .iter()
                    .map(|&p| norm_f64(backbone.distance_matrix[p][c_orig]))
                    .min_by(|a, b| a.total_cmp(b))
                    .expect("U_curr^comp has >= 1 feasible predecessor");

                let tied_prev: Vec<usize> = feasible_prev
                    .iter()
                    .copied()
                    .filter(|&p| {
                        norm_f64(backbone.distance_matrix[p][c_orig]).total_cmp(&d1)
                            == Ordering::Equal
                    })
                    .collect();

                let p_star = tied_prev
                    .iter()
                    .copied()
                    .min_by_key(|&p| prev_rank_of_orig[p])
                    .expect("tied_prev is non-empty");

                // Theorem 1: parent of secondary_split must be matched in M*
                debug_assert!(prev_primary_partner[p_star].is_some());

                let has_dist_tie = tied_prev.len() > 1;
                let sec_margin = if feasible_prev.len() >= 2 {
                    let d2 = feasible_prev
                        .iter()
                        .copied()
                        .filter(|&p| p != p_star)
                        .map(|p| norm_f64(backbone.distance_matrix[p][c_orig]))
                        .min_by(|a, b| a.total_cmp(b))
                        .expect("at least 2 feasible predecessors");
                    let diff = fsum([d2, -d1]).map_err(|_| {
                        CoreError::new(
                            CoreErrorCode::NumericalFailure,
                            "Secondary split margin overflow",
                        )
                    })?;
                    Some(norm_f64(diff.max(0.0)))
                } else {
                    None
                };

                // Theorem 3: forced inclusion of c_canon preserves maximum cardinality q*
                let forced = solve_dp(
                    &dp_ctx,
                    &Constraint {
                        force_col: Some(c_canon),
                        ..Default::default()
                    },
                )?
                .ok_or_else(|| {
                    CoreError::new(
                        CoreErrorCode::NumericalFailure,
                        "Forced secondary column DP unexpectedly infeasible",
                    )
                })?;
                debug_assert_eq!(forced.val.count, base.val.count);
                let prom_gap = forced.val.sum.minus_rounded(&base.val.sum).map_err(|_| {
                    CoreError::new(
                        CoreErrorCode::NumericalFailure,
                        "Secondary promotion gap overflow",
                    )
                })?;

                let rule_basis = if has_dist_tie {
                    "nearest_feasible_split_canonical_tiebreak".to_string()
                } else {
                    "nearest_feasible_split_unique".to_string()
                };

                raw_edges.push(RawEdgeSpec {
                    p_orig: p_star,
                    c_orig,
                    edge_role: EdgeRole::SecondarySplit,
                    rule_basis,
                    distance: d1,
                    primary_alternative_status: EdgeAlternativeStatus::PromotionGap,
                    optimality_margin: Some(norm_f64(prom_gap.max(0.0))),
                    secondary_margin: sec_margin,
                    ambiguous_tie: un.matched_in_some_optimum || has_dist_tie,
                });
            }

            // 3. Secondary merge edges (`nearest_feasible_v1` for unmatched_prev in U_prev^comp)
            for un in &backbone.unmatched_prev {
                if un.reason != "not_selected_in_global_optimum" {
                    continue;
                }
                let p_orig = prev_id_to_orig[&un.cluster_id];
                let p_canon = prev_rank_of_orig[p_orig];

                let feasible_curr: Vec<usize> = (0..n)
                    .filter(|&c_orig| backbone.distance_matrix[p_orig][c_orig] <= tau)
                    .collect();
                debug_assert!(!feasible_curr.is_empty());

                let d1 = feasible_curr
                    .iter()
                    .map(|&c| norm_f64(backbone.distance_matrix[p_orig][c]))
                    .min_by(|a, b| a.total_cmp(b))
                    .expect("U_prev^comp has >= 1 feasible successor");

                let tied_curr: Vec<usize> = feasible_curr
                    .iter()
                    .copied()
                    .filter(|&c| {
                        norm_f64(backbone.distance_matrix[p_orig][c]).total_cmp(&d1)
                            == Ordering::Equal
                    })
                    .collect();

                let c_star = tied_curr
                    .iter()
                    .copied()
                    .min_by_key(|&c| curr_rank_of_orig[c])
                    .expect("tied_curr is non-empty");

                // Theorem 1: target of secondary_merge must be matched in M*
                debug_assert!(curr_primary_partner[c_star].is_some());

                let has_dist_tie = tied_curr.len() > 1;
                let sec_margin = if feasible_curr.len() >= 2 {
                    let d2 = feasible_curr
                        .iter()
                        .copied()
                        .filter(|&c| c != c_star)
                        .map(|c| norm_f64(backbone.distance_matrix[p_orig][c]))
                        .min_by(|a, b| a.total_cmp(b))
                        .expect("at least 2 feasible successors");
                    let diff = fsum([d2, -d1]).map_err(|_| {
                        CoreError::new(
                            CoreErrorCode::NumericalFailure,
                            "Secondary merge margin overflow",
                        )
                    })?;
                    Some(norm_f64(diff.max(0.0)))
                } else {
                    None
                };

                // Theorem 3: forced inclusion of p_canon preserves maximum cardinality q*
                let forced = solve_dp(
                    &dp_ctx,
                    &Constraint {
                        force_row: Some(p_canon),
                        ..Default::default()
                    },
                )?
                .ok_or_else(|| {
                    CoreError::new(
                        CoreErrorCode::NumericalFailure,
                        "Forced secondary row DP unexpectedly infeasible",
                    )
                })?;
                debug_assert_eq!(forced.val.count, base.val.count);
                let prom_gap = forced.val.sum.minus_rounded(&base.val.sum).map_err(|_| {
                    CoreError::new(
                        CoreErrorCode::NumericalFailure,
                        "Secondary promotion gap overflow",
                    )
                })?;

                let rule_basis = if has_dist_tie {
                    "nearest_feasible_merge_canonical_tiebreak".to_string()
                } else {
                    "nearest_feasible_merge_unique".to_string()
                };

                raw_edges.push(RawEdgeSpec {
                    p_orig,
                    c_orig: c_star,
                    edge_role: EdgeRole::SecondaryMerge,
                    rule_basis,
                    distance: d1,
                    primary_alternative_status: EdgeAlternativeStatus::PromotionGap,
                    optimality_margin: Some(norm_f64(prom_gap.max(0.0))),
                    secondary_margin: sec_margin,
                    ambiguous_tie: un.matched_in_some_optimum || has_dist_tie,
                });
            }
        }
    }

    // Star connected components decomposition
    // Each primary link (p_prim, c_prim) anchors one connected component.
    struct StarComponent {
        sort_key: (u8, usize),
        event_type: TransitionEventKind,
        p_origs: Vec<usize>,
        c_origs: Vec<usize>,
        primary_orig_pair: Option<(usize, usize)>,
        edge_indices: Vec<usize>,
    }

    let mut components: Vec<StarComponent> = Vec::new();
    let mut edge_event_type: Vec<TransitionEventKind> =
        vec![TransitionEventKind::Continuation; raw_edges.len()];
    let mut edge_attributed_flow: Vec<f64> = vec![0.0; raw_edges.len()];

    for (prim_idx, prim_spec) in raw_edges.iter().enumerate() {
        if prim_spec.edge_role != EdgeRole::Primary {
            continue;
        }
        let p_prim = prim_spec.p_orig;
        let c_prim = prim_spec.c_orig;

        let mut p_origs = vec![p_prim];
        let mut c_origs = vec![c_prim];
        let mut edge_indices = vec![prim_idx];

        let mut split_cnt = 0usize;
        let mut merge_cnt = 0usize;

        for (e_idx, e_spec) in raw_edges.iter().enumerate() {
            match e_spec.edge_role {
                EdgeRole::Primary => {}
                EdgeRole::SecondarySplit => {
                    if e_spec.p_orig == p_prim {
                        c_origs.push(e_spec.c_orig);
                        edge_indices.push(e_idx);
                        split_cnt += 1;
                    }
                }
                EdgeRole::SecondaryMerge => {
                    if e_spec.c_orig == c_prim {
                        p_origs.push(e_spec.p_orig);
                        edge_indices.push(e_idx);
                        merge_cnt += 1;
                    }
                }
            }
        }

        // Theorem 2: split and merge cannot co-occur on the same primary edge
        debug_assert!(split_cnt == 0 || merge_cnt == 0);

        let ev_kind = if split_cnt > 0 {
            TransitionEventKind::Split
        } else if merge_cnt > 0 {
            TransitionEventKind::Merge
        } else {
            TransitionEventKind::Continuation
        };

        for &e_idx in &edge_indices {
            edge_event_type[e_idx] = ev_kind;
        }

        // Compute `star_min_mass_proportional_v1` attributed flow for this component
        match config.flow_rule {
            FlowRule::StarMinMassProportionalV1 => {
                let e_p = Expansion::from_values(
                    p_origs.iter().map(|&i| prev_scenarios[i].member_fraction),
                )
                .map_err(|_| {
                    CoreError::new(
                        CoreErrorCode::NumericalFailure,
                        "Component prev fraction expansion overflow",
                    )
                })?;
                let e_q = Expansion::from_values(
                    c_origs.iter().map(|&j| curr_scenarios[j].member_fraction),
                )
                .map_err(|_| {
                    CoreError::new(
                        CoreErrorCode::NumericalFailure,
                        "Component curr fraction expansion overflow",
                    )
                })?;
                let p_sum_round = e_p.round();
                let q_sum_round = e_q.round();

                match ev_kind {
                    TransitionEventKind::Continuation => {
                        let p_i = prev_scenarios[p_prim].member_fraction;
                        let q_j = curr_scenarios[c_prim].member_fraction;
                        edge_attributed_flow[prim_idx] = norm_f64(p_i.min(q_j));
                    }
                    TransitionEventKind::Split => {
                        let p_i = prev_scenarios[p_prim].member_fraction;
                        let plenty = e_p.cmp_exact(&e_q) != Ordering::Less;
                        for &e_idx in &edge_indices {
                            let c_o = raw_edges[e_idx].c_orig;
                            let q_j = curr_scenarios[c_o].member_fraction;
                            let f = if plenty {
                                q_j
                            } else {
                                p_i.min(q_j).min((p_i * q_j) / q_sum_round)
                            };
                            if !f.is_finite() || f <= 0.0 {
                                return Err(CoreError::new(
                                    CoreErrorCode::NumericalFailure,
                                    format!("Attributed split flow underflow/non-finite: {f}"),
                                ));
                            }
                            edge_attributed_flow[e_idx] = norm_f64(f);
                        }
                    }
                    TransitionEventKind::Merge => {
                        let q_j = curr_scenarios[c_prim].member_fraction;
                        let plenty = e_q.cmp_exact(&e_p) != Ordering::Less;
                        for &e_idx in &edge_indices {
                            let p_o = raw_edges[e_idx].p_orig;
                            let p_i = prev_scenarios[p_o].member_fraction;
                            let f = if plenty {
                                p_i
                            } else {
                                p_i.min(q_j).min((q_j * p_i) / p_sum_round)
                            };
                            if !f.is_finite() || f <= 0.0 {
                                return Err(CoreError::new(
                                    CoreErrorCode::NumericalFailure,
                                    format!("Attributed merge flow underflow/non-finite: {f}"),
                                ));
                            }
                            edge_attributed_flow[e_idx] = norm_f64(f);
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        let min_prev_rank = p_origs
            .iter()
            .map(|&i| prev_rank_of_orig[i])
            .min()
            .expect("component has >= 1 prev node");

        components.push(StarComponent {
            sort_key: (0, min_prev_rank),
            event_type: ev_kind,
            p_origs,
            c_origs,
            primary_orig_pair: Some((p_prim, c_prim)),
            edge_indices,
        });
    }

    // Disappearance components (U_prev^none)
    for un in &backbone.unmatched_prev {
        if un.reason == "no_candidate_within_threshold" {
            let p_orig = prev_id_to_orig[&un.cluster_id];
            components.push(StarComponent {
                sort_key: (0, prev_rank_of_orig[p_orig]),
                event_type: TransitionEventKind::Disappearance,
                p_origs: vec![p_orig],
                c_origs: Vec::new(),
                primary_orig_pair: None,
                edge_indices: Vec::new(),
            });
        }
    }

    // Emergence components (U_curr^none)
    for un in &backbone.unmatched_curr {
        if un.reason == "no_candidate_within_threshold" {
            let c_orig = curr_id_to_orig[&un.cluster_id];
            components.push(StarComponent {
                sort_key: (1, curr_rank_of_orig[c_orig]),
                event_type: TransitionEventKind::Emergence,
                p_origs: Vec::new(),
                c_origs: vec![c_orig],
                primary_orig_pair: None,
                edge_indices: Vec::new(),
            });
        }
    }

    // Materialize finalized EvolutionEdge structs
    let mut built_edges_unsorted: Vec<EvolutionEdge> = Vec::with_capacity(raw_edges.len());
    for (e_idx, spec) in raw_edges.iter().enumerate() {
        let p_sc = &prev_scenarios[spec.p_orig];
        let c_sc = &curr_scenarios[spec.c_orig];
        let d = spec.distance;
        let distance_over_scale =
            checked_opt_ratio(d, meta.pair_scale, "edge.distance_over_scale")?;
        let p_frac = norm_f64(p_sc.member_fraction);
        let c_frac = norm_f64(c_sc.member_fraction);
        let edge_delta_fraction = norm_f64(c_frac - p_frac);
        let attributed_flow = edge_attributed_flow[e_idx];
        let edge_strength = if tau > 0.0 {
            let r = checked_ratio(d, tau, "edge.distance_over_threshold")?;
            norm_f64((1.0 - r).clamp(0.0, 1.0))
        } else {
            1.0
        };
        let threshold_margin = norm_f64((tau - d).max(0.0));
        let threshold_margin_ratio = checked_opt_ratio(
            threshold_margin,
            reference_scale,
            "edge.threshold_margin_ratio",
        )?;
        let optimality_margin_ratio = match (spec.optimality_margin, reference_scale) {
            (Some(g), Some(r)) => Some(checked_ratio(g, r, "edge.optimality_margin_ratio")?),
            _ => None,
        };
        let secondary_margin_ratio = match (spec.secondary_margin, reference_scale) {
            (Some(s), Some(r)) => Some(checked_ratio(s, r, "edge.secondary_margin_ratio")?),
            _ => None,
        };

        let mut uncertainty_reasons = Vec::new();
        if spec.ambiguous_tie {
            uncertainty_reasons.push(UncertaintyReason::AmbiguousTie);
        }
        if tau == 0.0
            || reference_scale
                .is_some_and(|r| threshold_margin <= config.high_uncertainty_threshold_ratio * r)
        {
            uncertainty_reasons.push(UncertaintyReason::NearThreshold);
        }
        if let Some(g) = spec.optimality_margin {
            if g == 0.0
                || reference_scale
                    .is_some_and(|r| g <= config.high_uncertainty_optimality_ratio * r)
            {
                uncertainty_reasons.push(UncertaintyReason::NearPrimarySwitch);
            }
        }
        if let Some(s) = spec.secondary_margin {
            if s == 0.0
                || reference_scale.is_some_and(|r| s <= config.high_uncertainty_secondary_ratio * r)
            {
                uncertainty_reasons.push(UncertaintyReason::NearSecondarySwitch);
            }
        }
        let high_uncertainty = !uncertainty_reasons.is_empty();

        built_edges_unsorted.push(EvolutionEdge {
            prev_cluster_id: p_sc.cluster_id,
            curr_cluster_id: c_sc.cluster_id,
            edge_role: spec.edge_role,
            event_type: edge_event_type[e_idx],
            rule_basis: spec.rule_basis.clone(),
            distance: d,
            distance_over_scale,
            prev_member_fraction: p_frac,
            curr_member_fraction: c_frac,
            edge_delta_fraction,
            attributed_flow,
            edge_strength,
            threshold_margin,
            threshold_margin_ratio,
            primary_alternative_status: spec.primary_alternative_status,
            optimality_margin: spec.optimality_margin,
            optimality_margin_ratio,
            secondary_margin: spec.secondary_margin,
            secondary_margin_ratio,
            ambiguous_tie: spec.ambiguous_tie,
            high_uncertainty,
            uncertainty_reasons,
        });
    }

    // Build prev_nodes & curr_nodes summaries
    let unmatched_prev_map: HashMap<usize, UnmatchedReason> = backbone
        .unmatched_prev
        .iter()
        .map(|u| {
            let r = if u.reason == "no_candidate_within_threshold" {
                UnmatchedReason::NoCandidateWithinThreshold
            } else {
                UnmatchedReason::NotSelectedInGlobalOptimum
            };
            (u.cluster_id, r)
        })
        .collect();

    let unmatched_curr_map: HashMap<usize, UnmatchedReason> = backbone
        .unmatched_curr
        .iter()
        .map(|u| {
            let r = if u.reason == "no_candidate_within_threshold" {
                UnmatchedReason::NoCandidateWithinThreshold
            } else {
                UnmatchedReason::NotSelectedInGlobalOptimum
            };
            (u.cluster_id, r)
        })
        .collect();

    let mut prev_nodes = Vec::with_capacity(m);
    for (p_orig, p_sc) in prev_scenarios.iter().enumerate() {
        let p_cid = p_sc.cluster_id;
        let incident: Vec<&EvolutionEdge> = built_edges_unsorted
            .iter()
            .filter(|e| e.prev_cluster_id == p_cid)
            .collect();
        let prim_deg = incident
            .iter()
            .filter(|e| e.edge_role == EdgeRole::Primary)
            .count();
        let sec_deg = incident
            .iter()
            .filter(|e| e.edge_role != EdgeRole::Primary)
            .count();

        let (node_role, part_split, part_merge) = if prim_deg == 1 {
            let prim_e = incident
                .iter()
                .find(|e| e.edge_role == EdgeRole::Primary)
                .unwrap();
            match prim_e.event_type {
                TransitionEventKind::Continuation => (NodeRole::ContinuationSource, false, false),
                TransitionEventKind::Split => (NodeRole::SplitParent, true, false),
                TransitionEventKind::Merge => (NodeRole::MergePrimaryParent, false, true),
                _ => unreachable!(),
            }
        } else if sec_deg > 0 {
            (NodeRole::MergeSecondaryParent, false, true)
        } else {
            (NodeRole::Disappeared, false, false)
        };

        let unmatched_reason = unmatched_prev_map.get(&p_cid).copied();
        let (min_cand_d, min_cand_d_scale) = if unmatched_reason.is_some() {
            let min_d = (0..n)
                .map(|j| norm_f64(backbone.distance_matrix[p_orig][j]))
                .min_by(|a, b| a.total_cmp(b))
                .expect("n >= 1");
            (
                Some(min_d),
                checked_opt_ratio(
                    min_d,
                    meta.pair_scale,
                    "prev_nodes.min_candidate_distance_over_scale",
                )?,
            )
        } else {
            (None, None)
        };

        let (prim_counterpart, prim_delta) = if let Some(c_orig) = prev_primary_partner[p_orig] {
            let c_sc = &curr_scenarios[c_orig];
            (
                Some(c_sc.cluster_id),
                Some(norm_f64(c_sc.member_fraction - p_sc.member_fraction)),
            )
        } else {
            (None, None)
        };

        let attributed_flow_fraction = norm_f64(
            fsum(incident.iter().map(|e| e.attributed_flow)).map_err(|_| {
                CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "Prev node attributed flow summation overflow",
                )
            })?,
        );
        let unattributed_fraction = norm_f64(
            fsum([p_sc.member_fraction, -attributed_flow_fraction])
                .map_err(|_| {
                    CoreError::new(
                        CoreErrorCode::NumericalFailure,
                        "Prev node unattributed residual overflow",
                    )
                })?
                .max(0.0),
        );

        let (node_thresh_margin, ambiguous_tie, high_uncertainty) =
            if node_role == NodeRole::Disappeared {
                let min_d = min_cand_d.expect("disappeared node has min_candidate_distance");
                let tm = norm_f64((min_d - tau).max(0.0));
                let hu = tau == 0.0
                    || reference_scale
                        .is_some_and(|r| tm <= config.high_uncertainty_threshold_ratio * r);
                (Some(tm), false, hu)
            } else {
                let amb = incident.iter().any(|e| e.ambiguous_tie);
                let hu = incident.iter().any(|e| e.high_uncertainty);
                (None, amb, hu)
            };

        prev_nodes.push(NodeEvolutionSummary {
            cluster_id: p_cid,
            canonical_rank: prev_rank_of_orig[p_orig],
            member_count: p_sc.member_count,
            member_fraction: norm_f64(p_sc.member_fraction),
            primary_degree: prim_deg,
            secondary_degree: sec_deg,
            node_role,
            participates_in_split: part_split,
            participates_in_merge: part_merge,
            unmatched_reason,
            min_candidate_distance: min_cand_d,
            min_candidate_distance_over_scale: min_cand_d_scale,
            primary_counterpart_cluster_id: prim_counterpart,
            primary_delta_fraction: prim_delta,
            attributed_flow_fraction,
            unattributed_fraction,
            threshold_margin: node_thresh_margin,
            ambiguous_tie,
            high_uncertainty,
        });
    }
    prev_nodes.sort_by_key(|n| n.cluster_id);

    let mut curr_nodes = Vec::with_capacity(n);
    for (c_orig, c_sc) in curr_scenarios.iter().enumerate() {
        let c_cid = c_sc.cluster_id;
        let incident: Vec<&EvolutionEdge> = built_edges_unsorted
            .iter()
            .filter(|e| e.curr_cluster_id == c_cid)
            .collect();
        let prim_deg = incident
            .iter()
            .filter(|e| e.edge_role == EdgeRole::Primary)
            .count();
        let sec_deg = incident
            .iter()
            .filter(|e| e.edge_role != EdgeRole::Primary)
            .count();

        let (node_role, part_split, part_merge) = if prim_deg == 1 {
            let prim_e = incident
                .iter()
                .find(|e| e.edge_role == EdgeRole::Primary)
                .unwrap();
            match prim_e.event_type {
                TransitionEventKind::Continuation => (NodeRole::ContinuationTarget, false, false),
                TransitionEventKind::Split => (NodeRole::SplitPrimaryChild, true, false),
                TransitionEventKind::Merge => (NodeRole::MergeTarget, false, true),
                _ => unreachable!(),
            }
        } else if sec_deg > 0 {
            (NodeRole::SplitSecondaryChild, true, false)
        } else {
            (NodeRole::Emerged, false, false)
        };

        let unmatched_reason = unmatched_curr_map.get(&c_cid).copied();
        let (min_cand_d, min_cand_d_scale) = if unmatched_reason.is_some() {
            let min_d = (0..m)
                .map(|i| norm_f64(backbone.distance_matrix[i][c_orig]))
                .min_by(|a, b| a.total_cmp(b))
                .expect("m >= 1");
            (
                Some(min_d),
                checked_opt_ratio(
                    min_d,
                    meta.pair_scale,
                    "curr_nodes.min_candidate_distance_over_scale",
                )?,
            )
        } else {
            (None, None)
        };

        let (prim_counterpart, prim_delta) = if let Some(p_orig) = curr_primary_partner[c_orig] {
            let p_sc = &prev_scenarios[p_orig];
            (
                Some(p_sc.cluster_id),
                Some(norm_f64(c_sc.member_fraction - p_sc.member_fraction)),
            )
        } else {
            (None, None)
        };

        let attributed_flow_fraction = norm_f64(
            fsum(incident.iter().map(|e| e.attributed_flow)).map_err(|_| {
                CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "Curr node attributed flow summation overflow",
                )
            })?,
        );
        let unattributed_fraction = norm_f64(
            fsum([c_sc.member_fraction, -attributed_flow_fraction])
                .map_err(|_| {
                    CoreError::new(
                        CoreErrorCode::NumericalFailure,
                        "Curr node unattributed residual overflow",
                    )
                })?
                .max(0.0),
        );

        let (node_thresh_margin, ambiguous_tie, high_uncertainty) =
            if node_role == NodeRole::Emerged {
                let min_d = min_cand_d.expect("emerged node has min_candidate_distance");
                let tm = norm_f64((min_d - tau).max(0.0));
                let hu = tau == 0.0
                    || reference_scale
                        .is_some_and(|r| tm <= config.high_uncertainty_threshold_ratio * r);
                (Some(tm), false, hu)
            } else {
                let amb = incident.iter().any(|e| e.ambiguous_tie);
                let hu = incident.iter().any(|e| e.high_uncertainty);
                (None, amb, hu)
            };

        curr_nodes.push(NodeEvolutionSummary {
            cluster_id: c_cid,
            canonical_rank: curr_rank_of_orig[c_orig],
            member_count: c_sc.member_count,
            member_fraction: norm_f64(c_sc.member_fraction),
            primary_degree: prim_deg,
            secondary_degree: sec_deg,
            node_role,
            participates_in_split: part_split,
            participates_in_merge: part_merge,
            unmatched_reason,
            min_candidate_distance: min_cand_d,
            min_candidate_distance_over_scale: min_cand_d_scale,
            primary_counterpart_cluster_id: prim_counterpart,
            primary_delta_fraction: prim_delta,
            attributed_flow_fraction,
            unattributed_fraction,
            threshold_margin: node_thresh_margin,
            ambiguous_tie,
            high_uncertainty,
        });
    }
    curr_nodes.sort_by_key(|n| n.cluster_id);

    // Sort components canonically and build EvolutionEvent records
    components.sort_by_key(|c| c.sort_key);

    let prev_node_by_cid: HashMap<usize, &NodeEvolutionSummary> =
        prev_nodes.iter().map(|n| (n.cluster_id, n)).collect();
    let curr_node_by_cid: HashMap<usize, &NodeEvolutionSummary> =
        curr_nodes.iter().map(|n| (n.cluster_id, n)).collect();

    let mut events = Vec::with_capacity(components.len());
    for (ev_idx, comp) in components.iter().enumerate() {
        let mut p_cids: Vec<usize> = comp
            .p_origs
            .iter()
            .map(|&i| prev_scenarios[i].cluster_id)
            .collect();
        p_cids.sort_unstable();
        let mut c_cids: Vec<usize> = comp
            .c_origs
            .iter()
            .map(|&j| curr_scenarios[j].cluster_id)
            .collect();
        c_cids.sort_unstable();

        let primary_link = comp
            .primary_orig_pair
            .map(|(pi, cj)| (prev_scenarios[pi].cluster_id, curr_scenarios[cj].cluster_id));

        let comp_edges: Vec<&EvolutionEdge> = comp
            .edge_indices
            .iter()
            .map(|&idx| &built_edges_unsorted[idx])
            .collect();

        let any_tiebreak = comp_edges
            .iter()
            .any(|e| e.rule_basis.ends_with("_canonical_tiebreak"));
        let rule_basis = match comp.event_type {
            TransitionEventKind::Continuation => comp_edges[0].rule_basis.clone(),
            TransitionEventKind::Split => {
                if any_tiebreak {
                    "primary_plus_nearest_feasible_split_canonical_tiebreak".to_string()
                } else {
                    "primary_plus_nearest_feasible_split".to_string()
                }
            }
            TransitionEventKind::Merge => {
                if any_tiebreak {
                    "primary_plus_nearest_feasible_merge_canonical_tiebreak".to_string()
                } else {
                    "primary_plus_nearest_feasible_merge".to_string()
                }
            }
            TransitionEventKind::Disappearance | TransitionEventKind::Emergence => {
                "no_candidate_within_threshold".to_string()
            }
            _ => unreachable!(),
        };

        let max_morph_dist = comp_edges
            .iter()
            .map(|e| e.distance)
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0);
        let max_morph_over_scale = checked_opt_ratio(
            max_morph_dist,
            meta.pair_scale,
            "events.max_morphology_over_scale",
        )?;

        let prev_frac_sum = norm_f64(
            fsum(
                comp.p_origs
                    .iter()
                    .map(|&i| prev_scenarios[i].member_fraction),
            )
            .map_err(|_| {
                CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "Event prev_fraction_sum overflow",
                )
            })?,
        );
        let curr_frac_sum = norm_f64(
            fsum(
                comp.c_origs
                    .iter()
                    .map(|&j| curr_scenarios[j].member_fraction),
            )
            .map_err(|_| {
                CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "Event curr_fraction_sum overflow",
                )
            })?,
        );

        let mut net_terms: Vec<f64> = Vec::with_capacity(comp.c_origs.len() + comp.p_origs.len());
        for &j in &comp.c_origs {
            net_terms.push(curr_scenarios[j].member_fraction);
        }
        for &i in &comp.p_origs {
            net_terms.push(-prev_scenarios[i].member_fraction);
        }
        let net_delta_fraction = norm_f64(fsum(net_terms).map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "Event net_delta_fraction overflow",
            )
        })?);

        let attributed_flow_sum = norm_f64(
            fsum(comp_edges.iter().map(|e| e.attributed_flow)).map_err(|_| {
                CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "Event attributed_flow_sum overflow",
                )
            })?,
        );

        let min_margin_ratio = match reference_scale {
            None => None,
            Some(r) => match comp.event_type {
                TransitionEventKind::Continuation
                | TransitionEventKind::Split
                | TransitionEventKind::Merge => {
                    let mut ratios = Vec::new();
                    for e in &comp_edges {
                        if let Some(tr) = e.threshold_margin_ratio {
                            ratios.push(tr);
                        }
                        if let Some(or) = e.optimality_margin_ratio {
                            ratios.push(or);
                        }
                        if let Some(sr) = e.secondary_margin_ratio {
                            ratios.push(sr);
                        }
                    }
                    ratios.into_iter().min_by(|a, b| a.total_cmp(b))
                }
                TransitionEventKind::Disappearance => {
                    let n_sum = prev_node_by_cid[&p_cids[0]];
                    n_sum
                        .threshold_margin
                        .map(|tm| checked_ratio(tm, r, "disappearance.min_margin_ratio"))
                        .transpose()?
                }
                TransitionEventKind::Emergence => {
                    let n_sum = curr_node_by_cid[&c_cids[0]];
                    n_sum
                        .threshold_margin
                        .map(|tm| checked_ratio(tm, r, "emergence.min_margin_ratio"))
                        .transpose()?
                }
                _ => unreachable!(),
            },
        };

        let (ev_amb, ev_hu) = match comp.event_type {
            TransitionEventKind::Continuation
            | TransitionEventKind::Split
            | TransitionEventKind::Merge => (
                comp_edges.iter().any(|e| e.ambiguous_tie),
                comp_edges.iter().any(|e| e.high_uncertainty),
            ),
            TransitionEventKind::Disappearance => {
                let n_sum = prev_node_by_cid[&p_cids[0]];
                (n_sum.ambiguous_tie, n_sum.high_uncertainty)
            }
            TransitionEventKind::Emergence => {
                let n_sum = curr_node_by_cid[&c_cids[0]];
                (n_sum.ambiguous_tie, n_sum.high_uncertainty)
            }
            _ => unreachable!(),
        };

        events.push(EvolutionEvent {
            event_index: ev_idx,
            event_type: comp.event_type,
            prev_cluster_ids: p_cids,
            curr_cluster_ids: c_cids,
            primary_link,
            rule_basis,
            max_morphology_distance: max_morph_dist,
            max_morphology_over_scale: max_morph_over_scale,
            prev_fraction_sum: prev_frac_sum,
            curr_fraction_sum: curr_frac_sum,
            net_delta_fraction,
            attributed_flow_sum,
            min_margin_ratio,
            ambiguous_tie: ev_amb,
            high_uncertainty: ev_hu,
        });
    }

    // Sort edges by (prev_cluster_id, curr_cluster_id) ascending
    let mut edges = built_edges_unsorted;
    edges.sort_by_key(|e| (e.prev_cluster_id, e.curr_cluster_id));

    // Compute TransitionEvolutionSummary
    let primary_edge_count = edges
        .iter()
        .filter(|e| e.edge_role == EdgeRole::Primary)
        .count();
    let secondary_split_edge_count = edges
        .iter()
        .filter(|e| e.edge_role == EdgeRole::SecondarySplit)
        .count();
    let secondary_merge_edge_count = edges
        .iter()
        .filter(|e| e.edge_role == EdgeRole::SecondaryMerge)
        .count();

    let continuation_count = events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Continuation)
        .count();
    let split_count = events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Split)
        .count();
    let merge_count = events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Merge)
        .count();
    let emergence_count = events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Emergence)
        .count();
    let disappearance_count = events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Disappearance)
        .count();

    let primary_total_distance = checked_fsum_finite(
        edges
            .iter()
            .filter(|e| e.edge_role == EdgeRole::Primary)
            .map(|e| e.distance),
        "primary_total_distance",
    )?;
    let primary_max_distance = edges
        .iter()
        .filter(|e| e.edge_role == EdgeRole::Primary)
        .map(|e| e.distance)
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(0.0);
    let primary_max_distance_over_scale = checked_opt_ratio(
        primary_max_distance,
        meta.pair_scale,
        "summary.primary_max_distance_over_scale",
    )?;
    let all_edges_max_distance = edges
        .iter()
        .map(|e| e.distance)
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(0.0);
    let all_edges_max_distance_over_scale = checked_opt_ratio(
        all_edges_max_distance,
        meta.pair_scale,
        "summary.all_edges_max_distance_over_scale",
    )?;

    // transition_tv_redistribution = 0.5 * fsum_{b in B} |q(b) - p(b)|
    let mut branch_l1_terms: Vec<f64> = Vec::with_capacity(m + n);
    for e in &edges {
        if e.edge_role == EdgeRole::Primary {
            branch_l1_terms.push((e.curr_member_fraction - e.prev_member_fraction).abs());
        }
    }
    for (i, p_sc) in prev_scenarios.iter().enumerate() {
        if prev_primary_partner[i].is_none() {
            branch_l1_terms.push(p_sc.member_fraction.abs());
        }
    }
    for (j, c_sc) in curr_scenarios.iter().enumerate() {
        if curr_primary_partner[j].is_none() {
            branch_l1_terms.push(c_sc.member_fraction.abs());
        }
    }
    let l1_branch = fsum(branch_l1_terms).map_err(|_| {
        CoreError::new(
            CoreErrorCode::NumericalFailure,
            "transition_tv_redistribution overflow",
        )
    })?;
    let transition_tv_redistribution = norm_f64(0.5 * l1_branch);

    let secondary_attributed_flow = norm_f64(
        fsum(
            edges
                .iter()
                .filter(|e| e.edge_role != EdgeRole::Primary)
                .map(|e| e.attributed_flow),
        )
        .map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "secondary_attributed_flow overflow",
            )
        })?,
    );
    let total_attributed_flow =
        norm_f64(fsum(edges.iter().map(|e| e.attributed_flow)).map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "total_attributed_flow overflow",
            )
        })?);
    let disappeared_fraction_sum = norm_f64(
        fsum(
            prev_nodes
                .iter()
                .filter(|n| n.node_role == NodeRole::Disappeared)
                .map(|n| n.member_fraction),
        )
        .map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "disappeared_fraction_sum overflow",
            )
        })?,
    );
    let emerged_fraction_sum = norm_f64(
        fsum(
            curr_nodes
                .iter()
                .filter(|n| n.node_role == NodeRole::Emerged)
                .map(|n| n.member_fraction),
        )
        .map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "emerged_fraction_sum overflow",
            )
        })?,
    );
    let component_tv_residual = norm_f64(
        0.5 * fsum(events.iter().map(|ev| ev.net_delta_fraction.abs())).map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "component_tv_residual overflow",
            )
        })?,
    );

    // Active threshold margin over E_evol U {(i, j) : d_{i,j} > tau}
    // and raw_matrix_threshold_margin over all (i, j)
    let active_pair_set: HashSet<(usize, usize)> =
        raw_edges.iter().map(|s| (s.p_orig, s.c_orig)).collect();

    let mut min_threshold_margin = f64::INFINITY;
    let mut raw_matrix_threshold_margin = f64::INFINITY;
    for i in 0..m {
        for j in 0..n {
            let d = backbone.distance_matrix[i][j];
            let margin = norm_f64((d - tau).abs());
            if margin.total_cmp(&raw_matrix_threshold_margin) == Ordering::Less {
                raw_matrix_threshold_margin = margin;
            }
            if d > tau || active_pair_set.contains(&(i, j)) {
                if margin.total_cmp(&min_threshold_margin) == Ordering::Less {
                    min_threshold_margin = margin;
                }
            }
        }
    }
    debug_assert!(min_threshold_margin.is_finite());
    debug_assert!(raw_matrix_threshold_margin.is_finite());

    let min_threshold_margin_ratio = checked_opt_ratio(
        min_threshold_margin,
        reference_scale,
        "summary.min_threshold_margin_ratio",
    )?;

    let min_optimality_margin = edges
        .iter()
        .filter_map(|e| e.optimality_margin)
        .min_by(|a, b| a.total_cmp(b));
    let min_optimality_margin_ratio = match (min_optimality_margin, reference_scale) {
        (Some(v), Some(r)) => Some(checked_ratio(v, r, "summary.min_optimality_margin_ratio")?),
        _ => None,
    };

    let min_secondary_margin = edges
        .iter()
        .filter_map(|e| e.secondary_margin)
        .min_by(|a, b| a.total_cmp(b));
    let min_secondary_margin_ratio = match (min_secondary_margin, reference_scale) {
        (Some(v), Some(r)) => Some(checked_ratio(v, r, "summary.min_secondary_margin_ratio")?),
        _ => None,
    };

    let ambiguous_tie = backbone.optima_count > 1 || edges.iter().any(|e| e.ambiguous_tie);
    let high_uncertainty_edge_count = edges.iter().filter(|e| e.high_uncertainty).count();
    let near_active_threshold = tau == 0.0
        || reference_scale
            .is_some_and(|r| min_threshold_margin <= config.high_uncertainty_threshold_ratio * r);
    let high_uncertainty = ambiguous_tie
        || high_uncertainty_edge_count > 0
        || prev_nodes.iter().any(|n| n.high_uncertainty)
        || curr_nodes.iter().any(|n| n.high_uncertainty)
        || near_active_threshold;

    let summary = TransitionEvolutionSummary {
        primary_edge_count,
        secondary_split_edge_count,
        secondary_merge_edge_count,
        continuation_count,
        split_count,
        merge_count,
        emergence_count,
        disappearance_count,
        primary_total_distance,
        primary_max_distance,
        primary_max_distance_over_scale,
        all_edges_max_distance,
        all_edges_max_distance_over_scale,
        transition_tv_redistribution,
        secondary_attributed_flow,
        total_attributed_flow,
        disappeared_fraction_sum,
        emerged_fraction_sum,
        component_tv_residual,
        min_threshold_margin,
        min_threshold_margin_ratio,
        raw_matrix_threshold_margin,
        min_optimality_margin,
        min_optimality_margin_ratio,
        min_secondary_margin,
        min_secondary_margin_ratio,
        ambiguous_tie,
        high_uncertainty_edge_count,
        high_uncertainty,
    };

    Ok(EvolutionResult {
        algorithm_version: EVOLUTION_ALGORITHM_VERSION.to_string(),
        secondary_rule: config.secondary_rule.as_str().to_string(),
        flow_rule: config.flow_rule.as_str().to_string(),
        uncertainty_rule: UNCERTAINTY_RULE_V1.to_string(),
        non_physical_disclaimer: NON_PHYSICAL_DISCLAIMER.to_string(),
        prev_cluster_ids: prev_scenarios.iter().map(|s| s.cluster_id).collect(),
        curr_cluster_ids: curr_scenarios.iter().map(|s| s.cluster_id).collect(),
        threshold: tau,
        pair_scale: meta.pair_scale,
        threshold_coefficient: meta.threshold_coefficient,
        prev_dispersion: meta.prev_dispersion,
        curr_dispersion: meta.curr_dispersion,
        reference_scale,
        backbone,
        edges,
        prev_nodes,
        curr_nodes,
        events,
        summary,
    })
}

// ---------------------------------------------------------------------------
// Multi-Batch Pure In-Memory Chain Orchestration (`evolve_chain`)
// ---------------------------------------------------------------------------

/// Orchestrate scenario evolution across `4..=8` planned sequential batches in pure memory.
pub fn evolve_chain(input: &EvolutionChainInput) -> Result<EvolutionChainResult, CoreError> {
    validate_evolution_config(&input.config)?;

    let planned_batch_count = input.batches.len();
    if !(MIN_CHAIN_BATCHES..=MAX_CHAIN_BATCHES).contains(&planned_batch_count) {
        return Err(CoreError::new(
            CoreErrorCode::InvalidEvolutionConfig,
            format!(
                "planned_batch_count must be in {MIN_CHAIN_BATCHES}..={MAX_CHAIN_BATCHES}, got {planned_batch_count}"
            ),
        ));
    }

    let prefix12: String = match &input.run_key_prefix {
        None => DEFAULT_CHAIN_RUN_KEY_PREFIX.to_string(),
        Some(s) => {
            if s.is_empty() {
                return Err(CoreError::new(
                    CoreErrorCode::InvalidEvolutionConfig,
                    "run_key_prefix must not be empty when provided",
                ));
            }
            s.chars().take(12).collect()
        }
    };

    if !input.threshold_coefficient.is_finite() || input.threshold_coefficient < 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidThreshold,
            format!(
                "Threshold coefficient must be finite and >= 0.0, got {}",
                input.threshold_coefficient
            ),
        ));
    }

    let w_sum = validate_weights(&input.weights)?;
    let n_points = input.weights.len();

    struct OkBatchData {
        scenarios: Vec<ScenarioNodeInput>,
        dispersion: f64,
        rank_by_cid: HashMap<usize, usize>,
    }

    let mut resolved_batches = Vec::with_capacity(planned_batch_count);
    let mut ok_data: Vec<Option<OkBatchData>> = Vec::with_capacity(planned_batch_count);

    let mut ok_batch_count = 0usize;
    let mut missing_batch_count = 0usize;
    let mut failed_batch_count = 0usize;

    for (idx, b) in input.batches.iter().enumerate() {
        if b.ordinal != idx {
            return Err(CoreError::new(
                CoreErrorCode::InvalidEvolutionConfig,
                format!("batches[{idx}].ordinal must equal {idx}, got {}", b.ordinal),
            ));
        }

        match &b.status {
            ChainBatchStatus::Ok {
                scenarios,
                dispersion,
            } => {
                if !dispersion.is_finite() {
                    return Err(CoreError::new(
                        CoreErrorCode::InvalidThreshold,
                        format!("Batch {idx} dispersion must be finite, got {dispersion}"),
                    ));
                }
                if *dispersion <= 0.0 {
                    return Err(CoreError::new(
                        CoreErrorCode::ZeroDispersion,
                        format!("Batch {idx} dispersion must be > 0.0, got {dispersion}"),
                    ));
                }
                validate_standalone_batch_scenarios(
                    scenarios,
                    n_points,
                    input.config.fraction_sum_tolerance,
                    Some(idx),
                )?;

                let canon = canonical_scenario_order(scenarios);
                let mut rank_by_cid = HashMap::with_capacity(scenarios.len());
                for (rank, &orig_i) in canon.iter().enumerate() {
                    rank_by_cid.insert(scenarios[orig_i].cluster_id, rank);
                }

                let mut sorted_scens = scenarios.clone();
                for sc in &mut sorted_scens {
                    sc.member_fraction = norm_f64(sc.member_fraction);
                    for v in &mut sc.mean_features {
                        *v = norm_f64(*v);
                    }
                }
                sorted_scens.sort_by_key(|s| s.cluster_id);

                ok_batch_count += 1;
                resolved_batches.push(ChainResolvedBatch {
                    ordinal: idx,
                    status: "ok".to_string(),
                    status_code: None,
                    status_reason: None,
                    dispersion: Some(norm_f64(*dispersion)),
                });
                ok_data.push(Some(OkBatchData {
                    scenarios: sorted_scens,
                    dispersion: norm_f64(*dispersion),
                    rank_by_cid,
                }));
            }
            ChainBatchStatus::Missing { reason } => {
                missing_batch_count += 1;
                resolved_batches.push(ChainResolvedBatch {
                    ordinal: idx,
                    status: "missing".to_string(),
                    status_code: None,
                    status_reason: Some(reason.clone()),
                    dispersion: None,
                });
                ok_data.push(None);
            }
            ChainBatchStatus::Failed { code, reason } => {
                if code.is_empty()
                    || !WHITELISTED_SINGLE_BATCH_FAILURE_CODES.contains(&code.as_str())
                {
                    return Err(CoreError::new(
                        CoreErrorCode::InvalidEvolutionConfig,
                        format!(
                            "batch {idx} Failed status carries non-whitelisted error_code '{code}'"
                        ),
                    )
                    .with_prev_index(idx));
                }
                failed_batch_count += 1;
                resolved_batches.push(ChainResolvedBatch {
                    ordinal: idx,
                    status: "failed".to_string(),
                    status_code: Some(code.clone()),
                    status_reason: Some(reason.clone()),
                    dispersion: None,
                });
                ok_data.push(None);
            }
        }
    }

    // 1. Evolve every adjacent 'ok' pair (prev_ord, prev_ord + 1)
    let mut pair_results: Vec<Option<EvolutionResult>> =
        Vec::with_capacity(planned_batch_count - 1);
    for prev_ord in 0..(planned_batch_count - 1) {
        if let (Some(prev_b), Some(curr_b)) = (&ok_data[prev_ord], &ok_data[prev_ord + 1]) {
            let res = evolve_scenarios_scaled(&ScaledEvolutionInput {
                prev_scenarios: prev_b.scenarios.clone(),
                curr_scenarios: curr_b.scenarios.clone(),
                weights: input.weights.clone(),
                threshold_coefficient: input.threshold_coefficient,
                prev_dispersion: prev_b.dispersion,
                curr_dispersion: curr_b.dispersion,
                config: input.config.clone(),
                backbone: None,
            })?;
            pair_results.push(Some(res));
        } else {
            pair_results.push(None);
        }
    }

    // 2. Assign deterministic M2-identical `branch_id` and `assignment_reason`
    // Map (ordinal, cluster_id) -> (branch_id, assignment_reason)
    let mut branch_map: HashMap<(usize, usize), (String, String)> = HashMap::new();

    for ord in 0..planned_batch_count {
        let Some(b_data) = &ok_data[ord] else {
            continue;
        };

        let incoming_res = if ord > 0 {
            pair_results[ord - 1].as_ref()
        } else {
            None
        };

        for sc in &b_data.scenarios {
            let cid = sc.cluster_id;
            let rank = b_data.rank_by_cid[&cid];

            if let Some(res) = incoming_res {
                if let Some(prim_edge) = res
                    .edges
                    .iter()
                    .find(|e| e.edge_role == EdgeRole::Primary && e.curr_cluster_id == cid)
                {
                    let (parent_branch, _) = branch_map
                        .get(&(ord - 1, prim_edge.prev_cluster_id))
                        .expect("parent batch processed before current batch");
                    let assign_reason = if prim_edge.ambiguous_tie {
                        "inherited_ambiguous"
                    } else {
                        "inherited"
                    };
                    branch_map.insert(
                        (ord, cid),
                        (parent_branch.clone(), assign_reason.to_string()),
                    );
                    continue;
                }

                // Unmatched in M* on an 'ok' transition
                let curr_node = res
                    .curr_nodes
                    .iter()
                    .find(|n| n.cluster_id == cid)
                    .expect("curr_node exists");
                let assign_reason = curr_node
                    .unmatched_reason
                    .expect("unmatched node has reason")
                    .as_str()
                    .to_string();
                let new_branch = format!("{prefix12}:b{ord:02}:k{rank:02}");
                branch_map.insert((ord, cid), (new_branch, assign_reason));
            } else {
                // First batch (ord == 0) or preceded by Missing/Failed batch
                let assign_reason = if ord == 0 {
                    "first_batch"
                } else {
                    "previous_planned_batch_unavailable"
                };
                let new_branch = format!("{prefix12}:b{ord:02}:k{rank:02}");
                branch_map.insert((ord, cid), (new_branch, assign_reason.to_string()));
            }
        }
    }

    // 3. Build ChainNodeSummary list (ordered by (ordinal, cluster_id))
    let mut nodes: Vec<ChainNodeSummary> = Vec::new();
    for ord in 0..planned_batch_count {
        let Some(b_data) = &ok_data[ord] else {
            continue;
        };
        let in_res = if ord > 0 {
            pair_results[ord - 1].as_ref()
        } else {
            None
        };
        let out_res = if ord + 1 < planned_batch_count {
            pair_results[ord].as_ref()
        } else {
            None
        };

        for sc in &b_data.scenarios {
            let cid = sc.cluster_id;
            let rank = b_data.rank_by_cid[&cid];
            let (b_id, assign_reason) = branch_map[&(ord, cid)].clone();

            let (in_prim, in_sec, in_role, prim_delta, unattr_in, in_hu) = if ord == 0 {
                (0, 0, "first_batch".to_string(), None, 0.0, false)
            } else if let Some(res) = in_res {
                let cn = res
                    .curr_nodes
                    .iter()
                    .find(|n| n.cluster_id == cid)
                    .expect("curr_node exists");
                (
                    cn.primary_degree,
                    cn.secondary_degree,
                    cn.node_role.as_str().to_string(),
                    cn.primary_delta_fraction,
                    cn.unattributed_fraction,
                    cn.high_uncertainty,
                )
            } else {
                (0, 0, "gap_restarted".to_string(), None, 0.0, false)
            };

            let (out_prim, out_sec, out_role, unattr_out, out_hu) =
                if ord + 1 == planned_batch_count {
                    (0, 0, "final_batch".to_string(), 0.0, false)
                } else if let Some(res) = out_res {
                    let pn = res
                        .prev_nodes
                        .iter()
                        .find(|n| n.cluster_id == cid)
                        .expect("prev_node exists");
                    (
                        pn.primary_degree,
                        pn.secondary_degree,
                        pn.node_role.as_str().to_string(),
                        pn.unattributed_fraction,
                        pn.high_uncertainty,
                    )
                } else {
                    (0, 0, "gap_interrupted".to_string(), 0.0, false)
                };

            nodes.push(ChainNodeSummary {
                ordinal: ord,
                cluster_id: cid,
                canonical_rank: rank,
                branch_id: b_id,
                mean_features: sc.mean_features.clone(),
                member_count: sc.member_count,
                member_fraction: sc.member_fraction,
                assignment_reason: assign_reason,
                in_degree_primary: in_prim,
                in_degree_secondary: in_sec,
                out_degree_primary: out_prim,
                out_degree_secondary: out_sec,
                incoming_role: in_role,
                outgoing_role: out_role,
                primary_delta_fraction: prim_delta,
                unattributed_inflow_fraction: unattr_in,
                unattributed_outflow_fraction: unattr_out,
                high_uncertainty: in_hu || out_hu,
            });
        }
    }

    // 4. Build ChainTransitionRecord (B - 1 rows), ChainEvolutionEdge, and ChainEvolutionEvent
    let mut transitions: Vec<ChainTransitionRecord> = Vec::with_capacity(planned_batch_count - 1);
    let mut all_edges: Vec<ChainEvolutionEdge> = Vec::new();
    let mut all_events: Vec<ChainEvolutionEvent> = Vec::new();

    for prev_ord in 0..(planned_batch_count - 1) {
        let curr_ord = prev_ord + 1;
        if let Some(res) = pair_results[prev_ord].clone() {
            for e in &res.edges {
                let prev_bid = branch_map[&(prev_ord, e.prev_cluster_id)].0.clone();
                let curr_bid = branch_map[&(curr_ord, e.curr_cluster_id)].0.clone();
                if e.edge_role == EdgeRole::Primary {
                    debug_assert_eq!(prev_bid, curr_bid);
                }
                all_edges.push(ChainEvolutionEdge {
                    prev_ordinal: prev_ord,
                    curr_ordinal: curr_ord,
                    prev_branch_id: prev_bid,
                    curr_branch_id: curr_bid,
                    edge: e.clone(),
                    non_physical_disclaimer: NON_PHYSICAL_DISCLAIMER.to_string(),
                });
            }

            let mut t_events = Vec::with_capacity(res.events.len());
            for ev in &res.events {
                let primary_branch_id = ev
                    .primary_link
                    .map(|(p_cid, _)| branch_map[&(prev_ord, p_cid)].0.clone());

                let mut b_set: Vec<String> = Vec::new();
                for &p_cid in &ev.prev_cluster_ids {
                    let b = &branch_map[&(prev_ord, p_cid)].0;
                    if !b_set.contains(b) {
                        b_set.push(b.clone());
                    }
                }
                for &c_cid in &ev.curr_cluster_ids {
                    let b = &branch_map[&(curr_ord, c_cid)].0;
                    if !b_set.contains(b) {
                        b_set.push(b.clone());
                    }
                }
                b_set.sort();

                let c_ev = ChainEvolutionEvent {
                    prev_ordinal: prev_ord,
                    event_index: ev.event_index,
                    event_type: ev.event_type,
                    prev_cluster_ids: ev.prev_cluster_ids.clone(),
                    curr_cluster_ids: ev.curr_cluster_ids.clone(),
                    branch_ids: b_set,
                    primary_branch_id,
                    rule_basis: ev.rule_basis.clone(),
                    max_morphology_distance: ev.max_morphology_distance,
                    max_morphology_over_scale: ev.max_morphology_over_scale,
                    prev_fraction_sum: ev.prev_fraction_sum,
                    curr_fraction_sum: ev.curr_fraction_sum,
                    net_delta_fraction: ev.net_delta_fraction,
                    attributed_flow_sum: ev.attributed_flow_sum,
                    min_margin_ratio: ev.min_margin_ratio,
                    ambiguous_tie: ev.ambiguous_tie,
                    high_uncertainty: ev.high_uncertainty,
                    non_physical_disclaimer: NON_PHYSICAL_DISCLAIMER.to_string(),
                };
                t_events.push(c_ev.clone());
                all_events.push(c_ev);
            }

            transitions.push(ChainTransitionRecord {
                prev_ordinal: prev_ord,
                curr_ordinal: curr_ord,
                transition_status: ChainTransitionStatus::Ok,
                chain_broken: false,
                gap_reason: None,
                pair_scale: res.pair_scale,
                threshold: Some(res.threshold),
                primary_edge_count: res.summary.primary_edge_count,
                secondary_split_edge_count: res.summary.secondary_split_edge_count,
                secondary_merge_edge_count: res.summary.secondary_merge_edge_count,
                transition_tv_redistribution: Some(res.summary.transition_tv_redistribution),
                min_threshold_margin: Some(res.summary.min_threshold_margin),
                ambiguous_tie: res.summary.ambiguous_tie,
                high_uncertainty: res.summary.high_uncertainty,
                evolve_result: Some(res),
                events: t_events,
            });
        } else {
            let prev_ok = ok_data[prev_ord].as_ref();
            let curr_ok = ok_data[curr_ord].as_ref();
            let gap_reason = match (prev_ok.is_some(), curr_ok.is_some()) {
                (true, false) => "curr_unavailable",
                (false, true) => "prev_unavailable",
                (false, false) => "both_unavailable",
                (true, true) => unreachable!(),
            };

            let mut t_events = Vec::new();
            if let Some(pb) = prev_ok {
                let mut ordered: Vec<&ScenarioNodeInput> = pb.scenarios.iter().collect();
                ordered.sort_by_key(|sc| pb.rank_by_cid[&sc.cluster_id]);
                for (ev_idx, sc) in ordered.iter().enumerate() {
                    let b_id = branch_map[&(prev_ord, sc.cluster_id)].0.clone();
                    let c_ev = ChainEvolutionEvent {
                        prev_ordinal: prev_ord,
                        event_index: ev_idx,
                        event_type: TransitionEventKind::GapInterrupted,
                        prev_cluster_ids: vec![sc.cluster_id],
                        curr_cluster_ids: Vec::new(),
                        branch_ids: vec![b_id],
                        primary_branch_id: None,
                        rule_basis: "next_planned_batch_unavailable".to_string(),
                        max_morphology_distance: 0.0,
                        max_morphology_over_scale: Some(0.0),
                        prev_fraction_sum: sc.member_fraction,
                        curr_fraction_sum: 0.0,
                        net_delta_fraction: norm_f64(-sc.member_fraction),
                        attributed_flow_sum: 0.0,
                        min_margin_ratio: None,
                        ambiguous_tie: false,
                        high_uncertainty: false,
                        non_physical_disclaimer: NON_PHYSICAL_DISCLAIMER.to_string(),
                    };
                    t_events.push(c_ev.clone());
                    all_events.push(c_ev);
                }
            } else if let Some(cb) = curr_ok {
                let mut ordered: Vec<&ScenarioNodeInput> = cb.scenarios.iter().collect();
                ordered.sort_by_key(|sc| cb.rank_by_cid[&sc.cluster_id]);
                for (ev_idx, sc) in ordered.iter().enumerate() {
                    let b_id = branch_map[&(curr_ord, sc.cluster_id)].0.clone();
                    let c_ev = ChainEvolutionEvent {
                        prev_ordinal: prev_ord,
                        event_index: ev_idx,
                        event_type: TransitionEventKind::GapRestarted,
                        prev_cluster_ids: Vec::new(),
                        curr_cluster_ids: vec![sc.cluster_id],
                        branch_ids: vec![b_id],
                        primary_branch_id: None,
                        rule_basis: "previous_planned_batch_unavailable".to_string(),
                        max_morphology_distance: 0.0,
                        max_morphology_over_scale: Some(0.0),
                        prev_fraction_sum: 0.0,
                        curr_fraction_sum: sc.member_fraction,
                        net_delta_fraction: sc.member_fraction,
                        attributed_flow_sum: 0.0,
                        min_margin_ratio: None,
                        ambiguous_tie: false,
                        high_uncertainty: false,
                        non_physical_disclaimer: NON_PHYSICAL_DISCLAIMER.to_string(),
                    };
                    t_events.push(c_ev.clone());
                    all_events.push(c_ev);
                }
            }

            transitions.push(ChainTransitionRecord {
                prev_ordinal: prev_ord,
                curr_ordinal: curr_ord,
                transition_status: ChainTransitionStatus::Gap,
                chain_broken: true,
                gap_reason: Some(gap_reason.to_string()),
                pair_scale: None,
                threshold: None,
                primary_edge_count: 0,
                secondary_split_edge_count: 0,
                secondary_merge_edge_count: 0,
                transition_tv_redistribution: None,
                min_threshold_margin: None,
                ambiguous_tie: false,
                high_uncertainty: false,
                evolve_result: None,
                events: t_events,
            });
        }
    }

    // 5. Build EvolutionBranchSummary list (ordered by branch_id ascending)
    let mut nodes_by_branch: BTreeMap<String, Vec<&ChainNodeSummary>> = BTreeMap::new();
    for node in &nodes {
        nodes_by_branch
            .entry(node.branch_id.clone())
            .or_default()
            .push(node);
    }

    let mut branches: Vec<EvolutionBranchSummary> = Vec::with_capacity(nodes_by_branch.len());
    for (b_id, mut b_nodes) in nodes_by_branch {
        b_nodes.sort_by_key(|n| n.ordinal);
        let birth_node = b_nodes.first().expect("branch has >= 1 node");
        let end_node = b_nodes.last().expect("branch has >= 1 node");
        let birth_ordinal = birth_node.ordinal;
        let end_ordinal = end_node.ordinal;
        let node_count = b_nodes.len();
        debug_assert_eq!(node_count, end_ordinal - birth_ordinal + 1);
        let step_count = node_count - 1;

        let birth_reason = match birth_node.incoming_role.as_str() {
            "first_batch" => "first_batch",
            "gap_restarted" => "gap_restarted",
            "split_secondary_child" => "secondary_split",
            "emerged" => "emergence",
            other => other,
        }
        .to_string();

        let termination_reason = match end_node.outgoing_role.as_str() {
            "final_batch" => "final_batch",
            "gap_interrupted" => "gap_interrupted",
            "merge_secondary_parent" => "secondary_merge",
            "disappeared" => "disappearance",
            other => other,
        }
        .to_string();

        let prim_steps: Vec<&ChainEvolutionEdge> = all_edges
            .iter()
            .filter(|ce| ce.edge.edge_role == EdgeRole::Primary && ce.prev_branch_id == b_id)
            .collect();
        debug_assert_eq!(prim_steps.len(), step_count);

        let cumulative_morphology_distance = checked_fsum_finite(
            prim_steps.iter().map(|ce| ce.edge.distance),
            "Branch cumulative_morphology_distance",
        )?;
        let cumulative_morphology_over_scale = checked_fsum_finite(
            prim_steps
                .iter()
                .map(|ce| ce.edge.distance_over_scale.expect("chain edges are scaled")),
            "Branch cumulative_morphology_over_scale",
        )?;
        let max_step_morphology_distance = prim_steps
            .iter()
            .map(|ce| ce.edge.distance)
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0);
        let max_step_morphology_over_scale = prim_steps
            .iter()
            .map(|ce| ce.edge.distance_over_scale.unwrap_or(0.0))
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0);

        let end_to_end_morphology_distance = if step_count == 0 {
            0.0
        } else {
            norm_f64(compute_weighted_rmse(
                &birth_node.mean_features,
                &end_node.mean_features,
                &input.weights,
                w_sum,
            )?)
        };

        let initial_member_fraction = birth_node.member_fraction;
        let final_member_fraction = end_node.member_fraction;
        let min_member_fraction = b_nodes
            .iter()
            .map(|n| n.member_fraction)
            .min_by(|a, b| a.total_cmp(b))
            .expect("branch non-empty");
        let max_member_fraction = b_nodes
            .iter()
            .map(|n| n.member_fraction)
            .max_by(|a, b| a.total_cmp(b))
            .expect("branch non-empty");
        let net_delta_fraction = norm_f64(final_member_fraction - initial_member_fraction);
        let cumulative_abs_delta_fraction = checked_fsum_finite(
            b_nodes
                .windows(2)
                .map(|w| (w[1].member_fraction - w[0].member_fraction).abs()),
            "Branch cumulative_abs_delta_fraction",
        )?;

        let split_out_event_count = all_events
            .iter()
            .filter(|ev| {
                ev.event_type == TransitionEventKind::Split
                    && ev.primary_branch_id.as_deref() == Some(b_id.as_str())
            })
            .count();
        let mut spawned_secondary_pairs: Vec<(usize, String)> = all_edges
            .iter()
            .filter(|ce| ce.edge.edge_role == EdgeRole::SecondarySplit && ce.prev_branch_id == b_id)
            .map(|ce| (ce.prev_ordinal, ce.curr_branch_id.clone()))
            .collect();
        spawned_secondary_pairs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let spawned_secondary_branch_ids: Vec<String> = spawned_secondary_pairs
            .into_iter()
            .map(|(_, bid)| bid)
            .collect();
        let split_from_parent_branch_id: Option<String> = all_edges
            .iter()
            .find(|ce| {
                ce.edge.edge_role == EdgeRole::SecondarySplit
                    && ce.curr_ordinal == birth_ordinal
                    && ce.curr_branch_id == b_id
            })
            .map(|ce| ce.prev_branch_id.clone());

        let merge_in_event_count = all_events
            .iter()
            .filter(|ev| {
                ev.event_type == TransitionEventKind::Merge
                    && ev.primary_branch_id.as_deref() == Some(b_id.as_str())
            })
            .count();
        let mut absorbed_secondary_pairs: Vec<(usize, String)> = all_edges
            .iter()
            .filter(|ce| ce.edge.edge_role == EdgeRole::SecondaryMerge && ce.curr_branch_id == b_id)
            .map(|ce| (ce.prev_ordinal, ce.prev_branch_id.clone()))
            .collect();
        absorbed_secondary_pairs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let absorbed_secondary_branch_ids: Vec<String> = absorbed_secondary_pairs
            .into_iter()
            .map(|(_, bid)| bid)
            .collect();
        let merged_into_branch_id: Option<String> = all_edges
            .iter()
            .find(|ce| {
                ce.edge.edge_role == EdgeRole::SecondaryMerge
                    && ce.prev_ordinal == end_ordinal
                    && ce.prev_branch_id == b_id
            })
            .map(|ce| ce.curr_branch_id.clone());

        let ambiguous_step_count = prim_steps.iter().filter(|ce| ce.edge.ambiguous_tie).count();
        let high_uncertainty_step_count = prim_steps
            .iter()
            .filter(|ce| ce.edge.high_uncertainty)
            .count();
        let high_uncertainty_step_ratio = if step_count > 0 {
            checked_ratio(
                high_uncertainty_step_count as f64,
                step_count as f64,
                "branch.high_uncertainty_step_ratio",
            )?
        } else {
            0.0
        };

        branches.push(EvolutionBranchSummary {
            branch_id: b_id,
            birth_ordinal,
            end_ordinal,
            node_count,
            step_count,
            birth_reason,
            termination_reason,
            cumulative_morphology_distance,
            cumulative_morphology_over_scale,
            max_step_morphology_distance,
            max_step_morphology_over_scale,
            end_to_end_morphology_distance,
            initial_member_fraction,
            final_member_fraction,
            min_member_fraction,
            max_member_fraction,
            net_delta_fraction,
            cumulative_abs_delta_fraction,
            split_out_event_count,
            spawned_secondary_branch_ids,
            split_from_parent_branch_id,
            merge_in_event_count,
            absorbed_secondary_branch_ids,
            merged_into_branch_id,
            ambiguous_step_count,
            high_uncertainty_step_count,
            high_uncertainty_step_ratio,
        });
    }

    // 6. Build EvolutionChainSummary
    let ok_transition_count = transitions
        .iter()
        .filter(|t| t.transition_status == ChainTransitionStatus::Ok)
        .count();
    let gap_transition_count = transitions
        .iter()
        .filter(|t| t.transition_status == ChainTransitionStatus::Gap)
        .count();
    let branch_count = branches.len();

    let primary_edge_count = all_edges
        .iter()
        .filter(|ce| ce.edge.edge_role == EdgeRole::Primary)
        .count();
    let secondary_split_edge_count = all_edges
        .iter()
        .filter(|ce| ce.edge.edge_role == EdgeRole::SecondarySplit)
        .count();
    let secondary_merge_edge_count = all_edges
        .iter()
        .filter(|ce| ce.edge.edge_role == EdgeRole::SecondaryMerge)
        .count();

    let continuation_count = all_events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Continuation)
        .count();
    let split_count = all_events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Split)
        .count();
    let merge_count = all_events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Merge)
        .count();
    let emergence_count = all_events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Emergence)
        .count();
    let disappearance_count = all_events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::Disappearance)
        .count();
    let gap_interrupted_count = all_events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::GapInterrupted)
        .count();
    let gap_restarted_count = all_events
        .iter()
        .filter(|ev| ev.event_type == TransitionEventKind::GapRestarted)
        .count();

    let cumulative_primary_distance = checked_fsum_finite(
        all_edges
            .iter()
            .filter(|ce| ce.edge.edge_role == EdgeRole::Primary)
            .map(|ce| ce.edge.distance),
        "Chain cumulative_primary_distance",
    )?;
    let max_edge_distance = all_edges
        .iter()
        .map(|ce| ce.edge.distance)
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(0.0);
    let max_edge_distance_over_scale = all_edges
        .iter()
        .map(|ce| ce.edge.distance_over_scale.unwrap_or(0.0))
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(0.0);

    let tv_sum = checked_fsum_finite(
        transitions
            .iter()
            .filter_map(|t| t.transition_tv_redistribution),
        "Chain transition_tv_redistribution sum",
    )?;
    let mean_transition_tv_redistribution = if ok_transition_count > 0 {
        checked_ratio(
            tv_sum,
            ok_transition_count as f64,
            "chain.mean_transition_tv_redistribution",
        )?
    } else {
        0.0
    };
    let max_transition_tv_redistribution = transitions
        .iter()
        .filter_map(|t| t.transition_tv_redistribution)
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(0.0);

    let ambiguous_transition_count = transitions
        .iter()
        .filter(|t| t.transition_status == ChainTransitionStatus::Ok && t.ambiguous_tie)
        .count();
    let high_uncertainty_transition_count = transitions
        .iter()
        .filter(|t| t.transition_status == ChainTransitionStatus::Ok && t.high_uncertainty)
        .count();
    let high_uncertainty_transition_ratio = if ok_transition_count > 0 {
        checked_ratio(
            high_uncertainty_transition_count as f64,
            ok_transition_count as f64,
            "chain.high_uncertainty_transition_ratio",
        )?
    } else {
        0.0
    };

    let total_edge_count = all_edges.len();
    let high_uncertainty_edge_count = all_edges
        .iter()
        .filter(|ce| ce.edge.high_uncertainty)
        .count();
    let high_uncertainty_edge_ratio = if total_edge_count > 0 {
        checked_ratio(
            high_uncertainty_edge_count as f64,
            total_edge_count as f64,
            "chain.high_uncertainty_edge_ratio",
        )?
    } else {
        0.0
    };

    let chain_summary = EvolutionChainSummary {
        ok_batch_count,
        missing_batch_count,
        failed_batch_count,
        ok_transition_count,
        gap_transition_count,
        branch_count,
        primary_edge_count,
        secondary_split_edge_count,
        secondary_merge_edge_count,
        continuation_count,
        split_count,
        merge_count,
        emergence_count,
        disappearance_count,
        gap_interrupted_count,
        gap_restarted_count,
        cumulative_primary_distance,
        max_edge_distance,
        max_edge_distance_over_scale,
        mean_transition_tv_redistribution,
        max_transition_tv_redistribution,
        ambiguous_transition_count,
        high_uncertainty_transition_count,
        high_uncertainty_transition_ratio,
        high_uncertainty_edge_count,
        high_uncertainty_edge_ratio,
    };

    Ok(EvolutionChainResult {
        algorithm_version: EVOLUTION_ALGORITHM_VERSION.to_string(),
        secondary_rule: input.config.secondary_rule.as_str().to_string(),
        flow_rule: input.config.flow_rule.as_str().to_string(),
        uncertainty_rule: UNCERTAINTY_RULE_V1.to_string(),
        non_physical_disclaimer: NON_PHYSICAL_DISCLAIMER.to_string(),
        run_key_prefix: prefix12,
        planned_batch_count,
        threshold_coefficient: norm_f64(input.threshold_coefficient),
        batches: resolved_batches,
        nodes,
        transitions,
        edges: all_edges,
        events: all_events,
        branches,
        chain_summary,
    })
}
