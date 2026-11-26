//! Thresholded lexicographic scenario matching core (bitmask DP).
//!
//! Maximizes link count first, then minimizes exact sum of weighted RMSE distances.
//! Pure Rust without external solvers or floating point quantization discrepancies.

use crate::fsum::Expansion;
use crate::{CoreError, CoreErrorCode};
use std::cmp::Ordering;

pub const MATCH_ALGORITHM_VERSION: &str = "m2.match.v1";
pub const MAX_SCENARIOS_PER_SIDE: usize = 12;
pub const MAX_SCENARIOS: usize = MAX_SCENARIOS_PER_SIDE;

#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioNodeInput {
    pub cluster_id: usize,
    pub mean_features: Vec<f64>,
    pub member_count: usize,
    pub member_fraction: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchInput {
    pub prev_scenarios: Vec<ScenarioNodeInput>,
    pub curr_scenarios: Vec<ScenarioNodeInput>,
    pub weights: Vec<f64>,
    pub threshold: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScaledMatchInput {
    pub prev_scenarios: Vec<ScenarioNodeInput>,
    pub curr_scenarios: Vec<ScenarioNodeInput>,
    pub weights: Vec<f64>,
    pub threshold_coefficient: f64,
    pub prev_dispersion: f64,
    pub curr_dispersion: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Substitutability {
    Tied,
    SameCount {
        gap: f64,
        alt_objective: f64,
    },
    CountDrop {
        alt_count: usize,
        alt_objective: f64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchedLink {
    pub prev_cluster_id: usize,
    pub curr_cluster_id: usize,
    pub distance_k: f64,
    pub distance_ratio: Option<f64>,
    pub substitutability: Substitutability,
    pub ambiguous: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnmatchedScenario {
    pub cluster_id: usize,
    pub reason: String,
    pub matched_in_some_optimum: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchObjective {
    pub matched_count: usize,
    pub weighted_distance_sum: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchResult {
    pub objective: MatchObjective,
    pub links: Vec<MatchedLink>,
    pub unmatched_prev: Vec<UnmatchedScenario>,
    pub unmatched_curr: Vec<UnmatchedScenario>,
    pub distance_matrix: Vec<Vec<f64>>,
    pub pair_scale: Option<f64>,
    pub threshold_coefficient: Option<f64>,
    pub prev_dispersion: Option<f64>,
    pub curr_dispersion: Option<f64>,
    pub optima_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct DpValue {
    pub(crate) count: usize,
    pub(crate) sum: Expansion,
}

pub(crate) fn cmp_dp_val(a: &DpValue, b: &DpValue) -> Ordering {
    // Greater count is better (Less in our ordering)
    b.count.cmp(&a.count).then_with(|| a.sum.cmp_exact(&b.sum))
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Constraint {
    pub(crate) forbid: Option<(usize, usize)>, // (canonical_row, canonical_col)
    pub(crate) force_row: Option<usize>,
    pub(crate) force_col: Option<usize>,
}

pub(crate) struct CanonContext<'a> {
    pub(crate) m: usize,
    pub(crate) n: usize,
    pub(crate) dist: &'a [Vec<f64>],
    pub(crate) tau: f64,
    pub(crate) rows: Vec<usize>, // canonical row -> original prev index
    pub(crate) cols: Vec<usize>, // canonical col -> original cur index
}

impl CanonContext<'_> {
    pub(crate) fn d(&self, r: usize, c: usize) -> f64 {
        self.dist[self.rows[r]][self.cols[c]]
    }

    pub(crate) fn feasible(&self, r: usize, c: usize, k: &Constraint) -> bool {
        self.d(r, c) <= self.tau && k.forbid != Some((r, c))
    }
}

pub(crate) struct SolvedMatching {
    pub(crate) val: DpValue,
    pub(crate) optima_count: usize,
    pub(crate) pairs: Vec<(usize, usize)>, // canonical (row, col)
}

pub(crate) fn solve_dp(
    ctx: &CanonContext,
    constraint: &Constraint,
) -> Result<Option<SolvedMatching>, CoreError> {
    let (m, n) = (ctx.m, ctx.n);
    let full = 1usize << n;
    let idx = |i: usize, mask: usize| i * full + mask;
    let mut g: Vec<Option<DpValue>> = vec![None; (m + 1) * full];
    let mut cnt: Vec<usize> = vec![0; (m + 1) * full];

    for mask in 0..full {
        if let Some(fc) = constraint.force_col {
            if (mask & (1 << fc)) == 0 {
                continue;
            }
        }
        g[idx(m, mask)] = Some(DpValue {
            count: 0,
            sum: Expansion::new(),
        });
        cnt[idx(m, mask)] = 1;
    }

    for i in (0..m).rev() {
        for mask in 0..full {
            let mut best: Option<DpValue> = None;
            let mut best_cnt: usize = 0;

            let mut consider = |cand: DpValue, c: usize| match &best {
                None => {
                    best = Some(cand);
                    best_cnt = c;
                }
                Some(b) => match cmp_dp_val(&cand, b) {
                    Ordering::Less => {
                        best = Some(cand);
                        best_cnt = c;
                    }
                    Ordering::Equal => best_cnt = best_cnt.saturating_add(c),
                    Ordering::Greater => {}
                },
            };

            // Option A: row i not matched
            if constraint.force_row != Some(i) {
                if let Some(v) = &g[idx(i + 1, mask)] {
                    consider(v.clone(), cnt[idx(i + 1, mask)]);
                }
            }

            // Option B: row i matched to column c
            for c in 0..n {
                if mask & (1 << c) != 0 || !ctx.feasible(i, c, constraint) {
                    continue;
                }
                if let Some(v) = &g[idx(i + 1, mask | (1 << c))] {
                    let mut next_sum = v.sum.clone();
                    next_sum.push(ctx.d(i, c)).map_err(|_| {
                        CoreError::new(
                            CoreErrorCode::NumericalFailure,
                            "Distance sum overflow in DP",
                        )
                    })?;
                    consider(
                        DpValue {
                            count: v.count + 1,
                            sum: next_sum,
                        },
                        cnt[idx(i + 1, mask | (1 << c))],
                    );
                }
            }

            g[idx(i, mask)] = best;
            cnt[idx(i, mask)] = best_cnt;
        }
    }

    let Some(root) = g[idx(0, 0)].clone() else {
        return Ok(None);
    };

    // Canonical tie-breaking solution reconstruction
    let mut pairs = Vec::new();
    let mut mask = 0usize;
    for i in 0..m {
        let target = g[idx(i, mask)].as_ref().unwrap();
        let mut chosen = None;
        for c in 0..n {
            if mask & (1 << c) != 0 || !ctx.feasible(i, c, constraint) {
                continue;
            }
            if let Some(v) = &g[idx(i + 1, mask | (1 << c))] {
                let mut cand_sum = v.sum.clone();
                if cand_sum.push(ctx.d(i, c)).is_ok() {
                    let cand = DpValue {
                        count: v.count + 1,
                        sum: cand_sum,
                    };
                    if cmp_dp_val(&cand, target) == Ordering::Equal {
                        chosen = Some(c);
                        break;
                    }
                }
            }
        }
        match chosen {
            Some(c) => {
                pairs.push((i, c));
                mask |= 1 << c;
            }
            None => {
                let v = g[idx(i + 1, mask)]
                    .as_ref()
                    .expect("unmatched option must exist");
                debug_assert_eq!(cmp_dp_val(v, target), Ordering::Equal);
            }
        }
    }

    Ok(Some(SolvedMatching {
        val: root,
        optima_count: cnt[idx(0, 0)],
        pairs,
    }))
}

pub(crate) fn canonical_scenario_order(scenarios: &[ScenarioNodeInput]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..scenarios.len()).collect();
    let norm = |x: f64| if x == 0.0 { 0.0 } else { x };
    order.sort_by(|&a, &b| {
        let sc_a = &scenarios[a];
        let sc_b = &scenarios[b];
        sc_a.mean_features
            .iter()
            .zip(&sc_b.mean_features)
            .map(|(&x, &y)| norm(x).total_cmp(&norm(y)))
            .find(|&o| o != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
            .then_with(|| sc_a.cluster_id.cmp(&sc_b.cluster_id))
    });
    order
}

pub fn compute_weighted_rmse(
    a: &[f64],
    b: &[f64],
    weights: &[f64],
    w_sum: f64,
) -> Result<f64, CoreError> {
    let mut expansion = Expansion::new();
    for g in 0..weights.len() {
        let diff = a[g] - b[g];
        let term = weights[g] * (diff * diff);
        expansion.push(term).map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "RMSE squared term overflow",
            )
        })?;
    }
    let d_sum = expansion.round();
    let mean_sq = d_sum / w_sum;
    if !mean_sq.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::NumericalFailure,
            format!("Weighted RMSE variance overflowed: mean_sq = {mean_sq}"),
        ));
    }
    if mean_sq < 0.0 {
        return Ok(0.0);
    }
    let dist = mean_sq.sqrt();
    if !dist.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::NumericalFailure,
            format!("Weighted RMSE root is non-finite: {dist}"),
        ));
    }
    Ok(dist)
}

/// Core match scenarios solver with absolute threshold.
pub fn match_scenarios(input: &MatchInput) -> Result<MatchResult, CoreError> {
    if input.prev_scenarios.is_empty() || input.curr_scenarios.is_empty() {
        return Err(CoreError::new(
            CoreErrorCode::EmptyScenarios,
            "prev_scenarios and curr_scenarios must both be non-empty",
        ));
    }
    let m = input.prev_scenarios.len();
    let n = input.curr_scenarios.len();
    if m > MAX_SCENARIOS || n > MAX_SCENARIOS {
        return Err(CoreError::new(
            CoreErrorCode::TooManyScenarios,
            format!("Scenario count ({m}, {n}) exceeds maximum supported limit of {MAX_SCENARIOS}"),
        ));
    }

    // Check duplicate cluster IDs
    let mut prev_ids = std::collections::HashSet::with_capacity(m);
    for (i, sc) in input.prev_scenarios.iter().enumerate() {
        if !prev_ids.insert(sc.cluster_id) {
            return Err(CoreError::new(
                CoreErrorCode::DuplicateScenarioId,
                format!("Duplicate cluster_id {} in prev_scenarios", sc.cluster_id),
            )
            .with_prev_index(i));
        }
    }
    let mut curr_ids = std::collections::HashSet::with_capacity(n);
    for (j, sc) in input.curr_scenarios.iter().enumerate() {
        if !curr_ids.insert(sc.cluster_id) {
            return Err(CoreError::new(
                CoreErrorCode::DuplicateScenarioId,
                format!("Duplicate cluster_id {} in curr_scenarios", sc.cluster_id),
            )
            .with_curr_index(j));
        }
    }

    if !input.threshold.is_finite() || input.threshold < 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidThreshold,
            format!(
                "Threshold must be finite and >= 0.0, got {}",
                input.threshold
            ),
        ));
    }
    if input.weights.is_empty() {
        return Err(CoreError::new(
            CoreErrorCode::EmptyFeatures,
            "Spatial weights must not be empty",
        ));
    }
    let n_points = input.weights.len();

    let mut w_exp = Expansion::new();
    for (g, &w) in input.weights.iter().enumerate() {
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

    // Check ragged features within scenarios
    let first_prev_len = input.prev_scenarios[0].mean_features.len();
    for (i, sc) in input.prev_scenarios.iter().enumerate() {
        if sc.mean_features.is_empty() {
            return Err(CoreError::new(
                CoreErrorCode::EmptyFeatures,
                format!("prev_scenarios[{i}] mean_features is empty"),
            )
            .with_prev_index(i));
        }
        if sc.mean_features.len() != first_prev_len {
            return Err(CoreError::new(
                CoreErrorCode::RaggedFeatures,
                format!(
                    "prev_scenarios[{i}] features length {} != prev_scenarios[0] length {first_prev_len}",
                    sc.mean_features.len()
                ),
            )
            .with_prev_index(i));
        }
        for (g, &val) in sc.mean_features.iter().enumerate() {
            if !val.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NonFiniteInput,
                    format!("prev_scenarios[{i}] feature {g} is non-finite: {val}"),
                )
                .with_prev_index(i)
                .with_feature_index(g));
            }
        }
    }

    let first_curr_len = input.curr_scenarios[0].mean_features.len();
    for (j, sc) in input.curr_scenarios.iter().enumerate() {
        if sc.mean_features.is_empty() {
            return Err(CoreError::new(
                CoreErrorCode::EmptyFeatures,
                format!("curr_scenarios[{j}] mean_features is empty"),
            )
            .with_curr_index(j));
        }
        if sc.mean_features.len() != first_curr_len {
            return Err(CoreError::new(
                CoreErrorCode::RaggedFeatures,
                format!(
                    "curr_scenarios[{j}] features length {} != curr_scenarios[0] length {first_curr_len}",
                    sc.mean_features.len()
                ),
            )
            .with_curr_index(j));
        }
        for (g, &val) in sc.mean_features.iter().enumerate() {
            if !val.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NonFiniteInput,
                    format!("curr_scenarios[{j}] feature {g} is non-finite: {val}"),
                )
                .with_curr_index(j)
                .with_feature_index(g));
            }
        }
    }

    // Dimension mismatch with weights length
    if first_prev_len != n_points || first_curr_len != n_points {
        return Err(CoreError::new(
            CoreErrorCode::DimensionMismatch,
            format!(
                "Scenario features length ({first_prev_len}, {first_curr_len}) does not match weights length {n_points}"
            ),
        ));
    }

    // Compute pairwise distance matrix
    let mut dist_matrix = Vec::with_capacity(m);
    for p in &input.prev_scenarios {
        let mut row = Vec::with_capacity(n);
        for c in &input.curr_scenarios {
            let d =
                compute_weighted_rmse(&p.mean_features, &c.mean_features, &input.weights, w_sum)?;
            row.push(d);
        }
        dist_matrix.push(row);
    }

    let ctx = CanonContext {
        m,
        n,
        dist: &dist_matrix,
        tau: input.threshold,
        rows: canonical_scenario_order(&input.prev_scenarios),
        cols: canonical_scenario_order(&input.curr_scenarios),
    };

    let base = solve_dp(&ctx, &Constraint::default())?.expect("empty matching is always feasible");

    let mut links = Vec::with_capacity(base.pairs.len());
    for &(r, c) in &base.pairs {
        let alt = solve_dp(
            &ctx,
            &Constraint {
                forbid: Some((r, c)),
                ..Default::default()
            },
        )?
        .expect("empty matching is always feasible");

        let alt_obj = alt.val.sum.round();
        let substitutability = if alt.val.count < base.val.count {
            Substitutability::CountDrop {
                alt_count: alt.val.count,
                alt_objective: alt_obj,
            }
        } else {
            let gap = alt.val.sum.minus_rounded(&base.val.sum).map_err(|_| {
                CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "Substitutability gap overflow",
                )
            })?;
            if gap == 0.0 {
                Substitutability::Tied
            } else {
                Substitutability::SameCount {
                    gap,
                    alt_objective: alt_obj,
                }
            }
        };

        let is_ambiguous = cmp_dp_val(&alt.val, &base.val) == Ordering::Equal;
        let prev_orig = ctx.rows[r];
        let curr_orig = ctx.cols[c];

        links.push(MatchedLink {
            prev_cluster_id: input.prev_scenarios[prev_orig].cluster_id,
            curr_cluster_id: input.curr_scenarios[curr_orig].cluster_id,
            distance_k: ctx.d(r, c),
            distance_ratio: None,
            substitutability,
            ambiguous: is_ambiguous,
        });
    }

    links.sort_by_key(|l| l.prev_cluster_id);

    let matched_rows: Vec<usize> = base.pairs.iter().map(|x| x.0).collect();
    let matched_cols: Vec<usize> = base.pairs.iter().map(|x| x.1).collect();

    let mut unmatched_prev = Vec::new();
    for r in 0..m {
        if !matched_rows.contains(&r) {
            let orig_idx = ctx.rows[r];
            let has_cand = (0..n).any(|c| ctx.d(r, c) <= input.threshold);
            let reason = if has_cand {
                "not_selected_in_global_optimum"
            } else {
                "no_candidate_within_threshold"
            };
            unmatched_prev.push(UnmatchedScenario {
                cluster_id: input.prev_scenarios[orig_idx].cluster_id,
                reason: reason.to_string(),
                matched_in_some_optimum: solve_dp(
                    &ctx,
                    &Constraint {
                        force_row: Some(r),
                        ..Default::default()
                    },
                )?
                .is_some_and(|alt| cmp_dp_val(&alt.val, &base.val) == Ordering::Equal),
            });
        }
    }
    unmatched_prev.sort_by_key(|u| u.cluster_id);

    let mut unmatched_curr = Vec::new();
    for c in 0..n {
        if !matched_cols.contains(&c) {
            let orig_idx = ctx.cols[c];
            let has_cand = (0..m).any(|r| ctx.d(r, c) <= input.threshold);
            let reason = if has_cand {
                "not_selected_in_global_optimum"
            } else {
                "no_candidate_within_threshold"
            };
            unmatched_curr.push(UnmatchedScenario {
                cluster_id: input.curr_scenarios[orig_idx].cluster_id,
                reason: reason.to_string(),
                matched_in_some_optimum: solve_dp(
                    &ctx,
                    &Constraint {
                        force_col: Some(c),
                        ..Default::default()
                    },
                )?
                .is_some_and(|alt| cmp_dp_val(&alt.val, &base.val) == Ordering::Equal),
            });
        }
    }
    unmatched_curr.sort_by_key(|u| u.cluster_id);

    Ok(MatchResult {
        objective: MatchObjective {
            matched_count: base.val.count,
            weighted_distance_sum: base.val.sum.round(),
        },
        links,
        unmatched_prev,
        unmatched_curr,
        distance_matrix: dist_matrix,
        pair_scale: None,
        threshold_coefficient: None,
        prev_dispersion: None,
        curr_dispersion: None,
        optima_count: base.optima_count,
    })
}

/// Scaled match scenarios solver using dispersion scale S and threshold coefficient c.
pub fn match_scenarios_scaled(input: &ScaledMatchInput) -> Result<MatchResult, CoreError> {
    if !input.threshold_coefficient.is_finite() || input.threshold_coefficient < 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidThreshold,
            format!(
                "Threshold coefficient must be finite and >= 0.0, got {}",
                input.threshold_coefficient
            ),
        ));
    }
    if !input.prev_dispersion.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::InvalidThreshold,
            format!(
                "prev_dispersion must be finite, got {}",
                input.prev_dispersion
            ),
        ));
    }
    if input.prev_dispersion <= 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::ZeroDispersion,
            format!(
                "prev_dispersion must be > 0.0, got {}",
                input.prev_dispersion
            ),
        ));
    }
    if !input.curr_dispersion.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::InvalidThreshold,
            format!(
                "curr_dispersion must be finite, got {}",
                input.curr_dispersion
            ),
        ));
    }
    if input.curr_dispersion <= 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::ZeroDispersion,
            format!(
                "curr_dispersion must be > 0.0, got {}",
                input.curr_dispersion
            ),
        ));
    }

    let pair_scale = (input.prev_dispersion + input.curr_dispersion) / 2.0;
    if !pair_scale.is_finite() || pair_scale <= 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidThreshold,
            format!("Calculated pair scale S is non-positive or non-finite: {pair_scale}"),
        ));
    }

    let tau = input.threshold_coefficient * pair_scale;
    if !tau.is_finite() || tau < 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidThreshold,
            format!("Calculated threshold tau is non-finite or negative: {tau}"),
        ));
    }

    let base_input = MatchInput {
        prev_scenarios: input.prev_scenarios.clone(),
        curr_scenarios: input.curr_scenarios.clone(),
        weights: input.weights.clone(),
        threshold: tau,
    };

    let mut result = match_scenarios(&base_input)?;
    result.pair_scale = Some(pair_scale);
    result.threshold_coefficient = Some(input.threshold_coefficient);
    result.prev_dispersion = Some(input.prev_dispersion);
    result.curr_dispersion = Some(input.curr_dispersion);

    for link in &mut result.links {
        link.distance_ratio = Some(link.distance_k / pair_scale);
    }

    Ok(result)
}
