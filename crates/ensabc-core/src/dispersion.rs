//! Spatial weighted ensemble dispersion calculation.
//!
//! Implements M2 population dispersion formula:
//! $$Q = \operatorname{fsum}_{(n,g)\in\{0..N-1\}\times\{0..p-1\}}\bigl(w_g \otimes (e_{n,g} \otimes e_{n,g})\bigr), \quad s = \sqrt{(Q \oslash N) \oslash W}$$
//! where all (member, point) squared terms enter one single exact expansion.

use crate::fsum::{Expansion, fsum};
use crate::{CoreError, CoreErrorCode, MemberInput};

/// Compute spatial weighted ensemble dispersion s (K).
pub fn ensemble_dispersion(members: &[MemberInput], weights: &[f64]) -> Result<f64, CoreError> {
    if members.is_empty() {
        return Err(CoreError::new(
            CoreErrorCode::EmptyMembers,
            "Member list for ensemble dispersion must not be empty",
        ));
    }
    let n_members = members.len();

    if weights.is_empty() {
        return Err(CoreError::new(
            CoreErrorCode::InvalidWeights,
            "Spatial weights must not be empty",
        ));
    }
    let n_points = weights.len();

    let mut w_expansion = Expansion::new();
    for (g, &w) in weights.iter().enumerate() {
        if !w.is_finite() || w < 0.0 {
            return Err(CoreError::new(
                CoreErrorCode::InvalidWeights,
                format!("Weight at index {g} must be finite and >= 0.0, got {w}"),
            )
            .with_feature_index(g));
        }
        w_expansion.push(w).map_err(|_| {
            CoreError::new(CoreErrorCode::NumericalFailure, "Weight summation overflow")
        })?;
    }
    let w_sum = w_expansion.round();
    if w_sum <= 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::InvalidWeights,
            "Sum of spatial weights must be strictly positive",
        ));
    }

    // Semantic validation belongs in core for both native and SQL callers.
    let mut member_ids = std::collections::HashSet::with_capacity(n_members);
    for (m_idx, m) in members.iter().enumerate() {
        if m.member_id.is_empty() {
            return Err(
                CoreError::new(CoreErrorCode::EmptyMemberId, "member_id cannot be empty")
                    .with_member_index(m_idx),
            );
        }
        if !member_ids.insert(&m.member_id) {
            return Err(CoreError::new(
                CoreErrorCode::DuplicateMemberId,
                format!("duplicate member_id: {}", m.member_id),
            )
            .with_member_index(m_idx));
        }
        if m.features.len() != n_points {
            return Err(CoreError::new(
                CoreErrorCode::RaggedFeatures,
                format!(
                    "Member {} features length {} does not match weights length {}",
                    m.member_id,
                    m.features.len(),
                    n_points
                ),
            )
            .with_member_index(m_idx));
        }
        for (f_idx, &val) in m.features.iter().enumerate() {
            if !val.is_finite() {
                return Err(CoreError::new(
                    CoreErrorCode::NonFiniteInput,
                    format!("Non-finite feature at member {m_idx}, point {f_idx}: {val}"),
                )
                .with_member_index(m_idx)
                .with_feature_index(f_idx));
            }
        }
    }

    // 1. Compute ensemble mean field column by column using independent fsum
    let mut mean_field = Vec::with_capacity(n_points);
    for g in 0..n_points {
        let col_vals = members.iter().map(|m| m.features[g]);
        let col_sum = fsum(col_vals).map_err(|_| {
            CoreError::new(
                CoreErrorCode::NumericalFailure,
                "Mean field summation overflow",
            )
        })?;
        let col_mean = col_sum / (n_members as f64);
        mean_field.push(col_mean);
    }

    // 2. Accumulate all (n, g) squared deviation terms into ONE exact expansion Q
    let mut q_expansion = Expansion::new();
    for m in members.iter() {
        for g in 0..n_points {
            let dev = m.features[g] - mean_field[g];
            let term = weights[g] * (dev * dev);
            q_expansion.push(term).map_err(|_| {
                CoreError::new(
                    CoreErrorCode::NumericalFailure,
                    "Dispersion squared term summation overflow",
                )
            })?;
        }
    }

    let q_sum = q_expansion.round();
    let mean_sq = (q_sum / (n_members as f64)) / w_sum;
    if !mean_sq.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::NumericalFailure,
            format!("Ensemble dispersion variance overflowed: mean_sq = {mean_sq}"),
        ));
    }
    if mean_sq <= 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::ZeroDispersion,
            format!("Ensemble dispersion is zero: mean_sq = {mean_sq}"),
        ));
    }

    let dispersion = mean_sq.sqrt();
    if !dispersion.is_finite() {
        return Err(CoreError::new(
            CoreErrorCode::NumericalFailure,
            format!("Ensemble dispersion root is non-finite: {dispersion}"),
        ));
    }
    if dispersion <= 0.0 {
        return Err(CoreError::new(
            CoreErrorCode::ZeroDispersion,
            format!("Ensemble dispersion root is zero: {dispersion}"),
        ));
    }

    Ok(dispersion)
}
