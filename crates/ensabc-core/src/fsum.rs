//! Shewchuk floating-point summation expansion and exact operations.
//!
//! Implements exact summation of binary64 (f64) values following Shewchuk (1997)
//! and CPython `math.fsum`. Maintains an expansion of non-overlapping partial sums
//! to guarantee zero intermediate roundoff error, rounding half-to-even only at the end.

use std::cmp::Ordering;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Expansion {
    partials: Vec<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsumOverflow;

impl Expansion {
    #[inline]
    pub fn new() -> Self {
        Self {
            partials: Vec::new(),
        }
    }

    pub fn from_values(values: impl IntoIterator<Item = f64>) -> Result<Self, FsumOverflow> {
        let mut e = Self::new();
        for v in values {
            e.push(v)?;
        }
        Ok(e)
    }

    /// Add a scalar value into the expansion using Priest/Shewchuk two-sum passes.
    pub fn push(&mut self, mut x: f64) -> Result<(), FsumOverflow> {
        if !x.is_finite() {
            return Err(FsumOverflow);
        }
        let mut i = 0;
        for j in 0..self.partials.len() {
            let mut y = self.partials[j];
            if x.abs() < y.abs() {
                std::mem::swap(&mut x, &mut y);
            }
            let hi = x + y;
            if !hi.is_finite() {
                return Err(FsumOverflow);
            }
            let lo = y - (hi - x);
            if lo != 0.0 {
                self.partials[i] = lo;
                i += 1;
            }
            x = hi;
        }
        self.partials.truncate(i);
        if x != 0.0 || self.partials.is_empty() {
            self.partials.push(x);
        }
        Ok(())
    }

    #[inline]
    pub fn plus(&self, x: f64) -> Result<Self, FsumOverflow> {
        let mut e = self.clone();
        e.push(x)?;
        Ok(e)
    }

    /// Correctly rounded (round-half-even) binary64 value of the exact real sum.
    pub fn round(&self) -> f64 {
        let p = &self.partials;
        let mut n = p.len();
        if n == 0 {
            return 0.0;
        }
        n -= 1;
        let mut hi = p[n];
        let mut lo = 0.0;
        while n > 0 {
            let x = hi;
            n -= 1;
            let y = p[n];
            hi = x + y;
            let yr = hi - x;
            lo = y - yr;
            if lo != 0.0 {
                break;
            }
        }
        if n > 0 && ((lo < 0.0 && p[n - 1] < 0.0) || (lo > 0.0 && p[n - 1] > 0.0)) {
            let y = lo * 2.0;
            let x = hi + y;
            let yr = x - hi;
            if y == yr {
                hi = x;
            }
        }
        if hi == 0.0 { 0.0 } else { hi }
    }

    /// Correctly rounded value of `(self - other)`.
    pub fn minus_rounded(&self, other: &Self) -> Result<f64, FsumOverflow> {
        let mut e = self.clone();
        for &v in &other.partials {
            e.push(-v)?;
        }
        Ok(e.round())
    }

    /// Exact sign comparison of two real sums.
    pub fn cmp_exact(&self, other: &Self) -> Ordering {
        // Difference of finite expansions
        match self.minus_rounded(other) {
            Ok(diff) => diff.partial_cmp(&0.0).unwrap_or(Ordering::Equal),
            Err(_) => {
                // In case of extreme range overflow, compare non-overlapping components top-down.
                let mut si = self.partials.len();
                let mut oi = other.partials.len();
                while si > 0 || oi > 0 {
                    let sv = if si > 0 {
                        si -= 1;
                        self.partials[si]
                    } else {
                        0.0
                    };
                    let ov = if oi > 0 {
                        oi -= 1;
                        other.partials[oi]
                    } else {
                        0.0
                    };
                    let sn = if sv == 0.0 { 0.0 } else { sv };
                    let on = if ov == 0.0 { 0.0 } else { ov };
                    match sn.total_cmp(&on) {
                        Ordering::Equal => continue,
                        ord => return ord,
                    }
                }
                Ordering::Equal
            }
        }
    }
}

/// Compute exact floating point sum and round half-to-even.
pub fn fsum(values: impl IntoIterator<Item = f64>) -> Result<f64, FsumOverflow> {
    Ok(Expansion::from_values(values)?.round())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MatchInput, MemberInput, ScenarioNodeInput, ensemble_dispersion, match_scenarios};

    #[test]
    fn test_shewchuk_cancellation() {
        let values = vec![1e16, 1.0, -1e16];
        let naive: f64 = values.iter().copied().sum();
        assert_eq!(naive, 0.0); // naive float sum loses 1.0 completely
        let exact = fsum(values).unwrap();
        assert_eq!(exact, 1.0);
    }

    #[test]
    fn test_round_half_even() {
        let ulp = 2.0_f64.powi(-52);
        let half_ulp = 2.0_f64.powi(-53);
        // At exact half-ulp: 1.0 has even LSB, so it rounds to 1.0
        let val1 = fsum(vec![1.0, half_ulp]).unwrap();
        assert_eq!(val1, 1.0);
        // 1.0 + ulp has odd LSB, so +half_ulp rounds to even (1.0 + 2*ulp)
        let val2 = fsum(vec![1.0 + ulp, half_ulp]).unwrap();
        assert_eq!(val2, 1.0 + 2.0 * ulp);
    }

    #[test]
    fn test_flat_vs_nested_fsum_difference() {
        // With N=6 and weights=[2.0, 1.0], ulp(2.0)=2^-51 so r=0.49*2^-51 rounds away in each
        // member's nested fsum(2.0, r) -> 2.0 (giving s_nested=0.816496580927726), whereas a
        // single flat expansion accumulates 6*r across members (giving s_flat=0.8164965809277261).
        let weights = vec![2.0, 1.0];
        let d_small = (0.49_f64 * 2.0_f64.powi(-51)).sqrt();
        let members6: Vec<MemberInput> = (0..6)
            .map(|i| {
                let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
                MemberInput {
                    member_index: i,
                    member_id: format!("m{i}"),
                    features: vec![sign * 1.0, sign * d_small],
                }
            })
            .collect();

        let nested_per_member: Vec<f64> = members6
            .iter()
            .map(|m| {
                fsum([
                    weights[0] * m.features[0].powi(2),
                    weights[1] * m.features[1].powi(2),
                ])
                .unwrap()
            })
            .collect();
        let q_nested = fsum(nested_per_member).unwrap();
        let s_nested = ((q_nested / 6.0) / 3.0).sqrt();

        let mut flat_exp = Expansion::new();
        for m in &members6 {
            flat_exp.push(weights[0] * m.features[0].powi(2)).unwrap();
            flat_exp.push(weights[1] * m.features[1].powi(2)).unwrap();
        }
        let q_flat = flat_exp.round();
        let s_flat = ((q_flat / 6.0) / 3.0).sqrt();

        assert_ne!(
            q_flat.to_bits(),
            q_nested.to_bits(),
            "flat and nested Q must differ"
        );
        assert_ne!(
            s_flat.to_bits(),
            s_nested.to_bits(),
            "flat and nested dispersion must differ"
        );

        let actual = ensemble_dispersion(&members6, &weights).unwrap();
        assert_eq!(actual.to_bits(), s_flat.to_bits());
    }

    #[test]
    fn test_weight_scaling_ulp_boundary() {
        // Real match_scenarios call showing exact 2^k scaling is bit-exact while general
        // positive weight scaling can shift d by 1 ulp and flip a strict d <= tau boundary.
        let a = vec![280.1, 281.7, 279.3];
        let b = vec![281.4, 280.2, 280.9];
        let base_weights = vec![0.7409511253549591, 0.6715589548470183, 0.5956993044924334];
        let run_pair = |weights: Vec<f64>, threshold: f64| {
            match_scenarios(&MatchInput {
                prev_scenarios: vec![ScenarioNodeInput {
                    cluster_id: 0,
                    mean_features: a.clone(),
                    member_count: 1,
                    member_fraction: 1.0,
                }],
                curr_scenarios: vec![ScenarioNodeInput {
                    cluster_id: 0,
                    mean_features: b.clone(),
                    member_count: 1,
                    member_fraction: 1.0,
                }],
                weights,
                threshold,
            })
            .unwrap()
        };

        let d_base = run_pair(base_weights.clone(), 10.0).distance_matrix[0][0];

        // Exact power-of-2 scaling must preserve distance bit-for-bit
        let pow2_weights: Vec<f64> = base_weights.iter().map(|w| w * 1024.0).collect();
        let pow2_res = run_pair(pow2_weights, d_base);
        assert_eq!(pow2_res.distance_matrix[0][0].to_bits(), d_base.to_bits());
        assert_eq!(pow2_res.objective.matched_count, 1);

        // Find a general positive scaling factor alpha where d_scaled != d_base (1 ulp shift),
        // then set tau = min(d_base, d_scaled) to verify strict d <= tau flips at the boundary.
        let mut observed_flip = false;
        for k in 1..200 {
            let alpha = (k as f64) * 0.3 + 0.1;
            let scaled_weights: Vec<f64> = base_weights.iter().map(|w| w * alpha).collect();
            let d_scaled = run_pair(scaled_weights.clone(), 10.0).distance_matrix[0][0];
            if d_scaled != d_base {
                let tau = d_base.min(d_scaled);
                let res_base = run_pair(base_weights.clone(), tau);
                let res_scaled = run_pair(scaled_weights, tau);
                assert_ne!(
                    res_base.objective.matched_count,
                    res_scaled.objective.matched_count
                );
                assert!((d_scaled - d_base).abs() / d_base < 1e-15);
                observed_flip = true;
                break;
            }
        }
        assert!(
            observed_flip,
            "expected to observe a 1-ulp decision flip under general scaling"
        );
    }
}
