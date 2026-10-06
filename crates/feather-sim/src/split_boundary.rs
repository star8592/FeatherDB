#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeLoadSample {
    pub token: u64,
    pub bytes: u64,
    pub heat: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitBoundaryStrategy {
    HashMidpoint,
    ByteMedian,
    HeatMedian,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SplitBoundaryDecision {
    pub strategy: SplitBoundaryStrategy,
    pub boundary: u128,
    pub left_bytes: u64,
    pub right_bytes: u64,
    pub left_heat: u64,
    pub right_heat: u64,
}

impl SplitBoundaryDecision {
    pub fn byte_imbalance_ppm(&self) -> u64 {
        imbalance_ppm(self.left_bytes, self.right_bytes)
    }

    pub fn heat_imbalance_ppm(&self) -> u64 {
        imbalance_ppm(self.left_heat, self.right_heat)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitBoundaryError {
    InvalidRange,
    SampleOutOfRange,
    EmptySamples,
    ZeroSignal,
    Overflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SplitBoundaryPolicy {
    pub byte_weight_ppm: u64,
    pub heat_weight_ppm: u64,
    pub max_byte_imbalance_ppm: u64,
    pub max_heat_imbalance_ppm: u64,
    pub min_telemetry_confidence_ppm: u64,
    pub min_score_improvement_ppm: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitPolicyReason {
    LowTelemetryConfidence,
    NoEligibleImprovement,
    DataAwareImprovement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SplitPolicyDecision {
    pub chosen: SplitBoundaryDecision,
    pub reason: SplitPolicyReason,
    pub midpoint_score_ppm: u64,
    pub chosen_score_ppm: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitPolicyError {
    InvalidWeight,
    InvalidLimit,
    Boundary(SplitBoundaryError),
}

impl From<SplitBoundaryError> for SplitPolicyError {
    fn from(value: SplitBoundaryError) -> Self {
        Self::Boundary(value)
    }
}

impl SplitBoundaryPolicy {
    pub fn validate(&self) -> Result<(), SplitPolicyError> {
        if self.byte_weight_ppm == 0 && self.heat_weight_ppm == 0 {
            return Err(SplitPolicyError::InvalidWeight);
        }

        for value in [
            self.byte_weight_ppm,
            self.heat_weight_ppm,
            self.max_byte_imbalance_ppm,
            self.max_heat_imbalance_ppm,
            self.min_telemetry_confidence_ppm,
            self.min_score_improvement_ppm,
        ] {
            if value > 1_000_000 {
                return Err(SplitPolicyError::InvalidLimit);
            }
        }

        Ok(())
    }
}

pub fn choose_split_boundary_with_policy(
    start: u128,
    end: u128,
    samples: &[RangeLoadSample],
    telemetry_confidence_ppm: u64,
    policy: &SplitBoundaryPolicy,
) -> Result<SplitPolicyDecision, SplitPolicyError> {
    policy.validate()?;
    if telemetry_confidence_ppm > 1_000_000 {
        return Err(SplitPolicyError::InvalidLimit);
    }

    let midpoint = choose_split_boundary(start, end, samples, SplitBoundaryStrategy::HashMidpoint)?;
    let midpoint_score = weighted_score_ppm(&midpoint, policy);

    if telemetry_confidence_ppm < policy.min_telemetry_confidence_ppm {
        return Ok(SplitPolicyDecision {
            chosen: midpoint,
            reason: SplitPolicyReason::LowTelemetryConfidence,
            midpoint_score_ppm: midpoint_score,
            chosen_score_ppm: midpoint_score,
        });
    }

    let mut best: Option<(SplitBoundaryDecision, u64)> = None;

    for strategy in [
        SplitBoundaryStrategy::ByteMedian,
        SplitBoundaryStrategy::HeatMedian,
    ] {
        let candidate = match choose_split_boundary(start, end, samples, strategy) {
            Ok(candidate) => candidate,
            Err(SplitBoundaryError::ZeroSignal) => continue,
            Err(error) => return Err(error.into()),
        };

        if candidate.byte_imbalance_ppm() > policy.max_byte_imbalance_ppm
            || candidate.heat_imbalance_ppm() > policy.max_heat_imbalance_ppm
        {
            continue;
        }

        let score = weighted_score_ppm(&candidate, policy);
        match best {
            None => best = Some((candidate, score)),
            Some((_, best_score)) if score < best_score => {
                best = Some((candidate, score));
            }
            _ => {}
        }
    }

    let Some((candidate, candidate_score)) = best else {
        return Ok(SplitPolicyDecision {
            chosen: midpoint,
            reason: SplitPolicyReason::NoEligibleImprovement,
            midpoint_score_ppm: midpoint_score,
            chosen_score_ppm: midpoint_score,
        });
    };

    let improvement = midpoint_score.saturating_sub(candidate_score);
    if candidate_score >= midpoint_score || improvement < policy.min_score_improvement_ppm {
        return Ok(SplitPolicyDecision {
            chosen: midpoint,
            reason: SplitPolicyReason::NoEligibleImprovement,
            midpoint_score_ppm: midpoint_score,
            chosen_score_ppm: midpoint_score,
        });
    }

    Ok(SplitPolicyDecision {
        chosen: candidate,
        reason: SplitPolicyReason::DataAwareImprovement,
        midpoint_score_ppm: midpoint_score,
        chosen_score_ppm: candidate_score,
    })
}

fn weighted_score_ppm(decision: &SplitBoundaryDecision, policy: &SplitBoundaryPolicy) -> u64 {
    let byte = decision.byte_imbalance_ppm() as u128;
    let heat = decision.heat_imbalance_ppm() as u128;
    let byte_weight = policy.byte_weight_ppm as u128;
    let heat_weight = policy.heat_weight_ppm as u128;
    let total_weight = byte_weight + heat_weight;

    (((byte * byte_weight) + (heat * heat_weight)) / total_weight) as u64
}

pub fn choose_split_boundary(
    start: u128,
    end: u128,
    samples: &[RangeLoadSample],
    strategy: SplitBoundaryStrategy,
) -> Result<SplitBoundaryDecision, SplitBoundaryError> {
    if start >= end || end > (1_u128 << 64) || end - start < 2 {
        return Err(SplitBoundaryError::InvalidRange);
    }
    if samples.is_empty() {
        return Err(SplitBoundaryError::EmptySamples);
    }

    for sample in samples {
        let token = sample.token as u128;
        if token < start || token >= end {
            return Err(SplitBoundaryError::SampleOutOfRange);
        }
    }

    let boundary = match strategy {
        SplitBoundaryStrategy::HashMidpoint => start + (end - start) / 2,
        SplitBoundaryStrategy::ByteMedian => {
            weighted_boundary(start, end, samples, |sample| sample.bytes)?
        }
        SplitBoundaryStrategy::HeatMedian => {
            weighted_boundary(start, end, samples, |sample| sample.heat)?
        }
    };

    summarize(start, end, boundary, samples, strategy)
}

fn weighted_boundary<F>(
    start: u128,
    end: u128,
    samples: &[RangeLoadSample],
    weight: F,
) -> Result<u128, SplitBoundaryError>
where
    F: Fn(&RangeLoadSample) -> u64,
{
    let mut ordered = samples.to_vec();
    ordered.sort_by_key(|sample| sample.token);

    let total = ordered.iter().try_fold(0_u128, |acc, sample| {
        acc.checked_add(weight(sample) as u128)
            .ok_or(SplitBoundaryError::Overflow)
    })?;
    if total == 0 {
        return Err(SplitBoundaryError::ZeroSignal);
    }

    let target = total.div_ceil(2);
    let mut cumulative = 0_u128;

    for sample in &ordered {
        cumulative = cumulative
            .checked_add(weight(sample) as u128)
            .ok_or(SplitBoundaryError::Overflow)?;

        if cumulative >= target {
            let raw = (sample.token as u128)
                .checked_add(1)
                .ok_or(SplitBoundaryError::Overflow)?;
            return Ok(raw.clamp(start + 1, end - 1));
        }
    }

    Err(SplitBoundaryError::ZeroSignal)
}

fn summarize(
    start: u128,
    end: u128,
    boundary: u128,
    samples: &[RangeLoadSample],
    strategy: SplitBoundaryStrategy,
) -> Result<SplitBoundaryDecision, SplitBoundaryError> {
    if boundary <= start || boundary >= end {
        return Err(SplitBoundaryError::InvalidRange);
    }

    let mut left_bytes = 0_u64;
    let mut right_bytes = 0_u64;
    let mut left_heat = 0_u64;
    let mut right_heat = 0_u64;

    for sample in samples {
        if (sample.token as u128) < boundary {
            left_bytes = left_bytes
                .checked_add(sample.bytes)
                .ok_or(SplitBoundaryError::Overflow)?;
            left_heat = left_heat
                .checked_add(sample.heat)
                .ok_or(SplitBoundaryError::Overflow)?;
        } else {
            right_bytes = right_bytes
                .checked_add(sample.bytes)
                .ok_or(SplitBoundaryError::Overflow)?;
            right_heat = right_heat
                .checked_add(sample.heat)
                .ok_or(SplitBoundaryError::Overflow)?;
        }
    }

    Ok(SplitBoundaryDecision {
        strategy,
        boundary,
        left_bytes,
        right_bytes,
        left_heat,
        right_heat,
    })
}

fn imbalance_ppm(left: u64, right: u64) -> u64 {
    let total = left as u128 + right as u128;
    if total == 0 {
        return 0;
    }
    let diff = left.abs_diff(right) as u128;
    ((diff * 1_000_000) / total) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skewed_samples() -> Vec<RangeLoadSample> {
        vec![
            RangeLoadSample {
                token: 10,
                bytes: 400,
                heat: 10,
            },
            RangeLoadSample {
                token: 20,
                bytes: 350,
                heat: 10,
            },
            RangeLoadSample {
                token: 30,
                bytes: 150,
                heat: 10,
            },
            RangeLoadSample {
                token: (u64::MAX / 5) * 3,
                bytes: 34,
                heat: 240,
            },
            RangeLoadSample {
                token: (u64::MAX / 10) * 7,
                bytes: 33,
                heat: 260,
            },
            RangeLoadSample {
                token: (u64::MAX / 10) * 9,
                bytes: 33,
                heat: 250,
            },
        ]
    }

    #[test]
    fn midpoint_can_be_badly_byte_skewed() {
        let samples = skewed_samples();
        let decision = choose_split_boundary(
            0,
            1_u128 << 64,
            &samples,
            SplitBoundaryStrategy::HashMidpoint,
        )
        .unwrap();

        assert_eq!(decision.left_bytes, 900);
        assert_eq!(decision.right_bytes, 100);
        assert_eq!(decision.byte_imbalance_ppm(), 800_000);
    }

    #[test]
    fn byte_median_improves_byte_balance_on_skewed_distribution() {
        let samples = skewed_samples();
        let midpoint = choose_split_boundary(
            0,
            1_u128 << 64,
            &samples,
            SplitBoundaryStrategy::HashMidpoint,
        )
        .unwrap();
        let byte =
            choose_split_boundary(0, 1_u128 << 64, &samples, SplitBoundaryStrategy::ByteMedian)
                .unwrap();

        assert!(byte.byte_imbalance_ppm() < midpoint.byte_imbalance_ppm());
    }

    #[test]
    fn heat_median_improves_heat_balance_on_skewed_distribution() {
        let samples = skewed_samples();
        let midpoint = choose_split_boundary(
            0,
            1_u128 << 64,
            &samples,
            SplitBoundaryStrategy::HashMidpoint,
        )
        .unwrap();
        let heat =
            choose_split_boundary(0, 1_u128 << 64, &samples, SplitBoundaryStrategy::HeatMedian)
                .unwrap();

        assert!(heat.heat_imbalance_ppm() < midpoint.heat_imbalance_ppm());
    }

    #[test]
    fn low_telemetry_confidence_forces_midpoint_fallback() {
        let samples = skewed_samples();
        let policy = SplitBoundaryPolicy {
            byte_weight_ppm: 500_000,
            heat_weight_ppm: 500_000,
            max_byte_imbalance_ppm: 1_000_000,
            max_heat_imbalance_ppm: 1_000_000,
            min_telemetry_confidence_ppm: 800_000,
            min_score_improvement_ppm: 10_000,
        };

        let decision =
            choose_split_boundary_with_policy(0, 1_u128 << 64, &samples, 799_999, &policy).unwrap();

        assert_eq!(
            decision.chosen.strategy,
            SplitBoundaryStrategy::HashMidpoint
        );
        assert_eq!(decision.reason, SplitPolicyReason::LowTelemetryConfidence);
    }

    #[test]
    fn storage_focused_policy_selects_byte_median() {
        let samples = skewed_samples();
        let policy = SplitBoundaryPolicy {
            byte_weight_ppm: 900_000,
            heat_weight_ppm: 100_000,
            max_byte_imbalance_ppm: 600_000,
            max_heat_imbalance_ppm: 1_000_000,
            min_telemetry_confidence_ppm: 700_000,
            min_score_improvement_ppm: 10_000,
        };

        let decision =
            choose_split_boundary_with_policy(0, 1_u128 << 64, &samples, 950_000, &policy).unwrap();

        assert_eq!(decision.chosen.strategy, SplitBoundaryStrategy::ByteMedian);
        assert_eq!(decision.reason, SplitPolicyReason::DataAwareImprovement);
        assert!(decision.chosen_score_ppm < decision.midpoint_score_ppm);
    }

    #[test]
    fn heat_focused_policy_selects_heat_median() {
        let samples = skewed_samples();
        let policy = SplitBoundaryPolicy {
            byte_weight_ppm: 100_000,
            heat_weight_ppm: 900_000,
            max_byte_imbalance_ppm: 950_000,
            max_heat_imbalance_ppm: 500_000,
            min_telemetry_confidence_ppm: 700_000,
            min_score_improvement_ppm: 10_000,
        };

        let decision =
            choose_split_boundary_with_policy(0, 1_u128 << 64, &samples, 950_000, &policy).unwrap();

        assert_eq!(decision.chosen.strategy, SplitBoundaryStrategy::HeatMedian);
        assert_eq!(decision.reason, SplitPolicyReason::DataAwareImprovement);
    }

    #[test]
    fn hard_limits_can_force_midpoint_even_when_data_aware_scores_better() {
        let samples = skewed_samples();
        let policy = SplitBoundaryPolicy {
            byte_weight_ppm: 500_000,
            heat_weight_ppm: 500_000,
            max_byte_imbalance_ppm: 700_000,
            max_heat_imbalance_ppm: 700_000,
            min_telemetry_confidence_ppm: 700_000,
            min_score_improvement_ppm: 1,
        };

        let decision =
            choose_split_boundary_with_policy(0, 1_u128 << 64, &samples, 1_000_000, &policy)
                .unwrap();

        assert_eq!(
            decision.chosen.strategy,
            SplitBoundaryStrategy::HashMidpoint
        );
        assert_eq!(decision.reason, SplitPolicyReason::NoEligibleImprovement);
    }

    #[test]
    fn policy_rejects_all_zero_objective_weights() {
        let policy = SplitBoundaryPolicy {
            byte_weight_ppm: 0,
            heat_weight_ppm: 0,
            max_byte_imbalance_ppm: 1_000_000,
            max_heat_imbalance_ppm: 1_000_000,
            min_telemetry_confidence_ppm: 0,
            min_score_improvement_ppm: 0,
        };

        assert_eq!(policy.validate(), Err(SplitPolicyError::InvalidWeight));
    }

    #[test]
    fn same_token_hotspot_remains_unsplittable() {
        let token = 42_u64;
        let samples = vec![
            RangeLoadSample {
                token,
                bytes: 1,
                heat: 500,
            },
            RangeLoadSample {
                token,
                bytes: 1,
                heat: 500,
            },
        ];

        let heat =
            choose_split_boundary(0, 1_u128 << 64, &samples, SplitBoundaryStrategy::HeatMedian)
                .unwrap();

        assert_eq!(heat.boundary, token as u128 + 1);
        assert_eq!(heat.left_heat, 1_000);
        assert_eq!(heat.right_heat, 0);
        assert_eq!(heat.heat_imbalance_ppm(), 1_000_000);
    }

    #[test]
    fn zero_signal_is_rejected_for_signal_median() {
        let samples = vec![RangeLoadSample {
            token: 1,
            bytes: 0,
            heat: 0,
        }];

        assert_eq!(
            choose_split_boundary(0, 100, &samples, SplitBoundaryStrategy::ByteMedian),
            Err(SplitBoundaryError::ZeroSignal)
        );
    }

    #[test]
    fn boundary_is_always_inside_range() {
        let samples = vec![
            RangeLoadSample {
                token: 0,
                bytes: 10,
                heat: 10,
            },
            RangeLoadSample {
                token: 9,
                bytes: 1,
                heat: 1,
            },
        ];

        for strategy in [
            SplitBoundaryStrategy::HashMidpoint,
            SplitBoundaryStrategy::ByteMedian,
            SplitBoundaryStrategy::HeatMedian,
        ] {
            let decision = choose_split_boundary(0, 10, &samples, strategy).unwrap();
            assert!(decision.boundary > 0);
            assert!(decision.boundary < 10);
        }
    }
}
