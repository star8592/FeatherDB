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
