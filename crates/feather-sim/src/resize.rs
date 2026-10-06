#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResizeKind {
    Split,
    Merge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResizeBlockReason {
    Cooldown { remaining_ticks: u64 },
    MinTabletCount,
    MaxTabletCount,
    MetadataBudget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResizePlan {
    pub kind: ResizeKind,
    pub topology_epoch: u64,
    pub from_generation: u64,
    pub from_count: u64,
    pub to_count: u64,
    pub planned_at_tick: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResizeDecision {
    NoChange,
    Blocked {
        kind: ResizeKind,
        reason: ResizeBlockReason,
    },
    Planned(ResizePlan),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResizeCommitOutcome {
    Applied,
    AlreadyApplied,
    StaleTopology,
    StaleGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResizeConfigError {
    ZeroTargetBytes,
    InvalidRatio,
    InvalidHysteresis,
    InvalidTabletBounds,
    ZeroMetadataCost,
    ZeroMetadataBudget,
    MetadataBudgetBelowMinimum,
    InvalidInitialTabletCount,
}

#[derive(Clone, Copy, Debug)]
pub struct TabletResizePolicy {
    pub target_tablet_bytes: u64,
    pub split_above_num: u64,
    pub split_above_den: u64,
    pub merge_below_num: u64,
    pub merge_below_den: u64,
    pub cooldown_ticks: u64,
    pub min_tablets: u64,
    pub max_tablets: u64,
    pub metadata_bytes_per_tablet: u64,
    pub metadata_budget_bytes: u64,
}

impl TabletResizePolicy {
    pub fn research_hysteresis(
        target_tablet_bytes: u64,
        cooldown_ticks: u64,
        metadata_bytes_per_tablet: u64,
        metadata_budget_bytes: u64,
    ) -> Result<Self, ResizeConfigError> {
        if metadata_bytes_per_tablet == 0 {
            return Err(ResizeConfigError::ZeroMetadataCost);
        }
        if metadata_budget_bytes == 0 {
            return Err(ResizeConfigError::ZeroMetadataBudget);
        }

        let max_tablets = metadata_budget_bytes / metadata_bytes_per_tablet;
        let policy = Self {
            target_tablet_bytes,
            split_above_num: 2,
            split_above_den: 1,
            merge_below_num: 1,
            merge_below_den: 2,
            cooldown_ticks,
            min_tablets: 1,
            max_tablets,
            metadata_bytes_per_tablet,
            metadata_budget_bytes,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), ResizeConfigError> {
        if self.target_tablet_bytes == 0 {
            return Err(ResizeConfigError::ZeroTargetBytes);
        }
        if self.split_above_num == 0
            || self.split_above_den == 0
            || self.merge_below_num == 0
            || self.merge_below_den == 0
        {
            return Err(ResizeConfigError::InvalidRatio);
        }

        let split_left = self.split_above_num as u128 * self.merge_below_den as u128;
        let merge_left = self.merge_below_num as u128 * self.split_above_den as u128;
        if self.split_above_num <= self.split_above_den
            || self.merge_below_num >= self.merge_below_den
            || merge_left >= split_left
        {
            return Err(ResizeConfigError::InvalidHysteresis);
        }

        if self.min_tablets == 0 || self.max_tablets < self.min_tablets {
            return Err(ResizeConfigError::InvalidTabletBounds);
        }
        if self.metadata_bytes_per_tablet == 0 {
            return Err(ResizeConfigError::ZeroMetadataCost);
        }
        if self.metadata_budget_bytes == 0 {
            return Err(ResizeConfigError::ZeroMetadataBudget);
        }
        if (self.min_tablets as u128) * (self.metadata_bytes_per_tablet as u128)
            > self.metadata_budget_bytes as u128
        {
            return Err(ResizeConfigError::MetadataBudgetBelowMinimum);
        }

        Ok(())
    }

    pub fn max_tablets_by_metadata(&self) -> u64 {
        self.metadata_budget_bytes / self.metadata_bytes_per_tablet
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TabletResizeState {
    pub generation: u64,
    pub tablet_count: u64,
    pub last_resize_tick: Option<u64>,
}

impl TabletResizeState {
    pub fn new(tablet_count: u64) -> Result<Self, ResizeConfigError> {
        if tablet_count == 0 || !tablet_count.is_power_of_two() {
            return Err(ResizeConfigError::InvalidInitialTabletCount);
        }

        Ok(Self {
            generation: 0,
            tablet_count,
            last_resize_tick: None,
        })
    }

    pub fn estimated_metadata_bytes(&self, policy: &TabletResizePolicy) -> u64 {
        self.tablet_count
            .saturating_mul(policy.metadata_bytes_per_tablet)
    }

    pub fn evaluate(
        &self,
        total_table_bytes: u64,
        now_tick: u64,
        topology_epoch: u64,
        policy: &TabletResizePolicy,
    ) -> Result<ResizeDecision, ResizeConfigError> {
        policy.validate()?;

        let kind = if ratio_exceeded(
            total_table_bytes,
            self.tablet_count,
            policy.target_tablet_bytes,
            policy.split_above_num,
            policy.split_above_den,
        ) {
            Some(ResizeKind::Split)
        } else if ratio_below(
            total_table_bytes,
            self.tablet_count,
            policy.target_tablet_bytes,
            policy.merge_below_num,
            policy.merge_below_den,
        ) {
            Some(ResizeKind::Merge)
        } else {
            None
        };

        let Some(kind) = kind else {
            return Ok(ResizeDecision::NoChange);
        };

        if let Some(last_tick) = self.last_resize_tick {
            let cooldown_until = last_tick.saturating_add(policy.cooldown_ticks);
            if now_tick < cooldown_until {
                return Ok(ResizeDecision::Blocked {
                    kind,
                    reason: ResizeBlockReason::Cooldown {
                        remaining_ticks: cooldown_until - now_tick,
                    },
                });
            }
        }

        let to_count = match kind {
            ResizeKind::Split => {
                let Some(next) = self.tablet_count.checked_mul(2) else {
                    return Ok(ResizeDecision::Blocked {
                        kind,
                        reason: ResizeBlockReason::MaxTabletCount,
                    });
                };

                let metadata_bytes = (next as u128) * (policy.metadata_bytes_per_tablet as u128);
                if metadata_bytes > policy.metadata_budget_bytes as u128 {
                    return Ok(ResizeDecision::Blocked {
                        kind,
                        reason: ResizeBlockReason::MetadataBudget,
                    });
                }

                if next > policy.max_tablets {
                    return Ok(ResizeDecision::Blocked {
                        kind,
                        reason: ResizeBlockReason::MaxTabletCount,
                    });
                }

                next
            }
            ResizeKind::Merge => {
                let next = self.tablet_count / 2;
                if next < policy.min_tablets || next == 0 {
                    return Ok(ResizeDecision::Blocked {
                        kind,
                        reason: ResizeBlockReason::MinTabletCount,
                    });
                }
                next
            }
        };

        Ok(ResizeDecision::Planned(ResizePlan {
            kind,
            topology_epoch,
            from_generation: self.generation,
            from_count: self.tablet_count,
            to_count,
            planned_at_tick: now_tick,
        }))
    }

    pub fn commit(
        &mut self,
        plan: ResizePlan,
        current_topology_epoch: u64,
        now_tick: u64,
    ) -> ResizeCommitOutcome {
        if current_topology_epoch != plan.topology_epoch {
            return ResizeCommitOutcome::StaleTopology;
        }

        if self.generation == plan.from_generation + 1 && self.tablet_count == plan.to_count {
            return ResizeCommitOutcome::AlreadyApplied;
        }

        if self.generation != plan.from_generation || self.tablet_count != plan.from_count {
            return ResizeCommitOutcome::StaleGeneration;
        }

        self.tablet_count = plan.to_count;
        self.generation += 1;
        self.last_resize_tick = Some(now_tick);
        ResizeCommitOutcome::Applied
    }
}

fn ratio_exceeded(
    total_bytes: u64,
    tablet_count: u64,
    target_bytes: u64,
    numerator: u64,
    denominator: u64,
) -> bool {
    (total_bytes as u128) * (denominator as u128)
        > (target_bytes as u128) * (tablet_count as u128) * (numerator as u128)
}

fn ratio_below(
    total_bytes: u64,
    tablet_count: u64,
    target_bytes: u64,
    numerator: u64,
    denominator: u64,
) -> bool {
    (total_bytes as u128) * (denominator as u128)
        < (target_bytes as u128) * (tablet_count as u128) * (numerator as u128)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> TabletResizePolicy {
        TabletResizePolicy {
            target_tablet_bytes: 100,
            split_above_num: 2,
            split_above_den: 1,
            merge_below_num: 1,
            merge_below_den: 2,
            cooldown_ticks: 10,
            min_tablets: 1,
            max_tablets: 64,
            metadata_bytes_per_tablet: 16,
            metadata_budget_bytes: 64 * 16,
        }
    }

    #[test]
    fn invalid_hysteresis_is_rejected() {
        let mut invalid = policy();
        invalid.split_above_num = 1;
        invalid.split_above_den = 1;
        assert_eq!(
            invalid.validate(),
            Err(ResizeConfigError::InvalidHysteresis)
        );

        let mut invalid = policy();
        invalid.merge_below_num = 1;
        invalid.merge_below_den = 1;
        assert_eq!(
            invalid.validate(),
            Err(ResizeConfigError::InvalidHysteresis)
        );
    }

    #[test]
    fn split_and_merge_use_strict_hysteresis_boundaries() {
        let state = TabletResizeState::new(8).unwrap();

        assert_eq!(
            state.evaluate(1_600, 0, 1, &policy()).unwrap(),
            ResizeDecision::NoChange
        );

        let split = state.evaluate(1_601, 0, 1, &policy()).unwrap();
        assert_eq!(
            split,
            ResizeDecision::Planned(ResizePlan {
                kind: ResizeKind::Split,
                topology_epoch: 1,
                from_generation: 0,
                from_count: 8,
                to_count: 16,
                planned_at_tick: 0,
            })
        );

        assert_eq!(
            state.evaluate(400, 0, 1, &policy()).unwrap(),
            ResizeDecision::NoChange
        );

        let merge = state.evaluate(399, 0, 1, &policy()).unwrap();
        assert_eq!(
            merge,
            ResizeDecision::Planned(ResizePlan {
                kind: ResizeKind::Merge,
                topology_epoch: 1,
                from_generation: 0,
                from_count: 8,
                to_count: 4,
                planned_at_tick: 0,
            })
        );
    }

    #[test]
    fn cooldown_blocks_immediate_reverse_resize() {
        let mut state = TabletResizeState::new(8).unwrap();
        let split = match state.evaluate(1_601, 100, 7, &policy()).unwrap() {
            ResizeDecision::Planned(plan) => plan,
            other => panic!("expected split plan, got {other:?}"),
        };
        assert_eq!(state.commit(split, 7, 100), ResizeCommitOutcome::Applied);

        assert_eq!(
            state.evaluate(100, 105, 7, &policy()).unwrap(),
            ResizeDecision::Blocked {
                kind: ResizeKind::Merge,
                reason: ResizeBlockReason::Cooldown { remaining_ticks: 5 },
            }
        );

        assert!(matches!(
            state.evaluate(100, 110, 7, &policy()).unwrap(),
            ResizeDecision::Planned(ResizePlan {
                kind: ResizeKind::Merge,
                ..
            })
        ));
    }

    #[test]
    fn metadata_budget_blocks_split_before_allocation() {
        let mut constrained = policy();
        constrained.metadata_budget_bytes = 255;
        let state = TabletResizeState::new(8).unwrap();

        assert_eq!(
            state.evaluate(1_601, 0, 1, &constrained).unwrap(),
            ResizeDecision::Blocked {
                kind: ResizeKind::Split,
                reason: ResizeBlockReason::MetadataBudget,
            }
        );
    }

    #[test]
    fn min_and_max_tablet_bounds_are_hard() {
        let mut maxed = policy();
        maxed.max_tablets = 8;
        let state = TabletResizeState::new(8).unwrap();
        assert_eq!(
            state.evaluate(1_601, 0, 1, &maxed).unwrap(),
            ResizeDecision::Blocked {
                kind: ResizeKind::Split,
                reason: ResizeBlockReason::MaxTabletCount,
            }
        );

        let min_state = TabletResizeState::new(1).unwrap();
        assert_eq!(
            min_state.evaluate(1, 0, 1, &policy()).unwrap(),
            ResizeDecision::Blocked {
                kind: ResizeKind::Merge,
                reason: ResizeBlockReason::MinTabletCount,
            }
        );
    }

    #[test]
    fn commit_is_idempotent_after_crash_and_retry() {
        let mut state = TabletResizeState::new(8).unwrap();
        let plan = match state.evaluate(1_601, 42, 9, &policy()).unwrap() {
            ResizeDecision::Planned(plan) => plan,
            other => panic!("expected plan, got {other:?}"),
        };

        let replayed_plan = match state.evaluate(1_601, 42, 9, &policy()).unwrap() {
            ResizeDecision::Planned(plan) => plan,
            other => panic!("expected replayable plan, got {other:?}"),
        };
        assert_eq!(plan, replayed_plan);

        assert_eq!(state.commit(plan, 9, 43), ResizeCommitOutcome::Applied);
        assert_eq!(
            state.commit(plan, 9, 44),
            ResizeCommitOutcome::AlreadyApplied
        );
        assert_eq!(state.tablet_count, 16);
        assert_eq!(state.generation, 1);
    }

    #[test]
    fn topology_epoch_fences_stale_resize_plan() {
        let mut state = TabletResizeState::new(8).unwrap();
        let plan = match state.evaluate(1_601, 0, 11, &policy()).unwrap() {
            ResizeDecision::Planned(plan) => plan,
            other => panic!("expected plan, got {other:?}"),
        };

        assert_eq!(
            state.commit(plan, 12, 1),
            ResizeCommitOutcome::StaleTopology
        );
        assert_eq!(state.tablet_count, 8);
        assert_eq!(state.generation, 0);
    }

    #[test]
    fn stale_generation_cannot_rewrite_newer_resize() {
        let mut state = TabletResizeState::new(8).unwrap();
        let first = match state.evaluate(1_601, 0, 3, &policy()).unwrap() {
            ResizeDecision::Planned(plan) => plan,
            other => panic!("expected plan, got {other:?}"),
        };
        assert_eq!(state.commit(first, 3, 0), ResizeCommitOutcome::Applied);

        let stale = ResizePlan {
            kind: ResizeKind::Merge,
            topology_epoch: 3,
            from_generation: 0,
            from_count: 8,
            to_count: 4,
            planned_at_tick: 0,
        };
        assert_eq!(
            state.commit(stale, 3, 20),
            ResizeCommitOutcome::StaleGeneration
        );
        assert_eq!(state.tablet_count, 16);
    }

    #[test]
    fn threshold_noise_does_not_create_resize_storm() {
        let state = TabletResizeState::new(8).unwrap();
        for tick in 0..10_000 {
            let total = if tick % 2 == 0 { 799 } else { 801 };
            assert_eq!(
                state.evaluate(total, tick, 1, &policy()).unwrap(),
                ResizeDecision::NoChange
            );
        }
    }

    #[test]
    fn metadata_estimate_is_bounded_and_explicit() {
        let state = TabletResizeState::new(8).unwrap();
        assert_eq!(state.estimated_metadata_bytes(&policy()), 128);
        assert_eq!(policy().max_tablets_by_metadata(), 64);
    }
}
