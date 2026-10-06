use std::collections::BTreeSet;

use crate::model::{NodeId, TabletId};
use crate::resize::{
    ResizeBlockReason, ResizeConfigError, ResizeDecision, ResizeKind, ResizePlan,
    TabletResizePolicy, TabletResizeState,
};

pub const HASH_SPACE_END: u128 = 1_u128 << 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeTablet {
    pub id: TabletId,
    pub start: u128,
    pub end: u128,
    pub bytes: u64,
    pub replicas: Vec<NodeId>,
}

impl RangeTablet {
    pub fn contains_token(&self, token: u64) -> bool {
        let token = token as u128;
        self.start <= token && token < self.end
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeResizeKind {
    SplitAll,
    MergePairs,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeResizePlan {
    pub kind: RangeResizeKind,
    pub topology_epoch: u64,
    pub from_generation: u64,
    pub from_count: usize,
    pub to_count: usize,
    pub from_next_tablet_id: TabletId,
    pub to_next_tablet_id: TabletId,
    pub target_tablets: Vec<RangeTablet>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeCommitOutcome {
    Applied,
    AlreadyApplied,
    StaleTopology,
    StaleGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeResizeError {
    EmptyReplicaSet,
    DuplicateReplica,
    InvalidCoverage,
    DuplicateTabletId,
    TabletTooNarrow(TabletId),
    OddTabletCount,
    ReplicaMismatch { left: TabletId, right: TabletId },
    IdOverflow,
    ByteOverflow,
    InvalidPlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeTabletMap {
    generation: u64,
    next_tablet_id: TabletId,
    tablets: Vec<RangeTablet>,
}

impl RangeTabletMap {
    pub fn single(
        tablet_id: TabletId,
        bytes: u64,
        mut replicas: Vec<NodeId>,
    ) -> Result<Self, RangeResizeError> {
        canonicalize_replicas(&mut replicas)?;

        let next_tablet_id = tablet_id
            .checked_add(1)
            .ok_or(RangeResizeError::IdOverflow)?;
        let map = Self {
            generation: 0,
            next_tablet_id,
            tablets: vec![RangeTablet {
                id: tablet_id,
                start: 0,
                end: HASH_SPACE_END,
                bytes,
                replicas,
            }],
        };
        map.validate()?;
        Ok(map)
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn next_tablet_id(&self) -> TabletId {
        self.next_tablet_id
    }

    pub fn tablets(&self) -> &[RangeTablet] {
        &self.tablets
    }

    pub fn tablet_count(&self) -> usize {
        self.tablets.len()
    }

    pub fn total_bytes(&self) -> u64 {
        self.tablets
            .iter()
            .fold(0_u64, |sum, tablet| sum.saturating_add(tablet.bytes))
    }

    pub fn route(&self, token: u64) -> Option<&RangeTablet> {
        let token = token as u128;
        let index = self.tablets.partition_point(|tablet| tablet.end <= token);
        self.tablets
            .get(index)
            .filter(|tablet| tablet.start <= token && token < tablet.end)
    }

    pub fn validate(&self) -> Result<(), RangeResizeError> {
        validate_tablets(&self.tablets)?;

        let max_id = self
            .tablets
            .iter()
            .map(|tablet| tablet.id)
            .max()
            .ok_or(RangeResizeError::InvalidCoverage)?;
        if self.next_tablet_id <= max_id {
            return Err(RangeResizeError::DuplicateTabletId);
        }

        Ok(())
    }

    pub fn plan_split_all(&self, topology_epoch: u64) -> Result<RangeResizePlan, RangeResizeError> {
        self.validate()?;

        let mut target = Vec::with_capacity(
            self.tablets
                .len()
                .checked_mul(2)
                .ok_or(RangeResizeError::IdOverflow)?,
        );
        let mut next_id = self.next_tablet_id;

        for parent in &self.tablets {
            let width = parent.end - parent.start;
            if width < 2 {
                return Err(RangeResizeError::TabletTooNarrow(parent.id));
            }

            let midpoint = parent.start + width / 2;
            let left_id = next_id;
            let right_id = left_id.checked_add(1).ok_or(RangeResizeError::IdOverflow)?;
            next_id = right_id
                .checked_add(1)
                .ok_or(RangeResizeError::IdOverflow)?;

            let left_bytes = parent.bytes / 2;
            let right_bytes = parent.bytes - left_bytes;

            target.push(RangeTablet {
                id: left_id,
                start: parent.start,
                end: midpoint,
                bytes: left_bytes,
                replicas: parent.replicas.clone(),
            });
            target.push(RangeTablet {
                id: right_id,
                start: midpoint,
                end: parent.end,
                bytes: right_bytes,
                replicas: parent.replicas.clone(),
            });
        }

        validate_tablets(&target)?;

        Ok(RangeResizePlan {
            kind: RangeResizeKind::SplitAll,
            topology_epoch,
            from_generation: self.generation,
            from_count: self.tablets.len(),
            to_count: target.len(),
            from_next_tablet_id: self.next_tablet_id,
            to_next_tablet_id: next_id,
            target_tablets: target,
        })
    }

    pub(crate) fn plan_merge_pairs(
        &self,
        topology_epoch: u64,
    ) -> Result<RangeResizePlan, RangeResizeError> {
        self.validate()?;

        if self.tablets.len() % 2 != 0 {
            return Err(RangeResizeError::OddTabletCount);
        }

        let mut target = Vec::with_capacity(self.tablets.len() / 2);
        let mut next_id = self.next_tablet_id;

        for pair in self.tablets.chunks_exact(2) {
            let left = &pair[0];
            let right = &pair[1];

            if left.end != right.start {
                return Err(RangeResizeError::InvalidCoverage);
            }
            if left.replicas != right.replicas {
                return Err(RangeResizeError::ReplicaMismatch {
                    left: left.id,
                    right: right.id,
                });
            }

            let merged_bytes = left
                .bytes
                .checked_add(right.bytes)
                .ok_or(RangeResizeError::ByteOverflow)?;
            let merged_id = next_id;
            next_id = next_id.checked_add(1).ok_or(RangeResizeError::IdOverflow)?;

            target.push(RangeTablet {
                id: merged_id,
                start: left.start,
                end: right.end,
                bytes: merged_bytes,
                replicas: left.replicas.clone(),
            });
        }

        validate_tablets(&target)?;

        Ok(RangeResizePlan {
            kind: RangeResizeKind::MergePairs,
            topology_epoch,
            from_generation: self.generation,
            from_count: self.tablets.len(),
            to_count: target.len(),
            from_next_tablet_id: self.next_tablet_id,
            to_next_tablet_id: next_id,
            target_tablets: target,
        })
    }

    fn validate_transform(&self, plan: &RangeResizePlan) -> Result<(), RangeResizeError> {
        match plan.kind {
            RangeResizeKind::SplitAll => {
                if plan.to_count != plan.from_count.saturating_mul(2)
                    || plan.target_tablets.len() != self.tablets.len().saturating_mul(2)
                {
                    return Err(RangeResizeError::InvalidPlan);
                }

                let mut expected_id = self.next_tablet_id;
                for (parent, children) in
                    self.tablets.iter().zip(plan.target_tablets.chunks_exact(2))
                {
                    let width = parent.end - parent.start;
                    if width < 2 {
                        return Err(RangeResizeError::TabletTooNarrow(parent.id));
                    }
                    let midpoint = parent.start + width / 2;
                    let left = &children[0];
                    let right = &children[1];
                    let right_id = expected_id
                        .checked_add(1)
                        .ok_or(RangeResizeError::IdOverflow)?;

                    if left.id != expected_id
                        || right.id != right_id
                        || left.start != parent.start
                        || left.end != midpoint
                        || right.start != midpoint
                        || right.end != parent.end
                        || left.replicas != parent.replicas
                        || right.replicas != parent.replicas
                        || left.bytes != parent.bytes / 2
                        || right.bytes != parent.bytes - parent.bytes / 2
                    {
                        return Err(RangeResizeError::InvalidPlan);
                    }

                    expected_id = right_id
                        .checked_add(1)
                        .ok_or(RangeResizeError::IdOverflow)?;
                }

                if expected_id != plan.to_next_tablet_id {
                    return Err(RangeResizeError::InvalidPlan);
                }
            }
            RangeResizeKind::MergePairs => {
                if self.tablets.len() % 2 != 0
                    || plan.from_count % 2 != 0
                    || plan.to_count != plan.from_count / 2
                    || plan.target_tablets.len() != self.tablets.len() / 2
                {
                    return Err(RangeResizeError::InvalidPlan);
                }

                let mut expected_id = self.next_tablet_id;
                for (parents, merged) in self.tablets.chunks_exact(2).zip(&plan.target_tablets) {
                    let left = &parents[0];
                    let right = &parents[1];
                    if left.end != right.start || left.replicas != right.replicas {
                        return Err(RangeResizeError::InvalidPlan);
                    }
                    let expected_bytes = left
                        .bytes
                        .checked_add(right.bytes)
                        .ok_or(RangeResizeError::ByteOverflow)?;

                    if merged.id != expected_id
                        || merged.start != left.start
                        || merged.end != right.end
                        || merged.replicas != left.replicas
                        || merged.bytes != expected_bytes
                    {
                        return Err(RangeResizeError::InvalidPlan);
                    }

                    expected_id = expected_id
                        .checked_add(1)
                        .ok_or(RangeResizeError::IdOverflow)?;
                }

                if expected_id != plan.to_next_tablet_id {
                    return Err(RangeResizeError::InvalidPlan);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn commit(
        &mut self,
        plan: &RangeResizePlan,
        current_topology_epoch: u64,
    ) -> Result<RangeCommitOutcome, RangeResizeError> {
        if current_topology_epoch != plan.topology_epoch {
            return Ok(RangeCommitOutcome::StaleTopology);
        }

        if self.generation == plan.from_generation + 1
            && self.tablets == plan.target_tablets
            && self.next_tablet_id == plan.to_next_tablet_id
        {
            return Ok(RangeCommitOutcome::AlreadyApplied);
        }

        if self.generation != plan.from_generation
            || self.tablets.len() != plan.from_count
            || self.next_tablet_id != plan.from_next_tablet_id
        {
            return Ok(RangeCommitOutcome::StaleGeneration);
        }

        if plan.to_count != plan.target_tablets.len() {
            return Err(RangeResizeError::InvalidPlan);
        }

        self.validate_transform(plan)?;
        validate_tablets(&plan.target_tablets)?;

        let mut candidate = Self {
            generation: self.generation + 1,
            next_tablet_id: plan.to_next_tablet_id,
            tablets: plan.target_tablets.clone(),
        };
        candidate.validate()?;

        std::mem::swap(self, &mut candidate);
        Ok(RangeCommitOutcome::Applied)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleResizePlan {
    controller: ResizePlan,
    range: RangeResizePlan,
}

impl LifecycleResizePlan {
    pub fn kind(&self) -> ResizeKind {
        self.controller.kind
    }

    pub fn from_count(&self) -> u64 {
        self.controller.from_count
    }

    pub fn to_count(&self) -> u64 {
        self.controller.to_count
    }

    pub fn topology_epoch(&self) -> u64 {
        self.controller.topology_epoch
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleResizeDecision {
    NoChange,
    Blocked {
        kind: ResizeKind,
        reason: ResizeBlockReason,
    },
    Planned(LifecycleResizePlan),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleResizeError {
    Config(ResizeConfigError),
    Range(RangeResizeError),
    TabletCountOverflow,
    PlanMismatch,
}

impl From<ResizeConfigError> for LifecycleResizeError {
    fn from(value: ResizeConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<RangeResizeError> for LifecycleResizeError {
    fn from(value: RangeResizeError) -> Self {
        Self::Range(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TabletRangeLifecycle {
    map: RangeTabletMap,
    last_resize_tick: Option<u64>,
}

impl TabletRangeLifecycle {
    pub fn new(map: RangeTabletMap) -> Result<Self, RangeResizeError> {
        map.validate()?;
        Ok(Self {
            map,
            last_resize_tick: None,
        })
    }

    pub fn map(&self) -> &RangeTabletMap {
        &self.map
    }

    pub fn last_resize_tick(&self) -> Option<u64> {
        self.last_resize_tick
    }

    pub fn resize_state(&self) -> Result<TabletResizeState, LifecycleResizeError> {
        let tablet_count = u64::try_from(self.map.tablet_count())
            .map_err(|_| LifecycleResizeError::TabletCountOverflow)?;
        Ok(TabletResizeState {
            generation: self.map.generation(),
            tablet_count,
            last_resize_tick: self.last_resize_tick,
        })
    }

    pub fn evaluate(
        &self,
        total_table_bytes: u64,
        now_tick: u64,
        topology_epoch: u64,
        policy: &TabletResizePolicy,
    ) -> Result<LifecycleResizeDecision, LifecycleResizeError> {
        let state = self.resize_state()?;
        match state.evaluate(total_table_bytes, now_tick, topology_epoch, policy)? {
            ResizeDecision::NoChange => Ok(LifecycleResizeDecision::NoChange),
            ResizeDecision::Blocked { kind, reason } => {
                Ok(LifecycleResizeDecision::Blocked { kind, reason })
            }
            ResizeDecision::Planned(controller) => {
                let range = match controller.kind {
                    ResizeKind::Split => self.map.plan_split_all(topology_epoch)?,
                    ResizeKind::Merge => self.map.plan_merge_pairs(topology_epoch)?,
                };

                if !controller_matches_range(&controller, &range) {
                    return Err(LifecycleResizeError::PlanMismatch);
                }

                Ok(LifecycleResizeDecision::Planned(LifecycleResizePlan {
                    controller,
                    range,
                }))
            }
        }
    }

    pub fn commit(
        &mut self,
        plan: &LifecycleResizePlan,
        current_topology_epoch: u64,
        now_tick: u64,
    ) -> Result<RangeCommitOutcome, LifecycleResizeError> {
        if !controller_matches_range(&plan.controller, &plan.range) {
            return Err(LifecycleResizeError::PlanMismatch);
        }

        let mut candidate = self.clone();
        let outcome = candidate.map.commit(&plan.range, current_topology_epoch)?;

        if outcome == RangeCommitOutcome::Applied {
            candidate.last_resize_tick = Some(now_tick);
            std::mem::swap(self, &mut candidate);
        }

        Ok(outcome)
    }
}

fn controller_matches_range(controller: &ResizePlan, range: &RangeResizePlan) -> bool {
    let kind_matches = matches!(
        (controller.kind, range.kind),
        (ResizeKind::Split, RangeResizeKind::SplitAll)
            | (ResizeKind::Merge, RangeResizeKind::MergePairs)
    );

    kind_matches
        && controller.topology_epoch == range.topology_epoch
        && controller.from_generation == range.from_generation
        && usize::try_from(controller.from_count).ok() == Some(range.from_count)
        && usize::try_from(controller.to_count).ok() == Some(range.to_count)
}

fn canonicalize_replicas(replicas: &mut [NodeId]) -> Result<(), RangeResizeError> {
    if replicas.is_empty() {
        return Err(RangeResizeError::EmptyReplicaSet);
    }
    replicas.sort_unstable();
    if replicas.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(RangeResizeError::DuplicateReplica);
    }
    Ok(())
}

fn validate_tablets(tablets: &[RangeTablet]) -> Result<(), RangeResizeError> {
    if tablets.is_empty() {
        return Err(RangeResizeError::InvalidCoverage);
    }

    let mut ids = BTreeSet::new();
    let mut expected_start = 0_u128;

    for tablet in tablets {
        if tablet.start != expected_start || tablet.start >= tablet.end {
            return Err(RangeResizeError::InvalidCoverage);
        }
        if !ids.insert(tablet.id) {
            return Err(RangeResizeError::DuplicateTabletId);
        }
        if tablet.replicas.is_empty() {
            return Err(RangeResizeError::EmptyReplicaSet);
        }
        if tablet.replicas.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(RangeResizeError::DuplicateReplica);
        }
        expected_start = tablet.end;
    }

    if expected_start != HASH_SPACE_END {
        return Err(RangeResizeError::InvalidCoverage);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(bytes: u64) -> RangeTabletMap {
        RangeTabletMap::single(10, bytes, vec![3, 1, 2]).unwrap()
    }

    fn lifecycle_policy() -> TabletResizePolicy {
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
    fn integrated_lifecycle_commits_count_range_map_and_cooldown_atomically() {
        let map = single(1_601);
        let mut lifecycle = TabletRangeLifecycle::new(map).unwrap();

        let plan = match lifecycle
            .evaluate(1_601, 100, 7, &lifecycle_policy())
            .unwrap()
        {
            LifecycleResizeDecision::Planned(plan) => plan,
            other => panic!("expected integrated split plan, got {other:?}"),
        };

        assert_eq!(plan.kind(), ResizeKind::Split);
        assert_eq!(plan.from_count(), 1);
        assert_eq!(plan.to_count(), 2);

        assert_eq!(
            lifecycle.commit(&plan, 7, 101).unwrap(),
            RangeCommitOutcome::Applied
        );
        assert_eq!(lifecycle.map().tablet_count(), 2);
        assert_eq!(lifecycle.map().generation(), 1);
        assert_eq!(lifecycle.last_resize_tick(), Some(101));
        assert_eq!(
            lifecycle.resize_state().unwrap(),
            TabletResizeState {
                generation: 1,
                tablet_count: 2,
                last_resize_tick: Some(101),
            }
        );

        assert_eq!(
            lifecycle.commit(&plan, 7, 102).unwrap(),
            RangeCommitOutcome::AlreadyApplied
        );
        assert_eq!(lifecycle.last_resize_tick(), Some(101));
    }

    #[test]
    fn integrated_merge_refuses_replica_mismatch_without_partial_commit() {
        let map = single(1_601);
        let mut lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let split = match lifecycle
            .evaluate(1_601, 0, 3, &lifecycle_policy())
            .unwrap()
        {
            LifecycleResizeDecision::Planned(plan) => plan,
            other => panic!("expected split plan, got {other:?}"),
        };
        lifecycle.commit(&split, 3, 0).unwrap();

        lifecycle.map.tablets[1].replicas = vec![1, 2, 4];
        let before = lifecycle.clone();

        assert_eq!(
            lifecycle.evaluate(1, 20, 3, &lifecycle_policy()),
            Err(LifecycleResizeError::Range(
                RangeResizeError::ReplicaMismatch {
                    left: lifecycle.map.tablets()[0].id,
                    right: lifecycle.map.tablets()[1].id,
                }
            ))
        );
        assert_eq!(lifecycle, before);
    }

    #[test]
    fn integrated_topology_epoch_fences_both_controller_and_range_map() {
        let map = single(1_601);
        let mut lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let before = lifecycle.clone();
        let plan = match lifecycle
            .evaluate(1_601, 0, 4, &lifecycle_policy())
            .unwrap()
        {
            LifecycleResizeDecision::Planned(plan) => plan,
            other => panic!("expected split plan, got {other:?}"),
        };

        assert_eq!(
            lifecycle.commit(&plan, 5, 1).unwrap(),
            RangeCommitOutcome::StaleTopology
        );
        assert_eq!(lifecycle, before);
    }

    #[test]
    fn duplicate_replica_input_is_rejected() {
        assert_eq!(
            RangeTabletMap::single(1, 100, vec![1, 1, 2]),
            Err(RangeResizeError::DuplicateReplica)
        );
    }

    #[test]
    fn tampered_plan_cannot_change_hash_boundaries() {
        let mut map = single(100);
        let original = map.clone();
        let mut plan = map.plan_split_all(5).unwrap();
        plan.target_tablets[0].end += 1;
        plan.target_tablets[1].start += 1;

        assert_eq!(map.commit(&plan, 5), Err(RangeResizeError::InvalidPlan));
        assert_eq!(map, original);
    }

    #[test]
    fn split_all_preserves_coverage_bytes_and_routing_replicas() {
        let mut map = single(101);
        let samples = [0_u64, 1, u64::MAX / 2, u64::MAX];
        let before: Vec<_> = samples
            .iter()
            .map(|token| map.route(*token).unwrap().replicas.clone())
            .collect();

        let plan = map.plan_split_all(7).unwrap();
        assert_eq!(plan.from_count, 1);
        assert_eq!(plan.to_count, 2);
        assert_eq!(map.commit(&plan, 7).unwrap(), RangeCommitOutcome::Applied);

        assert_eq!(map.tablet_count(), 2);
        assert_eq!(map.total_bytes(), 101);
        assert_eq!(map.tablets()[0].start, 0);
        assert_eq!(map.tablets()[0].end, 1_u128 << 63);
        assert_eq!(map.tablets()[1].start, 1_u128 << 63);
        assert_eq!(map.tablets()[1].end, HASH_SPACE_END);
        assert_eq!(map.tablets()[0].replicas, vec![1, 2, 3]);
        assert_eq!(map.tablets()[1].replicas, vec![1, 2, 3]);

        let after: Vec<_> = samples
            .iter()
            .map(|token| map.route(*token).unwrap().replicas.clone())
            .collect();
        assert_eq!(before, after);
        map.validate().unwrap();
    }

    #[test]
    fn repeated_split_doubles_count_without_gaps() {
        let mut map = single(1_000);
        for generation in 0..6 {
            let plan = map.plan_split_all(1).unwrap();
            assert_eq!(map.commit(&plan, 1).unwrap(), RangeCommitOutcome::Applied);
            assert_eq!(map.generation(), generation + 1);
            map.validate().unwrap();
        }
        assert_eq!(map.tablet_count(), 64);
        assert_eq!(map.total_bytes(), 1_000);
    }

    #[test]
    fn merge_after_split_restores_one_contiguous_range() {
        let mut map = single(101);
        let split = map.plan_split_all(4).unwrap();
        map.commit(&split, 4).unwrap();

        let merge = map.plan_merge_pairs(4).unwrap();
        assert_eq!(merge.from_count, 2);
        assert_eq!(merge.to_count, 1);
        map.commit(&merge, 4).unwrap();

        assert_eq!(map.tablet_count(), 1);
        assert_eq!(map.total_bytes(), 101);
        assert_eq!(map.tablets()[0].start, 0);
        assert_eq!(map.tablets()[0].end, HASH_SPACE_END);
        assert_eq!(map.tablets()[0].replicas, vec![1, 2, 3]);
    }

    #[test]
    fn merge_requires_identical_replica_sets() {
        let mut map = single(100);
        let split = map.plan_split_all(1).unwrap();
        map.commit(&split, 1).unwrap();

        map.tablets[1].replicas = vec![1, 2, 4];

        assert_eq!(
            map.plan_merge_pairs(1),
            Err(RangeResizeError::ReplicaMismatch {
                left: map.tablets()[0].id,
                right: map.tablets()[1].id,
            })
        );
    }

    #[test]
    fn plan_is_deterministic_and_commit_is_replay_safe() {
        let mut map = single(100);
        let a = map.plan_split_all(8).unwrap();
        let b = map.plan_split_all(8).unwrap();
        assert_eq!(a, b);

        assert_eq!(map.commit(&a, 8).unwrap(), RangeCommitOutcome::Applied);
        assert_eq!(
            map.commit(&a, 8).unwrap(),
            RangeCommitOutcome::AlreadyApplied
        );
    }

    #[test]
    fn topology_epoch_fences_range_resize() {
        let mut map = single(100);
        let plan = map.plan_split_all(8).unwrap();

        assert_eq!(
            map.commit(&plan, 9).unwrap(),
            RangeCommitOutcome::StaleTopology
        );
        assert_eq!(map.tablet_count(), 1);
        assert_eq!(map.generation(), 0);
    }

    #[test]
    fn stale_generation_cannot_overwrite_newer_map() {
        let mut map = single(100);
        let stale = map.plan_split_all(3).unwrap();
        let first = map.plan_split_all(3).unwrap();
        map.commit(&first, 3).unwrap();

        let newer = map.plan_split_all(3).unwrap();
        map.commit(&newer, 3).unwrap();

        assert_eq!(
            map.commit(&stale, 3).unwrap(),
            RangeCommitOutcome::StaleGeneration
        );
        assert_eq!(map.tablet_count(), 4);
        assert_eq!(map.generation(), 2);
    }

    #[test]
    fn every_sample_token_routes_after_multiple_resizes() {
        let mut map = single(10_000);
        for _ in 0..5 {
            let plan = map.plan_split_all(2).unwrap();
            map.commit(&plan, 2).unwrap();
        }

        for token in (0_u64..=u16::MAX as u64).step_by(257) {
            assert!(map.route(token).is_some());
        }

        for _ in 0..5 {
            let plan = map.plan_merge_pairs(2).unwrap();
            map.commit(&plan, 2).unwrap();
        }

        assert_eq!(map.tablet_count(), 1);
        map.validate().unwrap();
    }
}
