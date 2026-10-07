use crate::disk::{DiskError, DurableStore};
use crate::migration::MigrationBudget;
use crate::model::Cluster;
use crate::placement::FailureDomainPolicy;
use crate::range_resize::{LifecycleResizePlan, RangeCommitOutcome};
use crate::runtime_coordinator::{
    CoordinatedResizeCommitOutcome, TabletRuntimeCoordinator, TabletRuntimeError,
};
use crate::topology_snapshot::{
    DurableTopologyTxnWriter, TopologyRecovery, TopologySnapshot, TopologyTxnError,
    TopologyTxnState, read_prepared_topology, recover_topology,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableResizeProgress {
    Preparing,
    Prepared,
    Publishing,
    Complete,
    StorageFailed(DiskError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableResizeError {
    Runtime(TabletRuntimeError),
    Topology(TopologyTxnError),
    PreviewRejected(RangeCommitOutcome),
    ApplyRejected(CoordinatedResizeCommitOutcome),
    SnapshotMismatch,
    RecoveryMissingPrepared,
    RecoveryPreparedMismatch,
    EmptyRecovery,
}

impl From<TabletRuntimeError> for DurableResizeError {
    fn from(value: TabletRuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl From<TopologyTxnError> for DurableResizeError {
    fn from(value: TopologyTxnError) -> Self {
        Self::Topology(value)
    }
}

#[derive(Debug)]
pub struct DurableResizeTransaction {
    plan: LifecycleResizePlan,
    topology_epoch: u64,
    commit_tick: u64,
    target: TopologySnapshot,
    writer: DurableTopologyTxnWriter,
    applied: bool,
}

impl DurableResizeTransaction {
    pub fn new(
        txn_id: u64,
        runtime: &mut TabletRuntimeCoordinator,
        plan: LifecycleResizePlan,
        commit_tick: u64,
    ) -> Result<Self, DurableResizeError> {
        let current = runtime.capture_topology_snapshot()?;
        let topology_epoch = current.topology_epoch();
        let mut target_lifecycle = current.lifecycle().clone();
        let preview = target_lifecycle
            .commit(&plan, topology_epoch, commit_tick)
            .map_err(|error| DurableResizeError::Runtime(TabletRuntimeError::Lifecycle(error)))?;
        if preview != RangeCommitOutcome::Applied {
            return Err(DurableResizeError::PreviewRejected(preview));
        }

        let target = current.with_lifecycle(target_lifecycle);
        let writer = DurableTopologyTxnWriter::new(txn_id, &current, target.clone())?;

        Ok(Self {
            plan,
            topology_epoch,
            commit_tick,
            target,
            writer,
            applied: false,
        })
    }

    pub fn target_snapshot(&self) -> &TopologySnapshot {
        &self.target
    }

    pub fn writer_state(&self) -> TopologyTxnState {
        self.writer.state()
    }

    pub fn is_complete(&self) -> bool {
        self.writer.is_complete()
    }

    pub fn retry_storage(&mut self) {
        self.writer.retry();
    }

    pub fn tick<S: DurableStore>(
        &mut self,
        now_tick: u64,
        runtime: &mut TabletRuntimeCoordinator,
        store: &mut S,
    ) -> Result<DurableResizeProgress, DurableResizeError> {
        let entered_prepared = self.writer.state() == TopologyTxnState::Prepared;

        self.writer.tick(now_tick, store);

        if let TopologyTxnState::Failed { error, .. } = self.writer.state() {
            return Ok(DurableResizeProgress::StorageFailed(error));
        }

        // Intentionally do not apply on the same tick that PREPARED first
        // becomes durable. This creates an observable crash boundary.
        if entered_prepared && !self.applied {
            let outcome =
                runtime.commit_resize(&self.plan, self.topology_epoch, self.commit_tick)?;
            if !matches!(
                outcome,
                CoordinatedResizeCommitOutcome::Applied { .. }
                    | CoordinatedResizeCommitOutcome::AlreadyApplied
            ) {
                return Err(DurableResizeError::ApplyRejected(outcome));
            }

            let applied = runtime.capture_topology_snapshot()?;
            if applied != self.target {
                return Err(DurableResizeError::SnapshotMismatch);
            }

            if !self.writer.mark_applied() {
                return Err(DurableResizeError::SnapshotMismatch);
            }
            self.applied = true;
            self.writer.tick(now_tick, store);

            if let TopologyTxnState::Failed { error, .. } = self.writer.state() {
                return Ok(DurableResizeProgress::StorageFailed(error));
            }
        }

        Ok(match self.writer.state() {
            TopologyTxnState::PrepareIdle
            | TopologyTxnState::PreparePutPending
            | TopologyTxnState::PrepareSyncPending => DurableResizeProgress::Preparing,
            TopologyTxnState::Prepared => DurableResizeProgress::Prepared,
            TopologyTxnState::PublishIdle
            | TopologyTxnState::PublishPutPending
            | TopologyTxnState::PublishSyncPending => DurableResizeProgress::Publishing,
            TopologyTxnState::Complete => DurableResizeProgress::Complete,
            TopologyTxnState::Failed { error, .. } => DurableResizeProgress::StorageFailed(error),
        })
    }
}

#[derive(Debug)]
pub struct RecoveredTabletRuntime {
    runtime: TabletRuntimeCoordinator,
    publisher: Option<DurableTopologyTxnWriter>,
    replayed_txn_id: Option<u64>,
}

impl RecoveredTabletRuntime {
    pub fn runtime(&self) -> &TabletRuntimeCoordinator {
        &self.runtime
    }

    pub fn runtime_mut(&mut self) -> &mut TabletRuntimeCoordinator {
        &mut self.runtime
    }

    pub fn replayed_txn_id(&self) -> Option<u64> {
        self.replayed_txn_id
    }

    pub fn publication_complete(&self) -> bool {
        self.publisher
            .as_ref()
            .is_none_or(DurableTopologyTxnWriter::is_complete)
    }

    pub fn tick_publication<S: DurableStore>(
        &mut self,
        now_tick: u64,
        store: &mut S,
    ) -> Result<DurableResizeProgress, DurableResizeError> {
        let Some(publisher) = self.publisher.as_mut() else {
            return Ok(DurableResizeProgress::Complete);
        };
        publisher.tick(now_tick, store);
        Ok(match publisher.state() {
            TopologyTxnState::PublishIdle
            | TopologyTxnState::PublishPutPending
            | TopologyTxnState::PublishSyncPending => DurableResizeProgress::Publishing,
            TopologyTxnState::Complete => DurableResizeProgress::Complete,
            TopologyTxnState::Failed { error, .. } => DurableResizeProgress::StorageFailed(error),
            _ => return Err(DurableResizeError::RecoveryPreparedMismatch),
        })
    }

    pub fn retry_publication(&mut self) {
        if let Some(publisher) = self.publisher.as_mut() {
            publisher.retry();
        }
    }
}

pub fn recover_tablet_runtime<S: DurableStore>(
    now_tick: u64,
    store: &mut S,
    placement_policy: FailureDomainPolicy,
    migration_budget: MigrationBudget,
    window_tablets: usize,
) -> Result<RecoveredTabletRuntime, DurableResizeError> {
    match recover_topology(now_tick, store)? {
        TopologyRecovery::Empty => Err(DurableResizeError::EmptyRecovery),
        TopologyRecovery::Current(snapshot) => {
            let runtime = TabletRuntimeCoordinator::from_topology_snapshot(
                snapshot,
                placement_policy,
                migration_budget,
                window_tablets,
            )?;
            Ok(RecoveredTabletRuntime {
                runtime,
                publisher: None,
                replayed_txn_id: None,
            })
        }
        TopologyRecovery::ReplayPrepared { txn_id, target } => {
            let prepared = read_prepared_topology(now_tick, u64::MAX - 2, store)?
                .ok_or(DurableResizeError::RecoveryMissingPrepared)?;
            if prepared.txn_id != txn_id || prepared.target != target {
                return Err(DurableResizeError::RecoveryPreparedMismatch);
            }

            let runtime = TabletRuntimeCoordinator::from_topology_snapshot(
                target,
                placement_policy,
                migration_budget,
                window_tablets,
            )?;
            let publisher = DurableTopologyTxnWriter::resume_publish(prepared)?;

            Ok(RecoveredTabletRuntime {
                runtime,
                publisher: Some(publisher),
                replayed_txn_id: Some(txn_id),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::disk::{DiskRequest, DiskSubmit, SimDisk};
    use crate::model::{AdminState, Cluster, Node};
    use crate::range_resize::{RangeTabletMap, TabletRangeLifecycle};
    use crate::resize::TabletResizePolicy;

    fn node(id: u64, weight: u32, zone: &str, state: AdminState) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: "r1".into(),
            state,
        }
    }

    fn cluster() -> Cluster {
        Cluster {
            epoch: 7,
            replication_factor: 2,
            nodes: [
                node(1, 1, "a", AdminState::Active),
                node(2, 1, "b", AdminState::Removed),
                node(3, 4, "c", AdminState::Active),
                node(4, 8, "d", AdminState::Active),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: Vec::new(),
        }
    }

    fn budget() -> MigrationBudget {
        MigrationBudget {
            max_active: 4,
            max_per_node_active: 4,
            bytes_per_tick: 10_000,
            max_bytes_per_node_per_tick: 10_000,
            max_bytes_per_task_per_tick: 10_000,
        }
    }

    fn policy() -> TabletResizePolicy {
        TabletResizePolicy {
            target_tablet_bytes: 100,
            split_above_num: 2,
            split_above_den: 1,
            merge_below_num: 1,
            merge_below_den: 2,
            cooldown_ticks: 0,
            min_tablets: 1,
            max_tablets: 64,
            metadata_bytes_per_tablet: 24,
            metadata_budget_bytes: 64 * 24,
        }
    }

    fn runtime() -> TabletRuntimeCoordinator {
        let map = RangeTabletMap::single(10_000, 1_000, vec![1, 2]).unwrap();
        let lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let mut runtime = TabletRuntimeCoordinator::new(
            cluster(),
            lifecycle,
            FailureDomainPolicy::HIERARCHICAL,
            budget(),
            8,
        )
        .unwrap();
        for _ in 0..100 {
            if runtime.is_migration_converged() {
                break;
            }
            runtime.tick().unwrap();
        }
        assert!(runtime.is_migration_converged());
        runtime
    }

    fn plan(runtime: &mut TabletRuntimeCoordinator) -> LifecycleResizePlan {
        match runtime.evaluate_resize(1_000, 10, 7, &policy()).unwrap() {
            crate::runtime_coordinator::CoordinatedResizeDecision::Planned(plan) => plan,
            other => panic!("expected resize plan, got {other:?}"),
        }
    }

    fn persist_current(disk: &mut SimDisk, snapshot: &TopologySnapshot) {
        disk.set_delay(0);
        assert!(matches!(
            disk.submit(
                0,
                DiskRequest::Put {
                    op_id: 90_000,
                    key: b"topology/current".to_vec(),
                    value: snapshot.encode().unwrap(),
                },
            ),
            DiskSubmit::Completed(_)
        ));
        assert!(matches!(
            disk.submit(0, DiskRequest::Sync { op_id: 90_001 }),
            DiskSubmit::Completed(_)
        ));
    }

    #[test]
    fn durable_resize_prepares_applies_then_publishes() {
        let mut runtime = runtime();
        let plan = plan(&mut runtime);
        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);

        let mut txn = DurableResizeTransaction::new(100, &mut runtime, plan, 11).unwrap();

        assert_eq!(
            txn.tick(1, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Prepared
        );
        assert_eq!(runtime.lifecycle().map().generation(), 0);

        assert_eq!(
            txn.tick(2, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Complete
        );
        assert_eq!(runtime.lifecycle().map().generation(), 1);
        assert_eq!(
            runtime.capture_topology_snapshot().unwrap(),
            *txn.target_snapshot()
        );

        disk.crash();
        let recovered =
            recover_tablet_runtime(3, &mut disk, FailureDomainPolicy::HIERARCHICAL, budget(), 8)
                .unwrap();
        assert_eq!(recovered.replayed_txn_id(), None);
        assert_eq!(recovered.runtime().lifecycle().map().generation(), 1);
    }

    #[test]
    fn crash_after_prepared_before_apply_replays_target_runtime() {
        let mut runtime = runtime();
        let plan = plan(&mut runtime);
        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);

        let mut txn = DurableResizeTransaction::new(101, &mut runtime, plan, 11).unwrap();
        assert_eq!(
            txn.tick(1, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Prepared
        );
        assert_eq!(runtime.lifecycle().map().generation(), 0);

        disk.crash();
        drop(runtime);

        let mut recovered =
            recover_tablet_runtime(2, &mut disk, FailureDomainPolicy::HIERARCHICAL, budget(), 8)
                .unwrap();
        assert_eq!(recovered.replayed_txn_id(), Some(101));
        assert_eq!(recovered.runtime().lifecycle().map().generation(), 1);
        assert!(!recovered.publication_complete());

        assert_eq!(
            recovered.tick_publication(3, &mut disk).unwrap(),
            DurableResizeProgress::Complete
        );
        disk.crash();

        let recovered_again =
            recover_tablet_runtime(4, &mut disk, FailureDomainPolicy::HIERARCHICAL, budget(), 8)
                .unwrap();
        assert_eq!(recovered_again.replayed_txn_id(), None);
        assert_eq!(recovered_again.runtime().lifecycle().map().generation(), 1);
    }

    #[test]
    fn disk_full_during_prepare_never_mutates_runtime() {
        let mut runtime = runtime();
        let plan = plan(&mut runtime);
        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);
        disk.set_full(true);

        let mut txn = DurableResizeTransaction::new(102, &mut runtime, plan, 11).unwrap();
        assert_eq!(
            txn.tick(1, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::StorageFailed(DiskError::Full)
        );
        assert_eq!(runtime.lifecycle().map().generation(), 0);

        disk.set_full(false);
        txn.retry_storage();
        assert_eq!(
            txn.tick(2, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Prepared
        );
        assert_eq!(runtime.lifecycle().map().generation(), 0);
        assert_eq!(
            txn.tick(3, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Complete
        );
        assert_eq!(runtime.lifecycle().map().generation(), 1);
    }

    #[test]
    fn crash_after_runtime_apply_before_current_sync_replays_same_target() {
        let mut runtime = runtime();
        let plan = plan(&mut runtime);
        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);

        let mut txn = DurableResizeTransaction::new(103, &mut runtime, plan, 11).unwrap();
        assert_eq!(
            txn.tick(1, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Prepared
        );

        disk.set_delay(2);
        assert_eq!(
            txn.tick(2, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Publishing
        );
        assert_eq!(runtime.lifecycle().map().generation(), 1);
        assert_eq!(txn.writer_state(), TopologyTxnState::PublishPutPending);

        txn.tick(4, &mut runtime, &mut disk).unwrap();
        assert_eq!(txn.writer_state(), TopologyTxnState::PublishSyncPending);

        disk.crash();
        drop(runtime);

        disk.set_delay(0);
        let mut recovered =
            recover_tablet_runtime(5, &mut disk, FailureDomainPolicy::HIERARCHICAL, budget(), 8)
                .unwrap();
        assert_eq!(recovered.replayed_txn_id(), Some(103));
        assert_eq!(recovered.runtime().lifecycle().map().generation(), 1);
        assert_eq!(
            recovered.tick_publication(6, &mut disk).unwrap(),
            DurableResizeProgress::Complete
        );
    }

    #[test]
    fn disk_full_during_publish_keeps_prepared_recovery_path() {
        let mut runtime = runtime();
        let plan = plan(&mut runtime);
        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);

        let mut txn = DurableResizeTransaction::new(105, &mut runtime, plan, 11).unwrap();
        assert_eq!(
            txn.tick(1, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Prepared
        );

        disk.set_full(true);
        assert_eq!(
            txn.tick(2, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::StorageFailed(DiskError::Full)
        );
        assert_eq!(runtime.lifecycle().map().generation(), 1);

        disk.crash();
        drop(runtime);
        disk.set_full(false);

        let mut recovered =
            recover_tablet_runtime(3, &mut disk, FailureDomainPolicy::HIERARCHICAL, budget(), 8)
                .unwrap();
        assert_eq!(recovered.replayed_txn_id(), Some(105));
        assert_eq!(recovered.runtime().lifecycle().map().generation(), 1);
        assert_eq!(
            recovered.tick_publication(4, &mut disk).unwrap(),
            DurableResizeProgress::Complete
        );
    }

    #[test]
    fn crash_at_every_durable_resize_edge_recovers_without_ambiguity() {
        #[derive(Clone, Copy)]
        enum CrashEdge {
            PreparePutPending,
            PrepareSyncPending,
            Prepared,
            PublishPutPending,
            PublishSyncPending,
            Complete,
        }

        let cases = [
            (CrashEdge::PreparePutPending, 0_u64, false),
            (CrashEdge::PrepareSyncPending, 0_u64, false),
            (CrashEdge::Prepared, 1_u64, true),
            (CrashEdge::PublishPutPending, 1_u64, true),
            (CrashEdge::PublishSyncPending, 1_u64, true),
            (CrashEdge::Complete, 1_u64, false),
        ];

        for (case_index, (edge, expected_generation, expect_replay)) in
            cases.into_iter().enumerate()
        {
            let mut runtime = runtime();
            let plan = plan(&mut runtime);
            let current = runtime.capture_topology_snapshot().unwrap();
            let mut disk = SimDisk::default();
            persist_current(&mut disk, &current);
            disk.set_delay(2);

            let mut txn =
                DurableResizeTransaction::new(1_000 + case_index as u64, &mut runtime, plan, 11)
                    .unwrap();

            match edge {
                CrashEdge::PreparePutPending => {
                    txn.tick(0, &mut runtime, &mut disk).unwrap();
                    assert_eq!(txn.writer_state(), TopologyTxnState::PreparePutPending);
                }
                CrashEdge::PrepareSyncPending => {
                    txn.tick(0, &mut runtime, &mut disk).unwrap();
                    txn.tick(2, &mut runtime, &mut disk).unwrap();
                    assert_eq!(txn.writer_state(), TopologyTxnState::PrepareSyncPending);
                }
                CrashEdge::Prepared => {
                    txn.tick(0, &mut runtime, &mut disk).unwrap();
                    txn.tick(2, &mut runtime, &mut disk).unwrap();
                    txn.tick(4, &mut runtime, &mut disk).unwrap();
                    assert_eq!(txn.writer_state(), TopologyTxnState::Prepared);
                }
                CrashEdge::PublishPutPending => {
                    txn.tick(0, &mut runtime, &mut disk).unwrap();
                    txn.tick(2, &mut runtime, &mut disk).unwrap();
                    txn.tick(4, &mut runtime, &mut disk).unwrap();
                    txn.tick(5, &mut runtime, &mut disk).unwrap();
                    assert_eq!(txn.writer_state(), TopologyTxnState::PublishPutPending);
                }
                CrashEdge::PublishSyncPending => {
                    txn.tick(0, &mut runtime, &mut disk).unwrap();
                    txn.tick(2, &mut runtime, &mut disk).unwrap();
                    txn.tick(4, &mut runtime, &mut disk).unwrap();
                    txn.tick(5, &mut runtime, &mut disk).unwrap();
                    txn.tick(7, &mut runtime, &mut disk).unwrap();
                    assert_eq!(txn.writer_state(), TopologyTxnState::PublishSyncPending);
                }
                CrashEdge::Complete => {
                    txn.tick(0, &mut runtime, &mut disk).unwrap();
                    txn.tick(2, &mut runtime, &mut disk).unwrap();
                    txn.tick(4, &mut runtime, &mut disk).unwrap();
                    txn.tick(5, &mut runtime, &mut disk).unwrap();
                    txn.tick(7, &mut runtime, &mut disk).unwrap();
                    txn.tick(9, &mut runtime, &mut disk).unwrap();
                    assert_eq!(txn.writer_state(), TopologyTxnState::Complete);
                }
            }

            disk.crash();
            drop(runtime);
            disk.set_delay(0);

            let mut recovered = recover_tablet_runtime(
                20,
                &mut disk,
                FailureDomainPolicy::HIERARCHICAL,
                budget(),
                8,
            )
            .unwrap();
            assert_eq!(
                recovered.runtime().lifecycle().map().generation(),
                expected_generation
            );
            assert_eq!(recovered.replayed_txn_id().is_some(), expect_replay);

            if expect_replay {
                assert_eq!(
                    recovered.tick_publication(21, &mut disk).unwrap(),
                    DurableResizeProgress::Complete
                );
                disk.crash();
                let stable = recover_tablet_runtime(
                    22,
                    &mut disk,
                    FailureDomainPolicy::HIERARCHICAL,
                    budget(),
                    8,
                )
                .unwrap();
                assert_eq!(stable.replayed_txn_id(), None);
                assert_eq!(stable.runtime().lifecycle().map().generation(), 1);
            }
        }
    }

    #[test]
    fn recovered_runtime_reconstructs_post_resize_migration_work() {
        let mut runtime = runtime();
        let plan = plan(&mut runtime);
        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);

        let mut txn = DurableResizeTransaction::new(104, &mut runtime, plan, 11).unwrap();
        txn.tick(1, &mut runtime, &mut disk).unwrap();
        disk.crash();

        let mut recovered =
            recover_tablet_runtime(2, &mut disk, FailureDomainPolicy::HIERARCHICAL, budget(), 8)
                .unwrap();

        for _ in 0..1_000 {
            if recovered.runtime().is_migration_converged() {
                break;
            }
            recovered.runtime_mut().tick().unwrap();
        }
        assert!(recovered.runtime().is_migration_converged());
        assert_eq!(recovered.runtime().migration().catalog_generation(), 1);
        assert_eq!(recovered.runtime().lifecycle().map().generation(), 1);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableTopologyChangeError {
    Runtime(TabletRuntimeError),
    Topology(TopologyTxnError),
    SnapshotMismatch,
}

impl From<TabletRuntimeError> for DurableTopologyChangeError {
    fn from(value: TabletRuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl From<TopologyTxnError> for DurableTopologyChangeError {
    fn from(value: TopologyTxnError) -> Self {
        Self::Topology(value)
    }
}

#[derive(Debug)]
pub struct DurableTopologyChangeTransaction {
    proposed_cluster: Cluster,
    target: TopologySnapshot,
    writer: DurableTopologyTxnWriter,
    applied: bool,
}

impl DurableTopologyChangeTransaction {
    pub fn new(
        txn_id: u64,
        runtime: &mut TabletRuntimeCoordinator,
        proposed_cluster: Cluster,
    ) -> Result<Self, DurableTopologyChangeError> {
        let current = runtime.capture_topology_snapshot()?;
        let target = current
            .with_cluster(&proposed_cluster)
            .map_err(TopologyTxnError::from)?;
        let writer = DurableTopologyTxnWriter::new(txn_id, &current, target.clone())?;
        Ok(Self {
            proposed_cluster,
            target,
            writer,
            applied: false,
        })
    }

    pub fn target_snapshot(&self) -> &TopologySnapshot {
        &self.target
    }

    pub fn writer_state(&self) -> TopologyTxnState {
        self.writer.state()
    }

    pub fn retry_storage(&mut self) {
        self.writer.retry();
    }

    pub fn is_complete(&self) -> bool {
        self.writer.is_complete()
    }

    pub fn tick<S: DurableStore>(
        &mut self,
        now_tick: u64,
        runtime: &mut TabletRuntimeCoordinator,
        store: &mut S,
    ) -> Result<DurableResizeProgress, DurableTopologyChangeError> {
        let entered_prepared = self.writer.state() == TopologyTxnState::Prepared;
        self.writer.tick(now_tick, store);

        if let TopologyTxnState::Failed { error, .. } = self.writer.state() {
            return Ok(DurableResizeProgress::StorageFailed(error));
        }

        if entered_prepared && !self.applied {
            runtime.reconcile_topology(self.proposed_cluster.clone())?;

            let applied = runtime.capture_topology_snapshot()?;
            if applied != self.target {
                return Err(DurableTopologyChangeError::SnapshotMismatch);
            }

            if !self.writer.mark_applied() {
                return Err(DurableTopologyChangeError::SnapshotMismatch);
            }
            self.applied = true;
            self.writer.tick(now_tick, store);

            if let TopologyTxnState::Failed { error, .. } = self.writer.state() {
                return Ok(DurableResizeProgress::StorageFailed(error));
            }
        }

        Ok(match self.writer.state() {
            TopologyTxnState::PrepareIdle
            | TopologyTxnState::PreparePutPending
            | TopologyTxnState::PrepareSyncPending => DurableResizeProgress::Preparing,
            TopologyTxnState::Prepared => DurableResizeProgress::Prepared,
            TopologyTxnState::PublishIdle
            | TopologyTxnState::PublishPutPending
            | TopologyTxnState::PublishSyncPending => DurableResizeProgress::Publishing,
            TopologyTxnState::Complete => DurableResizeProgress::Complete,
            TopologyTxnState::Failed { error, .. } => DurableResizeProgress::StorageFailed(error),
        })
    }
}

#[cfg(test)]
mod topology_change_tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::disk::{DiskRequest, DiskSubmit, SimDisk};
    use crate::model::{AdminState, Node};
    use crate::range_resize::{RangeTabletMap, TabletRangeLifecycle};

    fn node(id: u64, weight: u32, zone: &str, state: AdminState) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: "r1".into(),
            state,
        }
    }

    fn initial_cluster() -> Cluster {
        Cluster {
            epoch: 7,
            replication_factor: 2,
            nodes: [
                node(1, 1, "a", AdminState::Active),
                node(2, 2, "b", AdminState::Active),
                node(3, 4, "c", AdminState::Active),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: Vec::new(),
        }
    }

    fn next_cluster() -> Cluster {
        let mut cluster = initial_cluster();
        cluster.epoch = 8;
        cluster
            .nodes
            .insert(4, node(4, 16, "d", AdminState::Active));
        cluster
    }

    fn budget() -> MigrationBudget {
        MigrationBudget {
            max_active: 4,
            max_per_node_active: 4,
            bytes_per_tick: 10_000,
            max_bytes_per_node_per_tick: 10_000,
            max_bytes_per_task_per_tick: 10_000,
        }
    }

    fn runtime() -> TabletRuntimeCoordinator {
        let map = RangeTabletMap::single(10_000, 1_000, vec![1, 2]).unwrap();
        let lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let mut runtime = TabletRuntimeCoordinator::new(
            initial_cluster(),
            lifecycle,
            FailureDomainPolicy::HIERARCHICAL,
            budget(),
            8,
        )
        .unwrap();

        for _ in 0..1_000 {
            if runtime.is_migration_converged() {
                break;
            }
            runtime.tick().unwrap();
        }
        assert!(runtime.is_migration_converged());
        runtime
    }

    fn persist_current(disk: &mut SimDisk, snapshot: &TopologySnapshot) {
        assert!(matches!(
            disk.submit(
                0,
                DiskRequest::Put {
                    op_id: 70_000,
                    key: b"topology/current".to_vec(),
                    value: snapshot.encode().unwrap(),
                },
            ),
            DiskSubmit::Completed(_)
        ));
        assert!(matches!(
            disk.submit(0, DiskRequest::Sync { op_id: 70_001 }),
            DiskSubmit::Completed(_)
        ));
    }

    #[test]
    fn durable_topology_change_prepares_applies_and_publishes() {
        let mut runtime = runtime();
        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);

        let mut txn =
            DurableTopologyChangeTransaction::new(200, &mut runtime, next_cluster()).unwrap();

        assert_eq!(
            txn.tick(1, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Prepared
        );
        assert_eq!(runtime.migration().topology_epoch(), 7);
        assert!(!runtime.migration().cluster().nodes.contains_key(&4));

        assert_eq!(
            txn.tick(2, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Complete
        );
        assert_eq!(runtime.migration().topology_epoch(), 8);
        assert!(runtime.migration().cluster().nodes.contains_key(&4));
        assert_eq!(
            runtime.capture_topology_snapshot().unwrap(),
            *txn.target_snapshot()
        );

        disk.crash();
        let recovered =
            recover_tablet_runtime(3, &mut disk, FailureDomainPolicy::HIERARCHICAL, budget(), 8)
                .unwrap();
        assert_eq!(recovered.replayed_txn_id(), None);
        assert_eq!(recovered.runtime().migration().topology_epoch(), 8);
        assert!(
            recovered
                .runtime()
                .migration()
                .cluster()
                .nodes
                .contains_key(&4)
        );
    }

    #[test]
    fn crash_after_prepared_before_topology_apply_recovers_new_cluster() {
        let mut runtime = runtime();
        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);

        let mut txn =
            DurableTopologyChangeTransaction::new(201, &mut runtime, next_cluster()).unwrap();
        assert_eq!(
            txn.tick(1, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Prepared
        );
        assert_eq!(runtime.migration().topology_epoch(), 7);

        disk.crash();
        drop(runtime);

        let mut recovered =
            recover_tablet_runtime(2, &mut disk, FailureDomainPolicy::HIERARCHICAL, budget(), 8)
                .unwrap();

        assert_eq!(recovered.replayed_txn_id(), Some(201));
        assert_eq!(recovered.runtime().migration().topology_epoch(), 8);
        assert!(
            recovered
                .runtime()
                .migration()
                .cluster()
                .nodes
                .contains_key(&4)
        );
        assert!(!recovered.runtime().is_migration_converged());

        assert_eq!(
            recovered.tick_publication(3, &mut disk).unwrap(),
            DurableResizeProgress::Complete
        );

        for _ in 0..1_000 {
            if recovered.runtime().is_migration_converged() {
                break;
            }
            recovered.runtime_mut().tick().unwrap();
        }
        assert!(recovered.runtime().is_migration_converged());
    }

    #[test]
    fn same_or_older_epoch_topology_change_is_rejected_before_prepare() {
        let mut runtime = runtime();
        let before = runtime.capture_topology_snapshot().unwrap();

        let same = initial_cluster();
        assert!(matches!(
            DurableTopologyChangeTransaction::new(202, &mut runtime, same),
            Err(DurableTopologyChangeError::Topology(
                TopologyTxnError::InvalidTransition
            ))
        ));

        let mut older = initial_cluster();
        older.epoch = 6;
        assert!(matches!(
            DurableTopologyChangeTransaction::new(203, &mut runtime, older),
            Err(DurableTopologyChangeError::Topology(
                TopologyTxnError::InvalidTransition
            ))
        ));

        assert_eq!(runtime.capture_topology_snapshot().unwrap(), before);
    }

    #[test]
    fn membership_dead_is_only_evidence_until_durable_topology_commit() {
        use crate::membership::{MemberStatus, MembershipCluster, MembershipConfig};
        use crate::transport::MessageBusLimits;

        let mut runtime = runtime();
        let mut membership = MembershipCluster::new(
            &[1, 2, 3],
            MembershipConfig {
                probe_interval_ticks: 1,
                direct_timeout_ticks: 2,
                indirect_timeout_ticks: 4,
                suspicion_timeout_ticks: 8,
                indirect_checks: 2,
                max_awareness_score: 8,
                piggyback_updates: 8,
                update_retransmits: 16,
            },
            MessageBusLimits {
                max_buffered_messages: 10_000,
                max_buffered_bytes: 8 * 1024 * 1024,
            },
        )
        .unwrap();

        assert!(membership.crash(3));
        membership.run_ticks(80);
        assert!(membership.all_live_observers_see(3, MemberStatus::Dead));

        // Failure detection alone cannot mutate durable ownership authority.
        assert_eq!(runtime.migration().topology_epoch(), 7);
        assert_eq!(
            runtime.migration().cluster().nodes.get(&3).unwrap().state,
            AdminState::Active
        );

        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);

        let mut proposed = initial_cluster();
        proposed.epoch = 8;
        proposed.nodes.get_mut(&3).unwrap().state = AdminState::Removed;
        let mut txn = DurableTopologyChangeTransaction::new(205, &mut runtime, proposed).unwrap();

        assert_eq!(
            txn.tick(81, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Prepared
        );
        assert_eq!(runtime.migration().topology_epoch(), 7);

        assert_eq!(
            txn.tick(82, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Complete
        );
        assert_eq!(runtime.migration().topology_epoch(), 8);
        assert_eq!(
            runtime.migration().cluster().nodes.get(&3).unwrap().state,
            AdminState::Removed
        );
        assert!(!runtime.is_migration_converged());
    }

    #[test]
    fn disk_full_before_topology_prepare_cannot_advance_epoch() {
        let mut runtime = runtime();
        let current = runtime.capture_topology_snapshot().unwrap();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);
        disk.set_full(true);

        let mut txn =
            DurableTopologyChangeTransaction::new(204, &mut runtime, next_cluster()).unwrap();
        assert_eq!(
            txn.tick(1, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::StorageFailed(DiskError::Full)
        );
        assert_eq!(runtime.migration().topology_epoch(), 7);

        disk.set_full(false);
        txn.retry_storage();
        assert_eq!(
            txn.tick(2, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Prepared
        );
        assert_eq!(runtime.migration().topology_epoch(), 7);
        assert_eq!(
            txn.tick(3, &mut runtime, &mut disk).unwrap(),
            DurableResizeProgress::Complete
        );
        assert_eq!(runtime.migration().topology_epoch(), 8);
    }
}
