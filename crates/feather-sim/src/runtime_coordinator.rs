use crate::compact::{CompactPlacement, CompactPlacementError};
use crate::compact_catalog::{CompactCatalogError, CompactTabletCatalog};
use crate::compact_window::{
    CompactWindowError, CompactWindowReconcileReport, CompactWindowScheduler,
};
use crate::migration::{MigrationBudget, NodeHealth, TickReport};
use crate::model::{Cluster, NodeId};
use crate::placement::FailureDomainPolicy;
use crate::range_resize::{
    LifecycleResizeDecision, LifecycleResizeError, LifecycleResizePlan, RangeCommitOutcome,
    RangeResizeError, TabletRangeLifecycle,
};
use crate::resize::{ResizeBlockReason, ResizeKind, TabletResizePolicy};
use crate::topology_snapshot::TopologySnapshot;
use crate::transport::{DirectMigrationTransport, MigrationTransport};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoordinatedResizeDecision {
    NoChange,
    BlockedByMigration {
        catalog_generation: u64,
    },
    Blocked {
        kind: ResizeKind,
        reason: ResizeBlockReason,
    },
    Planned(LifecycleResizePlan),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatedResizeCommitOutcome {
    Applied {
        from_generation: u64,
        to_generation: u64,
        migration_required: bool,
    },
    AlreadyApplied,
    StaleTopology,
    StaleGeneration,
    BlockedByMigration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TabletRuntimeError {
    Compact(CompactPlacementError),
    Catalog(CompactCatalogError),
    Migration(CompactWindowError),
    Lifecycle(LifecycleResizeError),
    Range(RangeResizeError),
    ShapeMismatch,
    TopologyEpochMismatch { runtime: u64, requested: u64 },
}

impl From<CompactPlacementError> for TabletRuntimeError {
    fn from(value: CompactPlacementError) -> Self {
        Self::Compact(value)
    }
}

impl From<CompactCatalogError> for TabletRuntimeError {
    fn from(value: CompactCatalogError) -> Self {
        Self::Catalog(value)
    }
}

impl From<CompactWindowError> for TabletRuntimeError {
    fn from(value: CompactWindowError) -> Self {
        Self::Migration(value)
    }
}

impl From<LifecycleResizeError> for TabletRuntimeError {
    fn from(value: LifecycleResizeError) -> Self {
        Self::Lifecycle(value)
    }
}

impl From<RangeResizeError> for TabletRuntimeError {
    fn from(value: RangeResizeError) -> Self {
        Self::Range(value)
    }
}

#[derive(Debug)]
pub struct TabletRuntimeCoordinator {
    lifecycle: TabletRangeLifecycle,
    migration: CompactWindowScheduler,
    placement_policy: FailureDomainPolicy,
    migration_budget: MigrationBudget,
    window_tablets: usize,
}

impl TabletRuntimeCoordinator {
    pub fn new(
        cluster: Cluster,
        lifecycle: TabletRangeLifecycle,
        placement_policy: FailureDomainPolicy,
        migration_budget: MigrationBudget,
        window_tablets: usize,
    ) -> Result<Self, TabletRuntimeError> {
        let catalog = CompactTabletCatalog::from_range_map(lifecycle.map())?;
        let actual = CompactPlacement::from_range_map(lifecycle.map())?;
        let desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &cluster,
            &catalog,
            placement_policy,
        )?;
        let migration = CompactWindowScheduler::new(
            cluster,
            catalog,
            actual,
            desired,
            placement_policy,
            migration_budget,
            window_tablets,
        )?;

        Ok(Self {
            lifecycle,
            migration,
            placement_policy,
            migration_budget,
            window_tablets,
        })
    }

    pub fn lifecycle(&self) -> &TabletRangeLifecycle {
        &self.lifecycle
    }

    pub fn from_topology_snapshot(
        cluster: Cluster,
        snapshot: TopologySnapshot,
        placement_policy: FailureDomainPolicy,
        migration_budget: MigrationBudget,
        window_tablets: usize,
    ) -> Result<Self, TabletRuntimeError> {
        if cluster.epoch != snapshot.topology_epoch() {
            return Err(TabletRuntimeError::TopologyEpochMismatch {
                runtime: snapshot.topology_epoch(),
                requested: cluster.epoch,
            });
        }
        Self::new(
            cluster,
            snapshot.into_lifecycle(),
            placement_policy,
            migration_budget,
            window_tablets,
        )
    }

    pub fn capture_topology_snapshot(&mut self) -> Result<TopologySnapshot, TabletRuntimeError> {
        self.sync_committed_replicas_to_range_map()?;
        Ok(TopologySnapshot::from_lifecycle(
            self.migration.topology_epoch(),
            &self.lifecycle,
        ))
    }

    pub fn migration(&self) -> &CompactWindowScheduler {
        &self.migration
    }

    pub fn is_migration_converged(&self) -> bool {
        self.migration.is_converged()
    }

    pub fn tick(&mut self) -> Result<TickReport, TabletRuntimeError> {
        Ok(self.migration.tick()?)
    }

    pub fn tick_with_transport<T: MigrationTransport>(
        &mut self,
        now_tick: u64,
        transport: &mut T,
    ) -> Result<TickReport, TabletRuntimeError> {
        Ok(self.migration.tick_with_transport(now_tick, transport)?)
    }

    pub fn set_node_health(&mut self, node_id: NodeId, health: NodeHealth) -> bool {
        self.migration.set_node_health(node_id, health)
    }

    pub fn reconcile_topology(
        &mut self,
        cluster: Cluster,
    ) -> Result<CompactWindowReconcileReport, TabletRuntimeError> {
        let mut transport = DirectMigrationTransport;
        self.reconcile_topology_with_transport(cluster, &mut transport)
    }

    pub fn reconcile_topology_with_transport<T: MigrationTransport>(
        &mut self,
        cluster: Cluster,
        transport: &mut T,
    ) -> Result<CompactWindowReconcileReport, TabletRuntimeError> {
        let catalog = CompactTabletCatalog::from_range_map(self.lifecycle.map())?;
        if catalog.generation() != self.migration.catalog_generation() {
            return Err(TabletRuntimeError::ShapeMismatch);
        }
        let desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &cluster,
            &catalog,
            self.placement_policy,
        )?;
        Ok(self
            .migration
            .reconcile_desired_with_transport(cluster, desired, transport)?)
    }

    pub fn evaluate_resize(
        &mut self,
        total_table_bytes: u64,
        now_tick: u64,
        topology_epoch: u64,
        policy: &TabletResizePolicy,
    ) -> Result<CoordinatedResizeDecision, TabletRuntimeError> {
        self.ensure_topology_epoch(topology_epoch)?;

        if !self.migration.is_converged() {
            return Ok(CoordinatedResizeDecision::BlockedByMigration {
                catalog_generation: self.migration.catalog_generation(),
            });
        }

        self.sync_committed_replicas_to_range_map()?;

        Ok(
            match self
                .lifecycle
                .evaluate(total_table_bytes, now_tick, topology_epoch, policy)?
            {
                LifecycleResizeDecision::NoChange => CoordinatedResizeDecision::NoChange,
                LifecycleResizeDecision::Blocked { kind, reason } => {
                    CoordinatedResizeDecision::Blocked { kind, reason }
                }
                LifecycleResizeDecision::Planned(plan) => CoordinatedResizeDecision::Planned(plan),
            },
        )
    }

    pub fn commit_resize(
        &mut self,
        plan: &LifecycleResizePlan,
        current_topology_epoch: u64,
        now_tick: u64,
    ) -> Result<CoordinatedResizeCommitOutcome, TabletRuntimeError> {
        if plan.topology_epoch() != current_topology_epoch
            || self.migration.topology_epoch() != current_topology_epoch
        {
            return Ok(CoordinatedResizeCommitOutcome::StaleTopology);
        }

        let current_generation = self.lifecycle.map().generation();

        // Preserve control-plane retry semantics before applying the migration
        // barrier. A plan from an already-advanced generation cannot mutate
        // the range map, so it is safe to ask the lifecycle for the exact
        // AlreadyApplied vs StaleGeneration result even while ownership work
        // for the newer generation is active.
        if current_generation != plan.from_generation() {
            return Ok(
                match self
                    .lifecycle
                    .commit(plan, current_topology_epoch, now_tick)?
                {
                    RangeCommitOutcome::Applied => {
                        unreachable!("generation-mismatched resize plan must never apply")
                    }
                    RangeCommitOutcome::AlreadyApplied => {
                        CoordinatedResizeCommitOutcome::AlreadyApplied
                    }
                    RangeCommitOutcome::StaleTopology => {
                        CoordinatedResizeCommitOutcome::StaleTopology
                    }
                    RangeCommitOutcome::StaleGeneration => {
                        CoordinatedResizeCommitOutcome::StaleGeneration
                    }
                },
            );
        }

        if !self.migration.is_converged() {
            return Ok(CoordinatedResizeCommitOutcome::BlockedByMigration);
        }

        self.sync_committed_replicas_to_range_map()?;
        let from_generation = current_generation;
        let outcome = self
            .lifecycle
            .commit(plan, current_topology_epoch, now_tick)?;

        match outcome {
            RangeCommitOutcome::Applied => {
                self.rebuild_migration_from_range_map()?;
                let to_generation = self.lifecycle.map().generation();
                Ok(CoordinatedResizeCommitOutcome::Applied {
                    from_generation,
                    to_generation,
                    migration_required: !self.migration.is_converged(),
                })
            }
            RangeCommitOutcome::AlreadyApplied => {
                if self.migration.catalog_generation() != self.lifecycle.map().generation() {
                    self.rebuild_migration_from_range_map()?;
                }
                Ok(CoordinatedResizeCommitOutcome::AlreadyApplied)
            }
            RangeCommitOutcome::StaleTopology => Ok(CoordinatedResizeCommitOutcome::StaleTopology),
            RangeCommitOutcome::StaleGeneration => {
                Ok(CoordinatedResizeCommitOutcome::StaleGeneration)
            }
        }
    }

    fn ensure_topology_epoch(&self, requested: u64) -> Result<(), TabletRuntimeError> {
        let runtime = self.migration.topology_epoch();
        if requested != runtime {
            return Err(TabletRuntimeError::TopologyEpochMismatch { runtime, requested });
        }
        Ok(())
    }

    fn sync_committed_replicas_to_range_map(&mut self) -> Result<(), TabletRuntimeError> {
        let tablet_count = self.lifecycle.map().tablet_count();
        if tablet_count != self.migration.actual().tablet_count() as usize
            || self.lifecycle.map().generation() != self.migration.catalog_generation()
        {
            return Err(TabletRuntimeError::ShapeMismatch);
        }

        for slot in 0..tablet_count {
            let replicas = self
                .migration
                .actual()
                .replicas_by_slot(slot)
                .ok_or(TabletRuntimeError::ShapeMismatch)?
                .to_vec();
            self.lifecycle.replace_replicas_by_slot(slot, &replicas)?;
        }
        self.lifecycle.map().validate()?;
        Ok(())
    }

    fn rebuild_migration_from_range_map(&mut self) -> Result<(), TabletRuntimeError> {
        let cluster = self.migration.cluster().clone();
        let health = cluster
            .nodes
            .keys()
            .copied()
            .filter_map(|node_id| {
                self.migration
                    .node_health(node_id)
                    .map(|node_health| (node_id, node_health))
            })
            .collect::<Vec<_>>();

        let catalog = CompactTabletCatalog::from_range_map(self.lifecycle.map())?;
        let actual = CompactPlacement::from_range_map(self.lifecycle.map())?;
        let desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &cluster,
            &catalog,
            self.placement_policy,
        )?;
        let mut migration = CompactWindowScheduler::new(
            cluster,
            catalog,
            actual,
            desired,
            self.placement_policy,
            self.migration_budget,
            self.window_tablets,
        )?;
        for (node_id, node_health) in health {
            let _ = migration.set_node_health(node_id, node_health);
        }
        self.migration = migration;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::{AdminState, Node};
    use crate::range_resize::RangeTabletMap;

    fn node(id: u64, weight: u32, zone: &str, state: AdminState) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: "r1".into(),
            state,
        }
    }

    fn cluster(epoch: u64) -> Cluster {
        Cluster {
            epoch,
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

    fn resize_policy() -> TabletResizePolicy {
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

    fn coordinator() -> TabletRuntimeCoordinator {
        let map = RangeTabletMap::single(10_000, 1_000, vec![1, 2]).unwrap();
        let lifecycle = TabletRangeLifecycle::new(map).unwrap();
        TabletRuntimeCoordinator::new(
            cluster(7),
            lifecycle,
            FailureDomainPolicy::HIERARCHICAL,
            MigrationBudget {
                max_active: 4,
                max_per_node_active: 4,
                bytes_per_tick: 10_000,
                max_bytes_per_node_per_tick: 10_000,
                max_bytes_per_task_per_tick: 10_000,
            },
            8,
        )
        .unwrap()
    }

    fn converge(runtime: &mut TabletRuntimeCoordinator) {
        for _ in 0..10_000 {
            if runtime.is_migration_converged() {
                return;
            }
            runtime.tick().unwrap();
        }
        panic!("runtime did not converge");
    }

    #[test]
    fn resize_is_blocked_until_ownership_migration_converges() {
        let mut runtime = coordinator();
        assert!(!runtime.is_migration_converged());

        assert_eq!(
            runtime
                .evaluate_resize(1_000, 0, 7, &resize_policy())
                .unwrap(),
            CoordinatedResizeDecision::BlockedByMigration {
                catalog_generation: 0,
            }
        );

        converge(&mut runtime);
        assert!(matches!(
            runtime
                .evaluate_resize(1_000, 1, 7, &resize_policy())
                .unwrap(),
            CoordinatedResizeDecision::Planned(_)
        ));
    }

    #[test]
    fn resize_inherits_committed_actual_replicas_then_rebuilds_generation() {
        let mut runtime = coordinator();
        converge(&mut runtime);

        let parent_replicas = runtime
            .migration()
            .actual()
            .replicas_by_slot(0)
            .unwrap()
            .to_vec();
        let plan = match runtime
            .evaluate_resize(1_000, 10, 7, &resize_policy())
            .unwrap()
        {
            CoordinatedResizeDecision::Planned(plan) => plan,
            other => panic!("expected resize plan, got {other:?}"),
        };

        let outcome = runtime.commit_resize(&plan, 7, 10).unwrap();
        assert!(matches!(
            outcome,
            CoordinatedResizeCommitOutcome::Applied {
                from_generation: 0,
                to_generation: 1,
                ..
            }
        ));
        assert_eq!(runtime.lifecycle().map().generation(), 1);
        assert_eq!(runtime.migration().catalog_generation(), 1);
        assert_eq!(runtime.lifecycle().map().tablet_count(), 2);
        assert!(
            runtime
                .lifecycle()
                .map()
                .tablets()
                .iter()
                .all(|tablet| tablet.replicas == parent_replicas)
        );
    }

    #[test]
    fn stale_topology_plan_cannot_commit_after_topology_epoch_advances() {
        let mut runtime = coordinator();
        converge(&mut runtime);
        let plan = match runtime
            .evaluate_resize(1_000, 10, 7, &resize_policy())
            .unwrap()
        {
            CoordinatedResizeDecision::Planned(plan) => plan,
            other => panic!("expected resize plan, got {other:?}"),
        };

        let mut next_cluster = cluster(8);
        next_cluster
            .nodes
            .insert(5, node(5, 16, "e", AdminState::Active));
        runtime.reconcile_topology(next_cluster).unwrap();

        assert_eq!(
            runtime.commit_resize(&plan, 8, 11).unwrap(),
            CoordinatedResizeCommitOutcome::StaleTopology
        );
        assert_eq!(runtime.lifecycle().map().generation(), 0);
    }

    #[test]
    fn resize_commit_retry_remains_idempotent_even_with_post_resize_work() {
        let mut runtime = coordinator();
        converge(&mut runtime);

        let plan = match runtime
            .evaluate_resize(1_000, 10, 7, &resize_policy())
            .unwrap()
        {
            CoordinatedResizeDecision::Planned(plan) => plan,
            other => panic!("expected resize plan, got {other:?}"),
        };

        assert!(matches!(
            runtime.commit_resize(&plan, 7, 10).unwrap(),
            CoordinatedResizeCommitOutcome::Applied {
                from_generation: 0,
                to_generation: 1,
                ..
            }
        ));

        assert_eq!(
            runtime.commit_resize(&plan, 7, 11).unwrap(),
            CoordinatedResizeCommitOutcome::AlreadyApplied
        );
        assert_eq!(runtime.lifecycle().map().generation(), 1);
    }

    #[test]
    fn older_resize_plan_reports_stale_generation_after_later_resize() {
        let mut runtime = coordinator();
        converge(&mut runtime);

        let first = match runtime
            .evaluate_resize(1_000, 10, 7, &resize_policy())
            .unwrap()
        {
            CoordinatedResizeDecision::Planned(plan) => plan,
            other => panic!("expected first resize plan, got {other:?}"),
        };
        runtime.commit_resize(&first, 7, 10).unwrap();
        converge(&mut runtime);

        let second = match runtime
            .evaluate_resize(2_000, 20, 7, &resize_policy())
            .unwrap()
        {
            CoordinatedResizeDecision::Planned(plan) => plan,
            other => panic!("expected second resize plan, got {other:?}"),
        };
        runtime.commit_resize(&second, 7, 20).unwrap();

        assert_eq!(
            runtime.commit_resize(&first, 7, 21).unwrap(),
            CoordinatedResizeCommitOutcome::StaleGeneration
        );
        assert_eq!(runtime.lifecycle().map().generation(), 2);
    }

    #[test]
    fn post_resize_ownership_work_blocks_another_resize_until_converged() {
        let mut runtime = coordinator();
        converge(&mut runtime);

        let plan = match runtime
            .evaluate_resize(1_000, 10, 7, &resize_policy())
            .unwrap()
        {
            CoordinatedResizeDecision::Planned(plan) => plan,
            other => panic!("expected resize plan, got {other:?}"),
        };
        runtime.commit_resize(&plan, 7, 10).unwrap();

        if runtime.is_migration_converged() {
            let mut next_cluster = cluster(8);
            next_cluster
                .nodes
                .insert(5, node(5, 16, "e", AdminState::Active));
            runtime.reconcile_topology(next_cluster).unwrap();
        }

        assert!(!runtime.is_migration_converged());
        assert_eq!(
            runtime
                .evaluate_resize(
                    2_000,
                    11,
                    runtime.migration().topology_epoch(),
                    &resize_policy()
                )
                .unwrap(),
            CoordinatedResizeDecision::BlockedByMigration {
                catalog_generation: 1,
            }
        );

        converge(&mut runtime);
    }

    #[test]
    fn transient_node_health_survives_resize_scheduler_rebuild() {
        let mut runtime = coordinator();
        converge(&mut runtime);
        assert!(runtime.set_node_health(3, NodeHealth::Suspect));

        let plan = match runtime
            .evaluate_resize(1_000, 10, 7, &resize_policy())
            .unwrap()
        {
            CoordinatedResizeDecision::Planned(plan) => plan,
            other => panic!("expected resize plan, got {other:?}"),
        };
        runtime.commit_resize(&plan, 7, 10).unwrap();

        assert_eq!(
            runtime.migration().node_health(3),
            Some(NodeHealth::Suspect)
        );
    }

    #[test]
    fn runtime_rejects_resize_evaluation_for_wrong_topology_epoch() {
        let mut runtime = coordinator();
        assert_eq!(
            runtime.evaluate_resize(1_000, 0, 8, &resize_policy()),
            Err(TabletRuntimeError::TopologyEpochMismatch {
                runtime: 7,
                requested: 8,
            })
        );
    }
}
