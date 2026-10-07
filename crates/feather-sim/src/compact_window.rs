use std::collections::BTreeMap;

use crate::compact::{CompactPlacement, CompactPlacementError};
use crate::compact_catalog::CompactTabletCatalog;
use crate::migration::{
    MigrationBudget, MigrationError, MigrationScheduler, MigrationTask, NodeHealth, TickReport,
    owner_can_stream,
};
use crate::model::{Cluster, NodeId, Placement, Tablet, TabletId};
use crate::placement::FailureDomainPolicy;
use crate::transport::{DirectMigrationTransport, MigrationTransport};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactWindowPhase {
    Repair,
    Rebalance,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompactWindowError {
    ZeroWindow,
    ShapeMismatch,
    MissingCatalogData(usize),
    StaleEpoch { current: u64, proposed: u64 },
    Compact(CompactPlacementError),
    Migration(MigrationError),
}

impl From<CompactPlacementError> for CompactWindowError {
    fn from(value: CompactPlacementError) -> Self {
        Self::Compact(value)
    }
}

impl From<MigrationError> for CompactWindowError {
    fn from(value: MigrationError) -> Self {
        Self::Migration(value)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactWindowReconcileReport {
    pub old_epoch: u64,
    pub new_epoch: u64,
    pub cancelled_tasks: usize,
    pub cancelled_inflight_transfers: usize,
}

#[derive(Debug)]
struct ActiveWindow {
    scheduler: MigrationScheduler,
    slots: Vec<(usize, TabletId)>,
}

#[derive(Debug)]
pub struct CompactWindowScheduler {
    cluster: Cluster,
    catalog: CompactTabletCatalog,
    policy: FailureDomainPolicy,
    budget: MigrationBudget,
    window_tablets: usize,
    actual: CompactPlacement,
    desired: CompactPlacement,
    health: BTreeMap<NodeId, NodeHealth>,
    phase: CompactWindowPhase,
    scan_slot: usize,
    active: Option<ActiveWindow>,
    peak_materialized_tasks: usize,
    peak_window_tablets: usize,
    total_windows_completed: usize,
    total_epoch_replacements: usize,
    total_cancelled_tasks: usize,
    total_cancelled_inflight_transfers: usize,
}

impl CompactWindowScheduler {
    pub fn new(
        mut cluster: Cluster,
        catalog: CompactTabletCatalog,
        actual: CompactPlacement,
        desired: CompactPlacement,
        policy: FailureDomainPolicy,
        budget: MigrationBudget,
        window_tablets: usize,
    ) -> Result<Self, CompactWindowError> {
        if window_tablets == 0 {
            return Err(CompactWindowError::ZeroWindow);
        }
        if actual.tablet_count() != desired.tablet_count()
            || actual.replica_count() != desired.replica_count()
            || actual.tablet_count() != catalog.tablet_count() as u64
        {
            return Err(CompactWindowError::ShapeMismatch);
        }

        // Per-window schedulers materialize only the tablet metadata they execute.
        cluster.tablets.clear();
        let health = cluster
            .nodes
            .keys()
            .copied()
            .map(|node_id| (node_id, NodeHealth::Healthy))
            .collect();

        Ok(Self {
            cluster,
            catalog,
            policy,
            budget,
            window_tablets,
            actual,
            desired,
            health,
            phase: CompactWindowPhase::Repair,
            scan_slot: 0,
            active: None,
            peak_materialized_tasks: 0,
            peak_window_tablets: 0,
            total_windows_completed: 0,
            total_epoch_replacements: 0,
            total_cancelled_tasks: 0,
            total_cancelled_inflight_transfers: 0,
        })
    }

    pub fn topology_epoch(&self) -> u64 {
        self.cluster.epoch
    }

    pub fn actual(&self) -> &CompactPlacement {
        &self.actual
    }

    pub fn desired(&self) -> &CompactPlacement {
        &self.desired
    }

    pub fn phase(&self) -> CompactWindowPhase {
        self.phase
    }

    pub fn is_converged(&self) -> bool {
        self.active.is_none() && self.actual == self.desired
    }

    pub fn peak_materialized_tasks(&self) -> usize {
        self.peak_materialized_tasks
    }

    pub fn peak_window_tablets(&self) -> usize {
        self.peak_window_tablets
    }

    pub fn total_windows_completed(&self) -> usize {
        self.total_windows_completed
    }

    pub fn total_epoch_replacements(&self) -> usize {
        self.total_epoch_replacements
    }

    pub fn total_cancelled_tasks(&self) -> usize {
        self.total_cancelled_tasks
    }

    pub fn total_cancelled_inflight_transfers(&self) -> usize {
        self.total_cancelled_inflight_transfers
    }

    pub fn active_tasks(&self) -> Option<&[MigrationTask]> {
        self.active.as_ref().map(|window| window.scheduler.tasks())
    }

    pub fn set_node_health(&mut self, node_id: NodeId, health: NodeHealth) -> bool {
        if !self.cluster.nodes.contains_key(&node_id) {
            return false;
        }
        self.health.insert(node_id, health);
        if let Some(active) = self.active.as_mut() {
            let _ = active.scheduler.set_node_health(node_id, health);
        }
        true
    }

    pub fn reconcile_desired_with_transport<T: MigrationTransport>(
        &mut self,
        mut cluster: Cluster,
        desired: CompactPlacement,
        transport: &mut T,
    ) -> Result<CompactWindowReconcileReport, CompactWindowError> {
        let old_epoch = self.cluster.epoch;
        if cluster.epoch <= old_epoch {
            return Err(CompactWindowError::StaleEpoch {
                current: old_epoch,
                proposed: cluster.epoch,
            });
        }
        if desired.tablet_count() != self.catalog.tablet_count() as u64
            || desired.tablet_count() != self.actual.tablet_count()
            || desired.replica_count() != self.actual.replica_count()
        {
            return Err(CompactWindowError::ShapeMismatch);
        }

        // Persist every ownership cutover already committed by the active
        // inner scheduler before discarding its reconstructible task state.
        self.sync_active_actual()?;

        let mut cancelled_tasks = 0_usize;
        let mut cancelled_inflight_transfers = 0_usize;
        if let Some(active) = self.active.as_mut() {
            cancelled_tasks = active
                .scheduler
                .tasks()
                .iter()
                .filter(|task| {
                    !matches!(
                        task.state,
                        crate::migration::MigrationState::Complete
                            | crate::migration::MigrationState::Stale
                    )
                })
                .count();
            cancelled_inflight_transfers = active.scheduler.cancel_outstanding_transfers(transport);
        }

        cluster.tablets.clear();
        self.health
            .retain(|node_id, _| cluster.nodes.contains_key(node_id));
        for node_id in cluster.nodes.keys() {
            self.health.entry(*node_id).or_insert(NodeHealth::Healthy);
        }

        self.cluster = cluster;
        self.desired = desired;
        self.active = None;
        self.phase = CompactWindowPhase::Repair;
        self.scan_slot = 0;
        self.total_epoch_replacements += 1;
        self.total_cancelled_tasks += cancelled_tasks;
        self.total_cancelled_inflight_transfers += cancelled_inflight_transfers;

        Ok(CompactWindowReconcileReport {
            old_epoch,
            new_epoch: self.cluster.epoch,
            cancelled_tasks,
            cancelled_inflight_transfers,
        })
    }

    pub fn tick(&mut self) -> Result<TickReport, CompactWindowError> {
        let mut transport = DirectMigrationTransport;
        self.tick_with_transport(0, &mut transport)
    }

    pub fn tick_with_transport<T: MigrationTransport>(
        &mut self,
        now_tick: u64,
        transport: &mut T,
    ) -> Result<TickReport, CompactWindowError> {
        if self.active.is_none() {
            self.load_next_window()?;
        }

        let Some(active) = self.active.as_mut() else {
            return Ok(TickReport::default());
        };

        let report = active.scheduler.tick_with_transport(now_tick, transport);

        self.sync_active_actual()?;

        let window_complete = self
            .active
            .as_ref()
            .is_some_and(|window| window.scheduler.is_converged());
        if window_complete {
            self.active = None;
            self.total_windows_completed += 1;
        }

        Ok(report)
    }

    fn sync_active_actual(&mut self) -> Result<(), CompactWindowError> {
        let Some(active) = self.active.as_ref() else {
            return Ok(());
        };

        for (slot, tablet_id) in &active.slots {
            let replicas = active
                .scheduler
                .actual()
                .replicas
                .get(tablet_id)
                .ok_or(CompactWindowError::MissingCatalogData(*slot))?;
            self.actual.set_replicas_by_slot(*slot, replicas)?;
        }
        Ok(())
    }

    fn load_next_window(&mut self) -> Result<(), CompactWindowError> {
        loop {
            match self.phase {
                CompactWindowPhase::Complete => return Ok(()),
                CompactWindowPhase::Repair | CompactWindowPhase::Rebalance => {}
            }

            let mut tablets = Vec::new();
            let mut slots = Vec::new();
            let mut actual_map = Placement::default();
            let mut desired_map = Placement::default();

            while self.scan_slot < self.catalog.tablet_count() && slots.len() < self.window_tablets
            {
                let slot = self.scan_slot;
                self.scan_slot += 1;

                let tablet_id = self
                    .catalog
                    .tablet_id(slot)
                    .ok_or(CompactWindowError::MissingCatalogData(slot))?;
                let bytes = self
                    .catalog
                    .bytes(slot)
                    .ok_or(CompactWindowError::MissingCatalogData(slot))?;
                let old = self
                    .actual
                    .replicas_by_slot(slot)
                    .ok_or(CompactWindowError::MissingCatalogData(slot))?;
                let final_desired = self
                    .desired
                    .replicas_by_slot(slot)
                    .ok_or(CompactWindowError::MissingCatalogData(slot))?;

                if old == final_desired {
                    continue;
                }

                let removed: Vec<_> = old
                    .iter()
                    .copied()
                    .filter(|node_id| !final_desired.contains(node_id))
                    .collect();
                let added: Vec<_> = final_desired
                    .iter()
                    .copied()
                    .filter(|node_id| !old.contains(node_id))
                    .collect();

                if removed.len() != added.len() {
                    return Err(CompactWindowError::ShapeMismatch);
                }

                let pairs: Vec<_> = removed.into_iter().zip(added).collect();
                let has_repair = pairs
                    .iter()
                    .any(|(owner, _)| !owner_can_stream(&self.cluster, *owner));

                let target = match self.phase {
                    CompactWindowPhase::Repair if has_repair => {
                        let mut intermediate = old.to_vec();
                        for (owner, to) in pairs
                            .iter()
                            .copied()
                            .filter(|(owner, _)| !owner_can_stream(&self.cluster, *owner))
                        {
                            let Some(replica) =
                                intermediate.iter_mut().find(|node_id| **node_id == owner)
                            else {
                                return Err(CompactWindowError::ShapeMismatch);
                            };
                            *replica = to;
                        }
                        intermediate.sort_unstable();
                        intermediate
                    }
                    CompactWindowPhase::Repair => continue,
                    CompactWindowPhase::Rebalance if !has_repair => final_desired.to_vec(),
                    CompactWindowPhase::Rebalance => continue,
                    CompactWindowPhase::Complete => unreachable!(),
                };

                if old == target.as_slice() {
                    continue;
                }

                tablets.push(Tablet {
                    id: tablet_id,
                    bytes,
                });
                slots.push((slot, tablet_id));
                actual_map.replicas.insert(tablet_id, old.to_vec());
                desired_map.replicas.insert(tablet_id, target);
            }

            if !slots.is_empty() {
                let mut window_cluster = self.cluster.clone();
                window_cluster.tablets = tablets;
                let mut scheduler = MigrationScheduler::new(
                    window_cluster,
                    self.policy,
                    actual_map,
                    desired_map,
                    self.budget,
                )?;

                for (node_id, health) in &self.health {
                    let _ = scheduler.set_node_health(*node_id, *health);
                }

                self.peak_materialized_tasks =
                    self.peak_materialized_tasks.max(scheduler.tasks().len());
                self.peak_window_tablets = self.peak_window_tablets.max(slots.len());
                self.active = Some(ActiveWindow { scheduler, slots });
                return Ok(());
            }

            if self.scan_slot >= self.catalog.tablet_count() {
                match self.phase {
                    CompactWindowPhase::Repair => {
                        self.phase = CompactWindowPhase::Rebalance;
                        self.scan_slot = 0;
                    }
                    CompactWindowPhase::Rebalance => {
                        self.phase = CompactWindowPhase::Complete;
                        self.scan_slot = 0;
                        return Ok(());
                    }
                    CompactWindowPhase::Complete => return Ok(()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::compact::CompactPlacement;
    use crate::compact_catalog::CompactTabletCatalog;
    use crate::migration::MigrationPriority;
    use crate::model::{AdminState, Node};
    use crate::range_resize::RangeTabletMap;
    use crate::transport::SimNetwork;

    fn node(id: u64, weight: u32, zone: &str, state: AdminState) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: "r1".into(),
            state,
        }
    }

    fn catalog(tablets_power: usize, replicas: Vec<NodeId>) -> CompactTabletCatalog {
        let mut map = RangeTabletMap::single(10_000, 1_000_000, replicas).unwrap();
        for _ in 0..tablets_power {
            let split = map.plan_split_all(9).unwrap();
            map.commit(&split, 9).unwrap();
        }
        CompactTabletCatalog::from_range_map(&map).unwrap()
    }

    fn rebalance_clusters() -> (Cluster, Cluster) {
        let before = Cluster {
            epoch: 1,
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
        };
        let mut after = before.clone();
        after.epoch = 2;
        after.nodes.insert(4, node(4, 8, "d", AdminState::Active));
        (before, after)
    }

    #[test]
    fn bounded_rebalance_converges_with_small_materialized_window() {
        let cat = catalog(10, vec![1, 2]);
        let (before_cluster, after_cluster) = rebalance_clusters();
        let actual = CompactPlacement::weighted_rendezvous_for_catalog(
            &before_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &after_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let mut scheduler = CompactWindowScheduler::new(
            after_cluster,
            cat,
            actual,
            desired.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            MigrationBudget {
                max_active: 4,
                max_per_node_active: 2,
                bytes_per_tick: 1_000_000,
                max_bytes_per_node_per_tick: 1_000_000,
                max_bytes_per_task_per_tick: 1_000_000,
            },
            16,
        )
        .unwrap();

        for _ in 0..10_000 {
            if scheduler.is_converged() {
                break;
            }
            scheduler.tick().unwrap();
        }

        assert!(scheduler.is_converged());
        assert_eq!(scheduler.actual(), &desired);
        assert!(scheduler.peak_window_tablets() <= 16);
        assert!(scheduler.peak_materialized_tasks() <= 16 * 2);
        assert!(scheduler.total_windows_completed() > 1);
    }

    #[test]
    fn forced_repair_runs_before_global_rebalance_phase() {
        let cat = catalog(7, vec![1, 2, 3]);
        let cluster = Cluster {
            epoch: 2,
            replication_factor: 3,
            nodes: [
                node(1, 1, "a", AdminState::Removed),
                node(2, 1, "b", AdminState::Active),
                node(3, 1, "c", AdminState::Active),
                node(4, 1, "d", AdminState::Active),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: Vec::new(),
        };

        let actual_map = {
            let mut map = RangeTabletMap::single(10_000, 1_000_000, vec![1, 2, 3]).unwrap();
            for _ in 0..7 {
                let split = map.plan_split_all(9).unwrap();
                map.commit(&split, 9).unwrap();
            }
            CompactPlacement::from_range_map(&map).unwrap()
        };
        let desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let mut scheduler = CompactWindowScheduler::new(
            cluster,
            cat,
            actual_map,
            desired.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            MigrationBudget {
                max_active: 2,
                max_per_node_active: 2,
                bytes_per_tick: 1_000_000,
                max_bytes_per_node_per_tick: 1_000_000,
                max_bytes_per_task_per_tick: 1_000_000,
            },
            8,
        )
        .unwrap();

        let _ = scheduler.tick().unwrap();
        assert_eq!(scheduler.phase(), CompactWindowPhase::Repair);
        assert!(
            scheduler
                .active_tasks()
                .unwrap_or(&[])
                .iter()
                .all(|task| task.priority == MigrationPriority::Repair)
        );

        for _ in 0..10_000 {
            if scheduler.is_converged() {
                break;
            }
            scheduler.tick().unwrap();
        }

        assert!(scheduler.is_converged());
        assert_eq!(scheduler.actual(), &desired);
    }

    #[test]
    fn mixed_topology_change_finishes_all_repairs_before_rebalance_phase() {
        let cat = CompactTabletCatalog::uniform(20_000, 512, 512).unwrap();
        let before_cluster = Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, 1, "a", AdminState::Active),
                node(2, 1, "b", AdminState::Active),
                node(3, 1, "c", AdminState::Active),
                node(4, 1, "d", AdminState::Active),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: Vec::new(),
        };
        let actual = CompactPlacement::weighted_rendezvous_for_catalog(
            &before_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        assert!(actual.replica_counts().get(&1).copied().unwrap_or(0) > 0);

        let mut after_cluster = before_cluster.clone();
        after_cluster.epoch = 2;
        after_cluster.nodes.get_mut(&1).unwrap().state = AdminState::Removed;
        after_cluster
            .nodes
            .insert(5, node(5, 8, "e", AdminState::Active));
        let desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &after_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let mut scheduler = CompactWindowScheduler::new(
            after_cluster,
            cat,
            actual,
            desired.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            MigrationBudget {
                max_active: 4,
                max_per_node_active: 4,
                bytes_per_tick: 1_000_000,
                max_bytes_per_node_per_tick: 1_000_000,
                max_bytes_per_task_per_tick: 1_000_000,
            },
            16,
        )
        .unwrap();

        let mut saw_rebalance_phase = false;
        for _ in 0..10_000 {
            if scheduler.is_converged() {
                break;
            }
            scheduler.tick().unwrap();

            match scheduler.phase() {
                CompactWindowPhase::Repair => {
                    if let Some(tasks) = scheduler.active_tasks() {
                        assert!(
                            tasks
                                .iter()
                                .all(|task| task.priority == MigrationPriority::Repair)
                        );
                    }
                }
                CompactWindowPhase::Rebalance | CompactWindowPhase::Complete => {
                    saw_rebalance_phase = true;
                    assert_eq!(
                        scheduler
                            .actual()
                            .replica_counts()
                            .get(&1)
                            .copied()
                            .unwrap_or(0),
                        0
                    );
                }
            }
        }

        assert!(saw_rebalance_phase);
        assert!(scheduler.is_converged());
        assert_eq!(scheduler.actual(), &desired);
    }

    #[test]
    fn partitioned_active_window_resumes_after_heal() {
        let cat = catalog(5, vec![1, 2, 3]);
        let cluster = Cluster {
            epoch: 2,
            replication_factor: 3,
            nodes: [
                node(1, 1, "a", AdminState::Removed),
                node(2, 1, "b", AdminState::Active),
                node(3, 1, "c", AdminState::Active),
                node(4, 1, "d", AdminState::Active),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: Vec::new(),
        };
        let actual_map = {
            let mut map = RangeTabletMap::single(10_000, 1_000_000, vec![1, 2, 3]).unwrap();
            for _ in 0..5 {
                let split = map.plan_split_all(9).unwrap();
                map.commit(&split, 9).unwrap();
            }
            CompactPlacement::from_range_map(&map).unwrap()
        };
        let desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let mut scheduler = CompactWindowScheduler::new(
            cluster,
            cat,
            actual_map,
            desired.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            MigrationBudget {
                max_active: 1,
                max_per_node_active: 1,
                bytes_per_tick: 1_000_000,
                max_bytes_per_node_per_tick: 1_000_000,
                max_bytes_per_task_per_tick: 1_000_000,
            },
            4,
        )
        .unwrap();

        let mut network = SimNetwork::default();
        network.partition(2, 4, false);

        for tick in 0..4 {
            scheduler.tick_with_transport(tick, &mut network).unwrap();
        }
        assert!(!scheduler.is_converged());

        network.heal(2, 4, false);
        for tick in 4..10_000 {
            if scheduler.is_converged() {
                break;
            }
            scheduler.tick_with_transport(tick, &mut network).unwrap();
        }

        assert!(scheduler.is_converged());
        assert_eq!(scheduler.actual(), &desired);
    }

    #[test]
    fn stale_or_equal_epoch_replacement_is_rejected_without_mutation() {
        let cat = catalog(5, vec![1, 2]);
        let (before_cluster, after_cluster) = rebalance_clusters();
        let actual = CompactPlacement::weighted_rendezvous_for_catalog(
            &before_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &after_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let mut scheduler = CompactWindowScheduler::new(
            after_cluster.clone(),
            cat,
            actual.clone(),
            desired.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            MigrationBudget::conservative(),
            8,
        )
        .unwrap();
        let before_actual = scheduler.actual().clone();
        let before_desired = scheduler.desired().clone();
        let mut transport = DirectMigrationTransport;

        assert_eq!(
            scheduler.reconcile_desired_with_transport(after_cluster, desired, &mut transport,),
            Err(CompactWindowError::StaleEpoch {
                current: 2,
                proposed: 2,
            })
        );
        assert_eq!(scheduler.actual(), &before_actual);
        assert_eq!(scheduler.desired(), &before_desired);
        assert_eq!(scheduler.total_epoch_replacements(), 0);
    }

    #[test]
    fn higher_epoch_cancels_old_inflight_window_and_replans_from_actual() {
        let cat = CompactTabletCatalog::uniform(30_000, 32, 3_200).unwrap();
        let old_cluster = Cluster {
            epoch: 2,
            replication_factor: 3,
            nodes: [
                node(1, 1, "a", AdminState::Removed),
                node(2, 1, "b", AdminState::Active),
                node(3, 1, "c", AdminState::Active),
                node(4, 1, "d", AdminState::Active),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: Vec::new(),
        };

        let actual_map = {
            let mut map = RangeTabletMap::single(30_000, 3_200, vec![1, 2, 3]).unwrap();
            for _ in 0..5 {
                let split = map.plan_split_all(9).unwrap();
                map.commit(&split, 9).unwrap();
            }
            CompactPlacement::from_range_map(&map).unwrap()
        };
        let old_desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &old_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let mut scheduler = CompactWindowScheduler::new(
            old_cluster.clone(),
            cat.clone(),
            actual_map,
            old_desired,
            FailureDomainPolicy::HIERARCHICAL,
            MigrationBudget {
                max_active: 1,
                max_per_node_active: 1,
                bytes_per_tick: 100,
                max_bytes_per_node_per_tick: 100,
                max_bytes_per_task_per_tick: 100,
            },
            4,
        )
        .unwrap();

        let mut network = SimNetwork::default();
        network.set_delay(2, 4, 20);
        let first = scheduler.tick_with_transport(0, &mut network).unwrap();
        assert_eq!(first.bytes_copied, 0);
        assert_eq!(network.in_flight_count(), 1);
        assert_eq!(scheduler.topology_epoch(), 2);

        let actual_before_reconcile = scheduler.actual().clone();

        let mut next_cluster = old_cluster;
        next_cluster.epoch = 3;
        next_cluster.nodes.get_mut(&4).unwrap().state = AdminState::Removed;
        next_cluster
            .nodes
            .insert(5, node(5, 1, "e", AdminState::Active));
        let next_desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &next_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let report = scheduler
            .reconcile_desired_with_transport(next_cluster, next_desired.clone(), &mut network)
            .unwrap();

        assert_eq!(report.old_epoch, 2);
        assert_eq!(report.new_epoch, 3);
        assert_eq!(report.cancelled_inflight_transfers, 1);
        assert!(report.cancelled_tasks >= 1);
        assert_eq!(network.in_flight_count(), 0);
        assert_eq!(scheduler.topology_epoch(), 3);
        assert_eq!(scheduler.actual(), &actual_before_reconcile);
        assert_eq!(scheduler.desired(), &next_desired);
        assert!(scheduler.active_tasks().is_none());

        for tick in 1..10_000 {
            if scheduler.is_converged() {
                break;
            }
            scheduler.tick_with_transport(tick, &mut network).unwrap();
        }

        assert!(scheduler.is_converged());
        assert_eq!(scheduler.actual(), &next_desired);
        assert_eq!(scheduler.total_epoch_replacements(), 1);
        assert_eq!(scheduler.total_cancelled_inflight_transfers(), 1);
        assert_eq!(
            scheduler
                .actual()
                .replica_counts()
                .get(&4)
                .copied()
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn restart_reconstructs_window_from_compact_durable_maps() {
        let cat = catalog(8, vec![1, 2]);
        let (before_cluster, after_cluster) = rebalance_clusters();
        let actual = CompactPlacement::weighted_rendezvous_for_catalog(
            &before_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let desired = CompactPlacement::weighted_rendezvous_for_catalog(
            &after_cluster,
            &cat,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let budget = MigrationBudget {
            max_active: 1,
            max_per_node_active: 1,
            bytes_per_tick: 32,
            max_bytes_per_node_per_tick: 32,
            max_bytes_per_task_per_tick: 32,
        };

        let mut first = CompactWindowScheduler::new(
            after_cluster.clone(),
            cat.clone(),
            actual,
            desired.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            budget,
            8,
        )
        .unwrap();

        for _ in 0..5 {
            first.tick().unwrap();
        }
        let durable_actual = first.actual().clone();

        let mut restarted = CompactWindowScheduler::new(
            after_cluster,
            cat,
            durable_actual,
            desired.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            budget,
            8,
        )
        .unwrap();

        for _ in 0..100_000 {
            if restarted.is_converged() {
                break;
            }
            restarted.tick().unwrap();
        }

        assert!(restarted.is_converged());
        assert_eq!(restarted.actual(), &desired);
        assert!(restarted.peak_materialized_tasks() <= 8 * 2);
    }
}
