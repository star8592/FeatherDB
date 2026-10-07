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
        })
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
