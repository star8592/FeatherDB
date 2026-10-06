use std::collections::{BTreeMap, BTreeSet};

use crate::model::{AdminState, Cluster, NodeId, Placement, TabletId};
use crate::placement::FailureDomainPolicy;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum MigrationPriority {
    Repair,
    Rebalance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationState {
    Pending,
    Copying,
    ReadyToCutover,
    Complete,
    Stale,
}

#[derive(Clone, Debug)]
pub struct MigrationTask {
    pub id: u64,
    pub epoch: u64,
    pub tablet_id: TabletId,
    pub copy_source: NodeId,
    pub owner_to_replace: NodeId,
    pub to: NodeId,
    pub bytes_total: u64,
    pub bytes_remaining: u64,
    pub priority: MigrationPriority,
    pub state: MigrationState,
}

#[derive(Clone, Copy, Debug)]
pub struct MigrationBudget {
    pub max_active: usize,
    pub max_per_node_active: usize,
    pub bytes_per_tick: u64,
    pub max_bytes_per_node_per_tick: u64,
    pub max_bytes_per_task_per_tick: u64,
}

impl MigrationBudget {
    pub const fn conservative() -> Self {
        Self {
            max_active: 2,
            max_per_node_active: 1,
            bytes_per_tick: 8 * 1024 * 1024,
            max_bytes_per_node_per_tick: 4 * 1024 * 1024,
            max_bytes_per_task_per_tick: 4 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TickReport {
    pub started: usize,
    pub completed: usize,
    pub stale: usize,
    pub bytes_copied: u64,
    pub active: usize,
    pub queued: usize,
    pub ready_to_cutover: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationError {
    MissingActualTablet(TabletId),
    MissingDesiredTablet(TabletId),
    ReplicaCountMismatch(TabletId),
    UnsafeDesiredPlacement(TabletId),
    NoRepairSource(TabletId),
}

#[derive(Clone, Debug)]
pub struct MigrationScheduler {
    cluster: Cluster,
    policy: FailureDomainPolicy,
    budget: MigrationBudget,
    actual: Placement,
    desired: Placement,
    tasks: Vec<MigrationTask>,
    next_task_id: u64,
    pub total_completed: usize,
    pub total_cancelled: usize,
}

impl MigrationScheduler {
    pub fn new(
        cluster: Cluster,
        policy: FailureDomainPolicy,
        actual: Placement,
        desired: Placement,
        budget: MigrationBudget,
    ) -> Result<Self, MigrationError> {
        let mut scheduler = Self {
            cluster,
            policy,
            budget,
            actual,
            desired,
            tasks: Vec::new(),
            next_task_id: 1,
            total_completed: 0,
            total_cancelled: 0,
        };
        scheduler.rebuild_tasks()?;
        Ok(scheduler)
    }

    pub fn actual(&self) -> &Placement {
        &self.actual
    }

    pub fn desired(&self) -> &Placement {
        &self.desired
    }

    pub fn tasks(&self) -> &[MigrationTask] {
        &self.tasks
    }

    pub fn is_converged(&self) -> bool {
        self.actual == self.desired
            && self
                .tasks
                .iter()
                .all(|task| matches!(task.state, MigrationState::Complete | MigrationState::Stale))
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.tasks
            .iter()
            .filter(|task| {
                matches!(
                    task.state,
                    MigrationState::Pending
                        | MigrationState::Copying
                        | MigrationState::ReadyToCutover
                )
            })
            .map(|task| task.bytes_remaining)
            .sum()
    }

    pub fn reconcile_desired(
        &mut self,
        cluster: Cluster,
        desired: Placement,
    ) -> Result<(), MigrationError> {
        self.total_cancelled += self
            .tasks
            .iter()
            .filter(|task| !matches!(task.state, MigrationState::Complete | MigrationState::Stale))
            .count();

        self.cluster = cluster;
        self.desired = desired;
        self.tasks.clear();
        self.rebuild_tasks()
    }

    pub fn tick(&mut self) -> TickReport {
        let mut report = TickReport::default();

        self.try_ready_cutovers(&mut report);

        let mut active_count = self.active_task_count();
        let mut per_node = self.active_per_node();
        let mut bytes_left = self.budget.bytes_per_tick;
        let mut bytes_by_node = BTreeMap::<NodeId, u64>::new();

        for index in 0..self.tasks.len() {
            if bytes_left == 0 {
                break;
            }

            if self.tasks[index].epoch != self.cluster.epoch {
                if !matches!(
                    self.tasks[index].state,
                    MigrationState::Complete | MigrationState::Stale
                ) {
                    self.tasks[index].state = MigrationState::Stale;
                    report.stale += 1;
                }
                continue;
            }

            if self.tasks[index].state == MigrationState::Pending {
                let copy_source = self.tasks[index].copy_source;
                let to = self.tasks[index].to;

                if active_count >= self.budget.max_active
                    || per_node.get(&copy_source).copied().unwrap_or(0)
                        >= self.budget.max_per_node_active
                    || per_node.get(&to).copied().unwrap_or(0) >= self.budget.max_per_node_active
                    || !source_readable(&self.cluster, copy_source)
                    || !target_writable(&self.cluster, to)
                {
                    continue;
                }

                self.tasks[index].state = MigrationState::Copying;
                active_count += 1;
                *per_node.entry(copy_source).or_default() += 1;
                *per_node.entry(to).or_default() += 1;
                report.started += 1;
            }

            if self.tasks[index].state != MigrationState::Copying {
                continue;
            }

            let copy_source = self.tasks[index].copy_source;
            let to = self.tasks[index].to;
            let from_left = self
                .budget
                .max_bytes_per_node_per_tick
                .saturating_sub(bytes_by_node.get(&copy_source).copied().unwrap_or(0));
            let to_left = self
                .budget
                .max_bytes_per_node_per_tick
                .saturating_sub(bytes_by_node.get(&to).copied().unwrap_or(0));

            let amount = self.tasks[index]
                .bytes_remaining
                .min(bytes_left)
                .min(from_left)
                .min(to_left)
                .min(self.budget.max_bytes_per_task_per_tick);

            if amount == 0 {
                continue;
            }

            self.tasks[index].bytes_remaining -= amount;
            bytes_left -= amount;
            *bytes_by_node.entry(copy_source).or_default() += amount;
            *bytes_by_node.entry(to).or_default() += amount;
            report.bytes_copied += amount;

            if self.tasks[index].bytes_remaining == 0 {
                self.tasks[index].state = MigrationState::ReadyToCutover;
                if self.try_cutover(index) {
                    report.completed += 1;
                    self.total_completed += 1;
                    active_count = active_count.saturating_sub(1);
                    decrement_node(&mut per_node, self.tasks[index].copy_source);
                    decrement_node(&mut per_node, self.tasks[index].to);
                }
            }
        }

        self.try_ready_cutovers(&mut report);

        report.active = self.active_task_count();
        report.queued = self
            .tasks
            .iter()
            .filter(|task| task.state == MigrationState::Pending)
            .count();
        report.ready_to_cutover = self
            .tasks
            .iter()
            .filter(|task| task.state == MigrationState::ReadyToCutover)
            .count();
        report
    }

    fn rebuild_tasks(&mut self) -> Result<(), MigrationError> {
        let mut tasks = Vec::new();

        for tablet in &self.cluster.tablets {
            let old = self
                .actual
                .replicas
                .get(&tablet.id)
                .ok_or(MigrationError::MissingActualTablet(tablet.id))?;
            let new = self
                .desired
                .replicas
                .get(&tablet.id)
                .ok_or(MigrationError::MissingDesiredTablet(tablet.id))?;

            if old.len() != new.len() {
                return Err(MigrationError::ReplicaCountMismatch(tablet.id));
            }
            if !replica_set_safe(&self.cluster, new, self.policy) {
                return Err(MigrationError::UnsafeDesiredPlacement(tablet.id));
            }

            let mut removed: Vec<_> = old
                .iter()
                .copied()
                .filter(|node_id| !new.contains(node_id))
                .collect();
            let mut added: Vec<_> = new
                .iter()
                .copied()
                .filter(|node_id| !old.contains(node_id))
                .collect();
            removed.sort_unstable();
            added.sort_unstable();

            if removed.len() != added.len() {
                return Err(MigrationError::ReplicaCountMismatch(tablet.id));
            }

            for (owner_to_replace, to) in removed.into_iter().zip(added) {
                let (copy_source, priority) = if source_readable(&self.cluster, owner_to_replace) {
                    (owner_to_replace, MigrationPriority::Rebalance)
                } else {
                    (
                        choose_repair_source(&self.cluster, old, owner_to_replace)
                            .ok_or(MigrationError::NoRepairSource(tablet.id))?,
                        MigrationPriority::Repair,
                    )
                };

                tasks.push(MigrationTask {
                    id: self.next_task_id,
                    epoch: self.cluster.epoch,
                    tablet_id: tablet.id,
                    copy_source,
                    owner_to_replace,
                    to,
                    bytes_total: tablet.bytes,
                    bytes_remaining: tablet.bytes,
                    priority,
                    state: MigrationState::Pending,
                });
                self.next_task_id += 1;
            }
        }

        tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| a.tablet_id.cmp(&b.tablet_id))
                .then_with(|| a.owner_to_replace.cmp(&b.owner_to_replace))
                .then_with(|| a.copy_source.cmp(&b.copy_source))
                .then_with(|| a.to.cmp(&b.to))
        });

        self.tasks = tasks;
        Ok(())
    }

    fn try_ready_cutovers(&mut self, report: &mut TickReport) {
        for index in 0..self.tasks.len() {
            if self.tasks[index].state == MigrationState::ReadyToCutover && self.try_cutover(index)
            {
                report.completed += 1;
                self.total_completed += 1;
            }
        }
    }

    fn try_cutover(&mut self, index: usize) -> bool {
        let task = &self.tasks[index];
        if task.epoch != self.cluster.epoch {
            self.tasks[index].state = MigrationState::Stale;
            return false;
        }

        let Some(desired) = self.desired.replicas.get(&task.tablet_id) else {
            self.tasks[index].state = MigrationState::Stale;
            return false;
        };

        if !desired.contains(&task.to) || desired.contains(&task.owner_to_replace) {
            self.tasks[index].state = MigrationState::Stale;
            return false;
        }

        let Some(current) = self.actual.replicas.get(&task.tablet_id).cloned() else {
            self.tasks[index].state = MigrationState::Stale;
            return false;
        };

        if !current.contains(&task.owner_to_replace) || current.contains(&task.to) {
            if current == *desired {
                self.tasks[index].state = MigrationState::Complete;
                return true;
            }
            return false;
        }

        let mut candidate = current.clone();
        let Some(slot) = candidate
            .iter_mut()
            .find(|node_id| **node_id == task.owner_to_replace)
        else {
            return false;
        };
        *slot = task.to;
        candidate.sort_unstable();

        let safe = match task.priority {
            MigrationPriority::Repair => {
                repair_cutover_safe(&self.cluster, &current, &candidate, task.to, self.policy)
            }
            MigrationPriority::Rebalance => {
                replica_set_safe(&self.cluster, &candidate, self.policy)
            }
        };
        if !safe {
            return false;
        }

        self.actual.replicas.insert(task.tablet_id, candidate);
        self.tasks[index].state = MigrationState::Complete;
        true
    }

    fn active_task_count(&self) -> usize {
        self.tasks
            .iter()
            .filter(|task| {
                matches!(
                    task.state,
                    MigrationState::Copying | MigrationState::ReadyToCutover
                )
            })
            .count()
    }

    fn active_per_node(&self) -> BTreeMap<NodeId, usize> {
        let mut result = BTreeMap::new();
        for task in &self.tasks {
            if matches!(
                task.state,
                MigrationState::Copying | MigrationState::ReadyToCutover
            ) {
                *result.entry(task.copy_source).or_default() += 1;
                *result.entry(task.to).or_default() += 1;
            }
        }
        result
    }
}

fn choose_repair_source(
    cluster: &Cluster,
    replicas: &[NodeId],
    owner_to_replace: NodeId,
) -> Option<NodeId> {
    let mut candidates: Vec<_> = replicas
        .iter()
        .copied()
        .filter(|node_id| *node_id != owner_to_replace)
        .filter(|node_id| source_readable(cluster, *node_id))
        .collect();

    candidates.sort_by_key(|node_id| {
        let state_rank = match cluster.nodes[node_id].state {
            AdminState::Active => 0_u8,
            AdminState::Draining => 1_u8,
            _ => 2_u8,
        };
        (state_rank, *node_id)
    });
    candidates.into_iter().next()
}

fn repair_cutover_safe(
    cluster: &Cluster,
    current: &[NodeId],
    candidate: &[NodeId],
    target: NodeId,
    policy: FailureDomainPolicy,
) -> bool {
    if !target_writable(cluster, target)
        || candidate.iter().collect::<BTreeSet<_>>().len() != candidate.len()
    {
        return false;
    }

    let current_available = current
        .iter()
        .filter(|node_id| source_readable(cluster, **node_id))
        .count();
    let candidate_available = candidate
        .iter()
        .filter(|node_id| source_readable(cluster, **node_id))
        .count();

    if candidate_available <= current_available {
        return false;
    }

    if policy.distinct_zones {
        let before = current
            .iter()
            .filter(|node_id| source_readable(cluster, **node_id))
            .map(|node_id| cluster.nodes[node_id].zone.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        let after = candidate
            .iter()
            .filter(|node_id| source_readable(cluster, **node_id))
            .map(|node_id| cluster.nodes[node_id].zone.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        if after < before {
            return false;
        }
    }

    if policy.distinct_racks {
        let before = current
            .iter()
            .filter(|node_id| source_readable(cluster, **node_id))
            .map(|node_id| {
                let node = &cluster.nodes[node_id];
                (node.zone.as_str(), node.rack.as_str())
            })
            .collect::<BTreeSet<_>>()
            .len();
        let after = candidate
            .iter()
            .filter(|node_id| source_readable(cluster, **node_id))
            .map(|node_id| {
                let node = &cluster.nodes[node_id];
                (node.zone.as_str(), node.rack.as_str())
            })
            .collect::<BTreeSet<_>>()
            .len();
        if after < before {
            return false;
        }
    }

    true
}

fn decrement_node(counts: &mut BTreeMap<NodeId, usize>, node_id: NodeId) {
    if let Some(count) = counts.get_mut(&node_id) {
        *count = count.saturating_sub(1);
    }
}

fn source_readable(cluster: &Cluster, node_id: NodeId) -> bool {
    cluster
        .nodes
        .get(&node_id)
        .is_some_and(|node| matches!(node.state, AdminState::Active | AdminState::Draining))
}

fn target_writable(cluster: &Cluster, node_id: NodeId) -> bool {
    cluster
        .nodes
        .get(&node_id)
        .is_some_and(|node| node.state == AdminState::Active)
}

fn replica_set_safe(cluster: &Cluster, replicas: &[NodeId], policy: FailureDomainPolicy) -> bool {
    if replicas.iter().collect::<BTreeSet<_>>().len() != replicas.len() {
        return false;
    }

    if replicas
        .iter()
        .any(|node_id| !target_writable(cluster, *node_id))
    {
        return false;
    }

    if policy.distinct_zones {
        let eligible_zones = cluster
            .nodes
            .values()
            .filter(|node| node.state == AdminState::Active)
            .map(|node| node.zone.as_str())
            .collect::<BTreeSet<_>>()
            .len();

        if eligible_zones >= replicas.len() {
            let zones = replicas
                .iter()
                .map(|node_id| cluster.nodes[node_id].zone.as_str())
                .collect::<BTreeSet<_>>();
            if zones.len() != replicas.len() {
                return false;
            }
        }
    }

    if policy.distinct_racks {
        let eligible_racks = cluster
            .nodes
            .values()
            .filter(|node| node.state == AdminState::Active)
            .map(|node| (node.zone.as_str(), node.rack.as_str()))
            .collect::<BTreeSet<_>>()
            .len();

        if eligible_racks >= replicas.len() {
            let racks = replicas
                .iter()
                .map(|node_id| {
                    let node = &cluster.nodes[node_id];
                    (node.zone.as_str(), node.rack.as_str())
                })
                .collect::<BTreeSet<_>>();
            if racks.len() != replicas.len() {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::{Node, Tablet};
    use crate::placement::PlacementStrategy;
    use crate::planner::plan_rebalance;

    fn node(id: u64, state: AdminState, zone: &str) -> Node {
        Node {
            id,
            weight: 1,
            zone: zone.into(),
            rack: "r1".into(),
            state,
        }
    }

    fn one_tablet_cluster(bytes: u64) -> Cluster {
        Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, AdminState::Active, "a"),
                node(2, AdminState::Active, "b"),
                node(3, AdminState::Active, "c"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![Tablet { id: 1, bytes }],
        }
    }

    #[test]
    fn ownership_changes_only_after_copy_finishes() {
        let cluster = one_tablet_cluster(100);
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![1, 3])]),
        };
        let budget = MigrationBudget {
            max_active: 1,
            max_per_node_active: 1,
            bytes_per_tick: 40,
            max_bytes_per_node_per_tick: 40,
            max_bytes_per_task_per_tick: 40,
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual.clone(),
            desired.clone(),
            budget,
        )
        .unwrap();

        scheduler.tick();
        assert_eq!(scheduler.actual(), &actual);
        scheduler.tick();
        assert_eq!(scheduler.actual(), &actual);
        scheduler.tick();
        assert_eq!(scheduler.actual(), &desired);
        assert!(scheduler.is_converged());
    }

    #[test]
    fn per_node_concurrency_limit_is_enforced() {
        let cluster = Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, AdminState::Active, "a"),
                node(2, AdminState::Active, "b"),
                node(3, AdminState::Active, "c"),
                node(4, AdminState::Active, "d"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![Tablet { id: 1, bytes: 100 }, Tablet { id: 2, bytes: 100 }],
        };
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2]), (2, vec![1, 2])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![2, 3]), (2, vec![2, 4])]),
        };
        let budget = MigrationBudget {
            max_active: 2,
            max_per_node_active: 1,
            bytes_per_tick: 100,
            max_bytes_per_node_per_tick: 100,
            max_bytes_per_task_per_tick: 50,
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired,
            budget,
        )
        .unwrap();

        let report = scheduler.tick();
        assert_eq!(report.started, 1);
        assert_eq!(report.active, 1);
    }

    #[test]
    fn removed_owner_is_never_used_as_copy_source() {
        let mut cluster = one_tablet_cluster(100);
        cluster.nodes.get_mut(&1).unwrap().state = AdminState::Removed;
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![2, 3])]),
        };
        let budget = MigrationBudget {
            max_active: 1,
            max_per_node_active: 1,
            bytes_per_tick: 100,
            max_bytes_per_node_per_tick: 100,
            max_bytes_per_task_per_tick: 100,
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired.clone(),
            budget,
        )
        .unwrap();

        let task = &scheduler.tasks()[0];
        assert_eq!(task.priority, MigrationPriority::Repair);
        assert_eq!(task.owner_to_replace, 1);
        assert_eq!(task.copy_source, 2);

        let report = scheduler.tick();
        assert_eq!(report.started, 1);
        assert_eq!(scheduler.actual(), &desired);
        assert!(scheduler.is_converged());
    }

    #[test]
    fn repair_uses_surviving_replica_for_removed_owner() {
        let cluster = Cluster {
            epoch: 1,
            replication_factor: 3,
            nodes: [
                node(1, AdminState::Removed, "a"),
                node(2, AdminState::Active, "b"),
                node(3, AdminState::Active, "c"),
                node(4, AdminState::Active, "d"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![Tablet { id: 1, bytes: 100 }],
        };
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2, 3])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![2, 3, 4])]),
        };
        let budget = MigrationBudget {
            max_active: 1,
            max_per_node_active: 1,
            bytes_per_tick: 100,
            max_bytes_per_node_per_tick: 100,
            max_bytes_per_task_per_tick: 100,
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired.clone(),
            budget,
        )
        .unwrap();

        assert_eq!(scheduler.tasks().len(), 1);
        let task = &scheduler.tasks()[0];
        assert_eq!(task.priority, MigrationPriority::Repair);
        assert_eq!(task.owner_to_replace, 1);
        assert_ne!(task.copy_source, 1);
        assert!(matches!(task.copy_source, 2 | 3));

        scheduler.tick();
        assert_eq!(scheduler.actual(), &desired);
        assert!(scheduler.is_converged());
    }

    #[test]
    fn repair_without_surviving_replica_is_unrecoverable() {
        let cluster = Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, AdminState::Removed, "a"),
                node(2, AdminState::Removed, "b"),
                node(3, AdminState::Active, "c"),
                node(4, AdminState::Active, "d"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![Tablet { id: 1, bytes: 100 }],
        };
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![3, 4])]),
        };

        let result = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired,
            MigrationBudget::conservative(),
        );

        assert_eq!(result.unwrap_err(), MigrationError::NoRepairSource(1));
    }

    #[test]
    fn repair_priority_preempts_rebalance() {
        let cluster = Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, AdminState::Removed, "a"),
                node(2, AdminState::Active, "b"),
                node(3, AdminState::Active, "c"),
                node(4, AdminState::Active, "d"),
                node(5, AdminState::Active, "e"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![Tablet { id: 1, bytes: 100 }, Tablet { id: 2, bytes: 100 }],
        };
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2]), (2, vec![2, 4])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![2, 3]), (2, vec![2, 5])]),
        };
        let budget = MigrationBudget {
            max_active: 1,
            max_per_node_active: 1,
            bytes_per_tick: 10,
            max_bytes_per_node_per_tick: 10,
            max_bytes_per_task_per_tick: 10,
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired,
            budget,
        )
        .unwrap();

        assert_eq!(scheduler.tasks()[0].priority, MigrationPriority::Repair);
        assert_eq!(scheduler.tasks()[1].priority, MigrationPriority::Rebalance);

        scheduler.tick();

        assert_eq!(scheduler.tasks()[0].state, MigrationState::Copying);
        assert_eq!(scheduler.tasks()[1].state, MigrationState::Pending);
    }

    #[test]
    fn multiple_failed_owners_repair_from_one_survivor() {
        let cluster = Cluster {
            epoch: 1,
            replication_factor: 3,
            nodes: [
                node(1, AdminState::Removed, "a"),
                node(2, AdminState::Removed, "b"),
                node(3, AdminState::Active, "c"),
                node(4, AdminState::Active, "d"),
                node(5, AdminState::Active, "e"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![Tablet { id: 1, bytes: 100 }],
        };
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2, 3])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![3, 4, 5])]),
        };
        let budget = MigrationBudget {
            max_active: 1,
            max_per_node_active: 1,
            bytes_per_tick: 100,
            max_bytes_per_node_per_tick: 100,
            max_bytes_per_task_per_tick: 100,
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired.clone(),
            budget,
        )
        .unwrap();

        assert_eq!(scheduler.tasks().len(), 2);
        assert!(
            scheduler
                .tasks()
                .iter()
                .all(|task| task.priority == MigrationPriority::Repair)
        );
        assert!(scheduler.tasks().iter().all(|task| task.copy_source == 3));

        for _ in 0..4 {
            if scheduler.is_converged() {
                break;
            }
            scheduler.tick();
        }

        assert_eq!(scheduler.actual(), &desired);
        assert!(scheduler.is_converged());
    }

    #[test]
    fn desired_epoch_change_cancels_old_work() {
        let cluster = one_tablet_cluster(100);
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![1, 3])]),
        };
        let budget = MigrationBudget {
            max_active: 1,
            max_per_node_active: 1,
            bytes_per_tick: 40,
            max_bytes_per_node_per_tick: 40,
            max_bytes_per_task_per_tick: 40,
        };
        let mut scheduler = MigrationScheduler::new(
            cluster.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            actual.clone(),
            desired,
            budget,
        )
        .unwrap();

        scheduler.tick();
        assert_eq!(scheduler.remaining_bytes(), 60);

        let mut next_cluster = cluster;
        next_cluster.epoch = 2;
        scheduler
            .reconcile_desired(next_cluster, actual.clone())
            .unwrap();

        assert_eq!(scheduler.total_cancelled, 1);
        assert_eq!(scheduler.actual(), &actual);
        assert!(scheduler.is_converged());
    }

    #[test]
    fn planner_desired_map_converges_through_scheduler() {
        let before = Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, AdminState::Active, "a"),
                node(2, AdminState::Active, "b"),
                node(3, AdminState::Active, "c"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: (0..100).map(|id| Tablet { id, bytes: 10 }).collect(),
        };
        let actual =
            PlacementStrategy::WeightedRendezvous.place(&before, FailureDomainPolicy::HIERARCHICAL);

        let mut after = before.clone();
        after.epoch = 2;
        after.nodes.insert(4, node(4, AdminState::Active, "d"));
        let planned = plan_rebalance(&after, &actual, FailureDomainPolicy::HIERARCHICAL);
        assert!(planned.converged);

        let budget = MigrationBudget {
            max_active: 4,
            max_per_node_active: 2,
            bytes_per_tick: 80,
            max_bytes_per_node_per_tick: 40,
            max_bytes_per_task_per_tick: 20,
        };
        let mut scheduler = MigrationScheduler::new(
            after,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            planned.placement.clone(),
            budget,
        )
        .unwrap();

        for _ in 0..10_000 {
            if scheduler.is_converged() {
                break;
            }
            scheduler.tick();
        }

        assert!(scheduler.is_converged());
        assert_eq!(scheduler.actual(), &planned.placement);
    }
}
