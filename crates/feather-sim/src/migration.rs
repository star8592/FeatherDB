use std::collections::{BTreeMap, BTreeSet};

use crate::model::{AdminState, Cluster, NodeId, Placement, TabletId};
use crate::placement::FailureDomainPolicy;
use crate::transport::{
    DirectMigrationTransport, MigrationTransport, TransferPoll, TransferRequest, TransferSubmit,
};

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeHealth {
    Healthy,
    Suspect,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TabletAvailability {
    Healthy { available: usize, total: usize },
    Degraded { available: usize, total: usize },
    Lost { total: usize },
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
    pub in_flight_bytes: u64,
    pub in_flight_offset: u64,
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
    pub source_failovers: usize,
    pub grouped_cutovers: usize,
    pub transfer_drops: usize,
    pub transfer_duplicates: usize,
    pub bytes_attempted: u64,
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
    health: BTreeMap<NodeId, NodeHealth>,
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
        let health = cluster
            .nodes
            .keys()
            .copied()
            .map(|node_id| (node_id, NodeHealth::Healthy))
            .collect();

        let mut scheduler = Self {
            cluster,
            policy,
            budget,
            actual,
            desired,
            health,
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

    pub fn set_node_health(&mut self, node_id: NodeId, health: NodeHealth) -> bool {
        if !self.cluster.nodes.contains_key(&node_id) {
            return false;
        }
        self.health.insert(node_id, health);
        true
    }

    pub fn node_health(&self, node_id: NodeId) -> Option<NodeHealth> {
        self.health.get(&node_id).copied()
    }

    pub fn tablet_availability(&self, tablet_id: TabletId) -> Option<TabletAvailability> {
        let replicas = self.actual.replicas.get(&tablet_id)?;
        let available = replicas
            .iter()
            .filter(|node_id| runtime_source_readable(&self.cluster, &self.health, **node_id))
            .count();
        let total = replicas.len();

        Some(if available == 0 {
            TabletAvailability::Lost { total }
        } else if available == total {
            TabletAvailability::Healthy { available, total }
        } else {
            TabletAvailability::Degraded { available, total }
        })
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
        self.health
            .retain(|node_id, _| self.cluster.nodes.contains_key(node_id));
        for node_id in self.cluster.nodes.keys() {
            self.health.entry(*node_id).or_insert(NodeHealth::Healthy);
        }
        self.desired = desired;
        self.tasks.clear();
        self.rebuild_tasks()
    }

    pub fn tick(&mut self) -> TickReport {
        let mut transport = DirectMigrationTransport;
        self.tick_with_transport(0, &mut transport)
    }

    pub fn tick_with_transport<T: MigrationTransport>(
        &mut self,
        now_tick: u64,
        transport: &mut T,
    ) -> TickReport {
        let mut report = TickReport::default();

        self.refresh_repair_sources(&mut report, transport);
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
                    || !runtime_source_readable(&self.cluster, &self.health, copy_source)
                    || !runtime_target_writable(&self.cluster, &self.health, to)
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

            if self.tasks[index].in_flight_bytes > 0 {
                match transport.poll(now_tick, self.tasks[index].id) {
                    TransferPoll::Pending => continue,
                    TransferPoll::Dropped => {
                        self.tasks[index].in_flight_bytes = 0;
                        self.tasks[index].in_flight_offset = 0;
                        report.transfer_drops += 1;
                    }
                    TransferPoll::Delivered { bytes, duplicates } => {
                        let delivered = bytes.min(self.tasks[index].in_flight_bytes);
                        self.tasks[index].bytes_remaining =
                            self.tasks[index].bytes_remaining.saturating_sub(delivered);
                        self.tasks[index].in_flight_bytes = 0;
                        self.tasks[index].in_flight_offset = 0;
                        report.bytes_copied += delivered;
                        report.transfer_duplicates += duplicates as usize;
                    }
                }

                if self.tasks[index].in_flight_bytes > 0 {
                    continue;
                }
            }

            if self.tasks[index].bytes_remaining == 0 {
                self.tasks[index].state = MigrationState::ReadyToCutover;
                if self.try_cutover(index) {
                    report.completed += 1;
                    self.total_completed += 1;
                    active_count = active_count.saturating_sub(1);
                    decrement_node(&mut per_node, self.tasks[index].copy_source);
                    decrement_node(&mut per_node, self.tasks[index].to);
                }
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

            let request = TransferRequest {
                task_id: self.tasks[index].id,
                chunk_offset: self.tasks[index]
                    .bytes_total
                    .saturating_sub(self.tasks[index].bytes_remaining),
                from: copy_source,
                to,
                bytes: amount,
            };

            bytes_left -= amount;
            *bytes_by_node.entry(copy_source).or_default() += amount;
            *bytes_by_node.entry(to).or_default() += amount;
            report.bytes_attempted += amount;

            match transport.submit(now_tick, request) {
                TransferSubmit::Delivered { bytes, duplicates } => {
                    let delivered = bytes.min(amount);
                    self.tasks[index].bytes_remaining =
                        self.tasks[index].bytes_remaining.saturating_sub(delivered);
                    report.bytes_copied += delivered;
                    report.transfer_duplicates += duplicates as usize;
                }
                TransferSubmit::InFlight => {
                    self.tasks[index].in_flight_bytes = amount;
                    self.tasks[index].in_flight_offset = request.chunk_offset;
                }
                TransferSubmit::Dropped => {
                    report.transfer_drops += 1;
                }
            }

            if self.tasks[index].bytes_remaining == 0 && self.tasks[index].in_flight_bytes == 0 {
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

    fn refresh_repair_sources<T: MigrationTransport>(
        &mut self,
        report: &mut TickReport,
        transport: &mut T,
    ) {
        for index in 0..self.tasks.len() {
            if self.tasks[index].priority != MigrationPriority::Repair
                || matches!(
                    self.tasks[index].state,
                    MigrationState::Complete
                        | MigrationState::Stale
                        | MigrationState::ReadyToCutover
                )
            {
                continue;
            }

            let current_source = self.tasks[index].copy_source;
            if runtime_source_readable(&self.cluster, &self.health, current_source) {
                continue;
            }

            let tablet_id = self.tasks[index].tablet_id;
            let owner_to_replace = self.tasks[index].owner_to_replace;
            let Some(replicas) = self.actual.replicas.get(&tablet_id) else {
                continue;
            };

            let next_source =
                choose_repair_source(&self.cluster, &self.health, replicas, owner_to_replace);

            transport.cancel(self.tasks[index].id);
            self.tasks[index].state = MigrationState::Pending;
            self.tasks[index].bytes_remaining = self.tasks[index].bytes_total;
            self.tasks[index].in_flight_bytes = 0;
            self.tasks[index].in_flight_offset = 0;

            if let Some(next_source) = next_source {
                if next_source != current_source {
                    self.tasks[index].copy_source = next_source;
                    report.source_failovers += 1;
                }
            }
        }
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
                let (copy_source, priority) = if owner_can_stream(&self.cluster, owner_to_replace) {
                    (owner_to_replace, MigrationPriority::Rebalance)
                } else {
                    (
                        choose_repair_source(&self.cluster, &self.health, old, owner_to_replace)
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
                    in_flight_bytes: 0,
                    in_flight_offset: 0,
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

        let tablets: BTreeSet<_> = self
            .tasks
            .iter()
            .filter(|task| task.state == MigrationState::ReadyToCutover)
            .map(|task| task.tablet_id)
            .collect();

        for tablet_id in tablets {
            let completed = self.try_grouped_rebalance_cutover(tablet_id);
            if completed > 0 {
                report.completed += completed;
                report.grouped_cutovers += 1;
                self.total_completed += completed;
            }
        }
    }

    fn try_grouped_rebalance_cutover(&mut self, tablet_id: TabletId) -> usize {
        let indices: Vec<_> = self
            .tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| {
                task.tablet_id == tablet_id
                    && !matches!(task.state, MigrationState::Complete | MigrationState::Stale)
            })
            .map(|(index, _)| index)
            .collect();

        if indices.len() < 2
            || indices.iter().any(|index| {
                self.tasks[*index].priority != MigrationPriority::Rebalance
                    || self.tasks[*index].state != MigrationState::ReadyToCutover
                    || self.tasks[*index].epoch != self.cluster.epoch
            })
        {
            return 0;
        }

        let Some(current) = self.actual.replicas.get(&tablet_id).cloned() else {
            return 0;
        };
        let Some(desired) = self.desired.replicas.get(&tablet_id).cloned() else {
            return 0;
        };

        let mut candidate = current;

        for index in &indices {
            let task = &self.tasks[*index];

            if !desired.contains(&task.to)
                || desired.contains(&task.owner_to_replace)
                || candidate.contains(&task.to)
            {
                return 0;
            }

            let Some(slot) = candidate
                .iter_mut()
                .find(|node_id| **node_id == task.owner_to_replace)
            else {
                return 0;
            };
            *slot = task.to;
        }

        candidate.sort_unstable();

        if candidate != desired || !replica_set_safe(&self.cluster, &candidate, self.policy) {
            return 0;
        }

        self.actual.replicas.insert(tablet_id, candidate);
        for index in &indices {
            self.tasks[*index].state = MigrationState::Complete;
        }

        indices.len()
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
            MigrationPriority::Repair => repair_cutover_safe(
                &self.cluster,
                &self.health,
                &current,
                &candidate,
                task.to,
                self.policy,
            ),
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
    health: &BTreeMap<NodeId, NodeHealth>,
    replicas: &[NodeId],
    owner_to_replace: NodeId,
) -> Option<NodeId> {
    let mut candidates: Vec<_> = replicas
        .iter()
        .copied()
        .filter(|node_id| *node_id != owner_to_replace)
        .filter(|node_id| runtime_source_readable(cluster, health, *node_id))
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
    health: &BTreeMap<NodeId, NodeHealth>,
    current: &[NodeId],
    candidate: &[NodeId],
    target: NodeId,
    policy: FailureDomainPolicy,
) -> bool {
    if !runtime_target_writable(cluster, health, target)
        || candidate.iter().collect::<BTreeSet<_>>().len() != candidate.len()
    {
        return false;
    }

    let current_available = current
        .iter()
        .filter(|node_id| runtime_source_readable(cluster, health, **node_id))
        .count();
    let candidate_available = candidate
        .iter()
        .filter(|node_id| runtime_source_readable(cluster, health, **node_id))
        .count();

    if candidate_available <= current_available {
        return false;
    }

    if policy.distinct_zones {
        let before = current
            .iter()
            .filter(|node_id| runtime_source_readable(cluster, health, **node_id))
            .map(|node_id| cluster.nodes[node_id].zone.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        let after = candidate
            .iter()
            .filter(|node_id| runtime_source_readable(cluster, health, **node_id))
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
            .filter(|node_id| runtime_source_readable(cluster, health, **node_id))
            .map(|node_id| {
                let node = &cluster.nodes[node_id];
                (node.zone.as_str(), node.rack.as_str())
            })
            .collect::<BTreeSet<_>>()
            .len();
        let after = candidate
            .iter()
            .filter(|node_id| runtime_source_readable(cluster, health, **node_id))
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

fn owner_can_stream(cluster: &Cluster, node_id: NodeId) -> bool {
    cluster
        .nodes
        .get(&node_id)
        .is_some_and(|node| matches!(node.state, AdminState::Active | AdminState::Draining))
}

fn runtime_source_readable(
    cluster: &Cluster,
    health: &BTreeMap<NodeId, NodeHealth>,
    node_id: NodeId,
) -> bool {
    owner_can_stream(cluster, node_id)
        && health
            .get(&node_id)
            .copied()
            .unwrap_or(NodeHealth::Unavailable)
            == NodeHealth::Healthy
}

fn topology_target_writable(cluster: &Cluster, node_id: NodeId) -> bool {
    cluster
        .nodes
        .get(&node_id)
        .is_some_and(|node| node.state == AdminState::Active)
}

fn runtime_target_writable(
    cluster: &Cluster,
    health: &BTreeMap<NodeId, NodeHealth>,
    node_id: NodeId,
) -> bool {
    topology_target_writable(cluster, node_id)
        && health
            .get(&node_id)
            .copied()
            .unwrap_or(NodeHealth::Unavailable)
            == NodeHealth::Healthy
}

fn replica_set_safe(cluster: &Cluster, replicas: &[NodeId], policy: FailureDomainPolicy) -> bool {
    if replicas.iter().collect::<BTreeSet<_>>().len() != replicas.len() {
        return false;
    }

    if replicas
        .iter()
        .any(|node_id| !topology_target_writable(cluster, *node_id))
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
    fn transient_unavailability_degrades_tablet_without_changing_ownership() {
        let cluster = one_tablet_cluster(100);
        let placement = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            placement.clone(),
            placement.clone(),
            MigrationBudget::conservative(),
        )
        .unwrap();

        assert_eq!(
            scheduler.tablet_availability(1),
            Some(TabletAvailability::Healthy {
                available: 2,
                total: 2
            })
        );

        assert!(scheduler.set_node_health(1, NodeHealth::Unavailable));

        assert_eq!(
            scheduler.tablet_availability(1),
            Some(TabletAvailability::Degraded {
                available: 1,
                total: 2
            })
        );
        assert_eq!(scheduler.actual(), &placement);
        assert_eq!(scheduler.desired(), &placement);
        assert!(scheduler.tasks().is_empty());
    }

    #[test]
    fn repair_source_failover_restarts_copy_from_survivor() {
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
            bytes_per_tick: 40,
            max_bytes_per_node_per_tick: 40,
            max_bytes_per_task_per_tick: 40,
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired.clone(),
            budget,
        )
        .unwrap();

        assert_eq!(scheduler.tasks()[0].copy_source, 2);
        let first = scheduler.tick();
        assert_eq!(first.bytes_copied, 40);
        assert_eq!(scheduler.tasks()[0].bytes_remaining, 60);

        assert!(scheduler.set_node_health(2, NodeHealth::Unavailable));
        assert_eq!(
            scheduler.tablet_availability(1),
            Some(TabletAvailability::Degraded {
                available: 1,
                total: 3
            })
        );

        let failover = scheduler.tick();
        assert_eq!(failover.source_failovers, 1);
        assert_eq!(scheduler.tasks()[0].copy_source, 3);
        assert_eq!(failover.bytes_copied, 40);
        assert_eq!(scheduler.tasks()[0].bytes_remaining, 60);

        for _ in 0..4 {
            if scheduler.is_converged() {
                break;
            }
            scheduler.tick();
        }

        assert_eq!(scheduler.actual(), &desired);
        assert!(scheduler.is_converged());
        assert_eq!(
            scheduler.tablet_availability(1),
            Some(TabletAvailability::Degraded {
                available: 2,
                total: 3
            })
        );
    }

    #[test]
    fn all_runtime_replicas_unavailable_is_lost_but_topology_is_unchanged() {
        let cluster = one_tablet_cluster(100);
        let placement = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            placement.clone(),
            placement.clone(),
            MigrationBudget::conservative(),
        )
        .unwrap();

        scheduler.set_node_health(1, NodeHealth::Unavailable);
        scheduler.set_node_health(2, NodeHealth::Unavailable);

        assert_eq!(
            scheduler.tablet_availability(1),
            Some(TabletAvailability::Lost { total: 2 })
        );
        assert_eq!(scheduler.actual(), &placement);
        assert_eq!(scheduler.desired(), &placement);
    }

    #[test]
    fn scheduler_restart_reconstructs_repair_from_durable_maps() {
        let cluster = Cluster {
            epoch: 9,
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
            bytes_per_tick: 40,
            max_bytes_per_node_per_tick: 40,
            max_bytes_per_task_per_tick: 40,
        };

        let mut first = MigrationScheduler::new(
            cluster.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            actual.clone(),
            desired.clone(),
            budget,
        )
        .unwrap();

        first.tick();
        assert_eq!(first.tasks()[0].bytes_remaining, 60);
        assert_eq!(first.actual(), &actual);

        let mut restarted = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            first.actual().clone(),
            desired.clone(),
            budget,
        )
        .unwrap();

        assert_eq!(restarted.tasks().len(), 1);
        assert_eq!(restarted.tasks()[0].priority, MigrationPriority::Repair);
        assert_eq!(restarted.tasks()[0].bytes_remaining, 100);

        for _ in 0..4 {
            if restarted.is_converged() {
                break;
            }
            restarted.tick();
        }

        assert_eq!(restarted.actual(), &desired);
        assert!(restarted.is_converged());
    }

    #[test]
    fn grouped_cutover_resolves_cross_zone_swap_without_unsafe_intermediate() {
        let cluster = Cluster {
            epoch: 1,
            replication_factor: 3,
            nodes: [
                node(1, AdminState::Active, "a"),
                node(2, AdminState::Active, "b"),
                node(3, AdminState::Active, "c"),
                node(4, AdminState::Active, "b"),
                node(5, AdminState::Active, "a"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![Tablet { id: 1, bytes: 100 }],
        };

        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 3, 4])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![2, 3, 5])]),
        };

        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual.clone(),
            desired.clone(),
            MigrationBudget {
                max_active: 2,
                max_per_node_active: 1,
                bytes_per_tick: 200,
                max_bytes_per_node_per_tick: 100,
                max_bytes_per_task_per_tick: 100,
            },
        )
        .unwrap();

        assert_eq!(scheduler.tasks().len(), 2);

        let report = scheduler.tick();

        assert_eq!(report.completed, 2);
        assert_eq!(report.grouped_cutovers, 1);
        assert_eq!(scheduler.actual(), &desired);
        assert!(scheduler.is_converged());

        // Neither one-at-a-time replacement would have preserved three zones:
        // A->B duplicates B, and B->A duplicates A. The whole replica-set
        // transition is safe and is therefore committed atomically.
        assert_ne!(scheduler.actual(), &actual);
    }

    #[test]
    fn delayed_transport_does_not_cut_over_before_delivery() {
        let cluster = one_tablet_cluster(100);
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![1, 3])]),
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual.clone(),
            desired.clone(),
            MigrationBudget {
                max_active: 1,
                max_per_node_active: 1,
                bytes_per_tick: 100,
                max_bytes_per_node_per_tick: 100,
                max_bytes_per_task_per_tick: 100,
            },
        )
        .unwrap();

        let mut network = crate::transport::SimNetwork::default();
        network.set_delay(2, 3, 3);

        let first = scheduler.tick_with_transport(0, &mut network);
        assert_eq!(first.bytes_attempted, 100);
        assert_eq!(first.bytes_copied, 0);
        assert_eq!(scheduler.tasks()[0].in_flight_bytes, 100);
        assert_eq!(scheduler.actual(), &actual);

        scheduler.tick_with_transport(1, &mut network);
        scheduler.tick_with_transport(2, &mut network);
        assert_eq!(scheduler.actual(), &actual);
        assert_eq!(scheduler.tasks()[0].bytes_remaining, 100);

        let delivered = scheduler.tick_with_transport(3, &mut network);
        assert_eq!(delivered.bytes_copied, 100);
        assert_eq!(scheduler.actual(), &desired);
        assert!(scheduler.is_converged());
    }

    #[test]
    fn partition_blocks_copy_until_heal_then_converges() {
        let cluster = one_tablet_cluster(100);
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![1, 3])]),
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual.clone(),
            desired.clone(),
            MigrationBudget {
                max_active: 1,
                max_per_node_active: 1,
                bytes_per_tick: 100,
                max_bytes_per_node_per_tick: 100,
                max_bytes_per_task_per_tick: 100,
            },
        )
        .unwrap();

        let mut network = crate::transport::SimNetwork::default();
        network.partition(2, 3, false);

        for tick in 0..3 {
            let report = scheduler.tick_with_transport(tick, &mut network);
            assert_eq!(report.transfer_drops, 1);
            assert_eq!(report.bytes_copied, 0);
            assert_eq!(scheduler.actual(), &actual);
        }

        network.heal(2, 3, false);
        let healed = scheduler.tick_with_transport(3, &mut network);
        assert_eq!(healed.bytes_copied, 100);
        assert_eq!(scheduler.actual(), &desired);
        assert!(scheduler.is_converged());
    }

    #[test]
    fn duplicate_delivery_is_idempotent_for_copy_progress() {
        let cluster = one_tablet_cluster(100);
        let actual = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let desired = Placement {
            replicas: BTreeMap::from([(1, vec![1, 3])]),
        };
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired.clone(),
            MigrationBudget {
                max_active: 1,
                max_per_node_active: 1,
                bytes_per_tick: 100,
                max_bytes_per_node_per_tick: 100,
                max_bytes_per_task_per_tick: 100,
            },
        )
        .unwrap();

        let mut network = crate::transport::SimNetwork::default();
        network.duplicate_next(2, 3, 1);

        let report = scheduler.tick_with_transport(0, &mut network);

        assert_eq!(report.transfer_duplicates, 1);
        assert_eq!(report.bytes_attempted, 100);
        assert_eq!(report.bytes_copied, 100);
        assert_eq!(scheduler.tasks()[0].bytes_remaining, 0);
        assert_eq!(scheduler.actual(), &desired);
    }

    #[test]
    fn repair_source_failover_cancels_old_in_flight_chunk() {
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
        let mut scheduler = MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired.clone(),
            MigrationBudget {
                max_active: 1,
                max_per_node_active: 1,
                bytes_per_tick: 100,
                max_bytes_per_node_per_tick: 100,
                max_bytes_per_task_per_tick: 100,
            },
        )
        .unwrap();

        let mut network = crate::transport::SimNetwork::default();
        network.set_delay(2, 4, 10);

        let first = scheduler.tick_with_transport(0, &mut network);
        assert_eq!(first.bytes_copied, 0);
        assert_eq!(network.in_flight_count(), 1);
        assert_eq!(scheduler.tasks()[0].copy_source, 2);

        scheduler.set_node_health(2, NodeHealth::Unavailable);
        let failover = scheduler.tick_with_transport(1, &mut network);

        assert_eq!(failover.source_failovers, 1);
        assert_eq!(scheduler.tasks()[0].copy_source, 3);
        assert_eq!(network.in_flight_count(), 0);
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
