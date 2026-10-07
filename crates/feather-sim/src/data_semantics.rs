use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};

use crate::model::NodeId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuorumPolicy {
    pub replication_factor: usize,
    pub read: usize,
    pub write: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuorumPolicyError {
    ZeroReplication,
    ZeroRead,
    ZeroWrite,
    ReadExceedsReplication,
    WriteExceedsReplication,
}

impl QuorumPolicy {
    pub fn validate(self) -> Result<Self, QuorumPolicyError> {
        if self.replication_factor == 0 {
            return Err(QuorumPolicyError::ZeroReplication);
        }
        if self.read == 0 {
            return Err(QuorumPolicyError::ZeroRead);
        }
        if self.write == 0 {
            return Err(QuorumPolicyError::ZeroWrite);
        }
        if self.read > self.replication_factor {
            return Err(QuorumPolicyError::ReadExceedsReplication);
        }
        if self.write > self.replication_factor {
            return Err(QuorumPolicyError::WriteExceedsReplication);
        }
        Ok(self)
    }

    pub fn has_quorum_intersection(self) -> bool {
        self.read.saturating_add(self.write) > self.replication_factor
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct HlcTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
    pub node_id: NodeId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HlcClock {
    node_id: NodeId,
    last: HlcTimestamp,
}

impl HlcClock {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            last: HlcTimestamp {
                physical_ms: 0,
                logical: 0,
                node_id,
            },
        }
    }

    pub fn last(self) -> HlcTimestamp {
        self.last
    }

    pub fn tick(&mut self, physical_ms: u64) -> HlcTimestamp {
        if physical_ms > self.last.physical_ms {
            self.last.physical_ms = physical_ms;
            self.last.logical = 0;
        } else {
            self.last.logical = self.last.logical.saturating_add(1);
        }
        self.last.node_id = self.node_id;
        self.last
    }

    pub fn observe(&mut self, remote: HlcTimestamp, physical_ms: u64) -> HlcTimestamp {
        let max_physical = physical_ms
            .max(self.last.physical_ms)
            .max(remote.physical_ms);

        let logical = if max_physical == self.last.physical_ms && max_physical == remote.physical_ms
        {
            self.last.logical.max(remote.logical).saturating_add(1)
        } else if max_physical == self.last.physical_ms {
            self.last.logical.saturating_add(1)
        } else if max_physical == remote.physical_ms {
            remote.logical.saturating_add(1)
        } else {
            0
        };

        self.last = HlcTimestamp {
            physical_ms: max_physical,
            logical,
            node_id: self.node_id,
        };
        self.last
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CausalRelation {
    Before,
    Equal,
    After,
    Concurrent,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VersionVector {
    counters: BTreeMap<NodeId, u64>,
}

impl VersionVector {
    pub fn counter(&self, node_id: NodeId) -> u64 {
        self.counters.get(&node_id).copied().unwrap_or_default()
    }

    pub fn increment(&mut self, node_id: NodeId) -> u64 {
        let next = self.counter(node_id).saturating_add(1);
        self.counters.insert(node_id, next);
        next
    }

    pub fn merge(&mut self, other: &Self) {
        for (node_id, counter) in &other.counters {
            let current = self.counter(*node_id);
            if *counter > current {
                self.counters.insert(*node_id, *counter);
            }
        }
    }

    pub fn relation(&self, other: &Self) -> CausalRelation {
        let mut less = false;
        let mut greater = false;

        for node_id in self.counters.keys().chain(other.counters.keys()) {
            match self.counter(*node_id).cmp(&other.counter(*node_id)) {
                Ordering::Less => less = true,
                Ordering::Greater => greater = true,
                Ordering::Equal => {}
            }
            if less && greater {
                return CausalRelation::Concurrent;
            }
        }

        match (less, greater) {
            (true, false) => CausalRelation::Before,
            (false, true) => CausalRelation::After,
            (false, false) => CausalRelation::Equal,
            (true, true) => CausalRelation::Concurrent,
        }
    }

    pub fn dominates_or_equals(&self, other: &Self) -> bool {
        matches!(
            self.relation(other),
            CausalRelation::After | CausalRelation::Equal
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedValue {
    pub value: Option<Vec<u8>>,
    pub context: VersionVector,
    pub timestamp: HlcTimestamp,
    pub origin: NodeId,
}

impl VersionedValue {
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SiblingSet {
    versions: Vec<VersionedValue>,
}

impl SiblingSet {
    pub fn versions(&self) -> &[VersionedValue] {
        &self.versions
    }

    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    pub fn merged_context(&self) -> VersionVector {
        let mut context = VersionVector::default();
        for version in &self.versions {
            context.merge(&version.context);
        }
        context
    }

    pub fn merge_version(&mut self, incoming: VersionedValue) -> bool {
        let mut dominated = false;
        self.versions.retain(
            |existing| match existing.context.relation(&incoming.context) {
                CausalRelation::Before => false,
                CausalRelation::After => {
                    dominated = true;
                    true
                }
                CausalRelation::Equal if existing == &incoming => {
                    dominated = true;
                    true
                }
                CausalRelation::Equal | CausalRelation::Concurrent => true,
            },
        );

        if dominated {
            return false;
        }

        self.versions.push(incoming);
        self.versions.sort_by(|a, b| {
            a.timestamp
                .cmp(&b.timestamp)
                .then_with(|| a.origin.cmp(&b.origin))
                .then_with(|| a.value.cmp(&b.value))
        });
        true
    }

    pub fn merge_set(&mut self, other: &Self) {
        for version in &other.versions {
            self.merge_version(version.clone());
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConflictPolicy {
    KeepSiblings,
    LastWriterWinsHlc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedValue {
    Missing,
    Value(Vec<u8>),
    Deleted,
    Conflict(Vec<VersionedValue>),
}

pub fn resolve_siblings(siblings: &SiblingSet, policy: ConflictPolicy) -> ResolvedValue {
    if siblings.versions.is_empty() {
        return ResolvedValue::Missing;
    }

    let chosen = match policy {
        ConflictPolicy::KeepSiblings if siblings.versions.len() > 1 => {
            return ResolvedValue::Conflict(siblings.versions.clone());
        }
        ConflictPolicy::KeepSiblings | ConflictPolicy::LastWriterWinsHlc => siblings
            .versions
            .iter()
            .max_by_key(|version| version.timestamp)
            .expect("non-empty sibling set"),
    };

    match &chosen.value {
        Some(value) => ResolvedValue::Value(value.clone()),
        None => ResolvedValue::Deleted,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteOutcome {
    pub acknowledgements: usize,
    pub required: usize,
    pub succeeded: bool,
    pub hints_created: usize,
    pub context: VersionVector,
    pub version: VersionedValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadOutcome {
    pub responses: usize,
    pub required: usize,
    pub siblings: SiblingSet,
    pub context: VersionVector,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataError {
    InvalidPolicy(QuorumPolicyError),
    UnknownNode(NodeId),
    CoordinatorUnavailable(NodeId),
    ReplicaSetTooSmall { have: usize, need: usize },
    DuplicateReplica(NodeId),
    ReadUnavailable { responses: usize, required: usize },
    CausalNotReady,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Hint {
    target: NodeId,
    key: Vec<u8>,
    version: VersionedValue,
}

#[derive(Clone, Debug)]
struct ReplicaNode {
    online: bool,
    clock: HlcClock,
    data: BTreeMap<Vec<u8>, SiblingSet>,
}

impl ReplicaNode {
    fn new(node_id: NodeId) -> Self {
        Self {
            online: true,
            clock: HlcClock::new(node_id),
            data: BTreeMap::new(),
        }
    }

    fn apply(&mut self, key: &[u8], version: VersionedValue, physical_ms: u64) {
        self.clock.observe(version.timestamp, physical_ms);
        self.data
            .entry(key.to_vec())
            .or_default()
            .merge_version(version);
    }

    fn read(&self, key: &[u8]) -> SiblingSet {
        self.data.get(key).cloned().unwrap_or_default()
    }
}

#[derive(Clone, Copy)]
struct WriteConfig<'a> {
    client_context: Option<&'a VersionVector>,
    physical_ms: u64,
    required: usize,
}

#[derive(Clone, Debug)]
pub struct LeaderlessDataCluster {
    policy: QuorumPolicy,
    nodes: BTreeMap<NodeId, ReplicaNode>,
    hints: VecDeque<Hint>,
    max_hints: usize,
    dropped_hints: u64,
}

impl LeaderlessDataCluster {
    pub fn new(
        node_ids: &[NodeId],
        policy: QuorumPolicy,
        max_hints: usize,
    ) -> Result<Self, DataError> {
        let policy = policy.validate().map_err(DataError::InvalidPolicy)?;
        let mut nodes = BTreeMap::new();
        for node_id in node_ids {
            if nodes.insert(*node_id, ReplicaNode::new(*node_id)).is_some() {
                return Err(DataError::DuplicateReplica(*node_id));
            }
        }
        Ok(Self {
            policy,
            nodes,
            hints: VecDeque::new(),
            max_hints,
            dropped_hints: 0,
        })
    }

    pub fn policy(&self) -> QuorumPolicy {
        self.policy
    }

    pub fn set_online(&mut self, node_id: NodeId, online: bool) -> Result<(), DataError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(DataError::UnknownNode(node_id))?;
        node.online = online;
        Ok(())
    }

    pub fn is_online(&self, node_id: NodeId) -> Result<bool, DataError> {
        Ok(self
            .nodes
            .get(&node_id)
            .ok_or(DataError::UnknownNode(node_id))?
            .online)
    }

    pub fn pending_hints(&self) -> usize {
        self.hints.len()
    }

    pub fn dropped_hints(&self) -> u64 {
        self.dropped_hints
    }

    pub fn replica_siblings(&self, node_id: NodeId, key: &[u8]) -> Result<SiblingSet, DataError> {
        Ok(self
            .nodes
            .get(&node_id)
            .ok_or(DataError::UnknownNode(node_id))?
            .read(key))
    }

    pub fn put(
        &mut self,
        coordinator: NodeId,
        replicas: &[NodeId],
        key: &[u8],
        value: Vec<u8>,
        client_context: Option<&VersionVector>,
        physical_ms: u64,
    ) -> Result<WriteOutcome, DataError> {
        self.write_mutation(
            coordinator,
            replicas,
            key,
            Some(value),
            WriteConfig {
                client_context,
                physical_ms,
                required: self.policy.write,
            },
        )
    }

    pub fn put_eventual(
        &mut self,
        coordinator: NodeId,
        replicas: &[NodeId],
        key: &[u8],
        value: Vec<u8>,
        client_context: Option<&VersionVector>,
        physical_ms: u64,
    ) -> Result<WriteOutcome, DataError> {
        self.write_mutation(
            coordinator,
            replicas,
            key,
            Some(value),
            WriteConfig {
                client_context,
                physical_ms,
                required: 1,
            },
        )
    }

    pub fn delete(
        &mut self,
        coordinator: NodeId,
        replicas: &[NodeId],
        key: &[u8],
        client_context: Option<&VersionVector>,
        physical_ms: u64,
    ) -> Result<WriteOutcome, DataError> {
        self.write_mutation(
            coordinator,
            replicas,
            key,
            None,
            WriteConfig {
                client_context,
                physical_ms,
                required: self.policy.write,
            },
        )
    }

    pub fn read_quorum(&self, replicas: &[NodeId], key: &[u8]) -> Result<ReadOutcome, DataError> {
        self.read_required(replicas, key, self.policy.read)
    }

    pub fn read_eventual(&self, replicas: &[NodeId], key: &[u8]) -> Result<ReadOutcome, DataError> {
        self.read_required(replicas, key, 1)
    }

    pub fn read_causal(
        &self,
        replicas: &[NodeId],
        key: &[u8],
        required_context: &VersionVector,
    ) -> Result<ReadOutcome, DataError> {
        let replicas = self.validate_replicas(replicas)?;
        for node_id in replicas {
            let node = self
                .nodes
                .get(&node_id)
                .ok_or(DataError::UnknownNode(node_id))?;
            if !node.online {
                continue;
            }
            let siblings = node.read(key);
            let context = siblings.merged_context();
            if context.dominates_or_equals(required_context) {
                return Ok(ReadOutcome {
                    responses: 1,
                    required: 1,
                    siblings,
                    context,
                });
            }
        }
        Err(DataError::CausalNotReady)
    }

    pub fn replay_hints(
        &mut self,
        target: NodeId,
        max_to_replay: usize,
        physical_ms: u64,
    ) -> usize {
        if max_to_replay == 0 || !self.nodes.get(&target).is_some_and(|node| node.online) {
            return 0;
        }

        let mut replayed = 0_usize;
        let mut retained = VecDeque::with_capacity(self.hints.len());
        while let Some(hint) = self.hints.pop_front() {
            if hint.target == target && replayed < max_to_replay {
                if let Some(node) = self.nodes.get_mut(&target) {
                    node.apply(&hint.key, hint.version, physical_ms);
                    replayed += 1;
                }
            } else {
                retained.push_back(hint);
            }
        }
        self.hints = retained;
        replayed
    }

    pub fn anti_entropy_key(
        &mut self,
        replicas: &[NodeId],
        key: &[u8],
        physical_ms: u64,
    ) -> Result<usize, DataError> {
        let replicas = self.validate_replicas(replicas)?;
        let mut merged = SiblingSet::default();
        for node_id in &replicas {
            let node = self
                .nodes
                .get(node_id)
                .ok_or(DataError::UnknownNode(*node_id))?;
            if node.online {
                merged.merge_set(&node.read(key));
            }
        }

        let mut repaired = 0_usize;
        for node_id in replicas {
            let Some(node) = self.nodes.get_mut(&node_id) else {
                return Err(DataError::UnknownNode(node_id));
            };
            if !node.online {
                continue;
            }
            let before = node.read(key);
            for version in merged.versions() {
                node.apply(key, version.clone(), physical_ms);
            }
            if node.read(key) != before {
                repaired += 1;
            }
        }
        Ok(repaired)
    }

    fn write_mutation(
        &mut self,
        coordinator: NodeId,
        replicas: &[NodeId],
        key: &[u8],
        value: Option<Vec<u8>>,
        config: WriteConfig<'_>,
    ) -> Result<WriteOutcome, DataError> {
        let WriteConfig {
            client_context,
            physical_ms,
            required,
        } = config;
        let replicas = self.validate_replicas(replicas)?;
        let coordinator_node = self
            .nodes
            .get_mut(&coordinator)
            .ok_or(DataError::UnknownNode(coordinator))?;
        if !coordinator_node.online {
            return Err(DataError::CoordinatorUnavailable(coordinator));
        }

        let mut context = client_context.cloned().unwrap_or_default();
        context.increment(coordinator);
        let timestamp = coordinator_node.clock.tick(physical_ms);
        let version = VersionedValue {
            value,
            context: context.clone(),
            timestamp,
            origin: coordinator,
        };

        let mut acknowledgements = 0_usize;
        let mut hints_created = 0_usize;
        for target in replicas {
            let online = self
                .nodes
                .get(&target)
                .ok_or(DataError::UnknownNode(target))?
                .online;
            if online {
                self.nodes
                    .get_mut(&target)
                    .expect("validated replica")
                    .apply(key, version.clone(), physical_ms);
                acknowledgements += 1;
            } else if self.hints.len() < self.max_hints {
                self.hints.push_back(Hint {
                    target,
                    key: key.to_vec(),
                    version: version.clone(),
                });
                hints_created += 1;
            } else {
                self.dropped_hints = self.dropped_hints.saturating_add(1);
            }
        }

        Ok(WriteOutcome {
            acknowledgements,
            required,
            succeeded: acknowledgements >= required,
            hints_created,
            context,
            version,
        })
    }

    fn read_required(
        &self,
        replicas: &[NodeId],
        key: &[u8],
        required: usize,
    ) -> Result<ReadOutcome, DataError> {
        let replicas = self.validate_replicas(replicas)?;
        let mut siblings = SiblingSet::default();
        let mut responses = 0_usize;

        for node_id in replicas {
            let node = self
                .nodes
                .get(&node_id)
                .ok_or(DataError::UnknownNode(node_id))?;
            if !node.online {
                continue;
            }
            siblings.merge_set(&node.read(key));
            responses += 1;
            if responses == required {
                break;
            }
        }

        if responses < required {
            return Err(DataError::ReadUnavailable {
                responses,
                required,
            });
        }

        let context = siblings.merged_context();
        Ok(ReadOutcome {
            responses,
            required,
            siblings,
            context,
        })
    }

    fn validate_replicas(&self, replicas: &[NodeId]) -> Result<Vec<NodeId>, DataError> {
        if replicas.len() < self.policy.replication_factor {
            return Err(DataError::ReplicaSetTooSmall {
                have: replicas.len(),
                need: self.policy.replication_factor,
            });
        }

        let mut selected = Vec::with_capacity(self.policy.replication_factor);
        for node_id in replicas.iter().take(self.policy.replication_factor) {
            if !self.nodes.contains_key(node_id) {
                return Err(DataError::UnknownNode(*node_id));
            }
            if selected.contains(node_id) {
                return Err(DataError::DuplicateReplica(*node_id));
            }
            selected.push(*node_id);
        }
        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> QuorumPolicy {
        QuorumPolicy {
            replication_factor: 3,
            read: 2,
            write: 2,
        }
    }

    fn cluster(max_hints: usize) -> LeaderlessDataCluster {
        LeaderlessDataCluster::new(&[1, 2, 3, 4], policy(), max_hints).unwrap()
    }

    #[test]
    fn quorum_policy_requires_valid_bounds_and_reports_intersection() {
        assert!(policy().validate().is_ok());
        assert!(policy().has_quorum_intersection());
        assert!(
            !QuorumPolicy {
                replication_factor: 3,
                read: 1,
                write: 1,
            }
            .validate()
            .unwrap()
            .has_quorum_intersection()
        );
        assert_eq!(
            QuorumPolicy {
                replication_factor: 3,
                read: 4,
                write: 2,
            }
            .validate(),
            Err(QuorumPolicyError::ReadExceedsReplication)
        );
    }

    #[test]
    fn hlc_is_monotonic_across_wall_clock_rollback_and_remote_observe() {
        let mut a = HlcClock::new(1);
        let first = a.tick(100);
        let rollback = a.tick(90);
        assert!(rollback > first);
        assert_eq!(rollback.physical_ms, 100);

        let remote = HlcTimestamp {
            physical_ms: 150,
            logical: 7,
            node_id: 2,
        };
        let observed = a.observe(remote, 120);
        assert!(observed > rollback);
        assert_eq!(observed.physical_ms, 150);
        assert_eq!(observed.logical, 8);
    }

    #[test]
    fn quorum_write_and_intersecting_quorum_read_observe_value() {
        let mut cluster = cluster(16);
        cluster.set_online(3, false).unwrap();

        let write = cluster
            .put(1, &[1, 2, 3], b"k", b"v1".to_vec(), None, 10)
            .unwrap();
        assert!(write.succeeded);
        assert_eq!(write.acknowledgements, 2);
        assert_eq!(write.hints_created, 1);

        cluster.set_online(3, true).unwrap();
        cluster.set_online(1, false).unwrap();
        let read = cluster.read_quorum(&[1, 2, 3], b"k").unwrap();
        assert_eq!(read.responses, 2);
        assert_eq!(
            resolve_siblings(&read.siblings, ConflictPolicy::KeepSiblings),
            ResolvedValue::Value(b"v1".to_vec())
        );
    }

    #[test]
    fn failed_quorum_write_can_leave_visible_partial_mutation() {
        let mut cluster = cluster(0);
        cluster.set_online(2, false).unwrap();
        cluster.set_online(3, false).unwrap();

        let write = cluster
            .put(1, &[1, 2, 3], b"k", b"partial".to_vec(), None, 10)
            .unwrap();
        assert!(!write.succeeded);
        assert_eq!(write.acknowledgements, 1);

        let read = cluster.read_eventual(&[1, 2, 3], b"k").unwrap();
        assert_eq!(
            resolve_siblings(&read.siblings, ConflictPolicy::KeepSiblings),
            ResolvedValue::Value(b"partial".to_vec())
        );
    }

    #[test]
    fn concurrent_writes_are_exposed_as_siblings_not_overwritten_by_wall_clock() {
        let mut cluster = cluster(16);
        let a = cluster
            .put_eventual(1, &[1, 2, 3], b"k", b"a".to_vec(), None, 100)
            .unwrap();
        cluster.set_online(1, false).unwrap();
        cluster.set_online(2, true).unwrap();
        let b = cluster
            .put_eventual(2, &[1, 2, 3], b"k", b"b".to_vec(), None, 200)
            .unwrap();

        assert_eq!(a.context.relation(&b.context), CausalRelation::Concurrent);
        cluster.set_online(1, true).unwrap();
        cluster.anti_entropy_key(&[1, 2, 3], b"k", 300).unwrap();
        let read = cluster.read_quorum(&[1, 2, 3], b"k").unwrap();
        assert_eq!(read.siblings.versions().len(), 2);
        assert!(matches!(
            resolve_siblings(&read.siblings, ConflictPolicy::KeepSiblings),
            ResolvedValue::Conflict(_)
        ));
    }

    #[test]
    fn causal_overwrite_dominates_previously_observed_siblings() {
        let mut cluster = cluster(16);
        cluster.set_online(3, false).unwrap();
        cluster
            .put_eventual(1, &[1, 2, 3], b"k", b"a".to_vec(), None, 100)
            .unwrap();
        cluster.set_online(1, false).unwrap();
        cluster.set_online(3, true).unwrap();
        cluster
            .put_eventual(2, &[1, 2, 3], b"k", b"b".to_vec(), None, 101)
            .unwrap();
        cluster.set_online(1, true).unwrap();
        cluster.anti_entropy_key(&[1, 2, 3], b"k", 102).unwrap();

        let read = cluster.read_quorum(&[1, 2, 3], b"k").unwrap();
        assert_eq!(read.siblings.versions().len(), 2);

        let resolved = cluster
            .put(
                3,
                &[1, 2, 3],
                b"k",
                b"resolved".to_vec(),
                Some(&read.context),
                103,
            )
            .unwrap();
        assert!(resolved.succeeded);

        let after = cluster.read_quorum(&[1, 2, 3], b"k").unwrap();
        assert_eq!(after.siblings.versions().len(), 1);
        assert_eq!(
            resolve_siblings(&after.siblings, ConflictPolicy::KeepSiblings),
            ResolvedValue::Value(b"resolved".to_vec())
        );
    }

    #[test]
    fn tombstone_prevents_old_hint_from_resurrecting_value() {
        let mut cluster = cluster(16);
        cluster.set_online(3, false).unwrap();
        let put = cluster
            .put(1, &[1, 2, 3], b"k", b"v".to_vec(), None, 10)
            .unwrap();
        assert!(put.succeeded);

        let delete = cluster
            .delete(2, &[1, 2, 3], b"k", Some(&put.context), 20)
            .unwrap();
        assert!(delete.succeeded);
        assert_eq!(cluster.pending_hints(), 2);

        cluster.set_online(3, true).unwrap();
        assert_eq!(cluster.replay_hints(3, usize::MAX, 30), 2);
        let on_three = cluster.replica_siblings(3, b"k").unwrap();
        assert_eq!(
            resolve_siblings(&on_three, ConflictPolicy::KeepSiblings),
            ResolvedValue::Deleted
        );
        assert_eq!(on_three.versions().len(), 1);
    }

    #[test]
    fn hints_are_bounded_and_anti_entropy_recovers_when_hint_was_dropped() {
        let mut cluster = cluster(0);
        cluster.set_online(3, false).unwrap();
        let write = cluster
            .put(1, &[1, 2, 3], b"k", b"v".to_vec(), None, 10)
            .unwrap();
        assert!(write.succeeded);
        assert_eq!(write.hints_created, 0);
        assert_eq!(cluster.dropped_hints(), 1);

        cluster.set_online(3, true).unwrap();
        assert_eq!(
            resolve_siblings(
                &cluster.replica_siblings(3, b"k").unwrap(),
                ConflictPolicy::KeepSiblings
            ),
            ResolvedValue::Missing
        );

        assert_eq!(cluster.anti_entropy_key(&[1, 2, 3], b"k", 20).unwrap(), 1);
        assert_eq!(
            resolve_siblings(
                &cluster.replica_siblings(3, b"k").unwrap(),
                ConflictPolicy::KeepSiblings
            ),
            ResolvedValue::Value(b"v".to_vec())
        );
    }

    #[test]
    fn causal_read_waits_until_required_context_reaches_a_replica() {
        let mut cluster = cluster(16);
        cluster.set_online(3, false).unwrap();
        let write = cluster
            .put(1, &[1, 2, 3], b"k", b"v".to_vec(), None, 10)
            .unwrap();
        assert!(write.succeeded);

        cluster.set_online(1, false).unwrap();
        cluster.set_online(2, false).unwrap();
        cluster.set_online(3, true).unwrap();
        assert_eq!(
            cluster.read_causal(&[1, 2, 3], b"k", &write.context),
            Err(DataError::CausalNotReady)
        );

        cluster.set_online(2, true).unwrap();
        assert_eq!(cluster.replay_hints(3, 1, 20), 1);
        cluster.set_online(2, false).unwrap();
        let read = cluster
            .read_causal(&[1, 2, 3], b"k", &write.context)
            .unwrap();
        assert!(read.context.dominates_or_equals(&write.context));
    }

    #[test]
    fn explicit_hlc_lww_is_deterministic_but_not_default() {
        let mut siblings = SiblingSet::default();
        siblings.merge_version(VersionedValue {
            value: Some(b"a".to_vec()),
            context: {
                let mut v = VersionVector::default();
                v.increment(1);
                v
            },
            timestamp: HlcTimestamp {
                physical_ms: 100,
                logical: 0,
                node_id: 1,
            },
            origin: 1,
        });
        siblings.merge_version(VersionedValue {
            value: Some(b"b".to_vec()),
            context: {
                let mut v = VersionVector::default();
                v.increment(2);
                v
            },
            timestamp: HlcTimestamp {
                physical_ms: 101,
                logical: 0,
                node_id: 2,
            },
            origin: 2,
        });

        assert!(matches!(
            resolve_siblings(&siblings, ConflictPolicy::KeepSiblings),
            ResolvedValue::Conflict(_)
        ));
        assert_eq!(
            resolve_siblings(&siblings, ConflictPolicy::LastWriterWinsHlc),
            ResolvedValue::Value(b"b".to_vec())
        );
    }
}
