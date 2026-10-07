use std::collections::BTreeMap;

use crate::model::{AdminState, Cluster, Node, NodeId, Tablet};
use crate::range_resize::{RangeResizeError, RangeTablet, RangeTabletMap, TabletRangeLifecycle};

const SNAPSHOT_MAGIC: [u8; 4] = *b"FTS2";
const SNAPSHOT_MIN_BYTES: usize = 4 + 8 + 4 + 4 + 8 + 8 + 1 + 8 + 4 + 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologySnapshotError {
    InvalidLength,
    InvalidMagic,
    InvalidChecksum,
    InvalidUtf8,
    InvalidAdminState,
    InvalidNodeMap,
    TooManyNodes,
    TooManyTablets,
    TooManyReplicas,
    StringTooLong,
    ReplicationFactorOverflow,
    Range(RangeResizeError),
}

impl From<RangeResizeError> for TopologySnapshotError {
    fn from(value: RangeResizeError) -> Self {
        Self::Range(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopologySnapshot {
    topology_epoch: u64,
    replication_factor: u32,
    nodes: BTreeMap<NodeId, Node>,
    lifecycle: TabletRangeLifecycle,
}

impl TopologySnapshot {
    pub fn new(topology_epoch: u64, lifecycle: TabletRangeLifecycle) -> Self {
        Self {
            topology_epoch,
            replication_factor: 0,
            nodes: BTreeMap::new(),
            lifecycle,
        }
    }

    pub fn from_lifecycle(topology_epoch: u64, lifecycle: &TabletRangeLifecycle) -> Self {
        Self::new(topology_epoch, lifecycle.clone())
    }

    pub fn from_runtime(
        cluster: &Cluster,
        lifecycle: &TabletRangeLifecycle,
    ) -> Result<Self, TopologySnapshotError> {
        let replication_factor = u32::try_from(cluster.replication_factor)
            .map_err(|_| TopologySnapshotError::ReplicationFactorOverflow)?;
        if cluster
            .nodes
            .iter()
            .any(|(node_id, node)| *node_id != node.id)
        {
            return Err(TopologySnapshotError::InvalidNodeMap);
        }
        Ok(Self {
            topology_epoch: cluster.epoch,
            replication_factor,
            nodes: cluster.nodes.clone(),
            lifecycle: lifecycle.clone(),
        })
    }

    pub fn topology_epoch(&self) -> u64 {
        self.topology_epoch
    }

    pub fn replication_factor(&self) -> usize {
        self.replication_factor as usize
    }

    pub fn nodes(&self) -> &BTreeMap<NodeId, Node> {
        &self.nodes
    }

    pub fn lifecycle(&self) -> &TabletRangeLifecycle {
        &self.lifecycle
    }

    pub fn into_lifecycle(self) -> TabletRangeLifecycle {
        self.lifecycle
    }

    pub fn cluster(&self) -> Cluster {
        Cluster {
            epoch: self.topology_epoch,
            replication_factor: self.replication_factor(),
            nodes: self.nodes.clone(),
            tablets: self
                .lifecycle
                .map()
                .tablets()
                .iter()
                .map(|tablet| Tablet {
                    id: tablet.id,
                    bytes: tablet.bytes,
                })
                .collect(),
        }
    }

    pub fn with_lifecycle(&self, lifecycle: TabletRangeLifecycle) -> Self {
        Self {
            topology_epoch: self.topology_epoch,
            replication_factor: self.replication_factor,
            nodes: self.nodes.clone(),
            lifecycle,
        }
    }

    pub fn with_cluster(&self, cluster: &Cluster) -> Result<Self, TopologySnapshotError> {
        let replication_factor = u32::try_from(cluster.replication_factor)
            .map_err(|_| TopologySnapshotError::ReplicationFactorOverflow)?;
        if cluster
            .nodes
            .iter()
            .any(|(node_id, node)| *node_id != node.id)
        {
            return Err(TopologySnapshotError::InvalidNodeMap);
        }
        Ok(Self {
            topology_epoch: cluster.epoch,
            replication_factor,
            nodes: cluster.nodes.clone(),
            lifecycle: self.lifecycle.clone(),
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, TopologySnapshotError> {
        let map = self.lifecycle.map();
        let tablet_count =
            u32::try_from(map.tablet_count()).map_err(|_| TopologySnapshotError::TooManyTablets)?;
        let node_count =
            u32::try_from(self.nodes.len()).map_err(|_| TopologySnapshotError::TooManyNodes)?;

        let mut bytes = Vec::with_capacity(
            SNAPSHOT_MIN_BYTES
                .saturating_add(self.nodes.len().saturating_mul(48))
                .saturating_add(map.tablet_count().saturating_mul(64)),
        );
        bytes.extend_from_slice(&SNAPSHOT_MAGIC);
        bytes.extend_from_slice(&self.topology_epoch.to_le_bytes());
        bytes.extend_from_slice(&self.replication_factor.to_le_bytes());
        bytes.extend_from_slice(&node_count.to_le_bytes());

        for (node_id, node) in &self.nodes {
            if *node_id != node.id {
                return Err(TopologySnapshotError::InvalidNodeMap);
            }
            let zone = node.zone.as_bytes();
            let rack = node.rack.as_bytes();
            let zone_len =
                u16::try_from(zone.len()).map_err(|_| TopologySnapshotError::StringTooLong)?;
            let rack_len =
                u16::try_from(rack.len()).map_err(|_| TopologySnapshotError::StringTooLong)?;

            bytes.extend_from_slice(&node.id.to_le_bytes());
            bytes.extend_from_slice(&node.weight.to_le_bytes());
            bytes.push(encode_admin_state(node.state));
            bytes.extend_from_slice(&zone_len.to_le_bytes());
            bytes.extend_from_slice(&rack_len.to_le_bytes());
            bytes.extend_from_slice(zone);
            bytes.extend_from_slice(rack);
        }

        bytes.extend_from_slice(&map.generation().to_le_bytes());
        bytes.extend_from_slice(&map.next_tablet_id().to_le_bytes());

        match self.lifecycle.last_resize_tick() {
            Some(tick) => {
                bytes.push(1);
                bytes.extend_from_slice(&tick.to_le_bytes());
            }
            None => {
                bytes.push(0);
                bytes.extend_from_slice(&0_u64.to_le_bytes());
            }
        }

        bytes.extend_from_slice(&tablet_count.to_le_bytes());

        for tablet in map.tablets() {
            let replica_count = u16::try_from(tablet.replicas.len())
                .map_err(|_| TopologySnapshotError::TooManyReplicas)?;
            bytes.extend_from_slice(&tablet.id.to_le_bytes());
            bytes.extend_from_slice(&tablet.start.to_le_bytes());
            bytes.extend_from_slice(&tablet.end.to_le_bytes());
            bytes.extend_from_slice(&tablet.bytes.to_le_bytes());
            bytes.extend_from_slice(&replica_count.to_le_bytes());
            for replica in &tablet.replicas {
                bytes.extend_from_slice(&replica.to_le_bytes());
            }
        }

        let checksum = checksum64(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, TopologySnapshotError> {
        if bytes.len() < SNAPSHOT_MIN_BYTES {
            return Err(TopologySnapshotError::InvalidLength);
        }
        let payload_len = bytes
            .len()
            .checked_sub(8)
            .ok_or(TopologySnapshotError::InvalidLength)?;
        let expected_checksum = u64::from_le_bytes(
            bytes[payload_len..]
                .try_into()
                .map_err(|_| TopologySnapshotError::InvalidLength)?,
        );
        if checksum64(&bytes[..payload_len]) != expected_checksum {
            return Err(TopologySnapshotError::InvalidChecksum);
        }

        let mut cursor = Cursor::new(&bytes[..payload_len]);
        if cursor.read_exact::<4>()? != SNAPSHOT_MAGIC {
            return Err(TopologySnapshotError::InvalidMagic);
        }

        let topology_epoch = cursor.read_u64()?;
        let replication_factor = cursor.read_u32()?;
        let node_count = cursor.read_u32()? as usize;
        let mut nodes = BTreeMap::new();

        for _ in 0..node_count {
            let id = cursor.read_u64()?;
            let weight = cursor.read_u32()?;
            let state = decode_admin_state(cursor.read_u8()?)?;
            let zone_len = cursor.read_u16()? as usize;
            let rack_len = cursor.read_u16()? as usize;
            let zone = String::from_utf8(cursor.read_vec(zone_len)?)
                .map_err(|_| TopologySnapshotError::InvalidUtf8)?;
            let rack = String::from_utf8(cursor.read_vec(rack_len)?)
                .map_err(|_| TopologySnapshotError::InvalidUtf8)?;
            let node = Node {
                id,
                weight,
                zone,
                rack,
                state,
            };
            if nodes.insert(id, node).is_some() {
                return Err(TopologySnapshotError::InvalidNodeMap);
            }
        }

        let generation = cursor.read_u64()?;
        let next_tablet_id = cursor.read_u64()?;
        let has_last_resize = cursor.read_u8()?;
        let last_resize_value = cursor.read_u64()?;
        let last_resize_tick = match has_last_resize {
            0 => None,
            1 => Some(last_resize_value),
            _ => return Err(TopologySnapshotError::InvalidLength),
        };

        let tablet_count = cursor.read_u32()? as usize;
        let mut tablets = Vec::with_capacity(tablet_count);

        for _ in 0..tablet_count {
            let id = cursor.read_u64()?;
            let start = cursor.read_u128()?;
            let end = cursor.read_u128()?;
            let tablet_bytes = cursor.read_u64()?;
            let replica_count = cursor.read_u16()? as usize;
            let mut replicas = Vec::with_capacity(replica_count);
            for _ in 0..replica_count {
                replicas.push(cursor.read_u64()?);
            }
            tablets.push(RangeTablet {
                id,
                start,
                end,
                bytes: tablet_bytes,
                replicas,
            });
        }

        if !cursor.is_empty() {
            return Err(TopologySnapshotError::InvalidLength);
        }

        let map = RangeTabletMap::from_parts(generation, next_tablet_id, tablets)?;
        let lifecycle = TabletRangeLifecycle::from_parts(map, last_resize_tick)?;
        Ok(Self {
            topology_epoch,
            replication_factor,
            nodes,
            lifecycle,
        })
    }

    pub fn stable_checksum(&self) -> Result<u64, TopologySnapshotError> {
        Ok(checksum64(&self.encode()?))
    }
}

fn encode_admin_state(state: AdminState) -> u8 {
    match state {
        AdminState::Joining => 0,
        AdminState::Active => 1,
        AdminState::Draining => 2,
        AdminState::Removed => 3,
    }
}

fn decode_admin_state(value: u8) -> Result<AdminState, TopologySnapshotError> {
    match value {
        0 => Ok(AdminState::Joining),
        1 => Ok(AdminState::Active),
        2 => Ok(AdminState::Draining),
        3 => Ok(AdminState::Removed),
        _ => Err(TopologySnapshotError::InvalidAdminState),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N], TopologySnapshotError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or(TopologySnapshotError::InvalidLength)?;
        let chunk = self
            .bytes
            .get(self.offset..end)
            .ok_or(TopologySnapshotError::InvalidLength)?;
        self.offset = end;
        chunk
            .try_into()
            .map_err(|_| TopologySnapshotError::InvalidLength)
    }

    fn read_vec(&mut self, len: usize) -> Result<Vec<u8>, TopologySnapshotError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(TopologySnapshotError::InvalidLength)?;
        let chunk = self
            .bytes
            .get(self.offset..end)
            .ok_or(TopologySnapshotError::InvalidLength)?;
        self.offset = end;
        Ok(chunk.to_vec())
    }

    fn read_u8(&mut self) -> Result<u8, TopologySnapshotError> {
        Ok(self.read_exact::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, TopologySnapshotError> {
        Ok(u16::from_le_bytes(self.read_exact()?))
    }

    fn read_u32(&mut self) -> Result<u32, TopologySnapshotError> {
        Ok(u32::from_le_bytes(self.read_exact()?))
    }

    fn read_u64(&mut self) -> Result<u64, TopologySnapshotError> {
        Ok(u64::from_le_bytes(self.read_exact()?))
    }

    fn read_u128(&mut self) -> Result<u128, TopologySnapshotError> {
        Ok(u128::from_le_bytes(self.read_exact()?))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::range_resize::{LifecycleResizeDecision, RangeTabletMap};
    use crate::resize::TabletResizePolicy;

    fn policy() -> TabletResizePolicy {
        TabletResizePolicy {
            target_tablet_bytes: 100,
            split_above_num: 2,
            split_above_den: 1,
            merge_below_num: 1,
            merge_below_den: 2,
            cooldown_ticks: 10,
            min_tablets: 1,
            max_tablets: 1024,
            metadata_bytes_per_tablet: 24,
            metadata_budget_bytes: 1024 * 24,
        }
    }

    fn lifecycle_after_splits(rounds: usize) -> TabletRangeLifecycle {
        let map = RangeTabletMap::single(10_000, 1_000_000, vec![3, 1, 2]).unwrap();
        let mut lifecycle = TabletRangeLifecycle::new(map).unwrap();

        for round in 0..rounds {
            let total = lifecycle.map().total_bytes();
            let plan = match lifecycle
                .evaluate(total, 100 + round as u64 * 20, 7, &policy())
                .unwrap()
            {
                LifecycleResizeDecision::Planned(plan) => plan,
                other => panic!("expected split plan, got {other:?}"),
            };
            lifecycle.commit(&plan, 7, 101 + round as u64 * 20).unwrap();
        }
        lifecycle
    }

    #[test]
    fn snapshot_round_trip_preserves_full_range_lifecycle() {
        let lifecycle = lifecycle_after_splits(3);
        let snapshot = TopologySnapshot::from_lifecycle(7, &lifecycle);
        let encoded = snapshot.encode().unwrap();
        let decoded = TopologySnapshot::decode(&encoded).unwrap();

        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.topology_epoch(), 7);
        assert_eq!(decoded.lifecycle().map(), lifecycle.map());
        assert_eq!(
            decoded.lifecycle().last_resize_tick(),
            lifecycle.last_resize_tick()
        );
    }

    #[test]
    fn snapshot_preserves_full_u64_hash_space_end() {
        let lifecycle = lifecycle_after_splits(2);
        let decoded = TopologySnapshot::decode(
            &TopologySnapshot::from_lifecycle(9, &lifecycle)
                .encode()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(
            decoded.lifecycle().map().tablets().last().unwrap().end,
            crate::range_resize::HASH_SPACE_END
        );
        assert_eq!(
            decoded.lifecycle().map().route(u64::MAX).unwrap().id,
            lifecycle.map().route(u64::MAX).unwrap().id
        );
    }

    #[test]
    fn snapshot_detects_single_byte_corruption() {
        let lifecycle = lifecycle_after_splits(1);
        let mut encoded = TopologySnapshot::from_lifecycle(7, &lifecycle)
            .encode()
            .unwrap();
        encoded[40] ^= 0x80;

        assert_eq!(
            TopologySnapshot::decode(&encoded),
            Err(TopologySnapshotError::InvalidChecksum)
        );
    }

    #[test]
    fn snapshot_rejects_truncation() {
        let lifecycle = lifecycle_after_splits(1);
        let mut encoded = TopologySnapshot::from_lifecycle(7, &lifecycle)
            .encode()
            .unwrap();
        encoded.truncate(encoded.len() - 5);

        assert!(matches!(
            TopologySnapshot::decode(&encoded),
            Err(TopologySnapshotError::InvalidChecksum | TopologySnapshotError::InvalidLength)
        ));
    }

    #[test]
    fn snapshot_encoding_is_deterministic() {
        let lifecycle = lifecycle_after_splits(3);
        let a = TopologySnapshot::from_lifecycle(7, &lifecycle)
            .encode()
            .unwrap();
        let b = TopologySnapshot::from_lifecycle(7, &lifecycle)
            .encode()
            .unwrap();

        assert_eq!(a, b);
        assert_eq!(
            TopologySnapshot::decode(&a)
                .unwrap()
                .stable_checksum()
                .unwrap(),
            TopologySnapshot::decode(&b)
                .unwrap()
                .stable_checksum()
                .unwrap()
        );
    }
}

const CURRENT_TOPOLOGY_KEY: &[u8] = b"topology/current";
const PREPARED_TOPOLOGY_KEY: &[u8] = b"topology/prepared";
const PREPARED_MAGIC: [u8; 4] = *b"FPT1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedTopologyTxn {
    pub txn_id: u64,
    pub from_topology_epoch: u64,
    pub from_generation: u64,
    pub target: TopologySnapshot,
}

impl PreparedTopologyTxn {
    pub fn new(
        txn_id: u64,
        current: &TopologySnapshot,
        target: TopologySnapshot,
    ) -> Result<Self, TopologyTxnError> {
        let current_identity = (
            current.topology_epoch(),
            current.lifecycle().map().generation(),
        );
        let target_identity = (
            target.topology_epoch(),
            target.lifecycle().map().generation(),
        );
        if target_identity <= current_identity {
            return Err(TopologyTxnError::InvalidTransition);
        }
        Ok(Self {
            txn_id,
            from_topology_epoch: current_identity.0,
            from_generation: current_identity.1,
            target,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, TopologyTxnError> {
        let target = self.target.encode()?;
        let target_len =
            u32::try_from(target.len()).map_err(|_| TopologyTxnError::InvalidRecord)?;
        let mut bytes = Vec::with_capacity(4 + 8 + 8 + 8 + 4 + target.len() + 8);
        bytes.extend_from_slice(&PREPARED_MAGIC);
        bytes.extend_from_slice(&self.txn_id.to_le_bytes());
        bytes.extend_from_slice(&self.from_topology_epoch.to_le_bytes());
        bytes.extend_from_slice(&self.from_generation.to_le_bytes());
        bytes.extend_from_slice(&target_len.to_le_bytes());
        bytes.extend_from_slice(&target);
        let checksum = checksum64(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, TopologyTxnError> {
        if bytes.len() < 40 {
            return Err(TopologyTxnError::InvalidRecord);
        }
        let payload_len = bytes
            .len()
            .checked_sub(8)
            .ok_or(TopologyTxnError::InvalidRecord)?;
        let expected_checksum = u64::from_le_bytes(
            bytes[payload_len..]
                .try_into()
                .map_err(|_| TopologyTxnError::InvalidRecord)?,
        );
        if checksum64(&bytes[..payload_len]) != expected_checksum {
            return Err(TopologyTxnError::InvalidRecord);
        }

        let mut cursor = Cursor::new(&bytes[..payload_len]);
        if cursor.read_exact::<4>()? != PREPARED_MAGIC {
            return Err(TopologyTxnError::InvalidRecord);
        }
        let txn_id = cursor.read_u64()?;
        let from_topology_epoch = cursor.read_u64()?;
        let from_generation = cursor.read_u64()?;
        let target_len = cursor.read_u32()? as usize;
        let end = cursor
            .offset
            .checked_add(target_len)
            .ok_or(TopologyTxnError::InvalidRecord)?;
        let target_bytes = cursor
            .bytes
            .get(cursor.offset..end)
            .ok_or(TopologyTxnError::InvalidRecord)?;
        cursor.offset = end;
        if !cursor.is_empty() {
            return Err(TopologyTxnError::InvalidRecord);
        }
        let target = TopologySnapshot::decode(target_bytes)?;
        Ok(Self {
            txn_id,
            from_topology_epoch,
            from_generation,
            target,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyTxnError {
    Disk(crate::disk::DiskError),
    Snapshot(TopologySnapshotError),
    InvalidRecord,
    InvalidTransition,
    MissingCurrent,
    CurrentMismatch,
    ActivePrepared { txn_id: u64 },
    PendingRead,
    RecoveryConflict,
}

impl From<crate::disk::DiskError> for TopologyTxnError {
    fn from(value: crate::disk::DiskError) -> Self {
        Self::Disk(value)
    }
}

impl From<TopologySnapshotError> for TopologyTxnError {
    fn from(value: TopologySnapshotError) -> Self {
        Self::Snapshot(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyTxnState {
    PrepareIdle,
    PreparePutPending,
    PrepareSyncPending,
    Prepared,
    PublishIdle,
    PublishPutPending,
    PublishSyncPending,
    Complete,
    Failed {
        error: crate::disk::DiskError,
        resume_publish: bool,
    },
}

#[derive(Clone, Debug)]
pub struct DurableTopologyTxnWriter {
    prepared: PreparedTopologyTxn,
    prepared_bytes: Vec<u8>,
    target_bytes: Vec<u8>,
    state: TopologyTxnState,
    next_op_id: crate::disk::DiskOpId,
    active_op: Option<crate::disk::DiskOpId>,
}

impl DurableTopologyTxnWriter {
    pub fn new(
        txn_id: u64,
        current: &TopologySnapshot,
        target: TopologySnapshot,
    ) -> Result<Self, TopologyTxnError> {
        let prepared = PreparedTopologyTxn::new(txn_id, current, target)?;
        let prepared_bytes = prepared.encode()?;
        let target_bytes = prepared.target.encode()?;
        Ok(Self {
            prepared,
            prepared_bytes,
            target_bytes,
            state: TopologyTxnState::PrepareIdle,
            next_op_id: txn_id.saturating_mul(16).saturating_add(1),
            active_op: None,
        })
    }

    pub fn resume_publish(prepared: PreparedTopologyTxn) -> Result<Self, TopologyTxnError> {
        let prepared_bytes = prepared.encode()?;
        let target_bytes = prepared.target.encode()?;
        Ok(Self {
            next_op_id: prepared.txn_id.saturating_mul(16).saturating_add(9),
            prepared,
            prepared_bytes,
            target_bytes,
            state: TopologyTxnState::PublishIdle,
            active_op: None,
        })
    }

    pub fn state(&self) -> TopologyTxnState {
        self.state
    }

    pub fn prepared_record(&self) -> &PreparedTopologyTxn {
        &self.prepared
    }

    pub fn is_prepared(&self) -> bool {
        matches!(
            self.state,
            TopologyTxnState::Prepared
                | TopologyTxnState::PublishIdle
                | TopologyTxnState::PublishPutPending
                | TopologyTxnState::PublishSyncPending
                | TopologyTxnState::Complete
        )
    }

    pub fn is_complete(&self) -> bool {
        self.state == TopologyTxnState::Complete
    }

    pub fn mark_applied(&mut self) -> bool {
        if self.state != TopologyTxnState::Prepared {
            return false;
        }
        self.state = TopologyTxnState::PublishIdle;
        true
    }

    pub fn retry(&mut self) {
        if let TopologyTxnState::Failed { resume_publish, .. } = self.state {
            self.active_op = None;
            self.state = if resume_publish {
                TopologyTxnState::PublishIdle
            } else {
                TopologyTxnState::PrepareIdle
            };
        }
    }

    pub fn tick<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        match self.state {
            TopologyTxnState::PrepareIdle => self.submit_prepared_put(now_tick, store),
            TopologyTxnState::PreparePutPending => self.poll_prepared_put(now_tick, store),
            TopologyTxnState::PrepareSyncPending => self.poll_prepared_sync(now_tick, store),
            TopologyTxnState::Prepared => {}
            TopologyTxnState::PublishIdle => self.submit_current_put(now_tick, store),
            TopologyTxnState::PublishPutPending => self.poll_current_put(now_tick, store),
            TopologyTxnState::PublishSyncPending => self.poll_current_sync(now_tick, store),
            TopologyTxnState::Complete | TopologyTxnState::Failed { .. } => {}
        }
    }

    fn alloc_op_id(&mut self) -> crate::disk::DiskOpId {
        let op_id = self.next_op_id;
        self.next_op_id = self.next_op_id.saturating_add(1);
        op_id
    }

    fn fail(&mut self, error: crate::disk::DiskError, resume_publish: bool) {
        self.active_op = None;
        self.state = TopologyTxnState::Failed {
            error,
            resume_publish,
        };
    }

    fn submit_prepared_put<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let op_id = self.alloc_op_id();
        match store.submit(
            now_tick,
            crate::disk::DiskRequest::Put {
                op_id,
                key: PREPARED_TOPOLOGY_KEY.to_vec(),
                value: self.prepared_bytes.clone(),
            },
        ) {
            crate::disk::DiskSubmit::Completed(_) => self.submit_prepared_sync(now_tick, store),
            crate::disk::DiskSubmit::Pending => {
                self.active_op = Some(op_id);
                self.state = TopologyTxnState::PreparePutPending;
            }
            crate::disk::DiskSubmit::Failed(error) => self.fail(error, false),
        }
    }

    fn poll_prepared_put<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let Some(op_id) = self.active_op else {
            self.fail(crate::disk::DiskError::Cancelled, false);
            return;
        };
        match store.poll(now_tick, op_id) {
            crate::disk::DiskPoll::Pending => {}
            crate::disk::DiskPoll::Completed(_) => {
                self.active_op = None;
                self.submit_prepared_sync(now_tick, store);
            }
            crate::disk::DiskPoll::Failed(error) => self.fail(error, false),
        }
    }

    fn submit_prepared_sync<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let op_id = self.alloc_op_id();
        match store.submit(now_tick, crate::disk::DiskRequest::Sync { op_id }) {
            crate::disk::DiskSubmit::Completed(_) => {
                self.active_op = None;
                self.state = TopologyTxnState::Prepared;
            }
            crate::disk::DiskSubmit::Pending => {
                self.active_op = Some(op_id);
                self.state = TopologyTxnState::PrepareSyncPending;
            }
            crate::disk::DiskSubmit::Failed(error) => self.fail(error, false),
        }
    }

    fn poll_prepared_sync<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let Some(op_id) = self.active_op else {
            self.fail(crate::disk::DiskError::Cancelled, false);
            return;
        };
        match store.poll(now_tick, op_id) {
            crate::disk::DiskPoll::Pending => {}
            crate::disk::DiskPoll::Completed(_) => {
                self.active_op = None;
                self.state = TopologyTxnState::Prepared;
            }
            crate::disk::DiskPoll::Failed(error) => self.fail(error, false),
        }
    }

    fn submit_current_put<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let op_id = self.alloc_op_id();
        match store.submit(
            now_tick,
            crate::disk::DiskRequest::Put {
                op_id,
                key: CURRENT_TOPOLOGY_KEY.to_vec(),
                value: self.target_bytes.clone(),
            },
        ) {
            crate::disk::DiskSubmit::Completed(_) => self.submit_current_sync(now_tick, store),
            crate::disk::DiskSubmit::Pending => {
                self.active_op = Some(op_id);
                self.state = TopologyTxnState::PublishPutPending;
            }
            crate::disk::DiskSubmit::Failed(error) => self.fail(error, true),
        }
    }

    fn poll_current_put<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let Some(op_id) = self.active_op else {
            self.fail(crate::disk::DiskError::Cancelled, true);
            return;
        };
        match store.poll(now_tick, op_id) {
            crate::disk::DiskPoll::Pending => {}
            crate::disk::DiskPoll::Completed(_) => {
                self.active_op = None;
                self.submit_current_sync(now_tick, store);
            }
            crate::disk::DiskPoll::Failed(error) => self.fail(error, true),
        }
    }

    fn submit_current_sync<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let op_id = self.alloc_op_id();
        match store.submit(now_tick, crate::disk::DiskRequest::Sync { op_id }) {
            crate::disk::DiskSubmit::Completed(_) => {
                self.active_op = None;
                self.state = TopologyTxnState::Complete;
            }
            crate::disk::DiskSubmit::Pending => {
                self.active_op = Some(op_id);
                self.state = TopologyTxnState::PublishSyncPending;
            }
            crate::disk::DiskSubmit::Failed(error) => self.fail(error, true),
        }
    }

    fn poll_current_sync<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let Some(op_id) = self.active_op else {
            self.fail(crate::disk::DiskError::Cancelled, true);
            return;
        };
        match store.poll(now_tick, op_id) {
            crate::disk::DiskPoll::Pending => {}
            crate::disk::DiskPoll::Completed(_) => {
                self.active_op = None;
                self.state = TopologyTxnState::Complete;
            }
            crate::disk::DiskPoll::Failed(error) => self.fail(error, true),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyTxnStartState {
    Clean,
    StaleCommitted { txn_id: u64 },
}

pub fn validate_topology_txn_start<S: crate::disk::DurableStore>(
    now_tick: u64,
    expected_current: &TopologySnapshot,
    store: &mut S,
) -> Result<TopologyTxnStartState, TopologyTxnError> {
    let current = read_current_topology(now_tick, u64::MAX - 10, store)?
        .ok_or(TopologyTxnError::MissingCurrent)?;
    if &current != expected_current {
        return Err(TopologyTxnError::CurrentMismatch);
    }

    let prepared = read_prepared_topology(now_tick, u64::MAX - 9, store)?;
    let Some(prepared) = prepared else {
        return Ok(TopologyTxnStartState::Clean);
    };

    if current == prepared.target {
        return Ok(TopologyTxnStartState::StaleCommitted {
            txn_id: prepared.txn_id,
        });
    }

    let current_identity = (
        current.topology_epoch(),
        current.lifecycle().map().generation(),
    );
    if current_identity == (prepared.from_topology_epoch, prepared.from_generation) {
        return Err(TopologyTxnError::ActivePrepared {
            txn_id: prepared.txn_id,
        });
    }

    Err(TopologyTxnError::RecoveryConflict)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TopologyRecovery {
    Empty,
    Current(TopologySnapshot),
    ReplayPrepared {
        txn_id: u64,
        target: TopologySnapshot,
    },
}

pub fn read_current_topology<S: crate::disk::DurableStore>(
    now_tick: u64,
    op_id: crate::disk::DiskOpId,
    store: &mut S,
) -> Result<Option<TopologySnapshot>, TopologyTxnError> {
    read_blob(now_tick, op_id, CURRENT_TOPOLOGY_KEY, store)?
        .map(|bytes| TopologySnapshot::decode(&bytes).map_err(TopologyTxnError::from))
        .transpose()
}

pub fn read_prepared_topology<S: crate::disk::DurableStore>(
    now_tick: u64,
    op_id: crate::disk::DiskOpId,
    store: &mut S,
) -> Result<Option<PreparedTopologyTxn>, TopologyTxnError> {
    read_blob(now_tick, op_id, PREPARED_TOPOLOGY_KEY, store)?
        .map(|bytes| PreparedTopologyTxn::decode(&bytes))
        .transpose()
}

pub fn recover_topology<S: crate::disk::DurableStore>(
    now_tick: u64,
    store: &mut S,
) -> Result<TopologyRecovery, TopologyTxnError> {
    let current = read_current_topology(now_tick, u64::MAX - 1, store)?;
    let prepared = read_prepared_topology(now_tick, u64::MAX, store)?;

    match (current, prepared) {
        (None, None) => Ok(TopologyRecovery::Empty),
        (Some(current), None) => Ok(TopologyRecovery::Current(current)),
        (None, Some(prepared)) => Ok(TopologyRecovery::ReplayPrepared {
            txn_id: prepared.txn_id,
            target: prepared.target,
        }),
        (Some(current), Some(prepared)) => {
            if current == prepared.target {
                return Ok(TopologyRecovery::Current(current));
            }
            let current_identity = (
                current.topology_epoch(),
                current.lifecycle().map().generation(),
            );
            if current_identity == (prepared.from_topology_epoch, prepared.from_generation) {
                return Ok(TopologyRecovery::ReplayPrepared {
                    txn_id: prepared.txn_id,
                    target: prepared.target,
                });
            }
            Err(TopologyTxnError::RecoveryConflict)
        }
    }
}

fn read_blob<S: crate::disk::DurableStore>(
    now_tick: u64,
    op_id: crate::disk::DiskOpId,
    key: &[u8],
    store: &mut S,
) -> Result<Option<Vec<u8>>, TopologyTxnError> {
    match store.submit(
        now_tick,
        crate::disk::DiskRequest::Read {
            op_id,
            key: key.to_vec(),
        },
    ) {
        crate::disk::DiskSubmit::Completed(crate::disk::DiskCompletion::Read(value)) => Ok(value),
        crate::disk::DiskSubmit::Completed(crate::disk::DiskCompletion::Unit) => {
            Err(TopologyTxnError::InvalidRecord)
        }
        crate::disk::DiskSubmit::Pending => Err(TopologyTxnError::PendingRead),
        crate::disk::DiskSubmit::Failed(error) => Err(TopologyTxnError::Disk(error)),
    }
}

#[cfg(test)]
mod txn_tests {
    use super::*;
    use crate::disk::{DiskRequest, DurableStore, SimDisk};
    use crate::range_resize::{LifecycleResizeDecision, RangeTabletMap};
    use crate::resize::TabletResizePolicy;

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

    fn current_and_target() -> (TopologySnapshot, TopologySnapshot) {
        let map = RangeTabletMap::single(100, 1_000, vec![1, 2, 3]).unwrap();
        let current_lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let mut target_lifecycle = current_lifecycle.clone();
        let plan = match target_lifecycle.evaluate(1_000, 10, 7, &policy()).unwrap() {
            LifecycleResizeDecision::Planned(plan) => plan,
            other => panic!("expected plan, got {other:?}"),
        };
        target_lifecycle.commit(&plan, 7, 11).unwrap();

        (
            TopologySnapshot::from_lifecycle(7, &current_lifecycle),
            TopologySnapshot::from_lifecycle(7, &target_lifecycle),
        )
    }

    fn write_current(disk: &mut SimDisk, snapshot: &TopologySnapshot) {
        disk.set_delay(0);
        assert!(matches!(
            disk.submit(
                0,
                DiskRequest::Put {
                    op_id: 90_000,
                    key: CURRENT_TOPOLOGY_KEY.to_vec(),
                    value: snapshot.encode().unwrap(),
                },
            ),
            crate::disk::DiskSubmit::Completed(_)
        ));
        assert!(matches!(
            disk.submit(0, DiskRequest::Sync { op_id: 90_001 }),
            crate::disk::DiskSubmit::Completed(_)
        ));
    }

    #[test]
    fn prepared_snapshot_is_replayable_before_publish() {
        let (current, target) = current_and_target();
        let mut disk = SimDisk::default();
        write_current(&mut disk, &current);

        let mut writer = DurableTopologyTxnWriter::new(7, &current, target.clone()).unwrap();
        writer.tick(1, &mut disk);
        assert_eq!(writer.state(), TopologyTxnState::Prepared);

        disk.crash();
        assert_eq!(
            recover_topology(2, &mut disk).unwrap(),
            TopologyRecovery::ReplayPrepared {
                txn_id: 7,
                target: target.clone(),
            }
        );

        assert!(writer.mark_applied());
        writer.tick(3, &mut disk);
        assert!(writer.is_complete());
        disk.crash();

        assert_eq!(
            recover_topology(4, &mut disk).unwrap(),
            TopologyRecovery::Current(target)
        );
    }

    #[test]
    fn crash_before_prepared_sync_keeps_old_current_authoritative() {
        let (current, target) = current_and_target();
        let mut disk = SimDisk::default();
        write_current(&mut disk, &current);

        disk.set_delay(3);
        let mut writer = DurableTopologyTxnWriter::new(8, &current, target).unwrap();
        writer.tick(10, &mut disk);
        assert_eq!(writer.state(), TopologyTxnState::PreparePutPending);

        disk.crash();
        writer.tick(13, &mut disk);
        assert!(matches!(
            writer.state(),
            TopologyTxnState::Failed {
                error: crate::disk::DiskError::Cancelled,
                resume_publish: false,
            }
        ));

        disk.set_delay(0);
        assert_eq!(
            recover_topology(14, &mut disk).unwrap(),
            TopologyRecovery::Current(current)
        );
    }

    #[test]
    fn crash_after_publish_put_before_sync_replays_prepared_target() {
        let (current, target) = current_and_target();
        let mut disk = SimDisk::default();
        write_current(&mut disk, &current);

        let mut writer = DurableTopologyTxnWriter::new(9, &current, target.clone()).unwrap();
        writer.tick(1, &mut disk);
        assert_eq!(writer.state(), TopologyTxnState::Prepared);
        assert!(writer.mark_applied());

        disk.set_delay(2);
        writer.tick(10, &mut disk);
        assert_eq!(writer.state(), TopologyTxnState::PublishPutPending);
        writer.tick(12, &mut disk);
        assert_eq!(writer.state(), TopologyTxnState::PublishSyncPending);

        disk.crash();
        writer.tick(14, &mut disk);
        disk.set_delay(0);

        assert_eq!(
            recover_topology(15, &mut disk).unwrap(),
            TopologyRecovery::ReplayPrepared { txn_id: 9, target }
        );
    }

    #[test]
    fn recovery_detects_prepared_transaction_against_unrelated_current() {
        let (current, target) = current_and_target();
        let mut disk = SimDisk::default();
        write_current(&mut disk, &current);

        let mut writer = DurableTopologyTxnWriter::new(10, &current, target.clone()).unwrap();
        writer.tick(1, &mut disk);
        assert!(writer.is_prepared());

        let mut newer_lifecycle = target.lifecycle().clone();
        let plan = match newer_lifecycle
            .evaluate(newer_lifecycle.map().total_bytes(), 20, 7, &policy())
            .unwrap()
        {
            LifecycleResizeDecision::Planned(plan) => plan,
            other => panic!("expected second plan, got {other:?}"),
        };
        newer_lifecycle.commit(&plan, 7, 21).unwrap();
        let newer = TopologySnapshot::from_lifecycle(7, &newer_lifecycle);
        write_current(&mut disk, &newer);

        assert_eq!(
            recover_topology(22, &mut disk),
            Err(TopologyTxnError::RecoveryConflict)
        );
    }

    #[test]
    fn corrupted_prepared_record_is_rejected_not_replayed() {
        let (current, target) = current_and_target();
        let mut disk = SimDisk::default();
        write_current(&mut disk, &current);

        let mut writer = DurableTopologyTxnWriter::new(11, &current, target).unwrap();
        writer.tick(1, &mut disk);
        assert!(writer.is_prepared());

        disk.corrupt_next_read(1);
        assert!(matches!(
            recover_topology(2, &mut disk),
            Err(TopologyTxnError::InvalidRecord)
                | Err(TopologyTxnError::Snapshot(
                    TopologySnapshotError::InvalidChecksum
                ))
        ));
    }
}

#[cfg(test)]
mod full_cluster_snapshot_tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::{AdminState, Cluster, Node};
    use crate::range_resize::{RangeTabletMap, TabletRangeLifecycle};

    fn node(id: u64, weight: u32, zone: &str, rack: &str, state: AdminState) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: rack.into(),
            state,
        }
    }

    #[test]
    fn full_cluster_topology_round_trip_preserves_nodes_and_rf() {
        let map = RangeTabletMap::single(50_000, 10_000, vec![1, 2, 3]).unwrap();
        let lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let cluster = Cluster {
            epoch: 42,
            replication_factor: 3,
            nodes: [
                node(1, 1, "z1", "r1", AdminState::Active),
                node(2, 2, "z2", "r2", AdminState::Joining),
                node(3, 4, "z3", "r3", AdminState::Draining),
                node(4, 8, "z4", "r4", AdminState::Removed),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: Vec::new(),
        };

        let snapshot = TopologySnapshot::from_runtime(&cluster, &lifecycle).unwrap();
        let decoded = TopologySnapshot::decode(&snapshot.encode().unwrap()).unwrap();
        let recovered_cluster = decoded.cluster();

        assert_eq!(decoded, snapshot);
        assert_eq!(recovered_cluster.epoch, cluster.epoch);
        assert_eq!(
            recovered_cluster.replication_factor,
            cluster.replication_factor
        );
        assert_eq!(recovered_cluster.nodes, cluster.nodes);
        assert_eq!(
            recovered_cluster.tablets.len(),
            lifecycle.map().tablet_count()
        );
    }

    #[test]
    fn replacing_cluster_keeps_exact_range_lifecycle() {
        let map = RangeTabletMap::single(50_000, 10_000, vec![1, 2]).unwrap();
        let lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let old = Cluster {
            epoch: 7,
            replication_factor: 2,
            nodes: [
                node(1, 1, "z1", "r1", AdminState::Active),
                node(2, 1, "z2", "r1", AdminState::Active),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: Vec::new(),
        };
        let mut new = old.clone();
        new.epoch = 8;
        new.nodes
            .insert(3, node(3, 8, "z3", "r1", AdminState::Active));

        let snapshot = TopologySnapshot::from_runtime(&old, &lifecycle).unwrap();
        let target = snapshot.with_cluster(&new).unwrap();

        assert_eq!(target.topology_epoch(), 8);
        assert_eq!(target.nodes(), &new.nodes);
        assert_eq!(target.lifecycle(), snapshot.lifecycle());
        assert_eq!(
            target.lifecycle().map().generation(),
            snapshot.lifecycle().map().generation()
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedGcState {
    NotNeeded,
    DeleteIdle,
    DeletePending,
    SyncPending,
    Complete,
    Failed(crate::disk::DiskError),
}

#[derive(Clone, Debug)]
pub struct PreparedTopologyGc {
    state: PreparedGcState,
    next_op_id: crate::disk::DiskOpId,
    active_op: Option<crate::disk::DiskOpId>,
}

impl PreparedTopologyGc {
    pub fn begin<S: crate::disk::DurableStore>(
        now_tick: u64,
        store: &mut S,
    ) -> Result<Self, TopologyTxnError> {
        let current = read_current_topology(now_tick, u64::MAX - 20, store)?;
        let prepared = read_prepared_topology(now_tick, u64::MAX - 19, store)?;

        let state = match (current, prepared) {
            (_, None) => PreparedGcState::NotNeeded,
            (Some(current), Some(prepared)) if current == prepared.target => {
                PreparedGcState::DeleteIdle
            }
            (Some(current), Some(prepared)) => {
                let current_identity = (
                    current.topology_epoch(),
                    current.lifecycle().map().generation(),
                );
                if current_identity == (prepared.from_topology_epoch, prepared.from_generation) {
                    return Err(TopologyTxnError::ActivePrepared {
                        txn_id: prepared.txn_id,
                    });
                }
                return Err(TopologyTxnError::RecoveryConflict);
            }
            (None, Some(prepared)) => {
                return Err(TopologyTxnError::ActivePrepared {
                    txn_id: prepared.txn_id,
                });
            }
        };

        Ok(Self {
            state,
            next_op_id: u64::MAX - 18,
            active_op: None,
        })
    }

    pub fn state(&self) -> PreparedGcState {
        self.state
    }

    pub fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PreparedGcState::NotNeeded | PreparedGcState::Complete
        )
    }

    pub fn retry(&mut self) {
        if matches!(self.state, PreparedGcState::Failed(_)) {
            self.active_op = None;
            self.state = PreparedGcState::DeleteIdle;
        }
    }

    pub fn tick<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        match self.state {
            PreparedGcState::NotNeeded | PreparedGcState::Complete => {}
            PreparedGcState::DeleteIdle => self.submit_delete(now_tick, store),
            PreparedGcState::DeletePending => self.poll_delete(now_tick, store),
            PreparedGcState::SyncPending => self.poll_sync(now_tick, store),
            PreparedGcState::Failed(_) => {}
        }
    }

    fn alloc_op_id(&mut self) -> crate::disk::DiskOpId {
        let op_id = self.next_op_id;
        self.next_op_id = self.next_op_id.saturating_sub(1);
        op_id
    }

    fn submit_delete<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let op_id = self.alloc_op_id();
        match store.submit(
            now_tick,
            crate::disk::DiskRequest::Delete {
                op_id,
                key: PREPARED_TOPOLOGY_KEY.to_vec(),
            },
        ) {
            crate::disk::DiskSubmit::Completed(_) => self.submit_sync(now_tick, store),
            crate::disk::DiskSubmit::Pending => {
                self.active_op = Some(op_id);
                self.state = PreparedGcState::DeletePending;
            }
            crate::disk::DiskSubmit::Failed(error) => {
                self.active_op = None;
                self.state = PreparedGcState::Failed(error);
            }
        }
    }

    fn poll_delete<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let Some(op_id) = self.active_op else {
            self.state = PreparedGcState::Failed(crate::disk::DiskError::Cancelled);
            return;
        };
        match store.poll(now_tick, op_id) {
            crate::disk::DiskPoll::Pending => {}
            crate::disk::DiskPoll::Completed(_) => {
                self.active_op = None;
                self.submit_sync(now_tick, store);
            }
            crate::disk::DiskPoll::Failed(error) => {
                self.active_op = None;
                self.state = PreparedGcState::Failed(error);
            }
        }
    }

    fn submit_sync<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let op_id = self.alloc_op_id();
        match store.submit(now_tick, crate::disk::DiskRequest::Sync { op_id }) {
            crate::disk::DiskSubmit::Completed(_) => {
                self.active_op = None;
                self.state = PreparedGcState::Complete;
            }
            crate::disk::DiskSubmit::Pending => {
                self.active_op = Some(op_id);
                self.state = PreparedGcState::SyncPending;
            }
            crate::disk::DiskSubmit::Failed(error) => {
                self.active_op = None;
                self.state = PreparedGcState::Failed(error);
            }
        }
    }

    fn poll_sync<S: crate::disk::DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let Some(op_id) = self.active_op else {
            self.state = PreparedGcState::Failed(crate::disk::DiskError::Cancelled);
            return;
        };
        match store.poll(now_tick, op_id) {
            crate::disk::DiskPoll::Pending => {}
            crate::disk::DiskPoll::Completed(_) => {
                self.active_op = None;
                self.state = PreparedGcState::Complete;
            }
            crate::disk::DiskPoll::Failed(error) => {
                self.active_op = None;
                self.state = PreparedGcState::Failed(error);
            }
        }
    }
}

#[cfg(test)]
mod prepared_gc_tests {
    use super::*;
    use crate::disk::{DiskRequest, DiskSubmit, DurableStore, SimDisk};
    use crate::range_resize::{LifecycleResizeDecision, RangeTabletMap};
    use crate::resize::TabletResizePolicy;

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

    fn current_and_target() -> (TopologySnapshot, TopologySnapshot) {
        let map = RangeTabletMap::single(100, 1_000, vec![1, 2, 3]).unwrap();
        let current_lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let mut target_lifecycle = current_lifecycle.clone();
        let plan = match target_lifecycle.evaluate(1_000, 10, 7, &policy()).unwrap() {
            LifecycleResizeDecision::Planned(plan) => plan,
            other => panic!("expected plan, got {other:?}"),
        };
        target_lifecycle.commit(&plan, 7, 11).unwrap();

        (
            TopologySnapshot::from_lifecycle(7, &current_lifecycle),
            TopologySnapshot::from_lifecycle(7, &target_lifecycle),
        )
    }

    fn persist_current(disk: &mut SimDisk, snapshot: &TopologySnapshot) {
        assert!(matches!(
            disk.submit(
                0,
                DiskRequest::Put {
                    op_id: 80_000,
                    key: CURRENT_TOPOLOGY_KEY.to_vec(),
                    value: snapshot.encode().unwrap(),
                },
            ),
            DiskSubmit::Completed(_)
        ));
        assert!(matches!(
            disk.submit(0, DiskRequest::Sync { op_id: 80_001 }),
            DiskSubmit::Completed(_)
        ));
    }

    fn complete_txn(
        disk: &mut SimDisk,
        txn_id: u64,
        current: &TopologySnapshot,
        target: &TopologySnapshot,
    ) {
        let mut writer = DurableTopologyTxnWriter::new(txn_id, current, target.clone()).unwrap();
        writer.tick(1, disk);
        assert_eq!(writer.state(), TopologyTxnState::Prepared);
        assert!(writer.mark_applied());
        writer.tick(2, disk);
        assert_eq!(writer.state(), TopologyTxnState::Complete);
    }

    #[test]
    fn gc_deletes_only_stale_committed_prepared_record() {
        let (current, target) = current_and_target();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);
        complete_txn(&mut disk, 400, &current, &target);

        assert!(
            read_prepared_topology(3, 90_000, &mut disk)
                .unwrap()
                .is_some()
        );

        let mut gc = PreparedTopologyGc::begin(4, &mut disk).unwrap();
        assert_eq!(gc.state(), PreparedGcState::DeleteIdle);
        gc.tick(4, &mut disk);
        assert_eq!(gc.state(), PreparedGcState::Complete);

        assert_eq!(read_prepared_topology(5, 90_001, &mut disk).unwrap(), None);
        assert_eq!(
            recover_topology(5, &mut disk).unwrap(),
            TopologyRecovery::Current(target)
        );
    }

    #[test]
    fn gc_refuses_active_prepared_transaction() {
        let (current, target) = current_and_target();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);

        let mut writer = DurableTopologyTxnWriter::new(401, &current, target).unwrap();
        writer.tick(1, &mut disk);
        assert_eq!(writer.state(), TopologyTxnState::Prepared);

        assert!(matches!(
            PreparedTopologyGc::begin(2, &mut disk),
            Err(TopologyTxnError::ActivePrepared { txn_id: 401 })
        ));
    }

    #[test]
    fn crash_after_delete_before_gc_sync_restores_prepared_safely() {
        let (current, target) = current_and_target();
        let mut disk = SimDisk::default();
        persist_current(&mut disk, &current);
        complete_txn(&mut disk, 402, &current, &target);

        let mut gc = PreparedTopologyGc::begin(3, &mut disk).unwrap();
        disk.set_delay(2);
        gc.tick(10, &mut disk);
        assert_eq!(gc.state(), PreparedGcState::DeletePending);
        gc.tick(12, &mut disk);
        assert_eq!(gc.state(), PreparedGcState::SyncPending);

        disk.crash();
        disk.set_delay(0);

        assert!(
            read_prepared_topology(13, 90_002, &mut disk)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            recover_topology(13, &mut disk).unwrap(),
            TopologyRecovery::Current(target)
        );

        let mut retry_gc = PreparedTopologyGc::begin(14, &mut disk).unwrap();
        retry_gc.tick(14, &mut disk);
        assert!(retry_gc.is_complete());
        assert_eq!(read_prepared_topology(15, 90_003, &mut disk).unwrap(), None);
    }
}
