use std::collections::BTreeMap;
use std::mem::size_of;

use crate::compact_catalog::CompactTabletCatalog;
use crate::model::{Cluster, NodeId, Placement, TabletId};
use crate::placement::{FailureDomainPolicy, weighted_rendezvous_replicas};
use crate::range_resize::RangeTabletMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactPlacementError {
    SizeOverflow,
    ShapeMismatch,
    ReplicaCountMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactPlacement {
    tablet_count: u64,
    replica_count: usize,
    replicas: Vec<NodeId>,
}

impl CompactPlacement {
    pub fn weighted_rendezvous(
        cluster: &Cluster,
        tablet_count: u64,
        policy: FailureDomainPolicy,
    ) -> Result<Self, CompactPlacementError> {
        let replica_count = cluster
            .replication_factor
            .min(cluster.eligible_node_count());
        let tablet_count_usize =
            usize::try_from(tablet_count).map_err(|_| CompactPlacementError::SizeOverflow)?;
        let capacity = tablet_count_usize
            .checked_mul(replica_count)
            .ok_or(CompactPlacementError::SizeOverflow)?;

        let mut replicas = Vec::with_capacity(capacity);
        for tablet_id in 0..tablet_count {
            let selected = weighted_rendezvous_replicas(cluster, tablet_id, policy);
            debug_assert_eq!(selected.len(), replica_count);
            replicas.extend_from_slice(&selected);
        }

        Ok(Self {
            tablet_count,
            replica_count,
            replicas,
        })
    }

    pub fn weighted_rendezvous_for_catalog(
        cluster: &Cluster,
        catalog: &CompactTabletCatalog,
        policy: FailureDomainPolicy,
    ) -> Result<Self, CompactPlacementError> {
        let replica_count = cluster
            .replication_factor
            .min(cluster.eligible_node_count());
        let capacity = catalog
            .tablet_count()
            .checked_mul(replica_count)
            .ok_or(CompactPlacementError::SizeOverflow)?;

        let mut replicas = Vec::with_capacity(capacity);
        for tablet_id in catalog.tablet_ids() {
            let selected = weighted_rendezvous_replicas(cluster, *tablet_id, policy);
            debug_assert_eq!(selected.len(), replica_count);
            replicas.extend_from_slice(&selected);
        }

        Ok(Self {
            tablet_count: catalog.tablet_count() as u64,
            replica_count,
            replicas,
        })
    }

    pub fn from_range_map(map: &RangeTabletMap) -> Result<Self, CompactPlacementError> {
        let Some(first) = map.tablets().first() else {
            return Err(CompactPlacementError::ShapeMismatch);
        };
        let replica_count = first.replicas.len();
        if map
            .tablets()
            .iter()
            .any(|tablet| tablet.replicas.len() != replica_count)
        {
            return Err(CompactPlacementError::ReplicaCountMismatch);
        }

        let capacity = map
            .tablet_count()
            .checked_mul(replica_count)
            .ok_or(CompactPlacementError::SizeOverflow)?;
        let mut replicas = Vec::with_capacity(capacity);
        for tablet in map.tablets() {
            replicas.extend_from_slice(&tablet.replicas);
        }

        Ok(Self {
            tablet_count: map.tablet_count() as u64,
            replica_count,
            replicas,
        })
    }

    pub fn tablet_count(&self) -> u64 {
        self.tablet_count
    }

    pub fn replica_count(&self) -> usize {
        self.replica_count
    }

    pub fn replicas_by_slot(&self, slot: usize) -> Option<&[NodeId]> {
        if slot >= self.tablet_count as usize {
            return None;
        }
        if self.replica_count == 0 {
            return Some(&[]);
        }

        let start = slot.checked_mul(self.replica_count)?;
        let end = start.checked_add(self.replica_count)?;
        self.replicas.get(start..end)
    }

    /// Simulator convenience for contiguous slot IDs 0..tablet_count.
    /// Production-like stable TabletId routing should go through CompactTabletCatalog.
    pub fn replicas(&self, tablet_id: TabletId) -> Option<&[NodeId]> {
        let slot = usize::try_from(tablet_id).ok()?;
        self.replicas_by_slot(slot)
    }

    pub fn logical_replica_bytes(&self) -> usize {
        self.replicas.len().saturating_mul(size_of::<NodeId>())
    }

    pub fn allocated_replica_bytes(&self) -> usize {
        self.replicas.capacity().saturating_mul(size_of::<NodeId>())
    }

    pub fn bytes_per_tablet(&self) -> usize {
        self.replica_count.saturating_mul(size_of::<NodeId>())
    }

    pub(crate) fn set_replicas_by_slot(
        &mut self,
        slot: usize,
        replicas: &[NodeId],
    ) -> Result<(), CompactPlacementError> {
        if replicas.len() != self.replica_count || slot >= self.tablet_count as usize {
            return Err(CompactPlacementError::ReplicaCountMismatch);
        }
        if replicas.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(CompactPlacementError::ShapeMismatch);
        }

        let start = slot
            .checked_mul(self.replica_count)
            .ok_or(CompactPlacementError::SizeOverflow)?;
        let end = start
            .checked_add(self.replica_count)
            .ok_or(CompactPlacementError::SizeOverflow)?;
        self.replicas[start..end].copy_from_slice(replicas);
        Ok(())
    }

    pub fn replica_counts(&self) -> BTreeMap<NodeId, usize> {
        let mut counts = BTreeMap::new();
        for node_id in &self.replicas {
            *counts.entry(*node_id).or_default() += 1;
        }
        counts
    }

    pub fn changed_replica_count(&self, next: &Self) -> Result<u64, CompactPlacementError> {
        self.ensure_same_shape(next)?;

        let mut changed = 0_u64;
        for tablet_id in 0..self.tablet_count {
            let old = self.replicas(tablet_id).expect("validated compact shape");
            let new = next.replicas(tablet_id).expect("validated compact shape");
            changed = changed
                .saturating_add(new.iter().filter(|node_id| !old.contains(node_id)).count() as u64);
        }
        Ok(changed)
    }

    pub fn changed_tablet_count(&self, next: &Self) -> Result<u64, CompactPlacementError> {
        self.ensure_same_shape(next)?;

        let mut changed = 0_u64;
        for tablet_id in 0..self.tablet_count {
            if self.replicas(tablet_id) != next.replicas(tablet_id) {
                changed += 1;
            }
        }
        Ok(changed)
    }

    pub fn zone_collision_count(&self, cluster: &Cluster) -> u64 {
        let mut collisions = 0_u64;

        for tablet_id in 0..self.tablet_count {
            let Some(replicas) = self.replicas(tablet_id) else {
                continue;
            };

            for (index, left) in replicas.iter().enumerate() {
                for right in &replicas[index + 1..] {
                    if cluster.nodes[left].zone == cluster.nodes[right].zone {
                        collisions += 1;
                    }
                }
            }
        }

        collisions
    }

    pub fn stable_checksum(&self) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for node_id in &self.replicas {
            for byte in node_id.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
            }
        }
        hash ^= self.tablet_count;
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
        hash ^ self.replica_count as u64
    }

    pub fn equivalent_to_standard(&self, standard: &Placement) -> bool {
        if standard.replicas.len() != self.tablet_count as usize {
            return false;
        }

        (0..self.tablet_count).all(|tablet_id| {
            standard
                .replicas
                .get(&tablet_id)
                .is_some_and(|replicas| Some(replicas.as_slice()) == self.replicas(tablet_id))
        })
    }

    fn ensure_same_shape(&self, other: &Self) -> Result<(), CompactPlacementError> {
        if self.tablet_count != other.tablet_count || self.replica_count != other.replica_count {
            return Err(CompactPlacementError::ShapeMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::{AdminState, Node, Tablet};
    use crate::placement::PlacementStrategy;

    fn node(id: u64, weight: u32, zone: &str) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: "r1".into(),
            state: AdminState::Active,
        }
    }

    fn cluster(tablet_count: u64) -> Cluster {
        Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, 1, "a"),
                node(2, 2, "b"),
                node(3, 4, "c"),
                node(4, 8, "d"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: (0..tablet_count)
                .map(|id| Tablet { id, bytes: 1 })
                .collect(),
        }
    }

    #[test]
    fn compact_wrh_is_exactly_equivalent_to_standard_wrh() {
        let cluster = cluster(10_000);
        let standard = PlacementStrategy::WeightedRendezvous
            .place(&cluster, FailureDomainPolicy::HIERARCHICAL);
        let compact = CompactPlacement::weighted_rendezvous(
            &cluster,
            cluster.tablets.len() as u64,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        assert!(compact.equivalent_to_standard(&standard));
        assert_eq!(compact.tablet_count(), 10_000);
        assert_eq!(compact.replica_count(), 2);
        assert_eq!(compact.bytes_per_tablet(), 16);
    }

    #[test]
    fn compact_replica_counts_match_standard_counts() {
        let cluster = cluster(1_000);
        let standard = PlacementStrategy::WeightedRendezvous
            .place(&cluster, FailureDomainPolicy::HIERARCHICAL);
        let compact = CompactPlacement::weighted_rendezvous(
            &cluster,
            1_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let mut standard_counts = BTreeMap::new();
        for replicas in standard.replicas.values() {
            for node_id in replicas {
                *standard_counts.entry(*node_id).or_default() += 1;
            }
        }

        assert_eq!(compact.replica_counts(), standard_counts);
    }

    #[test]
    fn compact_join_movement_is_deterministic() {
        let before = cluster(0);
        let mut after = before.clone();
        after.epoch = 2;
        after.nodes.insert(5, node(5, 16, "e"));

        let a = CompactPlacement::weighted_rendezvous(
            &before,
            10_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let b = CompactPlacement::weighted_rendezvous(
            &after,
            10_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let a2 = CompactPlacement::weighted_rendezvous(
            &before,
            10_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let b2 = CompactPlacement::weighted_rendezvous(
            &after,
            10_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        assert_eq!(a.stable_checksum(), a2.stable_checksum());
        assert_eq!(b.stable_checksum(), b2.stable_checksum());
        assert_eq!(
            a.changed_replica_count(&b).unwrap(),
            a2.changed_replica_count(&b2).unwrap()
        );
    }

    #[test]
    fn compact_hierarchical_placement_has_no_zone_collisions() {
        let cluster = cluster(0);
        let compact = CompactPlacement::weighted_rendezvous(
            &cluster,
            50_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        assert_eq!(compact.zone_collision_count(&cluster), 0);
    }

    #[test]
    fn compact_placement_can_share_physical_range_catalog_order() {
        let mut map = RangeTabletMap::single(100, 1_000, vec![3, 1, 2]).unwrap();
        for _ in 0..4 {
            let split = map.plan_split_all(9).unwrap();
            map.commit(&split, 9).unwrap();
        }

        let compact = CompactPlacement::from_range_map(&map).unwrap();
        assert_eq!(compact.tablet_count(), map.tablet_count() as u64);

        for (slot, tablet) in map.tablets().iter().enumerate() {
            assert_eq!(
                compact.replicas_by_slot(slot),
                Some(tablet.replicas.as_slice())
            );
        }
    }

    #[test]
    fn catalog_wrh_hashes_stable_tablet_id_not_slot() {
        let mut map = RangeTabletMap::single(10_000, 1_000, vec![1, 2]).unwrap();
        let split = map.plan_split_all(4).unwrap();
        map.commit(&split, 4).unwrap();

        let catalog = CompactTabletCatalog::from_range_map(&map).unwrap();
        let cluster = cluster(0);
        let compact = CompactPlacement::weighted_rendezvous_for_catalog(
            &cluster,
            &catalog,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        for slot in 0..catalog.tablet_count() {
            let tablet_id = catalog.tablet_id(slot).unwrap();
            let expected = weighted_rendezvous_replicas(
                &cluster,
                tablet_id,
                FailureDomainPolicy::HIERARCHICAL,
            );
            assert_eq!(compact.replicas_by_slot(slot), Some(expected.as_slice()));
        }

        assert_ne!(catalog.tablet_id(0), Some(0));
    }

    #[test]
    fn shape_mismatch_is_rejected_for_movement() {
        let cluster = cluster(0);
        let a =
            CompactPlacement::weighted_rendezvous(&cluster, 10, FailureDomainPolicy::HIERARCHICAL)
                .unwrap();
        let b =
            CompactPlacement::weighted_rendezvous(&cluster, 11, FailureDomainPolicy::HIERARCHICAL)
                .unwrap();

        assert_eq!(
            a.changed_replica_count(&b),
            Err(CompactPlacementError::ShapeMismatch)
        );
    }
}
