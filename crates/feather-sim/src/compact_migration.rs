use crate::compact::{CompactPlacement, CompactPlacementError};
use crate::model::{NodeId, TabletId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactMigrationMove {
    pub tablet_id: TabletId,
    pub owner_to_replace: NodeId,
    pub to: NodeId,
}

#[derive(Debug)]
pub struct CompactMigrationCursor<'a> {
    actual: &'a CompactPlacement,
    desired: &'a CompactPlacement,
    next_tablet: u64,
    pending: Vec<CompactMigrationMove>,
    pending_index: usize,
    peak_buffered_moves: usize,
}

impl<'a> CompactMigrationCursor<'a> {
    pub fn new(
        actual: &'a CompactPlacement,
        desired: &'a CompactPlacement,
    ) -> Result<Self, CompactPlacementError> {
        if actual.tablet_count() != desired.tablet_count()
            || actual.replica_count() != desired.replica_count()
        {
            return Err(CompactPlacementError::ShapeMismatch);
        }

        Ok(Self {
            actual,
            desired,
            next_tablet: 0,
            pending: Vec::with_capacity(actual.replica_count()),
            pending_index: 0,
            peak_buffered_moves: 0,
        })
    }

    pub fn peak_buffered_moves(&self) -> usize {
        self.peak_buffered_moves
    }

    pub fn buffered_capacity_bytes(&self) -> usize {
        self.pending
            .capacity()
            .saturating_mul(std::mem::size_of::<CompactMigrationMove>())
    }

    fn refill(&mut self) -> bool {
        while self.next_tablet < self.actual.tablet_count() {
            let tablet_id = self.next_tablet;
            self.next_tablet += 1;
            self.pending.clear();
            self.pending_index = 0;

            let old = self
                .actual
                .replicas(tablet_id)
                .expect("compact shape validated");
            let new = self
                .desired
                .replicas(tablet_id)
                .expect("compact shape validated");

            let removed: Vec<_> = old
                .iter()
                .copied()
                .filter(|node_id| !new.contains(node_id))
                .collect();
            let added: Vec<_> = new
                .iter()
                .copied()
                .filter(|node_id| !old.contains(node_id))
                .collect();

            debug_assert_eq!(removed.len(), added.len());

            self.pending.extend(
                removed
                    .into_iter()
                    .zip(added)
                    .map(|(owner_to_replace, to)| CompactMigrationMove {
                        tablet_id,
                        owner_to_replace,
                        to,
                    }),
            );

            self.peak_buffered_moves = self.peak_buffered_moves.max(self.pending.len());

            if !self.pending.is_empty() {
                return true;
            }
        }

        false
    }
}

impl Iterator for CompactMigrationCursor<'_> {
    type Item = CompactMigrationMove;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.pending.get(self.pending_index).copied() {
                self.pending_index += 1;
                return Some(item);
            }

            if !self.refill() {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::{AdminState, Cluster, Node};
    use crate::placement::FailureDomainPolicy;

    fn node(id: u64, weight: u32, zone: &str) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: "r1".into(),
            state: AdminState::Active,
        }
    }

    fn cluster() -> Cluster {
        Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [node(1, 1, "a"), node(2, 2, "b"), node(3, 4, "c")]
                .into_iter()
                .map(|node| (node.id, node))
                .collect::<BTreeMap<_, _>>(),
            tablets: Vec::new(),
        }
    }

    #[test]
    fn cursor_move_count_matches_compact_diff() {
        let before_cluster = cluster();
        let mut after_cluster = before_cluster.clone();
        after_cluster.nodes.insert(4, node(4, 8, "d"));

        let before = CompactPlacement::weighted_rendezvous(
            &before_cluster,
            50_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let after = CompactPlacement::weighted_rendezvous(
            &after_cluster,
            50_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let expected = before.changed_replica_count(&after).unwrap();
        let mut cursor = CompactMigrationCursor::new(&before, &after).unwrap();
        let observed = cursor.by_ref().count() as u64;

        assert_eq!(observed, expected);
        assert!(cursor.peak_buffered_moves() <= before.replica_count());
    }

    #[test]
    fn cursor_is_deterministic() {
        let before_cluster = cluster();
        let mut after_cluster = before_cluster.clone();
        after_cluster.nodes.insert(4, node(4, 8, "d"));

        let before = CompactPlacement::weighted_rendezvous(
            &before_cluster,
            10_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let after = CompactPlacement::weighted_rendezvous(
            &after_cluster,
            10_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let a: Vec<_> = CompactMigrationCursor::new(&before, &after)
            .unwrap()
            .take(1_000)
            .collect();
        let b: Vec<_> = CompactMigrationCursor::new(&before, &after)
            .unwrap()
            .take(1_000)
            .collect();

        assert_eq!(a, b);
    }

    #[test]
    fn cursor_matches_eager_scheduler_rebalance_order_exactly() {
        use crate::migration::{MigrationBudget, MigrationScheduler};
        use crate::model::Tablet;
        use crate::placement::PlacementStrategy;

        let tablet_count = 1_000_u64;
        let mut before_cluster = cluster();
        before_cluster.tablets = (0..tablet_count)
            .map(|id| Tablet { id, bytes: 1 })
            .collect();

        let mut after_cluster = before_cluster.clone();
        after_cluster.epoch = 2;
        after_cluster.nodes.insert(4, node(4, 8, "d"));

        let actual = PlacementStrategy::WeightedRendezvous
            .place(&before_cluster, FailureDomainPolicy::HIERARCHICAL);
        let desired = PlacementStrategy::WeightedRendezvous
            .place(&after_cluster, FailureDomainPolicy::HIERARCHICAL);

        let eager = MigrationScheduler::new(
            after_cluster.clone(),
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired,
            MigrationBudget::conservative(),
        )
        .unwrap();
        let eager_moves: Vec<_> = eager
            .tasks()
            .iter()
            .map(|task| (task.tablet_id, task.owner_to_replace, task.to))
            .collect();

        let compact_before = CompactPlacement::weighted_rendezvous(
            &before_cluster,
            tablet_count,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let compact_after = CompactPlacement::weighted_rendezvous(
            &after_cluster,
            tablet_count,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let lazy_moves: Vec<_> = CompactMigrationCursor::new(&compact_before, &compact_after)
            .unwrap()
            .map(|movement| (movement.tablet_id, movement.owner_to_replace, movement.to))
            .collect();

        assert_eq!(lazy_moves, eager_moves);
    }

    #[test]
    fn cursor_buffer_is_rf_bounded() {
        let before_cluster = cluster();
        let mut after_cluster = before_cluster.clone();
        after_cluster.nodes.insert(4, node(4, 8, "d"));

        let before = CompactPlacement::weighted_rendezvous(
            &before_cluster,
            100_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();
        let after = CompactPlacement::weighted_rendezvous(
            &after_cluster,
            100_000,
            FailureDomainPolicy::HIERARCHICAL,
        )
        .unwrap();

        let mut cursor = CompactMigrationCursor::new(&before, &after).unwrap();
        let count = cursor.by_ref().count();

        assert!(count > 0);
        assert!(cursor.peak_buffered_moves() <= 2);
        assert!(
            cursor.buffered_capacity_bytes() <= 2 * std::mem::size_of::<CompactMigrationMove>()
        );
    }
}
