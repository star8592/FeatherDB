use std::collections::BTreeSet;

use crate::hash::{hash_pair, mix64, unit_interval_open};
use crate::model::{Cluster, NodeId, Placement};

#[derive(Clone, Copy, Debug)]
pub struct FailureDomainPolicy {
    pub distinct_zones: bool,
    pub distinct_racks: bool,
}

impl FailureDomainPolicy {
    pub const NONE: Self = Self {
        distinct_zones: false,
        distinct_racks: false,
    };

    pub const HIERARCHICAL: Self = Self {
        distinct_zones: true,
        distinct_racks: true,
    };
}

#[derive(Clone, Copy, Debug)]
pub enum PlacementStrategy {
    HashRing { virtual_nodes_per_weight: u16 },
    WeightedRendezvous,
}

impl PlacementStrategy {
    pub fn name(&self) -> &'static str {
        match self {
            Self::HashRing { .. } => "hash-ring",
            Self::WeightedRendezvous => "weighted-rendezvous",
        }
    }

    pub fn place(&self, cluster: &Cluster, policy: FailureDomainPolicy) -> Placement {
        match self {
            Self::HashRing {
                virtual_nodes_per_weight,
            } => place_hash_ring(cluster, *virtual_nodes_per_weight, policy),
            Self::WeightedRendezvous => place_weighted_rendezvous(cluster, policy),
        }
    }
}

fn select_replicas(
    cluster: &Cluster,
    ranked: &[NodeId],
    policy: FailureDomainPolicy,
) -> Vec<NodeId> {
    let target = cluster
        .replication_factor
        .min(cluster.eligible_node_count());
    let mut selected = Vec::with_capacity(target);

    if policy.distinct_zones {
        let mut zones = BTreeSet::new();
        for node_id in ranked {
            if selected.len() == target {
                break;
            }
            let node = &cluster.nodes[node_id];
            if zones.insert(node.zone.clone()) {
                selected.push(*node_id);
            }
        }
    }

    if policy.distinct_racks && selected.len() < target {
        let mut racks: BTreeSet<_> = selected
            .iter()
            .map(|id| {
                let node = &cluster.nodes[id];
                (node.zone.clone(), node.rack.clone())
            })
            .collect();

        for node_id in ranked {
            if selected.len() == target {
                break;
            }
            if selected.contains(node_id) {
                continue;
            }
            let node = &cluster.nodes[node_id];
            if racks.insert((node.zone.clone(), node.rack.clone())) {
                selected.push(*node_id);
            }
        }
    }

    for node_id in ranked {
        if selected.len() == target {
            break;
        }
        if !selected.contains(node_id) {
            selected.push(*node_id);
        }
    }

    selected.sort_unstable();
    selected
}

pub(crate) fn weighted_rendezvous_replicas(
    cluster: &Cluster,
    tablet_id: u64,
    policy: FailureDomainPolicy,
) -> Vec<NodeId> {
    let mut ranked: Vec<(f64, NodeId)> = cluster
        .nodes
        .values()
        .filter(|node| node.eligible())
        .map(|node| {
            let u = unit_interval_open(hash_pair(tablet_id, node.id));
            // Weighted HRW: score = -weight / ln(U), choose highest score.
            // f64 is acceptable for the simulator; production placement must
            // later define cross-platform deterministic numeric semantics.
            let score = -(node.weight as f64) / u.ln();
            (score, node.id)
        })
        .collect();

    ranked.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    let ranked_ids: Vec<_> = ranked.into_iter().map(|(_, id)| id).collect();
    select_replicas(cluster, &ranked_ids, policy)
}

fn place_weighted_rendezvous(cluster: &Cluster, policy: FailureDomainPolicy) -> Placement {
    let mut placement = Placement::default();

    for tablet in &cluster.tablets {
        placement.replicas.insert(
            tablet.id,
            weighted_rendezvous_replicas(cluster, tablet.id, policy),
        );
    }

    placement
}

fn place_hash_ring(
    cluster: &Cluster,
    virtual_nodes_per_weight: u16,
    policy: FailureDomainPolicy,
) -> Placement {
    let mut ring = Vec::new();

    for node in cluster.nodes.values().filter(|node| node.eligible()) {
        let vnode_count = usize::from(virtual_nodes_per_weight) * node.weight as usize;
        for vnode in 0..vnode_count {
            let token = hash_pair(node.id, vnode as u64);
            ring.push((token, node.id));
        }
    }
    ring.sort_unstable();

    let mut placement = Placement::default();
    if ring.is_empty() {
        return placement;
    }

    for tablet in &cluster.tablets {
        let token = mix64(tablet.id);
        let start = ring.partition_point(|(ring_token, _)| *ring_token < token);
        let mut ranked_ids = Vec::with_capacity(cluster.eligible_node_count());
        let mut seen = BTreeSet::new();

        for offset in 0..ring.len() {
            let (_, node_id) = ring[(start + offset) % ring.len()];
            if seen.insert(node_id) {
                ranked_ids.push(node_id);
                if ranked_ids.len() == cluster.eligible_node_count() {
                    break;
                }
            }
        }

        placement
            .replicas
            .insert(tablet.id, select_replicas(cluster, &ranked_ids, policy));
    }

    placement
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::model::{AdminState, Node, Tablet};

    fn node(id: u64, weight: u32, zone: &str, rack: &str) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: rack.into(),
            state: AdminState::Active,
        }
    }

    fn cluster(tablets: u64) -> Cluster {
        Cluster {
            epoch: 1,
            replication_factor: 3,
            nodes: [
                node(1, 1, "a", "1"),
                node(2, 2, "b", "1"),
                node(3, 4, "c", "1"),
                node(4, 8, "d", "1"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: (0..tablets).map(|id| Tablet { id, bytes: 1024 }).collect(),
        }
    }

    #[test]
    fn replica_order_is_canonical() {
        let cluster = cluster(256);
        for strategy in [
            PlacementStrategy::HashRing {
                virtual_nodes_per_weight: 64,
            },
            PlacementStrategy::WeightedRendezvous,
        ] {
            let placement = strategy.place(&cluster, FailureDomainPolicy::HIERARCHICAL);
            assert!(
                placement
                    .replicas
                    .values()
                    .all(|replicas| { replicas.windows(2).all(|pair| pair[0] < pair[1]) })
            );
        }
    }

    #[test]
    fn strategies_are_deterministic() {
        let cluster = cluster(512);
        for strategy in [
            PlacementStrategy::HashRing {
                virtual_nodes_per_weight: 64,
            },
            PlacementStrategy::WeightedRendezvous,
        ] {
            assert_eq!(
                strategy.place(&cluster, FailureDomainPolicy::HIERARCHICAL),
                strategy.place(&cluster, FailureDomainPolicy::HIERARCHICAL)
            );
        }
    }

    #[test]
    fn constrained_policy_separates_zones_when_possible() {
        let cluster = cluster(256);
        let placement = PlacementStrategy::WeightedRendezvous
            .place(&cluster, FailureDomainPolicy::HIERARCHICAL);

        for replicas in placement.replicas.values() {
            let zones: BTreeSet<_> = replicas
                .iter()
                .map(|id| cluster.nodes[id].zone.as_str())
                .collect();
            assert_eq!(zones.len(), cluster.replication_factor);
        }
    }

    #[test]
    fn draining_node_gets_no_new_placement() {
        let mut cluster = cluster(256);
        cluster.nodes.get_mut(&4).unwrap().state = AdminState::Draining;

        for strategy in [
            PlacementStrategy::HashRing {
                virtual_nodes_per_weight: 64,
            },
            PlacementStrategy::WeightedRendezvous,
        ] {
            let placement = strategy.place(&cluster, FailureDomainPolicy::HIERARCHICAL);
            assert!(
                placement
                    .replicas
                    .values()
                    .all(|replicas| !replicas.contains(&4))
            );
        }
    }

    #[test]
    fn insufficient_zones_degrade_to_unique_nodes() {
        let mut cluster = cluster(32);
        cluster.nodes.get_mut(&2).unwrap().zone = "a".into();
        cluster.nodes.get_mut(&3).unwrap().zone = "a".into();
        cluster.nodes.get_mut(&4).unwrap().zone = "a".into();

        let placement = PlacementStrategy::WeightedRendezvous
            .place(&cluster, FailureDomainPolicy::HIERARCHICAL);
        assert!(placement.replicas.values().all(|replicas| {
            replicas.len() == cluster.replication_factor
                && replicas.iter().collect::<BTreeSet<_>>().len() == cluster.replication_factor
        }));
    }

    #[test]
    fn weighted_rendezvous_respects_capacity_in_large_sample() {
        let cluster = cluster(100_000);
        let placement =
            PlacementStrategy::WeightedRendezvous.place(&cluster, FailureDomainPolicy::NONE);

        let mut counts = BTreeMap::<NodeId, usize>::new();
        for replicas in placement.replicas.values() {
            for node_id in replicas {
                *counts.entry(*node_id).or_default() += 1;
            }
        }

        // With RF=3 the exact inclusion probabilities are not simply weight/sum(weight),
        // so only assert monotonic responsibility by weight here.
        assert!(counts[&1] < counts[&2]);
        assert!(counts[&2] < counts[&3]);
        assert!(counts[&3] < counts[&4]);
    }

    #[test]
    fn wrh_join_does_not_remap_between_unchanged_nodes_for_rf1() {
        let mut before = cluster(10_000);
        before.replication_factor = 1;
        before.nodes.remove(&4);
        let before_p =
            PlacementStrategy::WeightedRendezvous.place(&before, FailureDomainPolicy::NONE);

        let mut after = before.clone();
        after.epoch += 1;
        after.nodes.insert(4, node(4, 8, "d", "1"));
        let after_p =
            PlacementStrategy::WeightedRendezvous.place(&after, FailureDomainPolicy::NONE);

        for tablet in &before.tablets {
            let old = before_p.replicas[&tablet.id][0];
            let new = after_p.replicas[&tablet.id][0];
            if old != new {
                assert_eq!(new, 4);
            }
        }
    }
}
