use std::collections::BTreeMap;

use crate::model::{Cluster, NodeId, Placement, Tablet};

#[derive(Clone, Debug)]
pub struct PlacementMetrics {
    pub replica_counts: BTreeMap<NodeId, usize>,
    pub zone_collisions: usize,
    pub rack_collisions: usize,
}

impl PlacementMetrics {
    pub fn calculate(cluster: &Cluster, placement: &Placement) -> Self {
        let mut replica_counts = BTreeMap::new();
        let mut zone_collisions = 0;
        let mut rack_collisions = 0;

        for replicas in placement.replicas.values() {
            let mut zones = Vec::new();
            let mut racks = Vec::new();

            for node_id in replicas {
                *replica_counts.entry(*node_id).or_default() += 1;
                let node = &cluster.nodes[node_id];
                if zones.contains(&node.zone) {
                    zone_collisions += 1;
                }
                let rack = (node.zone.as_str(), node.rack.as_str());
                if racks.contains(&rack) {
                    rack_collisions += 1;
                }
                zones.push(node.zone.clone());
                racks.push(rack);
            }
        }

        Self {
            replica_counts,
            zone_collisions,
            rack_collisions,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JoinMovementBreakdown {
    pub total_bytes: u64,
    pub bytes_to_joining_nodes: u64,
    pub excess_bytes_to_existing_nodes: u64,
}

pub fn join_movement_breakdown(
    before: &Placement,
    after: &Placement,
    tablets: &[Tablet],
    joining_nodes: &std::collections::BTreeSet<NodeId>,
) -> JoinMovementBreakdown {
    let mut result = JoinMovementBreakdown::default();

    for tablet in tablets {
        let old = before
            .replicas
            .get(&tablet.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let new = after
            .replicas
            .get(&tablet.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        for node_id in new.iter().filter(|node_id| !old.contains(node_id)) {
            result.total_bytes += tablet.bytes;
            if joining_nodes.contains(node_id) {
                result.bytes_to_joining_nodes += tablet.bytes;
            } else {
                result.excess_bytes_to_existing_nodes += tablet.bytes;
            }
        }
    }

    result
}

pub fn moved_bytes(before: &Placement, after: &Placement, tablets: &[Tablet]) -> u64 {
    tablets
        .iter()
        .map(|tablet| {
            let old = before
                .replicas
                .get(&tablet.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let new = after
                .replicas
                .get(&tablet.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            new.iter().filter(|id| !old.contains(id)).count() as u64 * tablet.bytes
        })
        .sum()
}

pub fn movement_ratio(
    before: &Placement,
    after: &Placement,
    tablets: &[Tablet],
    replication_factor: usize,
) -> f64 {
    let logical_bytes: u64 = tablets.iter().map(|tablet| tablet.bytes).sum();
    let denominator = logical_bytes.saturating_mul(replication_factor as u64);
    if denominator == 0 {
        return 0.0;
    }
    moved_bytes(before, after, tablets) as f64 / denominator as f64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::{AdminState, Node, Tablet};

    #[test]
    fn movement_counts_only_new_replicas() {
        let tablets = vec![Tablet { id: 1, bytes: 100 }];
        let before = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let after = Placement {
            replicas: BTreeMap::from([(1, vec![2, 3])]),
        };
        assert_eq!(moved_bytes(&before, &after, &tablets), 100);
        assert_eq!(movement_ratio(&before, &after, &tablets, 2), 0.5);
    }

    #[test]
    fn join_breakdown_separates_required_and_excess_changes() {
        use std::collections::BTreeSet;

        let tablets = vec![Tablet { id: 1, bytes: 100 }, Tablet { id: 2, bytes: 200 }];
        let before = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2]), (2, vec![1, 2])]),
        };
        let after = Placement {
            replicas: BTreeMap::from([(1, vec![1, 3]), (2, vec![1, 4])]),
        };
        let joining = BTreeSet::from([3]);
        let breakdown = join_movement_breakdown(&before, &after, &tablets, &joining);

        assert_eq!(breakdown.total_bytes, 300);
        assert_eq!(breakdown.bytes_to_joining_nodes, 100);
        assert_eq!(breakdown.excess_bytes_to_existing_nodes, 200);
    }

    #[test]
    fn collisions_are_observable() {
        let cluster = Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                Node {
                    id: 1,
                    weight: 1,
                    zone: "a".into(),
                    rack: "r1".into(),
                    state: AdminState::Active,
                },
                Node {
                    id: 2,
                    weight: 1,
                    zone: "a".into(),
                    rack: "r1".into(),
                    state: AdminState::Active,
                },
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![Tablet { id: 1, bytes: 1 }],
        };
        let placement = Placement {
            replicas: BTreeMap::from([(1, vec![1, 2])]),
        };
        let metrics = PlacementMetrics::calculate(&cluster, &placement);
        assert_eq!(metrics.zone_collisions, 1);
        assert_eq!(metrics.rack_collisions, 1);
    }
}
