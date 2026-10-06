use std::collections::BTreeMap;

use crate::model::{Cluster, NodeId, Placement, Tablet};

#[derive(Clone, Debug)]
pub struct PlacementMetrics {
    pub replica_counts: BTreeMap<NodeId, usize>,
    pub zone_collisions: usize,
    pub rack_collisions: usize,
    pub max_capacity_inclusion_error: f64,
    pub max_zone_aware_capacity_inclusion_error: Option<f64>,
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

        let targets = feasible_capacity_inclusion_targets(cluster);
        let tablet_count = cluster.tablets.len() as f64;
        let max_capacity_inclusion_error = if tablet_count == 0.0 {
            0.0
        } else {
            targets
                .iter()
                .map(|(node_id, target)| {
                    let actual =
                        replica_counts.get(node_id).copied().unwrap_or(0) as f64 / tablet_count;
                    (actual - target).abs()
                })
                .fold(0.0, f64::max)
        };

        let max_zone_aware_capacity_inclusion_error =
            zone_aware_capacity_inclusion_targets(cluster).map(|targets| {
                if tablet_count == 0.0 {
                    0.0
                } else {
                    targets
                        .iter()
                        .map(|(node_id, target)| {
                            let actual = replica_counts.get(node_id).copied().unwrap_or(0) as f64
                                / tablet_count;
                            (actual - target).abs()
                        })
                        .fold(0.0, f64::max)
                }
            });

        Self {
            replica_counts,
            zone_collisions,
            rack_collisions,
            max_capacity_inclusion_error,
            max_zone_aware_capacity_inclusion_error,
        }
    }
}

/// Capacity-only feasible average inclusion target.
///
/// For equal-sized tablets, each node can hold at most one replica of a tablet.
/// We therefore solve for targets p_i = min(1, lambda * weight_i) such that
/// sum(p_i) = RF. This is a node-capacity target only; rack/zone constraints
/// can further reduce the feasible set.
pub fn feasible_capacity_inclusion_targets(cluster: &Cluster) -> BTreeMap<NodeId, f64> {
    let eligible: Vec<_> = cluster
        .nodes
        .values()
        .filter(|node| node.eligible())
        .collect();
    let target_replica_count = cluster.replication_factor.min(eligible.len());

    if target_replica_count == 0 {
        return BTreeMap::new();
    }

    let mut result = BTreeMap::new();
    let mut remaining: Vec<_> = eligible;
    let mut replicas_left = target_replica_count as f64;

    loop {
        if remaining.is_empty() {
            break;
        }

        let weight_sum: f64 = remaining.iter().map(|node| node.weight as f64).sum();
        let lambda = replicas_left / weight_sum;

        let saturated: Vec<_> = remaining
            .iter()
            .filter(|node| lambda * node.weight as f64 >= 1.0)
            .map(|node| node.id)
            .collect();

        if saturated.is_empty() {
            for node in remaining {
                result.insert(node.id, lambda * node.weight as f64);
            }
            break;
        }

        for node_id in &saturated {
            result.insert(*node_id, 1.0);
        }
        replicas_left -= saturated.len() as f64;
        remaining.retain(|node| !saturated.contains(&node.id));

        if replicas_left <= 0.0 {
            for node in remaining {
                result.insert(node.id, 0.0);
            }
            break;
        }
    }

    result
}

fn capped_proportional_targets(
    weighted_items: &[(u64, f64)],
    total_mass: f64,
    cap: f64,
) -> BTreeMap<u64, f64> {
    let mut result = BTreeMap::new();
    let mut remaining = weighted_items.to_vec();
    let mut mass_left = total_mass;

    while !remaining.is_empty() && mass_left > 0.0 {
        let weight_sum: f64 = remaining.iter().map(|(_, weight)| *weight).sum();
        if weight_sum <= 0.0 {
            break;
        }

        let lambda = mass_left / weight_sum;
        let saturated: Vec<_> = remaining
            .iter()
            .filter(|(_, weight)| lambda * *weight >= cap)
            .map(|(id, _)| *id)
            .collect();

        if saturated.is_empty() {
            for (id, weight) in remaining {
                result.insert(id, lambda * weight);
            }
            return result;
        }

        for id in &saturated {
            result.insert(*id, cap);
        }
        mass_left -= cap * saturated.len() as f64;
        remaining.retain(|(id, _)| !saturated.contains(id));
    }

    for (id, _) in remaining {
        result.entry(id).or_insert(0.0);
    }

    result
}

/// Zone-aware feasible inclusion targets for a strict distinct-zone policy.
///
/// Each zone can contribute at most one replica of a tablet. Zone mass is first
/// allocated proportional to total eligible node weight, capped at one replica
/// per zone, then divided among nodes in that zone by node weight.
///
/// Returns None when the cluster has fewer eligible zones than RF.
pub fn zone_aware_capacity_inclusion_targets(cluster: &Cluster) -> Option<BTreeMap<NodeId, f64>> {
    let mut zones: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for node in cluster.nodes.values().filter(|node| node.eligible()) {
        zones.entry(node.zone.clone()).or_default().push(node);
    }

    let rf = cluster
        .replication_factor
        .min(cluster.eligible_node_count());

    if rf == 0 {
        return Some(BTreeMap::new());
    }
    if zones.len() < rf {
        return None;
    }

    let zone_items: Vec<_> = zones
        .iter()
        .enumerate()
        .map(|(index, (_, nodes))| {
            let weight = nodes.iter().map(|node| node.weight as f64).sum::<f64>();
            (index as u64, weight)
        })
        .collect();

    let zone_targets = capped_proportional_targets(&zone_items, rf as f64, 1.0);
    let mut result = BTreeMap::new();

    for (index, (_, nodes)) in zones.iter().enumerate() {
        let zone_mass = zone_targets.get(&(index as u64)).copied().unwrap_or(0.0);
        let total_weight = nodes.iter().map(|node| node.weight as f64).sum::<f64>();

        for node in nodes {
            let target = if total_weight > 0.0 {
                zone_mass * node.weight as f64 / total_weight
            } else {
                0.0
            };
            result.insert(node.id, target);
        }
    }

    Some(result)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransitionMovementBreakdown {
    pub total_changed_tablets: usize,
    pub affected_changed_tablets: usize,
    pub excess_changed_tablets: usize,
}

pub fn transition_movement_breakdown(
    before: &Placement,
    after: &Placement,
    tablets: &[Tablet],
    affected_nodes: &std::collections::BTreeSet<NodeId>,
) -> TransitionMovementBreakdown {
    let mut result = TransitionMovementBreakdown::default();

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

        if old == new {
            continue;
        }

        result.total_changed_tablets += 1;
        let involved = old.iter().any(|id| affected_nodes.contains(id))
            || new.iter().any(|id| affected_nodes.contains(id));

        if involved {
            result.affected_changed_tablets += 1;
        } else {
            result.excess_changed_tablets += 1;
        }
    }

    result
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
    fn feasible_capacity_target_caps_large_nodes() {
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
                    weight: 2,
                    zone: "b".into(),
                    rack: "r1".into(),
                    state: AdminState::Active,
                },
                Node {
                    id: 3,
                    weight: 4,
                    zone: "c".into(),
                    rack: "r1".into(),
                    state: AdminState::Active,
                },
                Node {
                    id: 4,
                    weight: 8,
                    zone: "d".into(),
                    rack: "r1".into(),
                    state: AdminState::Active,
                },
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![],
        };

        let targets = feasible_capacity_inclusion_targets(&cluster);
        assert!((targets[&1] - 1.0 / 7.0).abs() < 1e-12);
        assert!((targets[&2] - 2.0 / 7.0).abs() < 1e-12);
        assert!((targets[&3] - 4.0 / 7.0).abs() < 1e-12);
        assert!((targets[&4] - 1.0).abs() < 1e-12);
        assert!((targets.values().sum::<f64>() - 2.0).abs() < 1e-12);
    }

    #[test]
    fn zone_aware_target_accounts_for_zone_capacity() {
        let cluster = Cluster {
            epoch: 1,
            replication_factor: 3,
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
                    rack: "r2".into(),
                    state: AdminState::Active,
                },
                Node {
                    id: 3,
                    weight: 1,
                    zone: "b".into(),
                    rack: "r1".into(),
                    state: AdminState::Active,
                },
                Node {
                    id: 4,
                    weight: 1,
                    zone: "b".into(),
                    rack: "r2".into(),
                    state: AdminState::Active,
                },
                Node {
                    id: 5,
                    weight: 1,
                    zone: "c".into(),
                    rack: "r1".into(),
                    state: AdminState::Active,
                },
                Node {
                    id: 6,
                    weight: 1,
                    zone: "c".into(),
                    rack: "r2".into(),
                    state: AdminState::Active,
                },
                Node {
                    id: 7,
                    weight: 1,
                    zone: "d".into(),
                    rack: "r1".into(),
                    state: AdminState::Active,
                },
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: vec![],
        };

        let targets = zone_aware_capacity_inclusion_targets(&cluster).unwrap();
        for target in targets.values() {
            assert!((*target - 3.0 / 7.0).abs() < 1e-12);
        }
        assert!((targets.values().sum::<f64>() - 3.0).abs() < 1e-12);
    }

    #[test]
    fn transition_breakdown_flags_unrelated_remapping() {
        use std::collections::BTreeSet;

        let tablets = vec![Tablet { id: 1, bytes: 1 }, Tablet { id: 2, bytes: 1 }];
        let before = Placement {
            replicas: BTreeMap::from([(1, vec![1]), (2, vec![2])]),
        };
        let after = Placement {
            replicas: BTreeMap::from([(1, vec![3]), (2, vec![4])]),
        };
        let affected = BTreeSet::from([3]);
        let breakdown = transition_movement_breakdown(&before, &after, &tablets, &affected);

        assert_eq!(breakdown.total_changed_tablets, 2);
        assert_eq!(breakdown.affected_changed_tablets, 1);
        assert_eq!(breakdown.excess_changed_tablets, 1);
    }

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
