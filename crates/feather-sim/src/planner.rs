use std::collections::{BTreeMap, BTreeSet};

use crate::hash::{hash_pair, unit_interval_open};
use crate::metrics::{
    feasible_capacity_inclusion_targets, zone_aware_capacity_inclusion_targets,
};
use crate::model::{Cluster, NodeId, Placement, TabletId};
use crate::placement::FailureDomainPolicy;

#[derive(Clone, Debug)]
pub struct PlannerResult {
    pub placement: Placement,
    pub moves: usize,
    pub converged: bool,
}

pub fn plan_rebalance(
    cluster: &Cluster,
    current: &Placement,
    policy: FailureDomainPolicy,
    max_moves: usize,
) -> PlannerResult {
    let mut placement = current.clone();
    let targets = integer_target_counts(cluster, policy);
    let mut counts = replica_counts(&placement);

    for node_id in cluster.nodes.keys() {
        counts.entry(*node_id).or_default();
    }

    let mut moves = 0usize;
    let mut made_progress = true;

    while made_progress && moves < max_moves {
        made_progress = false;

        for tablet in &cluster.tablets {
            if moves >= max_moves {
                break;
            }

            let Some(replicas) = placement.replicas.get(&tablet.id).cloned() else {
                continue;
            };

            let mut sources: Vec<_> = replicas
                .iter()
                .copied()
                .filter(|node_id| {
                    let eligible = cluster
                        .nodes
                        .get(node_id)
                        .is_some_and(|node| node.eligible());
                    !eligible
                        || counts.get(node_id).copied().unwrap_or(0)
                            > targets.get(node_id).copied().unwrap_or(0)
                })
                .collect();

            sources.sort_by(|a, b| {
                overload(*b, &counts, &targets)
                    .cmp(&overload(*a, &counts, &targets))
                    .then_with(|| a.cmp(b))
            });

            for source in sources {
                let mut candidates: Vec<_> = cluster
                    .nodes
                    .values()
                    .filter(|node| node.eligible())
                    .filter(|node| !replicas.contains(&node.id))
                    .filter(|node| {
                        counts.get(&node.id).copied().unwrap_or(0)
                            < targets.get(&node.id).copied().unwrap_or(0)
                    })
                    .filter(|node| {
                        replacement_respects_policy(
                            cluster,
                            &replicas,
                            source,
                            node.id,
                            policy,
                        )
                    })
                    .map(|node| {
                        (
                            deficit(node.id, &counts, &targets),
                            wrh_score(tablet.id, node.id, node.weight),
                            node.id,
                        )
                    })
                    .collect();

                candidates.sort_by(|a, b| {
                    b.0.cmp(&a.0)
                        .then_with(|| b.1.total_cmp(&a.1))
                        .then_with(|| a.2.cmp(&b.2))
                });

                let Some((_, _, target)) = candidates.first().copied() else {
                    continue;
                };

                let updated = placement
                    .replicas
                    .get_mut(&tablet.id)
                    .expect("tablet disappeared");
                let slot = updated
                    .iter_mut()
                    .find(|node_id| **node_id == source)
                    .expect("source replica disappeared");
                *slot = target;
                updated.sort_unstable();

                *counts.entry(source).or_default() -= 1;
                *counts.entry(target).or_default() += 1;
                moves += 1;
                made_progress = true;
                break;
            }
        }
    }

    let converged = counts.iter().all(|(node_id, count)| {
        *count == targets.get(node_id).copied().unwrap_or(0)
    });

    PlannerResult {
        placement,
        moves,
        converged,
    }
}

fn replica_counts(placement: &Placement) -> BTreeMap<NodeId, usize> {
    let mut counts = BTreeMap::new();
    for replicas in placement.replicas.values() {
        for node_id in replicas {
            *counts.entry(*node_id).or_default() += 1;
        }
    }
    counts
}

fn integer_target_counts(
    cluster: &Cluster,
    policy: FailureDomainPolicy,
) -> BTreeMap<NodeId, usize> {
    let fractions = if policy.distinct_zones {
        zone_aware_capacity_inclusion_targets(cluster)
            .unwrap_or_else(|| feasible_capacity_inclusion_targets(cluster))
    } else {
        feasible_capacity_inclusion_targets(cluster)
    };

    let tablet_count = cluster.tablets.len();
    let total_slots = tablet_count
        .saturating_mul(cluster.replication_factor.min(cluster.eligible_node_count()));

    let mut result = BTreeMap::new();
    let mut remainders = Vec::new();
    let mut assigned = 0usize;

    for (node_id, fraction) in fractions {
        let exact = fraction * tablet_count as f64;
        let floor = exact.floor() as usize;
        result.insert(node_id, floor);
        assigned += floor;
        remainders.push((exact - floor as f64, node_id));
    }

    remainders.sort_by(|a, b| {
        b.0.total_cmp(&a.0)
            .then_with(|| a.1.cmp(&b.1))
    });

    for (_, node_id) in remainders
        .into_iter()
        .take(total_slots.saturating_sub(assigned))
    {
        *result.entry(node_id).or_default() += 1;
    }

    result
}

fn overload(
    node_id: NodeId,
    counts: &BTreeMap<NodeId, usize>,
    targets: &BTreeMap<NodeId, usize>,
) -> usize {
    counts
        .get(&node_id)
        .copied()
        .unwrap_or(0)
        .saturating_sub(targets.get(&node_id).copied().unwrap_or(0))
}

fn deficit(
    node_id: NodeId,
    counts: &BTreeMap<NodeId, usize>,
    targets: &BTreeMap<NodeId, usize>,
) -> usize {
    targets
        .get(&node_id)
        .copied()
        .unwrap_or(0)
        .saturating_sub(counts.get(&node_id).copied().unwrap_or(0))
}

fn wrh_score(tablet_id: TabletId, node_id: NodeId, weight: u32) -> f64 {
    let u = unit_interval_open(hash_pair(tablet_id, node_id));
    -(weight as f64) / u.ln()
}

fn replacement_respects_policy(
    cluster: &Cluster,
    replicas: &[NodeId],
    source: NodeId,
    target: NodeId,
    policy: FailureDomainPolicy,
) -> bool {
    let mut candidate = replicas.to_vec();
    let Some(slot) = candidate.iter_mut().find(|node_id| **node_id == source) else {
        return false;
    };
    *slot = target;

    if candidate.iter().collect::<BTreeSet<_>>().len() != candidate.len() {
        return false;
    }

    if policy.distinct_zones {
        let eligible_zone_count = cluster
            .nodes
            .values()
            .filter(|node| node.eligible())
            .map(|node| node.zone.as_str())
            .collect::<BTreeSet<_>>()
            .len();

        if eligible_zone_count >= candidate.len() {
            let zones = candidate
                .iter()
                .map(|node_id| cluster.nodes[node_id].zone.as_str())
                .collect::<BTreeSet<_>>();
            if zones.len() != candidate.len() {
                return false;
            }
        }
    }

    if policy.distinct_racks {
        let eligible_rack_count = cluster
            .nodes
            .values()
            .filter(|node| node.eligible())
            .map(|node| (node.zone.as_str(), node.rack.as_str()))
            .collect::<BTreeSet<_>>()
            .len();

        if eligible_rack_count >= candidate.len() {
            let racks = candidate
                .iter()
                .map(|node_id| {
                    let node = &cluster.nodes[node_id];
                    (node.zone.as_str(), node.rack.as_str())
                })
                .collect::<BTreeSet<_>>();
            if racks.len() != candidate.len() {
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
    use crate::metrics::PlacementMetrics;
    use crate::model::{AdminState, Node, Tablet};
    use crate::placement::PlacementStrategy;

    fn node(id: u64, weight: u32, zone: &str, rack: &str) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: rack.into(),
            state: AdminState::Active,
        }
    }

    #[test]
    fn planner_improves_domain_aware_balance_without_zone_collisions() {
        let before = Cluster {
            epoch: 1,
            replication_factor: 3,
            nodes: [
                node(1, 1, "a", "r1"),
                node(2, 1, "a", "r2"),
                node(3, 1, "b", "r1"),
                node(4, 1, "b", "r2"),
                node(5, 1, "c", "r1"),
                node(6, 1, "c", "r2"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: (0..10_000)
                .map(|id| Tablet { id, bytes: 1 })
                .collect(),
        };

        let current = PlacementStrategy::WeightedRendezvous
            .place(&before, FailureDomainPolicy::HIERARCHICAL);

        let mut after = before.clone();
        after.epoch = 2;
        after.nodes.insert(7, node(7, 1, "d", "r1"));

        let planned = plan_rebalance(
            &after,
            &current,
            FailureDomainPolicy::HIERARCHICAL,
            usize::MAX,
        );
        let metrics = PlacementMetrics::calculate(&after, &planned.placement);

        assert_eq!(metrics.zone_collisions, 0);
        assert!(
            metrics
                .max_zone_aware_capacity_inclusion_error
                .unwrap()
                < 0.001
        );
        assert!(planned.converged);
    }

    #[test]
    fn planner_respects_move_budget() {
        let before = Cluster {
            epoch: 1,
            replication_factor: 1,
            nodes: [node(1, 1, "a", "r1"), node(2, 1, "b", "r1")]
                .into_iter()
                .map(|node| (node.id, node))
                .collect(),
            tablets: (0..100).map(|id| Tablet { id, bytes: 1 }).collect(),
        };
        let current =
            PlacementStrategy::WeightedRendezvous.place(&before, FailureDomainPolicy::NONE);

        let mut after = before.clone();
        after.nodes.insert(3, node(3, 10, "c", "r1"));

        let planned = plan_rebalance(&after, &current, FailureDomainPolicy::NONE, 7);
        assert_eq!(planned.moves, 7);
        assert!(!planned.converged);
    }
}
