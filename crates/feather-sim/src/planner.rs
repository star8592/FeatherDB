use std::collections::{BTreeMap, BTreeSet};

use crate::hash::{hash_pair, unit_interval_open};
use crate::metrics::{feasible_capacity_inclusion_targets, zone_aware_capacity_inclusion_targets};
use crate::model::{Cluster, Node, NodeId, Placement, Tablet, TabletId};
use crate::placement::FailureDomainPolicy;

#[derive(Clone, Debug)]
pub struct PlannerResult {
    pub placement: Placement,
    pub moves: usize,
    pub movement_lower_bound: usize,
    pub movement_gap: usize,
    pub converged: bool,
}

pub fn plan_rebalance(
    cluster: &Cluster,
    current: &Placement,
    policy: FailureDomainPolicy,
) -> PlannerResult {
    let targets = integer_target_counts(cluster, policy);
    let mut placement =
        if policy.distinct_zones && eligible_zone_count(cluster) >= target_replica_count(cluster) {
            plan_with_zone_quotas(cluster, current, &targets)
        } else {
            plan_with_node_quotas(cluster, current, &targets, policy)
        };

    if let Some(desired) = placement.as_mut() {
        improve_stickiness(cluster, current, desired, policy);
    }

    let converged = placement
        .as_ref()
        .is_some_and(|placement| target_counts_match(cluster, placement, &targets));

    let placement = placement.unwrap_or_else(|| current.clone());
    let moves = changed_replica_count(current, &placement, &cluster.tablets);
    let movement_lower_bound = count_movement_lower_bound(current, &targets);
    let movement_gap = moves.saturating_sub(movement_lower_bound);

    PlannerResult {
        placement,
        moves,
        movement_lower_bound,
        movement_gap,
        converged,
    }
}

fn target_replica_count(cluster: &Cluster) -> usize {
    cluster
        .replication_factor
        .min(cluster.eligible_node_count())
}

fn eligible_zone_count(cluster: &Cluster) -> usize {
    cluster
        .nodes
        .values()
        .filter(|node| node.eligible())
        .map(|node| node.zone.as_str())
        .collect::<BTreeSet<_>>()
        .len()
}

fn plan_with_zone_quotas(
    cluster: &Cluster,
    current: &Placement,
    targets: &BTreeMap<NodeId, usize>,
) -> Option<Placement> {
    let rf = target_replica_count(cluster);
    let tablet_count = cluster.tablets.len();
    let mut node_remaining = targets.clone();
    let mut zone_remaining = BTreeMap::<String, usize>::new();

    for (node_id, target) in targets {
        let node = cluster.nodes.get(node_id)?;
        *zone_remaining.entry(node.zone.clone()).or_default() += *target;
    }

    let mut placement = Placement::default();

    for (index, tablet) in cluster.tablets.iter().enumerate() {
        let tablets_left = tablet_count - index;
        let old = current
            .replicas
            .get(&tablet.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        let mut selected_zones = Vec::<String>::new();

        let mut forced_zones: Vec<_> = zone_remaining
            .iter()
            .filter(|(_, remaining)| **remaining == tablets_left && **remaining > 0)
            .map(|(zone, _)| zone.clone())
            .collect();
        forced_zones.sort();

        if forced_zones.len() > rf {
            return None;
        }
        selected_zones.extend(forced_zones);

        let mut current_zones: Vec<_> = old
            .iter()
            .filter_map(|node_id| cluster.nodes.get(node_id))
            .filter(|node| node.eligible())
            .map(|node| node.zone.clone())
            .filter(|zone| {
                zone_remaining.get(zone).copied().unwrap_or(0) > 0 && !selected_zones.contains(zone)
            })
            .collect();
        current_zones.sort();
        current_zones.dedup();
        current_zones.sort_by(|a, b| {
            zone_remaining
                .get(b)
                .copied()
                .unwrap_or(0)
                .cmp(&zone_remaining.get(a).copied().unwrap_or(0))
                .then_with(|| a.cmp(b))
        });

        for zone in current_zones {
            if selected_zones.len() == rf {
                break;
            }
            selected_zones.push(zone);
        }

        if selected_zones.len() < rf {
            let mut other_zones: Vec<_> = zone_remaining
                .iter()
                .filter(|(zone, remaining)| **remaining > 0 && !selected_zones.contains(zone))
                .map(|(zone, remaining)| {
                    (
                        *remaining,
                        zone_best_wrh_score(cluster, tablet.id, zone, &node_remaining),
                        zone.clone(),
                    )
                })
                .collect();

            other_zones.sort_by(|a, b| {
                b.0.cmp(&a.0)
                    .then_with(|| b.1.total_cmp(&a.1))
                    .then_with(|| a.2.cmp(&b.2))
            });

            for (_, _, zone) in other_zones {
                if selected_zones.len() == rf {
                    break;
                }
                selected_zones.push(zone);
            }
        }

        if selected_zones.len() != rf {
            return None;
        }

        let mut replicas = Vec::with_capacity(rf);
        for zone in selected_zones {
            let zone_total_before = zone_remaining.get(&zone).copied().unwrap_or(0);
            if zone_total_before == 0 {
                return None;
            }

            let forced_node = cluster
                .nodes
                .values()
                .filter(|node| node.eligible() && node.zone == zone)
                .filter(|node| {
                    node_remaining.get(&node.id).copied().unwrap_or(0) == zone_total_before
                        && zone_total_before > 0
                })
                .map(|node| node.id)
                .min();

            let target = if let Some(node_id) = forced_node {
                node_id
            } else {
                choose_node_in_zone(cluster, tablet.id, &zone, old, &node_remaining)?
            };

            replicas.push(target);
            *node_remaining.get_mut(&target)? -= 1;
            *zone_remaining.get_mut(&zone)? -= 1;
        }

        replicas.sort_unstable();
        placement.replicas.insert(tablet.id, replicas);
    }

    if node_remaining.values().any(|remaining| *remaining != 0)
        || zone_remaining.values().any(|remaining| *remaining != 0)
    {
        return None;
    }

    Some(placement)
}

fn choose_node_in_zone(
    cluster: &Cluster,
    tablet_id: TabletId,
    zone: &str,
    old: &[NodeId],
    node_remaining: &BTreeMap<NodeId, usize>,
) -> Option<NodeId> {
    let mut candidates: Vec<_> = cluster
        .nodes
        .values()
        .filter(|node| node.eligible() && node.zone == zone)
        .filter(|node| node_remaining.get(&node.id).copied().unwrap_or(0) > 0)
        .map(|node| {
            (
                usize::from(old.contains(&node.id)),
                node_remaining.get(&node.id).copied().unwrap_or(0),
                wrh_score(tablet_id, node),
                node.id,
            )
        })
        .collect();

    candidates.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| b.1.cmp(&a.1))
            .then_with(|| b.2.total_cmp(&a.2))
            .then_with(|| a.3.cmp(&b.3))
    });

    candidates.first().map(|candidate| candidate.3)
}

fn zone_best_wrh_score(
    cluster: &Cluster,
    tablet_id: TabletId,
    zone: &str,
    node_remaining: &BTreeMap<NodeId, usize>,
) -> f64 {
    cluster
        .nodes
        .values()
        .filter(|node| node.eligible() && node.zone == zone)
        .filter(|node| node_remaining.get(&node.id).copied().unwrap_or(0) > 0)
        .map(|node| wrh_score(tablet_id, node))
        .fold(f64::NEG_INFINITY, f64::max)
}

fn plan_with_node_quotas(
    cluster: &Cluster,
    current: &Placement,
    targets: &BTreeMap<NodeId, usize>,
    policy: FailureDomainPolicy,
) -> Option<Placement> {
    let rf = target_replica_count(cluster);
    let tablet_count = cluster.tablets.len();
    let mut remaining = targets.clone();
    let mut placement = Placement::default();

    for (index, tablet) in cluster.tablets.iter().enumerate() {
        let tablets_left = tablet_count - index;
        let old = current
            .replicas
            .get(&tablet.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        let mut selected = Vec::with_capacity(rf);

        let mut forced: Vec<_> = remaining
            .iter()
            .filter(|(_, count)| **count == tablets_left && **count > 0)
            .map(|(node_id, _)| *node_id)
            .collect();
        forced.sort_unstable();

        if forced.len() > rf {
            return None;
        }

        for node_id in forced {
            if node_allowed(cluster, &selected, node_id, policy) {
                selected.push(node_id);
            } else {
                return None;
            }
        }

        let mut candidates: Vec<_> = cluster
            .nodes
            .values()
            .filter(|node| node.eligible())
            .filter(|node| !selected.contains(&node.id))
            .filter(|node| remaining.get(&node.id).copied().unwrap_or(0) > 0)
            .map(|node| {
                (
                    usize::from(old.contains(&node.id)),
                    remaining.get(&node.id).copied().unwrap_or(0),
                    wrh_score(tablet.id, node),
                    node.id,
                )
            })
            .collect();

        candidates.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| b.2.total_cmp(&a.2))
                .then_with(|| a.3.cmp(&b.3))
        });

        for (_, _, _, node_id) in candidates {
            if selected.len() == rf {
                break;
            }
            if node_allowed(cluster, &selected, node_id, policy) {
                selected.push(node_id);
            }
        }

        if selected.len() != rf {
            return None;
        }

        selected.sort_unstable();
        for node_id in &selected {
            *remaining.get_mut(node_id)? -= 1;
        }
        placement.replicas.insert(tablet.id, selected);
    }

    if remaining.values().any(|remaining| *remaining != 0) {
        return None;
    }

    Some(placement)
}

fn node_allowed(
    cluster: &Cluster,
    selected: &[NodeId],
    candidate: NodeId,
    policy: FailureDomainPolicy,
) -> bool {
    if selected.contains(&candidate) {
        return false;
    }

    let node = &cluster.nodes[&candidate];

    if policy.distinct_zones
        && eligible_zone_count(cluster) >= target_replica_count(cluster)
        && selected
            .iter()
            .any(|node_id| cluster.nodes[node_id].zone == node.zone)
    {
        return false;
    }

    if policy.distinct_racks {
        let rack = (node.zone.as_str(), node.rack.as_str());
        let eligible_racks = cluster
            .nodes
            .values()
            .filter(|node| node.eligible())
            .map(|node| (node.zone.as_str(), node.rack.as_str()))
            .collect::<BTreeSet<_>>()
            .len();

        if eligible_racks >= target_replica_count(cluster)
            && selected.iter().any(|node_id| {
                let other = &cluster.nodes[node_id];
                (other.zone.as_str(), other.rack.as_str()) == rack
            })
        {
            return false;
        }
    }

    true
}

fn improve_stickiness(
    cluster: &Cluster,
    current: &Placement,
    desired: &mut Placement,
    policy: FailureDomainPolicy,
) {
    for _ in 0..8 {
        let mut opportunities = BTreeMap::<(NodeId, NodeId), Vec<TabletId>>::new();

        for tablet in &cluster.tablets {
            let old = current
                .replicas
                .get(&tablet.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let assigned = desired
                .replicas
                .get(&tablet.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            for assigned_node in assigned {
                for old_node in old {
                    if !assigned.contains(old_node) {
                        opportunities
                            .entry((*assigned_node, *old_node))
                            .or_default()
                            .push(tablet.id);
                    }
                }
            }
        }

        let mut used = BTreeSet::new();
        let mut swaps = Vec::new();

        for tablet in &cluster.tablets {
            if used.contains(&tablet.id) {
                continue;
            }

            let old_a = current
                .replicas
                .get(&tablet.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let assigned_a = desired
                .replicas
                .get(&tablet.id)
                .cloned()
                .unwrap_or_default();

            let bad_a: Vec<_> = assigned_a
                .iter()
                .copied()
                .filter(|node_id| !old_a.contains(node_id))
                .collect();
            let missing_a: Vec<_> = old_a
                .iter()
                .copied()
                .filter(|node_id| !assigned_a.contains(node_id))
                .collect();

            let mut found = None;

            'search: for assigned_x in bad_a {
                for old_y in &missing_a {
                    let Some(candidates) = opportunities.get(&(*old_y, assigned_x)) else {
                        continue;
                    };

                    for tablet_b in candidates {
                        if *tablet_b == tablet.id || used.contains(tablet_b) {
                            continue;
                        }

                        let assigned_b =
                            desired.replicas.get(tablet_b).cloned().unwrap_or_default();

                        if !assigned_b.contains(old_y) || assigned_b.contains(&assigned_x) {
                            continue;
                        }

                        let mut next_a = assigned_a.clone();
                        let Some(slot_a) =
                            next_a.iter_mut().find(|node_id| **node_id == assigned_x)
                        else {
                            continue;
                        };
                        *slot_a = *old_y;
                        next_a.sort_unstable();

                        let mut next_b = assigned_b.clone();
                        let Some(slot_b) = next_b.iter_mut().find(|node_id| **node_id == *old_y)
                        else {
                            continue;
                        };
                        *slot_b = assigned_x;
                        next_b.sort_unstable();

                        if !replica_set_respects_policy(cluster, &next_a, policy)
                            || !replica_set_respects_policy(cluster, &next_b, policy)
                        {
                            continue;
                        }

                        let old_b = current
                            .replicas
                            .get(tablet_b)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]);

                        let before =
                            changed_slots(old_a, &assigned_a) + changed_slots(old_b, &assigned_b);
                        let after = changed_slots(old_a, &next_a) + changed_slots(old_b, &next_b);

                        if after < before {
                            found = Some((tablet.id, next_a, *tablet_b, next_b));
                            break 'search;
                        }
                    }
                }
            }

            if let Some((tablet_a, next_a, tablet_b, next_b)) = found {
                used.insert(tablet_a);
                used.insert(tablet_b);
                swaps.push((tablet_a, next_a, tablet_b, next_b));
            }
        }

        if swaps.is_empty() {
            break;
        }

        for (tablet_a, next_a, tablet_b, next_b) in swaps {
            desired.replicas.insert(tablet_a, next_a);
            desired.replicas.insert(tablet_b, next_b);
        }
    }
}

fn changed_slots(old: &[NodeId], new: &[NodeId]) -> usize {
    new.iter().filter(|node_id| !old.contains(node_id)).count()
}

fn replica_set_respects_policy(
    cluster: &Cluster,
    replicas: &[NodeId],
    policy: FailureDomainPolicy,
) -> bool {
    if replicas.iter().collect::<BTreeSet<_>>().len() != replicas.len() {
        return false;
    }

    if replicas.iter().any(|node_id| {
        !cluster
            .nodes
            .get(node_id)
            .is_some_and(|node| node.eligible())
    }) {
        return false;
    }

    if policy.distinct_zones && eligible_zone_count(cluster) >= replicas.len() {
        let zones = replicas
            .iter()
            .map(|node_id| cluster.nodes[node_id].zone.as_str())
            .collect::<BTreeSet<_>>();
        if zones.len() != replicas.len() {
            return false;
        }
    }

    if policy.distinct_racks {
        let eligible_racks = cluster
            .nodes
            .values()
            .filter(|node| node.eligible())
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
    let total_slots = tablet_count.saturating_mul(target_replica_count(cluster));

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

    remainders.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

    for (_, node_id) in remainders
        .into_iter()
        .take(total_slots.saturating_sub(assigned))
    {
        *result.entry(node_id).or_default() += 1;
    }

    result
}

fn target_counts_match(
    cluster: &Cluster,
    placement: &Placement,
    targets: &BTreeMap<NodeId, usize>,
) -> bool {
    let mut counts = BTreeMap::<NodeId, usize>::new();
    for replicas in placement.replicas.values() {
        if replicas.len() != target_replica_count(cluster) {
            return false;
        }
        if replicas.iter().collect::<BTreeSet<_>>().len() != replicas.len() {
            return false;
        }
        for node_id in replicas {
            *counts.entry(*node_id).or_default() += 1;
        }
    }

    cluster.nodes.keys().all(|node_id| {
        counts.get(node_id).copied().unwrap_or(0) == targets.get(node_id).copied().unwrap_or(0)
    })
}

fn count_movement_lower_bound(current: &Placement, targets: &BTreeMap<NodeId, usize>) -> usize {
    let mut current_counts = BTreeMap::<NodeId, usize>::new();
    for replicas in current.replicas.values() {
        for node_id in replicas {
            *current_counts.entry(*node_id).or_default() += 1;
        }
    }

    targets
        .iter()
        .map(|(node_id, target)| {
            target.saturating_sub(current_counts.get(node_id).copied().unwrap_or(0))
        })
        .sum()
}

fn changed_replica_count(before: &Placement, after: &Placement, tablets: &[Tablet]) -> usize {
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
            new.iter().filter(|node_id| !old.contains(node_id)).count()
        })
        .sum()
}

fn wrh_score(tablet_id: TabletId, node: &Node) -> f64 {
    let u = unit_interval_open(hash_pair(tablet_id, node.id));
    -(node.weight as f64) / u.ln()
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

    fn tablets(count: u64) -> Vec<Tablet> {
        (0..count).map(|id| Tablet { id, bytes: 1 }).collect()
    }

    #[test]
    fn planner_converges_strong_join_exactly() {
        let before = Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, 1, "a", "r1"),
                node(2, 2, "b", "r1"),
                node(3, 4, "c", "r1"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: tablets(10_000),
        };
        let current =
            PlacementStrategy::WeightedRendezvous.place(&before, FailureDomainPolicy::HIERARCHICAL);

        let mut after = before.clone();
        after.epoch = 2;
        after.nodes.insert(4, node(4, 8, "d", "r1"));

        let planned = plan_rebalance(&after, &current, FailureDomainPolicy::HIERARCHICAL);
        let metrics = PlacementMetrics::calculate(&after, &planned.placement);

        assert!(planned.converged);
        assert_eq!(planned.moves, 10_000);
        assert_eq!(metrics.zone_collisions, 0);
        assert!(metrics.max_zone_aware_capacity_inclusion_error.unwrap() < 0.001);
    }

    #[test]
    fn planner_eliminates_removed_node() {
        let before = Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, 1, "a", "r1"),
                node(2, 2, "b", "r1"),
                node(3, 4, "c", "r1"),
                node(4, 8, "d", "r1"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: tablets(10_000),
        };
        let current =
            PlacementStrategy::WeightedRendezvous.place(&before, FailureDomainPolicy::HIERARCHICAL);

        let mut after = before.clone();
        after.nodes.get_mut(&2).unwrap().state = AdminState::Removed;

        let planned = plan_rebalance(&after, &current, FailureDomainPolicy::HIERARCHICAL);

        assert!(planned.converged);
        assert!(
            planned
                .placement
                .replicas
                .values()
                .all(|replicas| !replicas.contains(&2))
        );
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
            tablets: tablets(10_000),
        };

        let current =
            PlacementStrategy::WeightedRendezvous.place(&before, FailureDomainPolicy::HIERARCHICAL);

        let mut after = before.clone();
        after.epoch = 2;
        after.nodes.insert(7, node(7, 1, "d", "r1"));

        let planned = plan_rebalance(&after, &current, FailureDomainPolicy::HIERARCHICAL);
        let metrics = PlacementMetrics::calculate(&after, &planned.placement);

        assert_eq!(metrics.zone_collisions, 0);
        assert!(metrics.max_zone_aware_capacity_inclusion_error.unwrap() < 0.001);
        assert!(planned.converged);
    }

    #[test]
    fn planner_is_deterministic() {
        let cluster = Cluster {
            epoch: 1,
            replication_factor: 2,
            nodes: [
                node(1, 1, "a", "r1"),
                node(2, 2, "b", "r1"),
                node(3, 4, "c", "r1"),
                node(4, 8, "d", "r1"),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect(),
            tablets: tablets(1_000),
        };
        let current = PlacementStrategy::WeightedRendezvous
            .place(&cluster, FailureDomainPolicy::HIERARCHICAL);

        let a = plan_rebalance(&cluster, &current, FailureDomainPolicy::HIERARCHICAL);
        let b = plan_rebalance(&cluster, &current, FailureDomainPolicy::HIERARCHICAL);

        assert_eq!(a.placement, b.placement);
        assert_eq!(a.moves, b.moves);
        assert_eq!(a.movement_lower_bound, b.movement_lower_bound);
        assert_eq!(a.movement_gap, b.movement_gap);
        assert_eq!(a.converged, b.converged);
    }
}
