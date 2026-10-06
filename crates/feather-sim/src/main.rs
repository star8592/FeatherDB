use std::collections::{BTreeMap, BTreeSet};

use feather_sim::{
    AdminState, Cluster, FailureDomainPolicy, Node, PlacementMetrics, PlacementStrategy, Tablet,
    movement_ratio, plan_rebalance, transition_movement_breakdown,
};

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
    (0..count)
        .map(|id| Tablet {
            id,
            bytes: 1024 * 1024,
        })
        .collect()
}

fn base_heterogeneous(tablet_count: u64) -> Cluster {
    Cluster {
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
        tablets: tablets(tablet_count),
    }
}

fn strong_join_scenario(tablet_count: u64) -> (Cluster, Cluster, BTreeSet<u64>) {
    let mut before = base_heterogeneous(tablet_count);
    before.nodes.remove(&4);
    let mut after = before.clone();
    after.epoch = 2;
    after.nodes.insert(4, node(4, 8, "d", "r1"));
    (before, after, BTreeSet::from([4]))
}

fn leave_scenario(tablet_count: u64) -> (Cluster, Cluster, BTreeSet<u64>) {
    let before = base_heterogeneous(tablet_count);
    let mut after = before.clone();
    after.epoch = 2;
    after.nodes.get_mut(&2).unwrap().state = AdminState::Removed;
    (before, after, BTreeSet::from([2]))
}

fn weight_change_scenario(tablet_count: u64) -> (Cluster, Cluster, BTreeSet<u64>) {
    let before = base_heterogeneous(tablet_count);
    let mut after = before.clone();
    after.epoch = 2;
    after.nodes.get_mut(&2).unwrap().weight = 6;
    (before, after, BTreeSet::from([2]))
}

fn domain_pressure_scenario(tablet_count: u64) -> (Cluster, Cluster, BTreeSet<u64>) {
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
        tablets: tablets(tablet_count),
    };

    let mut after = before.clone();
    after.epoch = 2;
    after.nodes.insert(7, node(7, 1, "d", "r1"));
    (before, after, BTreeSet::from([7]))
}

fn print_result(
    label: &str,
    before: &Cluster,
    after: &Cluster,
    affected_nodes: &BTreeSet<u64>,
    old: &feather_sim::Placement,
    new: &feather_sim::Placement,
) {
    let metrics = PlacementMetrics::calculate(after, new);
    let movement = movement_ratio(old, new, &after.tablets, before.replication_factor);
    let transition = transition_movement_breakdown(old, new, &after.tablets, affected_nodes);
    let zone_error = metrics
        .max_zone_aware_capacity_inclusion_error
        .map(|value| format!("{value:.6}"))
        .unwrap_or_else(|| "n/a".into());

    println!(
        "{label},{movement:.6},{},{},{},{:.6},{zone_error},{:?}",
        transition.excess_changed_tablets,
        metrics.zone_collisions,
        metrics.rack_collisions,
        metrics.max_capacity_inclusion_error,
        metrics.replica_counts
    );
}

fn run_scenario(name: &str, before: &Cluster, after: &Cluster, affected_nodes: &BTreeSet<u64>) {
    println!(
        "scenario={name} tablets={} rf={}",
        before.tablets.len(),
        before.replication_factor
    );
    println!(
        "strategy,movement_ratio,excess_changed_tablets,zone_collisions,rack_collisions,max_capacity_error,max_zone_aware_error,replica_counts"
    );

    for (strategy, policy, label) in [
        (
            PlacementStrategy::HashRing {
                virtual_nodes_per_weight: 64,
            },
            FailureDomainPolicy::NONE,
            "hash-ring",
        ),
        (
            PlacementStrategy::WeightedRendezvous,
            FailureDomainPolicy::NONE,
            "weighted-rendezvous",
        ),
        (
            PlacementStrategy::WeightedRendezvous,
            FailureDomainPolicy::HIERARCHICAL,
            "constrained-wrh",
        ),
    ] {
        let old = strategy.place(before, policy);
        let new = strategy.place(after, policy);
        print_result(label, before, after, affected_nodes, &old, &new);
    }

    let current =
        PlacementStrategy::WeightedRendezvous.place(before, FailureDomainPolicy::HIERARCHICAL);
    let planned = plan_rebalance(after, &current, FailureDomainPolicy::HIERARCHICAL);
    print_result(
        "stateful-planner",
        before,
        after,
        affected_nodes,
        &current,
        &planned.placement,
    );
    println!(
        "planner_status,moves={},movement_lower_bound={},gap={},converged={}",
        planned.moves, planned.movement_lower_bound, planned.movement_gap, planned.converged
    );
}

fn main() {
    let tablet_count = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse::<u64>().ok())
        .unwrap_or(10_000);

    println!("feather-sim placement comparison");

    for (name, scenario) in [
        (
            "heterogeneous-strong-join",
            strong_join_scenario(tablet_count),
        ),
        ("heterogeneous-leave", leave_scenario(tablet_count)),
        (
            "heterogeneous-weight-change",
            weight_change_scenario(tablet_count),
        ),
        (
            "failure-domain-pressure",
            domain_pressure_scenario(tablet_count),
        ),
    ] {
        run_scenario(name, &scenario.0, &scenario.1, &scenario.2);
    }
}
