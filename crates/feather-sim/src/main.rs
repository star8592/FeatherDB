use std::collections::{BTreeMap, BTreeSet};

use feather_sim::{
    AdminState, Cluster, FailureDomainPolicy, Node, PlacementMetrics, PlacementStrategy, Tablet,
    join_movement_breakdown, movement_ratio,
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

fn strong_join_scenario(tablet_count: u64) -> (Cluster, Cluster, BTreeSet<u64>) {
    let before_nodes = [
        node(1, 1, "a", "r1"),
        node(2, 2, "b", "r1"),
        node(3, 4, "c", "r1"),
    ]
    .into_iter()
    .map(|node| (node.id, node))
    .collect::<BTreeMap<_, _>>();

    let before = Cluster {
        epoch: 1,
        replication_factor: 2,
        nodes: before_nodes,
        tablets: tablets(tablet_count),
    };

    let mut after = before.clone();
    after.epoch = 2;
    after.nodes.insert(4, node(4, 8, "d", "r1"));

    (before, after, BTreeSet::from([4]))
}

fn domain_pressure_scenario(tablet_count: u64) -> (Cluster, Cluster, BTreeSet<u64>) {
    let before_nodes = [
        node(1, 1, "a", "r1"),
        node(2, 1, "a", "r2"),
        node(3, 1, "b", "r1"),
        node(4, 1, "b", "r2"),
        node(5, 1, "c", "r1"),
        node(6, 1, "c", "r2"),
    ]
    .into_iter()
    .map(|node| (node.id, node))
    .collect::<BTreeMap<_, _>>();

    let before = Cluster {
        epoch: 1,
        replication_factor: 3,
        nodes: before_nodes,
        tablets: tablets(tablet_count),
    };

    let mut after = before.clone();
    after.epoch = 2;
    after.nodes.insert(7, node(7, 1, "d", "r1"));

    (before, after, BTreeSet::from([7]))
}

fn run_scenario(name: &str, before: &Cluster, after: &Cluster, joining_nodes: &BTreeSet<u64>) {
    let experiments = [
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
    ];

    println!(
        "scenario={name} tablets={} rf={}",
        before.tablets.len(),
        before.replication_factor
    );
    println!(
        "strategy,movement_ratio,excess_join_bytes,zone_collisions,rack_collisions,replica_counts"
    );

    for (strategy, policy, label) in experiments {
        let old = strategy.place(before, policy);
        let new = strategy.place(after, policy);
        let metrics = PlacementMetrics::calculate(after, &new);
        let movement = movement_ratio(&old, &new, &after.tablets, before.replication_factor);
        let breakdown = join_movement_breakdown(&old, &new, &after.tablets, joining_nodes);

        println!(
            "{label},{movement:.6},{},{},{},{:?}",
            breakdown.excess_bytes_to_existing_nodes,
            metrics.zone_collisions,
            metrics.rack_collisions,
            metrics.replica_counts
        );
    }
}

fn main() {
    let tablet_count = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse::<u64>().ok())
        .unwrap_or(10_000);

    println!("feather-sim placement comparison");

    let (before, after, joining) = strong_join_scenario(tablet_count);
    run_scenario("heterogeneous-strong-join", &before, &after, &joining);

    let (before, after, joining) = domain_pressure_scenario(tablet_count);
    run_scenario("failure-domain-pressure", &before, &after, &joining);
}
