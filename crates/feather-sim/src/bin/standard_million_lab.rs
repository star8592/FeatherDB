use std::collections::BTreeMap;
use std::time::Instant;

use feather_sim::{
    AdminState, Cluster, FailureDomainPolicy, Node, PlacementStrategy, Tablet,
    transition_movement_breakdown,
};

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
        nodes: [node(1, 1, "a"), node(2, 2, "b"), node(3, 4, "c")]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
        tablets: (0..tablet_count)
            .map(|id| Tablet { id, bytes: 1 })
            .collect(),
    }
}

fn main() {
    let tablet_count = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(100_000);

    let before_cluster = cluster(tablet_count);
    let mut after_cluster = before_cluster.clone();
    after_cluster.epoch = 2;
    after_cluster.nodes.insert(4, node(4, 8, "d"));

    let started = Instant::now();
    let before = PlacementStrategy::WeightedRendezvous
        .place(&before_cluster, FailureDomainPolicy::HIERARCHICAL);
    let before_elapsed = started.elapsed();

    let started = Instant::now();
    let after = PlacementStrategy::WeightedRendezvous
        .place(&after_cluster, FailureDomainPolicy::HIERARCHICAL);
    let after_elapsed = started.elapsed();

    let started = Instant::now();
    let transition = transition_movement_breakdown(
        &before,
        &after,
        &after_cluster.tablets,
        &std::collections::BTreeSet::from([4]),
    );
    let movement_elapsed = started.elapsed();

    println!(
        "standard-million-lab tablets={} rf={} before_entries={} after_entries={}",
        tablet_count,
        before_cluster.replication_factor,
        before.replicas.len(),
        after.replicas.len()
    );
    println!(
        "timing before_ms={} after_ms={} movement_ms={}",
        before_elapsed.as_millis(),
        after_elapsed.as_millis(),
        movement_elapsed.as_millis()
    );
    println!(
        "movement changed_tablets={} excess_changed_tablets={}",
        transition.total_changed_tablets, transition.excess_changed_tablets
    );
}
