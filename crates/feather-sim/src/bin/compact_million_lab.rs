use std::collections::BTreeMap;
use std::time::Instant;

use feather_sim::{AdminState, Cluster, CompactPlacement, FailureDomainPolicy, Node};

fn node(id: u64, weight: u32, zone: &str) -> Node {
    Node {
        id,
        weight,
        zone: zone.into(),
        rack: "r1".into(),
        state: AdminState::Active,
    }
}

fn base_cluster() -> Cluster {
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

fn main() {
    let tablet_count = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1_000_000);

    let before_cluster = base_cluster();
    let mut after_cluster = before_cluster.clone();
    after_cluster.epoch = 2;
    after_cluster.nodes.insert(4, node(4, 8, "d"));

    let started = Instant::now();
    let before = CompactPlacement::weighted_rendezvous(
        &before_cluster,
        tablet_count,
        FailureDomainPolicy::HIERARCHICAL,
    )
    .expect("before compact placement");
    let before_elapsed = started.elapsed();

    let started = Instant::now();
    let after = CompactPlacement::weighted_rendezvous(
        &after_cluster,
        tablet_count,
        FailureDomainPolicy::HIERARCHICAL,
    )
    .expect("after compact placement");
    let after_elapsed = started.elapsed();

    let started = Instant::now();
    let changed_replicas = before
        .changed_replica_count(&after)
        .expect("same compact shape");
    let changed_tablets = before
        .changed_tablet_count(&after)
        .expect("same compact shape");
    let movement_elapsed = started.elapsed();

    let started = Instant::now();
    let mut cursor =
        feather_sim::CompactMigrationCursor::new(&before, &after).expect("same compact shape");
    let lazy_moves = cursor.by_ref().count() as u64;
    let lazy_elapsed = started.elapsed();
    assert_eq!(lazy_moves, changed_replicas);

    let before_collisions = before.zone_collision_count(&before_cluster);
    let after_collisions = after.zone_collision_count(&after_cluster);

    println!(
        "compact-million-lab tablets={} rf={} bytes_per_tablet={} before_replica_bytes={} after_replica_bytes={} retained_replica_bytes={}",
        tablet_count,
        before.replica_count(),
        before.bytes_per_tablet(),
        before.logical_replica_bytes(),
        after.logical_replica_bytes(),
        before
            .allocated_replica_bytes()
            .saturating_add(after.allocated_replica_bytes())
    );
    println!(
        "timing before_ms={} after_ms={} movement_ms={}",
        before_elapsed.as_millis(),
        after_elapsed.as_millis(),
        movement_elapsed.as_millis()
    );
    println!(
        "task_size_bytes={} estimated_eager_task_bytes={} lazy_move_size_bytes={} lazy_peak_buffered_moves={} lazy_buffer_capacity_bytes={} lazy_scan_ms={}",
        std::mem::size_of::<feather_sim::MigrationTask>(),
        (changed_replicas as usize)
            .saturating_mul(std::mem::size_of::<feather_sim::MigrationTask>()),
        std::mem::size_of::<feather_sim::CompactMigrationMove>(),
        cursor.peak_buffered_moves(),
        cursor.buffered_capacity_bytes(),
        lazy_elapsed.as_millis()
    );
    println!(
        "movement changed_replicas={} changed_tablets={} ratio={:.6}",
        changed_replicas,
        changed_tablets,
        changed_replicas as f64 / (tablet_count as f64 * before.replica_count() as f64)
    );
    println!(
        "safety before_zone_collisions={} after_zone_collisions={} before_checksum={} after_checksum={}",
        before_collisions,
        after_collisions,
        before.stable_checksum(),
        after.stable_checksum()
    );
    println!("replica_counts_before={:?}", before.replica_counts());
    println!("replica_counts_after={:?}", after.replica_counts());
}
