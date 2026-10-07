use std::collections::BTreeMap;
use std::time::Instant;

use feather_sim::{
    AdminState, Cluster, CompactPlacement, CompactTabletCatalog, CompactWindowScheduler,
    FailureDomainPolicy, MigrationBudget, Node,
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

fn main() {
    let tablet_count = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    let window_tablets = std::env::args()
        .nth(2)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);

    let catalog_started = Instant::now();
    let catalog = CompactTabletCatalog::uniform(10_000_000, tablet_count, tablet_count as u64)
        .expect("uniform compact catalog");
    let catalog_elapsed = catalog_started.elapsed();

    let before_cluster = Cluster {
        epoch: 1,
        replication_factor: 2,
        nodes: [node(1, 1, "a"), node(2, 2, "b"), node(3, 4, "c")]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
        tablets: Vec::new(),
    };
    let mut after_cluster = before_cluster.clone();
    after_cluster.epoch = 2;
    after_cluster.nodes.insert(4, node(4, 8, "d"));

    let started = Instant::now();
    let actual = CompactPlacement::weighted_rendezvous_for_catalog(
        &before_cluster,
        &catalog,
        FailureDomainPolicy::HIERARCHICAL,
    )
    .expect("actual compact placement");
    let desired = CompactPlacement::weighted_rendezvous_for_catalog(
        &after_cluster,
        &catalog,
        FailureDomainPolicy::HIERARCHICAL,
    )
    .expect("desired compact placement");
    let placement_elapsed = started.elapsed();

    let changed = actual
        .changed_replica_count(&desired)
        .expect("same compact shape");

    let mut scheduler = CompactWindowScheduler::new(
        after_cluster,
        catalog,
        actual,
        desired,
        FailureDomainPolicy::HIERARCHICAL,
        MigrationBudget {
            max_active: window_tablets,
            max_per_node_active: window_tablets,
            bytes_per_tick: u64::MAX,
            max_bytes_per_node_per_tick: u64::MAX,
            max_bytes_per_task_per_tick: u64::MAX,
        },
        window_tablets,
    )
    .expect("bounded window scheduler");

    let run_started = Instant::now();
    let mut ticks = 0_u64;
    let mut completed = 0_usize;
    while !scheduler.is_converged() && ticks < 100_000 {
        let report = scheduler.tick().expect("bounded window tick");
        completed += report.completed;
        ticks += 1;
    }
    let run_elapsed = run_started.elapsed();

    assert!(scheduler.is_converged());
    assert_eq!(completed as u64, changed);

    println!(
        "bounded-million-lab tablets={} window_tablets={} changed_moves={} ticks={} windows={}",
        tablet_count,
        window_tablets,
        changed,
        ticks,
        scheduler.total_windows_completed()
    );
    println!(
        "memory catalog_raw_bytes={} actual_desired_replica_bytes={} core_raw_bytes={} peak_window_tablets={} peak_materialized_tasks={} task_size_bytes={} peak_task_struct_bytes={}",
        tablet_count.saturating_mul(24),
        tablet_count
            .saturating_mul(2)
            .saturating_mul(2)
            .saturating_mul(8),
        tablet_count.saturating_mul(24 + 2 * 2 * 8),
        scheduler.peak_window_tablets(),
        scheduler.peak_materialized_tasks(),
        std::mem::size_of::<feather_sim::MigrationTask>(),
        scheduler
            .peak_materialized_tasks()
            .saturating_mul(std::mem::size_of::<feather_sim::MigrationTask>())
    );
    println!(
        "timing catalog_ms={} placements_ms={} execute_ms={}",
        catalog_elapsed.as_millis(),
        placement_elapsed.as_millis(),
        run_elapsed.as_millis()
    );
}
