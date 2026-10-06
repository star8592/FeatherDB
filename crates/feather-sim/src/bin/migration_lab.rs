use std::collections::BTreeMap;

use feather_sim::{
    AdminState, Cluster, FailureDomainPolicy, MigrationBudget, MigrationScheduler, Node, Placement,
    PlacementStrategy, Tablet, plan_rebalance,
};

const MIB: u64 = 1024 * 1024;

fn node(id: u64, zone: &str) -> Node {
    Node {
        id,
        weight: 1,
        zone: zone.into(),
        rack: "r1".into(),
        state: AdminState::Active,
    }
}

fn scenario(tablet_count: u64, tablet_bytes: u64) -> (Cluster, Placement, Placement, usize) {
    let before = Cluster {
        epoch: 1,
        replication_factor: 2,
        nodes: [node(1, "a"), node(2, "b"), node(3, "c")]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
        tablets: (0..tablet_count)
            .map(|id| Tablet {
                id,
                bytes: tablet_bytes,
            })
            .collect(),
    };

    let actual =
        PlacementStrategy::WeightedRendezvous.place(&before, FailureDomainPolicy::HIERARCHICAL);

    let mut after = before;
    after.epoch = 2;
    after.nodes.insert(4, node(4, "d"));

    let planned = plan_rebalance(&after, &actual, FailureDomainPolicy::HIERARCHICAL);
    assert!(planned.converged);

    (after, actual, planned.placement, planned.moves)
}

fn run_profile(
    name: &str,
    cluster: &Cluster,
    actual: &Placement,
    desired: &Placement,
    budget: MigrationBudget,
) {
    let mut scheduler = MigrationScheduler::new(
        cluster.clone(),
        FailureDomainPolicy::HIERARCHICAL,
        actual.clone(),
        desired.clone(),
        budget,
    )
    .expect("valid migration schedule");

    let mut ticks = 0_u64;
    let mut copied = 0_u64;
    let mut max_active = 0_usize;
    let mut max_queued = scheduler.tasks().len();

    while !scheduler.is_converged() && ticks < 1_000_000 {
        let report = scheduler.tick();
        ticks += 1;
        copied += report.bytes_copied;
        max_active = max_active.max(report.active);
        max_queued = max_queued.max(report.queued);

        if report.bytes_copied == 0
            && report.started == 0
            && report.completed == 0
            && report.active == 0
        {
            break;
        }
    }

    println!(
        "{name},converged={},ticks={},bytes_copied={},max_active={},peak_queued={},completed={}",
        scheduler.is_converged(),
        ticks,
        copied,
        max_active,
        max_queued,
        scheduler.total_completed
    );
}

fn main() {
    let tablet_count = 1_000;
    let tablet_bytes = 64 * MIB;
    let (cluster, actual, desired, moves) = scenario(tablet_count, tablet_bytes);

    println!(
        "migration-lab tablets={tablet_count} tablet_bytes={} planned_moves={moves}",
        tablet_bytes
    );

    run_profile(
        "node-rate-16m",
        &cluster,
        &actual,
        &desired,
        MigrationBudget {
            max_active: 4,
            max_per_node_active: 4,
            bytes_per_tick: 32 * MIB,
            max_bytes_per_node_per_tick: 16 * MIB,
            max_bytes_per_task_per_tick: 8 * MIB,
        },
    );

    run_profile(
        "node-rate-32m",
        &cluster,
        &actual,
        &desired,
        MigrationBudget {
            max_active: 4,
            max_per_node_active: 4,
            bytes_per_tick: 32 * MIB,
            max_bytes_per_node_per_tick: 32 * MIB,
            max_bytes_per_task_per_tick: 8 * MIB,
        },
    );
}
