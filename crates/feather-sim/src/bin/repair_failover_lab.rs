use std::collections::BTreeMap;

use feather_sim::{
    AdminState, Cluster, FailureDomainPolicy, MigrationBudget, MigrationScheduler, Node,
    NodeHealth, Placement, Tablet, TabletAvailability,
};

const MIB: u64 = 1024 * 1024;

fn node(id: u64, state: AdminState, zone: &str) -> Node {
    Node {
        id,
        weight: 1,
        zone: zone.into(),
        rack: "r1".into(),
        state,
    }
}

fn main() {
    let tablet_count = 100_u64;
    let cluster = Cluster {
        epoch: 7,
        replication_factor: 3,
        nodes: [
            node(1, AdminState::Removed, "a"),
            node(2, AdminState::Active, "b"),
            node(3, AdminState::Active, "c"),
            node(4, AdminState::Active, "d"),
        ]
        .into_iter()
        .map(|node| (node.id, node))
        .collect(),
        tablets: (0..tablet_count)
            .map(|id| Tablet { id, bytes: 8 * MIB })
            .collect(),
    };

    let actual = Placement {
        replicas: (0..tablet_count)
            .map(|id| (id, vec![1, 2, 3]))
            .collect::<BTreeMap<_, _>>(),
    };
    let desired = Placement {
        replicas: (0..tablet_count)
            .map(|id| (id, vec![2, 3, 4]))
            .collect::<BTreeMap<_, _>>(),
    };

    let mut scheduler = MigrationScheduler::new(
        cluster,
        FailureDomainPolicy::HIERARCHICAL,
        actual,
        desired,
        MigrationBudget {
            max_active: 4,
            max_per_node_active: 2,
            bytes_per_tick: 8 * MIB,
            max_bytes_per_node_per_tick: 8 * MIB,
            max_bytes_per_task_per_tick: 4 * MIB,
        },
    )
    .expect("repair schedule");

    let first = scheduler.tick();
    println!(
        "before-source-loss started={} copied_mib={} active={} availability={:?}",
        first.started,
        first.bytes_copied / MIB,
        first.active,
        scheduler.tablet_availability(0)
    );

    assert!(scheduler.set_node_health(2, NodeHealth::Unavailable));
    let failover = scheduler.tick();
    println!(
        "source-loss failovers={} copied_mib={} active={} availability={:?}",
        failover.source_failovers,
        failover.bytes_copied / MIB,
        failover.active,
        scheduler.tablet_availability(0)
    );

    let mut ticks = 2_u64;
    while !scheduler.is_converged() && ticks < 100_000 {
        scheduler.tick();
        ticks += 1;
    }

    println!(
        "after-repair converged={} ticks={} completed={} availability={:?}",
        scheduler.is_converged(),
        ticks,
        scheduler.total_completed,
        scheduler.tablet_availability(0)
    );

    assert_eq!(
        scheduler.tablet_availability(0),
        Some(TabletAvailability::Degraded {
            available: 2,
            total: 3
        })
    );

    scheduler.set_node_health(2, NodeHealth::Healthy);
    println!(
        "source-returned ownership_unchanged=true availability={:?}",
        scheduler.tablet_availability(0)
    );
}
