use std::collections::BTreeMap;

use feather_sim::{
    AdminState, Cluster, FailureDomainPolicy, FaultTrace, MigrationBudget, MigrationScheduler,
    Node, Placement, Tablet, replay_migration_faults,
};

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
        tablets: (0..128).map(|id| Tablet { id, bytes: 256 }).collect(),
    };

    let actual = Placement {
        replicas: (0..128)
            .map(|id| (id, vec![1, 2, 3]))
            .collect::<BTreeMap<_, _>>(),
    };
    let desired = Placement {
        replicas: (0..128)
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
            bytes_per_tick: 128,
            max_bytes_per_node_per_tick: 128,
            max_bytes_per_task_per_tick: 64,
        },
    )
    .expect("valid repair schedule");

    let trace = FaultTrace::generate_health_flaps(8592, &[2, 3], 40, 4, 4);
    let encoded = trace.to_text();
    let decoded = FaultTrace::from_text(&encoded).expect("trace round-trip");
    assert_eq!(decoded, trace);

    println!(
        "fault-replay-lab seed={} events={} last_fault_tick={} trace_bytes={}",
        trace.seed,
        trace.events().len(),
        trace.last_tick(),
        encoded.len()
    );

    let report = replay_migration_faults(&mut scheduler, &decoded, 100_000);

    println!(
        "replay converged={} ticks={} events_applied={} source_failovers={} completed={} bytes_copied={} remaining_bytes={}",
        report.converged,
        report.ticks_executed,
        report.events_applied,
        report.source_failovers,
        report.completed_migrations,
        report.bytes_copied,
        report.remaining_bytes
    );

    assert!(report.converged);
    assert_eq!(report.events_applied, trace.events().len());
    assert_eq!(report.remaining_bytes, 0);
}
