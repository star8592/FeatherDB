use std::collections::BTreeMap;

use feather_sim::{
    AdminState, Cluster, FailureDomainPolicy, FaultTrace, MigrationBudget, MigrationScheduler,
    Node, Placement, SimNetwork, Tablet, replay_migration_faults_with_network,
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

fn scheduler() -> MigrationScheduler {
    let cluster = Cluster {
        epoch: 11,
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
        tablets: (0..256).map(|id| Tablet { id, bytes: 512 }).collect(),
    };

    let actual = Placement {
        replicas: (0..256)
            .map(|id| (id, vec![1, 2, 3]))
            .collect::<BTreeMap<_, _>>(),
    };
    let desired = Placement {
        replicas: (0..256)
            .map(|id| (id, vec![2, 3, 4]))
            .collect::<BTreeMap<_, _>>(),
    };

    MigrationScheduler::new(
        cluster,
        FailureDomainPolicy::HIERARCHICAL,
        actual,
        desired,
        MigrationBudget {
            max_active: 8,
            max_per_node_active: 4,
            bytes_per_tick: 1024,
            max_bytes_per_node_per_tick: 1024,
            max_bytes_per_task_per_tick: 256,
        },
    )
    .expect("valid repair scheduler")
}

fn main() {
    let trace = FaultTrace::generate_network_faults_on_links(8592, &[(2, 4), (3, 4)], 80, 4, 5, 6);
    let encoded = trace.to_text();
    let decoded = FaultTrace::from_text(&encoded).expect("trace round-trip");
    assert_eq!(trace, decoded);

    let mut a = scheduler();
    let mut b = scheduler();
    let mut network_a = SimNetwork::default();
    let mut network_b = SimNetwork::default();

    let report_a = replay_migration_faults_with_network(&mut a, &mut network_a, &decoded, 100_000);
    let report_b = replay_migration_faults_with_network(&mut b, &mut network_b, &decoded, 100_000);

    assert_eq!(report_a, report_b);
    assert_eq!(a.actual(), b.actual());
    assert!(report_a.converged);
    assert_eq!(report_a.events_applied, trace.events().len());
    assert_eq!(report_a.remaining_bytes, 0);

    let mut delay = 0;
    let mut drop = 0;
    let mut duplicate = 0;
    let mut reorder = 0;
    let mut partition = 0;
    let mut heal = 0;
    for event in trace.events() {
        match event.action {
            feather_sim::FaultAction::SetLinkDelay { .. } => delay += 1,
            feather_sim::FaultAction::DropNext { .. } => drop += 1,
            feather_sim::FaultAction::DuplicateNext { .. } => duplicate += 1,
            feather_sim::FaultAction::ReorderNext { .. } => reorder += 1,
            feather_sim::FaultAction::Partition { .. } => partition += 1,
            feather_sim::FaultAction::Heal { .. } => heal += 1,
            feather_sim::FaultAction::SetNodeHealth { .. }
            | feather_sim::FaultAction::SetDiskFull { .. }
            | feather_sim::FaultAction::SetDiskDelay { .. }
            | feather_sim::FaultAction::DiskFailNext { .. }
            | feather_sim::FaultAction::CorruptNextDiskRead { .. }
            | feather_sim::FaultAction::CorruptNextDiskWrite { .. }
            | feather_sim::FaultAction::CrashDisk { .. } => {}
        }
    }

    println!(
        "network-fault-lab seed={} events={} last_fault_tick={} trace_bytes={} delay_events={} drop_events={} duplicate_events={} reorder_events={} partition_events={} heal_events={}",
        trace.seed,
        trace.events().len(),
        trace.last_tick(),
        encoded.len(),
        delay,
        drop,
        duplicate,
        reorder,
        partition,
        heal
    );
    println!(
        "replay converged={} ticks={} events_applied={} drops={} duplicates={} bytes_attempted={} bytes_copied={} completed={} remaining_bytes={}",
        report_a.converged,
        report_a.ticks_executed,
        report_a.events_applied,
        report_a.transfer_drops,
        report_a.transfer_duplicates,
        report_a.bytes_attempted,
        report_a.bytes_copied,
        report_a.completed_migrations,
        report_a.remaining_bytes
    );
}
