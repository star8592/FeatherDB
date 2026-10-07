use std::collections::BTreeMap;

use feather_sim::{
    ControlRecord, DurableControlWriter, DurableStore, FaultEvent, FaultTrace, MemberStatus,
    MembershipCluster, MembershipConfig, MessageBusLimits, SimDisk, apply_disk_fault_action,
    apply_network_fault_action, read_control_record,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct CampaignResult {
    final_tick: u64,
    final_joined: usize,
    final_digest: u64,
    network_events: usize,
    disk_events: usize,
    peak_buffered_messages: usize,
    durable_rewrites: usize,
    backpressure: u64,
}

fn config() -> MembershipConfig {
    MembershipConfig {
        probe_interval_ticks: 2,
        direct_timeout_ticks: 2,
        indirect_timeout_ticks: 4,
        suspicion_timeout_ticks: 12,
        indirect_checks: 3,
        max_awareness_score: 8,
        piggyback_updates: 8,
        update_retransmits: 16,
    }
}

fn merged_trace() -> FaultTrace {
    let ids: Vec<_> = (1..=50).collect();
    let network = FaultTrace::generate_network_faults(8592, &ids, 220, 2, 7, 6);
    let disk = FaultTrace::generate_disk_faults(8593, &ids, 180, 2, 8, 6);

    let mut events = Vec::with_capacity(network.events().len() + disk.events().len());
    let mut sequence = 0_u64;
    for event in network.events().iter().chain(disk.events()) {
        events.push(FaultEvent {
            tick: event.tick,
            sequence,
            action: event.action,
        });
        sequence = sequence.saturating_add(1);
    }
    FaultTrace::new(0x8592_8593, events)
}

fn run_campaign(trace: &FaultTrace) -> CampaignResult {
    let mut cluster = MembershipCluster::new(
        &(1..=20).collect::<Vec<_>>(),
        config(),
        MessageBusLimits {
            max_buffered_messages: 500_000,
            max_buffered_bytes: 128 * 1024 * 1024,
        },
    )
    .expect("membership cluster");

    for node_id in 21..=50 {
        let seed = 1 + node_id % 20;
        assert!(cluster.add_joining_node(node_id, seed));
    }

    let record = ControlRecord {
        topology_epoch: 7,
        catalog_generation: 3,
    };
    let mut disks = (1..=50)
        .map(|node_id| (node_id, SimDisk::default()))
        .collect::<BTreeMap<_, _>>();
    let mut writers = BTreeMap::<u64, DurableControlWriter>::new();

    let mut next_event = 0_usize;
    let mut peak_buffered_messages = 0_usize;
    let mut network_events = 0_usize;
    let mut disk_events = 0_usize;
    let fault_end = trace.last_tick();

    let max_tick = fault_end.saturating_add(1_500);
    while cluster.now() < max_tick {
        let now = cluster.now();

        while let Some(event) = trace.events().get(next_event) {
            if event.tick != now {
                break;
            }
            if apply_network_fault_action(cluster.network_mut(), event.action) {
                network_events += 1;
            }
            if apply_disk_fault_action(&mut disks, event.action) {
                disk_events += 1;
            }
            next_event += 1;
        }

        if now == 60 {
            for node_id in 1..=50 {
                writers.insert(node_id, DurableControlWriter::new(record));
            }
        }

        if now == 80 {
            assert!(cluster.crash(10));
        }
        if now == 180 {
            assert!(cluster.restart(10));
        }
        if now == 220 {
            for node_id in 45..=50 {
                assert!(cluster.graceful_leave(node_id));
            }
        }
        if now == 300 {
            for node_id in 45..=47 {
                assert!(cluster.rejoin(node_id, 1 + node_id % 20));
            }
        }

        for (node_id, writer) in &mut writers {
            if writer.is_committed() {
                continue;
            }
            if matches!(writer.state(), feather_sim::ControlCommitState::Failed(_)) {
                writer.retry();
            }
            writer.tick(now, disks.get_mut(node_id).expect("node disk"));
        }

        cluster.tick();
        peak_buffered_messages = peak_buffered_messages.max(cluster.buffered_message_count());

        if now > fault_end.saturating_add(800) {
            let alive = (1..=47)
                .all(|subject| cluster.all_live_observers_see(subject, MemberStatus::Alive));
            let left = (48..=50)
                .all(|subject| cluster.all_live_observers_see(subject, MemberStatus::Left));
            if alive && left && cluster.joined_node_count() == 47 {
                break;
            }
        }
    }

    assert_eq!(next_event, trace.events().len());
    assert_eq!(cluster.joined_node_count(), 47);
    for subject in 1..=47 {
        assert!(cluster.all_live_observers_see(subject, MemberStatus::Alive));
    }
    for subject in 48..=50 {
        assert!(cluster.all_live_observers_see(subject, MemberStatus::Left));
    }

    // Fault injection stops. Clear residual bounded one-shot faults/delays and
    // verify every node's durable control record. Silent corruption or a
    // failed unsynced write is repaired by rewriting the same idempotent record.
    for disk in disks.values_mut() {
        disk.set_full(false);
        disk.set_delay(0);
        disk.fail_next(0);
        disk.corrupt_next_read(0);
        disk.corrupt_next_write(0);
    }

    let mut durable_rewrites = 0_usize;
    for node_id in 1..=50 {
        let disk = disks.get_mut(&node_id).unwrap();
        if read_control_record(cluster.now(), 1_000_000 + node_id, disk) != Ok(Some(record)) {
            durable_rewrites += 1;
            let mut writer = DurableControlWriter::new(record);
            writer.tick(cluster.now(), disk);
            assert!(writer.is_committed());
            disk.crash();
            assert_eq!(
                read_control_record(cluster.now(), 2_000_000 + node_id, disk),
                Ok(Some(record))
            );
        }
    }

    let mut digest = 0xcbf2_9ce4_8422_2325_u64;
    let mut backpressure = 0_u64;
    for observer in 1..=50 {
        backpressure += cluster
            .node(observer)
            .unwrap()
            .stats()
            .backpressured_messages;
        for subject in 1..=50 {
            if let Some(state) = cluster.view(observer, subject) {
                for value in [observer, subject, state.incarnation, state.status as u64] {
                    for byte in value.to_le_bytes() {
                        digest ^= u64::from(byte);
                        digest = digest.wrapping_mul(0x0000_0100_0000_01B3);
                    }
                }
            }
        }
        let disk = disks.get_mut(&observer).unwrap();
        let durable = read_control_record(cluster.now(), 3_000_000 + observer, disk)
            .expect("checked durable record")
            .expect("durable record");
        for value in [durable.topology_epoch, durable.catalog_generation] {
            for byte in value.to_le_bytes() {
                digest ^= u64::from(byte);
                digest = digest.wrapping_mul(0x0000_0100_0000_01B3);
            }
        }
    }

    CampaignResult {
        final_tick: cluster.now(),
        final_joined: cluster.joined_node_count(),
        final_digest: digest,
        network_events,
        disk_events,
        peak_buffered_messages,
        durable_rewrites,
        backpressure,
    }
}

fn main() {
    let trace = merged_trace();
    let encoded = trace.to_text();
    assert_eq!(FaultTrace::from_text(&encoded).unwrap(), trace);

    let a = run_campaign(&trace);
    let b = run_campaign(&trace);
    assert_eq!(a, b);

    println!(
        "cross-fault-lab seed={} trace_events={} trace_bytes={} last_fault_tick={} final_tick={} final_joined={} digest={}",
        trace.seed,
        trace.events().len(),
        encoded.len(),
        trace.last_tick(),
        a.final_tick,
        a.final_joined,
        a.final_digest
    );
    println!(
        "cross-fault-lab network_events={} disk_events={} peak_buffered_messages={} durable_rewrites={} backpressure={}",
        a.network_events,
        a.disk_events,
        a.peak_buffered_messages,
        a.durable_rewrites,
        a.backpressure
    );
}
