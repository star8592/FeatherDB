use feather_sim::{
    FaultTrace, MessageBusLimits, MessageClass, ScheduledMessage, replay_message_schedule,
};

fn schedule(until_tick: u64) -> Vec<ScheduledMessage> {
    let mut messages = Vec::new();
    let mut sequence = 0_u64;

    for tick in 0..=until_tick {
        for (from, to, class, marker) in [
            (1, 2, MessageClass::Membership, 1_u8),
            (2, 3, MessageClass::Control, 2_u8),
            (3, 1, MessageClass::Data, 3_u8),
        ] {
            let mut payload = vec![marker; 16];
            payload[1..9].copy_from_slice(&tick.to_le_bytes());
            messages.push(ScheduledMessage {
                tick,
                sequence,
                from,
                to,
                class,
                payload,
            });
            sequence += 1;
        }
    }

    messages
}

fn digest(result: &feather_sim::MessageReplayResult) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for delivery in &result.deliveries {
        for value in [
            delivery.tick,
            delivery.message.message_id,
            delivery.message.from,
            delivery.message.to,
            delivery.message.class as u64,
        ] {
            for byte in value.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
            }
        }
        for byte in &delivery.message.payload {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
        }
    }
    hash
}

fn main() {
    let links = [(1, 2), (2, 3), (3, 1)];
    let trace = FaultTrace::generate_network_faults_on_links(8592, &links, 120, 3, 5, 6);
    let messages = schedule(trace.last_tick() + 32);
    let limits = MessageBusLimits {
        max_buffered_messages: 8_192,
        max_buffered_bytes: 8 * 1024 * 1024,
    };

    let a = replay_message_schedule(&trace, &messages, limits, 100_000);
    let b = replay_message_schedule(&trace, &messages, limits, 100_000);

    assert_eq!(a, b);
    assert!(a.report.drained);
    assert_eq!(a.report.sends_backpressured, 0);

    let mut membership = 0_usize;
    let mut control = 0_usize;
    let mut data = 0_usize;
    for delivery in &a.deliveries {
        match delivery.message.class {
            MessageClass::Membership => membership += 1,
            MessageClass::Control => control += 1,
            MessageClass::Data => data += 1,
            _ => {}
        }
    }

    println!(
        "message-bus-lab seed={} fault_events={} last_fault_tick={} scheduled={} delivery_digest={}",
        trace.seed,
        trace.events().len(),
        trace.last_tick(),
        messages.len(),
        digest(&a)
    );
    println!(
        "replay drained={} ticks={} network_faults_applied={} sends_queued={} sends_dropped={} physical_copies={} delivery_partition_drops={} delivered={} delivered_bytes={} peak_buffered_messages={} peak_buffered_bytes={}",
        a.report.drained,
        a.report.ticks_executed,
        a.report.network_fault_events_applied,
        a.report.sends_queued,
        a.report.sends_dropped,
        a.report.physical_copies_queued,
        a.report.dropped_at_delivery_partition,
        a.report.delivered_messages,
        a.report.delivered_bytes,
        a.report.peak_buffered_messages,
        a.report.peak_buffered_bytes,
    );
    println!(
        "delivered_by_class membership={} control={} data={}",
        membership, control, data
    );
}
