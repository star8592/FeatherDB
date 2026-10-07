use crate::fault::{FaultTrace, apply_network_fault_action};
use crate::model::NodeId;
use crate::transport::{
    MessageBusLimits, MessageClass, MessageSend, SimClock, SimMessage, SimNetwork,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledMessage {
    pub tick: u64,
    pub sequence: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub class: MessageClass,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveredMessage {
    pub tick: u64,
    pub message: SimMessage,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MessageReplayReport {
    pub ticks_executed: u64,
    pub network_fault_events_applied: usize,
    pub sends_queued: usize,
    pub physical_copies_queued: usize,
    pub sends_dropped: usize,
    pub sends_backpressured: usize,
    pub dropped_at_delivery_partition: usize,
    pub delivered_messages: usize,
    pub delivered_bytes: usize,
    pub peak_buffered_messages: usize,
    pub peak_buffered_bytes: usize,
    pub drained: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageReplayResult {
    pub report: MessageReplayReport,
    pub deliveries: Vec<DeliveredMessage>,
}

pub fn replay_message_schedule(
    trace: &FaultTrace,
    schedule: &[ScheduledMessage],
    limits: MessageBusLimits,
    max_ticks: u64,
) -> MessageReplayResult {
    let mut ordered_schedule = schedule.to_vec();
    ordered_schedule.sort_by_key(|message| (message.tick, message.sequence));

    let mut recipients: Vec<_> = ordered_schedule.iter().map(|message| message.to).collect();
    recipients.sort_unstable();
    recipients.dedup();

    let mut network = SimNetwork::with_message_limits(limits);
    let mut clock = SimClock::new();
    let mut next_fault = 0_usize;
    let mut next_message = 0_usize;
    let mut report = MessageReplayReport::default();
    let mut deliveries = Vec::new();

    for _ in 0..max_ticks {
        let now = clock.now();

        while let Some(event) = trace.events().get(next_fault) {
            if event.tick != now {
                break;
            }
            if apply_network_fault_action(&mut network, event.action) {
                report.network_fault_events_applied += 1;
            }
            next_fault += 1;
        }

        while let Some(message) = ordered_schedule.get(next_message) {
            if message.tick != now {
                break;
            }

            match network.send_message(
                now,
                message.from,
                message.to,
                message.class,
                message.payload.clone(),
            ) {
                MessageSend::Queued { copies, .. } => {
                    report.sends_queued += 1;
                    report.physical_copies_queued += copies as usize;
                }
                MessageSend::Dropped { .. } => {
                    report.sends_dropped += 1;
                }
                MessageSend::Backpressure { .. } => {
                    report.sends_backpressured += 1;
                }
            }

            next_message += 1;
        }

        report.peak_buffered_messages = report
            .peak_buffered_messages
            .max(network.buffered_message_count());
        report.peak_buffered_bytes = report
            .peak_buffered_bytes
            .max(network.buffered_message_bytes());

        let advance = network.advance_messages(now);
        report.dropped_at_delivery_partition += advance.dropped_by_partition;
        report.peak_buffered_messages = report
            .peak_buffered_messages
            .max(network.buffered_message_count());
        report.peak_buffered_bytes = report
            .peak_buffered_bytes
            .max(network.buffered_message_bytes());

        for recipient in &recipients {
            for message in network.drain_messages(*recipient) {
                report.delivered_messages += 1;
                report.delivered_bytes =
                    report.delivered_bytes.saturating_add(message.payload.len());
                deliveries.push(DeliveredMessage { tick: now, message });
            }
        }

        report.ticks_executed = now + 1;

        if next_fault == trace.events().len()
            && next_message == ordered_schedule.len()
            && network.buffered_message_count() == 0
        {
            report.drained = true;
            break;
        }

        clock.advance();
    }

    MessageReplayResult { report, deliveries }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::FaultTrace;

    fn mixed_schedule(ticks: u64) -> Vec<ScheduledMessage> {
        let mut schedule = Vec::new();
        let mut sequence = 0_u64;
        for tick in 0..ticks {
            for (from, to, class, byte) in [
                (1, 2, MessageClass::Membership, 1),
                (2, 3, MessageClass::Control, 2),
                (3, 1, MessageClass::Data, 3),
            ] {
                schedule.push(ScheduledMessage {
                    tick,
                    sequence,
                    from,
                    to,
                    class,
                    payload: vec![byte, tick as u8],
                });
                sequence += 1;
            }
        }
        schedule
    }

    #[test]
    fn same_fault_trace_and_schedule_replay_identically() {
        let links = [(1, 2), (2, 3), (3, 1)];
        let trace = FaultTrace::generate_network_faults_on_links(8592, &links, 80, 3, 4, 5);
        let schedule = mixed_schedule(trace.last_tick() + 16);
        let limits = MessageBusLimits {
            max_buffered_messages: 4_096,
            max_buffered_bytes: 4 * 1024 * 1024,
        };

        let a = replay_message_schedule(&trace, &schedule, limits, 10_000);
        let b = replay_message_schedule(&trace, &schedule, limits, 10_000);

        assert_eq!(a, b);
        assert!(a.report.drained);
        assert_eq!(a.report.network_fault_events_applied, trace.events().len());
        assert_eq!(
            a.report.sends_queued + a.report.sends_dropped + a.report.sends_backpressured,
            schedule.len()
        );
        assert!(a.report.delivered_messages > 0);
        assert!(a.report.sends_dropped > 0 || a.report.dropped_at_delivery_partition > 0);
    }

    #[test]
    fn delivery_trace_preserves_message_class_and_payload() {
        let trace = FaultTrace::new(1, Vec::new());
        let schedule = vec![
            ScheduledMessage {
                tick: 0,
                sequence: 2,
                from: 1,
                to: 2,
                class: MessageClass::Data,
                payload: vec![3],
            },
            ScheduledMessage {
                tick: 0,
                sequence: 0,
                from: 1,
                to: 2,
                class: MessageClass::Membership,
                payload: vec![1],
            },
            ScheduledMessage {
                tick: 0,
                sequence: 1,
                from: 1,
                to: 2,
                class: MessageClass::Control,
                payload: vec![2],
            },
        ];

        let result = replay_message_schedule(&trace, &schedule, MessageBusLimits::default(), 10);

        assert!(result.report.drained);
        assert_eq!(
            result
                .deliveries
                .iter()
                .map(|delivery| delivery.message.class)
                .collect::<Vec<_>>(),
            vec![
                MessageClass::Membership,
                MessageClass::Control,
                MessageClass::Data,
            ]
        );
        assert_eq!(
            result
                .deliveries
                .iter()
                .map(|delivery| delivery.message.payload[0])
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn bounded_bus_reports_backpressure_deterministically() {
        let trace = FaultTrace::new(1, Vec::new());
        let schedule = (0..10)
            .map(|sequence| ScheduledMessage {
                tick: 0,
                sequence,
                from: 1,
                to: 2,
                class: MessageClass::Gossip,
                payload: vec![0; 16],
            })
            .collect::<Vec<_>>();

        let result = replay_message_schedule(
            &trace,
            &schedule,
            MessageBusLimits {
                max_buffered_messages: 4,
                max_buffered_bytes: 64,
            },
            10,
        );

        assert_eq!(result.report.sends_queued, 4);
        assert_eq!(result.report.sends_backpressured, 6);
        assert_eq!(result.report.delivered_messages, 4);
        assert!(result.report.drained);
    }
}
