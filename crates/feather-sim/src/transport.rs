use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::model::NodeId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferRequest {
    pub task_id: u64,
    pub chunk_offset: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferSubmit {
    Delivered { bytes: u64, duplicates: u32 },
    InFlight,
    Dropped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferPoll {
    Pending,
    Delivered { bytes: u64, duplicates: u32 },
    Dropped,
}

pub trait MigrationTransport {
    fn submit(&mut self, now_tick: u64, request: TransferRequest) -> TransferSubmit;
    fn poll(&mut self, now_tick: u64, task_id: u64) -> TransferPoll;
    fn cancel(&mut self, task_id: u64);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DirectMigrationTransport;

impl MigrationTransport for DirectMigrationTransport {
    fn submit(&mut self, _now_tick: u64, request: TransferRequest) -> TransferSubmit {
        TransferSubmit::Delivered {
            bytes: request.bytes,
            duplicates: 0,
        }
    }

    fn poll(&mut self, _now_tick: u64, _task_id: u64) -> TransferPoll {
        TransferPoll::Pending
    }

    fn cancel(&mut self, _task_id: u64) {}
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SimClock {
    tick: u64,
}

impl SimClock {
    pub const fn new() -> Self {
        Self { tick: 0 }
    }

    pub const fn now(&self) -> u64 {
        self.tick
    }

    pub fn advance(&mut self) -> u64 {
        self.tick = self.tick.saturating_add(1);
        self.tick
    }

    pub fn advance_by(&mut self, ticks: u64) -> u64 {
        self.tick = self.tick.saturating_add(ticks);
        self.tick
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum MessageClass {
    Membership,
    Gossip,
    Control,
    Data,
    Repair,
    Client,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimMessage {
    pub message_id: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub class: MessageClass,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageBusLimits {
    pub max_buffered_messages: usize,
    pub max_buffered_bytes: usize,
}

impl Default for MessageBusLimits {
    fn default() -> Self {
        Self {
            max_buffered_messages: 65_536,
            max_buffered_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageSend {
    Queued {
        message_id: u64,
        copies: u32,
        deliver_at: u64,
    },
    Dropped {
        message_id: u64,
    },
    Backpressure {
        message_id: u64,
        required_messages: usize,
        required_bytes: usize,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MessageAdvanceReport {
    pub ready: usize,
    pub dropped_by_partition: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct LinkBehavior {
    delay_ticks: u64,
    drop_next: u64,
    duplicate_next: u64,
    reorder_next: u64,
    reorder_extra_delay_ticks: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LinkDisposition {
    Dropped,
    Deliver { deliver_at: u64, duplicates: u32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InFlightPacket {
    request: TransferRequest,
    deliver_at: u64,
    duplicates: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MessagePacket {
    message: SimMessage,
}

#[derive(Clone, Debug)]
pub struct SimNetwork {
    links: BTreeMap<(NodeId, NodeId), LinkBehavior>,
    partitions: BTreeSet<(NodeId, NodeId)>,
    in_flight: BTreeMap<u64, InFlightPacket>,
    message_in_flight: BTreeMap<(u64, u64), MessagePacket>,
    message_ready: BTreeMap<NodeId, VecDeque<SimMessage>>,
    next_message_id: u64,
    next_message_sequence: u64,
    message_limits: MessageBusLimits,
    buffered_message_count: usize,
    buffered_message_bytes: usize,
}

impl Default for SimNetwork {
    fn default() -> Self {
        Self::with_message_limits(MessageBusLimits::default())
    }
}

impl SimNetwork {
    pub fn with_message_limits(message_limits: MessageBusLimits) -> Self {
        Self {
            links: BTreeMap::new(),
            partitions: BTreeSet::new(),
            in_flight: BTreeMap::new(),
            message_in_flight: BTreeMap::new(),
            message_ready: BTreeMap::new(),
            next_message_id: 1,
            next_message_sequence: 1,
            message_limits,
            buffered_message_count: 0,
            buffered_message_bytes: 0,
        }
    }

    pub fn set_delay(&mut self, from: NodeId, to: NodeId, ticks: u64) {
        self.links.entry((from, to)).or_default().delay_ticks = ticks;
    }

    pub fn drop_next(&mut self, from: NodeId, to: NodeId, count: u64) {
        self.links.entry((from, to)).or_default().drop_next = count;
    }

    pub fn duplicate_next(&mut self, from: NodeId, to: NodeId, count: u64) {
        self.links.entry((from, to)).or_default().duplicate_next = count;
    }

    pub fn reorder_next(&mut self, from: NodeId, to: NodeId, count: u64, extra_delay_ticks: u64) {
        let behavior = self.links.entry((from, to)).or_default();
        behavior.reorder_next = count;
        behavior.reorder_extra_delay_ticks = extra_delay_ticks;
    }

    pub fn partition(&mut self, a: NodeId, b: NodeId, bidirectional: bool) {
        self.partitions.insert((a, b));
        if bidirectional {
            self.partitions.insert((b, a));
        }
    }

    pub fn heal(&mut self, a: NodeId, b: NodeId, bidirectional: bool) {
        self.partitions.remove(&(a, b));
        if bidirectional {
            self.partitions.remove(&(b, a));
        }
    }

    pub fn clear_delay(&mut self, from: NodeId, to: NodeId) {
        self.set_delay(from, to, 0);
    }

    pub fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        self.partitions.contains(&(from, to))
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    pub fn message_in_flight_count(&self) -> usize {
        self.message_in_flight.len()
    }

    pub fn ready_message_count(&self) -> usize {
        self.message_ready.values().map(VecDeque::len).sum()
    }

    pub fn buffered_message_count(&self) -> usize {
        self.buffered_message_count
    }

    pub fn buffered_message_bytes(&self) -> usize {
        self.buffered_message_bytes
    }

    pub fn message_limits(&self) -> MessageBusLimits {
        self.message_limits
    }

    pub fn send_message(
        &mut self,
        now_tick: u64,
        from: NodeId,
        to: NodeId,
        class: MessageClass,
        payload: Vec<u8>,
    ) -> MessageSend {
        let message_id = self.next_message_id;
        self.next_message_id = self.next_message_id.saturating_add(1);

        let disposition = self.link_disposition(now_tick, from, to);
        let LinkDisposition::Deliver {
            deliver_at,
            duplicates,
        } = disposition
        else {
            return MessageSend::Dropped { message_id };
        };

        let copies = 1_usize.saturating_add(duplicates as usize);
        let required_bytes = payload.len().saturating_mul(copies);
        let required_messages = copies;

        if self
            .buffered_message_count
            .saturating_add(required_messages)
            > self.message_limits.max_buffered_messages
            || self.buffered_message_bytes.saturating_add(required_bytes)
                > self.message_limits.max_buffered_bytes
        {
            return MessageSend::Backpressure {
                message_id,
                required_messages,
                required_bytes,
            };
        }

        for _ in 0..copies {
            let sequence = self.next_message_sequence;
            self.next_message_sequence = self.next_message_sequence.saturating_add(1);
            self.message_in_flight.insert(
                (deliver_at, sequence),
                MessagePacket {
                    message: SimMessage {
                        message_id,
                        from,
                        to,
                        class,
                        payload: payload.clone(),
                    },
                },
            );
        }
        self.buffered_message_count = self.buffered_message_count.saturating_add(copies);
        self.buffered_message_bytes = self.buffered_message_bytes.saturating_add(required_bytes);

        MessageSend::Queued {
            message_id,
            copies: copies as u32,
            deliver_at,
        }
    }

    pub fn advance_messages(&mut self, now_tick: u64) -> MessageAdvanceReport {
        let keys: Vec<_> = self
            .message_in_flight
            .range(..=(now_tick, u64::MAX))
            .map(|(key, _)| *key)
            .collect();

        let mut report = MessageAdvanceReport::default();
        for key in keys {
            let Some(packet) = self.message_in_flight.remove(&key) else {
                continue;
            };

            if self
                .partitions
                .contains(&(packet.message.from, packet.message.to))
            {
                self.buffered_message_count = self.buffered_message_count.saturating_sub(1);
                self.buffered_message_bytes = self
                    .buffered_message_bytes
                    .saturating_sub(packet.message.payload.len());
                report.dropped_by_partition += 1;
                continue;
            }

            self.message_ready
                .entry(packet.message.to)
                .or_default()
                .push_back(packet.message);
            report.ready += 1;
        }

        report
    }

    pub fn recv_message(&mut self, node_id: NodeId) -> Option<SimMessage> {
        let queue = self.message_ready.get_mut(&node_id)?;
        let message = queue.pop_front()?;
        self.buffered_message_count = self.buffered_message_count.saturating_sub(1);
        self.buffered_message_bytes = self
            .buffered_message_bytes
            .saturating_sub(message.payload.len());
        if queue.is_empty() {
            self.message_ready.remove(&node_id);
        }
        Some(message)
    }

    pub fn drain_messages(&mut self, node_id: NodeId) -> Vec<SimMessage> {
        let Some(queue) = self.message_ready.remove(&node_id) else {
            return Vec::new();
        };
        let messages: Vec<_> = queue.into_iter().collect();
        self.buffered_message_count = self.buffered_message_count.saturating_sub(messages.len());
        let bytes = messages.iter().fold(0_usize, |sum, message| {
            sum.saturating_add(message.payload.len())
        });
        self.buffered_message_bytes = self.buffered_message_bytes.saturating_sub(bytes);
        messages
    }

    fn link_disposition(&mut self, now_tick: u64, from: NodeId, to: NodeId) -> LinkDisposition {
        if self.partitions.contains(&(from, to)) {
            return LinkDisposition::Dropped;
        }

        let behavior = self.links.entry((from, to)).or_default();
        if behavior.drop_next > 0 {
            behavior.drop_next -= 1;
            return LinkDisposition::Dropped;
        }

        let duplicates = if behavior.duplicate_next > 0 {
            behavior.duplicate_next -= 1;
            1
        } else {
            0
        };

        let reorder_delay = if behavior.reorder_next > 0 {
            behavior.reorder_next -= 1;
            behavior.reorder_extra_delay_ticks
        } else {
            0
        };

        LinkDisposition::Deliver {
            deliver_at: now_tick
                .saturating_add(behavior.delay_ticks)
                .saturating_add(reorder_delay),
            duplicates,
        }
    }
}

impl MigrationTransport for SimNetwork {
    fn submit(&mut self, now_tick: u64, request: TransferRequest) -> TransferSubmit {
        match self.link_disposition(now_tick, request.from, request.to) {
            LinkDisposition::Dropped => TransferSubmit::Dropped,
            LinkDisposition::Deliver {
                deliver_at,
                duplicates,
            } if deliver_at == now_tick => TransferSubmit::Delivered {
                bytes: request.bytes,
                duplicates,
            },
            LinkDisposition::Deliver {
                deliver_at,
                duplicates,
            } => {
                self.in_flight.insert(
                    request.task_id,
                    InFlightPacket {
                        request,
                        deliver_at,
                        duplicates,
                    },
                );
                TransferSubmit::InFlight
            }
        }
    }

    fn poll(&mut self, now_tick: u64, task_id: u64) -> TransferPoll {
        let Some(packet) = self.in_flight.get(&task_id).copied() else {
            return TransferPoll::Pending;
        };

        if self
            .partitions
            .contains(&(packet.request.from, packet.request.to))
        {
            self.in_flight.remove(&task_id);
            return TransferPoll::Dropped;
        }

        if now_tick < packet.deliver_at {
            return TransferPoll::Pending;
        }

        self.in_flight.remove(&task_id);
        TransferPoll::Delivered {
            bytes: packet.request.bytes,
            duplicates: packet.duplicates,
        }
    }

    fn cancel(&mut self, task_id: u64) {
        self.in_flight.remove(&task_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(task_id: u64, from: u64, to: u64, bytes: u64) -> TransferRequest {
        TransferRequest {
            task_id,
            chunk_offset: 0,
            from,
            to,
            bytes,
        }
    }

    #[test]
    fn direct_transport_delivers_immediately() {
        let mut transport = DirectMigrationTransport;
        assert_eq!(
            transport.submit(0, request(1, 1, 2, 100)),
            TransferSubmit::Delivered {
                bytes: 100,
                duplicates: 0
            }
        );
    }

    #[test]
    fn simulated_delay_uses_virtual_ticks() {
        let mut network = SimNetwork::default();
        network.set_delay(1, 2, 3);

        assert_eq!(
            network.submit(10, request(7, 1, 2, 64)),
            TransferSubmit::InFlight
        );
        assert_eq!(network.poll(12, 7), TransferPoll::Pending);
        assert_eq!(
            network.poll(13, 7),
            TransferPoll::Delivered {
                bytes: 64,
                duplicates: 0
            }
        );
    }

    #[test]
    fn partition_is_directional_when_requested() {
        let mut network = SimNetwork::default();
        network.partition(1, 2, false);

        assert_eq!(
            network.submit(0, request(1, 1, 2, 10)),
            TransferSubmit::Dropped
        );
        assert_eq!(
            network.submit(0, request(2, 2, 1, 10)),
            TransferSubmit::Delivered {
                bytes: 10,
                duplicates: 0
            }
        );
    }

    #[test]
    fn heal_restores_link() {
        let mut network = SimNetwork::default();
        network.partition(1, 2, true);
        network.heal(1, 2, true);

        assert_eq!(
            network.submit(0, request(1, 1, 2, 10)),
            TransferSubmit::Delivered {
                bytes: 10,
                duplicates: 0
            }
        );
    }

    #[test]
    fn drop_next_is_bounded() {
        let mut network = SimNetwork::default();
        network.drop_next(1, 2, 1);

        assert_eq!(
            network.submit(0, request(1, 1, 2, 10)),
            TransferSubmit::Dropped
        );
        assert_eq!(
            network.submit(1, request(1, 1, 2, 10)),
            TransferSubmit::Delivered {
                bytes: 10,
                duplicates: 0
            }
        );
    }

    #[test]
    fn duplicate_is_reported_but_payload_is_delivered_once() {
        let mut network = SimNetwork::default();
        network.duplicate_next(1, 2, 1);

        assert_eq!(
            network.submit(0, request(1, 1, 2, 10)),
            TransferSubmit::Delivered {
                bytes: 10,
                duplicates: 1
            }
        );
    }

    #[test]
    fn explicit_reorder_delays_earlier_packet_so_later_packet_overtakes() {
        let mut network = SimNetwork::default();
        network.reorder_next(1, 2, 1, 5);

        assert_eq!(
            network.submit(0, request(1, 1, 2, 10)),
            TransferSubmit::InFlight
        );
        assert_eq!(
            network.submit(1, request(2, 1, 2, 10)),
            TransferSubmit::Delivered {
                bytes: 10,
                duplicates: 0
            }
        );
        assert_eq!(network.poll(1, 1), TransferPoll::Pending);
        assert!(matches!(network.poll(5, 1), TransferPoll::Delivered { .. }));
    }

    #[test]
    fn different_delays_reorder_cross_task_delivery() {
        let mut network = SimNetwork::default();
        network.set_delay(1, 2, 5);
        assert_eq!(
            network.submit(0, request(1, 1, 2, 10)),
            TransferSubmit::InFlight
        );

        network.set_delay(3, 4, 1);
        assert_eq!(
            network.submit(0, request(2, 3, 4, 10)),
            TransferSubmit::InFlight
        );

        assert!(matches!(network.poll(1, 2), TransferPoll::Delivered { .. }));
        assert_eq!(network.poll(1, 1), TransferPoll::Pending);
        assert!(matches!(network.poll(5, 1), TransferPoll::Delivered { .. }));
    }

    #[test]
    fn clock_is_deterministic_and_saturating() {
        let mut clock = SimClock::new();
        assert_eq!(clock.now(), 0);
        assert_eq!(clock.advance(), 1);
        assert_eq!(clock.advance_by(9), 10);
    }

    #[test]
    fn generic_message_delivery_requires_explicit_advance() {
        let mut network = SimNetwork::default();
        let send = network.send_message(10, 1, 2, MessageClass::Membership, b"ping".to_vec());
        assert!(matches!(
            send,
            MessageSend::Queued {
                copies: 1,
                deliver_at: 10,
                ..
            }
        ));
        assert_eq!(network.ready_message_count(), 0);

        let report = network.advance_messages(10);
        assert_eq!(report.ready, 1);
        let message = network.recv_message(2).unwrap();
        assert_eq!(message.from, 1);
        assert_eq!(message.to, 2);
        assert_eq!(message.class, MessageClass::Membership);
        assert_eq!(message.payload, b"ping");
        assert_eq!(network.buffered_message_count(), 0);
    }

    #[test]
    fn generic_message_duplicate_keeps_same_logical_message_id() {
        let mut network = SimNetwork::default();
        network.duplicate_next(1, 2, 1);
        let send = network.send_message(0, 1, 2, MessageClass::Gossip, vec![7]);
        let MessageSend::Queued {
            message_id, copies, ..
        } = send
        else {
            panic!("message should be queued");
        };
        assert_eq!(copies, 2);

        network.advance_messages(0);
        let messages = network.drain_messages(2);
        assert_eq!(messages.len(), 2);
        assert!(
            messages
                .iter()
                .all(|message| message.message_id == message_id)
        );
        assert_eq!(network.buffered_message_count(), 0);
    }

    #[test]
    fn generic_message_reorder_is_deterministic() {
        let mut network = SimNetwork::default();
        network.reorder_next(1, 2, 1, 5);

        let first = network.send_message(0, 1, 2, MessageClass::Control, vec![1]);
        let second = network.send_message(1, 1, 2, MessageClass::Control, vec![2]);

        assert!(matches!(first, MessageSend::Queued { deliver_at: 5, .. }));
        assert!(matches!(second, MessageSend::Queued { deliver_at: 1, .. }));

        network.advance_messages(1);
        assert_eq!(network.recv_message(2).unwrap().payload, vec![2]);
        assert!(network.recv_message(2).is_none());

        network.advance_messages(5);
        assert_eq!(network.recv_message(2).unwrap().payload, vec![1]);
    }

    #[test]
    fn partition_after_send_drops_message_at_delivery_boundary() {
        let mut network = SimNetwork::default();
        network.set_delay(1, 2, 3);
        let send = network.send_message(0, 1, 2, MessageClass::Data, vec![1, 2, 3]);
        assert!(matches!(send, MessageSend::Queued { .. }));
        network.partition(1, 2, false);

        let report = network.advance_messages(3);
        assert_eq!(report.dropped_by_partition, 1);
        assert_eq!(network.buffered_message_count(), 0);
        assert!(network.recv_message(2).is_none());
    }

    #[test]
    fn message_bus_backpressure_is_explicit_and_all_or_nothing() {
        let mut network = SimNetwork::with_message_limits(MessageBusLimits {
            max_buffered_messages: 1,
            max_buffered_bytes: 4,
        });
        network.duplicate_next(1, 2, 1);

        let send = network.send_message(0, 1, 2, MessageClass::Client, vec![1, 2, 3]);
        assert!(matches!(
            send,
            MessageSend::Backpressure {
                required_messages: 2,
                required_bytes: 6,
                ..
            }
        ));
        assert_eq!(network.message_in_flight_count(), 0);
        assert_eq!(network.buffered_message_count(), 0);
    }

    #[test]
    fn migration_and_generic_messages_share_link_fault_budget() {
        let mut network = SimNetwork::default();
        network.drop_next(1, 2, 1);

        assert!(matches!(
            network.send_message(0, 1, 2, MessageClass::Gossip, vec![1]),
            MessageSend::Dropped { .. }
        ));

        assert_eq!(
            network.submit(0, request(7, 1, 2, 10)),
            TransferSubmit::Delivered {
                bytes: 10,
                duplicates: 0
            }
        );
    }

    #[test]
    fn message_ready_order_is_delivery_tick_then_send_sequence() {
        let mut network = SimNetwork::default();
        network.set_delay(1, 2, 2);
        network.send_message(0, 1, 2, MessageClass::Control, vec![1]);
        network.clear_delay(1, 2);
        network.send_message(1, 1, 2, MessageClass::Data, vec![2]);
        network.send_message(1, 1, 2, MessageClass::Repair, vec![3]);

        network.advance_messages(1);
        let early = network.drain_messages(2);
        assert_eq!(
            early
                .iter()
                .map(|message| message.payload[0])
                .collect::<Vec<_>>(),
            vec![2, 3]
        );

        network.advance_messages(2);
        assert_eq!(network.recv_message(2).unwrap().payload, vec![1]);
    }
}
