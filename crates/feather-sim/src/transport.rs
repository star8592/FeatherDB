use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct LinkBehavior {
    delay_ticks: u64,
    drop_next: u64,
    duplicate_next: u64,
    reorder_next: u64,
    reorder_extra_delay_ticks: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InFlightPacket {
    request: TransferRequest,
    deliver_at: u64,
    duplicates: u32,
}

#[derive(Clone, Debug, Default)]
pub struct SimNetwork {
    links: BTreeMap<(NodeId, NodeId), LinkBehavior>,
    partitions: BTreeSet<(NodeId, NodeId)>,
    in_flight: BTreeMap<u64, InFlightPacket>,
}

impl SimNetwork {
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
}

impl MigrationTransport for SimNetwork {
    fn submit(&mut self, now_tick: u64, request: TransferRequest) -> TransferSubmit {
        if self.partitions.contains(&(request.from, request.to)) {
            return TransferSubmit::Dropped;
        }

        let behavior = self.links.entry((request.from, request.to)).or_default();
        if behavior.drop_next > 0 {
            behavior.drop_next -= 1;
            return TransferSubmit::Dropped;
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
        let effective_delay = behavior.delay_ticks.saturating_add(reorder_delay);

        if effective_delay == 0 {
            return TransferSubmit::Delivered {
                bytes: request.bytes,
                duplicates,
            };
        }

        self.in_flight.insert(
            request.task_id,
            InFlightPacket {
                request,
                deliver_at: now_tick.saturating_add(effective_delay),
                duplicates,
            },
        );
        TransferSubmit::InFlight
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
}
