use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::model::NodeId;
use crate::transport::{
    MessageBusLimits, MessageClass, MessageSend, SimClock, SimMessage, SimNetwork,
};

const WIRE_VERSION: u8 = 1;
const WIRE_HEADER_BYTES: usize = 28;
const WIRE_UPDATE_BYTES: usize = 17;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum MemberStatus {
    Alive = 0,
    Suspect = 1,
    Dead = 2,
}

impl MemberStatus {
    fn from_wire(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Alive),
            1 => Some(Self::Suspect),
            2 => Some(Self::Dead),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemberState {
    pub incarnation: u64,
    pub status: MemberStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemberUpdate {
    pub node_id: NodeId,
    pub incarnation: u64,
    pub status: MemberStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MembershipConfig {
    pub probe_interval_ticks: u64,
    pub direct_timeout_ticks: u64,
    pub indirect_timeout_ticks: u64,
    pub suspicion_timeout_ticks: u64,
    pub indirect_checks: usize,
    pub max_awareness_score: u8,
    pub piggyback_updates: usize,
    pub update_retransmits: u8,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            probe_interval_ticks: 2,
            direct_timeout_ticks: 2,
            indirect_timeout_ticks: 4,
            suspicion_timeout_ticks: 8,
            indirect_checks: 3,
            max_awareness_score: 8,
            piggyback_updates: 8,
            update_retransmits: 8,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipConfigError {
    ZeroProbeInterval,
    DirectTimeoutBelowMinimumRoundTrip,
    IndirectTimeoutBelowMinimumRoundTrip,
    ZeroSuspicionTimeout,
    ZeroPiggybackLimit,
    ZeroRetransmits,
}

impl MembershipConfig {
    pub fn validate(&self) -> Result<(), MembershipConfigError> {
        if self.probe_interval_ticks == 0 {
            return Err(MembershipConfigError::ZeroProbeInterval);
        }
        // SimNetwork intentionally delivers only at explicit event-loop
        // boundaries. A direct Ping/Ack needs two ticks; PingReq -> Ping ->
        // Ack -> forwarded Ack needs four ticks after the indirect request.
        if self.direct_timeout_ticks < 2 {
            return Err(MembershipConfigError::DirectTimeoutBelowMinimumRoundTrip);
        }
        if self.indirect_timeout_ticks < 4 {
            return Err(MembershipConfigError::IndirectTimeoutBelowMinimumRoundTrip);
        }
        if self.suspicion_timeout_ticks == 0 {
            return Err(MembershipConfigError::ZeroSuspicionTimeout);
        }
        if self.piggyback_updates == 0 {
            return Err(MembershipConfigError::ZeroPiggybackLimit);
        }
        if self.update_retransmits == 0 {
            return Err(MembershipConfigError::ZeroRetransmits);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ProbeId {
    origin: NodeId,
    sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbePhase {
    Direct,
    Indirect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingProbe {
    id: ProbeId,
    target: NodeId,
    phase: ProbePhase,
    deadline_tick: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RelayProbe {
    origin: NodeId,
    target: NodeId,
    expires_at: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Suspicion {
    incarnation: u64,
    deadline_tick: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingUpdate {
    update: MemberUpdate,
    remaining: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum WireKind {
    Ping = 1,
    Ack = 2,
    PingReq = 3,
}

impl WireKind {
    fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Ping),
            2 => Some(Self::Ack),
            3 => Some(Self::PingReq),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WireMessage {
    kind: WireKind,
    probe: ProbeId,
    target: NodeId,
    updates: Vec<MemberUpdate>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MembershipStats {
    pub direct_probes_started: u64,
    pub indirect_probe_rounds: u64,
    pub acks_received: u64,
    pub suspects_created: u64,
    pub dead_created: u64,
    pub self_refutations: u64,
    pub updates_applied: u64,
    pub malformed_messages: u64,
    pub backpressured_messages: u64,
}

#[derive(Clone, Debug)]
pub struct SwimNode {
    id: NodeId,
    config: MembershipConfig,
    members: BTreeMap<NodeId, MemberState>,
    suspicions: BTreeMap<NodeId, Suspicion>,
    pending_probe: Option<PendingProbe>,
    relays: BTreeMap<(ProbeId, NodeId), RelayProbe>,
    updates: VecDeque<PendingUpdate>,
    next_probe_sequence: u64,
    next_probe_at: u64,
    awareness_score: u8,
    stats: MembershipStats,
}

impl SwimNode {
    pub fn new(
        id: NodeId,
        known_nodes: &[NodeId],
        config: MembershipConfig,
    ) -> Result<Self, MembershipConfigError> {
        config.validate()?;
        let mut members = BTreeMap::new();
        for node_id in known_nodes.iter().copied().chain(std::iter::once(id)) {
            members.entry(node_id).or_insert(MemberState {
                incarnation: 0,
                status: MemberStatus::Alive,
            });
        }

        Ok(Self {
            id,
            config,
            members,
            suspicions: BTreeMap::new(),
            pending_probe: None,
            relays: BTreeMap::new(),
            updates: VecDeque::new(),
            next_probe_sequence: 1,
            next_probe_at: 0,
            awareness_score: 0,
            stats: MembershipStats::default(),
        })
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn member(&self, node_id: NodeId) -> Option<MemberState> {
        self.members.get(&node_id).copied()
    }

    pub fn members(&self) -> &BTreeMap<NodeId, MemberState> {
        &self.members
    }

    pub fn awareness_score(&self) -> u8 {
        self.awareness_score
    }

    pub fn stats(&self) -> MembershipStats {
        self.stats
    }

    pub fn pending_probe_target(&self) -> Option<NodeId> {
        self.pending_probe.map(|probe| probe.target)
    }

    pub fn scaled_timeout(&self, base_ticks: u64) -> u64 {
        base_ticks.saturating_mul(u64::from(self.awareness_score).saturating_add(1))
    }

    pub fn tick(&mut self, now_tick: u64, network: &mut SimNetwork) {
        self.expire_relays(now_tick);
        self.expire_suspicions(now_tick);
        self.advance_pending_probe(now_tick, network);

        if self.pending_probe.is_none() && now_tick >= self.next_probe_at {
            self.start_probe(now_tick, network);
        }
    }

    pub fn handle_message(&mut self, now_tick: u64, message: SimMessage, network: &mut SimNetwork) {
        if message.class != MessageClass::Membership || message.to != self.id {
            return;
        }

        let Some(wire) = decode_wire(&message.payload) else {
            self.stats.malformed_messages = self.stats.malformed_messages.saturating_add(1);
            return;
        };

        for update in wire.updates.iter().copied() {
            if self.apply_update(now_tick, update) {
                self.stats.updates_applied = self.stats.updates_applied.saturating_add(1);
            }
        }

        match wire.kind {
            WireKind::Ping => {
                self.send_wire(
                    now_tick,
                    message.from,
                    WireKind::Ack,
                    wire.probe,
                    self.id,
                    network,
                );
            }
            WireKind::Ack => self.handle_ack(now_tick, message.from, wire, network),
            WireKind::PingReq => self.handle_ping_req(now_tick, wire, network),
        }
    }

    pub fn restart(&mut self, now_tick: u64) {
        let current = self.members.get(&self.id).copied().unwrap_or(MemberState {
            incarnation: 0,
            status: MemberStatus::Alive,
        });
        let incarnation = current.incarnation.saturating_add(1);
        self.members.insert(
            self.id,
            MemberState {
                incarnation,
                status: MemberStatus::Alive,
            },
        );
        self.pending_probe = None;
        self.relays.clear();
        self.suspicions.remove(&self.id);
        self.awareness_score = 0;
        self.next_probe_at = now_tick;
        self.queue_update(MemberUpdate {
            node_id: self.id,
            incarnation,
            status: MemberStatus::Alive,
        });
    }

    fn start_probe(&mut self, now_tick: u64, network: &mut SimNetwork) {
        let Some(target) = self.select_probe_target() else {
            self.next_probe_at =
                now_tick.saturating_add(self.scaled_timeout(self.config.probe_interval_ticks));
            return;
        };

        let probe = ProbeId {
            origin: self.id,
            sequence: self.next_probe_sequence,
        };
        self.next_probe_sequence = self.next_probe_sequence.saturating_add(1);
        self.send_wire(now_tick, target, WireKind::Ping, probe, target, network);
        self.pending_probe = Some(PendingProbe {
            id: probe,
            target,
            phase: ProbePhase::Direct,
            deadline_tick: now_tick
                .saturating_add(self.scaled_timeout(self.config.direct_timeout_ticks)),
        });
        self.stats.direct_probes_started = self.stats.direct_probes_started.saturating_add(1);
    }

    fn advance_pending_probe(&mut self, now_tick: u64, network: &mut SimNetwork) {
        let Some(pending) = self.pending_probe else {
            return;
        };
        if now_tick < pending.deadline_tick {
            return;
        }

        match pending.phase {
            ProbePhase::Direct => {
                let helpers = self.select_indirect_helpers(pending.target, pending.id);
                if helpers.is_empty() {
                    self.fail_probe(now_tick, pending.target);
                    return;
                }

                for helper in helpers {
                    self.send_wire(
                        now_tick,
                        helper,
                        WireKind::PingReq,
                        pending.id,
                        pending.target,
                        network,
                    );
                }
                self.pending_probe = Some(PendingProbe {
                    phase: ProbePhase::Indirect,
                    deadline_tick: now_tick
                        .saturating_add(self.scaled_timeout(self.config.indirect_timeout_ticks)),
                    ..pending
                });
                self.stats.indirect_probe_rounds =
                    self.stats.indirect_probe_rounds.saturating_add(1);
            }
            ProbePhase::Indirect => self.fail_probe(now_tick, pending.target),
        }
    }

    fn fail_probe(&mut self, now_tick: u64, target: NodeId) {
        self.pending_probe = None;
        self.awareness_score = self
            .awareness_score
            .saturating_add(1)
            .min(self.config.max_awareness_score);
        self.mark_suspect(now_tick, target);
        self.next_probe_at =
            now_tick.saturating_add(self.scaled_timeout(self.config.probe_interval_ticks));
    }

    fn probe_succeeded(&mut self, now_tick: u64) {
        self.pending_probe = None;
        self.awareness_score = self.awareness_score.saturating_sub(1);
        self.next_probe_at =
            now_tick.saturating_add(self.scaled_timeout(self.config.probe_interval_ticks));
        self.stats.acks_received = self.stats.acks_received.saturating_add(1);
    }

    fn handle_ack(
        &mut self,
        now_tick: u64,
        sender: NodeId,
        wire: WireMessage,
        network: &mut SimNetwork,
    ) {
        if wire.probe.origin == self.id {
            if self
                .pending_probe
                .is_some_and(|pending| pending.id == wire.probe && pending.target == wire.target)
            {
                self.probe_succeeded(now_tick);
            }
            return;
        }

        let key = (wire.probe, wire.target);
        let Some(relay) = self.relays.remove(&key) else {
            return;
        };
        if sender != relay.target || relay.origin != wire.probe.origin {
            return;
        }
        self.send_wire(
            now_tick,
            relay.origin,
            WireKind::Ack,
            wire.probe,
            relay.target,
            network,
        );
    }

    fn handle_ping_req(&mut self, now_tick: u64, wire: WireMessage, network: &mut SimNetwork) {
        if wire.target == self.id {
            self.send_wire(
                now_tick,
                wire.probe.origin,
                WireKind::Ack,
                wire.probe,
                self.id,
                network,
            );
            return;
        }

        let expires_at = now_tick
            .saturating_add(self.scaled_timeout(self.config.indirect_timeout_ticks))
            .saturating_add(1);
        self.relays.insert(
            (wire.probe, wire.target),
            RelayProbe {
                origin: wire.probe.origin,
                target: wire.target,
                expires_at,
            },
        );
        self.send_wire(
            now_tick,
            wire.target,
            WireKind::Ping,
            wire.probe,
            wire.target,
            network,
        );
    }

    fn mark_suspect(&mut self, now_tick: u64, target: NodeId) {
        let Some(current) = self.members.get(&target).copied() else {
            return;
        };
        if current.status == MemberStatus::Dead {
            return;
        }

        let update = MemberUpdate {
            node_id: target,
            incarnation: current.incarnation,
            status: MemberStatus::Suspect,
        };
        if self.apply_update(now_tick, update) {
            self.stats.suspects_created = self.stats.suspects_created.saturating_add(1);
        }
    }

    fn expire_suspicions(&mut self, now_tick: u64) {
        let expired: Vec<_> = self
            .suspicions
            .iter()
            .filter(|(_, suspicion)| now_tick >= suspicion.deadline_tick)
            .map(|(node_id, suspicion)| (*node_id, suspicion.incarnation))
            .collect();

        for (node_id, incarnation) in expired {
            let still_suspect = self.members.get(&node_id).is_some_and(|state| {
                state.incarnation == incarnation && state.status == MemberStatus::Suspect
            });
            self.suspicions.remove(&node_id);
            if !still_suspect {
                continue;
            }
            let update = MemberUpdate {
                node_id,
                incarnation,
                status: MemberStatus::Dead,
            };
            if self.apply_update(now_tick, update) {
                self.stats.dead_created = self.stats.dead_created.saturating_add(1);
            }
        }
    }

    fn expire_relays(&mut self, now_tick: u64) {
        self.relays.retain(|_, relay| now_tick < relay.expires_at);
    }

    fn apply_update(&mut self, now_tick: u64, update: MemberUpdate) -> bool {
        if update.node_id == self.id {
            return self.apply_self_update(update);
        }

        let current = self.members.get(&update.node_id).copied();
        let should_apply = match current {
            None => true,
            Some(current) => {
                update.incarnation > current.incarnation
                    || (update.incarnation == current.incarnation && update.status > current.status)
            }
        };
        if !should_apply {
            return false;
        }

        self.members.insert(
            update.node_id,
            MemberState {
                incarnation: update.incarnation,
                status: update.status,
            },
        );

        match update.status {
            MemberStatus::Alive => {
                self.suspicions.remove(&update.node_id);
            }
            MemberStatus::Suspect => {
                self.suspicions.insert(
                    update.node_id,
                    Suspicion {
                        incarnation: update.incarnation,
                        deadline_tick: now_tick.saturating_add(
                            self.scaled_timeout(self.config.suspicion_timeout_ticks),
                        ),
                    },
                );
            }
            MemberStatus::Dead => {
                self.suspicions.remove(&update.node_id);
                if self
                    .pending_probe
                    .is_some_and(|probe| probe.target == update.node_id)
                {
                    self.pending_probe = None;
                    self.next_probe_at = now_tick
                        .saturating_add(self.scaled_timeout(self.config.probe_interval_ticks));
                }
            }
        }

        self.queue_update(update);
        true
    }

    fn apply_self_update(&mut self, update: MemberUpdate) -> bool {
        let current = self.members.get(&self.id).copied().unwrap_or(MemberState {
            incarnation: 0,
            status: MemberStatus::Alive,
        });

        let challenges_self = update.incarnation > current.incarnation
            || (update.incarnation == current.incarnation && update.status != MemberStatus::Alive);
        if !challenges_self {
            return false;
        }

        let incarnation = update
            .incarnation
            .checked_add(1)
            .unwrap_or(update.incarnation);
        let refutation = MemberUpdate {
            node_id: self.id,
            incarnation,
            status: MemberStatus::Alive,
        };
        self.members.insert(
            self.id,
            MemberState {
                incarnation,
                status: MemberStatus::Alive,
            },
        );
        self.suspicions.remove(&self.id);
        self.queue_update(refutation);
        self.stats.self_refutations = self.stats.self_refutations.saturating_add(1);
        true
    }

    fn select_probe_target(&self) -> Option<NodeId> {
        let candidates: Vec<_> = self
            .members
            .iter()
            .filter(|(node_id, state)| **node_id != self.id && state.status != MemberStatus::Dead)
            .map(|(node_id, _)| *node_id)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let salt = mix64(self.id ^ self.next_probe_sequence.rotate_left(17));
        Some(candidates[salt as usize % candidates.len()])
    }

    fn select_indirect_helpers(&self, target: NodeId, probe: ProbeId) -> Vec<NodeId> {
        let mut candidates: Vec<_> = self
            .members
            .iter()
            .filter(|(node_id, state)| {
                **node_id != self.id && **node_id != target && state.status == MemberStatus::Alive
            })
            .map(|(node_id, _)| *node_id)
            .collect();
        candidates.sort_by_key(|node_id| {
            mix64(
                *node_id
                    ^ self.id.rotate_left(11)
                    ^ probe.sequence.rotate_left(29)
                    ^ probe.origin.rotate_left(43),
            )
        });
        candidates.truncate(self.config.indirect_checks.min(candidates.len()));
        candidates
    }

    fn send_wire(
        &mut self,
        now_tick: u64,
        to: NodeId,
        kind: WireKind,
        probe: ProbeId,
        target: NodeId,
        network: &mut SimNetwork,
    ) -> MessageSend {
        let updates = self.peek_updates();
        let payload = encode_wire(&WireMessage {
            kind,
            probe,
            target,
            updates,
        });
        let result = network.send_message(now_tick, self.id, to, MessageClass::Membership, payload);
        if matches!(result, MessageSend::Backpressure { .. }) {
            self.stats.backpressured_messages = self.stats.backpressured_messages.saturating_add(1);
        } else {
            self.mark_update_attempts();
        }
        result
    }

    fn queue_update(&mut self, update: MemberUpdate) {
        self.updates.retain(|pending| {
            pending.update.node_id != update.node_id
                || pending.update.incarnation > update.incarnation
        });
        if self.updates.iter().any(|pending| pending.update == update) {
            return;
        }
        self.updates.push_back(PendingUpdate {
            update,
            remaining: self.config.update_retransmits,
        });
    }

    fn peek_updates(&self) -> Vec<MemberUpdate> {
        self.updates
            .iter()
            .take(self.config.piggyback_updates)
            .map(|pending| pending.update)
            .collect()
    }

    fn mark_update_attempts(&mut self) {
        let count = self.config.piggyback_updates.min(self.updates.len());
        for _ in 0..count {
            let Some(mut pending) = self.updates.pop_front() else {
                break;
            };
            pending.remaining = pending.remaining.saturating_sub(1);
            if pending.remaining > 0 {
                self.updates.push_back(pending);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct MembershipCluster {
    nodes: BTreeMap<NodeId, SwimNode>,
    crashed: BTreeSet<NodeId>,
    network: SimNetwork,
    clock: SimClock,
}

impl MembershipCluster {
    pub fn new(
        node_ids: &[NodeId],
        config: MembershipConfig,
        limits: MessageBusLimits,
    ) -> Result<Self, MembershipConfigError> {
        config.validate()?;
        let mut ids = node_ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        let mut nodes = BTreeMap::new();
        for node_id in &ids {
            nodes.insert(*node_id, SwimNode::new(*node_id, &ids, config)?);
        }
        Ok(Self {
            nodes,
            crashed: BTreeSet::new(),
            network: SimNetwork::with_message_limits(limits),
            clock: SimClock::new(),
        })
    }

    pub fn now(&self) -> u64 {
        self.clock.now()
    }

    pub fn node(&self, node_id: NodeId) -> Option<&SwimNode> {
        self.nodes.get(&node_id)
    }

    pub fn network_mut(&mut self) -> &mut SimNetwork {
        &mut self.network
    }

    pub fn crash(&mut self, node_id: NodeId) -> bool {
        if !self.nodes.contains_key(&node_id) {
            return false;
        }
        self.crashed.insert(node_id)
    }

    pub fn restart(&mut self, node_id: NodeId) -> bool {
        if !self.crashed.remove(&node_id) {
            return false;
        }
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.restart(self.clock.now());
            true
        } else {
            false
        }
    }

    pub fn is_crashed(&self, node_id: NodeId) -> bool {
        self.crashed.contains(&node_id)
    }

    pub fn view(&self, observer: NodeId, subject: NodeId) -> Option<MemberState> {
        self.nodes.get(&observer)?.member(subject)
    }

    pub fn tick(&mut self) {
        let now = self.clock.now();
        self.network.advance_messages(now);
        let node_ids: Vec<_> = self.nodes.keys().copied().collect();

        for node_id in &node_ids {
            let messages = self.network.drain_messages(*node_id);
            if self.crashed.contains(node_id) {
                continue;
            }
            if let Some(node) = self.nodes.get_mut(node_id) {
                for message in messages {
                    node.handle_message(now, message, &mut self.network);
                }
            }
        }

        for node_id in node_ids {
            if self.crashed.contains(&node_id) {
                continue;
            }
            if let Some(node) = self.nodes.get_mut(&node_id) {
                node.tick(now, &mut self.network);
            }
        }

        self.clock.advance();
    }

    pub fn run_ticks(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.tick();
        }
    }

    pub fn all_live_observers_see(&self, subject: NodeId, status: MemberStatus) -> bool {
        self.nodes.iter().all(|(observer, node)| {
            self.crashed.contains(observer)
                || *observer == subject
                || node
                    .member(subject)
                    .is_some_and(|state| state.status == status)
        })
    }
}

fn encode_wire(message: &WireMessage) -> Vec<u8> {
    let update_count = message.updates.len().min(u16::MAX as usize);
    let mut payload = Vec::with_capacity(
        WIRE_HEADER_BYTES.saturating_add(update_count.saturating_mul(WIRE_UPDATE_BYTES)),
    );
    payload.push(WIRE_VERSION);
    payload.push(message.kind as u8);
    payload.extend_from_slice(&message.probe.origin.to_le_bytes());
    payload.extend_from_slice(&message.probe.sequence.to_le_bytes());
    payload.extend_from_slice(&message.target.to_le_bytes());
    payload.extend_from_slice(&(update_count as u16).to_le_bytes());
    for update in message.updates.iter().take(update_count) {
        payload.extend_from_slice(&update.node_id.to_le_bytes());
        payload.extend_from_slice(&update.incarnation.to_le_bytes());
        payload.push(update.status as u8);
    }
    payload
}

fn decode_wire(payload: &[u8]) -> Option<WireMessage> {
    if payload.len() < WIRE_HEADER_BYTES || payload[0] != WIRE_VERSION {
        return None;
    }
    let kind = WireKind::from_wire(payload[1])?;
    let probe_origin = read_u64(payload, 2)?;
    let probe_sequence = read_u64(payload, 10)?;
    let target = read_u64(payload, 18)?;
    let update_count = u16::from_le_bytes([*payload.get(26)?, *payload.get(27)?]) as usize;
    let expected = WIRE_HEADER_BYTES.checked_add(update_count.checked_mul(WIRE_UPDATE_BYTES)?)?;
    if payload.len() != expected {
        return None;
    }

    let mut updates = Vec::with_capacity(update_count);
    let mut offset = WIRE_HEADER_BYTES;
    for _ in 0..update_count {
        let node_id = read_u64(payload, offset)?;
        let incarnation = read_u64(payload, offset + 8)?;
        let status = MemberStatus::from_wire(*payload.get(offset + 16)?)?;
        updates.push(MemberUpdate {
            node_id,
            incarnation,
            status,
        });
        offset += WIRE_UPDATE_BYTES;
    }

    Some(WireMessage {
        kind,
        probe: ProbeId {
            origin: probe_origin,
            sequence: probe_sequence,
        },
        target,
        updates,
    })
}

fn read_u64(payload: &[u8], offset: usize) -> Option<u64> {
    let bytes: [u8; 8] = payload.get(offset..offset + 8)?.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> MembershipConfig {
        MembershipConfig {
            probe_interval_ticks: 1,
            direct_timeout_ticks: 2,
            indirect_timeout_ticks: 4,
            suspicion_timeout_ticks: 6,
            indirect_checks: 3,
            max_awareness_score: 8,
            piggyback_updates: 8,
            update_retransmits: 12,
        }
    }

    fn cluster(count: u64) -> MembershipCluster {
        MembershipCluster::new(
            &(1..=count).collect::<Vec<_>>(),
            config(),
            MessageBusLimits {
                max_buffered_messages: 100_000,
                max_buffered_bytes: 16 * 1024 * 1024,
            },
        )
        .unwrap()
    }

    #[test]
    fn config_rejects_timeouts_shorter_than_simulated_round_trip() {
        let mut invalid = config();
        invalid.direct_timeout_ticks = 1;
        assert_eq!(
            invalid.validate(),
            Err(MembershipConfigError::DirectTimeoutBelowMinimumRoundTrip)
        );

        invalid = config();
        invalid.indirect_timeout_ticks = 3;
        assert_eq!(
            invalid.validate(),
            Err(MembershipConfigError::IndirectTimeoutBelowMinimumRoundTrip)
        );
    }

    #[test]
    fn wire_round_trip_preserves_probe_and_updates() {
        let message = WireMessage {
            kind: WireKind::PingReq,
            probe: ProbeId {
                origin: 7,
                sequence: 99,
            },
            target: 42,
            updates: vec![
                MemberUpdate {
                    node_id: 3,
                    incarnation: 2,
                    status: MemberStatus::Suspect,
                },
                MemberUpdate {
                    node_id: 4,
                    incarnation: 8,
                    status: MemberStatus::Alive,
                },
            ],
        };
        assert_eq!(decode_wire(&encode_wire(&message)), Some(message));
    }

    #[test]
    fn malformed_membership_message_is_counted_not_applied() {
        let mut node = SwimNode::new(1, &[1, 2], config()).unwrap();
        let mut network = SimNetwork::default();
        node.handle_message(
            0,
            SimMessage {
                message_id: 1,
                from: 2,
                to: 1,
                class: MessageClass::Membership,
                payload: vec![1, 2, 3],
            },
            &mut network,
        );
        assert_eq!(node.stats().malformed_messages, 1);
        assert_eq!(node.member(2).unwrap().status, MemberStatus::Alive);
    }

    #[test]
    fn healthy_cluster_stays_alive() {
        let mut cluster = cluster(5);
        cluster.run_ticks(300);

        for observer in 1..=5 {
            for subject in 1..=5 {
                assert_eq!(
                    cluster.view(observer, subject).unwrap().status,
                    MemberStatus::Alive,
                    "observer={observer} subject={subject}"
                );
            }
            assert_eq!(
                cluster
                    .node(observer)
                    .unwrap()
                    .stats()
                    .indirect_probe_rounds,
                0,
                "healthy zero-delay cluster should not require indirect probes"
            );
        }
    }

    #[test]
    fn crashed_node_is_eventually_dead_everywhere() {
        let mut cluster = cluster(5);
        cluster.run_ticks(20);
        assert!(cluster.crash(5));

        for _ in 0..500 {
            if cluster.all_live_observers_see(5, MemberStatus::Dead) {
                break;
            }
            cluster.tick();
        }

        assert!(cluster.all_live_observers_see(5, MemberStatus::Dead));
    }

    #[test]
    fn indirect_probe_crosses_direct_partition() {
        let mut cluster = cluster(4);
        cluster.run_ticks(10);

        cluster.network_mut().partition(1, 2, true);
        let before = cluster.node(1).unwrap().stats().indirect_probe_rounds;
        cluster.run_ticks(120);
        let node1 = cluster.node(1).unwrap();

        assert!(node1.stats().indirect_probe_rounds > before);
        assert_ne!(node1.member(2).unwrap().status, MemberStatus::Dead);
    }

    #[test]
    fn suspicion_about_self_is_refuted_with_higher_incarnation() {
        let mut node = SwimNode::new(2, &[1, 2, 3], config()).unwrap();
        let old = node.member(2).unwrap().incarnation;
        assert!(node.apply_update(
            10,
            MemberUpdate {
                node_id: 2,
                incarnation: old,
                status: MemberStatus::Suspect,
            }
        ));

        let current = node.member(2).unwrap();
        assert_eq!(current.status, MemberStatus::Alive);
        assert!(current.incarnation > old);
        assert_eq!(node.stats().self_refutations, 1);
    }

    #[test]
    fn false_suspicion_is_refuted_through_third_party_while_direct_link_stays_down() {
        let mut cluster = cluster(3);
        cluster.run_ticks(20);

        // Isolate node 2 long enough for node 1 to create a real Suspect
        // update. Node 1<->2 remains partitioned throughout recovery.
        cluster.network_mut().partition(1, 2, true);
        cluster.network_mut().partition(3, 2, true);

        for _ in 0..200 {
            if cluster
                .view(1, 2)
                .is_some_and(|state| state.status == MemberStatus::Suspect)
            {
                break;
            }
            cluster.tick();
        }
        assert_eq!(cluster.view(1, 2).unwrap().status, MemberStatus::Suspect);

        let old_incarnation = cluster.view(2, 2).unwrap().incarnation;
        cluster.network_mut().heal(3, 2, true);

        for _ in 0..300 {
            let target_refuted = cluster
                .view(2, 2)
                .is_some_and(|state| state.incarnation > old_incarnation);
            let observer_healed = cluster.view(1, 2).is_some_and(|state| {
                state.status == MemberStatus::Alive && state.incarnation > old_incarnation
            });
            if target_refuted && observer_healed {
                break;
            }
            cluster.tick();
        }

        assert!(cluster.network_mut().is_partitioned(1, 2));
        assert!(
            cluster
                .view(2, 2)
                .is_some_and(|state| state.incarnation > old_incarnation)
        );
        assert!(cluster.view(1, 2).is_some_and(|state| {
            state.status == MemberStatus::Alive && state.incarnation > old_incarnation
        }));
        assert!(cluster.node(2).unwrap().stats().self_refutations > 0);
    }

    #[test]
    fn stale_alive_cannot_resurrect_same_incarnation_suspect() {
        let mut node = SwimNode::new(1, &[1, 2], config()).unwrap();
        assert!(node.apply_update(
            0,
            MemberUpdate {
                node_id: 2,
                incarnation: 0,
                status: MemberStatus::Suspect,
            }
        ));
        assert!(!node.apply_update(
            1,
            MemberUpdate {
                node_id: 2,
                incarnation: 0,
                status: MemberStatus::Alive,
            }
        ));
        assert_eq!(node.member(2).unwrap().status, MemberStatus::Suspect);
    }

    #[test]
    fn higher_incarnation_alive_refutes_suspicion() {
        let mut node = SwimNode::new(1, &[1, 2], config()).unwrap();
        node.apply_update(
            0,
            MemberUpdate {
                node_id: 2,
                incarnation: 0,
                status: MemberStatus::Suspect,
            },
        );
        assert!(node.apply_update(
            1,
            MemberUpdate {
                node_id: 2,
                incarnation: 1,
                status: MemberStatus::Alive,
            }
        ));
        assert_eq!(
            node.member(2),
            Some(MemberState {
                incarnation: 1,
                status: MemberStatus::Alive,
            })
        );
    }

    #[test]
    fn failed_probe_increases_local_health_timeout_and_success_recovers() {
        let mut cluster = cluster(3);
        cluster.network_mut().partition(1, 2, true);
        cluster.network_mut().partition(1, 3, true);

        for _ in 0..30 {
            cluster.tick();
            if cluster.node(1).unwrap().awareness_score() > 0 {
                break;
            }
        }
        let degraded_score = cluster.node(1).unwrap().awareness_score();
        assert!(degraded_score > 0);
        assert!(cluster.node(1).unwrap().scaled_timeout(2) > 2);

        cluster.network_mut().heal(1, 2, true);
        cluster.network_mut().heal(1, 3, true);
        let before = cluster.node(1).unwrap().stats().acks_received;
        for _ in 0..100 {
            cluster.tick();
            if cluster.node(1).unwrap().stats().acks_received > before {
                break;
            }
        }
        assert!(cluster.node(1).unwrap().stats().acks_received > before);
        assert!(cluster.node(1).unwrap().awareness_score() < degraded_score);
    }

    #[test]
    fn restart_refutes_old_dead_state_with_new_incarnation() {
        let mut cluster = cluster(4);
        cluster.run_ticks(20);
        cluster.crash(4);
        for _ in 0..500 {
            if cluster.all_live_observers_see(4, MemberStatus::Dead) {
                break;
            }
            cluster.tick();
        }
        assert!(cluster.all_live_observers_see(4, MemberStatus::Dead));

        assert!(cluster.restart(4));
        let restarted_incarnation = cluster.view(4, 4).unwrap().incarnation;
        assert!(restarted_incarnation > 0);

        for _ in 0..500 {
            let converged = (1..=4).all(|observer| {
                cluster.view(observer, 4).is_some_and(|state| {
                    state.status == MemberStatus::Alive
                        && state.incarnation == restarted_incarnation
                })
            });
            if converged {
                break;
            }
            cluster.tick();
        }

        assert!((1..=4).all(|observer| {
            cluster.view(observer, 4).is_some_and(|state| {
                state.status == MemberStatus::Alive && state.incarnation == restarted_incarnation
            })
        }));
    }

    #[test]
    fn duplicate_packets_do_not_break_probe_state() {
        let mut cluster = cluster(3);
        cluster.network_mut().duplicate_next(1, 2, 4);
        cluster.run_ticks(100);
        assert_eq!(cluster.view(1, 2).unwrap().status, MemberStatus::Alive);
        assert_eq!(cluster.view(2, 1).unwrap().status, MemberStatus::Alive);
    }
}
