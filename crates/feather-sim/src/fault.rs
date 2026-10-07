use std::collections::BTreeMap;

use crate::disk::{DurableStore, SimDisk};
use crate::migration::{MigrationScheduler, NodeHealth};
use crate::model::NodeId;
use crate::transport::{SimClock, SimNetwork};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultAction {
    SetNodeHealth {
        node_id: NodeId,
        health: NodeHealth,
    },
    SetLinkDelay {
        from: NodeId,
        to: NodeId,
        ticks: u64,
    },
    DropNext {
        from: NodeId,
        to: NodeId,
        count: u64,
    },
    DuplicateNext {
        from: NodeId,
        to: NodeId,
        count: u64,
    },
    ReorderNext {
        from: NodeId,
        to: NodeId,
        count: u64,
        extra_delay_ticks: u64,
    },
    Partition {
        a: NodeId,
        b: NodeId,
        bidirectional: bool,
    },
    Heal {
        a: NodeId,
        b: NodeId,
        bidirectional: bool,
    },
    SetDiskFull {
        node_id: NodeId,
        full: bool,
    },
    SetDiskDelay {
        node_id: NodeId,
        ticks: u64,
    },
    DiskFailNext {
        node_id: NodeId,
        count: u64,
    },
    CorruptNextDiskRead {
        node_id: NodeId,
        count: u64,
    },
    CorruptNextDiskWrite {
        node_id: NodeId,
        count: u64,
    },
    CrashDisk {
        node_id: NodeId,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FaultEvent {
    pub tick: u64,
    pub sequence: u64,
    pub action: FaultAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultTrace {
    pub seed: u64,
    events: Vec<FaultEvent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultTraceParseError {
    MissingHeader,
    InvalidVersion,
    InvalidSeed,
    InvalidEvent,
    InvalidHealth,
}

impl FaultTrace {
    pub fn new(seed: u64, mut events: Vec<FaultEvent>) -> Self {
        events.sort_by_key(|event| (event.tick, event.sequence));
        Self { seed, events }
    }

    pub fn events(&self) -> &[FaultEvent] {
        &self.events
    }

    pub fn last_tick(&self) -> u64 {
        self.events.last().map(|event| event.tick).unwrap_or(0)
    }

    pub fn to_text(&self) -> String {
        let mut out = format!("feather-fault-trace-v3,{}\n", self.seed);
        for event in &self.events {
            match event.action {
                FaultAction::SetNodeHealth { node_id, health } => {
                    let health = match health {
                        NodeHealth::Healthy => "healthy",
                        NodeHealth::Suspect => "suspect",
                        NodeHealth::Unavailable => "unavailable",
                    };
                    out.push_str(&format!(
                        "{},{},node-health,{},{}\n",
                        event.tick, event.sequence, node_id, health
                    ));
                }
                FaultAction::SetLinkDelay { from, to, ticks } => {
                    out.push_str(&format!(
                        "{},{},link-delay,{},{},{}\n",
                        event.tick, event.sequence, from, to, ticks
                    ));
                }
                FaultAction::DropNext { from, to, count } => {
                    out.push_str(&format!(
                        "{},{},drop-next,{},{},{}\n",
                        event.tick, event.sequence, from, to, count
                    ));
                }
                FaultAction::DuplicateNext { from, to, count } => {
                    out.push_str(&format!(
                        "{},{},duplicate-next,{},{},{}\n",
                        event.tick, event.sequence, from, to, count
                    ));
                }
                FaultAction::ReorderNext {
                    from,
                    to,
                    count,
                    extra_delay_ticks,
                } => {
                    out.push_str(&format!(
                        "{},{},reorder-next,{},{},{},{}
",
                        event.tick, event.sequence, from, to, count, extra_delay_ticks
                    ));
                }
                FaultAction::Partition {
                    a,
                    b,
                    bidirectional,
                } => {
                    out.push_str(&format!(
                        "{},{},partition,{},{},{}\n",
                        event.tick,
                        event.sequence,
                        a,
                        b,
                        if bidirectional { 1 } else { 0 }
                    ));
                }
                FaultAction::Heal {
                    a,
                    b,
                    bidirectional,
                } => {
                    out.push_str(&format!(
                        "{},{},heal,{},{},{}\n",
                        event.tick,
                        event.sequence,
                        a,
                        b,
                        if bidirectional { 1 } else { 0 }
                    ));
                }
                FaultAction::SetDiskFull { node_id, full } => {
                    out.push_str(&format!(
                        "{},{},disk-full,{},{}\n",
                        event.tick,
                        event.sequence,
                        node_id,
                        if full { 1 } else { 0 }
                    ));
                }
                FaultAction::SetDiskDelay { node_id, ticks } => {
                    out.push_str(&format!(
                        "{},{},disk-delay,{},{}\n",
                        event.tick, event.sequence, node_id, ticks
                    ));
                }
                FaultAction::DiskFailNext { node_id, count } => {
                    out.push_str(&format!(
                        "{},{},disk-fail-next,{},{}\n",
                        event.tick, event.sequence, node_id, count
                    ));
                }
                FaultAction::CorruptNextDiskRead { node_id, count } => {
                    out.push_str(&format!(
                        "{},{},disk-corrupt-read,{},{}\n",
                        event.tick, event.sequence, node_id, count
                    ));
                }
                FaultAction::CorruptNextDiskWrite { node_id, count } => {
                    out.push_str(&format!(
                        "{},{},disk-corrupt-write,{},{}\n",
                        event.tick, event.sequence, node_id, count
                    ));
                }
                FaultAction::CrashDisk { node_id } => {
                    out.push_str(&format!(
                        "{},{},disk-crash,{}\n",
                        event.tick, event.sequence, node_id
                    ));
                }
            }
        }
        out
    }

    pub fn from_text(input: &str) -> Result<Self, FaultTraceParseError> {
        let mut lines = input.lines();
        let header = lines.next().ok_or(FaultTraceParseError::MissingHeader)?;
        let mut header_parts = header.split(',');
        let version = header_parts
            .next()
            .ok_or(FaultTraceParseError::InvalidVersion)?;
        if !matches!(
            version,
            "feather-fault-trace-v1" | "feather-fault-trace-v2" | "feather-fault-trace-v3"
        ) {
            return Err(FaultTraceParseError::InvalidVersion);
        }
        let seed = header_parts
            .next()
            .ok_or(FaultTraceParseError::InvalidSeed)?
            .parse::<u64>()
            .map_err(|_| FaultTraceParseError::InvalidSeed)?;
        if header_parts.next().is_some() {
            return Err(FaultTraceParseError::InvalidSeed);
        }

        let mut events = Vec::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let parts: Vec<_> = line.split(',').collect();
            if parts.len() < 4 {
                return Err(FaultTraceParseError::InvalidEvent);
            }

            let tick = parts[0]
                .parse::<u64>()
                .map_err(|_| FaultTraceParseError::InvalidEvent)?;
            let sequence = parts[1]
                .parse::<u64>()
                .map_err(|_| FaultTraceParseError::InvalidEvent)?;

            let parse_node = |value: &str| {
                value
                    .parse::<NodeId>()
                    .map_err(|_| FaultTraceParseError::InvalidEvent)
            };
            let parse_u64 = |value: &str| {
                value
                    .parse::<u64>()
                    .map_err(|_| FaultTraceParseError::InvalidEvent)
            };
            let parse_bool = |value: &str| match value {
                "0" => Ok(false),
                "1" => Ok(true),
                _ => Err(FaultTraceParseError::InvalidEvent),
            };

            let supports_network =
                matches!(version, "feather-fault-trace-v2" | "feather-fault-trace-v3");
            let supports_disk = version == "feather-fault-trace-v3";

            let action = match parts[2] {
                "node-health" if parts.len() == 5 => {
                    let node_id = parse_node(parts[3])?;
                    let health = match parts[4] {
                        "healthy" => NodeHealth::Healthy,
                        "suspect" => NodeHealth::Suspect,
                        "unavailable" => NodeHealth::Unavailable,
                        _ => return Err(FaultTraceParseError::InvalidHealth),
                    };
                    FaultAction::SetNodeHealth { node_id, health }
                }
                "link-delay" if supports_network && parts.len() == 6 => FaultAction::SetLinkDelay {
                    from: parse_node(parts[3])?,
                    to: parse_node(parts[4])?,
                    ticks: parse_u64(parts[5])?,
                },
                "drop-next" if supports_network && parts.len() == 6 => FaultAction::DropNext {
                    from: parse_node(parts[3])?,
                    to: parse_node(parts[4])?,
                    count: parse_u64(parts[5])?,
                },
                "duplicate-next" if supports_network && parts.len() == 6 => {
                    FaultAction::DuplicateNext {
                        from: parse_node(parts[3])?,
                        to: parse_node(parts[4])?,
                        count: parse_u64(parts[5])?,
                    }
                }
                "reorder-next" if supports_network && parts.len() == 7 => {
                    FaultAction::ReorderNext {
                        from: parse_node(parts[3])?,
                        to: parse_node(parts[4])?,
                        count: parse_u64(parts[5])?,
                        extra_delay_ticks: parse_u64(parts[6])?,
                    }
                }
                "partition" if supports_network && parts.len() == 6 => FaultAction::Partition {
                    a: parse_node(parts[3])?,
                    b: parse_node(parts[4])?,
                    bidirectional: parse_bool(parts[5])?,
                },
                "heal" if supports_network && parts.len() == 6 => FaultAction::Heal {
                    a: parse_node(parts[3])?,
                    b: parse_node(parts[4])?,
                    bidirectional: parse_bool(parts[5])?,
                },
                "disk-full" if supports_disk && parts.len() == 5 => FaultAction::SetDiskFull {
                    node_id: parse_node(parts[3])?,
                    full: parse_bool(parts[4])?,
                },
                "disk-delay" if supports_disk && parts.len() == 5 => FaultAction::SetDiskDelay {
                    node_id: parse_node(parts[3])?,
                    ticks: parse_u64(parts[4])?,
                },
                "disk-fail-next" if supports_disk && parts.len() == 5 => {
                    FaultAction::DiskFailNext {
                        node_id: parse_node(parts[3])?,
                        count: parse_u64(parts[4])?,
                    }
                }
                "disk-corrupt-read" if supports_disk && parts.len() == 5 => {
                    FaultAction::CorruptNextDiskRead {
                        node_id: parse_node(parts[3])?,
                        count: parse_u64(parts[4])?,
                    }
                }
                "disk-corrupt-write" if supports_disk && parts.len() == 5 => {
                    FaultAction::CorruptNextDiskWrite {
                        node_id: parse_node(parts[3])?,
                        count: parse_u64(parts[4])?,
                    }
                }
                "disk-crash" if supports_disk && parts.len() == 4 => FaultAction::CrashDisk {
                    node_id: parse_node(parts[3])?,
                },
                _ => return Err(FaultTraceParseError::InvalidEvent),
            };

            events.push(FaultEvent {
                tick,
                sequence,
                action,
            });
        }

        Ok(Self::new(seed, events))
    }

    pub fn generate_health_flaps(
        seed: u64,
        node_ids: &[NodeId],
        flap_count: usize,
        max_gap_ticks: u64,
        max_down_ticks: u64,
    ) -> Self {
        assert!(!node_ids.is_empty(), "node_ids must not be empty");
        assert!(max_gap_ticks > 0, "max_gap_ticks must be > 0");
        assert!(max_down_ticks > 0, "max_down_ticks must be > 0");

        let mut rng = SplitMix64::new(seed);
        let mut events = Vec::with_capacity(flap_count.saturating_mul(2));
        let mut tick = 0_u64;
        let mut sequence = 0_u64;

        for _ in 0..flap_count {
            tick = tick.saturating_add(1 + rng.next_u64() % max_gap_ticks);
            let node_id = node_ids[(rng.next_u64() as usize) % node_ids.len()];
            let down_ticks = 1 + rng.next_u64() % max_down_ticks;

            events.push(FaultEvent {
                tick,
                sequence,
                action: FaultAction::SetNodeHealth {
                    node_id,
                    health: NodeHealth::Unavailable,
                },
            });
            sequence += 1;

            tick = tick.saturating_add(down_ticks);
            events.push(FaultEvent {
                tick,
                sequence,
                action: FaultAction::SetNodeHealth {
                    node_id,
                    health: NodeHealth::Healthy,
                },
            });
            sequence += 1;
        }

        Self::new(seed, events)
    }

    pub fn generate_network_faults(
        seed: u64,
        node_ids: &[NodeId],
        episode_count: usize,
        max_gap_ticks: u64,
        max_duration_ticks: u64,
        max_delay_ticks: u64,
    ) -> Self {
        assert!(node_ids.len() >= 2, "at least two nodes are required");
        assert!(max_gap_ticks > 0, "max_gap_ticks must be > 0");
        assert!(max_duration_ticks > 0, "max_duration_ticks must be > 0");
        assert!(max_delay_ticks > 0, "max_delay_ticks must be > 0");

        let mut rng = SplitMix64::new(seed);
        let mut events = Vec::new();
        let mut tick = 0_u64;
        let mut sequence = 0_u64;

        for _ in 0..episode_count {
            tick = tick.saturating_add(1 + rng.next_u64() % max_gap_ticks);

            let from_index = (rng.next_u64() as usize) % node_ids.len();
            let mut to_index = (rng.next_u64() as usize) % node_ids.len();
            if to_index == from_index {
                to_index = (to_index + 1) % node_ids.len();
            }
            let from = node_ids[from_index];
            let to = node_ids[to_index];

            match rng.next_u64() % 5 {
                0 => {
                    let duration = 1 + rng.next_u64() % max_duration_ticks;
                    let bidirectional = rng.next_u64().is_multiple_of(2);
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::Partition {
                            a: from,
                            b: to,
                            bidirectional,
                        },
                    });
                    sequence += 1;
                    events.push(FaultEvent {
                        tick: tick.saturating_add(duration),
                        sequence,
                        action: FaultAction::Heal {
                            a: from,
                            b: to,
                            bidirectional,
                        },
                    });
                    sequence += 1;
                }
                1 => {
                    let duration = 1 + rng.next_u64() % max_duration_ticks;
                    let delay = 1 + rng.next_u64() % max_delay_ticks;
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::SetLinkDelay {
                            from,
                            to,
                            ticks: delay,
                        },
                    });
                    sequence += 1;
                    events.push(FaultEvent {
                        tick: tick.saturating_add(duration),
                        sequence,
                        action: FaultAction::SetLinkDelay { from, to, ticks: 0 },
                    });
                    sequence += 1;
                }
                2 => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::DropNext {
                            from,
                            to,
                            count: 1 + rng.next_u64() % 3,
                        },
                    });
                    sequence += 1;
                }
                3 => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::DuplicateNext {
                            from,
                            to,
                            count: 1 + rng.next_u64() % 3,
                        },
                    });
                    sequence += 1;
                }
                _ => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::ReorderNext {
                            from,
                            to,
                            count: 1 + rng.next_u64() % 2,
                            extra_delay_ticks: 1 + rng.next_u64() % max_delay_ticks,
                        },
                    });
                    sequence += 1;
                }
            }
        }

        Self::new(seed, events)
    }

    pub fn generate_network_faults_on_links(
        seed: u64,
        directed_links: &[(NodeId, NodeId)],
        episode_count: usize,
        max_gap_ticks: u64,
        max_duration_ticks: u64,
        max_delay_ticks: u64,
    ) -> Self {
        assert!(
            !directed_links.is_empty(),
            "directed_links must not be empty"
        );
        assert!(max_gap_ticks > 0, "max_gap_ticks must be > 0");
        assert!(max_duration_ticks > 0, "max_duration_ticks must be > 0");
        assert!(max_delay_ticks > 0, "max_delay_ticks must be > 0");

        let mut rng = SplitMix64::new(seed);
        let mut events = Vec::new();
        let mut tick = 0_u64;
        let mut sequence = 0_u64;

        for _ in 0..episode_count {
            tick = tick.saturating_add(1 + rng.next_u64() % max_gap_ticks);
            let (from, to) = directed_links[(rng.next_u64() as usize) % directed_links.len()];

            match rng.next_u64() % 5 {
                0 => {
                    let duration = 1 + rng.next_u64() % max_duration_ticks;
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::Partition {
                            a: from,
                            b: to,
                            bidirectional: false,
                        },
                    });
                    sequence += 1;
                    events.push(FaultEvent {
                        tick: tick.saturating_add(duration),
                        sequence,
                        action: FaultAction::Heal {
                            a: from,
                            b: to,
                            bidirectional: false,
                        },
                    });
                    sequence += 1;
                }
                1 => {
                    let duration = 1 + rng.next_u64() % max_duration_ticks;
                    let delay = 1 + rng.next_u64() % max_delay_ticks;
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::SetLinkDelay {
                            from,
                            to,
                            ticks: delay,
                        },
                    });
                    sequence += 1;
                    events.push(FaultEvent {
                        tick: tick.saturating_add(duration),
                        sequence,
                        action: FaultAction::SetLinkDelay { from, to, ticks: 0 },
                    });
                    sequence += 1;
                }
                2 => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::DropNext {
                            from,
                            to,
                            count: 1 + rng.next_u64() % 3,
                        },
                    });
                    sequence += 1;
                }
                3 => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::DuplicateNext {
                            from,
                            to,
                            count: 1 + rng.next_u64() % 3,
                        },
                    });
                    sequence += 1;
                }
                _ => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::ReorderNext {
                            from,
                            to,
                            count: 1 + rng.next_u64() % 2,
                            extra_delay_ticks: 1 + rng.next_u64() % max_delay_ticks,
                        },
                    });
                    sequence += 1;
                }
            }
        }

        Self::new(seed, events)
    }

    pub fn generate_disk_faults(
        seed: u64,
        node_ids: &[NodeId],
        episode_count: usize,
        max_gap_ticks: u64,
        max_duration_ticks: u64,
        max_delay_ticks: u64,
    ) -> Self {
        assert!(!node_ids.is_empty(), "node_ids must not be empty");
        assert!(max_gap_ticks > 0, "max_gap_ticks must be > 0");
        assert!(max_duration_ticks > 0, "max_duration_ticks must be > 0");
        assert!(max_delay_ticks > 0, "max_delay_ticks must be > 0");

        let mut rng = SplitMix64::new(seed);
        let mut events = Vec::new();
        let mut tick = 0_u64;
        let mut sequence = 0_u64;

        for _ in 0..episode_count {
            tick = tick.saturating_add(1 + rng.next_u64() % max_gap_ticks);
            let node_id = node_ids[(rng.next_u64() as usize) % node_ids.len()];

            match rng.next_u64() % 6 {
                0 => {
                    let duration = 1 + rng.next_u64() % max_duration_ticks;
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::SetDiskFull {
                            node_id,
                            full: true,
                        },
                    });
                    sequence += 1;
                    events.push(FaultEvent {
                        tick: tick.saturating_add(duration),
                        sequence,
                        action: FaultAction::SetDiskFull {
                            node_id,
                            full: false,
                        },
                    });
                    sequence += 1;
                }
                1 => {
                    let duration = 1 + rng.next_u64() % max_duration_ticks;
                    let delay = 1 + rng.next_u64() % max_delay_ticks;
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::SetDiskDelay {
                            node_id,
                            ticks: delay,
                        },
                    });
                    sequence += 1;
                    events.push(FaultEvent {
                        tick: tick.saturating_add(duration),
                        sequence,
                        action: FaultAction::SetDiskDelay { node_id, ticks: 0 },
                    });
                    sequence += 1;
                }
                2 => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::DiskFailNext {
                            node_id,
                            count: 1 + rng.next_u64() % 3,
                        },
                    });
                    sequence += 1;
                }
                3 => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::CorruptNextDiskRead {
                            node_id,
                            count: 1 + rng.next_u64() % 2,
                        },
                    });
                    sequence += 1;
                }
                4 => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::CorruptNextDiskWrite {
                            node_id,
                            count: 1 + rng.next_u64() % 2,
                        },
                    });
                    sequence += 1;
                }
                _ => {
                    events.push(FaultEvent {
                        tick,
                        sequence,
                        action: FaultAction::CrashDisk { node_id },
                    });
                    sequence += 1;
                }
            }
        }

        Self::new(seed, events)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FaultReplayReport {
    pub ticks_executed: u64,
    pub events_applied: usize,
    pub source_failovers: usize,
    pub completed_migrations: usize,
    pub transfer_drops: usize,
    pub transfer_duplicates: usize,
    pub bytes_attempted: u64,
    pub bytes_copied: u64,
    pub converged: bool,
    pub remaining_bytes: u64,
}

pub fn replay_migration_faults(
    scheduler: &mut MigrationScheduler,
    trace: &FaultTrace,
    max_ticks: u64,
) -> FaultReplayReport {
    let mut report = FaultReplayReport::default();
    let mut next_event = 0_usize;

    for tick in 0..max_ticks {
        while let Some(event) = trace.events.get(next_event) {
            if event.tick != tick {
                break;
            }

            apply_fault_action(scheduler, None, event.action);

            report.events_applied += 1;
            next_event += 1;
        }

        let tick_report = scheduler.tick();
        report.ticks_executed = tick + 1;
        report.source_failovers += tick_report.source_failovers;
        report.completed_migrations += tick_report.completed;
        report.transfer_drops += tick_report.transfer_drops;
        report.transfer_duplicates += tick_report.transfer_duplicates;
        report.bytes_attempted += tick_report.bytes_attempted;
        report.bytes_copied += tick_report.bytes_copied;

        if next_event == trace.events.len() && scheduler.is_converged() {
            break;
        }
    }

    report.converged = scheduler.is_converged();
    report.remaining_bytes = scheduler.remaining_bytes();
    report
}

pub fn replay_migration_faults_with_network(
    scheduler: &mut MigrationScheduler,
    network: &mut SimNetwork,
    trace: &FaultTrace,
    max_ticks: u64,
) -> FaultReplayReport {
    let mut report = FaultReplayReport::default();
    let mut next_event = 0_usize;

    let mut clock = SimClock::new();
    for _ in 0..max_ticks {
        let now_tick = clock.now();
        while let Some(event) = trace.events.get(next_event) {
            if event.tick != now_tick {
                break;
            }

            apply_fault_action(scheduler, Some(network), event.action);
            report.events_applied += 1;
            next_event += 1;
        }

        let tick_report = scheduler.tick_with_transport(now_tick, network);
        report.ticks_executed = now_tick + 1;
        report.source_failovers += tick_report.source_failovers;
        report.completed_migrations += tick_report.completed;
        report.transfer_drops += tick_report.transfer_drops;
        report.transfer_duplicates += tick_report.transfer_duplicates;
        report.bytes_attempted += tick_report.bytes_attempted;
        report.bytes_copied += tick_report.bytes_copied;

        if next_event == trace.events.len() && scheduler.is_converged() {
            break;
        }
        clock.advance();
    }

    report.converged = scheduler.is_converged();
    report.remaining_bytes = scheduler.remaining_bytes();
    report
}

pub fn apply_network_fault_action(network: &mut SimNetwork, action: FaultAction) -> bool {
    match action {
        FaultAction::SetNodeHealth { .. }
        | FaultAction::SetDiskFull { .. }
        | FaultAction::SetDiskDelay { .. }
        | FaultAction::DiskFailNext { .. }
        | FaultAction::CorruptNextDiskRead { .. }
        | FaultAction::CorruptNextDiskWrite { .. }
        | FaultAction::CrashDisk { .. } => false,
        FaultAction::SetLinkDelay { from, to, ticks } => {
            network.set_delay(from, to, ticks);
            true
        }
        FaultAction::DropNext { from, to, count } => {
            network.drop_next(from, to, count);
            true
        }
        FaultAction::DuplicateNext { from, to, count } => {
            network.duplicate_next(from, to, count);
            true
        }
        FaultAction::ReorderNext {
            from,
            to,
            count,
            extra_delay_ticks,
        } => {
            network.reorder_next(from, to, count, extra_delay_ticks);
            true
        }
        FaultAction::Partition {
            a,
            b,
            bidirectional,
        } => {
            network.partition(a, b, bidirectional);
            true
        }
        FaultAction::Heal {
            a,
            b,
            bidirectional,
        } => {
            network.heal(a, b, bidirectional);
            true
        }
    }
}

pub fn apply_disk_fault_action(disks: &mut BTreeMap<NodeId, SimDisk>, action: FaultAction) -> bool {
    let node_id = match action {
        FaultAction::SetDiskFull { node_id, .. }
        | FaultAction::SetDiskDelay { node_id, .. }
        | FaultAction::DiskFailNext { node_id, .. }
        | FaultAction::CorruptNextDiskRead { node_id, .. }
        | FaultAction::CorruptNextDiskWrite { node_id, .. }
        | FaultAction::CrashDisk { node_id } => node_id,
        _ => return false,
    };
    let Some(disk) = disks.get_mut(&node_id) else {
        return false;
    };

    match action {
        FaultAction::SetDiskFull { full, .. } => disk.set_full(full),
        FaultAction::SetDiskDelay { ticks, .. } => disk.set_delay(ticks),
        FaultAction::DiskFailNext { count, .. } => disk.fail_next(count),
        FaultAction::CorruptNextDiskRead { count, .. } => disk.corrupt_next_read(count),
        FaultAction::CorruptNextDiskWrite { count, .. } => disk.corrupt_next_write(count),
        FaultAction::CrashDisk { .. } => disk.crash(),
        _ => unreachable!("disk action already filtered"),
    }
    true
}

fn apply_fault_action(
    scheduler: &mut MigrationScheduler,
    network: Option<&mut SimNetwork>,
    action: FaultAction,
) {
    if let FaultAction::SetNodeHealth { node_id, health } = action {
        let _ = scheduler.set_node_health(node_id, health);
        return;
    }

    if let Some(network) = network {
        let _ = apply_network_fault_action(network, action);
    }
}

#[derive(Clone, Copy, Debug)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::migration::{MigrationBudget, TabletAvailability};
    use crate::model::{AdminState, Cluster, Node, Placement, Tablet};
    use crate::placement::FailureDomainPolicy;

    fn node(id: u64, state: AdminState, zone: &str) -> Node {
        Node {
            id,
            weight: 1,
            zone: zone.into(),
            rack: "r1".into(),
            state,
        }
    }

    fn repair_scheduler() -> MigrationScheduler {
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
            tablets: (0..32).map(|id| Tablet { id, bytes: 128 }).collect(),
        };

        let actual = Placement {
            replicas: (0..32)
                .map(|id| (id, vec![1, 2, 3]))
                .collect::<BTreeMap<_, _>>(),
        };
        let desired = Placement {
            replicas: (0..32)
                .map(|id| (id, vec![2, 3, 4]))
                .collect::<BTreeMap<_, _>>(),
        };

        MigrationScheduler::new(
            cluster,
            FailureDomainPolicy::HIERARCHICAL,
            actual,
            desired,
            MigrationBudget {
                max_active: 2,
                max_per_node_active: 1,
                bytes_per_tick: 64,
                max_bytes_per_node_per_tick: 64,
                max_bytes_per_task_per_tick: 32,
            },
        )
        .unwrap()
    }

    #[test]
    fn same_seed_generates_identical_trace() {
        let a = FaultTrace::generate_health_flaps(42, &[2, 3], 20, 5, 4);
        let b = FaultTrace::generate_health_flaps(42, &[2, 3], 20, 5, 4);
        let c = FaultTrace::generate_health_flaps(43, &[2, 3], 20, 5, 4);

        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.events().len(), 40);
    }

    #[test]
    fn trace_text_round_trip_is_stable() {
        let trace = FaultTrace::generate_health_flaps(8592, &[2, 3], 12, 4, 3);
        let text = trace.to_text();
        let decoded = FaultTrace::from_text(&text).unwrap();

        assert_eq!(decoded, trace);
        assert_eq!(decoded.to_text(), text);
    }

    #[test]
    fn invalid_trace_version_is_rejected() {
        assert_eq!(
            FaultTrace::from_text("other-version,1\n"),
            Err(FaultTraceParseError::InvalidVersion)
        );
    }

    #[test]
    fn trace_events_are_totally_ordered() {
        let trace = FaultTrace::new(
            1,
            vec![
                FaultEvent {
                    tick: 5,
                    sequence: 2,
                    action: FaultAction::SetNodeHealth {
                        node_id: 2,
                        health: NodeHealth::Healthy,
                    },
                },
                FaultEvent {
                    tick: 1,
                    sequence: 1,
                    action: FaultAction::SetNodeHealth {
                        node_id: 2,
                        health: NodeHealth::Unavailable,
                    },
                },
            ],
        );

        assert_eq!(trace.events()[0].tick, 1);
        assert_eq!(trace.events()[1].tick, 5);
    }

    #[test]
    fn same_trace_replays_to_same_scheduler_result() {
        let trace = FaultTrace::generate_health_flaps(8592, &[2, 3], 8, 4, 3);
        let mut a = repair_scheduler();
        let mut b = repair_scheduler();

        let report_a = replay_migration_faults(&mut a, &trace, 10_000);
        let report_b = replay_migration_faults(&mut b, &trace, 10_000);

        assert_eq!(report_a, report_b);
        assert_eq!(a.actual(), b.actual());
        assert_eq!(a.tasks().len(), b.tasks().len());
        assert!(report_a.converged);
    }

    #[test]
    fn network_fault_actions_apply_without_migration_scheduler() {
        let mut network = SimNetwork::default();
        assert!(apply_network_fault_action(
            &mut network,
            FaultAction::Partition {
                a: 1,
                b: 2,
                bidirectional: false,
            },
        ));
        assert!(network.is_partitioned(1, 2));
        assert!(!network.is_partitioned(2, 1));

        assert!(!apply_network_fault_action(
            &mut network,
            FaultAction::SetNodeHealth {
                node_id: 1,
                health: NodeHealth::Unavailable,
            },
        ));
    }

    #[test]
    fn v1_health_trace_remains_readable() {
        let trace = FaultTrace::from_text(
            "feather-fault-trace-v1,7
0,0,node-health,2,unavailable
1,1,node-health,2,healthy
",
        )
        .unwrap();

        assert_eq!(trace.seed, 7);
        assert_eq!(trace.events().len(), 2);
    }

    #[test]
    fn v2_network_trace_remains_readable_after_v3_upgrade() {
        let trace = FaultTrace::from_text(
            "feather-fault-trace-v2,11\n0,0,link-delay,1,2,3\n1,1,partition,1,2,1\n",
        )
        .unwrap();

        assert_eq!(trace.seed, 11);
        assert_eq!(trace.events().len(), 2);
    }

    #[test]
    fn disk_trace_round_trip_is_stable_and_node_scoped() {
        let trace = FaultTrace::new(
            77,
            vec![
                FaultEvent {
                    tick: 1,
                    sequence: 0,
                    action: FaultAction::SetDiskFull {
                        node_id: 2,
                        full: true,
                    },
                },
                FaultEvent {
                    tick: 2,
                    sequence: 1,
                    action: FaultAction::SetDiskDelay {
                        node_id: 2,
                        ticks: 9,
                    },
                },
                FaultEvent {
                    tick: 3,
                    sequence: 2,
                    action: FaultAction::DiskFailNext {
                        node_id: 3,
                        count: 2,
                    },
                },
                FaultEvent {
                    tick: 4,
                    sequence: 3,
                    action: FaultAction::CorruptNextDiskRead {
                        node_id: 3,
                        count: 1,
                    },
                },
                FaultEvent {
                    tick: 5,
                    sequence: 4,
                    action: FaultAction::CorruptNextDiskWrite {
                        node_id: 4,
                        count: 1,
                    },
                },
                FaultEvent {
                    tick: 6,
                    sequence: 5,
                    action: FaultAction::CrashDisk { node_id: 4 },
                },
            ],
        );

        let text = trace.to_text();
        assert!(text.starts_with("feather-fault-trace-v3,77\n"));
        let decoded = FaultTrace::from_text(&text).unwrap();
        assert_eq!(decoded, trace);
        assert_eq!(decoded.to_text(), text);
    }

    #[test]
    fn disk_fault_actions_mutate_only_target_disk() {
        use crate::disk::{DiskCompletion, DiskError, DiskRequest, DiskSubmit};

        let mut disks = BTreeMap::from([(1, SimDisk::default()), (2, SimDisk::default())]);

        assert!(apply_disk_fault_action(
            &mut disks,
            FaultAction::SetDiskFull {
                node_id: 2,
                full: true,
            },
        ));
        assert!(!apply_disk_fault_action(
            &mut disks,
            FaultAction::SetDiskFull {
                node_id: 99,
                full: true,
            },
        ));

        assert_eq!(
            disks.get_mut(&1).unwrap().submit(
                0,
                DiskRequest::Put {
                    op_id: 1,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
            ),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
        assert_eq!(
            disks.get_mut(&2).unwrap().submit(
                0,
                DiskRequest::Put {
                    op_id: 1,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
            ),
            DiskSubmit::Failed(DiskError::Full)
        );
    }

    #[test]
    fn generated_disk_faults_are_deterministic_and_v3_serializable() {
        let a = FaultTrace::generate_disk_faults(8592, &[1, 2, 3], 100, 4, 8, 6);
        let b = FaultTrace::generate_disk_faults(8592, &[1, 2, 3], 100, 4, 8, 6);
        let c = FaultTrace::generate_disk_faults(8593, &[1, 2, 3], 100, 4, 8, 6);

        assert_eq!(a, b);
        assert_ne!(a, c);
        let text = a.to_text();
        assert!(text.starts_with("feather-fault-trace-v3,8592\n"));
        assert_eq!(FaultTrace::from_text(&text).unwrap(), a);
    }

    #[test]
    fn network_trace_round_trip_is_stable() {
        let trace = FaultTrace::new(
            9,
            vec![
                FaultEvent {
                    tick: 1,
                    sequence: 0,
                    action: FaultAction::SetLinkDelay {
                        from: 2,
                        to: 4,
                        ticks: 3,
                    },
                },
                FaultEvent {
                    tick: 2,
                    sequence: 1,
                    action: FaultAction::DropNext {
                        from: 2,
                        to: 4,
                        count: 2,
                    },
                },
                FaultEvent {
                    tick: 3,
                    sequence: 2,
                    action: FaultAction::DuplicateNext {
                        from: 3,
                        to: 4,
                        count: 1,
                    },
                },
                FaultEvent {
                    tick: 4,
                    sequence: 3,
                    action: FaultAction::ReorderNext {
                        from: 2,
                        to: 4,
                        count: 1,
                        extra_delay_ticks: 5,
                    },
                },
                FaultEvent {
                    tick: 5,
                    sequence: 4,
                    action: FaultAction::Partition {
                        a: 2,
                        b: 4,
                        bidirectional: false,
                    },
                },
                FaultEvent {
                    tick: 6,
                    sequence: 5,
                    action: FaultAction::Heal {
                        a: 2,
                        b: 4,
                        bidirectional: false,
                    },
                },
            ],
        );

        let text = trace.to_text();
        let decoded = FaultTrace::from_text(&text).unwrap();
        assert_eq!(decoded, trace);
        assert_eq!(decoded.to_text(), text);
    }

    #[test]
    fn same_seed_generates_identical_network_trace() {
        let a = FaultTrace::generate_network_faults(8592, &[2, 3, 4], 30, 4, 5, 6);
        let b = FaultTrace::generate_network_faults(8592, &[2, 3, 4], 30, 4, 5, 6);
        let c = FaultTrace::generate_network_faults(8593, &[2, 3, 4], 30, 4, 5, 6);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn link_scoped_network_faults_are_deterministic() {
        let links = [(2, 4), (3, 4)];
        let a = FaultTrace::generate_network_faults_on_links(8592, &links, 40, 3, 4, 5);
        let b = FaultTrace::generate_network_faults_on_links(8592, &links, 40, 3, 4, 5);

        assert_eq!(a, b);
        assert!(a.events().iter().all(|event| match event.action {
            FaultAction::SetNodeHealth { .. }
            | FaultAction::SetDiskFull { .. }
            | FaultAction::SetDiskDelay { .. }
            | FaultAction::DiskFailNext { .. }
            | FaultAction::CorruptNextDiskRead { .. }
            | FaultAction::CorruptNextDiskWrite { .. }
            | FaultAction::CrashDisk { .. } => false,
            FaultAction::SetLinkDelay { from, to, .. }
            | FaultAction::DropNext { from, to, .. }
            | FaultAction::DuplicateNext { from, to, .. }
            | FaultAction::ReorderNext { from, to, .. } => links.contains(&(from, to)),
            FaultAction::Partition { a, b, .. } | FaultAction::Heal { a, b, .. } => {
                links.contains(&(a, b))
            }
        }));
    }

    #[test]
    fn bounded_network_faults_replay_and_converge() {
        let trace = FaultTrace::generate_network_faults(8592, &[2, 3, 4], 40, 3, 4, 4);
        let mut a = repair_scheduler();
        let mut b = repair_scheduler();
        let mut network_a = SimNetwork::default();
        let mut network_b = SimNetwork::default();

        let report_a =
            replay_migration_faults_with_network(&mut a, &mut network_a, &trace, 100_000);
        let report_b =
            replay_migration_faults_with_network(&mut b, &mut network_b, &trace, 100_000);

        assert_eq!(report_a, report_b);
        assert_eq!(a.actual(), b.actual());
        assert_eq!(report_a.events_applied, trace.events().len());
        assert!(report_a.converged);
        assert_eq!(report_a.remaining_bytes, 0);
    }

    #[test]
    fn transient_health_faults_do_not_rewrite_topology_ownership() {
        let mut scheduler = repair_scheduler();
        let before_actual = scheduler.actual().clone();
        let before_desired = scheduler.desired().clone();

        let trace = FaultTrace::new(
            7,
            vec![
                FaultEvent {
                    tick: 0,
                    sequence: 0,
                    action: FaultAction::SetNodeHealth {
                        node_id: 2,
                        health: NodeHealth::Unavailable,
                    },
                },
                FaultEvent {
                    tick: 1,
                    sequence: 1,
                    action: FaultAction::SetNodeHealth {
                        node_id: 2,
                        health: NodeHealth::Healthy,
                    },
                },
            ],
        );

        let _ = replay_migration_faults(&mut scheduler, &trace, 2);

        assert_eq!(scheduler.desired(), &before_desired);
        assert_eq!(scheduler.actual(), &before_actual);
        assert!(matches!(
            scheduler.tablet_availability(0),
            Some(TabletAvailability::Degraded { .. }) | Some(TabletAvailability::Healthy { .. })
        ));
    }

    #[test]
    fn bounded_flaps_eventually_converge_after_faults_stop() {
        let trace = FaultTrace::generate_health_flaps(2026, &[2, 3], 25, 3, 3);
        let mut scheduler = repair_scheduler();

        let report = replay_migration_faults(&mut scheduler, &trace, 100_000);

        assert_eq!(report.events_applied, trace.events().len());
        assert!(report.converged);
        assert_eq!(report.remaining_bytes, 0);
        assert!(report.ticks_executed >= trace.last_tick());
    }
}
