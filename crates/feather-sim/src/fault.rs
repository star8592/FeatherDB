use crate::migration::{MigrationScheduler, NodeHealth};
use crate::model::NodeId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultAction {
    SetNodeHealth { node_id: NodeId, health: NodeHealth },
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
        let mut out = format!("feather-fault-trace-v1,{}\n", self.seed);
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
            }
        }
        out
    }

    pub fn from_text(input: &str) -> Result<Self, FaultTraceParseError> {
        let mut lines = input.lines();
        let header = lines.next().ok_or(FaultTraceParseError::MissingHeader)?;
        let mut header_parts = header.split(',');
        if header_parts.next() != Some("feather-fault-trace-v1") {
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
            if parts.len() != 5 || parts[2] != "node-health" {
                return Err(FaultTraceParseError::InvalidEvent);
            }

            let tick = parts[0]
                .parse::<u64>()
                .map_err(|_| FaultTraceParseError::InvalidEvent)?;
            let sequence = parts[1]
                .parse::<u64>()
                .map_err(|_| FaultTraceParseError::InvalidEvent)?;
            let node_id = parts[3]
                .parse::<NodeId>()
                .map_err(|_| FaultTraceParseError::InvalidEvent)?;
            let health = match parts[4] {
                "healthy" => NodeHealth::Healthy,
                "suspect" => NodeHealth::Suspect,
                "unavailable" => NodeHealth::Unavailable,
                _ => return Err(FaultTraceParseError::InvalidHealth),
            };

            events.push(FaultEvent {
                tick,
                sequence,
                action: FaultAction::SetNodeHealth { node_id, health },
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
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FaultReplayReport {
    pub ticks_executed: u64,
    pub events_applied: usize,
    pub source_failovers: usize,
    pub completed_migrations: usize,
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

            match event.action {
                FaultAction::SetNodeHealth { node_id, health } => {
                    let _ = scheduler.set_node_health(node_id, health);
                }
            }

            report.events_applied += 1;
            next_event += 1;
        }

        let tick_report = scheduler.tick();
        report.ticks_executed = tick + 1;
        report.source_failovers += tick_report.source_failovers;
        report.completed_migrations += tick_report.completed;
        report.bytes_copied += tick_report.bytes_copied;

        if next_event == trace.events.len() && scheduler.is_converged() {
            break;
        }
    }

    report.converged = scheduler.is_converged();
    report.remaining_bytes = scheduler.remaining_bytes();
    report
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
