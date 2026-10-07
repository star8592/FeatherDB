use feather_sim::{
    FaultTrace, MemberStatus, MembershipCluster, MembershipConfig, MessageBusLimits,
    apply_network_fault_action,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct CampaignResult {
    dead_converged_tick: Option<u64>,
    alive_converged_tick: Option<u64>,
    final_digest: u64,
    total_direct_probes: u64,
    total_indirect_rounds: u64,
    total_suspects: u64,
    total_dead: u64,
    total_refutations: u64,
    total_updates: u64,
    total_backpressure: u64,
    peak_awareness: u8,
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

fn run_campaign(trace: &FaultTrace) -> CampaignResult {
    let ids: Vec<_> = (1..=100).collect();
    let mut cluster = MembershipCluster::new(
        &ids,
        config(),
        MessageBusLimits {
            max_buffered_messages: 500_000,
            max_buffered_bytes: 128 * 1024 * 1024,
        },
    )
    .expect("valid membership cluster");

    let crash_tick = 120_u64;
    let restart_after_dead_tick = 900_u64;
    let subject = 100_u64;
    let max_tick = trace.last_tick().max(1_200) + 1_500;
    let mut next_fault = 0_usize;
    let mut dead_converged_tick = None;
    let mut alive_converged_tick = None;
    let mut restarted = false;
    let mut peak_awareness = 0_u8;

    for _ in 0..max_tick {
        let now = cluster.now();

        while let Some(event) = trace.events().get(next_fault) {
            if event.tick != now {
                break;
            }
            let _ = apply_network_fault_action(cluster.network_mut(), event.action);
            next_fault += 1;
        }

        if now == crash_tick {
            assert!(cluster.crash(subject));
        }

        if dead_converged_tick.is_none()
            && cluster.all_live_observers_see(subject, MemberStatus::Dead)
        {
            dead_converged_tick = Some(now);
        }

        if !restarted && now >= restart_after_dead_tick && dead_converged_tick.is_some() {
            assert!(cluster.restart(subject));
            restarted = true;
        }

        cluster.tick();

        for node_id in &ids {
            peak_awareness = peak_awareness.max(
                cluster
                    .node(*node_id)
                    .expect("membership node")
                    .awareness_score(),
            );
        }

        if restarted {
            let self_incarnation = cluster
                .view(subject, subject)
                .expect("subject self view")
                .incarnation;
            let subject_recovered = ids.iter().all(|observer| {
                cluster.view(*observer, subject).is_some_and(|state| {
                    state.status == MemberStatus::Alive && state.incarnation == self_incarnation
                })
            });
            let globally_alive = ids.iter().all(|observer| {
                ids.iter().all(|subject_id| {
                    cluster
                        .view(*observer, *subject_id)
                        .is_some_and(|state| state.status == MemberStatus::Alive)
                })
            });
            if subject_recovered && globally_alive && next_fault == trace.events().len() {
                alive_converged_tick = Some(cluster.now());
                break;
            }
        }
    }

    assert_eq!(next_fault, trace.events().len());
    assert!(
        dead_converged_tick.is_some(),
        "crashed node never converged Dead"
    );
    assert!(
        alive_converged_tick.is_some(),
        "restarted node never converged Alive"
    );

    let mut digest = 0xcbf2_9ce4_8422_2325_u64;
    let mut total_direct_probes = 0_u64;
    let mut total_indirect_rounds = 0_u64;
    let mut total_suspects = 0_u64;
    let mut total_dead = 0_u64;
    let mut total_refutations = 0_u64;
    let mut total_updates = 0_u64;
    let mut total_backpressure = 0_u64;

    for observer in &ids {
        let node = cluster.node(*observer).expect("node");
        let stats = node.stats();
        total_direct_probes += stats.direct_probes_started;
        total_indirect_rounds += stats.indirect_probe_rounds;
        total_suspects += stats.suspects_created;
        total_dead += stats.dead_created;
        total_refutations += stats.self_refutations;
        total_updates += stats.updates_applied;
        total_backpressure += stats.backpressured_messages;

        for subject_id in &ids {
            let state = cluster.view(*observer, *subject_id).expect("member state");
            for value in [
                *observer,
                *subject_id,
                state.incarnation,
                state.status as u64,
            ] {
                for byte in value.to_le_bytes() {
                    digest ^= u64::from(byte);
                    digest = digest.wrapping_mul(0x0000_0100_0000_01B3);
                }
            }
        }
    }

    CampaignResult {
        dead_converged_tick,
        alive_converged_tick,
        final_digest: digest,
        total_direct_probes,
        total_indirect_rounds,
        total_suspects,
        total_dead,
        total_refutations,
        total_updates,
        total_backpressure,
        peak_awareness,
    }
}

fn main() {
    let ids: Vec<_> = (1_u64..=100).collect();
    let links: Vec<_> = ids
        .iter()
        .flat_map(|from| {
            ids.iter()
                .filter(move |to| *to != from)
                .map(move |to| (*from, *to))
        })
        .collect();

    let trace = FaultTrace::generate_network_faults_on_links(8592, &links, 1_200, 2, 6, 6);

    let a = run_campaign(&trace);
    let b = run_campaign(&trace);
    assert_eq!(a, b);

    println!(
        "membership-lab seed={} nodes=100 fault_events={} last_fault_tick={} dead_converged_tick={} alive_converged_tick={} digest={}",
        trace.seed,
        trace.events().len(),
        trace.last_tick(),
        a.dead_converged_tick.unwrap(),
        a.alive_converged_tick.unwrap(),
        a.final_digest,
    );
    println!(
        "protocol direct_probes={} indirect_rounds={} suspects={} dead_updates={} refutations={} updates_applied={} peak_awareness={} backpressure={}",
        a.total_direct_probes,
        a.total_indirect_rounds,
        a.total_suspects,
        a.total_dead,
        a.total_refutations,
        a.total_updates,
        a.peak_awareness,
        a.total_backpressure,
    );
}
