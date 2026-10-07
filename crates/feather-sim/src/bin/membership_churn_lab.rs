use feather_sim::{MemberStatus, MembershipCluster, MembershipConfig, MessageBusLimits};

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChurnResult {
    join_converged_tick: u64,
    leave_converged_tick: u64,
    rejoin_converged_tick: u64,
    final_digest: u64,
    joined_nodes: usize,
    peak_buffered_messages: usize,
    join_requests_sent: u64,
    join_responses_received: u64,
    graceful_leaves_sent: u64,
    backpressured_messages: u64,
}

fn config() -> MembershipConfig {
    MembershipConfig {
        probe_interval_ticks: 1,
        direct_timeout_ticks: 2,
        indirect_timeout_ticks: 4,
        suspicion_timeout_ticks: 8,
        indirect_checks: 3,
        max_awareness_score: 8,
        piggyback_updates: 8,
        update_retransmits: 16,
    }
}

fn run_campaign() -> ChurnResult {
    let mut cluster = MembershipCluster::new(
        &(1..=20).collect::<Vec<_>>(),
        config(),
        MessageBusLimits {
            max_buffered_messages: 500_000,
            max_buffered_bytes: 128 * 1024 * 1024,
        },
    )
    .expect("valid membership cluster");

    for node_id in 21..=100 {
        let seed = 1 + (node_id % 20);
        assert!(cluster.add_joining_node(node_id, seed));
        if node_id % 10 == 0 {
            cluster.network_mut().partition(node_id, seed, true);
        }
    }

    let mut peak_buffered_messages = 0_usize;
    for _ in 0..40 {
        cluster.tick();
        peak_buffered_messages = peak_buffered_messages.max(cluster.buffered_message_count());
    }

    for node_id in (30..=100).step_by(10) {
        let seed = 1 + (node_id % 20);
        cluster.network_mut().heal(node_id, seed, true);
    }

    let join_converged_tick = loop {
        cluster.tick();
        peak_buffered_messages = peak_buffered_messages.max(cluster.buffered_message_count());
        if cluster.joined_node_count() == 100
            && (1..=100).all(|subject| cluster.all_live_observers_see(subject, MemberStatus::Alive))
        {
            break cluster.now();
        }
        assert!(cluster.now() < 1_000, "join phase failed to converge");
    };

    for node_id in 41..=60 {
        assert!(cluster.graceful_leave(node_id));
    }

    let leave_converged_tick = loop {
        cluster.tick();
        peak_buffered_messages = peak_buffered_messages.max(cluster.buffered_message_count());
        if cluster.joined_node_count() == 80
            && (41..=60).all(|subject| cluster.all_live_observers_see(subject, MemberStatus::Left))
        {
            break cluster.now();
        }
        assert!(cluster.now() < join_converged_tick + 1_000);
    };

    for node_id in 41..=50 {
        let seed = 1 + (node_id % 20);
        assert!(cluster.rejoin(node_id, seed));
        if node_id % 3 == 0 {
            cluster.network_mut().partition(node_id, seed, true);
        }
    }

    for _ in 0..40 {
        cluster.tick();
        peak_buffered_messages = peak_buffered_messages.max(cluster.buffered_message_count());
    }

    for node_id in 41..=50 {
        let seed = 1 + (node_id % 20);
        cluster.network_mut().heal(node_id, seed, true);
    }

    let rejoin_converged_tick = loop {
        cluster.tick();
        peak_buffered_messages = peak_buffered_messages.max(cluster.buffered_message_count());

        let alive = (1..=40)
            .chain(41..=50)
            .chain(61..=100)
            .all(|subject| cluster.all_live_observers_see(subject, MemberStatus::Alive));
        let left =
            (51..=60).all(|subject| cluster.all_live_observers_see(subject, MemberStatus::Left));

        if cluster.joined_node_count() == 90 && alive && left {
            break cluster.now();
        }
        assert!(cluster.now() < leave_converged_tick + 1_500);
    };

    // Let accepted messages drain so the final digest is taken from a quiescent view.
    cluster.run_ticks(20);
    peak_buffered_messages = peak_buffered_messages.max(cluster.buffered_message_count());

    let mut digest = 0xcbf2_9ce4_8422_2325_u64;
    let mut join_requests_sent = 0_u64;
    let mut join_responses_received = 0_u64;
    let mut graceful_leaves_sent = 0_u64;
    let mut backpressured_messages = 0_u64;

    for observer in 1..=100 {
        let node = cluster.node(observer).expect("node");
        let stats = node.stats();
        join_requests_sent += stats.join_requests_sent;
        join_responses_received += stats.join_responses_received;
        graceful_leaves_sent += stats.graceful_leaves_sent;
        backpressured_messages += stats.backpressured_messages;

        for subject in 1..=100 {
            if let Some(state) = cluster.view(observer, subject) {
                for value in [observer, subject, state.incarnation, state.status as u64] {
                    for byte in value.to_le_bytes() {
                        digest ^= u64::from(byte);
                        digest = digest.wrapping_mul(0x0000_0100_0000_01B3);
                    }
                }
            }
        }
    }

    ChurnResult {
        join_converged_tick,
        leave_converged_tick,
        rejoin_converged_tick,
        final_digest: digest,
        joined_nodes: cluster.joined_node_count(),
        peak_buffered_messages,
        join_requests_sent,
        join_responses_received,
        graceful_leaves_sent,
        backpressured_messages,
    }
}

fn main() {
    let a = run_campaign();
    let b = run_campaign();
    assert_eq!(a, b);

    println!(
        "membership-churn nodes_final={} join_converged_tick={} leave_converged_tick={} rejoin_converged_tick={} digest={}",
        a.joined_nodes,
        a.join_converged_tick,
        a.leave_converged_tick,
        a.rejoin_converged_tick,
        a.final_digest
    );
    println!(
        "membership-churn join_requests={} join_responses={} graceful_leaves={} peak_buffered_messages={} backpressure={}",
        a.join_requests_sent,
        a.join_responses_received,
        a.graceful_leaves_sent,
        a.peak_buffered_messages,
        a.backpressured_messages
    );
}
