use feather_sim::{
    ResizeBlockReason, ResizeCommitOutcome, ResizeDecision, TabletResizePolicy, TabletResizeState,
};

fn run_growth(name: &str, metadata_budget: u64) {
    let policy =
        TabletResizePolicy::research_hysteresis(100, 0, 16, metadata_budget).expect("valid policy");
    let mut state = TabletResizeState::new(1).expect("valid initial state");
    let total_bytes = 10_000_u64;
    let topology_epoch = 7_u64;
    let mut tick = 0_u64;
    let mut commits = 0_u64;

    loop {
        match state
            .evaluate(total_bytes, tick, topology_epoch, &policy)
            .expect("valid resize evaluation")
        {
            ResizeDecision::NoChange => {
                println!(
                    "{name},stable,count={},generation={},metadata_bytes={},commits={}",
                    state.tablet_count,
                    state.generation,
                    state.estimated_metadata_bytes(&policy),
                    commits
                );
                break;
            }
            ResizeDecision::Blocked { kind, reason } => {
                println!(
                    "{name},blocked,kind={kind:?},reason={reason:?},count={},generation={},metadata_bytes={},commits={}",
                    state.tablet_count,
                    state.generation,
                    state.estimated_metadata_bytes(&policy),
                    commits
                );
                break;
            }
            ResizeDecision::Planned(plan) => {
                let outcome = state.commit(plan, topology_epoch, tick);
                assert_eq!(outcome, ResizeCommitOutcome::Applied);
                commits += 1;
                tick += 1;
            }
        }
    }
}

fn run_cooldown() {
    let policy =
        TabletResizePolicy::research_hysteresis(100, 10, 16, 16 * 64).expect("valid policy");
    let mut state = TabletResizeState::new(8).expect("valid initial state");

    let split = match state.evaluate(1_601, 100, 9, &policy).unwrap() {
        ResizeDecision::Planned(plan) => plan,
        other => panic!("expected split plan, got {other:?}"),
    };
    assert_eq!(state.commit(split, 9, 100), ResizeCommitOutcome::Applied);

    let decision = state.evaluate(100, 101, 9, &policy).unwrap();
    assert_eq!(
        decision,
        ResizeDecision::Blocked {
            kind: feather_sim::ResizeKind::Merge,
            reason: ResizeBlockReason::Cooldown { remaining_ticks: 9 },
        }
    );

    println!(
        "cooldown,decision={decision:?},count={},generation={}",
        state.tablet_count, state.generation
    );
}

fn main() {
    println!("tablet-resize-lab target_bytes=100 total_growth_bytes=10000 metadata_per_tablet=16");
    run_growth("budget-64-tablets", 16 * 64);
    run_growth("budget-32-tablets", 16 * 32);
    run_cooldown();
}
