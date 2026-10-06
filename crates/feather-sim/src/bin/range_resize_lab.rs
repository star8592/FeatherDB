use feather_sim::{
    HASH_SPACE_END, LifecycleResizeDecision, RangeCommitOutcome, RangeTabletMap, ResizeBlockReason,
    ResizeKind, TabletRangeLifecycle, TabletResizePolicy,
};

fn policy() -> TabletResizePolicy {
    TabletResizePolicy::research_hysteresis(100, 0, 16, 16 * 64).expect("valid policy")
}

fn main() {
    let topology_epoch = 11_u64;
    let map = RangeTabletMap::single(10, 10_000, vec![1, 2, 3]).expect("valid initial range map");
    let mut lifecycle = TabletRangeLifecycle::new(map).expect("valid lifecycle");
    let mut tick = 0_u64;

    println!(
        "range-resize-lab,start,count={},generation={},bytes={},next_id={}",
        lifecycle.map().tablet_count(),
        lifecycle.map().generation(),
        lifecycle.map().total_bytes(),
        lifecycle.map().next_tablet_id()
    );

    loop {
        match lifecycle
            .evaluate(10_000, tick, topology_epoch, &policy())
            .expect("resize evaluation")
        {
            LifecycleResizeDecision::NoChange => break,
            LifecycleResizeDecision::Blocked { kind, reason } => {
                panic!("unexpected growth block: {kind:?} {reason:?}")
            }
            LifecycleResizeDecision::Planned(plan) => {
                assert_eq!(plan.kind(), ResizeKind::Split);
                assert_eq!(
                    lifecycle
                        .commit(&plan, topology_epoch, tick)
                        .expect("split commit"),
                    RangeCommitOutcome::Applied
                );
                tick += 1;
            }
        }
    }

    assert_eq!(lifecycle.map().tablet_count(), 64);
    assert_eq!(lifecycle.map().total_bytes(), 10_000);
    assert_eq!(lifecycle.map().tablets().first().unwrap().start, 0);
    assert_eq!(
        lifecycle.map().tablets().last().unwrap().end,
        HASH_SPACE_END
    );

    println!(
        "after-growth,count={},generation={},bytes={},next_id={}",
        lifecycle.map().tablet_count(),
        lifecycle.map().generation(),
        lifecycle.map().total_bytes(),
        lifecycle.map().next_tablet_id()
    );

    for token in [0_u64, 1, u64::MAX / 4, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
        assert!(lifecycle.map().route(token).is_some());
    }

    loop {
        match lifecycle
            .evaluate(1, tick, topology_epoch, &policy())
            .expect("merge evaluation")
        {
            LifecycleResizeDecision::NoChange => break,
            LifecycleResizeDecision::Blocked { kind, reason } => {
                if kind == ResizeKind::Merge && reason == ResizeBlockReason::MinTabletCount {
                    break;
                }
                panic!("unexpected shrink block: {kind:?} {reason:?}")
            }
            LifecycleResizeDecision::Planned(plan) => {
                assert_eq!(plan.kind(), ResizeKind::Merge);
                assert_eq!(
                    lifecycle
                        .commit(&plan, topology_epoch, tick)
                        .expect("merge commit"),
                    RangeCommitOutcome::Applied
                );
                tick += 1;
            }
        }
    }

    println!(
        "after-shrink,count={},generation={},bytes={},range=[{},{}),replicas={:?},next_id={}",
        lifecycle.map().tablet_count(),
        lifecycle.map().generation(),
        lifecycle.map().total_bytes(),
        lifecycle.map().tablets()[0].start,
        lifecycle.map().tablets()[0].end,
        lifecycle.map().tablets()[0].replicas,
        lifecycle.map().next_tablet_id()
    );

    assert_eq!(lifecycle.map().tablet_count(), 1);
    assert_eq!(lifecycle.map().tablets()[0].start, 0);
    assert_eq!(lifecycle.map().tablets()[0].end, HASH_SPACE_END);
}
