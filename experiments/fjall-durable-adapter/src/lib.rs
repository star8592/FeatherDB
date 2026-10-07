#![forbid(unsafe_code)]

#[cfg(test)]
use std::path::PathBuf;

#[cfg(test)]
use feather_storage_api::{DiskCompletion, DiskRequest, DiskSubmit, DurableStore};
pub use feather_storage_fjall::FjallDurableStore;

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "feather-fjall-adapter-{name}-{}-{}",
            std::process::id(),
            NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn put(store: &mut FjallDurableStore, op_id: u64, key: &[u8], value: &[u8]) {
        assert_eq!(
            store.submit(
                0,
                DiskRequest::Put {
                    op_id,
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
            ),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
    }

    fn delete(store: &mut FjallDurableStore, op_id: u64, key: &[u8]) {
        assert_eq!(
            store.submit(
                0,
                DiskRequest::Delete {
                    op_id,
                    key: key.to_vec(),
                },
            ),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
    }

    fn sync(store: &mut FjallDurableStore, op_id: u64) {
        assert_eq!(
            store.submit(0, DiskRequest::Sync { op_id }),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
    }

    fn read(store: &mut FjallDurableStore, op_id: u64, key: &[u8]) -> Option<Vec<u8>> {
        match store.submit(
            0,
            DiskRequest::Read {
                op_id,
                key: key.to_vec(),
            },
        ) {
            DiskSubmit::Completed(DiskCompletion::Read(value)) => value,
            other => panic!("unexpected read result: {other:?}"),
        }
    }

    #[test]
    fn unsynced_put_is_lost_after_crash_reopen() {
        let path = test_path("unsynced-put");
        let mut store = FjallDurableStore::open(&path).unwrap();

        put(&mut store, 1, b"k", b"v1");
        assert_eq!(read(&mut store, 2, b"k"), Some(b"v1".to_vec()));
        assert_eq!(store.staged_len(), 1);

        store.crash();
        store.reopen().unwrap();

        assert_eq!(read(&mut store, 3, b"k"), None);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn synced_put_survives_reopen() {
        let path = test_path("synced-put");
        let mut store = FjallDurableStore::open(&path).unwrap();

        put(&mut store, 10, b"k", b"v1");
        sync(&mut store, 11);
        assert_eq!(store.staged_len(), 0);

        store.reopen().unwrap();

        assert_eq!(read(&mut store, 12, b"k"), Some(b"v1".to_vec()));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn unsynced_delete_is_rolled_back_and_synced_delete_survives() {
        let path = test_path("delete");
        let mut store = FjallDurableStore::open(&path).unwrap();

        put(&mut store, 20, b"k", b"v1");
        sync(&mut store, 21);

        delete(&mut store, 22, b"k");
        assert_eq!(read(&mut store, 23, b"k"), None);
        store.crash();
        store.reopen().unwrap();
        assert_eq!(read(&mut store, 24, b"k"), Some(b"v1".to_vec()));

        delete(&mut store, 25, b"k");
        sync(&mut store, 26);
        store.reopen().unwrap();
        assert_eq!(read(&mut store, 27, b"k"), None);

        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod topology_protocol_tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use feather_sim::{
        DurableTopologyTxnWriter, LifecycleResizeDecision, RangeCommitOutcome, RangeTabletMap,
        TabletRangeLifecycle, TabletResizePolicy, TopologyRecovery, TopologySnapshot,
        TopologyTxnState, read_prepared_topology, recover_topology,
    };

    static NEXT_PROTOCOL_TEST_ID: AtomicU64 = AtomicU64::new(10_000);

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "feather-fjall-protocol-{name}-{}-{}",
            std::process::id(),
            NEXT_PROTOCOL_TEST_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn current_and_target() -> (TopologySnapshot, TopologySnapshot) {
        let map = RangeTabletMap::single(50_000, 1_000, vec![1, 2, 3]).unwrap();
        let current_lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let policy = TabletResizePolicy::research_hysteresis(100, 0, 24, 64 * 24).unwrap();
        let plan = match current_lifecycle.evaluate(1_000, 10, 7, &policy).unwrap() {
            LifecycleResizeDecision::Planned(plan) => plan,
            other => panic!("expected split plan, got {other:?}"),
        };

        let mut target_lifecycle = current_lifecycle.clone();
        assert_eq!(
            target_lifecycle.commit(&plan, 7, 11).unwrap(),
            RangeCommitOutcome::Applied
        );

        (
            TopologySnapshot::from_lifecycle(7, &current_lifecycle),
            TopologySnapshot::from_lifecycle(7, &target_lifecycle),
        )
    }

    fn persist_current(store: &mut FjallDurableStore, snapshot: &TopologySnapshot) {
        assert_eq!(
            store.submit(
                0,
                DiskRequest::Put {
                    op_id: 100,
                    key: b"topology/current".to_vec(),
                    value: snapshot.encode().unwrap(),
                },
            ),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
        assert_eq!(
            store.submit(0, DiskRequest::Sync { op_id: 101 }),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
    }

    #[test]
    fn prepared_current_protocol_survives_real_fjall_reopen() {
        let path = test_path("prepared-current");
        let mut store = FjallDurableStore::open(&path).unwrap();
        let (current, target) = current_and_target();
        persist_current(&mut store, &current);

        let mut writer = DurableTopologyTxnWriter::new(500, &current, target.clone()).unwrap();
        writer.tick(1, &mut store);
        assert_eq!(writer.state(), TopologyTxnState::Prepared);

        store.reopen().unwrap();
        assert_eq!(
            recover_topology(2, &mut store).unwrap(),
            TopologyRecovery::ReplayPrepared {
                txn_id: 500,
                target: target.clone(),
            }
        );

        let prepared = read_prepared_topology(3, 200, &mut store)
            .unwrap()
            .expect("prepared record");
        let mut publisher = DurableTopologyTxnWriter::resume_publish(prepared).unwrap();
        publisher.tick(4, &mut store);
        assert_eq!(publisher.state(), TopologyTxnState::Complete);

        store.reopen().unwrap();
        assert_eq!(
            recover_topology(5, &mut store).unwrap(),
            TopologyRecovery::Current(target)
        );

        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod topology_gc_tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use feather_sim::{
        DurableTopologyTxnWriter, LifecycleResizeDecision, PreparedGcState, PreparedTopologyGc,
        RangeCommitOutcome, RangeTabletMap, TabletRangeLifecycle, TabletResizePolicy,
        TopologyRecovery, TopologySnapshot, TopologyTxnError, TopologyTxnState,
        read_prepared_topology, recover_topology,
    };

    static NEXT_GC_TEST_ID: AtomicU64 = AtomicU64::new(20_000);

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "feather-fjall-gc-{name}-{}-{}",
            std::process::id(),
            NEXT_GC_TEST_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn current_and_target() -> (TopologySnapshot, TopologySnapshot) {
        let map = RangeTabletMap::single(60_000, 1_000, vec![1, 2, 3]).unwrap();
        let current_lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let policy = TabletResizePolicy::research_hysteresis(100, 0, 24, 64 * 24).unwrap();
        let plan = match current_lifecycle.evaluate(1_000, 10, 7, &policy).unwrap() {
            LifecycleResizeDecision::Planned(plan) => plan,
            other => panic!("expected split plan, got {other:?}"),
        };
        let mut target_lifecycle = current_lifecycle.clone();
        assert_eq!(
            target_lifecycle.commit(&plan, 7, 11).unwrap(),
            RangeCommitOutcome::Applied
        );
        (
            TopologySnapshot::from_lifecycle(7, &current_lifecycle),
            TopologySnapshot::from_lifecycle(7, &target_lifecycle),
        )
    }

    fn persist_current(store: &mut FjallDurableStore, snapshot: &TopologySnapshot) {
        assert_eq!(
            store.submit(
                0,
                DiskRequest::Put {
                    op_id: 300,
                    key: b"topology/current".to_vec(),
                    value: snapshot.encode().unwrap(),
                },
            ),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
        assert_eq!(
            store.submit(0, DiskRequest::Sync { op_id: 301 }),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
    }

    #[test]
    fn stale_prepared_gc_survives_real_fjall_reopen() {
        let path = test_path("stale");
        let mut store = FjallDurableStore::open(&path).unwrap();
        let (current, target) = current_and_target();
        persist_current(&mut store, &current);

        let mut writer = DurableTopologyTxnWriter::new(600, &current, target.clone()).unwrap();
        writer.tick(1, &mut store);
        assert_eq!(writer.state(), TopologyTxnState::Prepared);
        assert!(writer.mark_applied());
        writer.tick(2, &mut store);
        assert_eq!(writer.state(), TopologyTxnState::Complete);

        store.reopen().unwrap();
        assert_eq!(
            recover_topology(3, &mut store).unwrap(),
            TopologyRecovery::Current(target.clone())
        );
        assert!(
            read_prepared_topology(3, 302, &mut store)
                .unwrap()
                .is_some()
        );

        let mut gc = PreparedTopologyGc::begin(4, &mut store).unwrap();
        assert_eq!(gc.state(), PreparedGcState::DeleteIdle);
        gc.tick(4, &mut store);
        assert_eq!(gc.state(), PreparedGcState::Complete);

        store.reopen().unwrap();
        assert_eq!(read_prepared_topology(5, 303, &mut store).unwrap(), None);
        assert_eq!(
            recover_topology(5, &mut store).unwrap(),
            TopologyRecovery::Current(target)
        );

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn active_prepared_cannot_be_gc_on_real_fjall() {
        let path = test_path("active");
        let mut store = FjallDurableStore::open(&path).unwrap();
        let (current, target) = current_and_target();
        persist_current(&mut store, &current);

        let mut writer = DurableTopologyTxnWriter::new(601, &current, target).unwrap();
        writer.tick(1, &mut store);
        assert_eq!(writer.state(), TopologyTxnState::Prepared);

        assert!(matches!(
            PreparedTopologyGc::begin(2, &mut store),
            Err(TopologyTxnError::ActivePrepared { txn_id: 601 })
        ));

        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod runtime_recovery_tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use feather_sim::{
        AdminState, Cluster, CoordinatedResizeDecision, DurableResizeProgress,
        DurableResizeTransaction, FailureDomainPolicy, MigrationBudget, Node, RangeTabletMap,
        TabletRangeLifecycle, TabletResizePolicy, TabletRuntimeCoordinator, recover_tablet_runtime,
    };

    static NEXT_RUNTIME_TEST_ID: AtomicU64 = AtomicU64::new(30_000);

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "feather-fjall-runtime-{name}-{}-{}",
            std::process::id(),
            NEXT_RUNTIME_TEST_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn node(id: u64, weight: u32, zone: &str, state: AdminState) -> Node {
        Node {
            id,
            weight,
            zone: zone.into(),
            rack: "r1".into(),
            state,
        }
    }

    fn cluster() -> Cluster {
        Cluster {
            epoch: 7,
            replication_factor: 2,
            nodes: [
                node(1, 1, "a", AdminState::Active),
                node(2, 1, "b", AdminState::Removed),
                node(3, 4, "c", AdminState::Active),
                node(4, 8, "d", AdminState::Active),
            ]
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>(),
            tablets: Vec::new(),
        }
    }

    fn budget() -> MigrationBudget {
        MigrationBudget {
            max_active: 4,
            max_per_node_active: 4,
            bytes_per_tick: 10_000,
            max_bytes_per_node_per_tick: 10_000,
            max_bytes_per_task_per_tick: 10_000,
        }
    }

    fn policy() -> TabletResizePolicy {
        TabletResizePolicy {
            target_tablet_bytes: 100,
            split_above_num: 2,
            split_above_den: 1,
            merge_below_num: 1,
            merge_below_den: 2,
            cooldown_ticks: 0,
            min_tablets: 1,
            max_tablets: 64,
            metadata_bytes_per_tablet: 24,
            metadata_budget_bytes: 64 * 24,
        }
    }

    fn runtime() -> TabletRuntimeCoordinator {
        let map = RangeTabletMap::single(10_000, 1_000, vec![1, 2]).unwrap();
        let lifecycle = TabletRangeLifecycle::new(map).unwrap();
        let mut runtime = TabletRuntimeCoordinator::new(
            cluster(),
            lifecycle,
            FailureDomainPolicy::HIERARCHICAL,
            budget(),
            8,
        )
        .unwrap();

        for _ in 0..100 {
            if runtime.is_migration_converged() {
                break;
            }
            runtime.tick().unwrap();
        }
        assert!(runtime.is_migration_converged());
        runtime
    }

    fn persist_current(store: &mut FjallDurableStore, runtime: &mut TabletRuntimeCoordinator) {
        let snapshot = runtime.capture_topology_snapshot().unwrap();
        assert_eq!(
            store.submit(
                0,
                DiskRequest::Put {
                    op_id: 7000,
                    key: b"topology/current".to_vec(),
                    value: snapshot.encode().unwrap(),
                },
            ),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
        assert_eq!(
            store.submit(0, DiskRequest::Sync { op_id: 7001 }),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
    }

    #[test]
    fn durable_resize_recovers_runtime_and_migration_from_real_fjall() {
        let path = test_path("resize");
        let mut store = FjallDurableStore::open(&path).unwrap();
        let mut runtime = runtime();
        persist_current(&mut store, &mut runtime);

        let plan = match runtime.evaluate_resize(1_000, 10, 7, &policy()).unwrap() {
            CoordinatedResizeDecision::Planned(plan) => plan,
            other => panic!("expected resize plan, got {other:?}"),
        };

        let mut txn =
            DurableResizeTransaction::new(700, 0, &mut store, &mut runtime, plan, 11).unwrap();
        assert_eq!(
            txn.tick(1, &mut runtime, &mut store).unwrap(),
            DurableResizeProgress::Prepared
        );
        assert_eq!(runtime.lifecycle().map().generation(), 0);

        drop(runtime);
        store.reopen().unwrap();

        let mut recovered = recover_tablet_runtime(
            2,
            &mut store,
            FailureDomainPolicy::HIERARCHICAL,
            budget(),
            8,
        )
        .unwrap();
        assert_eq!(recovered.replayed_txn_id(), Some(700));
        assert_eq!(recovered.runtime().lifecycle().map().generation(), 1);
        assert!(!recovered.publication_complete());

        assert_eq!(
            recovered.tick_publication(3, &mut store).unwrap(),
            DurableResizeProgress::Complete
        );
        store.reopen().unwrap();

        let mut stable = recover_tablet_runtime(
            4,
            &mut store,
            FailureDomainPolicy::HIERARCHICAL,
            budget(),
            8,
        )
        .unwrap();
        assert_eq!(stable.replayed_txn_id(), None);
        assert_eq!(stable.runtime().lifecycle().map().generation(), 1);

        for _ in 0..1_000 {
            if stable.runtime().is_migration_converged() {
                break;
            }
            stable.runtime_mut().tick().unwrap();
        }
        assert!(stable.runtime().is_migration_converged());

        fs::remove_dir_all(path).unwrap();
    }
}
