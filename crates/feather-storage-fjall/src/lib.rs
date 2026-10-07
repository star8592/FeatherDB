#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use feather_storage_api::{
    DiskCompletion, DiskError, DiskOpId, DiskPoll, DiskRequest, DiskSubmit, DurableStore,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

pub struct FjallDurableStore {
    path: PathBuf,
    db: Option<Database>,
    keyspace: Option<Keyspace>,
    staged: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl FjallDurableStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let path = path.as_ref().to_path_buf();
        let (db, keyspace) = open_handles(&path)?;
        Ok(Self {
            path,
            db: Some(db),
            keyspace: Some(keyspace),
            staged: BTreeMap::new(),
        })
    }

    pub fn reopen(&mut self) -> Result<(), fjall::Error> {
        self.staged.clear();
        self.keyspace.take();
        self.db.take();
        let (db, keyspace) = open_handles(&self.path)?;
        self.db = Some(db);
        self.keyspace = Some(keyspace);
        Ok(())
    }

    pub fn staged_len(&self) -> usize {
        self.staged.len()
    }

    fn db(&self) -> &Database {
        self.db.as_ref().expect("store is open")
    }

    fn keyspace(&self) -> &Keyspace {
        self.keyspace.as_ref().expect("store is open")
    }

    fn read_visible(&self, key: &[u8]) -> Result<Option<Vec<u8>>, fjall::Error> {
        if let Some(staged) = self.staged.get(key) {
            return Ok(staged.clone());
        }
        Ok(self.keyspace().get(key)?.map(|value| value.to_vec()))
    }

    fn sync_staged(&mut self) -> Result<(), fjall::Error> {
        if self.staged.is_empty() {
            return self.db().persist(PersistMode::SyncAll);
        }

        let mut batch = self.db().batch();
        for (key, value) in &self.staged {
            match value {
                Some(value) => batch.insert(self.keyspace(), key.as_slice(), value.as_slice()),
                None => batch.remove(self.keyspace(), key.as_slice()),
            }
        }
        batch.durability(Some(PersistMode::SyncAll)).commit()?;
        self.staged.clear();
        Ok(())
    }
}

impl DurableStore for FjallDurableStore {
    fn submit(&mut self, _now_tick: u64, request: DiskRequest) -> DiskSubmit {
        match request {
            DiskRequest::Read { key, .. } => match self.read_visible(&key) {
                Ok(value) => DiskSubmit::Completed(DiskCompletion::Read(value)),
                Err(error) => DiskSubmit::Failed(map_fjall_error(&error)),
            },
            DiskRequest::Put { key, value, .. } => {
                self.staged.insert(key, Some(value));
                DiskSubmit::Completed(DiskCompletion::Unit)
            }
            DiskRequest::Delete { key, .. } => {
                self.staged.insert(key, None);
                DiskSubmit::Completed(DiskCompletion::Unit)
            }
            DiskRequest::Sync { .. } => match self.sync_staged() {
                Ok(()) => DiskSubmit::Completed(DiskCompletion::Unit),
                Err(error) => DiskSubmit::Failed(map_fjall_error(&error)),
            },
        }
    }

    fn poll(&mut self, _now_tick: u64, _op_id: DiskOpId) -> DiskPoll {
        DiskPoll::Failed(DiskError::Cancelled)
    }

    fn crash(&mut self) {
        self.staged.clear();
    }
}

fn map_fjall_error(error: &fjall::Error) -> DiskError {
    let kind = match error {
        fjall::Error::Io(error) => Some(error.kind()),
        fjall::Error::Storage(fjall::LsmError::Io(error)) => Some(error.kind()),
        _ => None,
    };
    if kind == Some(std::io::ErrorKind::StorageFull) {
        DiskError::Full
    } else {
        DiskError::Io
    }
}

fn open_handles(path: &Path) -> Result<(Database, Keyspace), fjall::Error> {
    let db = Database::builder(path).open()?;
    let keyspace = db.keyspace("control", KeyspaceCreateOptions::default)?;
    Ok((db, keyspace))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feather_storage_api::{DiskCompletion, DiskRequest, DiskSubmit, DurableStore};
    use tempfile::TempDir;

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
    fn unsynced_put_is_lost_after_reopen() {
        let dir = TempDir::new().unwrap();
        let mut store = FjallDurableStore::open(dir.path()).unwrap();
        put(&mut store, 1, b"k", b"v");
        assert_eq!(read(&mut store, 2, b"k"), Some(b"v".to_vec()));
        store.crash();
        store.reopen().unwrap();
        assert_eq!(read(&mut store, 3, b"k"), None);
    }

    #[test]
    fn synced_put_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let mut store = FjallDurableStore::open(dir.path()).unwrap();
        put(&mut store, 10, b"k", b"v");
        sync(&mut store, 11);
        store.reopen().unwrap();
        assert_eq!(read(&mut store, 12, b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn synced_delete_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let mut store = FjallDurableStore::open(dir.path()).unwrap();
        put(&mut store, 20, b"k", b"v");
        sync(&mut store, 21);
        assert_eq!(
            store.submit(
                0,
                DiskRequest::Delete {
                    op_id: 22,
                    key: b"k".to_vec(),
                },
            ),
            DiskSubmit::Completed(DiskCompletion::Unit)
        );
        sync(&mut store, 23);
        store.reopen().unwrap();
        assert_eq!(read(&mut store, 24, b"k"), None);
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

#[cfg(test)]
mod error_mapping_tests {
    use super::*;

    #[test]
    fn storage_full_is_preserved_from_fjall_io_layers() {
        let top = fjall::Error::Io(std::io::Error::from(std::io::ErrorKind::StorageFull));
        assert_eq!(map_fjall_error(&top), DiskError::Full);

        let nested = fjall::Error::Storage(fjall::LsmError::Io(std::io::Error::from(
            std::io::ErrorKind::StorageFull,
        )));
        assert_eq!(map_fjall_error(&nested), DiskError::Full);
    }

    #[test]
    fn unrelated_fjall_io_maps_to_generic_io() {
        let error = fjall::Error::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(map_fjall_error(&error), DiskError::Io);
    }
}
