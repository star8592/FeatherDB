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
                Err(_) => DiskSubmit::Failed(DiskError::Io),
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
                Err(_) => DiskSubmit::Failed(DiskError::Io),
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
