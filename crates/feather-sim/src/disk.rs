use std::collections::BTreeMap;

pub use feather_storage_api::{
    DiskCompletion, DiskError, DiskOpId, DiskPoll, DiskRequest, DiskSubmit, DurableStore,
};

#[derive(Clone, Debug, Default)]
struct MemoryDiskCore {
    durable: BTreeMap<Vec<u8>, Vec<u8>>,
    visible: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryDiskCore {
    fn execute(&mut self, request: DiskRequest) -> DiskCompletion {
        match request {
            DiskRequest::Read { key, .. } => DiskCompletion::Read(self.visible.get(&key).cloned()),
            DiskRequest::Put { key, value, .. } => {
                self.visible.insert(key, value);
                DiskCompletion::Unit
            }
            DiskRequest::Delete { key, .. } => {
                self.visible.remove(&key);
                DiskCompletion::Unit
            }
            DiskRequest::Sync { .. } => {
                self.durable = self.visible.clone();
                DiskCompletion::Unit
            }
        }
    }

    fn crash(&mut self) {
        self.visible = self.durable.clone();
    }

    fn durable_bytes(&self) -> usize {
        self.durable
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()))
            .sum()
    }
}

#[derive(Clone, Debug, Default)]
pub struct DirectMemoryStore {
    core: MemoryDiskCore,
}

impl DirectMemoryStore {
    pub fn durable_bytes(&self) -> usize {
        self.core.durable_bytes()
    }
}

impl DurableStore for DirectMemoryStore {
    fn submit(&mut self, _now_tick: u64, request: DiskRequest) -> DiskSubmit {
        DiskSubmit::Completed(self.core.execute(request))
    }

    fn poll(&mut self, _now_tick: u64, _op_id: DiskOpId) -> DiskPoll {
        DiskPoll::Failed(DiskError::Cancelled)
    }

    fn crash(&mut self) {
        self.core.crash();
    }
}

#[derive(Clone, Debug)]
struct PendingDiskOp {
    complete_at: u64,
    request: DiskRequest,
}

#[derive(Clone, Debug, Default)]
pub struct SimDisk {
    core: MemoryDiskCore,
    pending: BTreeMap<DiskOpId, PendingDiskOp>,
    delay_ticks: u64,
    full: bool,
    fail_next: u64,
    corrupt_next_read: u64,
    corrupt_next_write: u64,
}

impl SimDisk {
    pub fn set_delay(&mut self, ticks: u64) {
        self.delay_ticks = ticks;
    }

    pub fn set_full(&mut self, full: bool) {
        self.full = full;
    }

    pub fn fail_next(&mut self, count: u64) {
        self.fail_next = count;
    }

    pub fn corrupt_next_read(&mut self, count: u64) {
        self.corrupt_next_read = count;
    }

    pub fn corrupt_next_write(&mut self, count: u64) {
        self.corrupt_next_write = count;
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn durable_bytes(&self) -> usize {
        self.core.durable_bytes()
    }

    fn execute_faultable(&mut self, request: DiskRequest) -> DiskSubmit {
        if self.fail_next > 0 {
            self.fail_next -= 1;
            return DiskSubmit::Failed(DiskError::Io);
        }

        if self.full && matches!(request, DiskRequest::Put { .. }) {
            return DiskSubmit::Failed(DiskError::Full);
        }

        let request = match request {
            DiskRequest::Put {
                op_id,
                key,
                mut value,
            } if self.corrupt_next_write > 0 => {
                self.corrupt_next_write -= 1;
                corrupt_bytes(&mut value);
                DiskRequest::Put { op_id, key, value }
            }
            other => other,
        };

        let mut completion = self.core.execute(request);
        if self.corrupt_next_read > 0
            && let DiskCompletion::Read(Some(value)) = &mut completion
        {
            self.corrupt_next_read -= 1;
            corrupt_bytes(value);
        }
        DiskSubmit::Completed(completion)
    }
}

impl DurableStore for SimDisk {
    fn submit(&mut self, now_tick: u64, request: DiskRequest) -> DiskSubmit {
        let op_id = request.op_id();
        if self.pending.contains_key(&op_id) {
            return DiskSubmit::Pending;
        }
        if self.delay_ticks == 0 {
            return self.execute_faultable(request);
        }
        self.pending.insert(
            op_id,
            PendingDiskOp {
                complete_at: now_tick.saturating_add(self.delay_ticks),
                request,
            },
        );
        DiskSubmit::Pending
    }

    fn poll(&mut self, now_tick: u64, op_id: DiskOpId) -> DiskPoll {
        let Some(pending) = self.pending.get(&op_id) else {
            return DiskPoll::Failed(DiskError::Cancelled);
        };
        if now_tick < pending.complete_at {
            return DiskPoll::Pending;
        }
        let pending = self.pending.remove(&op_id).expect("pending op exists");
        match self.execute_faultable(pending.request) {
            DiskSubmit::Completed(completion) => DiskPoll::Completed(completion),
            DiskSubmit::Failed(error) => DiskPoll::Failed(error),
            DiskSubmit::Pending => unreachable!("fault execution cannot requeue"),
        }
    }

    fn crash(&mut self) {
        self.pending.clear();
        self.core.crash();
    }
}

fn corrupt_bytes(value: &mut [u8]) {
    if let Some(first) = value.first_mut() {
        *first ^= 0x80;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlRecord {
    pub topology_epoch: u64,
    pub catalog_generation: u64,
}

impl ControlRecord {
    const MAGIC: [u8; 4] = *b"FCR1";

    pub fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(28);
        bytes.extend_from_slice(&Self::MAGIC);
        bytes.extend_from_slice(&self.topology_epoch.to_le_bytes());
        bytes.extend_from_slice(&self.catalog_generation.to_le_bytes());
        let checksum = checksum64(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DiskError> {
        if bytes.len() != 28 || bytes[..4] != Self::MAGIC {
            return Err(DiskError::Corrupt);
        }
        let expected =
            u64::from_le_bytes(bytes[20..28].try_into().map_err(|_| DiskError::Corrupt)?);
        if checksum64(&bytes[..20]) != expected {
            return Err(DiskError::Corrupt);
        }
        Ok(Self {
            topology_epoch: u64::from_le_bytes(
                bytes[4..12].try_into().map_err(|_| DiskError::Corrupt)?,
            ),
            catalog_generation: u64::from_le_bytes(
                bytes[12..20].try_into().map_err(|_| DiskError::Corrupt)?,
            ),
        })
    }
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlCommitState {
    Idle,
    PutPending,
    SyncPending,
    Complete,
    Failed(DiskError),
}

#[derive(Clone, Debug)]
pub struct DurableControlWriter {
    key: Vec<u8>,
    record: ControlRecord,
    next_op_id: DiskOpId,
    active_op: Option<DiskOpId>,
    state: ControlCommitState,
}

impl DurableControlWriter {
    pub fn new(record: ControlRecord) -> Self {
        Self {
            key: b"control/current".to_vec(),
            record,
            next_op_id: 1,
            active_op: None,
            state: ControlCommitState::Idle,
        }
    }

    pub fn state(&self) -> ControlCommitState {
        self.state
    }

    pub fn is_committed(&self) -> bool {
        self.state == ControlCommitState::Complete
    }

    pub fn retry(&mut self) {
        if matches!(self.state, ControlCommitState::Failed(_)) {
            self.active_op = None;
            self.state = ControlCommitState::Idle;
        }
    }

    pub fn tick<S: DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        match self.state {
            ControlCommitState::Idle => self.submit_put(now_tick, store),
            ControlCommitState::PutPending => self.poll_put(now_tick, store),
            ControlCommitState::SyncPending => self.poll_sync(now_tick, store),
            ControlCommitState::Complete | ControlCommitState::Failed(_) => {}
        }
    }

    fn alloc_op_id(&mut self) -> DiskOpId {
        let id = self.next_op_id;
        self.next_op_id = self.next_op_id.saturating_add(1);
        id
    }

    fn submit_put<S: DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let op_id = self.alloc_op_id();
        match store.submit(
            now_tick,
            DiskRequest::Put {
                op_id,
                key: self.key.clone(),
                value: self.record.encode(),
            },
        ) {
            DiskSubmit::Completed(_) => self.submit_sync(now_tick, store),
            DiskSubmit::Pending => {
                self.active_op = Some(op_id);
                self.state = ControlCommitState::PutPending;
            }
            DiskSubmit::Failed(error) => self.state = ControlCommitState::Failed(error),
        }
    }

    fn poll_put<S: DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let Some(op_id) = self.active_op else {
            self.state = ControlCommitState::Failed(DiskError::Cancelled);
            return;
        };
        match store.poll(now_tick, op_id) {
            DiskPoll::Pending => {}
            DiskPoll::Completed(_) => {
                self.active_op = None;
                self.submit_sync(now_tick, store);
            }
            DiskPoll::Failed(error) => {
                self.active_op = None;
                self.state = ControlCommitState::Failed(error);
            }
        }
    }

    fn submit_sync<S: DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let op_id = self.alloc_op_id();
        match store.submit(now_tick, DiskRequest::Sync { op_id }) {
            DiskSubmit::Completed(_) => self.state = ControlCommitState::Complete,
            DiskSubmit::Pending => {
                self.active_op = Some(op_id);
                self.state = ControlCommitState::SyncPending;
            }
            DiskSubmit::Failed(error) => self.state = ControlCommitState::Failed(error),
        }
    }

    fn poll_sync<S: DurableStore>(&mut self, now_tick: u64, store: &mut S) {
        let Some(op_id) = self.active_op else {
            self.state = ControlCommitState::Failed(DiskError::Cancelled);
            return;
        };
        match store.poll(now_tick, op_id) {
            DiskPoll::Pending => {}
            DiskPoll::Completed(_) => {
                self.active_op = None;
                self.state = ControlCommitState::Complete;
            }
            DiskPoll::Failed(error) => {
                self.active_op = None;
                self.state = ControlCommitState::Failed(error);
            }
        }
    }
}

pub fn read_control_record<S: DurableStore>(
    now_tick: u64,
    op_id: DiskOpId,
    store: &mut S,
) -> Result<Option<ControlRecord>, DiskError> {
    match store.submit(
        now_tick,
        DiskRequest::Read {
            op_id,
            key: b"control/current".to_vec(),
        },
    ) {
        DiskSubmit::Completed(DiskCompletion::Read(value)) => {
            value.map(|bytes| ControlRecord::decode(&bytes)).transpose()
        }
        DiskSubmit::Completed(DiskCompletion::Unit) => Err(DiskError::Io),
        DiskSubmit::Pending => Err(DiskError::Cancelled),
        DiskSubmit::Failed(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsynced_put_is_lost_on_crash() {
        let mut disk = DirectMemoryStore::default();
        assert!(matches!(
            disk.submit(
                0,
                DiskRequest::Put {
                    op_id: 1,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }
            ),
            DiskSubmit::Completed(DiskCompletion::Unit)
        ));
        disk.crash();
        assert_eq!(
            disk.submit(
                1,
                DiskRequest::Read {
                    op_id: 2,
                    key: b"k".to_vec(),
                }
            ),
            DiskSubmit::Completed(DiskCompletion::Read(None))
        );
    }

    #[test]
    fn sync_makes_put_survive_crash() {
        let mut disk = DirectMemoryStore::default();
        disk.submit(
            0,
            DiskRequest::Put {
                op_id: 1,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        );
        disk.submit(0, DiskRequest::Sync { op_id: 2 });
        disk.crash();
        assert_eq!(
            disk.submit(
                1,
                DiskRequest::Read {
                    op_id: 3,
                    key: b"k".to_vec(),
                }
            ),
            DiskSubmit::Completed(DiskCompletion::Read(Some(b"v".to_vec())))
        );
    }

    #[test]
    fn slow_io_completes_only_after_virtual_deadline() {
        let mut disk = SimDisk::default();
        disk.set_delay(3);
        assert_eq!(
            disk.submit(
                10,
                DiskRequest::Put {
                    op_id: 7,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }
            ),
            DiskSubmit::Pending
        );
        assert_eq!(disk.poll(12, 7), DiskPoll::Pending);
        assert_eq!(disk.poll(13, 7), DiskPoll::Completed(DiskCompletion::Unit));
    }

    #[test]
    fn disk_full_rejects_put_without_mutating_visible_state() {
        let mut disk = SimDisk::default();
        disk.set_full(true);
        assert_eq!(
            disk.submit(
                0,
                DiskRequest::Put {
                    op_id: 1,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }
            ),
            DiskSubmit::Failed(DiskError::Full)
        );
        disk.set_full(false);
        assert_eq!(
            disk.submit(
                0,
                DiskRequest::Read {
                    op_id: 2,
                    key: b"k".to_vec(),
                }
            ),
            DiskSubmit::Completed(DiskCompletion::Read(None))
        );
    }

    #[test]
    fn crash_cancels_inflight_io() {
        let mut disk = SimDisk::default();
        disk.set_delay(10);
        assert_eq!(
            disk.submit(
                0,
                DiskRequest::Put {
                    op_id: 1,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }
            ),
            DiskSubmit::Pending
        );
        assert_eq!(disk.pending_count(), 1);
        disk.crash();
        assert_eq!(disk.pending_count(), 0);
        assert_eq!(disk.poll(20, 1), DiskPoll::Failed(DiskError::Cancelled));
    }

    #[test]
    fn corrupt_read_is_detected_by_control_record_checksum() {
        let record = ControlRecord {
            topology_epoch: 11,
            catalog_generation: 7,
        };
        let mut disk = SimDisk::default();
        let mut writer = DurableControlWriter::new(record);
        writer.tick(0, &mut disk);
        assert!(writer.is_committed());
        disk.corrupt_next_read(1);
        assert_eq!(
            read_control_record(1, 99, &mut disk),
            Err(DiskError::Corrupt)
        );
    }

    #[test]
    fn disk_full_commit_retries_idempotently_after_recovery() {
        let record = ControlRecord {
            topology_epoch: 22,
            catalog_generation: 3,
        };
        let mut disk = SimDisk::default();
        disk.set_full(true);
        let mut writer = DurableControlWriter::new(record);
        writer.tick(0, &mut disk);
        assert_eq!(writer.state(), ControlCommitState::Failed(DiskError::Full));
        assert_eq!(disk.durable_bytes(), 0);

        disk.set_full(false);
        writer.retry();
        writer.tick(1, &mut disk);
        assert!(writer.is_committed());
        disk.crash();
        assert_eq!(read_control_record(2, 99, &mut disk), Ok(Some(record)));
    }

    #[test]
    fn crash_between_put_and_sync_does_not_publish_control_record() {
        let record = ControlRecord {
            topology_epoch: 5,
            catalog_generation: 9,
        };
        let mut disk = SimDisk::default();
        disk.set_delay(2);
        let mut writer = DurableControlWriter::new(record);
        writer.tick(0, &mut disk);
        assert_eq!(writer.state(), ControlCommitState::PutPending);
        writer.tick(2, &mut disk);
        assert_eq!(writer.state(), ControlCommitState::SyncPending);

        disk.crash();
        writer.tick(4, &mut disk);
        assert_eq!(
            writer.state(),
            ControlCommitState::Failed(DiskError::Cancelled)
        );

        disk.set_delay(0);
        writer.retry();
        writer.tick(5, &mut disk);
        assert!(writer.is_committed());
        disk.crash();
        assert_eq!(read_control_record(6, 88, &mut disk), Ok(Some(record)));
    }

    #[test]
    fn corrupt_write_is_latent_until_checked_after_sync() {
        let record = ControlRecord {
            topology_epoch: 8,
            catalog_generation: 13,
        };
        let mut disk = SimDisk::default();
        disk.corrupt_next_write(1);
        let mut writer = DurableControlWriter::new(record);
        writer.tick(0, &mut disk);
        assert!(writer.is_committed());
        disk.crash();
        assert_eq!(
            read_control_record(1, 100, &mut disk),
            Err(DiskError::Corrupt)
        );
    }
}
