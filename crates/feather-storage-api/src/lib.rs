#![forbid(unsafe_code)]

pub type DiskOpId = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiskError {
    Full,
    Io,
    Cancelled,
    Corrupt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiskCompletion {
    Unit,
    Read(Option<Vec<u8>>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiskSubmit {
    Completed(DiskCompletion),
    Pending,
    Failed(DiskError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiskPoll {
    Pending,
    Completed(DiskCompletion),
    Failed(DiskError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiskRequest {
    Read {
        op_id: DiskOpId,
        key: Vec<u8>,
    },
    Put {
        op_id: DiskOpId,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        op_id: DiskOpId,
        key: Vec<u8>,
    },
    Sync {
        op_id: DiskOpId,
    },
}

impl DiskRequest {
    pub fn op_id(&self) -> DiskOpId {
        match self {
            Self::Read { op_id, .. }
            | Self::Put { op_id, .. }
            | Self::Delete { op_id, .. }
            | Self::Sync { op_id } => *op_id,
        }
    }
}

pub trait DurableStore {
    fn submit(&mut self, now_tick: u64, request: DiskRequest) -> DiskSubmit;
    fn poll(&mut self, now_tick: u64, op_id: DiskOpId) -> DiskPoll;
    fn crash(&mut self);
}
