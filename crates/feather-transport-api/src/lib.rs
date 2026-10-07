#![forbid(unsafe_code)]

pub type NodeId = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferRequest {
    pub task_id: u64,
    pub chunk_offset: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferSubmit {
    Delivered { bytes: u64, duplicates: u32 },
    InFlight,
    Dropped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferPoll {
    Pending,
    Delivered { bytes: u64, duplicates: u32 },
    Dropped,
}

pub trait MigrationTransport {
    fn submit(&mut self, now_tick: u64, request: TransferRequest) -> TransferSubmit;
    fn poll(&mut self, now_tick: u64, task_id: u64) -> TransferPoll;
    fn cancel(&mut self, task_id: u64);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum MessageClass {
    Membership,
    Gossip,
    Control,
    Data,
    Repair,
    Client,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageEnvelope {
    pub message_id: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub class: MessageClass,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageSubmit {
    Accepted {
        message_id: u64,
    },
    Dropped {
        message_id: u64,
    },
    Backpressure {
        message_id: u64,
        required_messages: usize,
        required_bytes: usize,
    },
}

pub trait MessageSink {
    fn submit_message(
        &mut self,
        now_tick: u64,
        from: NodeId,
        to: NodeId,
        class: MessageClass,
        payload: Vec<u8>,
    ) -> MessageSubmit;
}
