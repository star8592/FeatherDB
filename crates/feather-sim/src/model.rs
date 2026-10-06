use std::collections::BTreeMap;

pub type NodeId = u64;
pub type TabletId = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminState {
    Joining,
    Active,
    Draining,
    Removed,
}

#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeId,
    pub weight: u32,
    pub zone: String,
    pub rack: String,
    pub state: AdminState,
}

impl Node {
    pub fn eligible(&self) -> bool {
        self.state == AdminState::Active && self.weight > 0
    }
}

#[derive(Clone, Debug)]
pub struct Tablet {
    pub id: TabletId,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Placement {
    pub replicas: BTreeMap<TabletId, Vec<NodeId>>,
}

#[derive(Clone, Debug)]
pub struct Cluster {
    pub epoch: u64,
    pub replication_factor: usize,
    pub nodes: BTreeMap<NodeId, Node>,
    pub tablets: Vec<Tablet>,
}

impl Cluster {
    pub fn eligible_node_count(&self) -> usize {
        self.nodes.values().filter(|node| node.eligible()).count()
    }
}
