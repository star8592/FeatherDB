#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

pub type NodeId = u64;
pub type TabletId = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminState { Joining, Active, Draining, Removed }

#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeId,
    pub weight: u32,
    pub zone: String,
    pub rack: String,
    pub state: AdminState,
}

impl Node {
    fn eligible(&self) -> bool { self.state == AdminState::Active && self.weight > 0 }
}

#[derive(Clone, Debug)]
pub struct Tablet {
    pub id: TabletId,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default)]
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
    pub fn place(&self) -> Placement {
        let mut out = Placement::default();
        for tablet in &self.tablets {
            let mut ranked: Vec<_> = self.nodes.values().filter(|n| n.eligible())
                .map(|n| (score(tablet.id, n), n)).collect();
            ranked.sort_by(|a,b| b.0.cmp(&a.0).then_with(|| a.1.id.cmp(&b.1.id)));

            let mut chosen = Vec::new();
            let mut zones = BTreeSet::new();
            let mut racks = BTreeSet::new();

            // First pass: maximize failure-domain diversity.
            for (_, n) in &ranked {
                if chosen.len() == self.replication_factor { break; }
                if !zones.contains(&n.zone) && !racks.contains(&(n.zone.clone(), n.rack.clone())) {
                    chosen.push(n.id);
                    zones.insert(n.zone.clone());
                    racks.insert((n.zone.clone(), n.rack.clone()));
                }
            }
            // Second pass: degrade explicitly when topology cannot satisfy full diversity.
            for (_, n) in &ranked {
                if chosen.len() == self.replication_factor { break; }
                if !chosen.contains(&n.id) {
                    chosen.push(n.id);
                }
            }
            out.replicas.insert(tablet.id, chosen);
        }
        out
    }
}

// Integer-only deterministic score. V0 baseline; simulator will compare/replace it
// with mathematically rigorous weighted rendezvous variants before ADR acceptance.
fn score(tablet: TabletId, node: &Node) -> u128 {
    let mut x = tablet ^ node.id.rotate_left(17);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    (x as u128 + 1) * node.weight as u128
}

pub fn moved_bytes(before: &Placement, after: &Placement, tablets: &[Tablet]) -> u64 {
    tablets.iter().map(|t| {
        let a = before.replicas.get(&t.id).map(Vec::as_slice).unwrap_or(&[]);
        let b = after.replicas.get(&t.id).map(Vec::as_slice).unwrap_or(&[]);
        b.iter().filter(|id| !a.contains(id)).count() as u64 * t.bytes
    }).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id:u64, weight:u32, zone:&str, rack:&str) -> Node {
        Node { id, weight, zone:zone.into(), rack:rack.into(), state:AdminState::Active }
    }

    #[test]
    fn deterministic() {
        let c = Cluster { epoch:1, replication_factor:2,
            nodes:[node(1,1,"a","1"),node(2,2,"b","1"),node(3,4,"c","1")].into_iter().map(|n|(n.id,n)).collect(),
            tablets:(0..128).map(|id| Tablet{id,bytes:1024}).collect() };
        assert_eq!(c.place().replicas, c.place().replicas);
    }

    #[test]
    fn separates_zones_when_possible() {
        let c = Cluster { epoch:1, replication_factor:3,
            nodes:[node(1,1,"a","1"),node(2,1,"b","1"),node(3,1,"c","1")].into_iter().map(|n|(n.id,n)).collect(),
            tablets:vec![Tablet{id:7,bytes:1}] };
        let p=c.place();
        let rs=&p.replicas[&7];
        let zones:BTreeSet<_>=rs.iter().map(|id| c.nodes[id].zone.as_str()).collect();
        assert_eq!(zones.len(),3);
    }

    #[test]
    fn draining_node_gets_no_new_placement() {
        let mut n3=node(3,100,"c","1"); n3.state=AdminState::Draining;
        let c=Cluster { epoch:2, replication_factor:2,
            nodes:[node(1,1,"a","1"),node(2,1,"b","1"),n3].into_iter().map(|n|(n.id,n)).collect(),
            tablets:(0..64).map(|id| Tablet{id,bytes:1}).collect() };
        assert!(c.place().replicas.values().all(|r| !r.contains(&3)));
    }
}
