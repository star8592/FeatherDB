use feather_sim::{AdminState, Cluster, Node, Tablet, moved_bytes};
use std::collections::BTreeMap;

fn main() {
    let mut nodes = BTreeMap::new();
    for (id, w, z) in [(1, 1, "a"), (2, 2, "b"), (3, 4, "c")] {
        nodes.insert(
            id,
            Node {
                id,
                weight: w,
                zone: z.into(),
                rack: "r1".into(),
                state: AdminState::Active,
            },
        );
    }
    let tablets = (0..1024)
        .map(|id| Tablet {
            id,
            bytes: 1024 * 1024,
        })
        .collect::<Vec<_>>();
    let before = Cluster {
        epoch: 1,
        replication_factor: 2,
        nodes: nodes.clone(),
        tablets: tablets.clone(),
    };
    let p1 = before.place();

    nodes.insert(
        4,
        Node {
            id: 4,
            weight: 8,
            zone: "d".into(),
            rack: "r1".into(),
            state: AdminState::Active,
        },
    );
    let after = Cluster {
        epoch: 2,
        replication_factor: 2,
        nodes,
        tablets: tablets.clone(),
    };
    let p2 = after.place();

    println!("feather-sim v0");
    println!("tablets={}", tablets.len());
    println!("replication_factor=2");
    let moved = moved_bytes(&p1, &p2, &tablets);
    let logical_bytes: u64 = tablets.iter().map(|t| t.bytes).sum();
    let replicated_bytes = logical_bytes * before.replication_factor as u64;
    println!("moved_bytes_after_join={}", moved);
    println!("replicated_bytes={}", replicated_bytes);
    println!(
        "movement_ratio={:.4}",
        moved as f64 / replicated_bytes as f64
    );

    let mut counts = BTreeMap::<u64, usize>::new();
    for replicas in p2.replicas.values() {
        for id in replicas {
            *counts.entry(*id).or_default() += 1;
        }
    }
    println!("replica_counts={:?}", counts);
}
