use feather_sim::{AdminState, Cluster, Node, Tablet, moved_bytes};
use std::collections::BTreeMap;

fn main() {
    let mut nodes=BTreeMap::new();
    for (id,w,z) in [(1,1,"a"),(2,2,"b"),(3,4,"c")] {
        nodes.insert(id, Node{id,weight:w,zone:z.into(),rack:"r1".into(),state:AdminState::Active});
    }
    let tablets=(0..1024).map(|id| Tablet{id,bytes:1024*1024}).collect::<Vec<_>>();
    let before=Cluster{epoch:1,replication_factor:2,nodes:nodes.clone(),tablets:tablets.clone()};
    let p1=before.place();

    nodes.insert(4, Node{id:4,weight:8,zone:"d".into(),rack:"r1".into(),state:AdminState::Active});
    let after=Cluster{epoch:2,replication_factor:2,nodes,tablets:tablets.clone()};
    let p2=after.place();

    println!("feather-sim v0");
    println!("tablets={}", tablets.len());
    println!("replication_factor=2");
    println!("moved_bytes_after_join={}", moved_bytes(&p1,&p2,&tablets));
}
