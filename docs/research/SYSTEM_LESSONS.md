# System Lessons

> Working research. `Learn` does not mean copy. `Avoid` does not imply the referenced system is badly designed; systems optimize for different constraints.

| System / lineage | Learn | Question / avoid copying blindly |
|---|---|---|
| Dynamo / Riak | leaderless replication, quorum tunability, hinted handoff, anti-entropy | ring complexity, sibling UX, repair burden |
| Cassandra | mature AP operations, tunable consistency, repair lessons | operational surface area and background-maintenance complexity |
| ScyllaDB | shard-per-core discipline, tablets, explicit resource accounting | fixed per-tablet overhead; topology machinery can become complex |
| CockroachDB | strong correctness discipline, ranges, extensive testing | per-range consensus/SQL machinery is heavier than FeatherDB's initial target |
| TiKV | clear separation of concerns, region scheduling, Rust implementation experience | external placement/control services conflict with single-binary/no-external-control goal |
| FoundationDB | deterministic simulation, minimal ordered-KV core, layering | operational/developer complexity and large-system assumptions |
| TigerBeetle | simulation-first engineering, explicit invariants, tight resource control | specialized workload and consensus choices do not directly map to an AP-first KV |
| SWIM / Lifeguard | scalable membership, suspicion, local-health awareness | membership knowledge is not sufficient for authoritative topology commits |
| CRDT research | coordination-free convergence for suitable data types | metadata/state overhead and semantic complexity if made universal |
| Raft | understandable consensus for tiny authoritative metadata | avoid putting every ordinary data write through a consensus group by default |
| EPaxos / Accord | paths toward less leader-centric strong consistency | protocol/verification complexity; defer until V0 data plane is proven |

## Emerging architecture hypothesis

The current hypothesis is deliberately hybrid:

- **Membership observation:** SWIM/Lifeguard-like failure detection and dissemination.
- **Authoritative topology:** tiny embedded consensus state machine, if experiments show it is necessary.
- **Data placement:** virtual tablets independent of physical nodes.
- **Placement policy:** weighted/adaptive placement rather than assuming homogeneous nodes.
- **Default data path:** leaderless replication with bounded, explicit consistency policies.
- **Repair:** anti-entropy designed as a first-class subsystem, not a maintenance afterthought.
- **Local storage:** pluggable pure-Rust engine first; custom storage engine only after measurement justifies it.
- **Testing:** deterministic simulator before feature breadth.

None of these are frozen ADRs yet.

## Key question for the next iteration

Can we preserve the operational simplicity of a leaderless system while using a *very small* amount of consensus only where ambiguity in topology would otherwise create dangerous states?

That question should be answered with simulation and failure-state enumeration rather than taste.
