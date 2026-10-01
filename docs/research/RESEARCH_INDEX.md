# Research Index

## Academic foundations

- Dynamo — availability-first leaderless key/value architecture.
- SWIM — scalable failure detection and membership dissemination.
- Lifeguard — local-health-aware improvements to SWIM-style failure detection.
- CRDT literature — convergent replicated data types and causality.
- Raft — understandable replicated state machines; candidate only for a deliberately small metadata boundary.
- EPaxos / Accord — leaderless/decentralized approaches worth studying before strong-consistency decisions.
- anti-entropy / Merkle trees — replica divergence detection and repair.
- consistent / rendezvous / jump hashing — placement design space.

## Systems to dissect

| System | Study | Avoid cargo-culting |
|---|---|---|
| Dynamo/Riak | leaderless replication, sloppy quorum, handoff, versions | historical implementation constraints |
| Cassandra | repair, hinted handoff, operational reality | assuming ring/gossip is automatically simplest |
| ScyllaDB | tablets, capacity, topology evolution | per-tablet overhead without budgeting |
| CockroachDB | range lifecycle, correctness, rebalancing | making every data shard a consensus group by default |
| TiKV | region scheduling, placement | mandatory heavyweight external control plane for FeatherDB V0 |
| FoundationDB | deterministic simulation, failure testing | copying architecture before understanding its transaction contract |
| TigerBeetle | protocol-aware deterministic testing, bounded design | workload-specific assumptions |

## Evidence queue

For each system collect:

1. architecture/design docs;
2. relevant papers/talks;
3. major architectural migrations and why they happened;
4. high-signal bugs/postmortems;
5. operator/user complaints and praise;
6. resource behavior on small machines;
7. node lifecycle UX;
8. failure/repair semantics;
9. testing methodology;
10. lessons for FeatherDB and explicit non-lessons.

## Current architectural questions

- Can the data plane remain leaderless while topology metadata uses a very small consensus state machine?
- How small can that metadata state be, and what happens without quorum?
- Is weighted rendezvous + virtual tablets materially simpler than a token ring at churn?
- What version representation gives useful causal semantics without unbounded metadata?
- What repair structure works with mutable tablet boundaries?
- How do we guarantee bounded memory during migration/repair/topology changes?
- Can slow/weak nodes participate without becoming cluster-wide tail-latency amplifiers?
- What is the smallest practical Rust storage engine abstraction for deterministic simulation and production?
- Which parts of QUIC help, and which add avoidable memory/CPU overhead on 256–512 MB nodes?

No answer in this file is frozen architecture.
