# FeatherDB Research & Architecture Roadmap

Execution status and current sprint are tracked in `docs/DEVELOPMENT_PLAN.md`.

## Rule zero

Do not implement a production protocol because it sounds elegant. Every important mechanism needs evidence, explicit invariants, a failure model, resource budgets, and a simulation/test plan.

## Round 1 — Landscape

Build comparable notes for:

- Dynamo, Riak, Cassandra
- ScyllaDB (especially ring -> tablets and gossip topology -> Raft topology)
- CockroachDB and TiKV
- FoundationDB
- TigerBeetle
- SWIM and Lifeguard
- CRDTs, vector/dotted version vectors, HLC
- Raft, EPaxos, Accord
- anti-entropy and repair
- consistent, jump and rendezvous hashing
- Rust local storage engines and QUIC implementations

Deliverables: system matrix, paper index, user-pain map, design-space document.

## Round 2 — Attack our assumptions

Model and test at least:

- network partitions and asymmetric packet loss
- packet delay, duplication and reordering
- process pause / overloaded event loop
- node crash/restart and stale reincarnation
- rapid join/leave churn
- slow disk, disk full, partial/corrupt writes
- clock skew/jumps
- hot tablets and skewed keys
- mixed 1C/512MB through large-server nodes
- metadata growth to very large tablet counts
- concurrent topology changes
- repair storms and rebalance storms
- memory backpressure and OOM prevention

## Round 3 — Synthesis

Prototype/simulate alternatives rather than selecting by reputation:

- placement: weighted rendezvous vs alternatives
- membership: SWIM/Lifeguard variants
- data replication: leaderless quorum + version model
- metadata: minimal consensus boundary vs weaker alternatives
- tablet sizing/splitting/merging
- handoff and anti-entropy
- flow control / bounded concurrency

Every candidate must state safety and liveness invariants.

## Round 4 — V0 architecture freeze

Only after the previous rounds:

1. Publish architecture v0 and ADRs.
2. Freeze V0 wire/storage compatibility expectations only where necessary.
3. Implement a deterministic simulator before a production network stack becomes complex.
4. Build a three-node prototype.
5. Run kill/restart/partition/churn/low-memory tests.
6. Benchmark against resource budgets, not only throughput.

## Initial budgets to validate, not promises

- One daemon / one binary for normal deployment.
- No mandatory ZooKeeper/etcd/PD/sidecar.
- Minimum-machine research target: 1 CPU, 256–512 MB RAM.
- Idle RSS aspiration: <30 MB; normal small-node RSS aspiration: <100 MB.
- Explicit hard limits/backpressure for metadata, queues, repair, migration and request concurrency.

## Non-goals for V0

SQL, joins, distributed general-purpose ACID transactions, vector search, document query languages, analytics engines, and ecosystem breadth. The first job is a correct, boring, resilient distributed byte KV substrate.
