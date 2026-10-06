# FeatherDB

> Research-first, self-organizing distributed database for heterogeneous machines — written in Rust.

**Status:** architecture research / pre-prototype. No architecture is frozen yet.

FeatherDB explores whether a distributed database can combine low resource usage, leaderless data replication, elastic node membership, heterogeneous hardware, and strong correctness testing without requiring a large operational stack.

## Working goals

- Single binary; minimal external dependencies.
- Useful on small x86_64/ARM64 machines (initial research target: 1 CPU, 256–512 MB RAM).
- Any data node may accept requests; no permanent primary database node.
- Nodes should be easy to add, drain, remove, fail, and rejoin.
- Separate logical tablets/shards from physical nodes.
- Capacity-aware placement across heterogeneous machines.
- Start with a byte-oriented KV core; do not begin with SQL.
- Make consistency an explicit policy rather than pretending one mode fits every workload.
- Build deterministic simulation and fault injection alongside protocol code.
- Optimize for operational simplicity and bounded resource use, not feature count.

## Research method

Architecture decisions must triangulate three evidence streams:

1. **Academic:** Dynamo, SWIM/Lifeguard, CRDTs, quorum systems, Raft, EPaxos/Accord, anti-entropy, consistent/rendezvous hashing, etc.
2. **Engineering:** Cassandra/Riak, ScyllaDB, CockroachDB, TiKV, FoundationDB, TigerBeetle and related systems — including architectural migrations and failures.
3. **User pain:** GitHub issues/discussions, operator communities, Reddit/HN/DBA/DevOps/self-hosting discussions, postmortems and operational reports.

A design choice is not accepted merely because a successful database uses it. We document the constraints that caused the design, its resource/operational costs, and whether FeatherDB has the same constraints.

## Current hypothesis (not a commitment)

```text
Clients -> any node
             |
      +------+------+
      |             |
 leaderless      small, strongly
 data plane      consistent metadata
      |
 virtual tablets
      |
 capacity-aware placement
      |
 replicas on heterogeneous nodes

membership: SWIM/Lifeguard-like failure detection
replication: quorum/eventual/causal candidates
repair: anti-entropy/Merkle-style candidates
transport: QUIC candidate
storage: pluggable Rust engine candidates
correctness: deterministic simulation + real-cluster fault tests
```

## Architecture process

We deliberately do **not** freeze architecture after the first survey.

- Round 1 — landscape and evidence collection
- Round 2 — adversarial review: partitions, churn, slow nodes/disks, OOM, clock faults, corruption, topology races
- Round 3 — protocol synthesis and simulation models
- Round 4 — architecture freeze for the first prototype

See `docs/DEVELOPMENT_PLAN.md`, `docs/ROADMAP.md`, and `docs/research/`.

## Open source

FeatherDB is intended to be developed in the open. The exact license is intentionally **TBD** until the licensing and long-term project model are reviewed; do not assume a license from the absence of a LICENSE file.
