# User Pain Map

This is an evidence log, not marketing copy. A complaint becomes a FeatherDB requirement only after we understand its context and root cause.

## Collection schema

For each observation record: source/date/system; workload/scale; symptom; suspected/confirmed cause; severity; workaround; architectural lesson; proposed invariant/test; confidence.

## Early signals

### 1. Operations become a second platform

A March 2026 Cassandra operator discussion describes production operation as substantial DIY platform engineering: JMX/Prometheus/Grafana, repair scripts, backup jobs, custom shell tooling and tribal knowledge. This is anecdotal/community evidence, not a controlled survey, but it is a strong hypothesis to investigate across more operators.

Product question: can routine repair, backup health, capacity, topology and failure diagnosis be built into the database rather than assembled around it?

### 2. Metadata cardinality can become a memory failure mode

CockroachDB issue #172348 (2026) reports a range relocation path eagerly loading descriptors for roughly 600k ranges. Under concurrent operations the gateway's heap grew dramatically and was OOM-killed. The narrow bug is implementation-specific; the general lesson is broader: shard/tablet metadata and topology operations require streaming/lazy algorithms, accounting, backpressure and bounded memory.

Candidate FeatherDB invariant: no O(number_of_all_tablets) materialization on a request whose semantic scope is O(1), unless explicitly budgeted and bounded.

### 3. Fine-grained tablets have non-zero fixed cost

ScyllaDB's documentation explicitly notes that every tablet replica has constant memory overhead and that tablet count may need limiting to prevent shard OOM with many tables. ScyllaDB issue #23284 also documents overload in a 5,000-table tablet scenario.

Candidate lesson: tablets are useful indirection, but "more/smaller" is not free. FeatherDB needs an explicit metadata budget and adaptive tablet granularity.

### 4. Topology changes create their own workload

ScyllaDB issue #24934 reports elevated internal shard-0 activity during tablet streaming while nodes are added/removed. A 2026 issue (#30571) describes a race/use-after-free under concurrent heavy tablet streaming and a topology Raft barrier.

Candidate lesson: join/leave/rebalance must be treated as first-class workloads with rate limits, isolation, idempotent state transitions and simulation coverage — not background magic.

### 5. Correctness testing is itself architecture

FoundationDB documents deterministic whole-cluster simulation with failures of networks, disks, machines and datacenters, using reproducible seeds, and states that this capability was vital to building the system. FeatherDB should design time/network/disk/randomness abstractions early enough that production protocol logic can execute in deterministic simulation.

## Pain taxonomy to expand

- Installation/bootstrap
- Memory footprint and bounded queues
- CPU overhead at idle
- Configuration burden
- Node join/drain/remove/rejoin
- Rebalancing and topology changes
- Repair and anti-entropy
- Network partitions
- Slow nodes / gray failures
- Backup/restore
- Upgrades and compatibility
- Kubernetes and non-Kubernetes operation
- Small clusters / home lab / edge
- ARM and heterogeneous hardware
- Observability and diagnosis
- Security/bootstrap identity
- Cost and overprovisioning
- Developer experience

## Research discipline

We will deliberately collect contradictory reports and positive experiences. Community posts are evidence of pain existence, not prevalence. GitHub bugs prove a failure mode in a particular version/path, not that an entire architecture is defective. Architectural conclusions must be triangulated with design docs, code/issues, papers and experiments.
