# Official Documentation Audit — 2026-10-06

Status: completed for the current research/pre-prototype architecture boundary.

Purpose: verify FeatherDB's current development plan and active architecture hypotheses against current official documentation and canonical project references. This is not a claim that the architecture is finalized.

## Verdict

No current FeatherDB hypothesis was found to directly contradict the checked official documentation.

However, several ideas must be described more precisely:

1. WRH is a ranking primitive, not sufficient evidence for a complete heterogeneous replica allocator.
2. Tablet/range ownership should be explicit mutable metadata, not inferred forever from a static ring.
3. Placement planning and migration execution need separate rate/resource controls.
4. Hinted handoff-like mechanisms cannot replace anti-entropy repair.
5. Deterministic simulation should run production protocol logic, not a disconnected mock model.
6. Per-tablet/per-range Raft is a valid mature design, but FeatherDB intentionally does not adopt it by default because low-resource cost is a primary constraint; this remains a hypothesis to validate, not a proven superiority claim.

## Audit matrix

| FeatherDB hypothesis | Official/canonical evidence | Result | Action |
|---|---|---|---|
| Virtual tablets separate logical partitions from physical nodes | ScyllaDB tablets: partitions map deterministically to tablets; tablets have mutable replica locations and move during balancing | ALIGNED | Keep tablet indirection |
| Tablet map is mutable control-plane state | Scylla Rust driver: tablet replicas cannot be derived permanently from the ring because tablets move; routing map must be learned/updated | STRONGLY ALIGNED | Do not design clients around static key->node ownership |
| Stateful placement planner | Scylla tablet load balancer uses actual tablet disk usage; TiKV PD decides region move/split/merge from workload/storage capacity | STRONGLY ALIGNED | Continue Ranking -> Planner split |
| Placement and migration are distinct | TiKV PD generates scheduling operators and rate-limits them per store / cluster; Scylla performs tablet movement in background | STRONGLY ALIGNED | Keep desired vs actual placement |
| Background work needs bounded concurrency/backpressure | TiKV PD exposes snapshot/pending-peer/schedule limits; TiKV I/O limiter throttles background work preferentially | STRONGLY ALIGNED | Make budgets protocol-visible, not optional tuning |
| Failure-domain-aware placement | Ceph CRUSH explicitly models host/rack/zone/etc hierarchy and weighted placement rules | STRONGLY ALIGNED | Preserve domain constraints as hard policy when feasible |
| Heterogeneous capacity must influence placement | CRUSH bucket/device weights; TiKV PD storage-capacity scheduling; Scylla balances actual tablet disk use | STRONGLY ALIGNED | Use measured/capacity state in planner |
| Tablet count must be resource-bounded | Scylla 2026.3 dynamically splits/merges tablets and has tablet-per-shard goals to avoid overload | STRONGLY ALIGNED | Add tablet-count/memory model before freeze |
| Tiny consensus-backed topology control plane | Scylla uses Raft for schema/topology and sequences topology changes consistently | ALIGNED IN PRINCIPLE | Still need to choose FeatherDB voter model and metadata scope |
| Gossip/failure detection must not be topology authority | Scylla moved topology authority from gossip-style management to Raft sequencing | ALIGNED | Keep failure suspicion separate from durable removal |
| Default data plane can remain leaderless | Cassandra accepts replica mutations without per-key consensus at normal consistency levels | PLAUSIBLE / INTENTIONAL DIVERGENCE FROM TiKV/Cockroach | Must validate conflict/version semantics and repair rigorously |
| Do not use per-tablet Raft by default | TiKV and Cockroach demonstrate per-region/range Raft as a mature strong-consistency alternative | INTENTIONAL DIVERGENCE | Treat low-resource benefit as experimental claim until measured |
| Hints/handoff can reduce inconsistency but are insufficient | Cassandra docs explicitly state hints are best-effort; anti-entropy repair is required for guaranteed eventual convergence | STRONGLY ALIGNED | Never make hints the only repair mechanism |
| Merkle/range anti-entropy is a valid repair baseline | Cassandra repair compares Merkle trees and streams divergent ranges | ALIGNED AS BASELINE | Compare against log/range-hash alternatives before freeze |
| Simulation-first development | FoundationDB simulates whole clusters deterministically including network/disk/machine failures; TigerBeetle VOPR stubs nondeterministic clock/network/disk and replays by seed/commit | STRONGLY ALIGNED | Simulator must eventually execute production protocol code |
| WRH formula as research candidate | IETF BESS Weighted HRW draft describes weighted HRW; still an Internet-Draft, not an RFC | USABLE RESEARCH INPUT, NOT STANDARD CONTRACT | Do not cite as a finalized standard |
| Dynamic split/merge deserves consideration | Scylla tablets and TiKV Regions both split/merge as data size changes | STRONGLY ALIGNED | Move tablet sizing from open question to required simulator track |

## Important official-document corrections to our wording

### 1. Placement is not just hashing

Official Scylla and TiKV designs both support the newer FeatherDB conclusion that physical placement is a stateful scheduling problem.

- Scylla tablets move based on actual disk usage.
- TiKV PD records cluster state and schedules Region moves/splits/merges according to workload and storage capacity.
- TiKV also exposes explicit scheduling limits.

Therefore FeatherDB should not describe constrained WRH as "the placement engine". Better terminology:

    WRH / ranking
        -> placement planner
        -> desired tablet map
        -> migration scheduler
        -> actual tablet map

### 2. Tablet count is a first-class control variable

Scylla 2026.3 documentation is especially relevant: it dynamically reevaluates tablet counts, splits when average tablet size exceeds its target window, merges when below it, and has a tablets-per-shard goal to prevent overload.

FeatherDB therefore needs an explicit tablet cardinality policy before ADR-0002 can be accepted.

### 3. Migration throttling must be designed, not added later

TiKV PD and storage configuration expose:
- maximum concurrent snapshots;
- pending-peer bounds;
- Region/replica/hot-region scheduling limits;
- per-store scheduling rates;
- disk I/O rate limits that preferentially throttle background work.

FeatherDB's migration scheduler should treat these as architecture-level ideas. We should not rely on "background task priorities" without hard budgets.

### 4. Leaderless replication requires a real repair story

Cassandra documentation is explicit:
- hinted handoff is best effort;
- read repair is also not a substitute for full repair;
- anti-entropy repair is needed to guarantee convergence of missed updates.

Thus FeatherDB cannot claim simple quorum replication is enough. Phase 5 must specify:
- version/conflict semantics;
- durable acknowledgement conditions;
- handoff;
- anti-entropy;
- tombstone/delete semantics;
- repair scheduling and resource limits.

### 5. Simulation must converge with production implementation

FoundationDB and TigerBeetle do not merely test a separate abstract simulator. Their testing strategy exercises system/production logic under deterministic substitutes for clocks, network, disk, and scheduling.

FeatherDB's current placement-only simulator is appropriate for architecture exploration, but before protocol claims are accepted, shared protocol code must be runnable under deterministic I/O/time adapters.

## Current official sources checked

### ScyllaDB
- Data Distribution with Tablets  
  https://docs.scylladb.com/manual/stable/architecture/tablets.html
- Raft Consensus Algorithm in ScyllaDB  
  https://docs.scylladb.com/manual/stable/architecture/raft.html
- Scylla Rust driver tablet awareness  
  https://rust-driver.docs.scylladb.com/stable/load-balancing/tablets.html

### Apache Cassandra
- Hints  
  https://cassandra.apache.org/doc/latest/cassandra/managing/operating/hints.html
- Repair  
  https://cassandra.apache.org/doc/stable/cassandra/managing/operating/repair.html
- Dynamo architecture / replica synchronization  
  https://cassandra.apache.org/doc/stable/cassandra/architecture/dynamo

### Ceph
- CRUSH Maps  
  https://docs.ceph.com/en/latest/rados/operations/crush-map/

### TiKV
- Terminology / PD / Regions  
  https://tikv.org/docs/7.1/reference/architecture/terminology/
- PD configuration / scheduling limits  
  https://tikv.org/docs/7.1/deploy/configure/pd-configuration-file/
- TiKV storage / I/O rate limiting  
  https://tikv.org/docs/7.1/deploy/configure/tikv-configuration-file/

### FoundationDB
- Simulation and Testing  
  https://apple.github.io/foundationdb/testing.html
- Engineering / simulation and ratekeeper  
  https://apple.github.io/foundationdb/engineering.html

### TigerBeetle
- VOPR / Deterministic Simulation Testing  
  https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md

### CockroachDB
- Current architecture material confirms ranges are independently replicated with Raft and automatically redistributed/split; this is retained as a strong-consistency comparison rather than copied as FeatherDB's default data plane.  
  https://www.cockroachlabs.com/glossary/distributed-db/

### Weighted HRW
- IETF BESS Weighted HRW Internet-Draft (work in progress)  
  https://datatracker.ietf.org/doc/html/draft-ietf-bess-weighted-hrw-02

## Still NOT fully audited

These areas are intentionally deferred because the current implementation has not reached them:

1. SWIM + Lifeguard exact membership protocol and incarnation semantics.
2. QUIC transport implementation choice and flow-control/resource behavior.
3. Storage engine selection (Fjall/redb/RocksDB baseline) and durability guarantees.
4. HLC/dotted-version-vector/CRDT exact conflict model.
5. Control-plane Raft library choice and 1/2-node behavior.
6. Cryptographic identity/bootstrap/trust model.
7. Backup/restore and snapshot semantics.
8. Wire protocol and compatibility policy.
9. Licensing choice.
10. Exact dynamic tablet split/merge algorithm.

These must each get their own official-source audit before implementation/freeze.

## New mandatory process rule

Before an ADR may move from Proposed to Accepted:

1. cite current official docs or canonical papers for comparable mature designs;
2. state where FeatherDB aligns;
3. state where FeatherDB intentionally diverges;
4. state what experiment validates the divergence;
5. record any operational/resource tradeoff learned from official systems;
6. run the relevant simulator/test gate.

"Another database does this" is never sufficient evidence by itself.


## Migration scheduler implementation note

The executable migration model now reflects the previously audited official-system lessons:

- planning and physical execution are separate;
- scheduler concurrency is bounded;
- store/node-local transfer rate is independently bounded;
- actual ownership changes only after data copy completes;
- topology-epoch changes invalidate old work.

A deterministic lab demonstrated that a 16 MiB/tick target-node budget doubles convergence ticks relative to a 32 MiB/tick target-node budget for the same 32,000 MiB migration workload, confirming that node-local rate limits are behaviorally significant rather than decorative configuration.


## Forced-loss repair audit note

Current ScyllaDB documentation confirms two important distinctions now encoded in the simulator:

1. replacing a dead node streams data from other live cluster nodes to the replacement;
2. tablet-aware node removal rebuilds tablets on new replicas before removal completes.

FeatherDB therefore models failed-owner identity separately from copy-source identity.

Official references:
- https://docs.scylladb.com/manual/stable/operating-scylla/procedures/cluster-management/replace-dead-node.html
- https://docs.scylladb.com/manual/stable/operating-scylla/nodetool-commands/removenode.html

The official topology-quorum prerequisite also reinforces that failure suspicion must not directly authorize permanent ownership changes.


## Multi-replica cutover audit note

TiKV PD documents Joint Consensus as the default mechanism for replica scheduling; without it, PD schedules one replica at a time.

Reference:
- https://tikv.org/docs/7.1/deploy/configure/pd-configuration-file/

FeatherDB does not infer that it needs per-tablet Raft membership. The design lesson adopted here is only that multi-replica ownership changes require explicit safe intermediate-state semantics.

The current simulator uses atomic grouped tablet-map cutover after all required Rebalance copies have completed and the complete final replica set has been revalidated.


## Tablet resize controller audit note

Current ScyllaDB documentation gives concrete evidence for wide resize hysteresis:

- split when average tablet size grows above roughly 2x target;
- merge when average tablet size falls below roughly 0.5x target;
- tablet count is also constrained by per-shard tablet pressure because each tablet replica has fixed overhead.

Current TiKV/PD documentation exposes `split-merge-interval` specifically to prevent newly split Regions from being merged immediately.

References:
- https://docs.scylladb.com/manual/stable/architecture/tablets.html
- https://docs.scylladb.com/manual/stable/cql/ddl.html
- https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html
- https://tikv.org/docs/7.1/deploy/configure/pd-configuration-file/

FeatherDB now uses these as research evidence for:

    wide hysteresis
    cooldown
    metadata cardinality budget

It does not freeze Scylla's target tablet size or TiKV's wall-clock intervals as FeatherDB defaults.
