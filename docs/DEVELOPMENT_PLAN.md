# FeatherDB Development Plan

Status: Active

Official-source audit: `docs/reviews/OFFICIAL-DOC-AUDIT-2026-10-06.md`
Last reviewed: 2026-10-07

This is the execution plan for the research/pre-prototype phase. It supersedes ad-hoc implementation order; `docs/ROADMAP.md` remains the broader research roadmap.

## Engineering rules

1. Evidence before architecture freeze.
2. Simulator before distributed production code.
3. Every topology operation is idempotent, resumable, and epoch-fenced.
4. Every unbounded queue or metadata structure is a bug until proven otherwise.
5. Low-resource nodes are first-class test targets.
6. Placement policy and physical migration are separate mechanisms.
7. Failure detection never directly authorizes destructive membership changes.
8. A feature is not done without a machine-checkable invariant or explicit reason why one is impossible.
9. Before any ADR becomes Accepted, complete an official-document/canonical-paper audit and record intentional divergences plus the experiment that justifies them.

## Phase 0 — repository hygiene

Deliverables:
- resolve license metadata mismatch;
- ignore build artifacts;
- keep application `Cargo.lock` committed;
- `cargo fmt --check`, `cargo clippy -- -D warnings`, and `cargo test` clean.

Exit gate: clean Git status after generated build artifacts are ignored and all quality checks pass.

## Phase 1 — placement laboratory

### 1.1 Strategy abstraction

Implement interchangeable:
- hash ring baseline;
- weighted rendezvous (WRH);
- constrained WRH.

Separate:
- node eligibility,
- ranking algorithm,
- failure-domain selection policy,
- metrics.

### 1.2 Metrics

Measure:
- replica distribution;
- feasible capacity-allocation error (not raw weight share when RF/failure-domain constraints make that target impossible);
- bytes/tablets moved;
- zone/rack collisions;
- planner operations/time;
- estimated metadata/working-set bytes.

### 1.3 Scenarios

Run at 1K / 10K / 100K tablets where practical:
- homogeneous baseline;
- heterogeneous 1:2:4:8 capacity;
- weak-node join;
- strong-node join;
- node drain;
- node removal;
- insufficient failure domains.

Exit gate for ADR-0002 candidate:
- deterministic replay;
- zero avoidable domain collisions for constrained policy;
- bounded movement on join/leave;
- weight-proportional responsibility within documented tolerance;
- no algorithm selected solely from one happy-path benchmark.

### Split-boundary experiment status

Implemented research comparison:

- HashMidpoint;
- ByteMedian;
- HeatMedian;
- byte-imbalance metric;
- heat-imbalance metric;
- same-token hotspot negative case.

Result: no single boundary strategy dominates both byte and heat objectives. Midpoint remains the deterministic fallback; data-aware strategies stay explicit policy candidates.

Multi-objective split-boundary policy implemented:

- byte/heat weighting;
- hard byte/heat imbalance limits;
- telemetry-confidence gate;
- minimum-improvement gate;
- explicit fallback reason.

See docs/experiments/2026-10-07-split-boundary.md.

### Physical range-resize implementation status

Implemented:

- exact [0, 2^64) range coverage model;
- midpoint split with byte conservation;
- adjacent-pair merge;
- replica-equality requirement before merge;
- topology-epoch and generation fencing;
- replay-safe commits;
- TabletRangeLifecycle as the single coordinated controller + physical-map entrypoint.

See docs/experiments/2026-10-07-range-resize.md.

Still open:

- multi-objective split-boundary policy is implemented; production default remains unfrozen pending broader workload evidence;
- integration with migration while resize and placement change concurrently;
- deterministic crash injection at every resize transition.

### Tablet lifecycle track

Logical split/merge control implements hysteresis, cooldown, metadata-budget limits, generation/topology fencing, and crash-replay idempotency. Physical hash-range split/merge execution is now implemented in the simulator through TabletRangeLifecycle, with coordinated count/range commits and replay fencing. Boundary objective selection remains a policy/research question rather than a correctness gap. See docs/architecture/TABLET-LIFECYCLE-v0.md.

### Million-tablet compact metadata status

The object-rich BTreeMap<TabletId, Vec<NodeId>> representation is retained as the correctness/reference model, but is no longer suitable as the only large-scale simulator representation.

Implemented research path:

- CompactPlacement uses a flat replica array with implicit contiguous simulator slots;
- compact WRH is exactly equivalent to standard WRH in executable comparisons;
- 1M tablets / RF=2 requires 32 MB for simultaneous before+after flat replica arrays;
- observed compact peak RSS is ~34 MB vs ~226 MB for the original object-rich 1M comparison process;
- CompactMigrationCursor reconstructs future moves lazily with RF-bounded buffering;
- the 1M join produces 847,638 moves, while the lazy cursor peaks at one buffered move / 48 bytes in the tested RF=2 scenario;
- pure-rebalance move ordering matches the eager MigrationScheduler exactly in a direct executable comparison.
- CompactWindowScheduler now materializes only a bounded tablet window into the existing MigrationScheduler;
- global forced Repair is completed before ordinary Rebalance materialization;
- a 1M-tablet execution converges with 1024 peak full tasks (~90 KB task structs) and ~58 MB observed RSS in the synthetic release lab;
- window size and actual copy concurrency are independently controlled;
- Active-window epoch replacement is now fenced: higher epochs cancel stale in-flight transfers, retain committed actual ownership, and replan from current actual; equal/older epochs are rejected.
- AdaptiveTabletIndex is benchmarked and implemented: contiguous IDs use zero-allocation arithmetic lookup; non-contiguous IDs use sorted compact pairs; flat open addressing remains optional acceleration.

Stable identity follow-up is implemented: CompactTabletCatalog stores shared TabletId/range-start/bytes arrays, catalog-backed WRH hashes stable TabletId, and catalog-backed lazy migration preserves eager scheduler ordering. TabletId -> slot reverse lookup is now benchmarked and implemented through AdaptiveTabletIndex.

See docs/architecture/COMPACT-METADATA-v0.md, docs/architecture/BOUNDED-MIGRATION-WINDOW-v0.md, docs/architecture/REVERSE-INDEX-v0.md, docs/experiments/2026-10-07-million-tablet-compact.md, docs/experiments/2026-10-07-bounded-million-migration.md, and docs/experiments/2026-10-07-reverse-index.md.

## Phase 2 — migration model

### Phase 2 implementation status

Implemented in feather-sim:

- desired vs actual placement;
- copy-before-cutover migration tasks;
- topology-epoch cancellation/rebuild;
- global and per-node concurrency budgets;
- global, per-node, and per-task byte budgets;
- canonical replica-set representation;
- normal rebalance source/target state checks.

See docs/architecture/MIGRATION-SCHEDULER-v0.md and docs/experiments/2026-10-06-migration-budget.md.

### Repair / forced-loss implementation status

Implemented:

- copy_source is distinct from owner_to_replace;
- Removed owners are never treated as readable;
- surviving replicas rebuild lost ownership;
- Repair is ordered ahead of Rebalance;
- sequential recovery from one surviving replica is supported;
- all-replica loss returns explicit NoRepairSource;
- runtime health is kept separate from durable topology;
- Repair automatically reselects a surviving copy source;
- scheduler restart reconstructs Repair from actual/desired maps instead of persisting a second task journal.

See docs/architecture/REPAIR-PATH-v0.md and docs/experiments/2026-10-07-forced-loss-repair.md.

Still open:

- target failure/disk-full mid-copy;
- persistent degraded-repair age/debt metrics;
- checksum/version validation;
- later anti-entropy integration;
- grouped cutover now resolves the current blocked multi-replica transition case; add a dependency graph only if future scenarios still require it;
- foreground-pressure feedback;
- disk-full / slow-target injection.

Add:
- `desired_replica_set`;
- `actual_replica_set`;
- migration tasks;
- per-node and cluster byte/concurrency budgets;
- priority for replica repair over balancing;
- crash/restart of migration workers.

Exit gate:
- migration queues remain bounded;
- interrupted moves resume safely;
- foreground protection can stop/reduce background movement;
- desired state may advance without falsely claiming actual convergence.

## Phase 3 — topology transaction simulator

Implement executable versions of:
- Join;
- Drain/Leave;
- Forced removal;
- Replace;
- TopologyEpoch;
- OperationId idempotency;
- Incarnation fencing.

Inject a crash at every transition boundary.

Exit gate for ADR-0001 candidate:
- committed ownership never rolls backward;
- stale workers cannot publish newer ownership;
- repeated operations converge;
- metadata-quorum loss pauses mutations without fabricating ownership.

## Phase 4 — deterministic event/fault engine

### Phase 4 implementation status

Implemented:

- deterministic tick + sequence event ordering;
- SplitMix64 seed-driven health and network-fault generation;
- virtual SimClock;
- MigrationTransport abstraction shared by direct and simulated copy paths;
- deterministic Delay/Drop/Duplicate/Reorder/Partition/Heal semantics;
- stable V2 text trace with V1 health-trace compatibility;
- trace round-trip and replay;
- direct replay against real MigrationScheduler / Repair logic;
- in-flight chunk fencing and cancellation on Repair source failover;
- same-seed/same-trace reproducibility checks;
- bounded health/network-fault convergence tests;
- shared deterministic message bus for Membership/Gossip/Control/Data/Repair/Client classes;
- explicit bounded message/byte queues with Backpressure;
- protocol-independent ScheduledMessage replay harness;
- mixed-protocol fixed-seed replay with identical full delivery trace.

See docs/architecture/DETERMINISTIC-FAULTS-v0.md, docs/architecture/DETERMINISTIC-MESSAGE-BUS-v0.md, docs/experiments/2026-10-07-deterministic-fault-replay.md, docs/experiments/2026-10-07-deterministic-network.md, and docs/experiments/2026-10-07-message-bus.md.

Still open:

- disk-full/slow/corrupt I/O;
- CPU stall;
- 100-node topology churn campaign.

Seed-driven events:
- Crash/Restart;
- Partition/Heal;
- Delay/Drop/Duplicate/Reorder;
- DiskFull/slow I/O;
- CPU stall;
- telemetry oscillation;
- repeated join/leave churn.

Persist replay traces for every failure.

Exit gate:
- same seed reproduces the same trace and result;
- 100-node churn campaign converges after faults stop;
- all modeled queues and state sizes remain bounded.

## Phase 5 — leaderless data semantics

Only after placement/topology are credible, model:
- N/R/W quorum;
- version model (HLC + causal metadata candidates);
- conflict policies;
- hinted handoff candidate;
- anti-entropy/repair.

Do not implement general SQL or distributed ACID transactions.

## Phase 6 — implementation substrate benchmarks

Benchmark, do not guess:
- local storage: Fjall/redb and a baseline alternative;
- transport: QUIC implementation candidates;
- serialization/wire format;
- memory allocator/working-set behavior on 256/512 MiB limits.

## Phase 7 — three-node prototype

One `featherd` binary:
- bootstrap/join;
- PUT/GET/DELETE/CAS;
- tablet ownership;
- leaderless replication;
- bounded repair/migration;
- structured health/degraded-state reporting.

Fault tests:
- kill -9;
- restart;
- network partition;
- disk full;
- slow node;
- add/drain/replace.

## Progress snapshot — 2026-10-07

- Phase 0: implementation complete locally; pending commit in this review batch.
- Phase 1.1: complete for hash-ring / WRH / constrained-WRH comparison.
- Phase 1.2: partial; movement, excess-join movement, replica counts, zone/rack collisions implemented.
- Phase 1.3: partial; strong-node join and failure-domain-pressure scenarios implemented.
- RF=2 single-node removal movement now matches a failure-domain-aware constrained lower bound exactly.
- Tablet resize controller: logical count control implemented with hysteresis, cooldown, metadata budget, epoch/generation fencing, and replay idempotency.
- Tests: 135 passing.
- First experiment: `docs/experiments/2026-10-06-placement-baseline.md`.
- ADR-0001: still Proposed.
- ADR-0002: still Proposed.

Next priority: tablet-resize/migration generation fencing, actual Membership/SWIM state machine on the shared bus, deterministic disk adapter, then production-substrate benchmarks.

### Placement planner hypothesis

Experiment evidence now separates candidate ranking from final allocation. WRH is retained as a ranking/tie-break primitive; the implemented quota-aware planner computes a deterministic desired tablet map against safety, capacity and stickiness. Physical movement remains a separate scheduler problem. See `docs/architecture/PLACEMENT-PLANNER-v0.md`.

## Immediate sprint

1. Refactor `feather-sim` into model / placement / metrics modules.
2. Replace placeholder scoring with a real WRH implementation while retaining the placeholder only if useful as a named baseline.
3. Add hash-ring baseline.
4. Add comparable join-scenario metrics.
5. Run tests, fmt, clippy.
6. Record first comparison results in `docs/experiments/`.
7. Do not accept ADR-0002 yet; use results to decide the next experiment.

## Explicit non-goals now

- SQL and joins
- general distributed transactions
- vector search
- Kubernetes operator
- admin UI
- production QUIC protocol
- custom storage engine
- compatibility guarantees

Those can only be reconsidered after the core protocol evidence is strong.
