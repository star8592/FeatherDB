# Deterministic Fault Engine v0

Status: executable foundation, not a complete distributed-system simulator.

## Purpose

FeatherDB must make failures reproducible by seed instead of relying on nondeterministic stress tests that cannot replay the exact interleaving.

The simulator therefore drives production-intent state machines through deterministic substitutes for nondeterministic surfaces.

The architecture rule is:

    simulator event injection
      -> shared clock / transport abstraction
      -> real scheduler / repair state machine

not:

    simulator
      -> separate fake migration implementation

## Deterministic clock

`SimClock` is a virtual monotonic tick source.

The network replay loop reads only `SimClock::now()` and advances it explicitly. Wall-clock time and host scheduling therefore do not influence event order.

Current scope is tick time. Timers/sleeps inside future protocol code must be routed through the same clock abstraction before they are considered simulation-covered.

## Migration transport abstraction

`MigrationScheduler` no longer assumes that a scheduled byte chunk is immediately copied.

It emits a `TransferRequest` containing:

    task_id
    chunk_offset
    from
    to
    bytes

and receives transport outcomes through `MigrationTransport`.

The default `DirectMigrationTransport` preserves the previous immediate-copy behavior.

`SimNetwork` is the deterministic test adapter.

A task may have at most one in-flight chunk. `bytes_remaining` decreases only after delivery, never when a chunk is merely scheduled.

This is required for meaningful Delay/Drop/Partition semantics.

## Network fault semantics

The simulator now models:

### Delay

A directed link can add deterministic virtual-tick latency.

Ownership cannot cut over while the only outstanding chunk is delayed.

### Drop

A transfer attempt can be dropped.

The attempted bytes consume the scheduler's submission budget for that tick, but do not reduce `bytes_remaining`. A later tick retries the same logical offset.

### Duplicate

A transfer can be reported as duplicated.

Migration progress remains idempotent: one logical chunk decrements `bytes_remaining` once. Duplicate count is observable separately.

### Reorder

`ReorderNext` adds extra delay to selected earlier packets while later packets may use the base delay and overtake them.

A unit test proves a later task can deliver before the earlier delayed task.

### Partition / Heal

Partitions are directional by default and can optionally be bidirectional.

A partitioned directed link drops transfer attempts. Heal restores the link without rewriting topology ownership.

This makes asymmetric partition scenarios representable.

## Repair source failover and in-flight cancellation

If a Repair copy source becomes unavailable while a chunk is in flight:

1. the transport cancels that task's old packet;
2. the task returns to Pending;
3. partial progress is conservatively discarded;
4. another surviving replica becomes `copy_source`;
5. copying restarts safely.

The old delayed packet cannot later arrive and mutate progress after source failover.

## Fault trace V2

Fault traces are totally ordered by:

    (tick, sequence)

V2 supports:

    node-health
    link-delay
    drop-next
    duplicate-next
    reorder-next
    partition
    heal

Example:

    feather-fault-trace-v3,<seed>
    <tick>,<sequence>,link-delay,<from>,<to>,<ticks>
    <tick>,<sequence>,partition,<a>,<b>,<bidirectional>

V1 health-only traces remain readable.

Trace serialization round-trips byte-for-byte.

## Seed-driven generation

Two generators currently exist:

- `generate_health_flaps`
- `generate_network_faults`
- `generate_network_faults_on_links`

The link-scoped generator is useful when the campaign must fault only links that carry a real workload.

All generators use the same deterministic SplitMix64 stream.

Same seed + same inputs must generate exactly the same trace.

## Replay

`replay_migration_faults_with_network`:

1. reads current virtual time from `SimClock`;
2. applies all fault events at that tick;
3. executes the real `MigrationScheduler::tick_with_transport`;
4. records attempts, delivered bytes, drops, duplicates, completions and failovers;
5. advances virtual time explicitly;
6. stops after all events are consumed and migration converges, or the tick bound is exhausted.

## Executable invariants

Tests currently verify:

- same seed -> identical health trace;
- same seed -> identical network trace;
- V1 trace compatibility;
- V2 network trace byte-stable round-trip;
- deterministic total event ordering;
- directional partition;
- partition/heal recovery;
- deterministic delay;
- bounded drop behavior;
- duplicate idempotency;
- explicit packet reordering;
- delayed copy cannot cut over early;
- partitioned copy cannot claim progress;
- heal allows convergence;
- Repair source failover cancels stale in-flight data;
- same network trace + same initial state -> same final state/report;
- bounded network faults eventually converge after faults stop.

## Fixed-seed network lab

Configuration:

    seed = 8592
    tablets = 256
    RF = 3
    durable lost owner = node 1
    live repair sources = nodes 2 and 3
    repair target = node 4
    faulted directed links = 2->4 and 3->4
    network episodes = 80

Generated trace:

    total events = 112
    last fault tick = 210
    encoded trace bytes = 2658
    delay events = 38
    drop events = 20
    duplicate events = 11
    reorder events = 17
    partition events = 13
    heal events = 13

Replay result:

    converged = true
    ticks = 211
    events applied = 112
    transfer drops observed = 61
    transfer duplicates observed = 9
    bytes attempted = 146688
    bytes copied = 131072
    completed repairs = 256
    remaining bytes = 0

A second replay of the same trace produces the same report and final placement.

## Alignment with mature DST systems

FoundationDB documents deterministic whole-cluster simulation with virtual time and replaceable network/process surfaces. Its simulation is single-threaded and seed-reproducible.

TigerBeetle's VOPR similarly replaces nondeterministic clock, network and disk operations; its documented fault model includes packet drop/reorder, network partitions and storage faults, and failures are replayed by seed plus code identity.

References:

- https://apple.github.io/foundationdb/testing.html
- https://apple.github.io/foundationdb/engineering.html
- https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md
- https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst/

FeatherDB currently aligns with the mechanism, not the maturity level.

## Scope boundary

This is still not a complete network simulator.

Current `SimNetwork` is a migration-transfer adapter, not yet a general packet/message bus for membership, gossip, control-plane consensus, client requests, or replication messages.

Likewise, `SimClock` currently controls the replay loop but has not yet replaced every future timer source because those protocol components do not exist yet.

## Next fault surfaces

1. disk full / slow I/O / corruption adapter;
2. target-node crash during in-flight write;
3. CPU stall / scheduler starvation;
4. topology join/leave/replace churn;
5. 100-node randomized campaign;
6. persist failing traces automatically from CI;
7. actual Membership/SWIM and control-plane state machines on the shared message bus;
8. later protocol-specific liveness mode after safety-mode coverage is credible.

Every new nondeterministic surface must be introduced behind an abstraction usable by both production-intent code and the simulator.


## Reconcile-time transport cancellation

Topology reconciliation is now transport-aware.

Before MigrationScheduler discards stale tasks, it cancels every outstanding in-flight transfer through MigrationTransport.

This prevents delayed simulated packets from surviving an epoch change and later being mistaken for traffic belonging to a newly reconstructed task/window.

DirectMigrationTransport treats cancellation as a no-op because it has no queued packets. SimNetwork removes the packet from its in-flight map.

A unit test exercises a delayed packet and verifies in-flight count becomes zero during reconcile.


## General deterministic message bus

The network fault substrate is no longer migration-only. SimNetwork now provides a bounded generic message bus for Membership, Gossip, Control, Data, Repair and Client message classes. Generic messages and MigrationTransport share the same directional link fault state.

A protocol-independent replay harness runs ScheduledMessage workloads against FaultTrace V2 and records deterministic delivery traces and queue peaks.

See docs/architecture/DETERMINISTIC-MESSAGE-BUS-v0.md and docs/experiments/2026-10-07-message-bus.md.


## V3 node-scoped disk faults

FaultTrace V3 extends the same deterministic timeline with node-scoped storage actions:

    SetDiskFull(node, bool)
    SetDiskDelay(node, ticks)
    DiskFailNext(node, count)
    CorruptNextDiskRead(node, count)
    CorruptNextDiskWrite(node, count)
    CrashDisk(node)

V1 health-only traces and V2 network traces remain readable. Newly serialized traces use V3.

Network and disk application are deliberately separate: apply_network_fault_action ignores disk actions, while apply_disk_fault_action targets a BTreeMap<NodeId, SimDisk>. A whole-cluster harness may feed the same ordered event stream to both physical surfaces.

A deterministic generator now produces bounded disk-full/slow/failure/corruption/crash episodes by seed.

The first combined campaign executes membership churn, process crash/restart, network faults, and per-node disk faults on the same trace. See docs/experiments/2026-10-07-cross-fault.md.
