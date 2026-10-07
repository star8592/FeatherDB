# Bounded Active Migration Window v0

Status: executable simulator architecture; production scheduler candidate.

## Problem

At million-tablet scale, keeping actual/desired placement compact is not sufficient if every future movement is eagerly materialized as a full MigrationTask.

Measured on the current build:

    MigrationTask = 88 bytes

A 1M-tablet strong-node join generated roughly 848K moves, so task structs alone would consume about 75 MB before container/allocator overhead.

At the same time, rewriting migration execution into a separate "compact scheduler" would risk semantic drift from the correctness-first MigrationScheduler.

## Architecture

The bounded-window design separates:

    global durable state
      CompactTabletCatalog
      Compact actual placement
      Compact desired placement

from:

    reconstructible future work
      lazy slot/range scan

and:

    bounded executable work
      small standard Placement subset
      existing MigrationScheduler
      existing MigrationTask state machine

Only a bounded set of tablets is materialized into the full scheduler at once.

## Reuse, not reimplementation

Each active window constructs a small Cluster/Placement view and invokes the existing MigrationScheduler.

Therefore active-window execution automatically retains the existing semantics for:

- copy-before-cutover;
- global/per-node byte budgets;
- per-task byte budgets;
- source/target health checks;
- Repair source selection;
- Repair source failover;
- in-flight transport cancellation;
- Delay/Drop/Duplicate/Reorder/Partition/Heal transport behavior;
- epoch carried by MigrationTask;
- grouped tablet cutover;
- failure-domain cutover checks.

The compact layer controls only which tablets are materialized next.

## Global Repair before Rebalance

A naive window scan could materialize ordinary Rebalance work from early tablet slots while a later slot still needs forced-loss Repair.

That would violate the existing:

    Repair > Rebalance

priority rule.

CompactWindowScheduler therefore runs two global phases.

### Repair phase

It scans the complete catalog for tablets whose actual->desired diff removes an owner that cannot stream data.

For a mixed tablet containing both forced Repair and ordinary movement, the window desired state is an intermediate replica set containing only the Repair replacements.

Ordinary healthy-owner movement is deferred.

### Rebalance phase

Only after the Repair scan has completed does the scheduler reset its scan and materialize remaining ordinary Rebalance work.

An executable mixed-topology test verifies that the removed owner's replica count is zero before Rebalance phase begins.

## Window state

A window contains at most:

    window_tablets

tablet records.

The number of materialized MigrationTask objects is bounded by the per-tablet replica movement inside those tablets.

The copy concurrency remains separately bounded by MigrationBudget.

This separation is important:

    materialization window != network / disk copy concurrency

A deployment can materialize hundreds or thousands of cheap task descriptors while allowing only a small number of actual transfers.

## Durable-state rule

The active window is not durable truth.

After every inner MigrationScheduler tick, any committed actual ownership changes are synchronized back into CompactPlacement.

On restart, CompactWindowScheduler can be reconstructed from:

    Cluster / topology epoch
    CompactTabletCatalog
    compact actual map
    compact desired map
    policy / budget

The window scan starts again and skips already converged tablets.

Partial/in-flight copy work may be redone conservatively, but committed ownership is preserved.

This extends the earlier rule:

    tasks are reconstructible work, not an independent source of truth.

## Runtime health and network faults

NodeHealth updates are stored by CompactWindowScheduler and forwarded into the current active MigrationScheduler.

A SimNetwork can be supplied through tick_with_transport.

An executable test partitions the Repair source->target link, confirms the window does not converge, heals the link, and then verifies convergence.

## Million-tablet execution experiment

Scenario:

    1,000,000 tablets
    RF = 2
    shared compact catalog
    before nodes weights 1,2,4
    after adds node weight 8
    stable TabletIds start at 10,000,000

Observed movement:

    changed moves = 847,811

The number differs slightly from the earlier implicit-slot experiment because WRH correctly hashes the stable TabletId values rather than slot numbers.

### Window = 64

    windows = 13,248
    ticks = 13,248
    peak materialized tasks = 64
    task struct bytes = 5,632
    execute time = ~5.57 s
    peak RSS = 57,696 KB

### Window = 256

    windows = 3,312
    peak materialized tasks = 256
    task struct bytes = 22,528
    execute time = ~1.84 s
    peak RSS = 57,740 KB

### Window = 1024

    windows = 828
    peak materialized tasks = 1024
    task struct bytes = 90,112
    execute time = ~0.94 s
    peak RSS = 58,064 KB

All three runs converged to the same desired placement.

## Core metadata footprint in the experiment

Raw flat arrays:

    CompactTabletCatalog = 24 MB
    actual RF2 replicas = 16 MB
    desired RF2 replicas = 16 MB
    --------------------------------
    core = 56 MB

At window=1024 the full MigrationTask structs add only about 90 KB.

This is qualitatively different from eager materialization of ~75 MB of task structs.

## Window-size conclusion

The 64/256/1024 experiment shows a strong fixed-cost effect from repeatedly constructing tiny inner schedulers.

Increasing the materialization window from 64 to 1024 reduced CPU execution time substantially while changing RSS only slightly.

Do not freeze 1024 as a production default from this synthetic benchmark.

Production tuning must separate:

- materialized task window;
- max active copies;
- per-node active copies;
- byte/sec or bytes/tick budgets;
- storage/network backpressure.

The important architecture property is that materialized task memory is bounded and independently configurable.

## Open work

1. Topology/desired-map epoch change while a compact window is active.
2. Dynamic catalog generation change from tablet split/merge during migration.
3. Persist/replay fault traces directly against CompactWindowScheduler.
4. Reverse TabletId -> slot index benchmark.
5. Active-window backpressure based on target disk/transport signals.
6. Million-tablet forced-loss campaign, not only pure join/rebalance.
7. General message bus shared by membership/control/data protocols.

## Decision

Preferred scheduler direction:

    compact shared catalog
      + compact actual/desired maps
      + global Repair/Rebalance phase scan
      + bounded materialization window
      + existing MigrationScheduler for active work

This achieves the low-memory goal without creating a second migration protocol.


## Active-window topology epoch replacement

A bounded window is reconstructible work and must never outlive a newer committed topology.

CompactWindowScheduler now exposes a transport-aware desired-map replacement path with strict fencing:

    proposed TopologyEpoch > current TopologyEpoch

Equal or older epochs are rejected without mutation.

When a higher epoch arrives:

1. any ownership cutover already committed by the active inner scheduler is synchronized into compact actual placement;
2. every old in-flight transfer is cancelled through MigrationTransport;
3. unfinished old-window tasks are counted as cancelled and discarded;
4. node runtime health is retained for surviving node identities and initialized for new nodes;
5. the new cluster/desired placement becomes authoritative;
6. active window state is discarded;
7. global phase resets to Repair;
8. catalog scanning restarts from slot 0 against current actual -> new desired.

The new epoch therefore never resumes from stale task state.

### Delayed-packet fencing

This is important because each small inner MigrationScheduler may number tasks from 1 again.

Without transport cancellation, a delayed packet belonging to an old window could remain in SimNetwork and later collide with a reused task ID.

The lower-level MigrationScheduler now also has a transport-aware reconcile path and explicit cancel_outstanding_transfers().

Executable tests verify:

- a delayed packet is removed when MigrationScheduler reconciles;
- equal epoch replacement is rejected without changing actual/desired;
- a higher epoch cancels an active delayed compact window;
- old in-flight count reaches zero before replanning;
- already committed actual ownership is retained;
- the new scheduler converges to the new desired map;
- a node removed only in the newer epoch has zero replicas at final convergence.

This extends the central fencing invariant:

    TopologyEpoch controls ownership authority;
    old work may consume time, but it may not commit into a newer topology.
