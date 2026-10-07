# Deterministic Network Fault Experiment — 2026-10-07

## Goal

Verify that FeatherDB migration/repair progress remains deterministic and safe when the actual copy path experiences Delay, Drop, Duplicate, Reorder, Partition and Heal events.

## Architecture under test

The scheduler emits one logical copy chunk at a time through `MigrationTransport`.

A chunk contains:

    task_id
    chunk_offset
    source
    target
    bytes

Progress is acknowledged only after delivery.

The simulator uses:

    SimClock
      +
    SimNetwork
      +
    FaultTrace V2
      +
    real MigrationScheduler / Repair code

## Fixed-seed campaign

Seed:

    8592

Workload:

    tablets = 256
    RF = 3
    lost owner = 1
    repair sources = 2,3
    repair target = 4
    faulted links = 2->4, 3->4

Fault generator:

    episodes = 80
    max gap = 4 ticks
    max duration = 5 ticks
    max delay = 6 ticks

Generated:

    events = 112
    last fault tick = 210
    trace bytes = 2658

Event mix:

    link-delay = 38
    drop-next = 20
    duplicate-next = 11
    reorder-next = 17
    partition = 13
    heal = 13

Replay:

    converged = true
    ticks = 211
    events applied = 112
    transfer drops observed = 61
    transfer duplicates observed = 9
    bytes attempted = 146688
    bytes delivered = 131072
    repairs completed = 256
    remaining bytes = 0

A second replay of the exact V2 trace produces the same replay report and final placement.

## Important safety results

### Delayed data is not ownership

A delayed chunk leaves the tablet on the old owner until delivery and cutover validation.

### Dropped data is not progress

Dropped chunks do not reduce `bytes_remaining`.

### Duplicate data is idempotent

Duplicate delivery is observed as a fault metric but counts as one logical chunk for migration progress.

### Reordering is explicit

`ReorderNext` delays selected earlier packets so later packets can overtake them.

### Partition is directional

An asymmetric source->target partition is representable and does not require marking either node globally unavailable.

### Repair failover fences stale packets

When a Repair source fails over, any in-flight packet from the old source is cancelled before the new source restarts the chunk.

## Quality gate

- cargo test: **91 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**
- git diff --check: **pass**

## Conclusion

FeatherDB now has an executable deterministic network-fault substrate for migration/repair.

The next step should not be more migration-specific fault knobs. It should be a general deterministic message bus reused by future membership, control-plane and data-replication protocols.
