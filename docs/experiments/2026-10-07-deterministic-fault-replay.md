# Deterministic Fault Replay Experiment — 2026-10-07

## Goal

Prove that runtime failure schedules are reproducible and can drive the existing Repair/Migration implementation to a deterministic result.

## Fixed-seed campaign

Trace generator:

    seed = 8592
    fault nodes = [2,3]
    health flaps = 40
    max gap = 4 ticks
    max down duration = 4 ticks

Generated:

    events = 80
    last fault tick = 206
    encoded trace size = 2457 bytes

Workload:

    tablets = 128
    RF = 3
    owner 1 = Removed
    repair sources = nodes 2 and 3
    repair target = node 4

Replay result:

    converged = true
    ticks = 271
    events applied = 80
    source failovers = 1262
    completed migrations = 128
    bytes copied = 34688
    remaining bytes = 0

## Determinism checks

The test suite verifies:

- identical seed/config produces identical FaultTrace;
- trace text encodes and decodes byte-stably;
- identical trace + initial scheduler state produces identical replay report;
- after bounded faults stop, Repair converges.

## Quality gate

- cargo test: **62 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**

## Scope boundary

This experiment does not claim full deterministic simulation of FeatherDB yet.

It establishes:

    deterministic event ordering
      -> persisted replay trace
      -> real MigrationScheduler / Repair execution

Network, storage and clock nondeterminism remain future fault surfaces.
