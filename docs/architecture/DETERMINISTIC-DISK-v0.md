# Deterministic Disk Adapter v0

Status: executable durability/fault substrate; not a production storage engine.

## Purpose

FeatherDB's simulator already replaces clock and network nondeterminism. The next physical boundary is durable storage.

The disk adapter is intentionally below protocol state machines:

    production-intent state machine
        -> DurableStore
            -> DirectMemoryStore
            -> SimDisk
            -> future Fjall/redb adapters

The simulator must not maintain a separate fake topology or migration protocol.

## Research alignment

FoundationDB simulation models physical drives, drive performance, drive space and drive-full/failure conditions while running cluster workloads in deterministic simulation.

TigerBeetle VOPR replaces real disk operations with controllable storage and injects storage delay/corruption/failure while running real protocol code.

References:

- https://apple.github.io/foundationdb/testing.html
- https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md
- https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst/
- https://docs.tigerbeetle.com/concepts/safety/

FeatherDB aligns with the mechanism only. It does not yet provide TigerBeetle-style block recovery or a FoundationDB-scale storage simulator.

## DurableStore contract

The current interface supports:

    Read
    Put
    Delete
    Sync

Every request has a deterministic DiskOpId.

Operations may:

    complete immediately
    remain Pending until a virtual tick
    fail

Failures currently include:

    Full
    Io
    Cancelled
    Corrupt

## Persistence model

The in-memory disk core maintains:

    visible state
    durable state

Put/Delete mutate visible state.

Sync copies the visible state to durable state.

Crash:

    cancels all in-flight operations
    discards unsynced visible changes
    restores visible state from durable state

Therefore:

    successful Put != durable commit

Only a successful Sync establishes the durable boundary in this v0 model.

This is the central invariant the adapter is intended to enforce.

## DirectMemoryStore

DirectMemoryStore implements the same DurableStore interface without injected latency/faults.

It is useful for protocol tests that want persistence semantics without fault scheduling.

It still preserves Put-vs-Sync behavior, so tests cannot accidentally treat a successful write call as durable.

## SimDisk

SimDisk adds deterministic fault controls:

### Slow I/O

    set_delay(ticks)

A submitted operation becomes pollable only after the virtual completion tick.

### Disk full

    set_full(true)

Put is rejected with Full and does not mutate visible state.

### I/O failure

    fail_next(count)

The next operations fail with Io.

This can target Sync as well as Read/Put/Delete.

### Read corruption

    corrupt_next_read(count)

Returned bytes are deterministically modified.

### Write corruption

    corrupt_next_write(count)

The write may appear successful, including Sync, but corrupted bytes become durable.

The fault is therefore latent until a checksummed consumer reads and validates the record.

### Crash

Crash removes pending operations and rolls visible state back to the last synced durable image.

Polling an operation removed by crash returns Cancelled.

## Checksummed control record

A small production-intent durability example is included:

    ControlRecord {
        topology_epoch
        catalog_generation
    }

Encoding contains:

    magic/version marker
    topology epoch
    catalog generation
    checksum

Read validates the checksum and reports Corrupt on mismatch.

This is not the final control-plane serialization format. Its role is to prove that silent read/write corruption is visible to protocol code rather than silently accepted.

## DurableControlWriter

DurableControlWriter is a small asynchronous commit state machine:

    Idle
      -> PutPending
      -> SyncPending
      -> Complete

or:

      -> Failed(error)

The writer reports committed only after Sync completes.

After a failure, retry restarts the Put+Sync sequence. This is idempotent for the single current-control-record key.

If the process/disk crashes after Put but before Sync:

- the record is not durable;
- the pending Sync disappears;
- the writer observes Cancelled;
- retry can safely write and sync the same logical record again.

## Executable invariants

Tests verify:

- Put without Sync is lost after crash;
- Put + Sync survives crash;
- delayed I/O completes only after virtual time reaches its deadline;
- DiskFull Put does not mutate visible state;
- crash cancels delayed in-flight I/O;
- checksummed control record detects corrupted reads;
- DiskFull control commit retries safely after recovery;
- crash between Put and Sync never publishes a durable control record;
- silent write corruption survives Sync but is detected when the record is later read.

## Scope boundary

This v0 model is record-oriented and intentionally small.

It does not yet model:

- sectors/pages/blocks;
- torn writes;
- partial writes;
- fsync ordering across multiple files;
- misdirected I/O;
- persistent bad sectors;
- per-node disk capacity accounting;
- throughput/IOPS queues;
- read/write concurrency;
- storage-engine compaction;
- replicated physical repair.

The model should expand only when a real state machine or storage candidate needs those semantics.

## Next integration steps

1. Put the authoritative topology/catalog metadata transaction behind DurableStore.
2. Add per-node SimDisk instances to a whole-cluster simulation harness.
3. Extend the deterministic fault trace with node-scoped disk actions.
4. Run simultaneous network + disk + membership churn.
5. Benchmark Fjall/redb adapters behind the same API.
6. Add slow-disk feedback into migration/backpressure policy.

The key rule remains:

    protocol code owns correctness;
    the simulator controls physical nondeterminism.
