# Deterministic Disk Fault Experiment — 2026-10-07

## Goal

Establish an explicit durability boundary and prove basic control metadata behavior under deterministic disk faults before selecting a production storage engine.

## Model

The experiment uses the same DurableStore interface for DirectMemoryStore and SimDisk.

Write path:

    Put
    Sync
    committed

Crash before Sync restores the last durable image.

## Faults covered

    slow I/O
    disk full
    generic I/O failure
    cancelled in-flight I/O on crash
    corrupt read
    corrupt write

## Control record

The test payload is a checksummed:

    topology_epoch
    catalog_generation

record.

The exact encoding is research-only; checksum verification is the important behavior.

## Results

Nine focused tests pass:

1. unsynced Put is lost on crash;
2. synced Put survives crash;
3. delayed Put completes only after its virtual deadline;
4. DiskFull rejects Put without mutation;
5. crash removes pending I/O;
6. corrupted control-record read is detected;
7. DiskFull commit retries idempotently after recovery;
8. crash between Put and Sync does not publish the record;
9. corrupted write can become durable but is detected on later checked read.

## Important semantic result

The simulator can no longer equate:

    write API returned success

with:

    durable control-plane commit

The durable boundary is explicitly Sync.

This matters for future TopologyEpoch/catalog transactions, because a crash between metadata write and durability acknowledgment must not expose a half-committed topology.

## Current limitation

Disk faults are not yet part of the common FaultTrace format and SimDisk is not yet attached per node in MembershipCluster/RuntimeCoordinator.

This experiment proves the storage I/O abstraction and commit semantics, not whole-cluster disk-fault recovery.

## Next

- node-scoped disk fault trace actions;
- RuntimeCoordinator durable metadata integration;
- simultaneous DiskFull + migration/repair;
- production Fjall/redb benchmarks behind the same boundary.
