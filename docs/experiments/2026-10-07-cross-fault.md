# Cross-Surface Network + Membership + Disk Fault Experiment — 2026-10-07

## Goal

Run membership lifecycle and durable control-record writes against one deterministic timeline containing both network and node-scoped disk faults.

## Cluster lifecycle

Initial membership: 20 nodes. At start, 30 nodes dynamically join for a target of 50.

Scripted lifecycle events:
- node 10 crash at tick 80
- node 10 restart at tick 180
- nodes 45..50 graceful leave at tick 220
- nodes 45..47 rejoin at tick 300

Final expected membership: 47 joined, 3 Left.

## Fault trace V3

Common trace seed: 0x85928593
Concrete events: 544
Network events applied: 312
Disk events applied: 232
Last fault tick: 345

V3 disk actions include disk-full on/off, disk-delay/recovery, disk-fail-next, disk-corrupt-read, disk-corrupt-write and disk-crash.

V1 health traces and V2 network traces remain parse-compatible.

## Durable commit timing

Control-record writers start at tick 60 so Put/Sync activity overlaps the active disk-fault campaign.

Test record: TopologyEpoch=7, catalog generation=3. This record is a durability fixture, not yet a complete replayable topology transaction.

## Result

The complete campaign is run twice and produces identical output:
- final tick = 1147
- final joined = 47
- final digest = 12846308789212167
- peak buffered messages = 50
- backpressure = 0
- durable records requiring repair rewrite after faults stop = 4
- release peak RSS ~= 3.24 MB

The four repair rewrites demonstrate that disk faults hit real Put/Sync/checksum paths rather than merely coexisting with membership/network simulation.

## Coverage in one deterministic timeline

- dynamic join
- crash/restart
- graceful leave/rejoin
- packet delay/drop/duplicate/reorder/partition/heal
- disk full
- slow disk
- I/O failure
- disk crash
- silent read/write corruption
- durable Put/Sync retry and verification

## Important limitation discovered

A scalar {TopologyEpoch, catalog_generation} record cannot make RuntimeCoordinator topology/resize commit crash-safe by itself. If the scalar is synced before the target range/catalog plan is durably available, a crash can leave a generation number that cannot be replayed.

RuntimeCoordinator durability is intentionally not wired to the scalar record yet. The next implementation must persist replayable topology plan/snapshot material before publication.

See docs/architecture/DURABLE-TOPOLOGY-COMMIT-v0.md.
