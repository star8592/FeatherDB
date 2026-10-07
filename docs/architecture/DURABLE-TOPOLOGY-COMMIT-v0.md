# Durable Topology Commit v0

Status: design constraint discovered by deterministic disk integration; implementation not yet complete.

## Problem

FeatherDB now has explicit Put/Sync durability semantics and can inject disk-full, delay, I/O failure and silent corruption.

Persisting only TopologyEpoch + catalog_generation before applying an in-memory resize/topology change is not sufficient.

Failure example:
1. plan says generation 10 -> 11;
2. {epoch, generation=11} is synced;
3. process crashes before RangeResizePlan / target catalog is applied;
4. restart reads generation 11 but has no durable target map or plan to reconstruct generation 11.

Therefore a scalar ControlRecord is useful for disk semantics testing but is not the final topology transaction format.

## Required invariant

A durable control-plane commit must contain enough information that recovery can deterministically reach one valid state without guessing.

durable transition authority => durable replay material

Generation numbers alone are not replay material.

## Candidate transaction structure

A topology/resize transaction should carry at least:
- transaction id
- operation kind
- from/to TopologyEpoch
- from/to catalog generation
- target metadata or fully replayable plan
- payload checksum/hash
- transaction phase

For tablet resize, replay material must reconstruct the target RangeTabletMap exactly, including stable TabletIds and range boundaries. Future data-aware split boundaries make regeneration from only old state + policy unsafe.

## Candidate durability sequence

1. construct target plan/snapshot in memory
2. write PREPARED transaction with replay payload
3. Sync
4. apply/replay transition idempotently
5. write COMMITTED marker / new authoritative snapshot
6. Sync
7. only then publish new authority to external actors

A crash after step 3 can replay the prepared transaction; a crash before step 3 leaves the old durable state authoritative.

## Recovery rule

Recovery must never infer a missing plan from a generation number.

It should read the last valid durable snapshot plus a checksummed topology transaction log, then ignore never-durable prepares, replay valid PREPARED transactions according to explicit recovery rules, validate COMMITTED references, and stop safely on unrecoverable corruption.

## Interaction with ownership migration

Topology metadata authority and bulk data movement remain separate. MigrationTask queues remain reconstructible work. Recovery truth should remain durable topology/catalog state + durable actual ownership + durable desired ownership, not a persistent queue of every future migration task.

## Interaction with membership

SWIM Alive/Suspect/Dead/Left is failure evidence, not durable topology authority. Membership may propose a topology transaction, but only the durable control-plane transaction can advance TopologyEpoch or remove ownership.

## Why scalar checkpointing is not wired to RuntimeCoordinator

A convenience persist-current-epoch call would look durable while preserving the crash window above. RuntimeCoordinator durability is therefore deferred until replay material is included.

## Next implementation

1. define checksummed topology snapshot encoding;
2. encode/decode RangeTabletMap / CompactTabletCatalog with stable IDs;
3. define PREPARED/COMMITTED transaction records;
4. add append/sync/recovery behavior to DurableStore usage;
5. crash at every state-machine edge;
6. inject V3 disk faults at every edge;
7. only then route RuntimeCoordinator topology/resize publication through the durable transaction layer.

## Executable snapshot and PREPARED layer

The first replay-material layer is now implemented.

TopologySnapshot deterministically encodes the full RangeTabletMap lifecycle state: stable TabletIds, u128 range boundaries, replica sets, bytes, generation, next TabletId and resize cooldown metadata.

PreparedTopologyTxn persists transaction id, source epoch/generation and the complete target snapshot. The PREPARED record and target snapshot are independently checksummed.

DurableTopologyTxnWriter now models:

    PREPARED Put -> Sync -> Prepared
    explicit apply acknowledgement
    CURRENT Put -> Sync -> Complete

Recovery returns Current, ReplayPrepared, Empty or RecoveryConflict. It never reconstructs a target from generation numbers alone.

Crash tests cover pre-PREPARED-sync, post-PREPARED/pre-apply, and post-current-Put/pre-current-Sync boundaries.

RuntimeCoordinator wiring remains the next step; the replay substrate is now executable rather than only documented.


## RuntimeCoordinator durable resize integration

The replayable topology transaction is now wired into coordinated tablet resize through a thin orchestration layer rather than embedding disk concerns inside MigrationScheduler.

The order is:

    capture current runtime snapshot
      -> derive exact target lifecycle from LifecycleResizePlan
      -> PREPARED Put
      -> Sync
      -> stop at observable Prepared boundary
      -> RuntimeCoordinator.commit_resize()
      -> verify captured runtime snapshot == prepared target
      -> CURRENT Put
      -> Sync
      -> Complete

A newly durable PREPARED record is never applied in the same simulator tick that made it durable. This intentional boundary lets deterministic tests crash precisely after prepare and before in-memory apply.

### Recovery

recover_tablet_runtime() reads CURRENT/PREPARED and reconstructs TabletRuntimeCoordinator directly from the chosen TopologySnapshot.

If recovery returns ReplayPrepared, the runtime is reconstructed from the prepared target and a resume-publish writer continues CURRENT publication. Post-resize migration work is regenerated from the recovered range/catalog state rather than persisted as queued tasks.

### Crash matrix

The implementation now crashes at every durable resize state-machine edge:

    PreparePutPending  -> old CURRENT
    PrepareSyncPending -> old CURRENT
    Prepared           -> ReplayPrepared target
    PublishPutPending  -> ReplayPrepared target
    PublishSyncPending -> ReplayPrepared target
    Complete           -> new CURRENT

Every ReplayPrepared case can finish CURRENT publication and a second recovery then returns stable Current(target).

DiskFull is separately tested both before PREPARED and during CURRENT publication. Before PREPARED it cannot mutate RuntimeCoordinator. During publication the in-memory target may already be applied, but durable PREPARED remains sufficient to recover the same target after crash.

### Serialization assumption

This v0 path assumes one serialized durable topology transaction at a time. A concurrent independently committed topology epoch while a PREPARED resize exists would produce RecoveryConflict rather than guessing an order. Future control-plane consensus must serialize these transactions explicitly.


## Full control-plane snapshot (FTS2)

The durable snapshot has been upgraded from range-only replay material to full control-plane topology replay material. FTS2 stores:

    TopologyEpoch
    replication factor
    ordered node map
      NodeId
      weight
      zone
      rack
      AdminState
    RangeTabletMap generation
    next TabletId
    resize cooldown state
    stable tablet IDs / ranges / bytes / replicas
    checksum

This removes an unsafe recovery dependency where callers previously had to supply an external Cluster matching the durable epoch. Runtime recovery now reconstructs Cluster directly from the snapshot.

The snapshot codec is deterministic because nodes are serialized in BTreeMap order and range tablets are serialized in canonical range order.

## Durable topology-epoch change

The same PREPARED/CURRENT transaction protocol now handles node-topology changes as well as resize.

For a topology change:

    current full snapshot
      -> replace cluster topology with explicit proposed Cluster
      -> PREPARED + Sync
      -> RuntimeCoordinator.reconcile_topology()
      -> verify full applied snapshot == prepared target
      -> CURRENT + Sync

A higher TopologyEpoch with unchanged catalog generation is a valid forward transition. Equal or older epochs are rejected before PREPARED.

A crash after PREPARED but before reconcile reconstructs the new Cluster directly from the target snapshot and regenerates ownership migration from recovered actual ranges plus new placement policy.

## Failure detector authority boundary

SWIM membership state is now covered by an executable integration invariant:

    Dead / Left observation != ownership authority

A test crashes a node until all live SWIM observers classify it Dead. RuntimeCoordinator remains on the old TopologyEpoch and the node remains Active in durable cluster metadata. Only an explicit durable topology transaction advances the epoch, changes AdminState to Removed, and triggers ownership Repair.

This keeps failure evidence and ownership authority deliberately separated.
