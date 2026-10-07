# Topology Snapshot + PREPARED Recovery Experiment — 2026-10-07

## Goal

Provide enough durable topology material to recover a tablet resize/topology transition without inferring a target state from epoch/generation numbers.

## Snapshot payload

TopologySnapshot preserves:

- TopologyEpoch
- RangeTabletMap generation
- next TabletId
- last resize tick / cooldown state
- every stable TabletId
- u128 range start/end, including the 2^64 hash-space end
- tablet bytes
- ordered replica NodeIds
- checksum

Snapshot encoding is deterministic and checksummed.

## PREPARED record

PreparedTopologyTxn persists transaction id, from TopologyEpoch, from catalog generation, complete target TopologySnapshot, and a wrapper checksum. The target snapshot itself has an independent checksum.

## Durable writer state machine

    PrepareIdle -> PreparePutPending -> PrepareSyncPending -> Prepared

Only after the caller confirms that the prepared transition has been applied in memory does publication continue:

    PublishIdle -> PublishPutPending -> PublishSyncPending -> Complete

Failures remember whether retry resumes preparation or publication.

## Recovery outcomes

Recovery returns exactly one of:

- Empty
- Current(snapshot)
- ReplayPrepared { txn_id, target }
- RecoveryConflict error

If CURRENT already equals the prepared target, CURRENT wins. If CURRENT matches PREPARED.from, PREPARED is replayable. An unrelated CURRENT causes RecoveryConflict.

## Crash tests

10 focused snapshot/transaction tests pass, including full lifecycle snapshot round-trip, full u64 token-space end preservation, corruption/truncation detection, deterministic encoding, PREPARED replay, crash before PREPARED Sync, crash after CURRENT Put before Sync, unrelated-current conflict, and corrupted PREPARED rejection.

## Remaining integration

RuntimeCoordinator does not yet publish through this transaction writer. The next step is to derive the target snapshot from the coordinated resize/topology plan, fsync PREPARED, apply idempotently, publish CURRENT, and recover the coordinator directly from TopologyRecovery.
