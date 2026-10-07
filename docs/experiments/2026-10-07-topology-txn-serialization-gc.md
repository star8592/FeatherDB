# Topology Transaction Serialization + PREPARED GC — 2026-10-07

## Goal

Close the remaining single-slot topology transaction hazards before moving to real storage backends.

## Start guard

Every public durable resize/topology constructor now reads durable CURRENT/PREPARED before allocating new PREPARED work.

Required conditions:

    runtime snapshot == durable CURRENT
    no active PREPARED

Outcomes:

- no PREPARED: transaction may start;
- CURRENT == PREPARED.target: old PREPARED is stale-committed and may be overwritten;
- CURRENT == PREPARED.from: ActivePrepared, new transaction rejected;
- runtime != CURRENT: CurrentMismatch;
- unrelated CURRENT/PREPARED: RecoveryConflict.

## Executable guard tests

- active PREPARED resize blocks txn 301 from overwriting txn 300;
- runtime topology advanced in memory while disk remained old blocks the next topology transaction;
- a completed epoch-8 transaction leaves stale PREPARED, and epoch-9 transaction is allowed to replace it safely.

## PREPARED GC

PreparedTopologyGc removes only stale committed PREPARED records.

Tests verify:

1. stale committed PREPARED is deleted and synced;
2. active PREPARED is refused;
3. crash after Delete but before Sync restores PREPARED from the durable image, after which GC can retry and complete.

## Result

The single prepared slot is now serialized, replay-safe, and safely reclaimable without making the task queue durable truth.
