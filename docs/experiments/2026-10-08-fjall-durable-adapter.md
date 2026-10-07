# Fjall DurableStore Adapter Experiment — 2026-10-08

## Goal

Verify that Fjall 3.1.12 can implement FeatherDB's existing DurableStore semantics without changing the control-plane protocol.

## Adapter model

FjallDurableStore keeps Put/Delete mutations in an in-memory staged map. Read observes staged state first. Sync converts the complete staged set into one Fjall atomic WriteBatch with PersistMode::SyncAll and then clears staging.

Therefore the adapter preserves the simulator contract:

    Put/Delete success != durable commit
    Sync success = durable boundary

The adapter remains in experiments/ rather than the main workspace until the storage substrate is frozen.

## Current dependency

    Fjall 3.1.12
    Rust 1.99
    Edition 2024

Cargo update reports no newer compatible package.

## In-process tests

Seven tests pass:

1. unsynced Put is lost after crash/reopen;
2. synced Put survives reopen;
3. unsynced Delete rolls back, synced Delete persists;
4. PREPARED/CURRENT topology protocol survives real Fjall reopen;
5. stale PREPARED GC survives real Fjall reopen;
6. active PREPARED cannot be garbage-collected;
7. durable resize reconstructs RuntimeCoordinator and migration work from real Fjall.

## Process-level crash probe

A release crash_probe binary was run on the real ext4 storage path under /mnt/disk1.

Unsynced case:

    process stages Put
    prints READY
    external kill -9
    new process opens same database
    check-absent passes

Synced case:

    process stages Put
    SyncAll returns
    prints READY
    external kill -9
    new process opens same database
    check-present passes

Result:

    PASS-unsynced-lost
    PASS-synced-survives

This is stronger than an in-process reopen test because the writer process is terminated without normal destructors.

## Limitations

The process kill test is not a power-loss test and does not emulate drive write-cache lies, controller failure, torn sectors, or filesystem corruption. Those remain simulator/fault-injection concerns and later hardware-validation work.

## Decision impact

Fjall has now passed the first real adapter gate for FeatherDB control-plane durability semantics. This strengthens the single-engine Fjall V0 hypothesis, but does not yet freeze the data-plane storage choice.
