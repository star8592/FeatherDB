# Deterministic Fault Engine v0

Status: executable foundation, not a complete distributed-system simulator.

## Purpose

FeatherDB needs failures to be reproducible by seed instead of relying on nondeterministic stress tests that cannot replay the exact interleaving.

The simulator therefore introduces a deterministic event trace that directly drives existing production-intent state machines.

The first supported fault surface is runtime node health because MigrationScheduler and Repair already expose executable behavior for:

    Healthy
    Suspect
    Unavailable

Network and disk faults are intentionally deferred until those I/O surfaces have deterministic adapters.

## Event model

Each event has:

    tick
    sequence
    action

The current action is:

    SetNodeHealth { node_id, health }

Events are totally ordered by:

    (tick, sequence)

This removes host scheduling/time ordering from the test result.

## Seeded generation

FaultTrace::generate_health_flaps uses an internal SplitMix64 stream.

Given the same:

    seed
    node list
    flap count
    max gap
    max down duration

it generates exactly the same event trace.

Different seeds explore different schedules.

## Stable persisted trace

Fault traces have a versioned text format:

    feather-fault-trace-v1,<seed>
    <tick>,<sequence>,node-health,<node_id>,<health>

The format round-trips byte-for-byte.

This allows CI/simulator failures to persist the full trace rather than only a prose description.

## Replay

replay_migration_faults:

1. applies all events for the current tick;
2. executes the real MigrationScheduler tick;
3. records bytes, completions and source failovers;
4. stops only after all events have been consumed and the scheduler converges, or max_ticks is reached.

The fault runner does not contain a parallel migration/repair implementation.

## Current invariants

Executable tests verify:

- same seed -> identical trace;
- different seed -> different trace;
- text trace round-trip is stable;
- event order is deterministic;
- same trace + same starting state -> same replay report and final placement;
- transient runtime health faults do not rewrite topology ownership;
- bounded health faults eventually converge after faults stop.

## Fixed-seed lab

Configuration:

    seed = 8592
    tablets = 128
    RF = 3
    one owner durably Removed
    two surviving repair sources
    40 health flaps = 80 events

Observed:

    last_fault_tick = 206
    trace_bytes = 2457
    converged = true
    ticks = 271
    events_applied = 80
    source_failovers = 1262
    completed_repairs = 128
    remaining_bytes = 0

The large failover count is expected because each tablet Repair task independently reselects a source when its current source becomes unavailable.

## Relationship to mature DST systems

FoundationDB integrates deterministic simulation into the same asynchronous runtime used by production logic and reuses seeds to reproduce failures.

TigerBeetle similarly replaces nondeterministic clock/network/disk behavior in its VOPR and uses seed + commit identity to replay failures.

FeatherDB's current v0 is smaller: only runtime health is controlled today. The important architectural rule is already established:

    simulator event injection must drive real protocol/state-machine code,
    not a disconnected mock implementation.

## Next fault surfaces

1. deterministic simulated clock;
2. network Delay/Drop/Duplicate/Reorder/Partition/Heal;
3. disk full and slow I/O;
4. copy target failure;
5. CPU stall / delayed scheduler;
6. topology join/leave churn;
7. persistent failure traces from randomized campaigns.

Every new fault surface must be introduced through an abstraction shared with production-intent code.
