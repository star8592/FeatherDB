# Deterministic Message Bus v0

Status: executable simulator substrate.

## Purpose

The first deterministic network implementation was migration-specific.

That was sufficient to prove Delay/Drop/Duplicate/Reorder/Partition/Heal against Repair and migration, but future Membership, Gossip, Control-plane and Data-replication protocols must not each invent their own simulator transport.

The message bus turns SimNetwork into a shared deterministic network substrate.

## One fault surface

MigrationTransfer and generic protocol messages use the same per-link state:

    delay_ticks
    drop_next
    duplicate_next
    reorder_next
    partition state

There is no second fault engine.

If a Gossip packet consumes the next configured DropNext event on link A->B, the following Migration transfer on A->B does not also receive an independent copy of that fault.

This is deliberate.

The simulator models one physical/logical network shared by all protocol classes.

## Message classes

The bus currently distinguishes:

    Membership
    Gossip
    Control
    Data
    Repair
    Client

The class is metadata for protocol tests and observability.

Fault policy is link-level today, not class-specific.

Class-specific QoS or fault policy must not be added until a concrete protocol requires it.

## Message model

A queued message carries:

    message_id
    from
    to
    class
    payload bytes

Duplicate injection creates multiple physical deliveries with the same logical message_id.

This lets future protocol tests verify idempotency and duplicate suppression at the protocol layer.

## Explicit virtual-time delivery

send_message(now, ...) never directly invokes recipient protocol code.

Even zero-delay packets enter the deterministic queue.

The event loop must explicitly call:

    advance_messages(now)

and then:

    recv_message(node)
    or
    drain_messages(node)

This prevents host call-stack/reentrancy order from becoming hidden scheduler state.

## Deterministic ordering

In-flight messages are ordered by:

    (deliver_at_tick, send_sequence)

Therefore:

- same seed + same sends -> same delivery order;
- messages scheduled for the same tick retain deterministic send sequence;
- ReorderNext works by adding deterministic extra delay to selected earlier sends;
- later messages can overtake them.

## Partition semantics

A partition can affect a packet at two boundaries.

### Send-time partition

If the directed link is already partitioned when send_message is called:

    MessageSend::Dropped

No packet enters the queue.

### Delivery-time partition

A delayed packet may have been accepted before a partition begins.

When its delivery tick arrives, advance_messages checks the link again.

If the link is partitioned at that point, the packet is dropped and removed.

A later Heal does not resurrect a packet already dropped at delivery.

This matches the existing migration transfer semantics.

## Bounded queue / backpressure

Unbounded simulator queues hide real overload bugs.

MessageBusLimits defines:

    max_buffered_messages
    max_buffered_bytes

Buffered accounting includes both in-flight and ready-but-not-consumed messages.

When a send would exceed either bound, the result is:

    MessageSend::Backpressure

The enqueue is all-or-nothing, including duplicate injection.

Protocol code must eventually decide whether to retry, shed, coalesce, or fail.

The network layer does not silently grow memory.

## Shared SimNetwork

SimNetwork now owns both:

    migration in-flight chunks
    generic protocol packets

The migration adapter still exposes the existing MigrationTransport API.

Generic messages use send/advance/recv.

Both paths call the same internal link_disposition() function.

This is the core architectural property.

## Network-only FaultTrace application

FaultTrace network actions can now be applied without constructing a MigrationScheduler.

The helper:

    apply_network_fault_action()

handles:

    link-delay
    drop-next
    duplicate-next
    reorder-next
    partition
    heal

and explicitly returns false for node-health events.

This lets future protocol simulations reuse the exact same V2 fault trace format.

## Protocol-independent replay harness

message_replay.rs adds:

    ScheduledMessage
    DeliveredMessage
    MessageReplayReport
    MessageReplayResult
    replay_message_schedule()

The harness:

1. sorts scheduled protocol sends by (tick, sequence);
2. runs one SimClock;
3. applies FaultTrace events at the exact virtual tick;
4. sends all messages for that tick;
5. advances the shared network;
6. drains recipients deterministically;
7. records the complete delivery trace;
8. records queue peaks, drops, duplicates/backpressure effects;
9. stops only after all faults/sends are consumed and the network drains, or max_ticks is reached.

This is protocol-neutral.

Membership, control-plane or replication tests can build ScheduledMessage values without changing the network simulator.

## Fixed-seed mixed-protocol experiment

Seed:

    8592

Directed links:

    1 -> 2
    2 -> 3
    3 -> 1

Protocol classes sent every tick:

    1 -> 2 Membership
    2 -> 3 Control
    3 -> 1 Data

Fault campaign:

    generated network episodes = 120
    fault events = 171
    last fault tick = 246

Message schedule:

    logical sends = 837

Replay result:

    drained = true
    ticks = 279
    network fault events applied = 171
    sends queued = 731
    sends dropped at send time = 106
    physical copies queued = 766
    delivery-time partition drops = 11
    delivered messages = 755
    delivered bytes = 12,080
    peak buffered messages = 9
    peak buffered payload bytes = 144

Delivered by class:

    Membership = 251
    Control = 238
    Data = 266

Stable delivery digest:

    2716594287323703107

A second replay of the same trace and schedule produces the same full result and delivery list.

## Executable invariants

Tests verify:

- generic zero-delay send still requires explicit advance;
- duplicates preserve logical message_id;
- explicit reorder allows later packets to overtake earlier packets;
- partition after send can drop at delivery boundary;
- bounded queue returns explicit backpressure;
- backpressure is all-or-nothing;
- Migration and generic messages share the same DropNext budget;
- ready delivery order is deterministic;
- network faults can be applied without MigrationScheduler;
- same FaultTrace + same protocol schedule -> byte-for-byte/equality-identical replay result;
- mixed message classes preserve class and payload;
- bounded replay reports backpressure deterministically.

## Alignment with mature DST

FoundationDB's simulation approach replaces nondeterministic physical interfaces and uses virtual time so production-intent logic can execute under a deterministic scheduler.

TigerBeetle VOPR similarly replaces clock/network/disk operations and injects packet loss, reordering and partitions.

FeatherDB now has the same architectural direction for its network surface:

    protocol logic
      -> shared message/transport abstraction
      -> direct/real implementation later
      -> deterministic SimNetwork in simulation

References:

- https://apple.github.io/foundationdb/testing.html
- https://apple.github.io/foundationdb/engineering.html
- https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md

This is alignment of mechanism, not a claim of comparable maturity.

## Remaining work

1. Run an actual Membership/SWIM state machine on the bus rather than pre-scheduled synthetic messages.
2. Run control-plane topology messages on the same bus.
3. Add deterministic disk adapter.
4. Add node crash/restart lifecycle to the shared event loop.
5. Add CPU stall/scheduler starvation.
6. Add class-aware metrics before class-aware QoS.
7. Persist failing message replay schedules automatically in CI.

## Decision

All future simulated protocols must use the shared deterministic message bus or a thin adapter over it.

No protocol-specific private network simulator should be introduced.
