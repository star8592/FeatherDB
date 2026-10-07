# Mixed-Protocol Deterministic Message Bus Experiment — 2026-10-07

## Goal

Verify that Membership, Control and Data message classes can share one deterministic faulted network and reproduce the same delivery trace from the same seed.

## Setup

Seed:

    8592

Links:

    1 -> 2
    2 -> 3
    3 -> 1

Every virtual tick sends:

    Membership 1 -> 2
    Control    2 -> 3
    Data       3 -> 1

Each payload is 16 bytes and includes its class marker and source virtual tick.

Network faults come from the existing FaultTrace V2 generator.

## Fault campaign

    episodes = 120
    events = 171
    last fault tick = 246

Fault types include:

    Delay
    Drop
    Duplicate
    Reorder
    Partition
    Heal

The same link-fault state is shared with MigrationTransport.

## Result

    logical scheduled sends = 837
    sends queued = 731
    send-time drops = 106
    physical copies queued = 766
    delivery-time partition drops = 11
    delivered messages = 755
    delivered bytes = 12,080

Queue peak:

    9 messages
    144 payload bytes

Delivered classes:

    Membership = 251
    Control = 238
    Data = 266

Replay drained completely after:

    279 ticks

Stable delivery digest:

    2716594287323703107

The complete replay was executed twice and the MessageReplayResult values were exactly equal.

## Important observations

### Duplicate is a physical property

Duplicate injection increases physical queued copies while keeping the same logical message_id.

Protocol layers can therefore test duplicate suppression explicitly.

### Partition can race with delay

A packet accepted before a partition may still be dropped when its delivery tick arrives.

This is a materially different scenario from send-time partition and is now directly representable.

### Network faults are cross-protocol

A configured next-packet fault is consumed by whichever protocol packet uses that link next.

The simulator does not grant Membership, Control, Data and Migration independent fault universes.

### Queue memory is bounded

The replay harness records message and byte peaks and the bus has explicit hard limits.

There is no implicit unbounded Vec of future packets.

## Conclusion

The deterministic network layer is no longer migration-specific.

It is now suitable as the common transport substrate for the next Membership/SWIM and control-plane protocol simulations.
