# 100-Node SWIM Membership Experiment — 2026-10-07

## Goal

Run the first real protocol state machine on FeatherDB's shared deterministic message bus and verify reproducible failure detection/recovery under network faults.

## Protocol

Implemented subset:

- SWIM-style direct probing;
- indirect PingReq probing;
- Alive/Suspect/Dead;
- incarnation ordering;
- self-refutation;
- bounded piggyback dissemination;
- suspicion timeout;
- Lifeguard-inspired local health timeout scaling.

Not a full Lifeguard implementation.

## Network

Nodes:

    100

Fault seed:

    8592

Generated network episodes:

    1,200

Concrete fault events:

    1,676

Fault types:

    Delay
    Drop
    Duplicate
    Reorder
    Partition
    Heal

Last fault tick:

    1833

## Process failure

Node 100:

    crash tick = 120
    globally observed Dead tick = 163

Detection convergence:

    43 ticks

Node 100 is allowed to restart after tick 900.

Restart increments the node's incarnation and disseminates Alive.

Final requirement is stricter than recovery of node 100:

    every one of the 100 observers
    must see every one of the 100 members
    as Alive

after the bounded fault campaign ends.

This condition is satisfied at tick:

    1834

## Protocol totals

    direct probes = 45,152
    indirect rounds = 620
    suspects = 7
    dead updates = 39
    refutations in this random campaign = 0
    membership updates applied = 251
    peak awareness score = 1
    backpressure = 0

A separate targeted test forces a false suspicion and verifies a real self-refutation through a third party while the original direct path remains partitioned.

## Determinism

The complete campaign is run twice.

Final result, protocol counters and all membership views match.

Final view digest:

    16870183601735872677

## Resource observation

Release campaign observed peak RSS:

    ~4,144 KB

This is a simulator architecture measurement, not a production daemon memory claim.

## Timeout bug found by the campaign

First configuration:

    direct timeout = 1
    indirect timeout = 2

The deterministic event loop requires:

    Ping -> Ack = minimum 2 ticks
    PingReq -> Ping -> Ack -> forward = minimum 4 ticks

The invalid first configuration caused:

    45,611 direct probes
    45,490 indirect rounds

even though the cluster still eventually converged.

After adding explicit minimum-RTT configuration validation and changing timeouts to 2/4:

    45,152 direct probes
    620 indirect rounds

The experiment therefore caught a substantial hidden efficiency bug that ordinary “final state is correct” testing would have missed.

## Conclusion

The shared deterministic message bus is now exercised by a real decentralized failure detector rather than only synthetic message schedules.

The next protocol work should add dynamic membership bootstrap/join/leave and then subject that path to repeated churn.
