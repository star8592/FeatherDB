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

## Dynamic join/leave/rejoin follow-up

A second deterministic campaign starts with only 20 nodes and grows the live membership set dynamically.

Sequence:

    20 initial nodes
    +80 joining nodes
      - every 10th joiner's seed link temporarily partitioned
    => 100 joined

    20 graceful leaves
    => 80 joined / 20 Left

    10 explicit rejoins
      - selected seed links temporarily partitioned
    => 90 joined / 10 Left

Release result:

    join convergence tick = 215
    leave convergence tick = 216
    rejoin convergence tick = 315

Protocol counters:

    JoinReq = 200
    JoinResp accepted = 90
    graceful Leave operations = 20
    peak buffered messages = 100
    backpressure = 0

Final deterministic digest:

    14974812575840271653

Release peak RSS:

    ~4,464 KB

The campaign is executed twice and returns the same counters, convergence ticks and final digest.

A separate targeted test keeps a new node partitioned from its seed for 100 ticks, long enough to exceed the ordinary gossip retransmit budget. Join still succeeds after heal because JoinReq always carries the node's current self Alive/incarnation explicitly rather than depending on queued piggyback state.

`Left` is terminal at the same incarnation; explicit rejoin increments incarnation and therefore safely supersedes the old Left record.
