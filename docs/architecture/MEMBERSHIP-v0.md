# Membership / SWIM Core v0

Status: executable protocol prototype on the deterministic message bus.

## Purpose

FeatherDB needs decentralized failure detection that:

- has no master membership registry;
- scales without all-to-all heartbeat traffic;
- tolerates loss and asymmetric reachability;
- separates transient suspicion from durable topology authority;
- can run under the same deterministic network simulator as migration/control/data protocols.

The first executable membership layer follows SWIM's core structure and adds a deliberately limited Lifeguard-inspired Local Health Awareness mechanism.

It is not yet a complete Lifeguard/memberlist implementation.

## Research basis

SWIM separates failure detection from dissemination.

Its failure detector uses periodic randomized peer probing, indirect probes, and a Suspect stage before declaring failure. Membership updates are spread by infection-style piggybacking on probe traffic.

Canonical reference:

- Das, Gupta, Motivala, “SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol,” DSN 2002.
- https://doi.org/10.1109/DSN.2002.1028914
- https://research.google/pubs/swim-scalable-weakly-consistent-infection-style-process-group-membership-protocol/

Lifeguard identifies an important SWIM failure mode: a locally overloaded detector can mistake its own slow processing for remote failure. It introduces local-health awareness to reduce false positives.

Reference:

- Dadgar, Phillips, Currey, “Lifeguard: Local Health Awareness for More Accurate Failure Detection.”
- https://arxiv.org/abs/1707.00788

HashiCorp memberlist is a mature SWIM-derived implementation and explicitly incorporates Lifeguard extensions.

Reference:

- https://github.com/hashicorp/memberlist

A current Rust memberlist implementation also demonstrates a useful architecture for FeatherDB: a Sans-I/O state-machine core separated from clocks/transports.

Reference:

- https://github.com/al8n/memberlist

## State

Each node maintains a weakly consistent map:

    NodeId -> {
        incarnation
        status
    }

Status ordering:

    Alive < Suspect < Dead

Incarnation dominates status ordering.

For a remote member:

- higher incarnation always supersedes lower;
- at equal incarnation, more severe status supersedes less severe status;
- therefore stale Alive at the same incarnation cannot resurrect Suspect/Dead.

## Self-refutation

A node is authoritative for its own liveness incarnation.

If it receives a Suspect/Dead update about itself at an incarnation at least as new as its current one, it:

1. increments its incarnation;
2. writes self=Alive;
3. queues the new Alive update for dissemination.

This lets a live node refute false suspicion without a central authority.

A real network test proves that a target can refute through a third party even while the original observer-target direct link remains partitioned.

## Probe protocol

Probe identity is:

    (origin NodeId, sequence)

### Direct probe

Origin sends:

    Ping(origin, sequence, target)

Target returns:

    Ack(origin, sequence, target)

### Indirect probe

If the direct Ack does not arrive before the direct deadline:

1. origin deterministically selects up to K Alive helpers;
2. origin sends PingReq to each helper;
3. helper records a bounded relay;
4. helper sends Ping to target;
5. target Ack returns to helper;
6. helper forwards Ack to original probe origin.

Indirect paths therefore tolerate a failed direct link when another route exists.

## Deterministic peer selection

Production SWIM relies on randomized peer selection.

The simulator uses deterministic pseudo-random ranking derived from:

    local NodeId
    probe sequence
    candidate NodeId

This preserves repeatability while avoiding fixed round-robin correlation.

Same initial state + same seed/fault trace therefore yields the same probe decisions and results.

## Suspicion

A fully failed direct+indirect probe does not immediately create Dead.

It creates:

    Suspect(node, incarnation)

with a suspicion deadline.

If a higher-incarnation Alive arrives before expiry, suspicion is removed.

If the deadline expires while the same incarnation remains Suspect:

    Dead(node, incarnation)

is disseminated.

## Piggyback dissemination

Membership changes are queued as bounded MemberUpdate records.

Every Ping/Ack/PingReq can carry up to:

    piggyback_updates

updates.

Each update has a bounded retransmission budget.

Backpressured sends do not consume the retransmission attempt; sends accepted by the network do.

This keeps dissemination bounded rather than maintaining an unbounded gossip history.

## Local Health Awareness subset

The v0 implementation includes one Lifeguard-inspired mechanism:

    local awareness score

A fully failed probe increments local awareness up to a configured maximum.

A successful probe decrements it.

Timeouts are scaled as:

    effective_timeout = base_timeout * (awareness_score + 1)

This applies to probe/indirect/suspicion timing and probe cadence in the current model.

The intent is:

    if my detector is behaving poorly,
    become less aggressive about declaring peers unhealthy.

This is only an LHA subset.

Not yet implemented from the broader Lifeguard/memberlist family:

- full NACK semantics;
- Lifeguard's complete suspicion-confirmation behavior;
- buddy-system suspicion notification;
- all dynamic suspicion timeout formulas;
- push/pull anti-entropy.

Do not describe FeatherDB v0 membership as “full Lifeguard.”

## Simulator RTT invariant

SimNetwork intentionally prevents hidden same-tick reentrancy.

A zero-delay message sent in tick T becomes processable at a later event-loop boundary.

Therefore the minimum modeled round trips are:

    direct Ping -> Ack = 2 ticks
    PingReq -> Ping -> Ack -> forwarded Ack = 4 ticks

MembershipConfig rejects timeout values shorter than those simulator RTT minima.

This invariant was added after the first 100-node experiment exposed systematic premature direct timeouts.

## Crash and restart

MembershipCluster can crash a node:

- it stops processing protocol messages;
- queued incoming messages are drained/discarded;
- other nodes continue probing and disseminating suspicion/death.

Restart:

- increments the node's self incarnation;
- resets local probe/relay state;
- announces Alive at the new incarnation.

Higher-incarnation Alive can therefore supersede the old Dead view.

## Shared network substrate

Membership does not have a private simulator.

It uses:

    SimClock
    SimNetwork
    MessageClass::Membership

and therefore shares link fault state with:

    Gossip
    Control
    Data
    Repair
    Client
    MigrationTransport

This is important: membership cannot assume a healthier network than data movement sees.

## Executable invariants

Tests verify:

- membership wire encode/decode round-trip;
- malformed membership payloads are counted and ignored;
- timeout configuration cannot be shorter than simulated RTT;
- a healthy zero-delay cluster remains entirely Alive;
- a healthy zero-delay cluster does not unnecessarily enter indirect probing;
- crash eventually converges to Dead at all live observers;
- indirect probing survives a direct observer-target partition;
- self suspicion is refuted by a higher incarnation;
- stale Alive at the same incarnation cannot resurrect Suspect;
- higher-incarnation Alive refutes Suspect;
- Local Health Awareness increases timeout after failed probes and recovers after success;
- restart uses a higher incarnation and converges from Dead to Alive;
- duplicate packets do not corrupt probe state;
- false suspicion can be refuted through a third node while the original direct link remains down.

## 100-node deterministic campaign

A release-mode campaign runs:

    100 membership nodes
    shared SimNetwork
    seed = 8592
    1,200 generated fault episodes
    1,676 concrete fault events
    Delay/Drop/Duplicate/Reorder/Partition/Heal
    node 100 crash at tick 120
    node 100 restart no earlier than tick 900

Fault campaign last tick:

    1833

Results after RTT tuning:

    crash node globally Dead by tick 163
    crash -> global Dead latency = 43 ticks

    final all 100 x 100 membership views Alive by tick 1834

Protocol totals:

    direct probes = 45,152
    indirect rounds = 620
    suspects created = 7
    dead updates created = 39
    updates applied = 251
    peak local awareness score = 1
    message-bus backpressure = 0

Final deterministic membership digest:

    16870183601735872677

The entire campaign is executed twice and produces the same result.

Release-process peak RSS in the campaign:

    ~4.1 MB

These values are architecture-test measurements on the current machine/build, not product performance claims.

## Important experiment correction

The first run mistakenly configured:

    direct timeout = 1 tick
    indirect timeout = 2 ticks

while the explicit event loop requires minimum 2/4 tick round trips.

That run entered indirect probing on nearly every direct probe:

    direct probes = 45,611
    indirect rounds = 45,490

After correcting the timing invariant:

    direct probes = 45,152
    indirect rounds = 620

This is exactly why deterministic simulation is being built before production networking: an apparently “stable” failure detector can still hide severe protocol overhead and false-timeout behavior.

## Separation from topology authority

Membership status is runtime evidence.

It must not directly rewrite durable tablet ownership or TopologyEpoch.

The intended path remains:

    SWIM/LHA observations
      -> health/failure evidence
      -> topology decision/control plane
      -> new TopologyEpoch
      -> desired placement
      -> fenced migration

A transient Suspect is not a durable RemoveNode transaction.

## Current limitations

The v0 cluster starts with a pre-seeded member list.

Still missing:

- dynamic join/bootstrap;
- graceful leave;
- membership metadata authentication;
- public-key node identity binding;
- WAN/site-aware probe policy;
- push/pull anti-entropy;
- bounded dissemination analysis under very large membership churn;
- CPU-stall injection tied to LHA;
- full Lifeguard extensions;
- control-plane policy converting failure evidence into durable topology changes.

These are deliberately explicit rather than hidden behind a “SWIM complete” label.

## Next step

The highest-value next membership step is dynamic join/leave/bootstrap on the same message bus, followed by faulted churn campaigns.

Only after membership behavior is credible should failure evidence drive automatic durable topology replacement/removal.
