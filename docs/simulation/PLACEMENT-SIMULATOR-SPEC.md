# Placement Simulator Specification

Status: V0 design

The simulator exists to falsify FeatherDB placement ideas before production networking or storage code makes them expensive to change.

## Deterministic state

All nondeterminism must be seed-driven.

State includes:
- logical clock
- topology epoch
- node identities
- node capacities
- node health
- failure-domain labels
- tablet sizes
- tablet heat
- replica assignments
- migration queue
- in-flight migrations

A failing run must be replayable using one seed.

## Core node model

Each node exposes:
- cpu_units
- memory_bytes
- disk_bytes
- disk_free_bytes
- network_units
- failure_domain
- administrative_state
- health_state

Administrative state:
- joining
- active
- draining
- removed

Health state:
- healthy
- suspect
- unavailable

## Workloads

### Balanced baseline

Homogeneous nodes, uniform tablet sizes and request rates.

### Heterogeneous cluster

Example:

    N1  1 CPU   512 MiB   20 GiB
    N2  2 CPU     2 GiB   80 GiB
    N3  8 CPU    16 GiB  500 GiB
    N4 32 CPU   128 GiB    4 TiB

Expected property:
responsibility should converge roughly in proportion to configured capacity without starving small nodes or overloading them.

### Weak-node join

Add a small node to a large cluster.

Expected:
- small but nonzero assignment
- no mass reshuffle
- bounded migration

### Strong-node join

Add a high-capacity node.

Expected:
- larger responsibility transfer
- migration still bounded

### Node drain

Drain one node while foreground load continues.

Expected:
- no new placement on draining node
- gradual movement
- replication invariant preserved

### Abrupt failure

Kill a replica holder.

Expected:
- emergency repair has priority over balancing
- no duplicate replicas on same failure domain where avoidable

### Failure-domain loss

Remove an entire rack/zone.

Expected:
- policy violation becomes explicit
- planner chooses the safest degraded state
- no false claim of full redundancy

### Telemetry oscillation

Alternate observed load every tick.

Expected:
- placement does not oscillate at telemetry frequency

### Tablet-hotspot scenario

A small fraction of tablets receive most traffic.

Expected:
- no single node accumulates disproportionate hot tablets when alternatives exist
- hotness-aware changes remain bounded

### Churn storm

Repeated joins, drains, crashes, and recoveries.

Expected:
- convergence after churn stops
- bounded memory
- bounded migration queue
- no stale topology operation can resurrect removed ownership

## Metrics

Record:

- coefficient of variation for disk utilization
- coefficient of variation for weighted responsibility
- maximum node utilization
- replica-domain violation count
- bytes moved
- tablets moved
- migration queue depth
- time to convergence
- planner CPU operations
- planner memory estimate
- stale-epoch rejection count
- number of placement oscillations

## Candidate algorithms

Simulator must support interchangeable strategies:

1. consistent-hash baseline
2. jump-hash baseline where applicable
3. weighted rendezvous
4. constrained weighted rendezvous
5. optional CRUSH-inspired hierarchy strategy

No strategy wins by benchmark throughput alone. The selection criterion is the best tradeoff across correctness, movement, resource cost, and simplicity.

## Initial acceptance scenarios

A V0 strategy should pass at minimum:

- 3 nodes / RF=3
- 5 nodes / RF=3
- 10 nodes / RF=3
- 100 nodes / RF=3
- heterogeneous 4-node cluster
- one-node join
- one-node drain
- one-node crash
- 30% packet/event loss in control notifications
- noisy capacity telemetry
- 100 sequential join/leave operations

## Principle

Target placement may change faster than data can move.

Therefore the simulator must maintain two distinct states:

    desired_replica_set
    actual_replica_set

and explicitly model the transition between them.

This separation is fundamental to FeatherDB's elasticity design.
