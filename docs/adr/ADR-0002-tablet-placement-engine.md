# ADR-0002: Tablet Placement Engine

Status: Proposed, not accepted

## Context

FeatherDB targets clusters built from heterogeneous and potentially low-end machines. Nodes must be easy to add, drain, replace, and remove without manual shard planning.

Direct consistent hashing is simple but couples logical ownership too closely to physical nodes and does not adequately express failure domains, heterogeneous capacity, or bounded rebalance.

## Decision under evaluation

Introduce a logical tablet layer.

    key -> tablet -> replica set

The tablet map is topology metadata.

Replica selection is computed from:
- eligible nodes
- failure-domain constraints
- smoothed node weights
- deterministic ranking
- existing assignments

Weighted rendezvous hashing is the leading candidate for deterministic candidate ranking, but is not itself the whole placement algorithm.

## Required properties

### Determinism

Given the same:
- topology epoch
- node set
- policy
- tablet id

all correct planners must produce the same target replica set.

### Heterogeneous capacity

A 512 MB node and a 128 GB node are both legal cluster members, but they must not receive equal responsibility by default.

### Failure-domain awareness

Placement must support at least:
- node
- host
- rack
- zone

The policy must degrade explicitly when there are insufficient domains instead of silently pretending redundancy exists.

### Minimal movement

A topology change should move only the tablets needed to restore policy and balance.

### Bounded execution

Computing a target placement and physically migrating data are separate phases. Rebalance must be budgeted and backpressured.

### Stable weights

Telemetry affects placement through slowly changing capacity classes/epochs, not instantaneous measurements.

## Rejected simplifications

### Pure consistent-hash ring

Rejected as the sole ownership mechanism because arbitrary weights, failure domains, and migration control become cumbersome.

### Jump Hash as complete placement policy

Rejected because sequential bucket semantics and missing failure-domain/weight policy make it insufficient.

### Full CRUSH clone

Rejected for V0 because FeatherDB does not need Ceph's complete policy language and device hierarchy.

### Per-tablet Raft

Rejected for V0 because it introduces substantial per-tablet runtime and protocol overhead. Stronger consistency modes remain a separate design problem.

## Open questions

1. How many tablets should exist per GiB and per node?
2. Should tablets be fixed-size logical ranges or dynamically split/merge?
3. How is hotness incorporated without causing placement oscillation?
4. Can target placement be computed locally from a compact topology snapshot?
5. How much tablet metadata can a 256 MB node safely retain?
6. What is the failure behavior if the planner crashes after target-map commit but before migration?
7. How should a weak node advertise a trustworthy capacity weight?
8. Do we need separate storage-capacity and request-throughput weights?

## Acceptance gate

This ADR must not become Accepted until deterministic simulation demonstrates:
- no replica-domain violations under supported topology
- bounded movement on join/leave
- convergence under repeated churn
- no unbounded migration queue
- stable behavior under noisy telemetry
- acceptable metadata cost on low-memory nodes
