# Topology State Machine v0

Status: research hypothesis, not frozen architecture.

## Goal

Make join, leave, replace and failure recovery explicit, durable, idempotent state machines instead of operator scripts.

## Node identity

A node has a stable cryptographic `NodeId` independent of IP/hostname. A process incarnation has an `IncarnationId`. Reusing disks or addresses must never silently resurrect an old incarnation.

## Membership states

`Unknown -> Joining -> Active -> Draining -> Left`

Failure observation is orthogonal: `Alive | Suspect | Unreachable`. Failure detection must not itself mutate durable membership.

A failed join may transition to `JoinAborted`; a replacement is represented as a transaction linking old NodeId and new NodeId rather than pretending the new process is the old process.

## Epoch rule

Every committed topology mutation advances a monotonically increasing `TopologyEpoch`. Data movement is authorized against an epoch. A stale worker may finish copying bytes but must not publish ownership for an obsolete epoch.

## Join transaction

1. authenticate identity
2. register Joining(node, incarnation, capacity)
3. compute candidate tablet assignments
4. reserve bounded transfer budget
5. stream/verify replicas
6. commit ownership at epoch E+1
7. expose node as Active

Every step is retryable. Repeating the same operation ID must converge to the same state.

## Graceful leave

1. Active -> Draining
2. stop assigning new ownership
3. create replacement replicas elsewhere
4. verify target replicas
5. commit tablet ownership changes
6. Draining -> Left

A crash at any point must leave enough durable intent to resume or abort safely.

## Forced removal

Failure detector evidence alone is insufficient. Forced removal is a topology decision. Before dropping the old replica, the control plane verifies that the resulting placement policy is satisfiable or explicitly records degraded redundancy.

## Replace

Replacement is not `remove + add` hidden behind scripts. It is a first-class operation with old/new identities, operation ID, source epoch and target epoch. A returning old node is fenced by incarnation/epoch checks.

## Concurrency

Independent tablet movements may execute concurrently. Conflicting topology mutations serialize only on the metadata they actually conflict on. Avoid a global stop-the-world rebalance.

## Required invariants

- A tablet has exactly one committed ownership map for a topology epoch.
- No stale operation can publish ownership into a newer epoch.
- Membership mutation is idempotent by operation ID.
- Failure suspicion never equals durable removal.
- A returning stale node cannot regain ownership merely because it has old data.
- Data movement is bounded by memory, disk and network permits.
- A topology event is coalescible; duplicate notifications cannot create unbounded client refresh storms.
- Control-plane failure may pause topology progress but must not corrupt already committed data ownership.

## Open questions

- Is one tiny cluster-wide consensus group sufficient, or should metadata be partitioned later?
- How should a 1-2 node development cluster degrade when no metadata quorum exists?
- What is the minimum durable metadata needed on every data node?
- Can topology snapshots plus an append-only metadata log keep memory O(active changes + local ownership), not O(global tablets)?
