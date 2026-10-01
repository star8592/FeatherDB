# ADR-0001: Hybrid control plane and leaderless data plane

Status: **Proposed / research-stage**

## Context

Pure gossip is attractive for availability and simplicity of local participation, but topology mutation is not merely failure detection. Join, decommission, replace, ownership transfer and schema/placement changes require a single durable interpretation of what has committed.

Conversely, putting every data shard/tablet behind an independent consensus group adds persistent protocol, memory, timer and operational overhead that conflicts with FeatherDB's low-resource target.

## Proposed direction

Use two deliberately different mechanisms:

### Data plane

- default leaderless replication
- any eligible node can coordinate a request
- per-namespace/per-operation consistency policy
- eventual/quorum/causal modes first
- anti-entropy repair and explicit conflict semantics

### Control plane

Use a very small consensus-backed metadata state machine only for durable cluster facts such as:

- topology epoch
- durable membership
- tablet ownership/placement intent
- schema/namespace metadata
- operation IDs and topology transaction state

SWIM/Lifeguard-style mechanisms remain failure detectors and dissemination accelerators; they are not the authority for destructive membership changes.

## Why

This aims to retain leaderless data availability while making topology transitions deterministic and replayable. It also keeps consensus overhead proportional to control metadata rather than number of data partitions.

## Constraints

- No external etcd/ZooKeeper/PD dependency.
- Control plane ships in the same `featherd` binary.
- Loss of metadata quorum must not corrupt committed ownership.
- Existing data operations may continue during a control-plane pause when their selected consistency policy permits it.
- Topology metadata must be streamable/snapshot-able and resource bounded.
- A 1-node development mode must exist without pretending it has distributed fault tolerance.

## Alternatives rejected for V0

1. **Gossip as topology authority** — too ambiguous under concurrent/failing membership changes.
2. **Raft per tablet** — stronger semantics but too much default machinery for the target footprint.
3. **External coordination service** — violates single-binary/zero-external-dependency goal.
4. **Blockchain/global append-only chain** — global ordering and consensus cost do not solve the target problem efficiently.

## Validation gates before acceptance

ADR stays Proposed until deterministic simulation demonstrates:

- crash at every join/leave/replace transition boundary;
- stale-incarnation fencing;
- metadata leader changes during active operations;
- quorum loss/recovery;
- concurrent independent tablet movement;
- bounded queues during event storms;
- no O(global tablet count) transient allocation for a single topology operation.

If these cannot be achieved simply, revisit the architecture rather than layering patches on top.
