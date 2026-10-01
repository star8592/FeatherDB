# Placement Algorithms Research

Status: research draft

## Goal

FeatherDB needs placement that works across heterogeneous machines, preserves failure-domain separation, minimizes unnecessary movement, and remains cheap enough for small nodes.

## Systems and algorithms studied

### Jump Consistent Hash

Strengths:
- tiny implementation
- no large routing table
- minimal movement when bucket count changes
- good balance for sequential bucket sets

Limitations:
- buckets are sequentially numbered
- awkward for arbitrary node removal
- does not natively express weights or failure domains
- does not by itself solve tablet migration scheduling

Conclusion: useful as a primitive or baseline, not sufficient as the placement policy.

Reference: Lamping & Veach, "A Fast, Minimal Memory, Consistent Hash Algorithm".

### Rendezvous / Weighted Rendezvous Hashing

Strengths:
- naturally ranks candidate nodes per key/tablet
- handles arbitrary node sets
- supports weighting more naturally than ring approaches
- easy to recompute independently

Risks:
- raw weighted HRW does not automatically encode rack/zone constraints
- weight changes can trigger excessive reshuffling unless bounded
- naive scoring can create instability when capacity telemetry oscillates

Conclusion: strong candidate for candidate ranking, but must sit behind explicit constraints and hysteresis.

### CRUSH

Strengths:
- decentralized placement
- explicit topology hierarchy
- replica separation across failure domains
- designed to minimize unnecessary movement
- policy can distinguish device classes and topology levels

Risks for FeatherDB:
- Ceph's full CRUSH model is more general and complex than we need
- reproducing CRUSH wholesale would violate the simplicity goal

Conclusion: borrow the failure-domain policy model and deterministic placement philosophy, not the entire Ceph machinery.

Reference: Weil et al., "CRUSH: Controlled, Scalable, Decentralized Placement of Replicated Data".

### ScyllaDB Tablets

Strengths:
- indirection between token range and physical node
- placement can evolve independently of hashing
- tablet movement is the unit of rebalance
- supports online scale-out and decommission
- recent Scylla versions balance based on actual disk usage and can split/merge tablets

Important lesson:
A tablet map is a better abstraction boundary than direct key->node hashing.

Risk:
Every tablet replica has metadata/runtime cost. Tablet count therefore must be explicitly resource-bounded.

## Proposed FeatherDB direction

Key mapping:

    key
      -> partition hash
      -> virtual tablet
      -> placement policy
      -> replica set

Placement inputs:

- node identity
- administrative state
- capacity weight
- disk free ratio
- memory budget
- CPU capacity
- network capacity
- failure domain labels
- current tablet assignments
- current migration budget

Hard constraints:

1. Never place two replicas of the same tablet on the same node.
2. If enough failure domains exist, replicas must be separated by the configured domain.
3. A node above hard disk/memory pressure limits is not eligible for new placement.
4. A draining node receives no new tablets.
5. A stale topology epoch cannot authorize placement changes.

Soft objectives:

1. Minimize peak utilization.
2. Minimize moved bytes.
3. Minimize number of simultaneous migrations.
4. Avoid hot-node concentration.
5. Respect heterogeneous capacity.
6. Avoid oscillation.

## Important design choice

Weights must not track instantaneous telemetry directly.

Instead:

    observed capacity
      -> smoothed capacity class
      -> bounded weight epoch
      -> placement decisions

This prevents a noisy CPU or disk metric from constantly reshuffling tablets.

## Movement budget

Placement and migration are separate concerns.

The ideal target map may change immediately, but physical movement must be rate limited.

Proposed controls:

- max migrations per node
- max bytes/sec per node
- max cluster-wide migration bytes
- per-disk I/O budget
- foreground-latency guard
- emergency override for failed replicas

## Initial hypothesis

Use:

- virtual tablets for indirection
- constrained weighted rendezvous for candidate ranking
- explicit failure-domain rules inspired by CRUSH
- a control-plane-owned tablet map
- bounded background migration

Do not use:
- direct key->node placement as the final ownership model
- instantaneous resource telemetry as placement weight
- unlimited rebalance
- one consensus group per tablet in V0

## Validation required

The deterministic simulator must measure:

- balance error
- moved bytes after join
- moved bytes after leave
- moved bytes after weight change
- replica failure-domain violations
- time to convergence
- maximum queued migration bytes
- behavior under churn
- behavior when one weak node joins a strong cluster
- behavior when telemetry oscillates
