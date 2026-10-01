# FeatherDB Pre-Freeze Invariants v0

These are design constraints for experiments. They are intentionally stronger than a feature roadmap.

## Safety

1. **No acknowledged-write fabrication** — a read must never return a version that was never accepted by the replication protocol.
2. **Topology epochs are monotonic** — nodes cannot silently move back to an older committed topology.
3. **Replica ownership is explicit** — every tablet/epoch has a deterministic replica-set interpretation.
4. **Join/leave/replace are idempotent** — replaying a topology command cannot duplicate ownership or lose committed ownership transitions.
5. **Repair never destroys a causally newer value solely because another replica has an older wall-clock timestamp.**

## Liveness

6. A minority node failure must not stop writes for policies whose configured quorum remains satisfiable.
7. A recovered replica must be able to converge without full-cluster restart.
8. Interrupted tablet movement must resume or safely roll forward after process crash.
9. Persistent I/O failure must degrade visibly rather than spin in an unbounded retry loop.

## Elasticity

10. Joining a node must not require rewriting all cluster data.
11. Removing a node must be resumable and rate-limited.
12. Placement must support heterogeneous capacity; equal hardware is not an architectural assumption.
13. Rebalancing must have explicit bandwidth/CPU/memory budgets and foreground-work protection.

## Resource bounds

14. A tablet-local request must not allocate memory proportional to total cluster tablet count.
15. Membership traffic per node should remain approximately bounded as the cluster grows; validate experimentally.
16. Background repair/rebalance queues must be bounded and apply backpressure.
17. Every protocol structure that grows with cluster/tablet/key count must have a documented bound or eviction/compaction mechanism.

## Operability

18. The basic deployment is one executable with no mandatory external control-plane service.
19. A healthy small cluster should not require Kubernetes.
20. Common lifecycle operations must expose progress as structured state, not require log archaeology.
21. The database must explain *why* it is degraded: unavailable replicas, quorum deficit, disk pressure, topology transition, repair debt, etc.

## Simulation requirements

Every invariant that can be machine-checked should become a simulator assertion. The simulator must eventually inject at least:

- crash/restart
- message loss, duplication, delay and reordering
- network partitions
- slow nodes
- clock skew (where clocks are used)
- disk errors and partial persistence
- repeated join/leave/replace commands
- concurrent topology operations
- hot tablets
- constrained memory/disk/network budgets

A production protocol is not considered mature because a happy-path 3-node demo works.
