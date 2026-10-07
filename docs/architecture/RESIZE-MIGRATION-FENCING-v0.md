# Resize × Migration Generation Fencing v0

Status: executable conservative coordination policy.

## Problem

Tablet resize and ownership migration are individually safe only under their own state assumptions.

Resize changes:

    RangeGeneration
    TabletId set
    range boundaries
    compact catalog shape

Migration changes:

    actual replica ownership
    desired replica ownership
    in-flight transfer state

Running both independently creates a classic cross-state-machine hazard:

- resize may clone stale replica ownership into new child tablets;
- migration may commit ownership for TabletIds that no longer exist;
- a retried resize plan may be misclassified because post-resize migration has already started;
- topology epoch replacement can invalidate both at once.

Two correct modules therefore do not automatically compose into a correct runtime.

## Initial policy

FeatherDB v0 intentionally serializes authority-changing range and ownership commits.

The rule is:

    ownership migration must converge
      before RangeGeneration may commit

and after a resize commit:

    new RangeGeneration
      -> new CompactTabletCatalog
      -> actual ownership inherited from committed range map
      -> desired placement recomputed using stable new TabletIds
      -> bounded migration resumes if needed

This is a conservative correctness rule, not a claim that concurrent split+migration is fundamentally impossible.

ScyllaDB's mature tablet system can split tablets and later migrate the halves independently, but that is backed by its Raft topology machinery. Its current vnode-to-tablet migration procedure also explicitly forbids concurrent topology changes and repair while migration is in progress. That supports starting FeatherDB with serialized high-risk metadata transformations and relaxing the barrier only after stronger protocol evidence.

Official references:

- https://docs.scylladb.com/manual/stable/architecture/tablets.html
- https://docs.scylladb.com/manual/branch-2026.3/operating-scylla/procedures/config-change/migrate-vnodes-to-tablets.html

## TabletRuntimeCoordinator

The production-intent composition layer is:

    TabletRuntimeCoordinator
      ├─ TabletRangeLifecycle
      └─ CompactWindowScheduler

It owns the sequencing contract between RangeGeneration and ownership work.

The underlying components remain independently testable mechanism layers.

## Resize evaluation

Before evaluating split/merge:

1. requested TopologyEpoch must equal the runtime migration epoch;
2. ownership migration must already be converged;
3. committed compact actual replicas are synchronized back into RangeTabletMap;
4. the range lifecycle evaluates hysteresis/cooldown/metadata limits;
5. only then may a LifecycleResizePlan be returned.

If migration is not converged:

    CoordinatedResizeDecision::BlockedByMigration

No resize plan is produced from stale ownership.

## Replica synchronization before resize

Compact actual placement is the authoritative committed ownership while migration is operating.

RangeTabletMap may still contain ownership from the previous synchronization boundary.

Before resize, the coordinator copies each canonical actual replica set back into the matching range slot.

Therefore split children inherit:

    current committed actual replicas

not:

    old desired replicas
    stale pre-migration replicas
    in-flight uncommitted targets

This is machine-tested.

## Resize commit

A commit is accepted only if:

    plan TopologyEpoch == runtime TopologyEpoch

and, for a plan whose from_generation is still current:

    ownership migration is converged

The range lifecycle then applies its existing generation-fenced, replay-safe commit.

On Applied:

1. RangeGeneration increments;
2. a new CompactTabletCatalog is built from the committed RangeTabletMap;
3. new actual CompactPlacement is read from the inherited range replica sets;
4. desired placement is recomputed with WRH using the new stable TabletIds;
5. a fresh CompactWindowScheduler is created;
6. runtime NodeHealth for surviving nodes is restored;
7. ownership migration may begin for the new generation.

## Generation replay semantics

The migration barrier must not break control-plane idempotency.

Therefore generation mismatch is evaluated before the active-migration barrier.

If:

    current_generation == plan.from_generation + 1

and the underlying committed range map exactly matches the plan target:

    AlreadyApplied

is returned even if ownership migration for that new generation is currently active.

If the runtime has moved beyond that plan generation:

    StaleGeneration

is returned.

A retry is never incorrectly downgraded to BlockedByMigration.

## Topology fencing

TopologyEpoch remains the outer authority fence.

If topology advances after a resize plan is produced:

    old resize commit -> StaleTopology

No RangeGeneration mutation occurs.

The existing CompactWindowScheduler topology reconcile logic separately:

- preserves already committed actual ownership;
- cancels stale in-flight packets/tasks;
- installs the higher epoch desired map;
- replans from current actual.

Thus authority ordering is:

    TopologyEpoch
      > RangeGeneration
        > reconstructible migration task/window state

## Post-resize migration barrier

A resize can produce new TabletIds whose WRH placement differs from the inherited parent ownership.

That is expected.

If the newly rebuilt migration scheduler is not converged, another resize is blocked until it converges.

This avoids:

    resize generation N
      -> ownership transition
      -> resize generation N+1
         while generation N ownership is still ambiguous

## Runtime health

NodeHealth is transient runtime state, not durable topology.

When a successful resize rebuilds CompactWindowScheduler, health state for surviving node identities is copied into the new scheduler.

A test verifies that a Suspect node remains Suspect after the resize rebuild.

## Executable invariants

Tests currently verify:

- resize evaluation is blocked while initial ownership migration is incomplete;
- after migration converges, resize can be planned;
- current committed actual replicas are inherited into split children;
- RangeGeneration and CompactTabletCatalog generation advance together;
- topology epoch advancement fences an older resize plan;
- a new generation's ownership work blocks another resize;
- resize commit retry returns AlreadyApplied even if post-resize ownership work exists;
- a plan older than the current generation returns StaleGeneration;
- transient NodeHealth survives scheduler rebuild;
- resize evaluation under the wrong TopologyEpoch is rejected.

## What is intentionally not implemented yet

FeatherDB does not yet allow a tablet to split while that same logical ownership unit has an active transfer and then retarget/fork the transfer into child TabletIds.

Supporting that safely would require explicit lineage semantics such as:

    parent generation
    child identities
    transfer checkpoint lineage
    cutover dependency graph
    stale parent packet fencing

That is substantially more protocol surface.

The v0 barrier deletes this complexity until workload evidence shows concurrent split+migration is necessary.

## Future relaxation criteria

Only consider concurrent resize+migration after deterministic simulation can prove:

1. parent/child lineage survives crash/replay;
2. no stale parent transfer can commit after child generation publication;
3. repair priority remains globally correct;
4. split and migration cutovers have a deterministic dependency order;
5. queue/state memory remains bounded;
6. the throughput/latency benefit materially justifies the additional protocol state.

Until then:

    safe serialization > clever concurrency.
