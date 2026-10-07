# Resize × Migration Coordination Experiment — 2026-10-07

## Goal

Verify that RangeGeneration changes and ownership migration compose safely without creating two competing ownership truths.

## Coordinator

The experiment uses TabletRuntimeCoordinator over:

    TabletRangeLifecycle
    CompactWindowScheduler

The coordinator blocks resize while ownership migration is incomplete.

## Scenario 1 — initial ownership movement blocks resize

Initial RangeTabletMap contains a replica on a Removed node.

The desired compact placement requires Repair/Rebalance.

Before migration convergence:

    evaluate_resize -> BlockedByMigration

After convergence:

    evaluate_resize -> Planned

## Scenario 2 — committed actual ownership is inherited

After initial ownership migration converges, the coordinator synchronizes compact actual replicas into RangeTabletMap and commits a split.

Result:

    generation 0 -> 1
    tablet count 1 -> 2

Both child ranges inherit the parent's current committed actual replicas.

The new compact catalog reports generation 1.

## Scenario 3 — topology epoch fences resize

A resize plan is produced under TopologyEpoch 7.

Before commit, topology reconciles to epoch 8.

Retrying the old resize plan returns:

    StaleTopology

RangeGeneration remains unchanged.

## Scenario 4 — post-resize ownership work creates a barrier

After resize, new stable TabletIds can produce a new WRH desired placement.

If ownership work is outstanding:

    next evaluate_resize -> BlockedByMigration

Only after convergence can another resize generation be planned.

## Scenario 5 — commit retry remains idempotent

A resize commit succeeds:

    Applied { generation 0 -> 1 }

The same plan is immediately retried.

Even if generation-1 ownership work is active, the result is:

    AlreadyApplied

not BlockedByMigration.

This is required for control-plane retry safety.

## Scenario 6 — older plan is stale

After a second resize advances generation 1 -> 2, the generation-0 plan is retried.

Result:

    StaleGeneration

Generation remains 2.

## Scenario 7 — transient health survives rebuild

A node is marked:

    NodeHealth::Suspect

A resize commit rebuilds the compact migration scheduler.

The node remains Suspect in the rebuilt scheduler.

## Result

All coordinator scenarios pass deterministically.

The experiment supports a conservative initial rule:

    do not commit RangeGeneration while ownership migration is unresolved.

This deliberately trades some theoretical concurrency for a significantly smaller correctness state space.
