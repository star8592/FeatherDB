# Bounded Million-Tablet Migration Experiment — 2026-10-07

## Goal

Execute a complete million-tablet placement transition while bounding the number of full MigrationTask objects in memory.

This goes beyond the earlier lazy-diff scan: the experiment repeatedly materializes bounded windows into the real MigrationScheduler and performs actual ownership cutovers.

## Scenario

    tablets = 1,000,000
    RF = 2
    stable TabletId range starts at 10,000,000

Before:

    node 1 weight 1
    node 2 weight 2
    node 3 weight 4

After:

    add node 4 weight 8

All nodes occupy distinct zones.

Catalog bytes are one byte/tablet in this control-plane execution benchmark so transfer throughput does not dominate scheduler CPU.

## Placement

    changed moves = 847,811

The stable-ID result differs slightly from the older slot-ID lab, as expected, because Weighted Rendezvous hashes stable TabletId.

## Window sweep

| Window tablets | Windows | Peak full tasks | Task-struct bytes | Execute time | Peak RSS |
|---:|---:|---:|---:|---:|---:|
| 64 | 13,248 | 64 | 5,632 B | ~5.57 s | 57,696 KB |
| 256 | 3,312 | 256 | 22,528 B | ~1.84 s | 57,740 KB |
| 1,024 | 828 | 1,024 | 90,112 B | ~0.94 s | 58,064 KB |

Every run converged.

The compact core arrays were:

    catalog = 24,000,000 bytes
    actual replicas = 16,000,000 bytes
    desired replicas = 16,000,000 bytes
    total raw core = 56,000,000 bytes

## Comparison with eager future work

Current MigrationTask size:

    88 bytes

If all 847,811 moves were materialized at once:

    74,607,368 bytes

of task structs would be required before allocator overhead.

At window=1024:

    90,112 bytes

of full task structs were materialized at peak.

## Correctness coverage

Tests additionally verify:

- bounded pure Rebalance reaches desired placement;
- peak window/tablet/task counts respect configured bounds;
- Forced Repair is globally materialized before ordinary Rebalance;
- mixed Repair+Rebalance tablets defer their ordinary move until repair phase is complete;
- Removed-node replica count reaches zero before global Rebalance phase;
- network Partition blocks active repair and Heal resumes convergence;
- process/scheduler restart reconstructs work from compact actual/desired maps.

## Interpretation

The dominant memory at one million tablets is now the durable compact metadata itself, not queued future work.

Window size is primarily a CPU/amortization knob, while MigrationBudget remains the I/O/concurrency knob.

The synthetic 1024 window is not a production default. It is evidence that hundreds-to-thousands of materialized task descriptors can be cheap while actual copy concurrency remains independently small.

## Quality target

The branch must pass the full repository quality gate after these tests are added.

## Next experiments

- epoch replacement while a window is active;
- million-tablet forced-loss Repair campaign;
- reverse TabletId-index alternatives;
- general deterministic message bus;
- production storage/transport substrate benchmarks.


## Epoch replacement follow-up

A focused test starts a forced Repair under TopologyEpoch 2 with a delayed source->target packet still in SimNetwork.

Before that packet is delivered, TopologyEpoch 3 is committed with a different desired placement that also removes the old target node.

Observed behavior:

    old in-flight packet count before reconcile = 1
    proposed epoch = 3
    old epoch = 2
    cancelled in-flight transfers = 1
    old in-flight packet count after reconcile = 0

The compact scheduler retains its current committed actual map, discards the old active window, resets to Repair phase, and replans against epoch-3 desired placement.

Final result:

    converged = true
    actual == epoch-3 desired
    replicas on node removed in epoch 3 = 0

Equal-epoch replacement is independently rejected as stale and leaves state unchanged.

This closes the main late-packet/old-window fencing risk identified after the million-tablet bounded-window experiment.
