# Million-Tablet Compact Placement Experiment — 2026-10-07

## Goal

Measure whether FeatherDB's placement and migration planning model can remain credible at one million tablets without allowing simulator/container overhead to dominate the result.

## Scenario

RF=2, strict distinct-zone policy.

Before:

    node 1 weight 1
    node 2 weight 2
    node 3 weight 4

After:

    add node 4 weight 8

All nodes are in distinct zones.

Both the original and compact paths use the same Weighted Rendezvous single-tablet selection function.

## Correctness equivalence

Tests verify:

- compact WRH == standard WRH tablet-by-tablet at 10K;
- replica counts are identical;
- movement is deterministic;
- no avoidable zone collision is introduced;
- CompactMigrationCursor emits exactly the same ordered pure-rebalance moves as eager MigrationScheduler at 1K.

## Scaling results

### Compact representation

| Tablets | Two flat replica maps | Placement before | Placement after | Movement scan | Peak RSS |
|---:|---:|---:|---:|---:|---:|
| 10K | 320,000 B | ~1 ms | ~1 ms | <1 ms | 3,164 KB |
| 100K | 3,200,000 B | ~12 ms | ~14 ms | ~1 ms | 6,004 KB |
| 1M | 32,000,000 B | ~107-111 ms | ~122 ms | ~9-10 ms | ~34,032 KB |

1M placement result:

    changed replicas = 847,638
    changed tablets = 847,638
    movement ratio = 0.423819
    before zone collisions = 0
    after zone collisions = 0

### Original object-rich representation

100K:

    before placement = 21 ms
    after placement = 20 ms
    movement = 7 ms
    peak RSS = 25,200 KB

1M:

    before placement = 172 ms
    after placement = 190 ms
    movement = 75 ms
    peak RSS = 226,016 KB
    changed tablets = 847,638

The process-level 1M RSS ratio is approximately:

    226,016 / 34,032 ~= 6.6x

The original path also carries explicit Tablet vectors; this ratio must not be attributed solely to BTreeMap.

## Migration queue finding

Current full MigrationTask:

    88 bytes

At 847,638 moves, eager task structs alone are approximately:

    74,592,144 bytes

before Vec/allocator overhead.

CompactMigrationCursor:

    CompactMigrationMove = 24 bytes
    peak buffered moves = 1
    buffer capacity = 48 bytes
    full 847,638-move scan = 18 ms

The lazy cursor produces exactly the same number of moves and, in a direct small-scenario comparison, the same move ordering as the eager pure-rebalance scheduler.

## Quality implication

A production-scale scheduler should not materialize one full state-machine object per future move.

The preferred direction is:

    durable actual map
    durable desired map
      -> reconstructible lazy diff
      -> bounded active task window

This also agrees with the already-established crash/restart principle that task queues are work, not independent durable truth.

## Limitations

- Compact tablet slots are contiguous simulator IDs; stable split/merge TabletId mapping still needs a compact catalog.
- The compact cursor does not yet implement Repair priority/source selection, epoch invalidation or transport state itself.
- No claim is made yet for a 1M-tablet full topology transaction or Repair campaign.
- RSS measurements are machine/build specific and are used for relative architecture evidence, not a universal product benchmark.

## Decision

Keep compact placement and lazy work generation as architecture candidates.

Do not promote them to the production metadata/scheduler format until:

1. stable TabletId/range catalog is compacted;
2. Repair and epoch semantics are preserved with a bounded active window;
3. deterministic network/fault tests run against that bounded scheduler;
4. memory remains bounded under churn, not just one static join.
