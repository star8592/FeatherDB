# Tablet Lifecycle v0

Status: research hypothesis, not frozen architecture.

## Purpose

Tablet count cannot be a compile-time constant or an arbitrary huge number. It controls:
- migration granularity;
- hotspot isolation;
- repair granularity;
- metadata memory;
- scheduling overhead;
- storage-engine object/queue overhead.

The lifecycle therefore needs explicit split, merge, and anti-oscillation rules.

## Official-system lessons

### ScyllaDB

Current ScyllaDB documentation dynamically reevaluates tablet count. A table may split when average tablet size grows above roughly twice its target and merge when it falls below roughly half the target. It also considers tablet replicas per shard because each tablet replica has constant memory overhead, and excessive tablet counts can overload a shard.

References:
- https://docs.scylladb.com/manual/stable/architecture/tablets.html
- https://docs.scylladb.com/manual/stable/cql/ddl.html

### TiKV

TiKV Regions split as they grow and can merge as they shrink. PD has an explicit split-merge interval so a newly split Region is not immediately merged again. Merge concurrency is also rate-limited.

References:
- https://tikv.org/docs/7.1/deploy/configure/pd-configuration-file/
- https://tikv.org/docs/3.0/tasks/configure/region-merge/

## FeatherDB design constraints

### Hysteresis

Split and merge thresholds must not be the same threshold.

Conceptually:

    merge_threshold < target < split_threshold

The exact ratios are not frozen. Scylla's 0.5x / 2x policy is evidence for wide hysteresis, not a value to copy without experiments.

### Cooldown

After split or merge, the affected tablets/table must enter a resize cooldown. This prevents repeated resize cycles caused by noisy size estimates or short-lived traffic.

### Resource ceiling

Tablet count is bounded by a node/shard metadata budget.

A resize decision must consider:
- metadata bytes per replica;
- planner working-set cost;
- repair/migration queue cost;
- storage-engine per-range cost;
- target low-memory node class.

### Size is not the only signal

Later candidates:
- byte size;
- request heat;
- write amplification;
- repair debt;
- hotspot concentration.

A hot tablet may justify split before byte size alone would.

### Split and placement are separate

A split creates smaller logical ownership units. It does not automatically authorize unlimited physical migration.

    split/merge decision
        -> new logical tablet map
        -> placement planner
        -> migration scheduler

### Merge safety

Merge requires compatible adjacent logical ranges and a placement/replication state that can transition safely. A merge is not merely deleting one tablet ID.

## Initial simulator state machine

Tablet resize state:

    Stable
      -> SplitPlanned
      -> Splitting
      -> SplitCommitted
      -> Cooldown
      -> Stable

and symmetrically:

    Stable
      -> MergePlanned
      -> Merging
      -> MergeCommitted
      -> Cooldown
      -> Stable

Every transition must be idempotent and topology-epoch fenced.

## Acceptance experiments

1. Grow/shrink around the target threshold for 10,000 ticks: no oscillation storm.
2. Sudden 10x growth: split converges without exceeding metadata budget.
3. Large delete/shrink: merge reduces metadata without immediate re-split.
4. Crash at every split/merge transition boundary.
5. Simultaneous node rebalance and tablet resize.
6. Low-memory node joins during resize.
7. Hot tablet split with stable total byte size.
8. One million logical tablets: planner/simulator memory remains bounded or streams work.

## Open decisions

- target tablet byte size;
- split/merge ratios;
- cooldown duration;
- power-of-two count requirement or not;
- whether split boundaries are hash-space midpoints or data-aware;
- whether merges require identical replica sets before commit;
- how resize interacts with anti-entropy state.

These remain experimental until simulator evidence exists.


## Executable controller status

The first lifecycle controller is now implemented in `feather-sim`.

It models logical tablet-count decisions before physical key-range execution.

Implemented state:

    stable tablet-count generation
        -> evaluate resize trigger
        -> reconstructible ResizePlan
        -> generation/topology-fenced commit
        -> new tablet-count generation
        -> cooldown

The plan carries:

    kind
    topology_epoch
    from_generation
    from_count
    to_count
    planned_at_tick

Commit outcomes are explicit:

    Applied
    AlreadyApplied
    StaleTopology
    StaleGeneration

This deliberately keeps resize planning reconstructible and avoids creating a persistent resize-task journal before the physical executor exists.

### Research baseline vs product defaults

The simulator has a `research_hysteresis` constructor using the 2x split / 0.5x merge shape documented by ScyllaDB.

The caller must still explicitly supply:

- target tablet bytes;
- cooldown ticks;
- estimated metadata bytes per tablet;
- metadata budget.

The target size, cooldown, and metadata cost are therefore not hard-coded product claims.

### Metadata ceiling

A split is rejected before allocation if:

    next_tablet_count * metadata_bytes_per_tablet
        > metadata_budget_bytes

The budget is protocol-visible and testable.

This is important for FeatherDB's low-memory-node target: tablet cardinality cannot expand without a bounded metadata model.

### Current crash semantics

At the logical controller layer:

- crash before commit: plan can be regenerated;
- crash after commit but before acknowledgement: replay returns AlreadyApplied;
- topology changes fence old plans;
- later resize generations fence stale commits.

Physical split/merge execution still needs its own transition states and crash-injection campaign.

See:

    docs/experiments/2026-10-07-tablet-resize-controller.md


## Physical range-map execution

The simulator now has an executable RangeTabletMap and TabletRangeLifecycle.

A range tablet contains:

    tablet_id
    [start, end) over the 64-bit hash space
    bytes
    canonical replica set

The full hash space is represented as:

    [0, 2^64)

and every committed map must cover it exactly once with no gaps or overlaps.

### Split

The current research split operation divides every logical range at its midpoint.

For a parent:

    [start, end)

children become:

    [start, midpoint)
    [midpoint, end)

Replica ownership is inherited unchanged at split time.

Bytes are divided conservatively:

    left = floor(parent_bytes / 2)
    right = parent_bytes - left

so total bytes are preserved exactly.

### Merge

Merge currently operates only on adjacent pairs.

A pair can merge only when:

- left.end == right.start;
- both ranges have exactly the same canonical replica set;
- byte addition does not overflow.

If replica sets differ, merge is rejected. The placement/migration layer must first reconcile ownership.

### Single public lifecycle entrypoint

External callers use TabletRangeLifecycle rather than directly committing RangeTabletMap mutations.

TabletRangeLifecycle derives the resize controller state from the physical map:

    generation
    tablet_count
    last_resize_tick

and produces one LifecycleResizePlan containing both:

    ResizePlan
    RangeResizePlan

The commit path clones lifecycle state, validates the physical transform, and only swaps the candidate state into place after the full transform succeeds.

This prevents a split-brain inside metadata such as:

    logical tablet_count = 16
    physical range count = 8

### Fencing and replay

Every range resize plan carries:

    topology_epoch
    from_generation
    from_count
    from_next_tablet_id

Commit checks all of them.

Current executable behavior:

- stale topology epoch -> no change;
- stale generation -> no change;
- identical replay after successful commit -> AlreadyApplied;
- tampered boundaries -> InvalidPlan;
- replica mismatch on merge -> rejected without partial commit.

### Current lab

The range-resize lab starts with one range holding 10,000 bytes and replicas [1,2,3].

It repeatedly grows to 64 tablets and then shrinks back to one.

Observed:

    start:
      count=1
      generation=0
      bytes=10000

    after growth:
      count=64
      generation=6
      bytes=10000

    after shrink:
      count=1
      generation=12
      bytes=10000
      range=[0, 2^64)
      replicas=[1,2,3]

All sampled tokens remained routable throughout the sequence.

### Important limitation

Midpoint split is currently a control-plane correctness baseline, not the final production split-boundary policy.

Future work must compare:

- midpoint hash-space split;
- data-size-aware split;
- hot-key / traffic-aware split.

The invariant to preserve is stronger than the exact boundary algorithm:

    every committed generation has total, non-overlapping hash-space coverage
    and deterministic routing.


## Split-boundary policy

The physical range transform currently uses hash midpoint as the correctness baseline.

A separate executable experiment now compares HashMidpoint, ByteMedian, and HeatMedian using sampled per-token bytes and request heat.

The first experiment demonstrates that the objectives conflict:

- byte-balanced boundaries can worsen heat balance;
- heat-balanced boundaries can worsen byte balance;
- a same-token hotspot is fundamentally unsplittable by range boundary.

Therefore boundary selection is now modeled as a policy decision.

Proposed control flow:

    resize trigger
      -> choose objective
      -> validate telemetry confidence
      -> choose boundary strategy
      -> validate interior boundary
      -> execute fenced range transform

HashMidpoint remains the fallback when data/heat telemetry is missing, stale, noisy, or not worth the added control-plane complexity.

See docs/experiments/2026-10-07-split-boundary.md.
