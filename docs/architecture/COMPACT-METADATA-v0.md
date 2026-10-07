# Compact Million-Tablet Metadata v0

Status: executable simulator optimization and production-format research input; not a frozen on-disk/control-plane format.

## Problem

The original research representation stores placement as:

    BTreeMap<TabletId, Vec<NodeId>>

This is convenient for correctness work but carries:

- one tree key/node structure per tablet;
- one independently allocated Vec per tablet;
- allocator and pointer overhead unrelated to the protocol semantics.

At one million tablets, those costs become large enough to distort resource experiments.

The migration scheduler also eagerly materializes every move as a MigrationTask, even though migration work is reconstructible from actual and desired maps.

## CompactPlacement

The new simulator representation stores:

    tablet_count
    replica_count
    flat Vec<NodeId>

For RF=2, placement metadata is:

    2 * 8 bytes = 16 bytes/tablet

for the replica array itself.

Tablet slot is implicit in the array index in this research representation.

The Weighted Rendezvous replica-selection function is shared by both the original and compact representations. Compact mode does not maintain a second placement algorithm.

## Equivalence

Executable tests verify:

- standard WRH and compact WRH are exactly equal tablet-by-tablet at 10K tablets;
- replica counts match exactly;
- hierarchical placement has the same zero-zone-collision property;
- checksums and movement are deterministic.

Therefore the compact experiment changes representation, not placement policy.

## Lazy migration work

A million-tablet join can produce hundreds of thousands of moves.

Current MigrationTask size on this build:

    88 bytes

For the 1M strong-node join experiment:

    moves = 847,638
    eager task struct bytes = 74,592,144

This excludes Vec capacity and allocator overhead.

Because unfinished migration work is reconstructible from:

    actual placement
    desired placement

the experiment introduces CompactMigrationCursor.

It scans actual/desired maps in tablet order and buffers only the moves for the current tablet.

For RF=2 in the 1M experiment:

    CompactMigrationMove size = 24 bytes
    peak buffered moves = 1
    cursor buffer capacity = 48 bytes
    full movement scan = 18 ms

The cursor emitted exactly 847,638 moves.

A separate test verifies that, for a 1K pure-rebalance scenario, the lazy cursor emits exactly the same:

    (tablet_id, owner_to_replace, target)

sequence as the existing eager MigrationScheduler task list.

## 1M release experiment

Scenario:

    tablets = 1,000,000
    RF = 2

Before:

    node 1 weight 1 / zone A
    node 2 weight 2 / zone B
    node 3 weight 4 / zone C

After:

    add node 4 weight 8 / zone D

Compact results:

    before flat replica bytes = 16,000,000
    after flat replica bytes = 16,000,000
    both maps = 32,000,000 bytes

    before placement = ~107-111 ms
    after placement = ~122 ms
    movement scan = ~9-10 ms
    lazy movement scan = 18 ms

    changed replicas = 847,638
    changed tablets = 847,638
    movement ratio = 0.423819

    before zone collisions = 0
    after zone collisions = 0

Observed release-process peak RSS:

    ~34,032 KB

A repeat run was ~34,152 KB, so the measurement is stable enough for this architecture experiment.

## Original representation comparison

The same logical WRH scenario using the original Placement plus explicit Tablet vectors measured:

100K:

    standard peak RSS = 25,200 KB
    compact peak RSS = 6,004 KB

1M:

    standard peak RSS = 226,016 KB
    compact peak RSS = ~34,032 KB

Observed peak-process ratio at 1M:

    ~6.6x

Timing in the same release build:

    standard 1M wall ~0.49 s
    compact 1M wall ~0.32 s

The movement result is identical:

    847,638 changed tablets

## Benchmark caveat

The ~6.6x RSS ratio is a process-level architecture comparison, not a claim that BTreeMap alone costs 6.6x.

The standard lab retains explicit Tablet vectors in both before/after Cluster values, while compact placement intentionally uses implicit contiguous tablet slots and does not allocate those per-tablet objects.

Therefore the experiment demonstrates that the original object-rich simulator shape is unsuitable for million-tablet resource tests. It does not isolate one container's exact overhead.

The protocol-relevant result is stronger and simpler:

    replica ownership itself only needs a compact flat representation,
    and eagerly materializing all migration work is unnecessary.

## Compact stable TabletId/range catalog

The previously identified stable-identity gap now has an executable research implementation: CompactTabletCatalog.

The catalog stores three flat arrays in range order:

    TabletId      8 bytes/tablet
    range_start   8 bytes/tablet
    bytes         8 bytes/tablet

The range end is implied by the next slot's start. The final slot ends at 2^64.

Therefore the core catalog payload is:

    24 bytes/tablet

For 1,000,000 tablets:

    ~24,000,000 bytes

Actual and desired placement share this catalog and retain only replica arrays.

For RF=2, the raw flat-array core becomes approximately:

    shared catalog        24 MB
    actual replicas       16 MB
    desired replicas      16 MB
    ---------------------------
    total                 56 MB

This excludes allocator slack, runtime indexes, telemetry, active tasks and other control-plane state, so it is not a full process-memory claim.

Executable tests verify that CompactTabletCatalog:

- preserves RangeTabletMap generation and next TabletId;
- preserves stable TabletId values after repeated split/merge;
- routes sampled tokens to exactly the same TabletId as RangeTabletMap;
- preserves total bytes;
- reconstructs implicit range ends exactly.

CompactPlacement now supports weighted_rendezvous_for_catalog(). WRH hashes the stable TabletId from the catalog, not the compact slot number.

CompactMigrationCursor also supports a catalog-backed mode. A direct test verifies that catalog-backed lazy moves preserve stable TabletId and exactly match the eager scheduler's pure-rebalance order.

### Reverse-index decision

The TabletId -> slot question has now been benchmarked and implemented as AdaptiveTabletIndex.

Default behavior:

    contiguous current-generation IDs
      -> arithmetic slot lookup, zero heap index

    non-contiguous IDs
      -> sorted compact (TabletId, u32 slot) pairs

A 1M fragmented-ID experiment measured sorted rebuild at about 22 ms and lookup around 150-180 ns/op, while using 16 MB explicit index storage.

A custom flat open-address implementation reached ~35-36 ns/op but used about 25 MB explicit storage, so it remains an optional acceleration candidate rather than the permanent default.

Current whole-map split/merge generations are executable-tested to retain the zero-allocation contiguous fast path.

See docs/architecture/REVERSE-INDEX-v0.md and docs/experiments/2026-10-07-reverse-index.md.

## Scheduler decision

Do not replace the current correctness-first MigrationScheduler with CompactMigrationCursor yet.

The eager scheduler already models:

- Repair priority;
- source failover;
- epoch fencing;
- in-flight transport;
- grouped cutover;
- runtime health;
- network faults.

The compact cursor currently proves pure-rebalance equivalence only.

Recommended evolution:

    compact actual/desired maps
      -> lazy move cursor
      -> bounded active-work window
      -> existing migration state machine semantics

Only the active/in-flight window should become full MigrationTask objects.

This preserves correctness behavior while making queued work proportional to configured concurrency rather than total cluster movement.

## Current conclusion

Million-tablet placement does not require million-tablet object graphs.

The current evidence supports:

1. compact flat ownership arrays;
2. a shared compact tablet catalog for stable physical identities;
3. reconstructible lazy migration work;
4. full task objects only for bounded active/in-flight operations.

This is now the preferred research direction for low-memory FeatherDB metadata.


## Bounded active execution follow-up

The lazy-move result has now been extended into CompactWindowScheduler. Future movement remains implicit in compact actual/desired maps, while only a bounded tablet window is converted into the existing full MigrationScheduler.

This preserves the correctness-first migration/repair state machine instead of introducing a second compact execution protocol.

In the 1M stable-ID RF2 experiment, a 1024-tablet materialization window limited full MigrationTask structs to ~90 KB while the complete 847,811-move transition converged.

See docs/architecture/BOUNDED-MIGRATION-WINDOW-v0.md.
