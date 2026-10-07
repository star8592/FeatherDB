# TabletId Reverse Index v0

Status: benchmark-backed simulator architecture candidate.

## Problem

CompactTabletCatalog stores tablet metadata in range order:

    slot -> TabletId
    slot -> range start
    slot -> bytes

Routing by token naturally returns a slot, so the data path does not require a permanent TabletId -> slot hash table.

Control-plane operations sometimes start with a stable TabletId and need to recover the compact slot.

At one million tablets, adding an object-heavy reverse map by default could erase a meaningful fraction of the compact-metadata savings.

## Candidates

The benchmark compares four representations.

### Contiguous arithmetic index

If current-generation IDs are exactly:

    first_id, first_id + 1, ... first_id + N - 1

then:

    slot = tablet_id - first_id

after bounds checking.

Extra heap allocation:

    0 bytes

This is the ideal path when lifecycle allocation happens to produce a contiguous generation.

### Sorted compact pairs

Store:

    Vec<(TabletId, u32 slot)>

sorted by TabletId and binary-search it.

Properties:

- simple;
- deterministic;
- compact;
- immutable per catalog generation;
- full rebuild is cheap;
- lookup is O(log N).

### std::collections::HashMap

Used as a familiar baseline.

Its exact backing allocation is intentionally not inferred from unstable internals; process RSS is the authoritative benchmark for memory comparison.

### Flat open addressing

Research implementation stores separate flat arrays:

    keys: Vec<TabletId>
    slots: Vec<u32>

with power-of-two capacity and linear probing.

This is significantly faster than the standard HashMap in the measured workload and has explicit memory accounting, but requires collision/load-factor/tombstone/update policy if promoted to a mutable production index.

## 1M fragmented-ID benchmark

Common retained TabletId catalog vector:

    1,000,000 * 8 bytes = 8 MB

Workload:

    5,000,000 lookups
    90% hits
    10% misses
    release build
    deterministic query stream

Results:

| Index | Build | Lookup | Reported index bytes | Peak RSS |
|---|---:|---:|---:|---:|
| baseline IDs only | n/a | n/a | 0 | 10,056 KB |
| sorted pairs | 22.7 ms | 182.5 ns/op | 16,000,000 B | 25,728 KB |
| std HashMap | 27.8 ms | 94.2 ns/op | payload/capacity estimate 29,360,128 B | 44,936 KB |
| flat open addressing | 20.3 ms | 35.5 ns/op | 25,165,824 B | 34,756 KB |
| contiguous arithmetic | 0.58 ms validation | 5.75 ns/op | 0 B | 10,112 KB |

The contiguous line uses a contiguous-ID workload; the other general indexes use a deliberately fragmented slot->ID ordering.

## 1% ID-churn rebuild

A second 1M workload replaces 1% of IDs with new high IDs to mimic a generation where selective lifecycle operations have broken contiguity.

Rebuild + lookup:

| Index | Rebuild | Lookup | Peak RSS |
|---|---:|---:|---:|
| sorted pairs | 22.1 ms | 147.3 ns/op | 25,608 KB |
| std HashMap | 25.1 ms | 85.9 ns/op | 44,972 KB |
| flat open addressing | 21.1 ms | 36.5 ns/op | 34,692 KB |

The key finding is not only lookup speed:

    full reverse-index rebuild at 1M is ~20-25 ms

in this synthetic release benchmark.

That makes generation-level rebuild attractive because it avoids incremental mutation/tombstone/rehash complexity.

## Current lifecycle fast path

The current RangeTabletMap whole-map split/merge implementation allocates a fresh contiguous TabletId interval for each new generation.

An executable test performs repeated split and merge generations, builds CompactTabletCatalog each time, and verifies:

    AdaptiveTabletIndexKind::Contiguous
    allocated_bytes == 0

for every observed generation.

This is an implementation property of the current lifecycle algorithm, not a permanent protocol requirement.

Future selective split can create range-order catalogs whose stable TabletIds are not contiguous.

## Adaptive default

The default research abstraction is:

    AdaptiveTabletIndex

Selection:

    if IDs form one contiguous interval:
        ContiguousTabletIndex
    else:
        SortedTabletIndex

The caller sees one TabletReverseIndex interface and does not depend on representation.

Why sorted rather than open-address fallback by default?

1. reverse lookup is control-plane support, not token-routing hot path;
2. the measured sorted index uses ~16 MB at 1M versus ~25 MB explicit open-address storage;
3. a complete 1M rebuild is only ~22 ms in the synthetic benchmark;
4. sorted immutable state is simpler to validate, serialize and replace atomically;
5. it has no load-factor, tombstone or incremental rehash state;
6. FeatherDB's current objective is low-resource correctness before speculative control-plane micro-optimization.

## Optional acceleration

OpenAddressTabletIndex remains in the simulator/benchmark code as an acceleration candidate.

It should be promoted only if production profiling proves TabletId reverse lookup is materially hot.

A future implementation could also build the open-address table ephemerally during topology work and release it afterward.

## Why not FST by default

The Rust fst crate is explicitly designed for memory-efficient very large sets/maps and can search compressed immutable maps, including mmap-backed data. However, its map keys are byte strings, values are u64, construction requires lexicographically sorted keys, and maps are immutable after construction.

Those properties make it interesting for cold/on-disk metadata indexing, but they add encoding/dependency/format complexity that the current in-memory u64->u32 control-plane index does not need.

Reference:

- https://docs.rs/fst/latest/fst/
- https://docs.rs/fst/latest/fst/map/struct.Map.html

Do not add fst until a storage/mmap use case justifies it.

## Decision

For v0 research:

    token -> slot:
        CompactTabletCatalog range starts

    TabletId -> slot:
        AdaptiveTabletIndex
            Contiguous arithmetic when possible
            Sorted compact pairs otherwise

    high-speed reverse lookup:
        OpenAddressTabletIndex remains optional / benchmark-only candidate

This keeps the permanent metadata structure simple and memory-biased while preserving a tested acceleration path.
