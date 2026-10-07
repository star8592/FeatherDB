# Million-Tablet Reverse-Index Experiment — 2026-10-07

## Goal

Choose a TabletId -> compact slot lookup structure using measured memory and lookup cost rather than assuming HashMap is the correct default.

## Workload

    tablets = 1,000,000
    queries = 5,000,000
    hit rate = 90%
    miss rate = 10%
    release build

The fragmented workload deliberately breaks correspondence between range-order slot and TabletId order.

The process retains the common 8 MB TabletId vector so RSS reflects the incremental index cost on top of the catalog identity column.

## Results

### Fragmented IDs

    baseline peak RSS = 10,056 KB

Sorted pairs:

    build = 22.670 ms
    lookup = 182.48 ns/op
    index bytes = 16,000,000
    peak RSS = 25,728 KB

std HashMap:

    build = 27.824 ms
    lookup = 94.16 ns/op
    peak RSS = 44,936 KB

Flat open addressing:

    build = 20.313 ms
    lookup = 35.48 ns/op
    index bytes = 25,165,824
    peak RSS = 34,756 KB

### Contiguous IDs

Arithmetic fast path:

    validation/build = 0.582 ms
    lookup = 5.75 ns/op
    index heap bytes = 0
    peak RSS = 10,112 KB

### 1% churn / non-contiguous generation

Sorted:

    rebuild = 22.134 ms
    lookup = 147.28 ns/op
    peak RSS = 25,608 KB

std HashMap:

    rebuild = 25.089 ms
    lookup = 85.93 ns/op
    peak RSS = 44,972 KB

Open addressing:

    rebuild = 21.092 ms
    lookup = 36.49 ns/op
    peak RSS = 34,692 KB

## Correctness tests

Executable tests verify:

- every general index returns the original slot for fragmented IDs;
- missing IDs return None;
- duplicate IDs are rejected;
- contiguous fast path detects exact intervals and rejects gaps;
- open-address storage has explicit bounded allocation;
- AdaptiveTabletIndex chooses Contiguous for contiguous catalogs;
- AdaptiveTabletIndex chooses Sorted for sparse/non-contiguous IDs;
- repeated current RangeTabletMap full split/merge generations all retain the zero-allocation contiguous fast path.

## Interpretation

Open addressing wins general-case lookup latency, but it is not the best default for FeatherDB's current control-plane workload.

Sorted pairs save roughly 9 MB of explicit index payload versus the current open-address capacity at 1M and can be fully rebuilt in about 22 ms.

Because token routing does not need TabletId reverse lookup, spending extra permanent memory to optimize ~150 ns control-plane lookups is not yet justified.

## Decision

Default:

    Contiguous arithmetic if possible
    Sorted compact pairs otherwise

Optional future acceleration:

    ephemeral or profile-gated flat open addressing

Do not make std HashMap the default million-tablet reverse index.

These values are machine/build-specific architecture measurements, not product performance claims.
