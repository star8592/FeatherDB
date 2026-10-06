# Tablet Resize Controller Experiment — 2026-10-07

## Goal

Make tablet-count control executable before implementing physical key-range split/merge.

The controller must prevent resize oscillation, remain bounded by metadata budget, reject stale topology work, and be replay-safe across crashes.

## Research policy

The executable research baseline uses:

    merge threshold < target < split threshold

with a wide hysteresis baseline of:

    merge below 0.5x target average size
    split above 2.0x target average size

These ratios are research inputs inspired by current ScyllaDB tablet behavior. They are not frozen FeatherDB product defaults.

The caller must explicitly provide:

- target tablet bytes;
- cooldown ticks;
- estimated metadata bytes per tablet;
- metadata budget.

No production metadata-cost number or cooldown duration is hard-coded as a product claim.

## Executable invariants

The controller now verifies:

- split and merge use strict hysteresis boundaries;
- invalid hysteresis configuration is rejected;
- cooldown blocks immediate reverse resize;
- min/max tablet counts are hard bounds;
- metadata budget blocks split before allocation;
- topology epoch fences stale plans;
- resize generation fences stale commits;
- replay after lost acknowledgement is idempotent;
- 10,000 ticks of near-target noise produce no resize storm;
- metadata usage is explicit and bounded.

## Growth lab

Synthetic lab inputs:

    target_tablet_bytes = 100
    total_table_bytes = 10,000
    metadata_bytes_per_tablet = 16

These numbers are deliberately synthetic and are not production sizing recommendations.

### Budget for 64 tablets

Starting at one tablet:

    1 -> 2 -> 4 -> 8 -> 16 -> 32 -> 64

Result:

    stable
    tablet_count = 64
    resize_generation = 6
    metadata_bytes = 1,024
    commits = 6

At 64 tablets the average size is below the split threshold, so resizing stops.

### Budget for 32 tablets

Starting at one tablet:

    1 -> 2 -> 4 -> 8 -> 16 -> 32

The next split is rejected with:

    MetadataBudget

Result:

    tablet_count = 32
    resize_generation = 5
    metadata_bytes = 512

The controller does not allow growth pressure to bypass the metadata ceiling.

## Cooldown lab

After an 8 -> 16 split at tick 100, the synthetic table size is immediately reduced enough to request a merge.

At tick 101:

    Merge -> Blocked(Cooldown)
    remaining_ticks = 9

This demonstrates explicit anti-oscillation state instead of relying on noisy telemetry smoothing alone.

## Crash/replay model

A resize plan carries:

    topology_epoch
    from_generation
    from_count
    to_count
    kind

If the process crashes before commit, the same durable state can regenerate the same plan.

If commit succeeds but the acknowledgement is lost, replaying the same plan returns:

    AlreadyApplied

A topology-epoch change returns:

    StaleTopology

A newer resize generation returns:

    StaleGeneration

## Quality gate

- cargo test: **42 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**

## Official-system alignment

Current ScyllaDB documentation states that average tablet size above roughly twice the target causes split, while average size below roughly half the target causes merge. It also limits tablet pressure using tablets-per-shard goals because tablet replicas have fixed memory overhead.

Current TiKV/PD documentation exposes a split-merge interval specifically to keep newly split Regions from being immediately merged.

References:

- https://docs.scylladb.com/manual/stable/architecture/tablets.html
- https://docs.scylladb.com/manual/stable/cql/ddl.html
- https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html
- https://tikv.org/docs/7.1/deploy/configure/pd-configuration-file/

## What is not implemented yet

This controller changes logical tablet-count state only.

Still open:

- concrete hash/key-range child boundaries;
- physical data split execution;
- adjacent-range merge compatibility;
- interaction with actual replica placement during resize;
- crash injection during physical split/merge copying;
- hotness-driven split;
- production metadata-byte measurement.

The next implementation must not confuse "resize count committed" with "physical data transformation completed."
