# Storage Substrate v0

Status: benchmarked candidates; Fjall is the leading single-engine V0 hypothesis, not yet frozen.

## Required semantics

The production substrate must support the same intent already exercised by deterministic simulation:

    get
    put
    delete
    atomic batch
    explicit durable sync boundary
    reopen/recovery
    ordered/range access for tablet-local data

Control-plane code must never rely on an engine's implicit/default durability. The PREPARED/CURRENT protocol requires an explicit durability boundary corresponding to fsync-class persistence.

## Candidate A: Fjall

Strengths relevant to FeatherDB:

- pure safe Rust;
- LSM data model suited to sustained writes;
- thread-safe database/keyspace API;
- range and prefix iteration;
- atomic cross-keyspace write batches;
- explicit Buffer / SyncData / SyncAll persistence;
- optimistic multi-writer serializable transactions available;
- stable disk format policy and migration path by major version.

Risks:

- memory depends materially on cache, memtables, keyspace count and compaction behavior;
- default 64 MiB per-keyspace memtable is not automatically compatible with FeatherDB's smallest-node goals;
- naive 8 MiB memtable tuning performed worse in the first experiment;
- current 3.1.12 release requires Rust 1.90 while FeatherDB core currently declares 1.85.

## Candidate B: redb

Strengths:

- pure Rust COW B+tree;
- strong ACID transaction semantics;
- Immediate durability maps cleanly to control-plane commits;
- lower RSS and much faster reopen in the first benchmark;
- excellent fit for small serialized metadata workloads.

Risks for the main data plane:

- only one write transaction may be in progress at a time;
- rotational-disk durable streaming batches were substantially slower in the first benchmark;
- larger file footprint in that workload;
- current 4.3.0 release also requires Rust 1.90.

## One engine vs two engines

A split architecture such as redb-control + Fjall-data looks locally attractive, but it creates another durability boundary and another operational object. It would require coordinated backup/restore, version migration, health reporting, corruption handling and file lifecycle across two engines.

FeatherDB's complexity budget therefore favors one engine unless a second engine eliminates a measured bottleneck that cannot be solved reasonably inside the first.

The current control-plane commit rate is intentionally low, so redb's control-commit advantage does not yet justify a second engine.

## Current V0 hypothesis

Use Fjall as the first production adapter candidate, with explicit SyncAll durability for PREPARED/CURRENT and carefully separated keyspaces.

Do not freeze this until:

1. the real Fjall adapter passes the topology crash/recovery state machine;
2. forced-process crash/reopen verifies unsynced vs synced behavior;
3. concurrent writer and compaction-tail tests are run;
4. memory is measured under a production-like keyspace layout rather than synthetic two-keyspace defaults;
5. the Rust 1.90 MSRV decision is explicit.

redb stays in the benchmark matrix and remains a plausible fallback if the data-plane concurrency assumption changes.
