# ADR-0004: Fjall as the V0 Storage Substrate

Status: Accepted
Date: 2026-10-08

## Context

FeatherDB requires an embedded pure-Rust storage substrate with explicit fsync-class durability, range access, bounded operational complexity and acceptable sustained writes on heterogeneous hardware. Fjall 3.1.12 and redb 4.3.0 were compared under equal durability semantics on rotational storage and NVMe. A real Fjall DurableStore adapter then passed PREPARED/CURRENT crash recovery, process kill -9 durability checks and RuntimeCoordinator reconstruction.

## Decision

Use Fjall as the single V0 embedded storage substrate. Keep control and data state in separate keyspaces within the same engine. Control-plane durability remains serialized. Future data-plane durability must use bounded admission and group commit rather than one SyncAll per concurrent client request.

## Writer policy

Rotational/slow storage defaults to one durable commit in flight. Fast NVMe may use small bounded concurrency only when observed latency supports it. Writer concurrency is a device/workload policy, not a hard-coded engine property.

## Alternatives

redb remains a reference/fallback for small serialized metadata workloads, but a redb-control + Fjall-data split is rejected for V0 because it creates two recovery, backup, migration and corruption domains.

## Evidence

See STORAGE-SUBSTRATE-v0.md, 2026-10-07-storage-backend-bench.md, 2026-10-08-fjall-durable-adapter.md and 2026-10-08-fjall-concurrency-tail.md.
