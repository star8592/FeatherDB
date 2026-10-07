# Fjall Concurrent Writer + Compaction Tail Experiment — 2026-10-08

## Goal

Measure durable-write tail latency under real Fjall compaction pressure before choosing a default writer concurrency policy.

This experiment uses the current Fjall 3.1.12 engine directly, not the simulator.

## Workload

Each run writes exactly:

    320 durable batches
    x 1,000 records
    x 256-byte values
    = 320,000 records

Every batch commits with PersistMode::SyncAll.

The workload records:

- foreground wall time;
- p50/p95/p99/max per durable batch;
- completed compactions;
- journal count;
- disk bytes;
- process peak RSS.

The same total logical work is used for one-writer and four-writer cases.

## Rotational ext4 results

Storage:

    /dev/sdb1
    Seagate ST16000NM000J-2TW103
    ext4, noatime

| Memtable | Writers | Foreground | p50 | p95 | p99 | max | Compactions | Peak RSS | Disk bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 MiB | 1 | 35.269 s | 91.7 ms | 208 ms | 432 ms | 840 ms | 4 | 120,824 KiB | 146,240,165 |
| 64 MiB | 4 | 42.328 s | 359 ms | 1.797 s | 2.374 s | 2.846 s | 4 | 121,360 KiB | 147,047,320 |
| 16 MiB | 1 | 39.331 s | 91.9 ms | 203 ms | 765 ms | 2.078 s | 24 | 44,760 KiB | 150,934,364 |
| 16 MiB | 4 | 44.852 s | 359 ms | 1.571 s | 2.356 s | 2.568 s | 20 | 51,360 KiB | 218,150,592 |

## NVMe directional check

Storage:

    CT1000T705SSD3
    ext4, noatime

Default 64 MiB memtable:

| Writers | Foreground | p50 | p95 | p99 | max | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 882 ms | 2.687 ms | 3.035 ms | 3.285 ms | 14.758 ms | 121,424 KiB |
| 4 | 718 ms | 8.709 ms | 9.760 ms | 12.267 ms | 23.369 ms | 121,100 KiB |

## Findings

### 1. Rotational storage must not run unconstrained durable writers

On the HDD, four writers are worse in both total throughput and tail latency.

At the default 64 MiB memtable:

    1 writer p99 = 432 ms
    4 writer p99 = 2.374 s

and the total foreground time increases from 35.3 s to 42.3 s.

Therefore FeatherDB must not let concurrent requests independently call SyncAll on slow storage.

### 2. NVMe can benefit from bounded concurrency

On NVMe, four writers improve total foreground wall time:

    882 ms -> 718 ms

but per-batch p99 still increases:

    3.285 ms -> 12.267 ms

This means concurrency can improve throughput on fast devices while still increasing latency.

The correct control is therefore bounded, device-aware admission rather than a global single-writer rule.

### 3. Smaller memtables exchange memory for compaction tail

On the HDD, 16 MiB reduces peak RSS substantially in the one-writer case:

    120,824 KiB -> 44,760 KiB

but compactions rise from 4 to 24 and p99 rises from 432 ms to 765 ms.

Four writers + 16 MiB also inflate disk usage to about 208 MiB in this bounded run.

There is no universal “low-memory memtable” setting.

### 4. Keyspace layout matters

This experiment uses one data keyspace. Earlier synthetic low-memory tests with two keyspaces and an 8 MiB cache did not reduce process RSS.

Memory behavior therefore depends on:

    keyspace count
    memtable cap
    block cache
    compaction concurrency
    workload shape
    live Slice lifetimes

A node memory profile must be benchmarked against the actual production keyspace layout.

## V0 policy

### Control plane

Keep a single serialized DurableStore writer.

PREPARED/CURRENT topology changes are sparse and correctness-sensitive; they do not need multi-writer fsync concurrency.

### Data plane

Do not map every client write directly to a separate SyncAll.

Use:

    bounded admission queue
        -> batch/group commit
        -> explicit durable boundary

The group-commit scheduler should expose:

- max queued bytes;
- max queued operations;
- max group-commit delay;
- max concurrent durable commits;
- device profile / measured latency feedback.

### Device adaptation

Initial policy direction:

    rotational / slow storage:
        max concurrent durable commits = 1

    fast NVMe:
        permit small bounded concurrency if measured p99 remains inside budget

Do not identify storage class only by device name. Runtime calibration and observed durable latency should eventually feed the policy.

## Decision impact

Fjall remains the V0 storage leader.

The experiment does not expose a correctness problem. It exposes an admission-control requirement around fsync-class durability.

The production Fjall control adapter already naturally serializes through &mut DurableStore. The future data-plane write path must add a bounded group-commit layer before being considered production-ready.
