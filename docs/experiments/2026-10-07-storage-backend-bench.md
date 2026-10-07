# Storage Backend Benchmark — 2026-10-07

## Goal

Compare current pure-Rust embedded storage candidates under FeatherDB-relevant durability semantics before wiring a production backend into the database.

Candidates:

- Fjall 3.1.12, Database + SyncAll durability
- redb 4.3.0, write transactions + Immediate durability

At the time this benchmark was first created, both candidate releases required Rust 1.90 while the FeatherDB core still declared Rust 1.85, so the benchmark used an isolated Rust 1.90 manifest. On 2026-10-08 the repository baseline was intentionally upgraded to Rust 1.99.0 / Edition 2024.

## Fairness rules

The benchmark does not compare Fjall's default buffered write against redb's durable commit. Durable boundaries are explicit:

- Fjall: insert/batch then Database::persist(PersistMode::SyncAll), or WriteBatch with SyncAll durability
- redb: WriteTransaction with Durability::Immediate then commit

Workloads:

1. control sync: overwrite one 2 KiB control record 200 times, every operation durable;
2. giant bulk: 100,000 unique records x 256 bytes in one durable atomic batch;
3. streaming durable batches: 100 batches x 1,000 records, every batch durable;
4. reopen database;
5. 100,000 deterministic point reads;
6. record process peak RSS and final on-disk bytes.

The checksum is identical across engines, preventing read-path dead-code elimination.

## Hardware

Host: Intel Core Ultra 7 265K, 91 GiB RAM.

HDD experiment path:

    /mnt/disk1
    /dev/sdb1
    ext4, rw,noatime
    Seagate ST16000NM000J-2TW103, 14.6 TiB, rotational

NVMe direction check:

    /
    /dev/nvme0n1p2
    ext4, rw,noatime
    CT1000T705SSD3

Release build. HDD results are three-run medians. NVMe results are one directional run per backend and are not treated as statistically equivalent to the HDD medians.

## HDD three-run medians

| Metric | Fjall 3.1.12 default | redb 4.3.0 | redb / Fjall |
|---|---:|---:|---:|
| 200 durable control overwrites | 17.592 s | 15.615 s | 0.89x |
| 100k one-shot durable bulk | 0.550 s | 0.959 s | 1.74x |
| 100 x 1k durable streaming batches | 9.868 s | 17.168 s | 1.74x |
| reopen | 278 ms | 36 ms | 0.13x |
| 100k point reads | 49 ms | 51 ms | ~1.04x |
| peak RSS | 76,796 KiB | 58,792 KiB | 0.77x |
| final DB bytes | 57,428,692 | 77,598,720 | 1.35x |
| whole run wall time | 31.50 s | 34.34 s | 1.09x |

Interpretation:

- redb is better in this test for sparse durable control commits, restart latency and peak process memory;
- Fjall is materially better on the rotational disk for sustained durable batch writes and space consumption;
- point reads are effectively the same at this small data size.

## NVMe directional run

| Metric | Fjall 3.1.12 | redb 4.3.0 |
|---|---:|---:|
| 200 durable control overwrites | 280 ms | 153 ms |
| 100k one-shot durable bulk | 103 ms | 116 ms |
| 100 x 1k durable streaming batches | 279 ms | 282 ms |
| reopen | 178 ms | <1 ms reported |
| 100k point reads | 49 ms | 37 ms |
| peak RSS | 77,020 KiB | 59,468 KiB |
| DB bytes | 57,428,692 | 77,598,720 |

On NVMe, Fjall's streaming-write advantage largely disappears in this workload. The storage medium materially changes the performance trade-off.

## Naive low-memory Fjall experiment

Configuration:

- database cache = 8 MiB;
- worker threads = 1;
- each keyspace max memtable = 8 MiB.

Three-run median:

| Metric | Fjall default | Fjall low-memory attempt |
|---|---:|---:|
| durable control | 17.592 s | 16.889 s |
| giant bulk | 550 ms | 475 ms |
| streaming durable batches | 9.868 s | 12.456 s |
| reopen | 278 ms | 304 ms |
| point reads | 49 ms | 50 ms |
| peak RSS | 76,796 KiB | 78,736 KiB |
| DB bytes | 57,428,692 | 105,989,616 |

The attempted low-memory profile does not reduce peak RSS for this workload, slows sustained durable batching by ~26%, and increases file footprint by ~1.85x. It is rejected as a default configuration.

This is evidence against assuming that smaller memtables automatically lower process peak memory: flush/compaction behavior and multiple keyspaces matter.

## API/concurrency considerations

redb provides ACID COW B+tree transactions and concurrent readers, but only one write transaction may be in progress at a time.

Fjall is an LSM engine with thread-safe Database/Keyspace access, range/prefix iteration, atomic cross-keyspace batches, and optional multi-writer optimistic serializable transactions.

These semantics matter to FeatherDB's data plane beyond the microbenchmark numbers.

## Current decision

Do not introduce two storage engines into FeatherDB V0 merely because redb wins control-plane microbenchmarks. That would duplicate dependency, backup, recovery, tuning, file-layout and operational concerns for a control-plane workload that is intentionally sparse.

Current leading hypothesis:

    one Fjall engine for V0
      control metadata in dedicated keyspace(s)
      tablet/data state in dedicated keyspace(s)
      explicit SyncAll at durability boundaries

redb remains a strong control-plane reference and fallback candidate.

This is not frozen yet. Before selection, Fjall must run the real PREPARED/CURRENT durable topology state machine through an actual adapter, and concurrency/compaction behavior must be tested.

## MSRV decision

Fjall 3.1.12 and redb 4.3.0 both require at least Rust 1.90. The repository baseline has now been explicitly raised to Rust 1.99.0, so current engine versions no longer require a separate compiler exception. Older engine releases are not selected merely for compiler compatibility.

## Scope caveats

This is not a universal database benchmark. It does not yet measure:

- multi-threaded writer contention;
- long-duration compaction tails/p99 latency;
- multi-gigabyte datasets;
- recovery after forced process kill;
- write amplification;
- range-scan throughput;
- power-loss testing;
- ARM/low-memory hardware.

The results are architecture evidence for FeatherDB, not marketing numbers.
