use std::hint::black_box;
use std::time::Instant;

use feather_sim::{
    ContiguousTabletIndex, OpenAddressTabletIndex, SortedTabletIndex, StdHashTabletIndex,
    TabletReverseIndex,
};

fn fragmented_ids(count: usize) -> Vec<u64> {
    let low = 10_000_000_u64;
    let high = 10_000_000_000_u64;
    (0..count)
        .map(|slot| {
            if slot % 2 == 0 {
                low + (slot / 2) as u64
            } else {
                high + (slot / 2) as u64
            }
        })
        .collect()
}

fn contiguous_ids(count: usize) -> Vec<u64> {
    (10_000_000..10_000_000 + count as u64).collect()
}

fn churned_ids(count: usize, churn_percent: usize) -> Vec<u64> {
    let mut ids = fragmented_ids(count);
    let churn_count = count.saturating_mul(churn_percent) / 100;
    if churn_count == 0 {
        return ids;
    }
    let stride = (count / churn_count).max(1);
    for (next, slot) in (20_000_000_000_u64..).zip((0..count).step_by(stride).take(churn_count)) {
        ids[slot] = next;
    }
    ids
}

fn query<I: TabletReverseIndex>(index: &I, ids: &[u64], query_count: usize) -> (u64, u128) {
    let started = Instant::now();
    let mut state = 0x8592_1234_5678_9abc_u64;
    let mut checksum = 0_u64;
    for i in 0..query_count {
        state = splitmix64(state.wrapping_add(i as u64));
        let hit = i % 10 != 0;
        let tablet_id = if hit {
            ids[(state as usize) % ids.len()]
        } else {
            50_000_000_000_u64 + state % 10_000_000
        };
        checksum =
            checksum.wrapping_add(index.get(black_box(tablet_id)).unwrap_or(u32::MAX) as u64);
    }
    (black_box(checksum), started.elapsed().as_nanos())
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

struct BenchResult<'a> {
    mode: &'a str,
    pattern: &'a str,
    count: usize,
    query_count: usize,
    build_ns: u128,
    query_ns: u128,
    allocated_bytes: usize,
    checksum: u64,
}

impl BenchResult<'_> {
    fn print(&self) {
        println!(
            "reverse-index mode={} pattern={} tablets={} queries={} build_ms={:.3} query_ms={:.3} ns_per_lookup={:.2} index_reported_bytes={} checksum={}",
            self.mode,
            self.pattern,
            self.count,
            self.query_count,
            self.build_ns as f64 / 1_000_000.0,
            self.query_ns as f64 / 1_000_000.0,
            self.query_ns as f64 / self.query_count as f64,
            self.allocated_bytes,
            self.checksum
        );
    }
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "sorted".into());
    let pattern = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "fragmented".into());
    let count = std::env::args()
        .nth(3)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    let query_count = std::env::args()
        .nth(4)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(5_000_000);

    let ids = match pattern.as_str() {
        "contiguous" => contiguous_ids(count),
        "fragmented" => fragmented_ids(count),
        "churn1" => churned_ids(count, 1),
        other => panic!("unknown pattern: {other}"),
    };

    match mode.as_str() {
        "baseline" => {
            println!(
                "reverse-index mode=baseline pattern={} tablets={} ids_bytes={} checksum={}",
                pattern,
                count,
                ids.capacity() * std::mem::size_of::<u64>(),
                ids.iter()
                    .fold(0_u64, |sum, value| sum.wrapping_add(*value))
            );
        }
        "contiguous" => {
            let started = Instant::now();
            let index = ContiguousTabletIndex::try_from_ids(&ids)
                .expect("contiguous index build")
                .expect("input must be contiguous");
            let build_ns = started.elapsed().as_nanos();
            let (checksum, query_ns) = query(&index, &ids, query_count);
            BenchResult {
                mode: "contiguous",
                pattern: &pattern,
                count,
                query_count,
                build_ns,
                query_ns,
                allocated_bytes: index.allocated_bytes(),
                checksum,
            }
            .print();
        }
        "sorted" => {
            let started = Instant::now();
            let index = SortedTabletIndex::build(&ids).expect("sorted index build");
            let build_ns = started.elapsed().as_nanos();
            let (checksum, query_ns) = query(&index, &ids, query_count);
            BenchResult {
                mode: "sorted",
                pattern: &pattern,
                count,
                query_count,
                build_ns,
                query_ns,
                allocated_bytes: index.allocated_bytes(),
                checksum,
            }
            .print();
        }
        "hash" => {
            let started = Instant::now();
            let index = StdHashTabletIndex::build(&ids).expect("hash index build");
            let build_ns = started.elapsed().as_nanos();
            let (checksum, query_ns) = query(&index, &ids, query_count);
            BenchResult {
                mode: "hash",
                pattern: &pattern,
                count,
                query_count,
                build_ns,
                query_ns,
                allocated_bytes: index.allocated_bytes(),
                checksum,
            }
            .print();
        }
        "open" => {
            let started = Instant::now();
            let index = OpenAddressTabletIndex::build(&ids).expect("open-address index build");
            let build_ns = started.elapsed().as_nanos();
            let (checksum, query_ns) = query(&index, &ids, query_count);
            BenchResult {
                mode: "open",
                pattern: &pattern,
                count,
                query_count,
                build_ns,
                query_ns,
                allocated_bytes: index.allocated_bytes(),
                checksum,
            }
            .print();
        }
        other => panic!("unknown mode: {other}"),
    }
}
