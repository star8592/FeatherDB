use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use fjall::{Database, KeyspaceCreateOptions, PersistMode};

#[derive(Clone, Copy, Debug)]
struct Config {
    writers: usize,
    total_batches: usize,
    records_per_batch: usize,
    value_bytes: usize,
    memtable_mib: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let config = Config {
        writers: parse_arg(&args, 1, 1)?,
        total_batches: parse_arg(&args, 2, 300)?,
        records_per_batch: parse_arg(&args, 3, 1_000)?,
        value_bytes: parse_arg(&args, 4, 256)?,
        memtable_mib: parse_arg(&args, 5, 64)?,
    };
    if config.writers == 0
        || config.total_batches == 0
        || config.records_per_batch == 0
        || config.value_bytes == 0
    {
        return Err("all workload dimensions must be > 0".into());
    }

    let base = std::env::var_os("FEATHER_BENCH_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".storage-bench-tmp"));
    let path = base.join(format!(
        "fjall-tail-{}w-{}m-{}",
        config.writers,
        config.memtable_mib,
        std::process::id()
    ));
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    fs::create_dir_all(&path)?;

    let db = Database::builder(&path).worker_threads(4).open()?;
    let keyspace = db.keyspace("data", || {
        KeyspaceCreateOptions::default().max_memtable_size(config.memtable_mib * 1024 * 1024)
    })?;

    let barrier = Arc::new(Barrier::new(config.writers));
    let per_writer = config.total_batches.div_ceil(config.writers);
    let start = Instant::now();
    let mut joins = Vec::new();

    for writer_id in 0..config.writers {
        let db = db.clone();
        let keyspace = keyspace.clone();
        let barrier = barrier.clone();
        joins.push(thread::spawn(move || -> Result<Vec<u64>, String> {
            let first = writer_id * per_writer;
            let end = (first + per_writer).min(config.total_batches);
            let mut latencies = Vec::with_capacity(end.saturating_sub(first));
            barrier.wait();

            for batch_index in first..end {
                let batch_started = Instant::now();
                let mut batch = db.batch();
                for record_index in 0..config.records_per_batch {
                    let logical = (batch_index as u64)
                        .wrapping_mul(config.records_per_batch as u64)
                        .wrapping_add(record_index as u64);
                    let key = logical.to_be_bytes();
                    let value = value_for(logical, config.value_bytes);
                    batch.insert(&keyspace, key, value);
                }
                batch
                    .durability(Some(PersistMode::SyncAll))
                    .commit()
                    .map_err(|error| format!("batch commit failed: {error}"))?;
                latencies.push(batch_started.elapsed().as_micros() as u64);
            }
            Ok(latencies)
        }));
    }

    let mut latencies = Vec::with_capacity(config.total_batches);
    for join in joins {
        latencies.extend(join.join().map_err(|_| "writer thread panicked")??);
    }
    let foreground_elapsed = start.elapsed();

    db.persist(PersistMode::SyncAll)?;
    let compactions_at_foreground_end = db.compactions_completed();
    let maintenance_started = Instant::now();
    let mut stable_rounds = 0_u8;
    let mut last_completed = db.compactions_completed();
    while maintenance_started.elapsed() < Duration::from_secs(120) {
        thread::sleep(Duration::from_millis(100));
        let active = db.active_compactions();
        let completed = db.compactions_completed();
        if active == 0 && completed == last_completed {
            stable_rounds = stable_rounds.saturating_add(1);
            if stable_rounds >= 10 {
                break;
            }
        } else {
            stable_rounds = 0;
        }
        last_completed = completed;
    }
    let maintenance_wait = maintenance_started.elapsed();

    latencies.sort_unstable();
    let total_records = config.total_batches * config.records_per_batch;
    println!(
        "fjall-tail writers={} batches={} records_per_batch={} total_records={} value_bytes={} memtable_mib={} foreground_ms={} maintenance_wait_ms={} p50_us={} p95_us={} p99_us={} max_us={} compactions_at_fg_end={} compactions_final={} journals={} disk_bytes={}",
        config.writers,
        config.total_batches,
        config.records_per_batch,
        total_records,
        config.value_bytes,
        config.memtable_mib,
        foreground_elapsed.as_millis(),
        maintenance_wait.as_millis(),
        percentile(&latencies, 50),
        percentile(&latencies, 95),
        percentile(&latencies, 99),
        latencies.last().copied().unwrap_or_default(),
        compactions_at_foreground_end,
        db.compactions_completed(),
        db.journal_count(),
        db.disk_space()?,
    );

    drop(keyspace);
    drop(db);
    fs::remove_dir_all(path)?;
    Ok(())
}

fn parse_arg<T>(args: &[String], index: usize, default: T) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    Ok(match args.get(index) {
        Some(value) => value.parse()?,
        None => default,
    })
}

fn value_for(key: u64, len: usize) -> Vec<u8> {
    let mut state = key ^ 0x9e37_79b9_7f4a_7c15;
    let mut value = vec![0_u8; len];
    for byte in &mut value {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state = state.wrapping_mul(0x2545_f491_4f6c_dd1d);
        *byte = state as u8;
    }
    value
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() - 1) * percentile) / 100;
    values[index]
}
