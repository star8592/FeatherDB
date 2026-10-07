use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fjall::{Database as FjallDatabase, KeyspaceCreateOptions, PersistMode};
use redb::{Database as RedbDatabase, Durability, ReadableDatabase, TableDefinition};

const REDB_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("kv");
const REDB_STREAMING_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("streaming");

#[derive(Clone, Copy, Debug)]
struct Config {
    control_ops: usize,
    bulk_records: usize,
    value_bytes: usize,
    reads: usize,
    streaming_batch_size: usize,
}

#[derive(Debug)]
struct ResultRow {
    backend: &'static str,
    control_elapsed: Duration,
    bulk_elapsed: Duration,
    reopen_elapsed: Duration,
    read_elapsed: Duration,
    streaming_elapsed: Duration,
    streaming_batches: usize,
    checksum: u64,
    disk_bytes: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        return Err("usage: feather-storage-bench <fjall|redb> [control_ops] [bulk_records] [value_bytes] [reads]".into());
    }

    let config = Config {
        control_ops: parse_arg(&args, 2, 200)?,
        bulk_records: parse_arg(&args, 3, 100_000)?,
        value_bytes: parse_arg(&args, 4, 256)?,
        reads: parse_arg(&args, 5, 100_000)?,
        streaming_batch_size: parse_arg(&args, 6, 1_000)?,
    };
    if config.bulk_records == 0 || config.value_bytes == 0 || config.streaming_batch_size == 0 {
        return Err("bulk_records, value_bytes and streaming_batch_size must be > 0".into());
    }

    let root = unique_temp_root(&args[1]);
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }
    fs::create_dir_all(&root)?;

    let result = match args[1].as_str() {
        "fjall" => run_fjall(&root, config, false)?,
        "fjall-lowmem" => run_fjall(&root, config, true)?,
        "redb" => run_redb(&root, config)?,
        other => return Err(format!("unknown backend: {other}").into()),
    };

    println!(
        "storage-bench backend={} control_ops={} bulk_records={} value_bytes={} reads={} streaming_batch_size={}",
        result.backend,
        config.control_ops,
        config.bulk_records,
        config.value_bytes,
        config.reads,
        config.streaming_batch_size
    );
    println!(
        "timing control_ms={} bulk_ms={} streaming_ms={} streaming_batches={} reopen_ms={} reads_ms={}",
        result.control_elapsed.as_millis(),
        result.bulk_elapsed.as_millis(),
        result.streaming_elapsed.as_millis(),
        result.streaming_batches,
        result.reopen_elapsed.as_millis(),
        result.read_elapsed.as_millis(),
    );
    println!(
        "result checksum={} disk_bytes={}",
        result.checksum, result.disk_bytes
    );

    fs::remove_dir_all(&root)?;
    Ok(())
}

fn parse_arg(args: &[String], index: usize, default: usize) -> Result<usize, Box<dyn Error>> {
    Ok(match args.get(index) {
        Some(value) => value.parse()?,
        None => default,
    })
}

fn unique_temp_root(backend: &str) -> PathBuf {
    let base = std::env::var_os("FEATHER_BENCH_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".storage-bench-tmp"));
    base.join(format!(
        "feather-storage-bench-{}-{}",
        backend,
        std::process::id()
    ))
}

fn control_value(sequence: usize) -> Vec<u8> {
    let mut value = vec![0_u8; 2048];
    for (index, byte) in value.iter_mut().enumerate() {
        *byte = ((sequence as u64)
            .wrapping_mul(131)
            .wrapping_add(index as u64 * 17)
            & 0xff) as u8;
    }
    value
}

fn bulk_value(key: u64, len: usize) -> Vec<u8> {
    let mut value = vec![0_u8; len];
    let mut state = key ^ 0x9e37_79b9_7f4a_7c15;
    for byte in &mut value {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state = state.wrapping_mul(0x2545_f491_4f6c_dd1d);
        *byte = state as u8;
    }
    value
}

fn read_key(iteration: usize, records: usize) -> u64 {
    ((iteration as u64).wrapping_mul(1_000_003) % records as u64) + 1
}

fn checksum_bytes(mut checksum: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(0x0000_0100_0000_01b3);
    }
    checksum
}

fn run_fjall(root: &Path, config: Config, low_memory: bool) -> Result<ResultRow, Box<dyn Error>> {
    let db_path = root.join("fjall");
    let db = if low_memory {
        FjallDatabase::builder(&db_path)
            .cache_size(8 * 1024 * 1024)
            .worker_threads(1)
            .open()?
    } else {
        FjallDatabase::builder(&db_path).open()?
    };
    let keyspace_options = || {
        if low_memory {
            KeyspaceCreateOptions::default().max_memtable_size(8 * 1024 * 1024)
        } else {
            KeyspaceCreateOptions::default()
        }
    };
    let kv = db.keyspace("kv", keyspace_options)?;

    let control_started = Instant::now();
    for sequence in 0..config.control_ops {
        let value = control_value(sequence);
        kv.insert(0_u64.to_be_bytes(), value)?;
        db.persist(PersistMode::SyncAll)?;
    }
    let control_elapsed = control_started.elapsed();

    let bulk_started = Instant::now();
    let mut batch = db.batch();
    for index in 0..config.bulk_records {
        let key = (index as u64) + 1;
        batch.insert(&kv, key.to_be_bytes(), bulk_value(key, config.value_bytes));
    }
    batch.durability(Some(PersistMode::SyncAll)).commit()?;
    let bulk_elapsed = bulk_started.elapsed();

    let streaming = db.keyspace("streaming", keyspace_options)?;
    let streaming_started = Instant::now();
    let streaming_batches = config.bulk_records.div_ceil(config.streaming_batch_size);
    for batch_index in 0..streaming_batches {
        let start = batch_index * config.streaming_batch_size;
        let end = (start + config.streaming_batch_size).min(config.bulk_records);
        let mut batch = db.batch();
        for index in start..end {
            let key = (index as u64) + 1;
            batch.insert(
                &streaming,
                key.to_be_bytes(),
                bulk_value(key ^ 0xfeed_beef, config.value_bytes),
            );
        }
        batch.durability(Some(PersistMode::SyncAll)).commit()?;
    }
    let streaming_elapsed = streaming_started.elapsed();

    drop(streaming);
    drop(kv);
    drop(db);

    let reopen_started = Instant::now();
    let db = if low_memory {
        FjallDatabase::builder(&db_path)
            .cache_size(8 * 1024 * 1024)
            .worker_threads(1)
            .open()?
    } else {
        FjallDatabase::builder(&db_path).open()?
    };
    let kv = db.keyspace("kv", KeyspaceCreateOptions::default)?;
    let reopen_elapsed = reopen_started.elapsed();

    let read_started = Instant::now();
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    for iteration in 0..config.reads {
        let key = read_key(iteration, config.bulk_records);
        let value = kv
            .get(key.to_be_bytes())?
            .ok_or("fjall missing benchmark key")?;
        checksum = checksum_bytes(checksum, value.as_ref());
    }
    let read_elapsed = read_started.elapsed();
    drop(kv);
    drop(db);

    Ok(ResultRow {
        backend: if low_memory {
            "fjall-3.1.12-lowmem-syncall"
        } else {
            "fjall-3.1.12-syncall"
        },
        control_elapsed,
        bulk_elapsed,
        reopen_elapsed,
        read_elapsed,
        streaming_elapsed,
        streaming_batches,
        checksum,
        disk_bytes: recursive_size(&db_path)?,
    })
}

fn run_redb(root: &Path, config: Config) -> Result<ResultRow, Box<dyn Error>> {
    let db_path = root.join("redb.db");
    let db = RedbDatabase::create(&db_path)?;

    {
        let mut txn = db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        let _ = txn.open_table(REDB_TABLE)?;
        let _ = txn.open_table(REDB_STREAMING_TABLE)?;
        txn.commit()?;
    }

    let control_started = Instant::now();
    for sequence in 0..config.control_ops {
        let value = control_value(sequence);
        let mut txn = db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        {
            let mut table = txn.open_table(REDB_TABLE)?;
            table.insert(0, value.as_slice())?;
        }
        txn.commit()?;
    }
    let control_elapsed = control_started.elapsed();

    let bulk_started = Instant::now();
    let mut txn = db.begin_write()?;
    txn.set_durability(Durability::Immediate)?;
    {
        let mut table = txn.open_table(REDB_TABLE)?;
        for index in 0..config.bulk_records {
            let key = (index as u64) + 1;
            let value = bulk_value(key, config.value_bytes);
            table.insert(key, value.as_slice())?;
        }
    }
    txn.commit()?;
    let bulk_elapsed = bulk_started.elapsed();

    let streaming_started = Instant::now();
    let streaming_batches = config.bulk_records.div_ceil(config.streaming_batch_size);
    for batch_index in 0..streaming_batches {
        let start = batch_index * config.streaming_batch_size;
        let end = (start + config.streaming_batch_size).min(config.bulk_records);
        let mut txn = db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        {
            let mut table = txn.open_table(REDB_STREAMING_TABLE)?;
            for index in start..end {
                let key = (index as u64) + 1;
                let value = bulk_value(key ^ 0xfeed_beef, config.value_bytes);
                table.insert(key, value.as_slice())?;
            }
        }
        txn.commit()?;
    }
    let streaming_elapsed = streaming_started.elapsed();

    drop(db);

    let reopen_started = Instant::now();
    let db = RedbDatabase::open(&db_path)?;
    let reopen_elapsed = reopen_started.elapsed();

    let read_started = Instant::now();
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    let txn = db.begin_read()?;
    let table = txn.open_table(REDB_TABLE)?;
    for iteration in 0..config.reads {
        let key = read_key(iteration, config.bulk_records);
        let value = table.get(key)?.ok_or("redb missing benchmark key")?;
        checksum = checksum_bytes(checksum, value.value());
    }
    let read_elapsed = read_started.elapsed();
    drop(table);
    drop(txn);
    drop(db);

    Ok(ResultRow {
        backend: "redb-4.3.0-immediate",
        control_elapsed,
        bulk_elapsed,
        reopen_elapsed,
        read_elapsed,
        streaming_elapsed,
        streaming_batches,
        checksum,
        disk_bytes: fs::metadata(&db_path)?.len(),
    })
}

fn recursive_size(path: &Path) -> Result<u64, Box<dyn Error>> {
    if path.is_file() {
        return Ok(fs::metadata(path)?.len());
    }
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        if child.is_dir() {
            total = total.saturating_add(recursive_size(&child)?);
        } else {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}
