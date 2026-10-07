use std::error::Error;
use std::io::{self, Write};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use feather_storage_api::{DiskCompletion, DiskRequest, DiskSubmit, DurableStore};
use feather_storage_fjall::FjallDurableStore;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().ok_or("missing mode")?;
    let path = PathBuf::from(args.next().ok_or("missing path")?);
    let mut store = FjallDurableStore::open(&path)?;

    match mode.as_str() {
        "write-unsynced" => {
            put(&mut store)?;
            ready();
            sleep_forever();
        }
        "write-synced" => {
            put(&mut store)?;
            expect_unit(store.submit(0, DiskRequest::Sync { op_id: 2 }))?;
            ready();
            sleep_forever();
        }
        "check-absent" => {
            let value = read(&mut store)?;
            if value.is_some() {
                return Err(format!("expected absent value, got {value:?}").into());
            }
        }
        "check-present" => {
            let value = read(&mut store)?;
            if value.as_deref() != Some(b"durable-value".as_slice()) {
                return Err(format!("expected durable-value, got {value:?}").into());
            }
        }
        other => return Err(format!("unknown mode: {other}").into()),
    }

    Ok(())
}

fn put(store: &mut FjallDurableStore) -> Result<(), Box<dyn Error>> {
    expect_unit(store.submit(
        0,
        DiskRequest::Put {
            op_id: 1,
            key: b"probe-key".to_vec(),
            value: b"durable-value".to_vec(),
        },
    ))
}

fn read(store: &mut FjallDurableStore) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    match store.submit(
        0,
        DiskRequest::Read {
            op_id: 3,
            key: b"probe-key".to_vec(),
        },
    ) {
        DiskSubmit::Completed(DiskCompletion::Read(value)) => Ok(value),
        other => Err(format!("unexpected read result: {other:?}").into()),
    }
}

fn expect_unit(result: DiskSubmit) -> Result<(), Box<dyn Error>> {
    match result {
        DiskSubmit::Completed(DiskCompletion::Unit) => Ok(()),
        other => Err(format!("unexpected write result: {other:?}").into()),
    }
}

fn ready() {
    println!("READY");
    io::stdout().flush().expect("flush READY");
}

fn sleep_forever() -> ! {
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}
