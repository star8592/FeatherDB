use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;

const CONTROL_ROUNDTRIPS: usize = 2_000;
const CONTROL_BYTES: usize = 512;
const BULK_BYTES: usize = 64 * 1024 * 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let certified = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert = certified.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());

    let server_config = ServerConfig::with_single_cert(vec![cert.clone()], key.into())?;
    let server = Endpoint::server(server_config, "127.0.0.1:0".parse()?)?;
    let server_addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let incoming = server.accept().await.context("server endpoint closed")?;
        let connection = incoming.await.context("server handshake failed")?;
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let bytes = recv.read_to_end(BULK_BYTES + 1024).await?;
                send.write_all(&bytes).await?;
                send.finish()?;
                Ok::<(), anyhow::Error>(())
            });
        }
        Ok::<(), anyhow::Error>(())
    });

    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let mut client = Endpoint::client("127.0.0.1:0".parse()?)?;
    client.set_default_client_config(ClientConfig::with_root_certificates(Arc::new(roots))?);

    let handshake_started = Instant::now();
    let connection = client
        .connect(server_addr, "localhost")?
        .await
        .context("client handshake failed")?;
    let handshake = handshake_started.elapsed();

    let control_payload = vec![0x5a; CONTROL_BYTES];
    let mut control_latencies = Vec::with_capacity(CONTROL_ROUNDTRIPS);
    for _ in 0..CONTROL_ROUNDTRIPS {
        let started = Instant::now();
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(&control_payload).await?;
        send.finish()?;
        let echoed = recv.read_to_end(CONTROL_BYTES + 1).await?;
        if echoed != control_payload {
            anyhow::bail!("control echo mismatch");
        }
        control_latencies.push(started.elapsed().as_micros() as u64);
    }
    control_latencies.sort_unstable();

    let bulk_payload = deterministic_bytes(BULK_BYTES);
    let bulk_started = Instant::now();
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&bulk_payload).await?;
    send.finish()?;
    let echoed = recv.read_to_end(BULK_BYTES + 1).await?;
    let bulk_elapsed = bulk_started.elapsed();
    if echoed != bulk_payload {
        anyhow::bail!("bulk echo mismatch");
    }

    let duplex_bytes = (BULK_BYTES as f64) * 2.0;
    let mib_per_second = duplex_bytes / (1024.0 * 1024.0) / bulk_elapsed.as_secs_f64();

    println!(
        "quic-loopback quinn=0.11.12 tokio=1.53.2 rustls=0.23.45 handshake_us={} control_roundtrips={} control_bytes={} p50_us={} p95_us={} p99_us={} max_us={} bulk_bytes={} bulk_roundtrip_ms={} duplex_mib_s={:.2}",
        handshake.as_micros(),
        CONTROL_ROUNDTRIPS,
        CONTROL_BYTES,
        percentile(&control_latencies, 50),
        percentile(&control_latencies, 95),
        percentile(&control_latencies, 99),
        control_latencies.last().copied().unwrap_or_default(),
        BULK_BYTES,
        bulk_elapsed.as_millis(),
        mib_per_second,
    );

    connection.close(0_u32.into(), b"done");
    client.wait_idle().await;
    let _ = server_task.await?;
    Ok(())
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() - 1) * percentile) / 100;
    values[index]
}

fn deterministic_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x8592_5a5a_dead_beef_u64;
    let mut bytes = vec![0_u8; len];
    for byte in &mut bytes {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state = state.wrapping_mul(0x2545_f491_4f6c_dd1d);
        *byte = state as u8;
    }
    bytes
}
