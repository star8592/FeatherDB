use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

const CONTROL_ROUNDTRIPS: usize = 2_000;
const CONTROL_BYTES: usize = 512;
const BULK_BYTES: usize = 64 * 1024 * 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("gen") => generate(
            args.get(2).context("cert path")?,
            args.get(3).context("key path")?,
            args.get(4).context("server SAN")?,
        ),
        Some("server") => {
            server(
                args.get(2).context("bind addr")?.parse()?,
                args.get(3).context("cert path")?,
                args.get(4).context("key path")?,
            )
            .await
        }
        Some("client") => {
            client(
                args.get(2).context("server addr")?.parse()?,
                args.get(3).context("server name")?,
                args.get(4).context("cert path")?,
            )
            .await
        }
        _ => bail!(
            "usage: lan_probe gen <cert.der> <key.der> <san> | server <bind> <cert.der> <key.der> | client <server> <server-name> <cert.der>"
        ),
    }
}

fn generate(cert_path: &str, key_path: &str, san: &str) -> Result<()> {
    let certified = generate_simple_self_signed(vec![san.to_string()])?;
    fs::write(cert_path, certified.cert.der())?;
    fs::write(key_path, certified.signing_key.serialize_der())?;
    println!("generated cert={cert_path} key={key_path} san={san}");
    Ok(())
}

async fn server(bind: SocketAddr, cert_path: &str, key_path: &str) -> Result<()> {
    let cert = CertificateDer::from(fs::read(cert_path)?);
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(fs::read(key_path)?));
    let server_config = ServerConfig::with_single_cert(vec![cert], key)?;
    let endpoint = Endpoint::server(server_config, bind)?;
    println!("READY {}", endpoint.local_addr()?);

    let incoming = endpoint.accept().await.context("endpoint closed")?;
    let connection = incoming.await.context("handshake failed")?;
    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
        tokio::spawn(async move {
            let bytes = recv.read_to_end(BULK_BYTES + 1024).await?;
            send.write_all(&bytes).await?;
            send.finish()?;
            Ok::<(), anyhow::Error>(())
        });
    }
    endpoint.wait_idle().await;
    Ok(())
}

async fn client(server_addr: SocketAddr, server_name: &str, cert_path: &str) -> Result<()> {
    let cert = CertificateDer::from(fs::read(cert_path)?);
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(ClientConfig::with_root_certificates(Arc::new(roots))?);

    let handshake_started = Instant::now();
    let connection = endpoint.connect(server_addr, server_name)?.await?;
    let handshake = handshake_started.elapsed();

    let control_payload = vec![0x5a; CONTROL_BYTES];
    let mut latencies = Vec::with_capacity(CONTROL_ROUNDTRIPS);
    for _ in 0..CONTROL_ROUNDTRIPS {
        let started = Instant::now();
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(&control_payload).await?;
        send.finish()?;
        let echoed = recv.read_to_end(CONTROL_BYTES + 1).await?;
        if echoed != control_payload {
            bail!("control echo mismatch");
        }
        latencies.push(started.elapsed().as_micros() as u64);
    }
    latencies.sort_unstable();

    let bulk_payload = deterministic_bytes(BULK_BYTES);
    let bulk_started = Instant::now();
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&bulk_payload).await?;
    send.finish()?;
    let echoed = recv.read_to_end(BULK_BYTES + 1).await?;
    let bulk_elapsed = bulk_started.elapsed();
    if echoed != bulk_payload {
        bail!("bulk echo mismatch");
    }

    let duplex_mib_s = (BULK_BYTES as f64 * 2.0) / (1024.0 * 1024.0) / bulk_elapsed.as_secs_f64();
    println!(
        "quic-lan client={} server={} handshake_us={} p50_us={} p95_us={} p99_us={} max_us={} bulk_roundtrip_ms={} duplex_mib_s={:.2}",
        endpoint.local_addr()?,
        server_addr,
        handshake.as_micros(),
        percentile(&latencies, 50),
        percentile(&latencies, 95),
        percentile(&latencies, 99),
        latencies.last().copied().unwrap_or_default(),
        bulk_elapsed.as_millis(),
        duplex_mib_s,
    );
    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    Ok(())
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values[((values.len() - 1) * percentile) / 100]
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
