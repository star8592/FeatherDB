# Quinn QUIC Substrate Benchmark — 2026-10-08

## Goal

Validate the current Quinn/rustls/Tokio stack as FeatherDB's production-network substrate candidate before coupling it to membership, control or migration protocol code.

## Versions

    Quinn 0.11.12
    Tokio 1.53.2
    rustls 0.23.45, ring-only
    rcgen 0.14.10
    Rust 1.99

The final benchmark explicitly disables rustls default features and enables the ring provider. This removes the implicit aws-lc-rs/aws-lc-sys dependency rather than compiling two crypto providers.

All versions were current stable releases when the experiment was created.

## Workload

One client and one server endpoint run over IPv4 loopback with a real QUIC/TLS handshake and an rcgen self-signed certificate trusted by the client.

After connecting:

1. open 2,000 bidirectional streams sequentially;
2. send 512 bytes and echo the exact bytes;
3. record per-stream round-trip latency;
4. open one additional bidirectional stream;
5. send and echo 64 MiB;
6. verify every returned byte;
7. record release-process RSS.

## Three-run results — final ring-only configuration

| Metric | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| handshake | 1,182 us | 1,180 us | 636 us | 1,180 us |
| control p50 | 23 us | 23 us | 23 us | 23 us |
| control p95 | 38 us | 41 us | 45 us | 41 us |
| control p99 | 112 us | 207 us | 189 us | 189 us |
| control max | 342 us | 606 us | 306 us | 342 us |
| 64 MiB duplex echo | 200 ms | 189 ms | 203 ms | 200 ms |
| duplex throughput | 639.51 MiB/s | 675.40 MiB/s | 628.60 MiB/s | 639.51 MiB/s |
| peak RSS | 216,488 KiB | 211,984 KiB | 218,196 KiB | 216,488 KiB |

The earlier default-rustls run was discarded as the architectural baseline because the direct rustls dependency enabled aws-lc-rs while Quinn itself was configured for ring. The ring-only result above is the reproducible V0 reference.

## Interpretation

The loopback substrate is healthy: handshake and short-stream latency are small relative to database/storage operations, and large-stream throughput is sufficient to continue with Quinn as the core transport candidate.

The ~208 MiB median RSS is NOT treated as a per-connection production memory requirement. The benchmark deliberately allocates a 64 MiB send buffer and receives a 64 MiB echoed buffer in the same process while both client and server endpoints coexist. Production migration must stream bounded chunks instead of read_to_end on full tablet payloads.

## Architectural consequence

Quinn is accepted as the V0 core QUIC substrate candidate. This does not yet mean the existing MigrationTransport trait can be implemented directly by Quinn: the current trait models simulated transfer progress by byte count and lacks real peer addressing, payload ownership and async stream lifecycle.

The next network architecture step is therefore protocol/transport separation:

    protocol state machine
        -> outbound message/transfer intents
        -> simulator adapter OR async Quinn runtime

Do not fake production Quinn behind a simulation-only interface.

## Iroh note

Iroh 1.3.0 provides public-key endpoint identity, NAT traversal and encrypted relay fallback over QUIC. It remains an attractive optional edge/internet connectivity layer, but is not required for the V0 core cluster path because its additional machinery conflicts with FeatherDB's minimal common-path goal.

## Scope limitations

This is loopback only. It does not yet measure WAN RTT, packet loss, NAT traversal, multi-machine throughput, congestion control, datagram loss, reconnect behavior, thousands of peers or application backpressure.


## Independent verification rerun

A second three-run release verification reproduced the same order of magnitude and architecture conclusion:

| Metric | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| handshake | 1,220 us | 1,178 us | 1,319 us | 1,220 us |
| control p50 | 23 us | 23 us | 21 us | 23 us |
| control p95 | 42 us | 32 us | 33 us | 33 us |
| control p99 | 207 us | 97 us | 121 us | 121 us |
| control max | 490 us | 378 us | 462 us | 462 us |
| 64 MiB duplex echo | 185 ms | 199 ms | 179 ms | 185 ms |
| duplex throughput | 690.36 MiB/s | 641.54 MiB/s | 711.12 MiB/s | 690.36 MiB/s |
| peak RSS | 219,708 KiB | 213,284 KiB | 209,540 KiB | 213,284 KiB |

The rerun confirms the candidate decision. The RSS caveat remains unchanged because the benchmark intentionally materializes large send and receive buffers in one process.


## Two-machine LAN validation

A second gate ran the same TLS/QUIC workload across two real FeatherDB development machines on 10.10.10.0/24:

    server: Z890, 10.10.10.42, wired Ethernet
    client: XPS15, 10.10.10.40, Wi-Fi

ICMP baseline before the test was approximately 3 ms RTT with 0% packet loss. The release lan_probe binary was built on the Z890 and copied to the XPS15. The server used a real self-signed certificate containing the server IP SAN; the client trusted exactly that certificate. Certificate verification was not disabled.

Three runs:

| Metric | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| handshake | 5.006 ms | 4.080 ms | 6.250 ms | 5.006 ms |
| control p50 | 2.130 ms | 3.492 ms | 2.405 ms | 2.405 ms |
| control p95 | 4.133 ms | 4.414 ms | 4.299 ms | 4.299 ms |
| control p99 | 4.453 ms | 6.271 ms | 6.040 ms | 6.040 ms |
| control max | 41.722 ms | 44.087 ms | 42.037 ms | 42.037 ms |
| 64 MiB duplex echo | 2.924 s | 2.938 s | 2.848 s | 2.924 s |
| duplex throughput | 43.77 MiB/s | 43.56 MiB/s | 44.94 MiB/s | 43.77 MiB/s |
| client peak RSS | 204,960 KiB | 206,508 KiB | 205,280 KiB | 205,280 KiB |

This validates real cross-machine UDP routing, QUIC handshake, TLS certificate verification, repeated stream creation and bulk integrity. The measured throughput is a property of the current wired/Wi-Fi LAN path, not a Quinn ceiling.
