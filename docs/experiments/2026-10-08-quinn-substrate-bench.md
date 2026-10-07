# Quinn QUIC Substrate Benchmark — 2026-10-08

## Goal

Validate the current Quinn/rustls/Tokio stack as FeatherDB's production-network substrate candidate before coupling it to membership, control or migration protocol code.

## Versions

    Quinn 0.11.12
    Tokio 1.53.2
    rustls 0.23.45
    rcgen 0.14.10
    Rust 1.99

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

## Three-run results

| Metric | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| handshake | 623 us | 1,173 us | 1,024 us | 1,024 us |
| control p50 | 23 us | 22 us | 22 us | 22 us |
| control p95 | 40 us | 36 us | 33 us | 36 us |
| control p99 | 171 us | 117 us | 75 us | 117 us |
| control max | 373 us | 243 us | 266 us | 266 us |
| 64 MiB duplex echo | 197 ms | 185 ms | 178 ms | 185 ms |
| duplex throughput | 648.75 MiB/s | 691.01 MiB/s | 715.54 MiB/s | 691.01 MiB/s |
| peak RSS | 221,272 KiB | 212,756 KiB | 211,876 KiB | 212,756 KiB |

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
