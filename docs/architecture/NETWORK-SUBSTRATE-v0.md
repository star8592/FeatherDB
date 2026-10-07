# Network Substrate v0

Status: candidate architecture; QUIC benchmark is being validated before production adapter freeze.

## Goals

FeatherDB needs one production transport substrate that preserves the simulator's protocol intent while remaining:

- encrypted by default;
- peer-to-peer and symmetric;
- suitable for arbitrary node join/remove;
- efficient for many small control messages and large migration/repair transfers;
- usable on Linux x86_64/ARM64 without external sidecars;
- compatible with deterministic protocol testing.

## Current candidates

### Quinn 0.11.12 — primary core candidate

Quinn is a pure-Rust QUIC implementation with:

- simultaneous client/server operation;
- bidirectional/unidirectional streams;
- application datagrams;
- rustls-backed TLS;
- a high-level async API;
- quinn-proto as a sans-I/O protocol state machine.

The sans-I/O split is especially attractive for FeatherDB because production I/O and deterministic simulation can remain separate concerns.

Current benchmark dependency set:

    quinn 0.11.12
    tokio 1.53.2
    rustls 0.23.45
    rcgen 0.14.10
    Rust 1.99

## Iroh 1.3.0 — optional future connectivity layer

Iroh provides peer-to-peer QUIC connections dialed by public key, with:

- endpoint public key as authenticated identity;
- direct-address connectivity;
- NAT hole punching;
- encrypted relay fallback;
- multiple QUIC streams.

These features fit FeatherDB's long-term heterogeneous/edge/decentralized-node story very well.

However, Iroh also introduces substantially more connectivity machinery than a normal LAN/datacenter cluster needs. FeatherDB's V0 complexity budget therefore should not require Iroh for every node.

Current direction:

    core cluster transport:
        Quinn directly

    optional edge / internet / NAT traversal:
        Iroh-compatible connectivity layer later

This keeps the common path small while preserving an upgrade path for machines that cannot directly reach each other.

## Logical node identity vs TLS session identity

FeatherDB must not define NodeId as an IP address.

The long-term logical identity remains public-key based.

For the first Quinn production adapter, transport authentication and logical FeatherDB membership authorization must remain separate concepts:

    TLS authenticates the encrypted transport endpoint
    FeatherDB control plane authorizes the logical node identity

Do not let a successful TLS handshake implicitly grant cluster ownership rights.

A later identity design may bind the persistent FeatherDB node public key into the transport certificate or use a cluster CA/bootstrap proof. That decision is not required for the substrate benchmark.

## Traffic classes

The current simulator already distinguishes:

    Membership
    Gossip
    Control
    Data
    Repair
    Client

The production transport should preserve these semantics rather than flattening all traffic into one FIFO.

Provisional QUIC mapping:

- Membership/Gossip: small messages; datagram is a candidate where loss is acceptable, otherwise short-lived or multiplexed streams.
- Control: reliable ordered stream(s), bounded and high priority.
- Data/Repair: dedicated bulk streams with explicit application backpressure.
- Client: independent request streams so bulk repair cannot cause head-of-line blocking across unrelated requests.

The final stream/datagram mapping must be benchmarked; this document does not freeze it.

## Backpressure

The simulator already makes backpressure explicit.

Production QUIC integration must expose equivalent pressure rather than allowing unbounded task spawning or buffering.

Required limits include:

- max in-flight control messages;
- max concurrent migration/repair streams per peer;
- max buffered bytes per peer and globally;
- max stream receive windows appropriate to node capacity;
- cancellation when topology epochs invalidate work.

Weak nodes must be able to advertise smaller limits.

## Benchmark gate

Before Quinn is promoted into a production crate, the loopback benchmark must verify:

1. TLS/QUIC handshake succeeds with a real rustls trust root;
2. repeated 512-byte bidirectional control messages produce bounded p50/p95/p99/max latency;
3. a 64 MiB bidirectional bulk stream completes without corruption;
4. release RSS is recorded;
5. dependency versions and lockfile are reproducible.

This is a substrate microbenchmark only. It is not a claim about WAN performance.

## Next production steps

1. finish Quinn loopback substrate benchmark;
2. define a neutral production transport API, without making feather-sim a dependency;
3. adapt SimNetwork to that intent API where appropriate;
4. implement Quinn transport behind the same logical message classes;
5. test connection loss, reconnect, stream cancellation and backpressure;
6. run multi-machine tests across the main server and XPS15;
7. only then route SWIM/control/migration traffic over production QUIC.


## Benchmark result

The first Quinn 0.11.12 loopback benchmark passed three consecutive runs. In the final ring-only configuration, median handshake was 1.180 ms; 512-byte fresh bidirectional-stream control RTT measured p50 23 us / p95 41 us / p99 189 us; a 64 MiB bidirectional echo completed in 200 ms, approximately 640 MiB/s aggregate duplex goodput.

The benchmark's ~208 MiB median peak RSS is dominated by deliberately holding 64 MiB send and receive buffers plus both endpoints in one process. Production repair/migration must be chunk-streaming and bounded; read_to_end on a tablet-sized payload is prohibited.

Quinn is therefore accepted as the V0 core substrate candidate, subject to a later multi-machine and reconnect/backpressure gate.

The existing MigrationTransport abstraction is explicitly not considered a production network API. It is a deterministic migration-progress intent used by the simulator. Production networking needs outbound protocol intents carrying real payload/address/identity information.

See docs/experiments/2026-10-08-quinn-substrate-bench.md.


## Official-source audit

Current candidate versions were checked against their official crate documentation on 2026-10-08.

Quinn 0.11.12 documents simultaneous client/server operation, streams, datagrams, stable Rust support, and its `quinn-proto` sans-I/O protocol layer. `quinn::Runtime` abstracts timers, task spawning and UDP socket wrapping, which supports FeatherDB's separation between protocol intent and physical runtime.

Iroh 1.3.0 is a higher-level public-key-addressed P2P QUIC stack. Its endpoint/connection layer adds connectivity behavior that is useful for NAT/relay scenarios but unnecessary for the minimal V0 datacenter/LAN path.

Candidate references:

- https://docs.rs/quinn/0.11.12/quinn/
- https://docs.rs/quinn/0.11.12/quinn/trait.Runtime.html
- https://docs.rs/iroh/1.3.0/iroh/


## Crypto provider policy

The V0 Quinn/rustls stack is explicitly ring-only. Direct rustls dependencies use default-features = false with ring/std/tls12, while Quinn uses rustls-ring. This avoids implicitly compiling aws-lc-rs alongside ring and keeps the common binary/dependency surface smaller.


## Real two-machine gate

The Quinn substrate has now passed a real Z890-to-XPS15 LAN test with certificate verification enabled. Across three runs, median handshake was 5.006 ms, control-stream RTT p50/p95/p99 was 2.405/4.299/6.040 ms, and 64 MiB bidirectional echo delivered approximately 43.77 MiB/s aggregate duplex goodput across the current Ethernet/Wi-Fi path.

This clears the V0 multi-machine substrate gate. It does not yet clear WAN/NAT/reconnect stress.
