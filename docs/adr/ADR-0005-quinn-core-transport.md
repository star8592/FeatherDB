# ADR-0005: Quinn as the V0 Core Transport

Status: Accepted
Date: 2026-10-08

## Context

FeatherDB needs encrypted symmetric node-to-node transport for small membership/control traffic and large repair/migration streams. The protocol state machines must remain independently testable in deterministic simulation.

## Decision

Use Quinn 0.11.12 as the V0 core QUIC substrate with Tokio 1.53.2 and rustls 0.23.45 configured explicitly for the ring crypto provider. Use feather-transport-api for protocol outbound intent, feather-wire for the single FMSG wire format, and feather-transport-quinn for bounded production I/O.

Logical FeatherDB membership identity remains separate from transport-session authentication. A successful TLS connection does not by itself authorize topology ownership.

## Backpressure

Every peer has a bounded outbound queue and the node has a bounded inbound queue. Queue saturation is surfaced as MessageSubmit::Backpressure. Bulk migration must use bounded streaming; full-tablet read_to_end buffering is prohibited.

## Optional Iroh layer

Iroh remains an optional future connectivity layer for NAT traversal/public-key dialing/relay fallback. It is not a mandatory V0 dependency for directly reachable cluster nodes.

## Evidence

Loopback and real two-machine LAN benchmarks passed, and a real Quinn connection drove the actual SWIM Ping/Ack state machine. See NETWORK-SUBSTRATE-v0.md and 2026-10-08-quinn-substrate-bench.md.
