#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use feather_transport_api::{MessageClass, MessageEnvelope, MessageSink, MessageSubmit, NodeId};
use feather_wire::{MAX_MESSAGE_PAYLOAD, MESSAGE_HEADER_BYTES, decode_message, encode_message};
use quinn::Connection;
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuinnMessageLimits {
    pub outbound_messages_per_peer: usize,
    pub inbound_messages: usize,
    pub max_message_bytes: usize,
}

impl Default for QuinnMessageLimits {
    fn default() -> Self {
        Self {
            outbound_messages_per_peer: 1_024,
            inbound_messages: 4_096,
            max_message_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QuinnMessageStats {
    pub accepted: u64,
    pub dropped_no_peer: u64,
    pub outbound_backpressure: u64,
    pub inbound_backpressure: u64,
    pub malformed_frames: u64,
    pub stream_errors: u64,
}

#[derive(Default)]
struct AtomicStats {
    accepted: AtomicU64,
    dropped_no_peer: AtomicU64,
    outbound_backpressure: AtomicU64,
    inbound_backpressure: AtomicU64,
    malformed_frames: AtomicU64,
    stream_errors: AtomicU64,
}

impl AtomicStats {
    fn snapshot(&self) -> QuinnMessageStats {
        QuinnMessageStats {
            accepted: self.accepted.load(Ordering::Relaxed),
            dropped_no_peer: self.dropped_no_peer.load(Ordering::Relaxed),
            outbound_backpressure: self.outbound_backpressure.load(Ordering::Relaxed),
            inbound_backpressure: self.inbound_backpressure.load(Ordering::Relaxed),
            malformed_frames: self.malformed_frames.load(Ordering::Relaxed),
            stream_errors: self.stream_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct OutboundFrame {
    envelope: MessageEnvelope,
}

struct PeerState {
    outbound: mpsc::Sender<OutboundFrame>,
    connection: Connection,
}

pub struct QuinnMessageTransport {
    local_node_id: NodeId,
    limits: QuinnMessageLimits,
    peers: BTreeMap<NodeId, PeerState>,
    inbound_tx: mpsc::Sender<MessageEnvelope>,
    inbound_rx: mpsc::Receiver<MessageEnvelope>,
    next_message_id: u64,
    stats: Arc<AtomicStats>,
}

impl QuinnMessageTransport {
    pub fn new(local_node_id: NodeId, limits: QuinnMessageLimits) -> Self {
        assert!(limits.outbound_messages_per_peer > 0);
        assert!(limits.inbound_messages > 0);
        assert!(limits.max_message_bytes > 0);
        assert!(limits.max_message_bytes <= MAX_MESSAGE_PAYLOAD);
        let (inbound_tx, inbound_rx) = mpsc::channel(limits.inbound_messages);
        Self {
            local_node_id,
            limits,
            peers: BTreeMap::new(),
            inbound_tx,
            inbound_rx,
            next_message_id: 1,
            stats: Arc::new(AtomicStats::default()),
        }
    }

    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    pub fn limits(&self) -> QuinnMessageLimits {
        self.limits
    }

    pub fn stats(&self) -> QuinnMessageStats {
        self.stats.snapshot()
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn attach_peer(&mut self, peer_id: NodeId, connection: Connection) {
        self.detach_peer(peer_id);
        let (outbound_tx, mut outbound_rx) =
            mpsc::channel::<OutboundFrame>(self.limits.outbound_messages_per_peer);
        self.peers.insert(
            peer_id,
            PeerState {
                outbound: outbound_tx,
                connection: connection.clone(),
            },
        );

        let outbound_connection = connection.clone();
        let outbound_stats = self.stats.clone();
        tokio::spawn(async move {
            while let Some(frame) = outbound_rx.recv().await {
                let Ok(bytes) = encode_message(&frame.envelope) else {
                    outbound_stats.stream_errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let result = async {
                    let mut stream = outbound_connection.open_uni().await?;
                    stream.write_all(&bytes).await?;
                    stream.finish()?;
                    Ok::<(), quinn::WriteError>(())
                }
                .await;
                if result.is_err() {
                    outbound_stats.stream_errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        });

        let inbound_connection = connection;
        let inbound_tx = self.inbound_tx.clone();
        let inbound_stats = self.stats.clone();
        let max_frame_bytes = self
            .limits
            .max_message_bytes
            .saturating_add(MESSAGE_HEADER_BYTES);
        let local_node_id = self.local_node_id;
        tokio::spawn(async move {
            loop {
                let mut recv = match inbound_connection.accept_uni().await {
                    Ok(stream) => stream,
                    Err(_) => break,
                };
                let frame = match recv.read_to_end(max_frame_bytes).await {
                    Ok(frame) => frame,
                    Err(_) => {
                        inbound_stats.stream_errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                let Ok(envelope) = decode_message(&frame) else {
                    inbound_stats
                        .malformed_frames
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                if envelope.payload.len() > max_frame_bytes.saturating_sub(MESSAGE_HEADER_BYTES)
                    || envelope.to != local_node_id
                {
                    inbound_stats
                        .malformed_frames
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if inbound_tx.try_send(envelope).is_err() {
                    inbound_stats
                        .inbound_backpressure
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    pub fn detach_peer(&mut self, peer_id: NodeId) -> bool {
        let Some(peer) = self.peers.remove(&peer_id) else {
            return false;
        };
        peer.connection.close(0_u32.into(), b"peer detached");
        true
    }

    pub fn try_recv_message(&mut self) -> Option<MessageEnvelope> {
        self.inbound_rx.try_recv().ok()
    }

    pub async fn recv_message(&mut self) -> Option<MessageEnvelope> {
        self.inbound_rx.recv().await
    }
}

impl MessageSink for QuinnMessageTransport {
    fn submit_message(
        &mut self,
        _now_tick: u64,
        from: NodeId,
        to: NodeId,
        class: MessageClass,
        payload: Vec<u8>,
    ) -> MessageSubmit {
        let message_id = self.next_message_id;
        self.next_message_id = self.next_message_id.saturating_add(1);

        if from != self.local_node_id || payload.len() > self.limits.max_message_bytes {
            self.stats.dropped_no_peer.fetch_add(1, Ordering::Relaxed);
            return MessageSubmit::Dropped { message_id };
        }
        let Some(peer) = self.peers.get(&to) else {
            self.stats.dropped_no_peer.fetch_add(1, Ordering::Relaxed);
            return MessageSubmit::Dropped { message_id };
        };
        let required_bytes = payload.len().saturating_add(MESSAGE_HEADER_BYTES);
        let envelope = MessageEnvelope {
            message_id,
            from,
            to,
            class,
            payload,
        };
        match peer.outbound.try_send(OutboundFrame { envelope }) {
            Ok(()) => {
                self.stats.accepted.fetch_add(1, Ordering::Relaxed);
                MessageSubmit::Accepted { message_id }
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.stats
                    .outbound_backpressure
                    .fetch_add(1, Ordering::Relaxed);
                MessageSubmit::Backpressure {
                    message_id,
                    required_messages: 1,
                    required_bytes,
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.stats.dropped_no_peer.fetch_add(1, Ordering::Relaxed);
                MessageSubmit::Dropped { message_id }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_over_local_limit_is_dropped_before_queueing() {
        let limits = QuinnMessageLimits {
            max_message_bytes: 4,
            ..QuinnMessageLimits::default()
        };
        let mut transport = QuinnMessageTransport::new(1, limits);
        assert!(matches!(
            transport.submit_message(0, 1, 2, MessageClass::Control, vec![0; 5]),
            MessageSubmit::Dropped { .. }
        ));
    }

    #[test]
    fn no_peer_is_explicitly_dropped() {
        let mut transport = QuinnMessageTransport::new(1, QuinnMessageLimits::default());
        assert!(matches!(
            transport.submit_message(0, 1, 2, MessageClass::Membership, vec![1]),
            MessageSubmit::Dropped { .. }
        ));
        assert_eq!(transport.stats().dropped_no_peer, 1);
    }
}

#[cfg(test)]
mod swim_integration_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use feather_sim::{MembershipConfig, SwimNode};
    use quinn::{ClientConfig, Endpoint, ServerConfig};
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::PrivatePkcs8KeyDer;

    use super::*;

    fn membership_config() -> MembershipConfig {
        MembershipConfig {
            probe_interval_ticks: 1,
            direct_timeout_ticks: 2,
            indirect_timeout_ticks: 4,
            suspicion_timeout_ticks: 8,
            indirect_checks: 2,
            max_awareness_score: 8,
            piggyback_updates: 8,
            update_retransmits: 16,
        }
    }

    async fn connected_pair() -> (Endpoint, Endpoint, Connection, Connection) {
        let certified = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = certified.cert.der().clone();
        let key = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
        let server_config = ServerConfig::with_single_cert(vec![cert.clone()], key.into()).unwrap();
        let server = Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let server_addr = server.local_addr().unwrap();

        let mut roots = RootCertStore::empty();
        roots.add(cert).unwrap();
        let mut client = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client.set_default_client_config(
            ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
        );

        let client_connecting = client.connect(server_addr, "localhost").unwrap();
        let server_incoming = server.accept().await.unwrap();
        let (client_result, server_result) = tokio::join!(client_connecting, server_incoming);
        (
            client,
            server,
            client_result.unwrap(),
            server_result.unwrap(),
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_outbound_queue_reports_backpressure_before_worker_runs() {
        let (client_endpoint, server_endpoint, client_connection, server_connection) =
            connected_pair().await;
        let limits = QuinnMessageLimits {
            outbound_messages_per_peer: 1,
            inbound_messages: 4,
            max_message_bytes: 1024,
        };
        let mut transport1 = QuinnMessageTransport::new(1, limits);
        let mut transport2 = QuinnMessageTransport::new(2, limits);
        transport1.attach_peer(2, client_connection);
        transport2.attach_peer(1, server_connection);

        assert!(matches!(
            transport1.submit_message(0, 1, 2, MessageClass::Control, vec![1]),
            MessageSubmit::Accepted { .. }
        ));
        assert!(matches!(
            transport1.submit_message(0, 1, 2, MessageClass::Control, vec![2]),
            MessageSubmit::Backpressure { .. }
        ));
        assert_eq!(transport1.stats().outbound_backpressure, 1);

        drop(transport1);
        drop(transport2);
        client_endpoint.close(0_u32.into(), b"done");
        server_endpoint.close(0_u32.into(), b"done");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_quinn_connection_drives_swim_ping_ack() {
        let (client_endpoint, server_endpoint, client_connection, server_connection) =
            connected_pair().await;

        let limits = QuinnMessageLimits {
            outbound_messages_per_peer: 16,
            inbound_messages: 16,
            max_message_bytes: 64 * 1024,
        };
        let mut transport1 = QuinnMessageTransport::new(1, limits);
        let mut transport2 = QuinnMessageTransport::new(2, limits);
        transport1.attach_peer(2, client_connection);
        transport2.attach_peer(1, server_connection);

        let mut node1 = SwimNode::new(1, &[1, 2], membership_config()).unwrap();
        let mut node2 = SwimNode::new(2, &[1, 2], membership_config()).unwrap();

        node1.tick(0, &mut transport1);
        assert_eq!(node1.pending_probe_target(), Some(2));

        let ping = tokio::time::timeout(Duration::from_secs(2), transport2.recv_message())
            .await
            .expect("ping timeout")
            .expect("ping message");
        node2.handle_message(0, ping, &mut transport2);

        let ack = tokio::time::timeout(Duration::from_secs(2), transport1.recv_message())
            .await
            .expect("ack timeout")
            .expect("ack message");
        node1.handle_message(0, ack, &mut transport1);

        assert_eq!(node1.pending_probe_target(), None);
        assert_eq!(node1.stats().acks_received, 1);
        assert_eq!(transport1.stats().accepted, 1);
        assert_eq!(transport2.stats().accepted, 1);

        drop(transport1);
        drop(transport2);
        client_endpoint.close(0_u32.into(), b"done");
        server_endpoint.close(0_u32.into(), b"done");
    }
}
