//! MQTT adapter (feature `transport-mqtt`), using rumqttc (async) with QoS 0.
use crate::transport::{
    ConnectOptions, IncomingQuery, Payload, Publisher, QueryRegistration, QueryResponder,
    QueryResponderInner, Subscription, Transport, TransportError, TransportMessage,
};
use bytes::Bytes;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use std::time::Duration;
use tokio::task::JoinHandle;

fn fnv1a64_bytes(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let prime: u64 = 0x100000001b3;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(prime);
    }
    h
}

fn sanitize_client_id_base(base: &str) -> String {
    // Keep IDs reasonably short and portable across brokers.
    // Replace non [A-Za-z0-9_-] with '_' and cap length.
    let mut out = String::with_capacity(base.len().min(48));
    for ch in base.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
        if out.len() >= 48 {
            break;
        }
    }
    if out.is_empty() {
        out.push_str("mqb");
    }
    out
}

#[derive(Clone)]
pub struct MqttTransport {
    host: String,
    port: u16,
    keep_alive: Duration,
    username: Option<String>,
    password: Option<String>,
    max_in: usize,
    max_out: usize,
    qos: QoS,
    client_id: Option<String>,
    clean_session: bool,
}

pub async fn connect(opts: ConnectOptions) -> Result<Box<dyn Transport>, TransportError> {
    let host = opts
        .params
        .get("host")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1".into());
    let port: u16 = opts
        .params
        .get("port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1883);
    let keep_alive = Duration::from_secs(30);
    // Parse max packet sizes; allow a single "max_packet" for both directions, or per-direction overrides
    let default_max: usize = 2 * 1024 * 1024; // 2 MiB default to comfortably allow 16 KiB+ payloads
    let max_both: Option<usize> = opts.params.get("max_packet").and_then(|s| s.parse().ok());
    let max_in: usize = opts
        .params
        .get("max_in")
        .and_then(|s| s.parse().ok())
        .or(max_both)
        .unwrap_or(default_max);
    let max_out: usize = opts
        .params
        .get("max_out")
        .and_then(|s| s.parse().ok())
        .or(max_both)
        .unwrap_or(default_max);
    // Determine QoS (0,1,2)
    let qos_level: u8 = opts
        .params
        .get("qos")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let qos = match qos_level {
        2 => QoS::ExactlyOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::AtMostOnce,
    };
    // Parse client_id and clean_session for persistent sessions
    let client_id = opts.params.get("client_id").cloned();
    let clean_session: bool = opts
        .params
        .get("clean_session")
        .map(|s| s != "false" && s != "0")
        .unwrap_or(true); // Default true for backward compatibility
    // We don't keep this MqttOptions; we store connection params to create per-role clients
    Ok(Box::new(MqttTransport {
        host,
        port,
        keep_alive,
        username: opts.params.get("username").cloned(),
        password: opts.params.get("password").cloned(),
        max_in,
        max_out,
        qos,
        client_id,
        clean_session,
    }))
}

#[async_trait::async_trait]
impl Transport for MqttTransport {
    async fn subscribe(
        &self,
        expr: &str,
        handler: Box<dyn Fn(TransportMessage) + Send + Sync + 'static>,
    ) -> Result<Box<dyn Subscription>, TransportError> {
        // Create a dedicated client + eventloop for this subscription
        // Use a stable, per-topic client_id so broker-side session state (clean_session=false)
        // can be recovered across reconnects, while still avoiding collisions across many topics.
        let topic = map_expr(expr);
        let base = self
            .client_id
            .as_deref()
            .unwrap_or_else(|| {
                // If no base client_id is provided, fall back to a process-unique value.
                // Multi-topic roles are expected to provide a base id (derived from run-id).
                "mqb"
            });
        let base = sanitize_client_id_base(base);
        let topic_hash = fnv1a64_bytes(topic.as_bytes());
        let cid = format!("sub-{}-{:016x}", base, topic_hash);
        let cid_debug = cid.clone();
        let mut options = MqttOptions::new(cid, self.host.clone(), self.port);
        options.set_keep_alive(self.keep_alive);
        options.set_max_packet_size(self.max_in, self.max_out);
        options.set_clean_session(self.clean_session);
        if let Some(user) = &self.username {
            if let Some(pass) = &self.password {
                options.set_credentials(user, pass);
            }
        }
        let (client, mut eventloop) = AsyncClient::new(options, 65536);
        client
            .subscribe(topic.clone(), self.qos)
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        let handler = std::sync::Arc::new(handler);
        let qos = self.qos;
        let handle: JoinHandle<()> = tokio::spawn(async move {
            let mut consecutive_errors = 0u32;
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        consecutive_errors = 0;
                        (handler)(TransportMessage {
                            payload: Payload::from_bytes(Bytes::from(p.payload.to_vec())),
                        });
                    }
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                        // Reconnected - re-subscribe to ensure we get messages
                        consecutive_errors = 0;
                        tracing::info!(client_id = %cid_debug, "MQTT reconnected, re-subscribing");
                        if let Err(e) = client.subscribe(topic.clone(), qos).await {
                            tracing::warn!(client_id = %cid_debug, error = %e, "Failed to re-subscribe after reconnect");
                        }
                    }
                    Ok(_) => {
                        consecutive_errors = 0;
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        // Log at different levels based on error frequency
                        if consecutive_errors == 1 {
                            tracing::warn!(client_id = %cid_debug, error = %e, "MQTT connection error, will retry");
                        } else if consecutive_errors % 10 == 0 {
                            tracing::debug!(client_id = %cid_debug, error = %e, consecutive = consecutive_errors, "MQTT still reconnecting");
                        }
                        // Brief backoff before retry to avoid busy-loop
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        // Continue polling - rumqttc will attempt reconnection
                    }
                }
            }
            // drop client on exit (unreachable in normal operation)
            #[allow(unreachable_code)]
            drop(client);
        });
        Ok(Box::new(MqttSubscription { handle }))
    }

    async fn create_publisher(&self, topic: &str) -> Result<Box<dyn Publisher>, TransportError> {
        // Dedicated client + background poller for publisher
        // Use a stable, per-topic client_id so broker-side inflight state can be recovered
        // across reconnects, while still avoiding collisions across many topics.
        let base = self
            .client_id
            .as_deref()
            .unwrap_or_else(|| {
                // If no base client_id is provided, fall back to a process-unique value.
                // Multi-topic roles are expected to provide a base id (derived from run-id).
                "mqb"
            });
        let base = sanitize_client_id_base(base);
        let topic_hash = fnv1a64_bytes(topic.as_bytes());
        let cid = format!("pub-{}-{:016x}", base, topic_hash);
        let cid_debug = cid.clone();
        let mut options = MqttOptions::new(cid, self.host.clone(), self.port);
        options.set_keep_alive(self.keep_alive);
        options.set_max_packet_size(self.max_in, self.max_out);
        options.set_clean_session(self.clean_session);
        if let Some(user) = &self.username {
            if let Some(pass) = &self.password {
                options.set_credentials(user, pass);
            }
        }
        let (client, mut eventloop) = AsyncClient::new(options, 65536);
        let poller = tokio::spawn(async move {
            let mut consecutive_errors = 0u32;
            loop {
                match eventloop.poll().await {
                    Ok(_) => {
                        consecutive_errors = 0;
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        if consecutive_errors == 1 {
                            tracing::warn!(client_id = %cid_debug, error = %e, "MQTT publisher connection error, will retry");
                        } else if consecutive_errors % 10 == 0 {
                            tracing::debug!(client_id = %cid_debug, consecutive = consecutive_errors, "MQTT publisher still reconnecting");
                        }
                        // Brief backoff before retry
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
        Ok(Box::new(MqttPublisher {
            client,
            topic: topic.to_string(),
            qos: self.qos,
            poller,
        }))
    }

    async fn request(&self, _subject: &str, _payload: Bytes) -> Result<Payload, TransportError> {
        // Per-request client with unique reply topic
        let corr = uuid::Uuid::new_v4().to_string();
        let reply_topic = format!("mqb/replies/{}", corr);

        let mut sub_opts = MqttOptions::new(
            format!("req-sub-{}", uuid::Uuid::new_v4()),
            self.host.clone(),
            self.port,
        );
        sub_opts.set_keep_alive(self.keep_alive);
        sub_opts.set_max_packet_size(self.max_in, self.max_out);
        if let Some(user) = &self.username {
            if let Some(pass) = &self.password {
                sub_opts.set_credentials(user, pass);
            }
        }
        let (sub_client, mut sub_el) = AsyncClient::new(sub_opts, 65536);
        sub_client
            .subscribe(&reply_topic, self.qos)
            .await
            .map_err(|e| TransportError::Request(e.to_string()))?;

        // Publisher client
        let mut pub_opts = MqttOptions::new(
            format!("req-pub-{}", uuid::Uuid::new_v4()),
            self.host.clone(),
            self.port,
        );
        pub_opts.set_keep_alive(self.keep_alive);
        pub_opts.set_max_packet_size(self.max_in, self.max_out);
        if let Some(user) = &self.username {
            if let Some(pass) = &self.password {
                pub_opts.set_credentials(user, pass);
            }
        }
        let (pub_client, mut pub_el) = AsyncClient::new(pub_opts, 65536);
        // Drive publisher eventloop in background
        let _pub_poller = tokio::spawn(async move { while pub_el.poll().await.is_ok() {} });

        // Build envelope: [u16 reply_len][reply_topic][payload]
        let env = encode_req_env(&reply_topic, _payload.as_ref());
        pub_client
            .publish(_subject, self.qos, false, env)
            .await
            .map_err(|e| TransportError::Request(e.to_string()))?;

        // Wait for first reply
        let res = loop {
            match sub_el.poll().await {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic == reply_topic {
                        break Ok(Payload::from_bytes(Bytes::from(p.payload.to_vec())));
                    }
                }
                Ok(_) => {}
                Err(e) => break Err(TransportError::Request(e.to_string())),
            }
        };
        // cleanup: drop clients; poller will end
        drop(sub_client);
        drop(pub_client);
        res
    }

    async fn register_queryable(
        &self,
        _subject: &str,
        _handler: Box<dyn Fn(IncomingQuery) + Send + Sync + 'static>,
    ) -> Result<Box<dyn QueryRegistration>, TransportError> {
        let subject = _subject.to_string();
        let handler = std::sync::Arc::new(_handler);
        // Dedicated client + eventloop for serving queries
        let mut options = MqttOptions::new(
            format!("qry-{}", uuid::Uuid::new_v4()),
            self.host.clone(),
            self.port,
        );
        options.set_keep_alive(self.keep_alive);
        options.set_max_packet_size(self.max_in, self.max_out);
        if let Some(user) = &self.username {
            if let Some(pass) = &self.password {
                options.set_credentials(user, pass);
            }
        }
        let (client, mut eventloop) = AsyncClient::new(options, 65536);
        client
            .subscribe(&subject, self.qos)
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        let handle: JoinHandle<()> = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        if let Some((reply, payload)) = decode_req_env(&p.payload) {
                            let responder = MqttResponder {
                                client: client.clone(),
                                topic: reply,
                            };
                            let incoming = IncomingQuery {
                                subject: p.topic.clone(),
                                payload: Payload::from_bytes(Bytes::from(payload)),
                                correlation: None,
                                responder: QueryResponder {
                                    inner: std::sync::Arc::new(responder),
                                },
                            };
                            (handler)(incoming);
                        }
                    }
                    Ok(_) => {}
                    Err(_e) => break,
                }
            }
        });
        Ok(Box::new(MqttQueryRegistration { handle }))
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        Ok(())
    }
    async fn health_check(&self) -> Result<(), TransportError> {
        Ok(())
    }
    async fn force_disconnect(&self) -> Result<(), TransportError> {
        // MQTT: Transport stores connection params; actual clients are created per operation.
        // This is a no-op since we can't force-close connections we don't hold.
        // The crash simulation will work by dropping/recreating the transport.
        Ok(())
    }
}

struct MqttPublisher {
    client: AsyncClient,
    topic: String,
    qos: QoS,
    poller: JoinHandle<()>,
}

#[async_trait::async_trait]
impl Publisher for MqttPublisher {
    async fn publish(&self, payload: Bytes) -> Result<(), TransportError> {
        self.client
            .publish(&self.topic, self.qos, false, payload.to_vec())
            .await
            .map_err(|e| TransportError::Publish(e.to_string()))?;
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), TransportError> {
        // Graceful shutdown - client will send DISCONNECT when dropped
        self.poller.abort();
        Ok(())
    }
    async fn force_disconnect(&self) -> Result<(), TransportError> {
        // HARD CRASH: Abort the eventloop immediately.
        // This causes the TCP connection to be dropped without sending DISCONNECT.
        // The broker will see this as an unexpected disconnect (like network failure).
        // This is critical for testing QoS delivery guarantees.
        self.poller.abort();
        // Note: We don't call client.disconnect() - that would send DISCONNECT packet
        Ok(())
    }
}

struct MqttSubscription {
    handle: JoinHandle<()>,
}

#[async_trait::async_trait]
impl Subscription for MqttSubscription {
    async fn shutdown(&self) -> Result<(), TransportError> {
        self.handle.abort();
        Ok(())
    }
    async fn force_disconnect(&self) -> Result<(), TransportError> {
        // HARD CRASH: Abort the eventloop immediately without graceful close.
        // Broker will see this as unexpected disconnect.
        self.handle.abort();
        Ok(())
    }
}

struct MqttQueryRegistration {
    handle: JoinHandle<()>,
}

#[async_trait::async_trait]
impl QueryRegistration for MqttQueryRegistration {
    async fn shutdown(&self) -> Result<(), TransportError> {
        self.handle.abort();
        Ok(())
    }
}

struct MqttResponder {
    client: AsyncClient,
    topic: String,
}

#[async_trait::async_trait]
impl QueryResponderInner for MqttResponder {
    async fn send(&self, payload: Bytes) -> Result<(), TransportError> {
        self.client
            .publish(&self.topic, QoS::AtLeastOnce, false, payload.to_vec())
            .await
            .map_err(|e| TransportError::Publish(e.to_string()))?;
        Ok(())
    }
    async fn end(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

// Helpers
fn map_expr(expr: &str) -> String {
    if let Some(prefix) = expr.strip_suffix("/**") {
        format!("{}#/", prefix)
            .trim_end_matches('/')
            .replace("#/", "#")
    } else {
        expr.to_string()
    }
}

fn encode_req_env(reply_topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + reply_topic.len() + payload.len());
    let len = reply_topic.len() as u16;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(reply_topic.as_bytes());
    out.extend_from_slice(payload);
    out
}

fn decode_req_env(buf: &[u8]) -> Option<(String, Vec<u8>)> {
    if buf.len() < 2 {
        return None;
    }
    let len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    if buf.len() < 2 + len {
        return None;
    }
    let topic = String::from_utf8(buf[2..2 + len].to_vec()).ok()?;
    let payload = buf[2 + len..].to_vec();
    Some((topic, payload))
}
