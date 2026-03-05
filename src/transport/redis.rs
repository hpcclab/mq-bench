//! Redis adapter (feature `transport-redis`).
//! Pub/Sub via channels (PUBLISH/SUBSCRIBE). Req/Rep via LIST (RPUSH/BLPOP) as a baseline.

use crate::transport::{
    ConnectOptions, IncomingQuery, Payload, Publisher, QueryRegistration, QueryResponder,
    QueryResponderInner, Subscription, Transport, TransportError, TransportMessage,
};
use bytes::Bytes;
use futures::StreamExt;
// no extra imports needed for helpers
use redis::aio::ConnectionManager;
use tokio::sync::{Mutex, mpsc, oneshot};

pub struct RedisTransport {
    client: redis::Client,
    pub_mode: PubMode,
}

#[derive(Clone, Copy, Debug)]
enum PubMode {
    Shared,
    Single,
}

pub async fn connect(opts: ConnectOptions) -> Result<Box<dyn Transport>, TransportError> {
    let url = opts
        .params
        .get("url")
        .cloned()
        .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
    let client =
        redis::Client::open(url.as_str()).map_err(|e| TransportError::Connect(e.to_string()))?;
    let pub_mode = match opts.params.get("pub_mode").map(|s| s.as_str()) {
        Some("single") => PubMode::Single,
        _ => PubMode::Shared,
    };
    Ok(Box::new(RedisTransport { client, pub_mode }))
}

#[async_trait::async_trait]
impl Transport for RedisTransport {
    async fn subscribe(
        &self,
        _expr: &str,
        _handler: Box<dyn Fn(TransportMessage) + Send + Sync + 'static>,
    ) -> Result<Box<dyn Subscription>, TransportError> {
        let expr = _expr.to_string();
        let handler = std::sync::Arc::new(_handler);
        // Async PubSub connection with split sink/stream
        let pubsub = self
            .client
            .get_async_pubsub()
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        let (mut sink, mut stream) = pubsub.split();
        // Patterns:
        // - If expr ends with '/**', map to 'prefix*' and use psubscribe
        // - If expr contains Redis glob chars (*, ?, [), use psubscribe as-is
        // - Else, use exact subscribe
        if let Some(prefix) = expr.strip_suffix("/**") {
            let pattern = format!("{}*", prefix);
            sink.psubscribe(&pattern)
                .await
                .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        } else if expr.contains('*') || expr.contains('?') || expr.contains('[') {
            sink.psubscribe(&expr)
                .await
                .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        } else {
            sink.subscribe(&expr)
                .await
                .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        }
        // Spawn listener task
        let task = tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                if let Ok(payload) = msg.get_payload::<Vec<u8>>() {
                    let bytes = Bytes::from(payload);
                    (handler)(TransportMessage {
                        payload: Payload::from_bytes(bytes),
                    });
                }
            }
        });
        Ok(Box::new(RedisSubscription { handle: task }))
    }

    async fn create_publisher(&self, topic: &str) -> Result<Box<dyn Publisher>, TransportError> {
        match self.pub_mode {
            PubMode::Shared => {
                // Shared connection protected by a lock for multi-threaded callers
                let manager = ConnectionManager::new(self.client.clone())
                    .await
                    .map_err(|e| TransportError::Publish(e.to_string()))?;
                Ok(Box::new(RedisPublisher {
                    conn: Mutex::new(manager),
                    topic: topic.to_string(),
                }))
            }
            PubMode::Single => {
                // Spawn a single-threaded publisher task with a dedicated connection and a channel
                let (tx, rx) = mpsc::channel::<PubCmd>(1024);
                let client = self.client.clone();
                let topic_str = topic.to_string();
                tokio::spawn(async move {
                    publisher_task(client, topic_str, rx).await;
                });
                Ok(Box::new(RedisPublisherSingle { tx }))
            }
        }
    }

    async fn request(&self, _subject: &str, _payload: Bytes) -> Result<Payload, TransportError> {
        // Keys
        let req_key = format!("mq:req:{}", _subject);
        let reply_key = new_reply_key(_subject);

        // Encode message: [u16 reply_len][reply_key bytes][payload]
        let msg = encode_req(&reply_key, _payload.as_ref());

        // Push request using a multiplexed connection
        {
            let mut pub_conn = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| TransportError::Request(e.to_string()))?;
            let _: i64 = redis::cmd("RPUSH")
                .arg(&req_key)
                .arg(msg)
                .query_async(&mut pub_conn)
                .await
                .map_err(|e| TransportError::Request(e.to_string()))?;
        }

        // Wait for reply using a dedicated blocking connection; timeout handled by caller
        let mut blk_conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| TransportError::Request(e.to_string()))?;
        // BLPOP returns (key, value)
        let (_k, data): (String, Vec<u8>) = redis::cmd("BLPOP")
            .arg(&reply_key)
            .arg(0) // block indefinitely; outer timeout will cancel
            .query_async(&mut blk_conn)
            .await
            .map_err(|e| TransportError::Request(e.to_string()))?;
        Ok(Payload::from_bytes(Bytes::from(data)))
    }

    async fn register_queryable(
        &self,
        _subject: &str,
        _handler: Box<dyn Fn(IncomingQuery) + Send + Sync + 'static>,
    ) -> Result<Box<dyn QueryRegistration>, TransportError> {
        let subject = _subject.to_string();
        let handler = std::sync::Arc::new(_handler);
        let client = self.client.clone();
        let req_key = format!("mq:req:{}", subject);
        // Spawn a worker task that blocks on BLPOP and dispatches to the handler
        let handle = tokio::spawn(async move {
            // Dedicated blocking connection
            let conn_res = client.get_multiplexed_async_connection().await;
            if conn_res.is_err() {
                return;
            }
            let mut conn = conn_res.unwrap();
            loop {
                // (key, data)
                let res: Result<(String, Vec<u8>), _> = redis::cmd("BLPOP")
                    .arg(&req_key)
                    .arg(0)
                    .query_async(&mut conn)
                    .await;
                let (_k, raw) = match res {
                    Ok(v) => v,
                    Err(_e) => break,
                };
                if let Some((reply_key, payload)) = decode_req(&raw) {
                    let responder = RedisResponder {
                        client: client.clone(),
                        reply_key,
                    };
                    let incoming = IncomingQuery {
                        subject: subject.clone(),
                        payload: Payload::from_bytes(Bytes::from(payload)),
                        correlation: None,
                        responder: QueryResponder {
                            inner: std::sync::Arc::new(responder),
                        },
                    };
                    (handler)(incoming);
                }
            }
        });
        Ok(Box::new(RedisQueryRegistration { handle }))
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        Ok(())
    }
    async fn health_check(&self) -> Result<(), TransportError> {
        Ok(())
    }
    async fn force_disconnect(&self) -> Result<(), TransportError> {
        // Redis: Transport stores client; actual connections are created per operation.
        // This is a no-op since we can't force-close connections we don't hold.
        // The crash simulation will work by dropping/recreating the transport.
        Ok(())
    }
}

struct RedisPublisher {
    conn: Mutex<ConnectionManager>,
    topic: String,
}

#[async_trait::async_trait]
impl Publisher for RedisPublisher {
    async fn publish(&self, payload: Bytes) -> Result<(), TransportError> {
        let mut conn = self.conn.lock().await;
        let _: i64 = redis::cmd("PUBLISH")
            .arg(&self.topic)
            .arg(payload.as_ref())
            .query_async(&mut *conn)
            .await
            .map_err(|e| TransportError::Publish(e.to_string()))?;
        Ok(())
    }
}

// Lock-free (no Mutex) single-thread publisher via a background task.
struct RedisPublisherSingle {
    tx: mpsc::Sender<PubCmd>,
}

enum PubCmd {
    Msg(Bytes, oneshot::Sender<Result<(), TransportError>>),
    Shutdown,
}

#[async_trait::async_trait]
impl Publisher for RedisPublisherSingle {
    async fn publish(&self, payload: Bytes) -> Result<(), TransportError> {
        let (tx_ack, rx_ack) = oneshot::channel();
        self.tx
            .send(PubCmd::Msg(payload, tx_ack))
            .await
            .map_err(|_| TransportError::Disconnected)?;
        rx_ack
            .await
            .unwrap_or_else(|_| Err(TransportError::Disconnected))
    }
    async fn shutdown(&self) -> Result<(), TransportError> {
        // Best-effort shutdown signal; task will also stop when all senders are dropped.
        let _ = self.tx.send(PubCmd::Shutdown).await;
        Ok(())
    }
}

async fn publisher_task(client: redis::Client, topic: String, mut rx: mpsc::Receiver<PubCmd>) {
    // Establish a dedicated multiplexed connection
    let conn_result = client.get_multiplexed_async_connection().await;
    if let Err(e) = conn_result {
        // Fail all queued messages with an error and exit
        while let Some(cmd) = rx.recv().await {
            if let PubCmd::Msg(_, ack) = cmd {
                let _ = ack.send(Err(TransportError::Publish(e.to_string())));
            }
        }
        return;
    }
    let mut conn = conn_result.unwrap();
    while let Some(cmd) = rx.recv().await {
        match cmd {
            PubCmd::Msg(bytes, ack) => {
                let res = async {
                    let _: i64 = redis::cmd("PUBLISH")
                        .arg(&topic)
                        .arg(bytes.as_ref())
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| TransportError::Publish(e.to_string()))?;
                    Ok(())
                }
                .await;
                let _ = ack.send(res);
            }
            PubCmd::Shutdown => break,
        }
    }
}

struct RedisResponder {
    client: redis::Client,
    reply_key: String,
}

#[async_trait::async_trait]
impl QueryResponderInner for RedisResponder {
    async fn send(&self, _payload: Bytes) -> Result<(), TransportError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| TransportError::Request(e.to_string()))?;
        let _: i64 = redis::cmd("RPUSH")
            .arg(&self.reply_key)
            .arg(_payload.as_ref())
            .query_async(&mut conn)
            .await
            .map_err(|e| TransportError::Request(e.to_string()))?;
        Ok(())
    }
    async fn end(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

struct RedisSubscription {
    handle: tokio::task::JoinHandle<()>,
}

#[async_trait::async_trait]
impl Subscription for RedisSubscription {
    async fn shutdown(&self) -> Result<(), TransportError> {
        self.handle.abort();
        Ok(())
    }
}

struct RedisQueryRegistration {
    handle: tokio::task::JoinHandle<()>,
}

#[async_trait::async_trait]
impl QueryRegistration for RedisQueryRegistration {
    async fn shutdown(&self) -> Result<(), TransportError> {
        self.handle.abort();
        Ok(())
    }
}

// Helpers for simple req encoding
fn new_reply_key(subject: &str) -> String {
    // Unique enough: subject + task id + counter
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let c = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("mq:rep:{}:{}", subject, c)
}

fn encode_req(reply_key: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + reply_key.len() + payload.len());
    let len = reply_key.len() as u16;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(reply_key.as_bytes());
    out.extend_from_slice(payload);
    out
}

fn decode_req(buf: &[u8]) -> Option<(String, Vec<u8>)> {
    if buf.len() < 2 {
        return None;
    }
    let len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    if buf.len() < 2 + len {
        return None;
    }
    let key = String::from_utf8(buf[2..2 + len].to_vec()).ok()?;
    let payload = buf[2 + len..].to_vec();
    Some((key, payload))
}
