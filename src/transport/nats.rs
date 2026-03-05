//! NATS adapter (feature `transport-nats`) using async-nats. QoS is at-most-once.
use crate::transport::{
    ConnectOptions, IncomingQuery, Payload, Publisher, QueryRegistration, QueryResponder,
    QueryResponderInner, Subscription, Transport, TransportError, TransportMessage,
};
use bytes::Bytes;
use futures::StreamExt;
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct NatsTransport {
    url: String,
}

pub async fn connect(opts: ConnectOptions) -> Result<Box<dyn Transport>, TransportError> {
    let url = if let Some(u) = opts.params.get("url").cloned() {
        u
    } else if let Some(ep) = opts.params.get("endpoint").cloned() {
        ep
    } else {
        let host = opts
            .params
            .get("host")
            .cloned()
            .unwrap_or_else(|| "127.0.0.1".into());
        let port: u16 = opts
            .params
            .get("port")
            .and_then(|s| s.parse().ok())
            .unwrap_or(4222);
        format!("nats://{}:{}", host, port)
    };
    Ok(Box::new(NatsTransport { url }))
}

#[async_trait::async_trait]
impl Transport for NatsTransport {
    async fn subscribe(
        &self,
        expr: &str,
        handler: Box<dyn Fn(TransportMessage) + Send + Sync + 'static>,
    ) -> Result<Box<dyn Subscription>, TransportError> {
        let subject = map_expr(expr);
        let client = async_nats::connect(&self.url)
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        let mut sub = client
            .subscribe(subject)
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        let handler = std::sync::Arc::new(handler);
        let handle: JoinHandle<()> = tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                (handler)(TransportMessage {
                    payload: Payload::from_bytes(Bytes::from(msg.payload.to_vec())),
                });
            }
        });
        Ok(Box::new(NatsSubscription {
            handle,
            _client: client,
        }))
    }

    async fn create_publisher(&self, topic: &str) -> Result<Box<dyn Publisher>, TransportError> {
        let client = async_nats::connect(&self.url)
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        Ok(Box::new(NatsPublisher {
            client,
            subject: map_topic(topic),
        }))
    }

    async fn request(&self, subject: &str, payload: Bytes) -> Result<Payload, TransportError> {
        let client = async_nats::connect(&self.url)
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        let timeout = Duration::from_secs(5);
        let fut = client.request(map_topic(subject), payload.to_vec().into());
        let resp = match tokio::time::timeout(timeout, fut).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(TransportError::Request(e.to_string())),
            Err(_) => return Err(TransportError::Timeout),
        };
        Ok(Payload::from_bytes(Bytes::from(resp.payload.to_vec())))
    }

    async fn register_queryable(
        &self,
        subject: &str,
        handler: Box<dyn Fn(IncomingQuery) + Send + Sync + 'static>,
    ) -> Result<Box<dyn QueryRegistration>, TransportError> {
        let client = async_nats::connect(&self.url)
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        let subject = map_topic(subject);
        let handler = std::sync::Arc::new(handler);
        let mut sub = client
            .subscribe(subject.clone())
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        let handle = tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                let reply_to = msg.reply.clone();
                let responder = NatsResponder {
                    client: client.clone(),
                    reply: reply_to,
                };
                let incoming = IncomingQuery {
                    subject: subject.clone(),
                    payload: Payload::from_bytes(Bytes::from(msg.payload.to_vec())),
                    correlation: None,
                    responder: QueryResponder {
                        inner: std::sync::Arc::new(responder),
                    },
                };
                (handler)(incoming);
            }
        });
        Ok(Box::new(NatsQueryRegistration { handle }))
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        Ok(())
    }
    async fn health_check(&self) -> Result<(), TransportError> {
        Ok(())
    }
    async fn force_disconnect(&self) -> Result<(), TransportError> {
        // NATS: Transport only stores URL; actual connections are per-operation.
        // This is a no-op since we can't force-close connections we don't hold.
        // The crash simulation will work by dropping/recreating the transport.
        Ok(())
    }
}

struct NatsPublisher {
    client: async_nats::Client,
    subject: String,
}

#[async_trait::async_trait]
impl Publisher for NatsPublisher {
    async fn publish(&self, payload: Bytes) -> Result<(), TransportError> {
        self.client
            .publish(self.subject.clone(), payload.to_vec().into())
            .await
            .map_err(|e| TransportError::Publish(e.to_string()))?;
        Ok(())
    }
}

struct NatsSubscription {
    handle: JoinHandle<()>,
    // keep client alive to maintain subscription
    _client: async_nats::Client,
}

#[async_trait::async_trait]
impl Subscription for NatsSubscription {
    async fn shutdown(&self) -> Result<(), TransportError> {
        self.handle.abort();
        Ok(())
    }
}

struct NatsQueryRegistration {
    handle: JoinHandle<()>,
}

#[async_trait::async_trait]
impl QueryRegistration for NatsQueryRegistration {
    async fn shutdown(&self) -> Result<(), TransportError> {
        self.handle.abort();
        Ok(())
    }
}

struct NatsResponder {
    client: async_nats::Client,
    reply: Option<async_nats::Subject>,
}

#[async_trait::async_trait]
impl QueryResponderInner for NatsResponder {
    async fn send(&self, payload: Bytes) -> Result<(), TransportError> {
        if let Some(ref r) = self.reply {
            self.client
                .publish(r.clone(), payload.to_vec().into())
                .await
                .map_err(|e| TransportError::Publish(e.to_string()))?;
        }
        Ok(())
    }
    async fn end(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

fn map_expr(expr: &str) -> String {
    if let Some(prefix) = expr.strip_suffix("/**") {
        let base = map_topic(prefix);
        if base.ends_with('.') {
            format!("{}>", base)
        } else {
            format!("{}.>", base)
        }
    } else {
        map_topic(expr)
    }
}

fn map_topic(topic: &str) -> String {
    // Convert slash-separated keys to dot-separated NATS subjects.
    // e.g., "bench/topic/1" -> "bench.topic.1"
    topic.trim_matches('.').replace('/', ".")
}
