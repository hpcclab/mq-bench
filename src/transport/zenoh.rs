//! Zenoh adapter implementing the Transport trait.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;
use zenoh::qos::Reliability;

use super::{
    ConnectOptions, IncomingQuery, Payload, QueryResponder, QueryResponderInner, Transport,
    TransportError, TransportMessage,
};
#[allow(unused_imports)]
use zenoh::bytes::ZBytes;

pub struct ZenohTransport {
    session: zenoh::Session,
    pub_reliability: Reliability,
}

pub async fn connect(opts: ConnectOptions) -> Result<Box<dyn Transport>, TransportError> {
    let mut cfg = zenoh::config::Config::default();
    // Mode configurable via connect param: mode=client|peer (default: client)
    let mode = opts
        .params
        .get("mode")
        .cloned()
        .unwrap_or_else(|| "client".to_string());
    cfg.insert_json5("mode", &format!("\"{}\"", mode))
        .map_err(|e| TransportError::Connect(e.to_string()))?;
    if let Some(eps) = opts
        .params
        .get("endpoint")
        .cloned()
        .or_else(|| opts.params.get("endpoints").cloned())
    {
        let list = if eps.starts_with('[') {
            eps
        } else {
            format!("[\"{}\"]", eps)
        };
        cfg.insert_json5("connect/endpoints", &list)
            .map_err(|e| TransportError::Connect(e.to_string()))?;
    }
    

    let session = zenoh::open(cfg)
        .await
        .map_err(|e| TransportError::Connect(e.to_string()))?;
    // Map ConnectOptions qos -> Reliability (0: BestEffort, 1/2: Reliable)
    let qos_level: u8 = opts
        .params
        .get("qos")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let pub_reliability = if qos_level == 0 {
        Reliability::BestEffort
    } else {
        Reliability::Reliable
    };
    Ok(Box::new(ZenohTransport { session, pub_reliability }))
}

#[async_trait::async_trait]
impl Transport for ZenohTransport {
    async fn subscribe(
        &self,
        expr: &str,
        handler: Box<dyn Fn(TransportMessage) + Send + Sync + 'static>,
    ) -> Result<Box<dyn super::Subscription>, TransportError> {
        // Use Zenoh's callback directly and forward to handler without extra channel when possible.
        let handler = Arc::new(handler);
        let sub = self
            .session
            .declare_subscriber(expr)
            .callback(move |sample| {
                // Minimal work in callback
                (handler)(TransportMessage {
                    payload: Payload::from_zenoh(sample.payload().clone()),
                });
            })
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        // Box the concrete subscriber as Any to avoid depending on its concrete type
        let boxed = Box::new(sub);
        Ok(Box::new(ZenohSubscription {
            inner: Mutex::new(Some(boxed)),
        }))
    }

    async fn create_publisher(
        &self,
        topic: &str,
    ) -> Result<Box<dyn super::Publisher>, TransportError> {
        let pub_decl = self
            .session
            .declare_publisher(topic.to_string())
            .reliability(self.pub_reliability)
            .await
            .map_err(|e| TransportError::Publish(e.to_string()))?;
        Ok(Box::new(ZenohPublisher { inner: pub_decl }))
    }

    async fn request(&self, subject: &str, _payload: Bytes) -> Result<Payload, TransportError> {
        // Minimal implementation: wait for first reply, ignore payload content for now.
        let replies = self
            .session
            .get(subject)
            .with(flume::bounded(1))
            .await
            .map_err(|e| TransportError::Request(e.to_string()))?;
        let reply = replies
            .recv_async()
            .await
            .map_err(|e| TransportError::Request(e.to_string()))?;
        match reply.result() {
            Ok(sample) => Ok(Payload::from_zenoh(sample.payload().clone())),
            Err(e) => Err(TransportError::Request(e.to_string())),
        }
    }

    async fn register_queryable(
        &self,
        subject: &str,
        handler: Box<dyn Fn(IncomingQuery) + Send + Sync + 'static>,
    ) -> Result<Box<dyn super::QueryRegistration>, TransportError> {
        let handler = Arc::new(handler);
        let h2 = handler.clone();
        let q = self
            .session
            .declare_queryable(subject)
            .callback(move |query| {
                let subject = query.key_expr().to_string();
                let payload = query.payload().cloned().unwrap_or_else(|| ZBytes::new());
                let responder = ZenohResponder::new(query);
                let incoming = IncomingQuery {
                    subject,
                    payload: Payload::from_zenoh(payload),
                    correlation: None,
                    responder: QueryResponder {
                        inner: Arc::new(responder),
                    },
                };
                (h2)(incoming);
            })
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        let boxed: Box<dyn std::any::Any + Send> = Box::new(q);
        Ok(Box::new(ZenohQueryRegistration {
            inner: Mutex::new(Some(boxed)),
        }))
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.session
            .close()
            .await
            .map_err(|e| TransportError::Other(e.to_string()))
    }

    async fn health_check(&self) -> Result<(), TransportError> {
        Ok(())
    }

    async fn force_disconnect(&self) -> Result<(), TransportError> {
        // Zenoh: Close the session to force disconnect.
        // This will terminate all publishers and subscribers.
        self.session
            .close()
            .await
            .map_err(|e| TransportError::Other(e.to_string()))
    }
}

struct ZenohResponder {
    query: zenoh::query::Query,
}

impl ZenohResponder {
    fn new(query: zenoh::query::Query) -> Self {
        Self { query }
    }
}

#[async_trait::async_trait]
impl QueryResponderInner for ZenohResponder {
    async fn send(&self, payload: Bytes) -> Result<(), TransportError> {
        self.query
            .reply(self.query.key_expr(), payload)
            .await
            .map_err(|e| TransportError::Other(e.to_string()))
    }
    async fn end(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

struct ZenohSubscription {
    inner: Mutex<Option<Box<dyn std::any::Any + Send>>>,
}

#[async_trait::async_trait]
impl super::Subscription for ZenohSubscription {
    async fn shutdown(&self) -> Result<(), TransportError> {
        // Drop the inner subscriber to stop callbacks
        let _ = self.inner.lock().await.take();
        Ok(())
    }
}

struct ZenohQueryRegistration {
    inner: Mutex<Option<Box<dyn std::any::Any + Send>>>,
}

#[async_trait::async_trait]
impl super::QueryRegistration for ZenohQueryRegistration {
    async fn shutdown(&self) -> Result<(), TransportError> {
        let _ = self.inner.lock().await.take();
        Ok(())
    }
}

struct ZenohPublisher {
    inner: zenoh::pubsub::Publisher<'static>,
}

#[async_trait::async_trait]
impl super::Publisher for ZenohPublisher {
    async fn publish(&self, payload: Bytes) -> Result<(), TransportError> {
        self.inner
            .put(payload)
            .await
            .map_err(|e| TransportError::Publish(e.to_string()))
    }
    async fn shutdown(&self) -> Result<(), TransportError> {
        Ok(())
    }
}
