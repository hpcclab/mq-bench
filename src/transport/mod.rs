//! Transport abstraction: trait, types, and builder factory.

pub mod config;
#[cfg(any(test, feature = "transport-mock"))]
pub mod mock;
#[cfg(feature = "transport-mqtt")]
pub mod mqtt;
#[cfg(feature = "transport-redis")]
pub mod redis;
#[cfg(feature = "transport-nats")]
pub mod nats;
#[cfg(feature = "transport-zenoh")]
pub mod zenoh;
#[cfg(feature = "transport-amqp-0-9")]
pub mod amqp;

use std::collections::BTreeMap;
use std::pin::Pin;

#[cfg(feature = "transport-zenoh")]
use ::zenoh::bytes::ZBytes;
use bytes::Bytes;
use futures::Stream;
use std::borrow::Cow;

#[derive(Clone, Debug)]
pub enum Engine {
    Zenoh,
    Tcp,
    Redis,
    Mqtt,
    Nats,
    Amqp,
    #[cfg(any(test, feature = "transport-mock"))]
    Mock,
}

#[derive(Clone, Debug, Default)]
pub struct ConnectOptions {
    pub params: BTreeMap<String, String>,
}

#[derive(thiserror::Error, Debug)]
pub enum TransportError {
    #[error("connect: {0}")]
    Connect(String),
    #[error("publish: {0}")]
    Publish(String),
    #[error("subscribe: {0}")]
    Subscribe(String),
    #[error("request: {0}")]
    Request(String),
    #[error("timeout")]
    Timeout,
    #[error("disconnected")]
    Disconnected,
    #[error("other: {0}")]
    Other(String),
}

impl TransportError {
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Self::Timeout | Self::Disconnected)
    }
}

pub type TransportStream =
    Pin<Box<dyn Stream<Item = Result<TransportMessage, TransportError>> + Send>>; // legacy

#[derive(Clone, Debug)]
pub struct TransportMessage {
    pub payload: Payload,
}

pub type QueryStream = Pin<Box<dyn Stream<Item = Result<IncomingQuery, TransportError>> + Send>>;

#[derive(Debug)]
pub struct IncomingQuery {
    pub subject: String,
    pub payload: Payload,
    pub correlation: Option<String>,
    pub responder: QueryResponder,
}

#[derive(Clone, Debug)]
pub enum Payload {
    Bytes(Bytes),
    #[cfg(feature = "transport-zenoh")]
    Zenoh(ZBytes),
}

impl Payload {
    pub fn from_bytes(b: Bytes) -> Self {
        Payload::Bytes(b)
    }
    #[cfg(feature = "transport-zenoh")]
    pub fn from_zenoh(z: ZBytes) -> Self {
        Payload::Zenoh(z)
    }
    pub fn as_cow(&self) -> Cow<'_, [u8]> {
        match self {
            Payload::Bytes(b) => Cow::Borrowed(b.as_ref()),
            #[cfg(feature = "transport-zenoh")]
            Payload::Zenoh(z) => z.to_bytes(),
        }
    }
    pub fn into_bytes(self) -> Bytes {
        match self {
            Payload::Bytes(b) => b,
            #[cfg(feature = "transport-zenoh")]
            Payload::Zenoh(z) => match z.to_bytes() {
                Cow::Borrowed(s) => Bytes::copy_from_slice(s),
                Cow::Owned(v) => Bytes::from(v),
            },
        }
    }
}

pub struct QueryResponder {
    inner: std::sync::Arc<dyn QueryResponderInner + Send + Sync>,
}

impl std::fmt::Debug for QueryResponder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResponder").finish()
    }
}

#[async_trait::async_trait]
pub trait QueryResponderInner: Send + Sync {
    async fn send(&self, payload: Bytes) -> Result<(), TransportError>;
    async fn end(&self) -> Result<(), TransportError>;
}

impl QueryResponder {
    pub async fn send(&self, payload: Bytes) -> Result<(), TransportError> {
        self.inner.send(payload).await
    }
    pub async fn end(&self) -> Result<(), TransportError> {
        self.inner.end().await
    }
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    // async fn publish(&self, topic: &str, payload: Bytes) -> Result<(), TransportError>;
    // Handler-based subscribe for better perf and adapter flexibility.
    // Returns a subscription handle which must be kept alive; dropping or shutdown stops delivery.
    async fn subscribe(
        &self,
        expr: &str,
        handler: Box<dyn Fn(TransportMessage) + Send + Sync + 'static>,
    ) -> Result<Box<dyn Subscription>, TransportError>;
    // Pre-declare publisher for high-throughput publish on the same topic.
    async fn create_publisher(&self, topic: &str) -> Result<Box<dyn Publisher>, TransportError>;
    async fn request(&self, subject: &str, payload: Bytes) -> Result<Payload, TransportError>;
    // Handler-based queryable registration. Returns a guard handle to keep it alive.
    async fn register_queryable(
        &self,
        subject: &str,
        handler: Box<dyn Fn(IncomingQuery) + Send + Sync + 'static>,
    ) -> Result<Box<dyn QueryRegistration>, TransportError>;
    async fn shutdown(&self) -> Result<(), TransportError>;
    async fn health_check(&self) -> Result<(), TransportError>;
}

#[async_trait::async_trait]
pub trait Subscription: Send + Sync {
    async fn shutdown(&self) -> Result<(), TransportError>;
}

#[async_trait::async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(&self, payload: Bytes) -> Result<(), TransportError>;
    async fn shutdown(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait QueryRegistration: Send + Sync {
    async fn shutdown(&self) -> Result<(), TransportError>;
}

pub struct TransportBuilder;

impl TransportBuilder {
    pub async fn connect(
        engine: Engine,
        opts: ConnectOptions,
    ) -> Result<Box<dyn Transport>, TransportError> {
        match engine {
            Engine::Zenoh => {
                #[cfg(feature = "transport-zenoh")]
                {
                    return crate::transport::zenoh::connect(opts).await;
                }
                #[cfg(not(feature = "transport-zenoh"))]
                {
                    Err(TransportError::Connect("zenoh feature disabled".into()))
                }
            }
            Engine::Redis => {
                #[cfg(feature = "transport-redis")]
                {
                    return crate::transport::redis::connect(opts).await;
                }
                #[cfg(not(feature = "transport-redis"))]
                {
                    Err(TransportError::Connect("redis feature disabled".into()))
                }
            }
            Engine::Mqtt => {
                #[cfg(feature = "transport-mqtt")]
                {
                    return crate::transport::mqtt::connect(opts).await;
                }
                #[cfg(not(feature = "transport-mqtt"))]
                {
                    Err(TransportError::Connect("mqtt feature disabled".into()))
                }
            }
            Engine::Nats => {
                #[cfg(feature = "transport-nats")]
                {
                    return crate::transport::nats::connect(opts).await;
                }
                #[cfg(not(feature = "transport-nats"))]
                {
                    Err(TransportError::Connect("nats feature disabled".into()))
                }
            }
            Engine::Amqp => {
                #[cfg(feature = "transport-amqp-0-9")]
                {
                    return crate::transport::amqp::connect(opts).await;
                }
                #[cfg(not(feature = "transport-amqp-0-9"))]
                {
                    Err(TransportError::Connect("amqp feature disabled".into()))
                }
            }
            #[cfg(any(test, feature = "transport-mock"))]
            Engine::Mock => {
                return crate::transport::mock::connect(opts).await;
            }
            _ => Err(TransportError::Connect("engine not yet implemented".into())),
        }
    }
}
