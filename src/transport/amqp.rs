//! AMQP (RabbitMQ) adapter using lapin. QoS: at-most-once.
use crate::transport::{
    ConnectOptions, IncomingQuery, Payload, Publisher, QueryRegistration, Subscription, Transport,
    TransportError, TransportMessage,
};
use bytes::Bytes;
use futures::StreamExt;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct AmqpTransport {
    url: String,
}

pub async fn connect(opts: ConnectOptions) -> Result<Box<dyn Transport>, TransportError> {
    let url = if let Some(u) = opts.params.get("url").cloned() {
        u
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
            .unwrap_or(5672);
        let user = opts
            .params
            .get("user")
            .cloned()
            .unwrap_or_else(|| "guest".into());
        let pass = opts
            .params
            .get("pass")
            .cloned()
            .unwrap_or_else(|| "guest".into());
        let vhost = opts
            .params
            .get("vhost")
            .cloned()
            .unwrap_or_else(|| "%2f".into());
        format!("amqp://{}:{}@{}:{}/{}", user, pass, host, port, vhost)
    };
    Ok(Box::new(AmqpTransport { url }))
}

#[async_trait::async_trait]
impl Transport for AmqpTransport {
    async fn subscribe(
        &self,
        expr: &str,
        handler: Box<dyn Fn(TransportMessage) + Send + Sync + 'static>,
    ) -> Result<Box<dyn Subscription>, TransportError> {
        // For simplicity, use a topic exchange named "amq.topic" and binding key from expr
        let binding_key = map_routing(expr);
        let conn = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        let queue = channel
            .queue_declare(
                "mq_bench_sub",
                QueueDeclareOptions {
                    durable: false,
                    exclusive: true,
                    auto_delete: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        channel
            .queue_bind(
                queue.name().as_str(),
                "amq.topic",
                &binding_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        let mut consumer = channel
            .basic_consume(
                queue.name().as_str(),
                "mq_bench",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| TransportError::Subscribe(e.to_string()))?;
        let handler = Arc::new(handler);
        let handle: JoinHandle<()> = tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                match delivery {
                    Ok(delivery) => {
                        let payload = Bytes::from(delivery.data.clone());
                        (handler)(TransportMessage {
                            payload: Payload::from_bytes(payload),
                        });
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Box::new(AmqpSubscription {
            handle,
            _conn: conn,
        }))
    }

    async fn create_publisher(&self, topic: &str) -> Result<Box<dyn Publisher>, TransportError> {
        let conn = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        // Do not declare built-in exchange "amq.topic": it's predeclared (durable) by RabbitMQ.
        Ok(Box::new(AmqpPublisher {
            channel,
            routing: map_routing(topic),
            _conn: conn,
        }))
    }

    async fn request(&self, _subject: &str, _payload: Bytes) -> Result<Payload, TransportError> {
        Err(TransportError::Other(
            "AMQP request/reply not implemented".into(),
        ))
    }

    async fn register_queryable(
        &self,
        _subject: &str,
        _handler: Box<dyn Fn(IncomingQuery) + Send + Sync + 'static>,
    ) -> Result<Box<dyn QueryRegistration>, TransportError> {
        Err(TransportError::Other(
            "AMQP queryable not implemented".into(),
        ))
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        Ok(())
    }
    async fn health_check(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

struct AmqpSubscription {
    handle: JoinHandle<()>,
    _conn: Connection,
}

#[async_trait::async_trait]
impl Subscription for AmqpSubscription {
    async fn shutdown(&self) -> Result<(), TransportError> {
        self.handle.abort();
        Ok(())
    }
}

struct AmqpPublisher {
    channel: Channel,
    routing: String,
    _conn: Connection,
}

#[async_trait::async_trait]
impl Publisher for AmqpPublisher {
    async fn publish(&self, payload: Bytes) -> Result<(), TransportError> {
        self.channel
            .basic_publish(
                "amq.topic",
                &self.routing,
                BasicPublishOptions::default(),
                payload.as_ref(),
                BasicProperties::default(),
            )
            .await
            .map_err(|e| TransportError::Publish(e.to_string()))?;
        Ok(())
    }
}

fn map_routing(topic: &str) -> String {
    // Convert our slash topics into AMQP topic routing keys (dots)
    topic.trim_matches('.').replace('/', ".")
}
