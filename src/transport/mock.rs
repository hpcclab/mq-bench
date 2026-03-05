//! Mock transport for unit tests: in-process pub/sub and req/rep.

use super::{
    ConnectOptions, IncomingQuery, Payload, Publisher, QueryRegistration, QueryResponder,
    QueryResponderInner, Subscription, Transport, TransportError, TransportMessage,
};
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Default)]
struct Bus {
    subs: HashMap<String, Vec<Arc<dyn Fn(TransportMessage) + Send + Sync + 'static>>>,
    qrys: HashMap<String, Arc<dyn Fn(IncomingQuery) + Send + Sync + 'static>>,
}

#[derive(Clone, Default)]
struct SharedBus(Arc<Mutex<Bus>>);

pub struct MockTransport {
    bus: SharedBus,
}

static BUS: OnceLock<SharedBus> = OnceLock::new();
fn shared_bus() -> SharedBus {
    BUS.get_or_init(|| SharedBus::default()).clone()
}

pub async fn connect(_opts: ConnectOptions) -> Result<Box<dyn Transport>, TransportError> {
    Ok(Box::new(MockTransport { bus: shared_bus() }))
}

#[async_trait::async_trait]
impl Transport for MockTransport {
    async fn subscribe(
        &self,
        expr: &str,
        handler: Box<dyn Fn(TransportMessage) + Send + Sync + 'static>,
    ) -> Result<Box<dyn Subscription>, TransportError> {
        let mut bus = self.bus.0.lock().unwrap();
        bus.subs
            .entry(expr.to_string())
            .or_default()
            .push(Arc::from(handler));
        Ok(Box::new(MockSub {
            bus: self.bus.clone(),
            expr: expr.to_string(),
        }))
    }

    async fn create_publisher(&self, topic: &str) -> Result<Box<dyn Publisher>, TransportError> {
        Ok(Box::new(MockPub {
            bus: self.bus.clone(),
            topic: topic.to_string(),
        }))
    }

    async fn request(&self, subject: &str, _payload: Bytes) -> Result<Payload, TransportError> {
        let q = {
            let bus = self.bus.0.lock().unwrap();
            bus.qrys.get(subject).cloned()
        };
        if let Some(cb) = q {
            let (tx, rx) = flume::bounded(1);
            let responder = MockResponder {
                tx: tx.clone(),
                _subject: subject.to_string(),
            };
            let incoming = IncomingQuery {
                subject: subject.to_string(),
                payload: Payload::from_bytes(Bytes::new()),
                correlation: None,
                responder: QueryResponder {
                    inner: Arc::new(responder),
                },
            };
            cb(incoming);
            let bytes = rx
                .recv_async()
                .await
                .map_err(|e| TransportError::Request(e.to_string()))?;
            Ok(Payload::from_bytes(bytes))
        } else {
            Err(TransportError::Request("no queryable".into()))
        }
    }

    async fn register_queryable(
        &self,
        subject: &str,
        handler: Box<dyn Fn(IncomingQuery) + Send + Sync + 'static>,
    ) -> Result<Box<dyn QueryRegistration>, TransportError> {
        let mut bus = self.bus.0.lock().unwrap();
        bus.qrys.insert(subject.to_string(), Arc::from(handler));
        Ok(Box::new(MockQry {
            bus: self.bus.clone(),
            subject: subject.to_string(),
        }))
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        Ok(())
    }
    async fn health_check(&self) -> Result<(), TransportError> {
        Ok(())
    }
    async fn force_disconnect(&self) -> Result<(), TransportError> {
        // Mock transport: clear all subscriptions and queryables to simulate disconnect
        let mut bus = self.bus.0.lock().unwrap();
        bus.subs.clear();
        bus.qrys.clear();
        Ok(())
    }
}

struct MockSub {
    bus: SharedBus,
    expr: String,
}
#[async_trait::async_trait]
impl Subscription for MockSub {
    async fn shutdown(&self) -> Result<(), TransportError> {
        let mut bus = self.bus.0.lock().unwrap();
        if let Some(vec) = bus.subs.get_mut(&self.expr) {
            vec.clear();
        }
        Ok(())
    }
}

struct MockPub {
    bus: SharedBus,
    topic: String,
}
#[async_trait::async_trait]
impl Publisher for MockPub {
    async fn publish(&self, payload: Bytes) -> Result<(), TransportError> {
        let handlers = {
            let bus = self.bus.0.lock().unwrap();
            bus.subs.get(&self.topic).cloned()
        };
        if let Some(hs) = handlers {
            for h in hs {
                h(TransportMessage {
                    payload: Payload::from_bytes(payload.clone()),
                });
            }
        }
        Ok(())
    }
}

struct MockQry {
    bus: SharedBus,
    subject: String,
}
#[async_trait::async_trait]
impl QueryRegistration for MockQry {
    async fn shutdown(&self) -> Result<(), TransportError> {
        let mut bus = self.bus.0.lock().unwrap();
        bus.qrys.remove(&self.subject);
        Ok(())
    }
}

struct MockResponder {
    tx: flume::Sender<Bytes>,
    _subject: String,
}
#[async_trait::async_trait]
impl QueryResponderInner for MockResponder {
    async fn send(&self, payload: Bytes) -> Result<(), TransportError> {
        let _ = self.tx.send_async(payload).await;
        Ok(())
    }
    async fn end(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pub_sub_mock_smoke() {
        let t = super::connect(ConnectOptions::default())
            .await
            .expect("connect");
        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let r2 = received.clone();
        let _sub = t
            .subscribe(
                "k1",
                Box::new(move |_m| {
                    r2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }),
            )
            .await
            .expect("subscribe");
        let pubr = t.create_publisher("k1").await.expect("pub");
        pubr.publish(bytes::Bytes::from_static(b"hello"))
            .await
            .expect("send");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(received.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn req_qry_mock_smoke() {
        let t = super::connect(ConnectOptions::default())
            .await
            .expect("connect");
        let _q = t
            .register_queryable(
                "q1",
                Box::new(move |inq| {
                    let responder = inq.responder;
                    tokio::spawn(async move {
                        let _ = responder.send(bytes::Bytes::from_static(b"ok")).await;
                    });
                }),
            )
            .await
            .expect("queryable");

        let _payload = t.request("q1", bytes::Bytes::new()).await.expect("request");
    }
}
