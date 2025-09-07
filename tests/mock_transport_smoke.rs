#![cfg(feature = "transport-mock")]
use mq_bench::transport::{Engine, ConnectOptions, TransportBuilder};

#[tokio::test]
async fn pub_sub_mock_smoke() {
    let opts = ConnectOptions::default();
    let t = TransportBuilder::connect(Engine::Mock, opts.clone()).await.expect("connect");
    let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let r2 = received.clone();
    let _sub = t.subscribe("k1", Box::new(move |_m| {
        r2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    })).await.expect("subscribe");
    let pubr = t.create_publisher("k1").await.expect("pub");
    pubr.publish(bytes::Bytes::from_static(b"hello")).await.expect("send");
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert_eq!(received.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn req_qry_mock_smoke() {
    let opts = ConnectOptions::default();
    let t = TransportBuilder::connect(Engine::Mock, opts.clone()).await.expect("connect");
    let _q = t.register_queryable("q1", Box::new(move |inq| {
        let responder = inq.responder;
        tokio::spawn(async move {
            let _ = responder.send(bytes::Bytes::from_static(b"ok")) .await;
        });
    })).await.expect("queryable");

    let _payload = t.request("q1", bytes::Bytes::new()).await.expect("request");
}
