use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use crate::rate::RateController;
use crate::transport::{ConnectOptions, Engine, TransportBuilder};
use anyhow::Result;
use bytes::Bytes;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
// use tokio::signal;
use flume;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

pub struct RequesterConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub key_expr: String,
    pub qps: Option<u32>,
    pub concurrency: u32,
    pub timeout_ms: u64,
    pub duration_secs: u64,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
    // Aggregation/external snapshot support
    pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
    pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
}

pub async fn run_requester(config: RequesterConfig) -> Result<()> {
    info!(
        engine = ?config.engine,
        key_expr = %config.key_expr,
        qps = ?config.qps,
        concurrency = config.concurrency,
        timeout_ms = config.timeout_ms,
        duration_secs = config.duration_secs,
        endpoint = ?config.connect.params.get("endpoint"),
        "Starting requester"
    );

    // Stats early so we can track connection failures
    let stats = if let Some(s) = &config.shared_stats {
        s.clone()
    } else {
        Arc::new(Stats::new())
    };

    // Transport session with optional retry
    stats.record_connection_attempt();
    let transport: Arc<Box<dyn crate::transport::Transport>> = match TransportBuilder::connect_with_retry(
        config.engine.clone(),
        config.connect.clone(),
    )
    .await
    {
        Ok(t) => Arc::from(t),
        Err(e) => {
            error!(error = %e, "Transport connect error");
            stats.record_connection_failure();
            return Ok(());
        }
    };

    let mut output = if let Some(ref path) = config.output_file {
        Some(OutputWriter::new_csv(path.clone()).await?)
    } else if config.shared_stats.is_none() {
        Some(OutputWriter::new_stdout())
    } else {
        None
    };

    // Stats worker channel: batch updates to avoid contention from many tasks
    #[derive(Clone, Copy)]
    enum Ev {
        Sent,
        Recv(u64),
        Err,
    }
    let (tx, rx) = flume::bounded::<Ev>(10_000);
    {
        let stats_worker = stats.clone();
        tokio::spawn(async move {
            while let Ok(ev) = rx.recv_async().await {
                match ev {
                    Ev::Sent => stats_worker.record_sent().await,
                    Ev::Recv(ns) => stats_worker.record_received(ns).await,
                    Ev::Err => stats_worker.record_error().await,
                }
            }
        });
    }

    // Snapshot task (optional)
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = Arc::clone(&stats);
        let interval_secs = config.snapshot_interval_secs;
        Some(tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(interval_secs));
            loop {
                interval_timer.tick().await;
                let snap = stats_clone.snapshot().await;
                debug!(
                    sent = snap.sent_count,
                    received = snap.received_count,
                    errors = snap.error_count,
                    "Requester stats"
                );
            }
        }))
    } else {
        None
    };

    // Query loop
    let start = Instant::now();
    let mut rate = config.qps.map(|q| RateController::new(q as f64));
    let mut inflight: FuturesUnordered<
        Pin<Box<dyn Future<Output = Result<Option<Duration>, ()>> + Send>>,
    > = FuturesUnordered::new();
    let mut total_sent = 0u64;
    let mut total_recv = 0u64;

    loop {
        // Check duration
        if start.elapsed().as_secs() >= config.duration_secs {
            info!("Duration limit reached, stopping requester");
            break;
        }

        // Maintain concurrency
        while inflight.len() < config.concurrency as usize {
            if let Some(rc) = &mut rate {
                rc.wait_for_next().await;
            }
            let key_expr = config.key_expr.clone();
            let tx_ev = tx.clone();
            let timeout_ms = config.timeout_ms;
            let transport = Arc::clone(&transport);
            let fut: Pin<Box<dyn Future<Output = Result<Option<Duration>, ()>> + Send>> =
                Box::pin(async move {
                    let t0 = Instant::now();
                    // First-reply request with timeout around the transport call
                    let fut = transport.request(&key_expr, Bytes::new());
                    match tokio::time::timeout(Duration::from_millis(timeout_ms), fut).await {
                        Ok(Ok(_payload)) => {
                            let now = t0.elapsed();
                            let _ = tx_ev.try_send(Ev::Sent);
                            let _ = tx_ev.try_send(Ev::Recv(now.as_nanos() as u64));
                            Ok(Some(now))
                        }
                        Ok(Err(e)) => {
                            warn!(error = %e, "Requester query error");
                            let _ = tx_ev.try_send(Ev::Err);
                            Err(())
                        }
                        Err(_to) => {
                            warn!("Requester timeout");
                            let _ = tx_ev.try_send(Ev::Err);
                            Err(())
                        }
                    }
                });
            inflight.push(fut);
            total_sent += 1;
        }

        // Poll for finished queries
        if let Some(_res) = inflight.next().await {
            total_recv += 1;
        }
    }

    // Drain remaining inflight
    while let Some(_res) = inflight.next().await {
        total_recv += 1;
    }

    // Final stats
    let final_stats = stats.snapshot().await;
    info!(
        queries_sent = total_sent,
        queries_completed = total_recv,
        errors = final_stats.error_count,
        "Final Requester Statistics"
    );
    if let Some(ref mut out) = output {
        out.write_snapshot(&final_stats).await?;
    }

    if let Some(h) = snapshot_handle {
        h.abort();
    }
    // Close transport
    transport
        .shutdown()
        .await
        .map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;
    Ok(())
}
