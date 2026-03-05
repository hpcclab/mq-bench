use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use crate::payload::generate_payload;
use crate::transport::{ConnectOptions, Engine, IncomingQuery, TransportBuilder};
use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::{interval, sleep};
use tracing::{debug, info, warn};

pub struct QueryableConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub serve_prefix: Vec<String>,
    pub reply_size: usize,
    pub proc_delay_ms: u64,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
    // Aggregation/external snapshot support
    pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
    pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
    // Test-only convenience: stop automatically after N seconds if provided
    pub test_stop_after_secs: Option<u64>,
}

pub async fn run_queryable(config: QueryableConfig) -> Result<()> {
    info!(
        engine = ?config.engine,
        prefixes = ?config.serve_prefix,
        reply_size = config.reply_size,
        proc_delay_ms = config.proc_delay_ms,
        endpoint = ?config.connect.params.get("endpoint"),
        "Starting queryable"
    );

    // Stats early so we can track connection failures
    let stats = if let Some(s) = &config.shared_stats {
        s.clone()
    } else {
        Arc::new(Stats::new())
    };

    // Transport session with optional retry
    stats.record_connection_attempt();
    let transport = match TransportBuilder::connect_with_retry(
        config.engine.clone(),
        config.connect.clone(),
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "Transport connect error");
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
                    served = snap.sent_count,
                    errors = snap.error_count,
                    "Queryable stats"
                );
            }
        }))
    } else {
        None
    };

    // Prepare a reusable payload buffer to avoid per-reply allocations
    let reply_size = config.reply_size;
    let payload_template = generate_payload(0, reply_size);

    // Register queryables with handler-based API, keep guards alive
    let mut _guards = Vec::new();
    for prefix in &config.serve_prefix {
        let stats_worker = stats.clone();
        let proc_delay = config.proc_delay_ms;
        let payload_template = payload_template.clone();
        let guard = transport
            .register_queryable(
                prefix,
                Box::new(move |IncomingQuery { responder, .. }| {
                    // Minimal handler: spawn to avoid blocking zenoh callback
                    let stats_worker = stats_worker.clone();
                    let payload_template = payload_template.clone();
                    tokio::spawn(async move {
                        if proc_delay > 0 {
                            sleep(Duration::from_millis(proc_delay)).await;
                        }
                        let payload = Bytes::from(payload_template.clone());
                        if let Err(e) = responder.send(payload).await {
                            warn!(error = %e, "Queryable reply error");
                            stats_worker.record_error().await;
                        } else {
                            stats_worker.record_sent().await;
                        }
                    });
                }),
            )
            .await
            .map_err(|e| anyhow::Error::msg(format!("register_queryable error: {}", e)))?;
        _guards.push(guard);
    }

    info!("Queryable(s) registered. Waiting for queries...");

    // Wait for Ctrl+C or optional test timeout
    if let Some(s) = config.test_stop_after_secs {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(s)) => {}
            _ = signal::ctrl_c() => {}
        }
    } else {
        signal::ctrl_c().await?;
        info!("Ctrl+C received, stopping queryable");
    }

    // Final stats
    let final_stats = stats.snapshot().await;
    info!(
        served = final_stats.sent_count,
        errors = final_stats.error_count,
        "Final Queryable Statistics"
    );
    if let Some(ref mut out) = output {
        out.write_snapshot(&final_stats).await?;
    }

    if let Some(h) = snapshot_handle {
        h.abort();
    }
    // Shutdown queryable registrations then transport
    for g in _guards {
        let _ = g.shutdown().await;
    }
    transport
        .shutdown()
        .await
        .map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;
    Ok(())
}
