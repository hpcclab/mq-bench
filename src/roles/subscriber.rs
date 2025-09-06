use crate::payload::parse_header;
use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::signal;
use tokio::time::interval;
use flume;

pub struct SubscriberConfig {
    pub endpoint: String,
    pub key_expr: String,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
    // Aggregation/external snapshot support
    pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
    pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
}

pub async fn run_subscriber(config: SubscriberConfig) -> Result<()> {
    println!("Starting subscriber:");
    println!("  Endpoint: {}", config.endpoint);
    println!("  Key: {}", config.key_expr);
    
    // Initialize Zenoh session with connection endpoint
    let mut zenoh_config = zenoh::config::Config::default();
    zenoh_config.insert_json5("mode", "\"client\"").map_err(|e| anyhow::Error::msg(e.to_string()))?;
    zenoh_config.insert_json5("connect/endpoints", &format!("[\"{}\"]", config.endpoint)).map_err(|e| anyhow::Error::msg(e.to_string()))?;
    zenoh_config.insert_json5("scouting/multicast/enabled", "false").map_err(|e| anyhow::Error::msg(e.to_string()))?;
    zenoh_config.insert_json5("scouting/gossip/enabled", "false").map_err(|e| anyhow::Error::msg(e.to_string()))?;
    
    let session = zenoh::open(zenoh_config).await.map_err(|e| anyhow::Error::msg(e.to_string()))?;
    
    println!("Connected to Zenoh");
    
    // Initialize statistics (use shared if provided)
    let stats = if let Some(s) = &config.shared_stats { s.clone() } else { Arc::new(Stats::new()) };
    
    // Setup output writer (only when not aggregated/external)
    let mut output = if let Some(ref path) = config.output_file {
        Some(OutputWriter::new_csv(path.clone()).await?)
    } else if config.shared_stats.is_none() {
        Some(OutputWriter::new_stdout())
    } else {
        None
    };
    
    // Start snapshot task (only if not disabled)
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = Arc::clone(&stats);
        let interval_secs = config.snapshot_interval_secs;
        let mut out = output.take();
        Some(tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(interval_secs));
            loop {
                interval_timer.tick().await;
                let snapshot = stats_clone.snapshot().await;
                if let Some(ref mut o) = out {
                    let _ = o.write_snapshot(&snapshot).await;
                } else {
                    println!(
                        "Subscriber stats - Received: {}, Errors: {}, Rate: {:.2} msg/s, P99 Latency: {:.2}ms",
                        snapshot.received_count,
                        snapshot.error_count,
                        snapshot.interval_throughput(),
                        snapshot.latency_ns_p99 as f64 / 1_000_000.0
                    );
                }
            }
        }))
    } else { None };
    
    // Channel + worker to avoid per-message task spawn overhead
    let (tx, rx) = flume::bounded::<u64>(10_000);
    let stats_worker = stats.clone();
    tokio::spawn(async move {
        while let Ok(lat) = rx.recv_async().await {
            // Update stats in a single task to reduce contention
            stats_worker.record_received(lat).await;
        }
    });

    // Create subscriber with callback, push latencies into channel
    let tx_cb = tx.clone();
    let _subscriber = session
        .declare_subscriber(&config.key_expr)
        .callback(move |sample| {
            let tx = tx_cb.clone();
            // Compute latency synchronously and try to enqueue
            let receive_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
        match parse_header(&sample.payload().to_bytes()) {
                Ok(header) => {
                    let latency_ns = receive_time.saturating_sub(header.timestamp_ns);
            let _ = tx.try_send(latency_ns); // drop on backpressure to avoid blocking
                }
                Err(e) => {
                    eprintln!("Failed to parse message header: {}", e);
                    // We avoid recording error here to keep callback lightweight
                }
            }
        })
        .await
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    
    println!("Subscribed to key expression: {}", config.key_expr);
    
    // Wait for Ctrl+C
    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("Ctrl+C received, stopping subscriber");
        }
    }
    
    // Final statistics
    let final_stats = stats.snapshot().await;
    println!("\nFinal Subscriber Statistics:");
    println!("  Messages received: {}", final_stats.received_count);
    println!("  Errors: {}", final_stats.error_count);
    println!("  Average rate: {:.2} msg/s", final_stats.total_throughput());
    println!("  Latency P50: {:.2}ms", final_stats.latency_ns_p50 as f64 / 1_000_000.0);
    println!("  Latency P95: {:.2}ms", final_stats.latency_ns_p95 as f64 / 1_000_000.0);
    println!("  Latency P99: {:.2}ms", final_stats.latency_ns_p99 as f64 / 1_000_000.0);
    println!("  Total duration: {:.2}s", final_stats.total_duration.as_secs_f64());
    
    // Write final snapshot to output
    if let Some(ref mut out) = output {
        out.write_snapshot(&final_stats).await?;
    }
    
    // Clean up
    if let Some(h) = snapshot_handle { h.abort(); }
    session.close().await.map_err(|e| anyhow::Error::msg(e.to_string()))?;
    
    Ok(())
}
