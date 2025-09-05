use crate::payload::parse_header;
use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::signal;
use tokio::time::interval;

pub struct SubscriberConfig {
    pub endpoint: String,
    pub key_expr: String,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
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
    
    // Initialize statistics
    let stats = Arc::new(Stats::new());
    
    // Setup output writer
    let mut output = if let Some(ref path) = config.output_file {
        OutputWriter::new_csv(path.clone()).await?
    } else {
        OutputWriter::new_stdout()
    };
    
    // Start snapshot task
    let stats_clone = Arc::clone(&stats);
    let snapshot_handle = tokio::spawn(async move {
        let mut interval_timer = interval(Duration::from_secs(config.snapshot_interval_secs));
        loop {
            interval_timer.tick().await;
            let snapshot = stats_clone.snapshot().await;
            println!("Subscriber stats - Received: {}, Errors: {}, Rate: {:.2} msg/s, P99 Latency: {:.2}ms", 
                    snapshot.received_count, 
                    snapshot.error_count,
                    snapshot.interval_throughput(),
                    snapshot.latency_ns_p99 as f64 / 1_000_000.0);
        }
    });
    
    // Create subscriber with callback
    let stats_clone = Arc::clone(&stats);
    let _subscriber = session.declare_subscriber(&config.key_expr)
        .callback(move |sample| {
            let stats = stats_clone.clone();
            tokio::spawn(async move {
                let receive_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                
                match parse_header(&sample.payload().to_bytes()) {
                    Ok(header) => {
                        // Calculate latency
                        let latency_ns = receive_time.saturating_sub(header.timestamp_ns);
                        stats.record_received(latency_ns).await;
                    }
                    Err(e) => {
                        eprintln!("Failed to parse message header: {}", e);
                        stats.record_error().await;
                    }
                }
            });
        })
        .await.map_err(|e| anyhow::Error::msg(e.to_string()))?;
    
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
    output.write_snapshot(&final_stats).await?;
    
    // Clean up
    snapshot_handle.abort();
    session.close().await.map_err(|e| anyhow::Error::msg(e.to_string()))?;
    
    Ok(())
}
