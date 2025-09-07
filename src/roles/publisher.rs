use crate::payload::generate_payload;
use crate::rate::RateController;
use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use anyhow::Result;
use bytes::Bytes;
use crate::transport::{TransportBuilder, Engine, ConnectOptions, Transport};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::interval;

pub struct PublisherConfig {
    pub endpoint: String,
    pub key_expr: String,
    pub payload_size: usize,
    pub rate: Option<f64>,
    pub duration_secs: Option<u64>,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
    // Aggregation support
    pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
    pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
}

pub async fn run_publisher(config: PublisherConfig) -> Result<()> {
    println!("Starting publisher:");
    println!("  Endpoint: {}", config.endpoint);
    println!("  Key: {}", config.key_expr);
    println!("  Payload size: {} bytes", config.payload_size);
    if let Some(r) = config.rate { println!("  Rate: {:.2} msg/s", r); } else { println!("  Rate: unlimited (no delay)"); }
    if let Some(duration) = config.duration_secs {
        println!("  Duration: {} seconds", duration);
    } else {
        println!("  Duration: unlimited (until Ctrl+C)");
    }
    // Initialize Transport (default to Zenoh engine; map endpoint to connect options)
    let mut opts = ConnectOptions::default();
    opts.params.insert("endpoint".into(), config.endpoint.clone());
    let transport: Box<dyn Transport> = TransportBuilder::connect(Engine::Zenoh, opts)
        .await
        .map_err(|e| anyhow::Error::msg(format!("transport connect error: {}", e)))?;
    println!("Connected via transport: Zenoh");
    // Pre-declare publisher for performance
    let publisher = transport.create_publisher(&config.key_expr)
        .await
        .map_err(|e| anyhow::Error::msg(format!("create_publisher error: {}", e)))?;
    
    // Initialize statistics and rate controller
    let stats = if let Some(s) = &config.shared_stats { s.clone() } else { Arc::new(Stats::new()) };
    let mut rate_controller = config.rate.map(|r| RateController::new(r));
    
    // Setup output writer (only when not aggregated)
    let mut output = if let Some(ref path) = config.output_file {
        Some(OutputWriter::new_csv(path.clone()).await?)
    } else if config.shared_stats.is_none() {
        Some(OutputWriter::new_stdout())
    } else {
        None
    };
    
    // Start snapshot task
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = Arc::clone(&stats);
        let interval_secs = config.snapshot_interval_secs;
        Some(tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(interval_secs));
            loop {
                interval_timer.tick().await;
                let snapshot = stats_clone.snapshot().await;
                // Compute avg and interval send rates
                let elapsed = snapshot.total_duration.as_secs_f64();
                let avg_send_rate = if elapsed > 0.0 { snapshot.sent_count as f64 / elapsed } else { 0.0 };
                let inst_send_rate = if snapshot.interval_duration.as_secs_f64() > 0.0 {
                    snapshot.interval_sent_count as f64 / snapshot.interval_duration.as_secs_f64()
                } else { 0.0 };
                println!(
                    "Publisher stats - Sent: {}, Errors: {}, Rate(avg): {:.2} msg/s, Rate(inst): {:.2} msg/s",
                    snapshot.sent_count,
                    snapshot.error_count,
                    avg_send_rate,
                    inst_send_rate
                );
            }
        }))
    } else { None };
    
    // Publishing loop
    let mut sequence = 0u64;
    let start_time = std::time::Instant::now();
    
    let publishing_task = async {
        loop {
            // Check duration limit
            if let Some(duration) = config.duration_secs {
                if start_time.elapsed().as_secs() >= duration {
                    println!("Duration limit reached, stopping publisher");
                    break;
                }
            }
            
            // Wait for next scheduled send (if paced)
            if let Some(rc) = &mut rate_controller {
                rc.wait_for_next().await;
            }
            
            // Generate and send payload
        let payload = generate_payload(sequence, config.payload_size);
        let bytes = Bytes::from(payload);

    match publisher.publish(bytes).await {
                Ok(_) => {
                    stats.record_sent().await;
                    sequence += 1;
                }
                Err(e) => {
            eprintln!("Send error: {}", e);
                    stats.record_error().await;
                }
            }
        }
    };
    
    // Wait for either completion or Ctrl+C
    tokio::select! {
        _ = publishing_task => {
            println!("Publishing completed");
        }
        _ = signal::ctrl_c() => {
            println!("Ctrl+C received, stopping publisher");
        }
    }
    
    // Final statistics
    let final_stats = stats.snapshot().await;
    println!("\nFinal Publisher Statistics:");
    println!("  Messages sent: {}", final_stats.sent_count);
    println!("  Errors: {}", final_stats.error_count);
    // Show average send rate over the full run
    let total_elapsed = final_stats.total_duration.as_secs_f64();
    let avg_send_rate = if total_elapsed > 0.0 { final_stats.sent_count as f64 / total_elapsed } else { 0.0 };
    println!("  Average rate: {:.2} msg/s", avg_send_rate);
    println!("  Total duration: {:.2}s", final_stats.total_duration.as_secs_f64());
    
    // Write final snapshot to output (if not aggregated)
    if let Some(ref mut out) = output {
        out.write_snapshot(&final_stats).await?;
    }
    
    // Clean up
    if let Some(h) = snapshot_handle { h.abort(); }
    transport.shutdown().await.map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;
    
    Ok(())
}
