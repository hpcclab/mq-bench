use crate::payload::generate_payload;
use crate::rate::RateController;
use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::interval;

pub struct PublisherConfig {
    pub endpoint: String,
    pub key_expr: String,
    pub payload_size: usize,
    pub rate: f64,
    pub duration_secs: Option<u64>,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
}

pub async fn run_publisher(config: PublisherConfig) -> Result<()> {
    println!("Starting publisher:");
    println!("  Endpoint: {}", config.endpoint);
    println!("  Key: {}", config.key_expr);
    println!("  Payload size: {} bytes", config.payload_size);
    println!("  Rate: {:.2} msg/s", config.rate);
    if let Some(duration) = config.duration_secs {
        println!("  Duration: {} seconds", duration);
    } else {
        println!("  Duration: unlimited (until Ctrl+C)");
    }
    
    // Initialize Zenoh session with connection endpoint
    let mut zenoh_config = zenoh::config::Config::default();
    zenoh_config.insert_json5("mode", "\"client\"").map_err(|e| anyhow::Error::msg(e.to_string()))?;
    zenoh_config.insert_json5("connect/endpoints", &format!("[\"{}\"]", config.endpoint)).map_err(|e| anyhow::Error::msg(e.to_string()))?;
    zenoh_config.insert_json5("scouting/multicast/enabled", "false").map_err(|e| anyhow::Error::msg(e.to_string()))?;
    zenoh_config.insert_json5("scouting/gossip/enabled", "false").map_err(|e| anyhow::Error::msg(e.to_string()))?;
    
    let session = zenoh::open(zenoh_config).await.map_err(|e| anyhow::Error::msg(e.to_string()))?;
    let publisher = session.declare_publisher(&config.key_expr).await.map_err(|e| anyhow::Error::msg(e.to_string()))?;
    
    println!("Connected to Zenoh");
    
    // Initialize statistics and rate controller
    let stats = Arc::new(Stats::new());
    let mut rate_controller = RateController::new(config.rate);
    
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
            // Use sent-based rate for publisher (receiver-based throughput is always 0 here)
            let elapsed = snapshot.total_duration.as_secs_f64();
            let send_rate = if elapsed > 0.0 { snapshot.sent_count as f64 / elapsed } else { 0.0 };
            println!(
                "Publisher stats - Sent: {}, Errors: {}, Rate: {:.2} msg/s",
                snapshot.sent_count,
                snapshot.error_count,
                send_rate
            );
        }
    });
    
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
            
            // Wait for next scheduled send
            rate_controller.wait_for_next().await;
            
            // Generate and send payload
            let payload = generate_payload(sequence, config.payload_size);
            
            match publisher.put(payload).await {
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
    
    // Write final snapshot to output
    output.write_snapshot(&final_stats).await?;
    
    // Clean up
    snapshot_handle.abort();
    session.close().await.map_err(|e| anyhow::Error::msg(e.to_string()))?;
    
    Ok(())
}
