use crate::crash::{CrashConfig, CrashInjector};
use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use crate::payload::generate_payload;
use crate::rate::RateController;
use crate::transport::{ConnectOptions, Engine, Transport, TransportBuilder, TransportError};
use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

pub struct PublisherConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub key_expr: String,
    pub payload_size: usize,
    pub rate: Option<f64>,
    pub duration_secs: Option<u64>,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
    // Aggregation support
    pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
    pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
    // Crash injection
    pub crash_config: CrashConfig,
}

pub async fn run_publisher(config: PublisherConfig) -> Result<()> {
    info!(
        engine = ?config.engine,
        key = %config.key_expr,
        payload_size = config.payload_size,
        rate = ?config.rate,
        duration_secs = ?config.duration_secs,
        endpoint = ?config.connect.params.get("endpoint"),
        crash_enabled = config.crash_config.is_enabled(),
        "Starting publisher"
    );

    // Initialize statistics early so we can track connection failures
    let stats = if let Some(s) = &config.shared_stats {
        s.clone()
    } else {
        Arc::new(Stats::new())
    };

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
                let elapsed = snapshot.total_duration.as_secs_f64();
                let avg_send_rate = if elapsed > 0.0 {
                    snapshot.sent_count as f64 / elapsed
                } else {
                    0.0
                };
                let inst_send_rate = if snapshot.interval_duration.as_secs_f64() > 0.0 {
                    snapshot.interval_sent_count as f64 / snapshot.interval_duration.as_secs_f64()
                } else {
                    0.0
                };
                debug!(
                    sent = snapshot.sent_count,
                    errors = snapshot.error_count,
                    avg_rate = format!("{:.2}", avg_send_rate),
                    inst_rate = format!("{:.2}", inst_send_rate),
                    crashes = snapshot.crashes_injected,
                    reconnects = snapshot.reconnects,
                    "Publisher stats"
                );
            }
        }))
    } else {
        None
    };

    // Initialize crash injector
    let mut crash_injector = CrashInjector::new(config.crash_config.clone());

    // Publishing state (persists across reconnects)
    let mut sequence = 0u64;
    let start_time = std::time::Instant::now();
    let mut rate_controller = config.rate.map(|r| RateController::new(r));
    let mut stopped = false;

    // Outer loop: handles reconnection after crashes
    'reconnect: while !stopped {
        // Check duration limit before connecting
        if let Some(duration) = config.duration_secs {
            if start_time.elapsed().as_secs() >= duration {
                info!("Duration limit reached, stopping publisher");
                break;
            }
        }

        // Connect with retry
        stats.record_connection_attempt();
        let transport: Box<dyn Transport> = match TransportBuilder::connect_with_retry(
            config.engine.clone(),
            config.connect.clone(),
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "Transport connect error");
                stats.record_connection_failure();
                if config.connect.retry_enabled {
                    stats.record_reconnect_failure();
                }
                break;
            }
        };
        info!(engine = ?config.engine, "Connected via transport");

        // Pre-declare publisher
        let publisher = match transport.create_publisher(&config.key_expr).await {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "Create publisher error");
                stats.record_connection_failure();
                break;
            }
        };

        // Inner publishing loop
        let crash_triggered = loop {
            // Check duration limit
            if let Some(duration) = config.duration_secs {
                if start_time.elapsed().as_secs() >= duration {
                    info!("Duration limit reached, stopping publisher");
                    stopped = true;
                    break false;
                }
            }

            // Check for crash injection (only if crashes remaining)
            if crash_injector.is_enabled()
                && crash_injector.has_crashes_remaining()
                && crash_injector.should_crash()
            {
                if crash_injector.consume_crash() {
                    info!("Crash injection triggered");
                    stats.record_crash_injected();
                    break true;
                } else {
                    // No more crashes allowed, disable further checks
                    info!("Crash count limit reached, continuing without further crashes");
                }
            }

            // Determine if we should wait for crash timer
            let crash_check_enabled =
                crash_injector.is_enabled() && crash_injector.has_crashes_remaining();

            // Wait for next scheduled send (if paced) or crash timer
            if crash_check_enabled {
                let time_to_crash = crash_injector.time_until_crash();
                if let Some(rc) = &mut rate_controller {
                    tokio::select! {
                        _ = rc.wait_for_next() => {}
                        _ = tokio::time::sleep(time_to_crash) => {
                            continue; // Re-check crash condition
                        }
                        _ = signal::ctrl_c() => {
                            info!("Ctrl+C received, stopping publisher");
                            stopped = true;
                            break false;
                        }
                    }
                } else {
                    // No rate limit, check for crash or ctrl+c
                    tokio::select! {
                        _ = tokio::time::sleep(time_to_crash) => {
                            continue;
                        }
                        _ = signal::ctrl_c() => {
                            info!("Ctrl+C received, stopping publisher");
                            stopped = true;
                            break false;
                        }
                        else => {}
                    }
                }
            } else {
                // No crash injection, just rate control
                if let Some(rc) = &mut rate_controller {
                    tokio::select! {
                        _ = rc.wait_for_next() => {}
                        _ = signal::ctrl_c() => {
                            info!("Ctrl+C received, stopping publisher");
                            stopped = true;
                            break false;
                        }
                    }
                }
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
                    warn!(error = %e, "Send error");
                    stats.record_error().await;
                    // Check if error is recoverable
                    if matches!(e, TransportError::Disconnected) {
                        break true; // Trigger reconnect
                    }
                }
            }
        };

        // Handle transport cleanup based on crash vs normal exit
        if crash_triggered {
            // HARD CRASH: Force disconnect without graceful shutdown
            // This simulates abrupt failure (network loss, process kill, power loss)
            // Important for testing QoS guarantees - broker should NOT receive DISCONNECT
            info!("Simulating hard crash - aborting connection without graceful disconnect");
            let _ = publisher.force_disconnect().await;
            // Drop transport and publisher immediately without graceful shutdown
            drop(publisher);
            drop(transport);
        } else {
            // Normal exit: graceful shutdown
            let _ = transport.shutdown().await;
        }

        if crash_triggered && config.connect.retry_enabled {
            // Sample repair time and wait before reconnecting
            let repair_time = crash_injector.sample_repair_time();
            info!(repair_secs = repair_time.as_secs_f64(), "Simulating repair delay");
            tokio::time::sleep(repair_time).await;

            // Schedule next crash (deterministic timeline includes the repair downtime)
            crash_injector.schedule_next_crash_after(repair_time);
            stats.record_reconnect();
            info!("Attempting reconnection after crash");
            continue 'reconnect;
        } else if crash_triggered {
            // Crash occurred but retry not enabled - stop
            info!("Crash triggered but retry not enabled, stopping");
            break;
        }
    }

    // Final statistics
    let final_stats = stats.snapshot().await;
    let total_elapsed = final_stats.total_duration.as_secs_f64();
    let avg_send_rate = if total_elapsed > 0.0 {
        final_stats.sent_count as f64 / total_elapsed
    } else {
        0.0
    };
    info!(
        sent = final_stats.sent_count,
        errors = final_stats.error_count,
        crashes = final_stats.crashes_injected,
        reconnects = final_stats.reconnects,
        avg_rate = format!("{:.2}", avg_send_rate),
        duration = format!("{:.2}s", total_elapsed),
        "Final Publisher Statistics"
    );

    // Write final snapshot to output (if not aggregated)
    if let Some(ref mut out) = output {
        out.write_snapshot(&final_stats).await?;
    }

    // Clean up
    if let Some(h) = snapshot_handle {
        h.abort();
    }

    Ok(())
}
