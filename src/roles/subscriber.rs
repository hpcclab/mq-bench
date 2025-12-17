use crate::crash::{CrashConfig, CrashInjector};
use crate::metrics::sequence::SequenceTracker;
use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use crate::payload::parse_header;
use crate::time_sync::now_unix_ns_estimate;
use crate::transport::{ConnectOptions, Engine, Transport, TransportBuilder, TransportMessage};
use anyhow::Result;
use flume;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, info, warn};

pub struct SubscriberConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub key_expr: String,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
    // Aggregation/external snapshot support
    pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
    pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
    // Test-only convenience: stop automatically after N seconds if provided
    pub test_stop_after_secs: Option<u64>,
    // Crash injection
    pub crash_config: CrashConfig,
}

pub async fn run_subscriber(config: SubscriberConfig) -> Result<()> {
    info!(
        engine = ?config.engine,
        key = %config.key_expr,
        endpoint = ?config.connect.params.get("endpoint"),
        crash_enabled = config.crash_config.is_enabled(),
        "Starting subscriber"
    );

    // Initialize statistics early (use shared if provided)
    let stats = if let Some(s) = &config.shared_stats {
        s.clone()
    } else {
        Arc::new(Stats::new())
    };

    // Setup output writer (only when not aggregated/external)
    let mut output = if let Some(ref path) = config.output_file {
        Some(OutputWriter::new_csv(path.clone()).await?)
    } else if config.shared_stats.is_none() {
        Some(OutputWriter::new_stdout())
    } else {
        None
    };

    // Shared sequence tracker - wrapped in Arc<Mutex<>> so snapshot task can access stats
    // This persists across reconnections to track all sequences throughout the test
    let seq_tracker = Arc::new(Mutex::new(SequenceTracker::new()));

    // Start snapshot task (only if not disabled)
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = Arc::clone(&stats);
        let seq_tracker_snap = Arc::clone(&seq_tracker);
        let interval_secs = config.snapshot_interval_secs;
        let mut out = output.take();
        Some(tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(interval_secs));
            loop {
                interval_timer.tick().await;
                // Update stats with current sequence tracker state before snapshot
                {
                    let tracker = seq_tracker_snap.lock().await;
                    stats_clone.set_duplicates(tracker.duplicate_count());
                    stats_clone.set_gaps(tracker.gap_count());
                    stats_clone.set_head_loss(tracker.head_loss());
                }
                let snapshot = stats_clone.snapshot().await;
                if let Some(ref mut o) = out {
                    let _ = o.write_snapshot(&snapshot).await;
                } else {
                    debug!(
                        received = snapshot.received_count,
                        errors = snapshot.error_count,
                        duplicates = snapshot.duplicate_count,
                        gaps = snapshot.gap_count,
                        rate = format!("{:.2}", snapshot.interval_throughput()),
                        p99_ms = format!("{:.2}", snapshot.latency_ns_p99 as f64 / 1_000_000.0),
                        crashes = snapshot.crashes_injected,
                        reconnects = snapshot.reconnects,
                        "Subscriber stats"
                    );
                }
            }
        }))
    } else {
        None
    };

    // Channel + worker to avoid per-message work in callback; send (recv_time, header_bytes)
    // This channel persists across reconnections
    let (tx, rx) = flume::unbounded::<(u64, [u8; 24])>();
    let stats_worker = stats.clone();
    let seq_tracker_worker = Arc::clone(&seq_tracker);
    let worker_handle = tokio::spawn(async move {
        let mut buf = Vec::with_capacity(1024);
        loop {
            // Block until at least 1 item
            let first = match rx.recv_async().await {
                Ok(v) => v,
                Err(_) => {
                    // Channel closed - stats will be reported by main task
                    break;
                }
            };
            buf.clear();
            buf.push(first);
            // Drain a small batch without awaiting to amortize locking
            while let Ok(v) = rx.try_recv() {
                buf.push(v);
                if buf.len() >= 1024 {
                    break;
                }
            }
            // Parse headers, track sequences, compute latencies
            let mut latencies = Vec::with_capacity(buf.len());
            {
                let mut tracker = seq_tracker_worker.lock().await;
                for (recv_ns, hdr) in buf.drain(..) {
                    if let Ok(h) = parse_header(&hdr) {
                        // Track sequence (handles duplicates)
                        if tracker.record(h.seq) {
                            // Only record latency for new messages
                            latencies.push(recv_ns.saturating_sub(h.timestamp_ns));
                        }
                    }
                }
            }
            // Record latencies for new messages only
            if !latencies.is_empty() {
                stats_worker.record_received_batch(&latencies).await;
            }
        }
    });

    // Initialize crash injector
    let mut crash_injector = CrashInjector::new(config.crash_config.clone());

    // Calculate end time for duration-based stopping
    let start_time = std::time::Instant::now();
    let mut stopped = false;

    // Outer loop: handles reconnection after crashes
    'reconnect: while !stopped {
        // Check test timeout before connecting
        if let Some(s) = config.test_stop_after_secs {
            if start_time.elapsed().as_secs() >= s {
                info!("Test timeout reached, stopping subscriber");
                break;
            }
        }

        // Initialize Transport with optional retry
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

        // Subscribe via Transport with a handler
        let handler_tx = tx.clone();
        let subscription = match transport
            .subscribe(
                &config.key_expr,
                Box::new(move |msg: TransportMessage| {
                    // Minimal callback: copy 24-byte header and enqueue with receive timestamp
                    let mut hdr = [0u8; 24];
                    let bytes = msg.payload.as_cow();
                    if bytes.len() >= 24 {
                        hdr.copy_from_slice(&bytes[..24]);
                        let recv = now_unix_ns_estimate();
                        let _ = handler_tx.try_send((recv, hdr));
                    }
                }),
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "Subscribe error");
                stats.record_connection_failure();
                break;
            }
        };
        info!(key = %config.key_expr, "Subscribed to key expression");

        // Inner loop: wait for crash timer, ctrl+c, or test timeout
        let crash_triggered = loop {
            // Calculate remaining time for test timeout
            let remaining_test_time = if let Some(s) = config.test_stop_after_secs {
                let elapsed = start_time.elapsed().as_secs();
                if elapsed >= s {
                    stopped = true;
                    break false;
                }
                Duration::from_secs(s - elapsed)
            } else {
                Duration::MAX
            };

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
                    // No more crashes allowed
                    info!("Crash count limit reached, continuing without further crashes");
                }
            }

            // Determine if we should wait for crash timer
            let crash_check_enabled =
                crash_injector.is_enabled() && crash_injector.has_crashes_remaining();

            // Wait for crash timer, test timeout, or ctrl+c
            let time_to_crash = if crash_check_enabled {
                crash_injector.time_until_crash()
            } else {
                Duration::MAX
            };

            let wait_time = time_to_crash.min(remaining_test_time).min(Duration::from_secs(1));

            tokio::select! {
                _ = tokio::time::sleep(wait_time) => {
                    // Check conditions again
                    continue;
                }
                _ = signal::ctrl_c() => {
                    info!("Ctrl+C received, stopping subscriber");
                    stopped = true;
                    break false;
                }
            }
        };

        // Handle transport cleanup based on crash vs normal exit
        if crash_triggered {
            // HARD CRASH: Force disconnect without graceful shutdown
            // This simulates abrupt failure (network loss, process kill, power loss)
            // Important for testing QoS guarantees - broker should NOT receive DISCONNECT
            info!("Simulating hard crash - aborting connection without graceful disconnect");
            let _ = subscription.force_disconnect().await;
            // Drop subscription and transport immediately without graceful shutdown
            drop(subscription);
            drop(transport);
        } else {
            // Normal exit: graceful shutdown
            let _ = subscription.shutdown().await;
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

    // Drop tx to signal worker to finish, then wait for it to complete
    drop(tx);
    // Give the worker a moment to drain any remaining messages
    let _ = tokio::time::timeout(Duration::from_millis(100), worker_handle).await;

    // Update stats with final duplicate/gap counts from sequence tracker
    {
        let tracker = seq_tracker.lock().await;
        stats.set_duplicates(tracker.duplicate_count());
        stats.set_gaps(tracker.gap_count());
        stats.set_head_loss(tracker.head_loss());
    }

    // Final statistics
    let final_stats = stats.snapshot().await;
    info!(
        received = final_stats.received_count,
        errors = final_stats.error_count,
        duplicates = final_stats.duplicate_count,
        gaps = final_stats.gap_count,
        head_loss = final_stats.head_loss,
        crashes = final_stats.crashes_injected,
        reconnects = final_stats.reconnects,
        avg_rate = format!("{:.2}", final_stats.total_throughput()),
        p50_ms = format!("{:.2}", final_stats.latency_ns_p50 as f64 / 1_000_000.0),
        p95_ms = format!("{:.2}", final_stats.latency_ns_p95 as f64 / 1_000_000.0),
        p99_ms = format!("{:.2}", final_stats.latency_ns_p99 as f64 / 1_000_000.0),
        duration = format!("{:.2}s", final_stats.total_duration.as_secs_f64()),
        "Final Subscriber Statistics"
    );

    // Write final snapshot to output (create new writer if we have a path)
    // Note: The periodic output writer is owned by snapshot_handle task,
    // so we need to create a new one for the final snapshot.
    if let Some(ref path) = config.output_file {
        let mut final_out = OutputWriter::new_csv(path.clone()).await?;
        final_out.write_snapshot(&final_stats).await?;
    } else if config.shared_stats.is_none() && output.is_some() {
        // stdout case - output wasn't taken
        if let Some(ref mut out) = output {
            out.write_snapshot(&final_stats).await?;
        }
    }

    // Clean up
    if let Some(h) = snapshot_handle {
        h.abort();
    }

    Ok(())
}
