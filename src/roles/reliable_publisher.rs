//! Reliable Publisher Role
//!
//! This publisher waits for broker ACK before considering a message "confirmed".
//! On crash/reconnect, it resumes from the last confirmed sequence number.
//!
//! Key differences from regular publisher:
//! - Synchronous publish: waits for PUBACK (QoS 1) or PUBCOMP (QoS 2) before next message
//! - Tracks confirmed sequence numbers
//! - On reconnect, resumes from last confirmed sequence + 1
//! - Lower throughput but guaranteed delivery for QoS 1/2
//!
//! For QoS 0, behaves like regular publisher (no ACK to wait for).

use crate::crash::{CrashConfig, CrashInjector};
use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use crate::payload::generate_payload;
use crate::rate::RateController;
use crate::transport::{ConnectOptions, Engine};
use anyhow::Result;
use bytes::Bytes;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

pub struct ReliablePublisherConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub key_expr: String,
    pub payload_size: usize,
    pub rate: Option<f64>,
    pub duration_secs: Option<u64>,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
    pub shared_stats: Option<Arc<Stats>>,
    pub disable_internal_snapshot: bool,
    pub crash_config: CrashConfig,
    /// Timeout for waiting for ACK (seconds)
    pub ack_timeout_secs: u64,
}

/// ACK notification from eventloop
#[derive(Debug)]
enum AckEvent {
    /// QoS 1: PUBACK received with packet ID
    PubAck(u16),
    /// QoS 2: PUBCOMP received (full handshake complete)
    PubComp(u16),
    /// Connection error
    Error(String),
}

pub async fn run_reliable_publisher(config: ReliablePublisherConfig) -> Result<()> {
    // Validate engine - only MQTT supported for reliable publishing
    if !matches!(config.engine, Engine::Mqtt) {
        return Err(anyhow::anyhow!(
            "Reliable publisher only supports MQTT engine (got {:?})",
            config.engine
        ));
    }

    info!(
        engine = ?config.engine,
        key = %config.key_expr,
        payload_size = config.payload_size,
        rate = ?config.rate,
        duration_secs = ?config.duration_secs,
        crash_enabled = config.crash_config.is_enabled(),
        "Starting reliable publisher"
    );

    // Parse MQTT connection options
    let host = config
        .connect
        .params
        .get("host")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1".into());
    let port: u16 = config
        .connect
        .params
        .get("port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1883);
    let qos_level: u8 = config
        .connect
        .params
        .get("qos")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1); // Default to QoS 1 for reliable publishing
    let qos = match qos_level {
        2 => QoS::ExactlyOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::AtMostOnce,
    };
    let client_id = config
        .connect
        .params
        .get("client_id")
        .cloned()
        .unwrap_or_else(|| format!("reliable-pub-{}", uuid::Uuid::new_v4()));
    let clean_session: bool = config
        .connect
        .params
        .get("clean_session")
        .map(|s| s != "false" && s != "0")
        .unwrap_or(false); // Default to persistent sessions for reliable publishing

    // Initialize statistics
    let stats = if let Some(s) = &config.shared_stats {
        s.clone()
    } else {
        Arc::new(Stats::new())
    };

    // Setup output writer
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
                debug!(
                    sent = snapshot.sent_count,
                    errors = snapshot.error_count,
                    crashes = snapshot.crashes_injected,
                    reconnects = snapshot.reconnects,
                    "Reliable publisher stats"
                );
            }
        }))
    } else {
        None
    };

    // Initialize crash injector
    let mut crash_injector = CrashInjector::new(config.crash_config.clone());

    // Publishing state (persists across reconnects)
    let mut confirmed_sequence = 0u64; // Last sequence that was ACKed
    let mut pending_sequence = 0u64; // Next sequence to send
    let start_time = std::time::Instant::now();
    let mut rate_controller = config.rate.map(|r| RateController::new(r));
    let mut stopped = false;
    let ack_timeout = Duration::from_secs(config.ack_timeout_secs);

    info!(
        qos = qos_level,
        client_id = %client_id,
        clean_session = clean_session,
        "Reliable publisher using persistent session"
    );

    // Outer loop: handles reconnection after crashes
    'reconnect: while !stopped {
        // Check duration limit before connecting
        if let Some(duration) = config.duration_secs {
            if start_time.elapsed().as_secs() >= duration {
                info!("Duration limit reached, stopping reliable publisher");
                break;
            }
        }

        // On reconnect after crash, resume from last confirmed sequence
        if pending_sequence > confirmed_sequence {
            info!(
                pending = pending_sequence,
                confirmed = confirmed_sequence,
                "Resuming from last confirmed sequence"
            );
            pending_sequence = confirmed_sequence;
        }

        // Connect to MQTT broker
        stats.record_connection_attempt();
        let mut options = MqttOptions::new(client_id.clone(), host.clone(), port);
        options.set_keep_alive(Duration::from_secs(30));
        options.set_clean_session(clean_session);
        options.set_max_packet_size(16 * 1024 * 1024, 16 * 1024 * 1024);
        
        let (client, mut eventloop) = AsyncClient::new(options, 1024);

        // Channel for ACK notifications
        let (ack_tx, mut ack_rx) = mpsc::channel::<AckEvent>(64);

        // Track inflight messages: packet_id -> sequence_number
        let inflight: Arc<tokio::sync::Mutex<HashMap<u16, u64>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let inflight_clone = Arc::clone(&inflight);

        // Spawn eventloop poller that sends ACK notifications
        let ack_tx_clone = ack_tx.clone();
        let poller = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                        // QoS 1: message acknowledged
                        let _ = ack_tx_clone.send(AckEvent::PubAck(ack.pkid)).await;
                    }
                    Ok(Event::Incoming(Incoming::PubComp(comp))) => {
                        // QoS 2: full handshake complete
                        let _ = ack_tx_clone.send(AckEvent::PubComp(comp.pkid)).await;
                    }
                    Ok(Event::Incoming(Incoming::PubRec(_))) => {
                        // QoS 2 step 2: broker received, waiting for PUBREL/PUBCOMP
                        // rumqttc handles PUBREL automatically
                    }
                    Ok(_) => {}
                    Err(e) => {
                        let _ = ack_tx_clone.send(AckEvent::Error(e.to_string())).await;
                        break;
                    }
                }
            }
        });

        info!(engine = ?config.engine, "Connected via transport");

        // Inner publishing loop
        let crash_triggered = 'publish: loop {
            // Check duration limit
            if let Some(duration) = config.duration_secs {
                if start_time.elapsed().as_secs() >= duration {
                    info!("Duration limit reached, stopping reliable publisher");
                    stopped = true;
                    break 'publish false;
                }
            }

            // Check for crash injection
            if crash_injector.is_enabled()
                && crash_injector.has_crashes_remaining()
                && crash_injector.should_crash()
            {
                if crash_injector.consume_crash() {
                    info!("Crash injection triggered");
                    stats.record_crash_injected();
                    break 'publish true;
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
                            info!("Ctrl+C received, stopping reliable publisher");
                            stopped = true;
                            break 'publish false;
                        }
                    }
                } else {
                    tokio::select! {
                        _ = tokio::time::sleep(time_to_crash) => {
                            continue;
                        }
                        _ = signal::ctrl_c() => {
                            info!("Ctrl+C received, stopping reliable publisher");
                            stopped = true;
                            break 'publish false;
                        }
                        else => {}
                    }
                }
            } else if let Some(rc) = &mut rate_controller {
                tokio::select! {
                    _ = rc.wait_for_next() => {}
                    _ = signal::ctrl_c() => {
                        info!("Ctrl+C received, stopping reliable publisher");
                        stopped = true;
                        break 'publish false;
                    }
                }
            }

            // Generate payload with current pending sequence
            let payload = generate_payload(pending_sequence, config.payload_size);
            let bytes = Bytes::from(payload);

            // Publish message
            match client
                .publish(&config.key_expr, qos, false, bytes.to_vec())
                .await
            {
                Ok(_) => {
                    debug!(seq = pending_sequence, "Message sent, waiting for ACK");

                    // For QoS 0, no ACK to wait for
                    if qos == QoS::AtMostOnce {
                        stats.record_sent().await;
                        confirmed_sequence = pending_sequence;
                        pending_sequence += 1;
                        continue;
                    }

                    // Wait for ACK with timeout
                    let ack_result = tokio::time::timeout(ack_timeout, ack_rx.recv()).await;

                    match ack_result {
                        Ok(Some(AckEvent::PubAck(pkid))) => {
                            debug!(seq = pending_sequence, pkid = pkid, "PUBACK received");
                            stats.record_sent().await;
                            confirmed_sequence = pending_sequence;
                            pending_sequence += 1;
                        }
                        Ok(Some(AckEvent::PubComp(pkid))) => {
                            debug!(seq = pending_sequence, pkid = pkid, "PUBCOMP received");
                            stats.record_sent().await;
                            confirmed_sequence = pending_sequence;
                            pending_sequence += 1;
                        }
                        Ok(Some(AckEvent::Error(e))) => {
                            warn!(error = %e, seq = pending_sequence, "Connection error during ACK wait");
                            stats.record_error().await;
                            break 'publish true; // Trigger reconnect
                        }
                        Ok(None) => {
                            warn!(seq = pending_sequence, "ACK channel closed");
                            break 'publish true; // Trigger reconnect
                        }
                        Err(_) => {
                            warn!(
                                seq = pending_sequence,
                                timeout_secs = config.ack_timeout_secs,
                                "ACK timeout"
                            );
                            stats.record_error().await;
                            // Don't increment sequence - will be retried
                            break 'publish true; // Trigger reconnect
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, seq = pending_sequence, "Publish error");
                    stats.record_error().await;
                    break 'publish true; // Trigger reconnect
                }
            }
        };

        // Cleanup
        poller.abort();
        drop(client);

        if crash_triggered && config.connect.retry_enabled {
            // Sample repair time and wait before reconnecting
            let repair_time = crash_injector.sample_repair_time();
            info!(
                repair_secs = repair_time.as_secs_f64(),
                confirmed_seq = confirmed_sequence,
                "Simulating repair delay, will resume from confirmed sequence"
            );
            tokio::time::sleep(repair_time).await;

            // Schedule next crash (deterministic timeline includes the repair downtime)
            crash_injector.schedule_next_crash_after(repair_time);
            stats.record_reconnect();
            info!("Attempting reconnection after crash");
            continue 'reconnect;
        } else if crash_triggered {
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
        confirmed = confirmed_sequence,
        errors = final_stats.error_count,
        crashes = final_stats.crashes_injected,
        reconnects = final_stats.reconnects,
        avg_rate = format!("{:.2}", avg_send_rate),
        duration = format!("{:.2}s", total_elapsed),
        "Final Reliable Publisher Statistics"
    );

    // Write final snapshot
    if let Some(ref mut out) = output {
        out.write_snapshot(&final_stats).await?;
    }

    if let Some(h) = snapshot_handle {
        h.abort();
    }

    Ok(())
}
