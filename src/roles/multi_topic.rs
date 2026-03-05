use anyhow::Result;
use bytes::Bytes;
use futures::future::join_all;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::crash::{CrashConfig, CrashInjector};
use crate::metrics::sequence::SequenceTracker;
use crate::metrics::stats::Stats;
use crate::payload::generate_payload;
use crate::rate::RateController;
use crate::transport::{ConnectOptions, Engine, Transport, TransportBuilder};

#[derive(Clone, Copy, Debug)]
pub enum KeyMappingMode {
    MDim,
    Hash,
}

/// Multi-topic fanout driver (single process)
/// Generates multi-segment keys like: {prefix}/t{tenant}/r{region}/svc{service}/k{shard}
/// and can drive many logical publishers without spawning many OS processes.
pub struct MultiTopicConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub topic_prefix: String,
    pub tenants: u32,
    pub regions: u32,
    pub services: u32,
    pub shards: u32,
    pub publishers: i64, // number of logical publishers (<= T*R*S*K); negative => use total_keys
    pub mapping: KeyMappingMode, // mapping mode from i -> (t,r,s,k)
    pub payload_size: usize,
    pub rate_per_pub: Option<f64>,
    pub duration_secs: u64,
    pub snapshot_interval_secs: u64,
    pub share_transport: bool, // when true, reuse one transport for all publishers
    pub ramp_up_secs: f64, // total ramp-up time in seconds (0 = no delay)
    // Aggregation support
    pub shared_stats: Option<Arc<Stats>>, // when set, aggregate externally
    pub disable_internal_snapshot: bool,
    // Crash injection support
    pub crash_config: CrashConfig,
    /// If true, crash/reconnect each topic independently (per-transport) instead of
    /// crashing the whole multi-topic role at once.
    pub crash_per_topic: bool,
    /// Optional deterministic phase staggering (seconds) applied per topic index.
    /// Effective only when `crash_per_topic=true`.
    pub crash_stagger_secs: f64,
}

fn fnv1a64(mut x: u64) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325; // offset basis
    let prime: u64 = 0x100000001b3;
    // mix 8 bytes of x
    for _ in 0..8 {
        let b = (x & 0xFF) as u8;
        h ^= b as u64;
        h = h.wrapping_mul(prime);
        x >>= 8;
    }
    h
}

fn derive_topic_seed(base_seed: u64, topic_idx: u64) -> u64 {
    base_seed ^ fnv1a64(topic_idx.wrapping_add(0x9e3779b97f4a7c15))
}

fn map_index(i: u64, t: u32, r: u32, s: u32, k: u32, mode: KeyMappingMode) -> (u32, u32, u32, u32) {
    let t64 = t as u64;
    let r64 = r as u64;
    let s64 = s as u64;
    let k64 = k as u64;
    match mode {
        KeyMappingMode::MDim => {
            let ti = (i % t64) as u32;
            let ri = ((i / t64) % r64) as u32;
            let si = ((i / (t64 * r64)) % s64) as u32;
            let ki = ((i / (t64 * r64 * s64)) % k64) as u32;
            (ti, ri, si, ki)
        }
        KeyMappingMode::Hash => {
            let h = fnv1a64(i + 0x9e3779b97f4a7c15); // add golden ratio to spread low i
            let ti = (h % t64) as u32;
            let ri = ((h / t64) % r64) as u32;
            let si = ((h / (t64 * r64)) % s64) as u32;
            let ki = ((h / (t64 * r64 * s64)) % k64) as u32;
            (ti, ri, si, ki)
        }
    }
}

pub async fn run_multi_topic(config: MultiTopicConfig) -> Result<()> {
    // Determine total keys and effective publisher count
    let total_keys = (config.tenants as u64)
        .saturating_mul(config.regions as u64)
        .saturating_mul(config.services as u64)
        .saturating_mul(config.shards as u64);
    let pubs: u64 = if config.publishers < 0 {
        total_keys
    } else {
        (config.publishers as u64).min(total_keys)
    };

    info!(
        engine = ?config.engine,
        prefix = %config.topic_prefix,
        tenants = config.tenants,
        regions = config.regions,
        services = config.services,
        shards = config.shards,
        pubs = pubs,
        payload = config.payload_size,
        rate = ?config.rate_per_pub,
        duration_secs = config.duration_secs,
        "[multi_topic] starting"
    );

    // Shared vs per-key transport depending on config

    // Stats
    let stats: Arc<Stats> = config
        .shared_stats
        .clone()
        .unwrap_or_else(|| Arc::new(Stats::new()));

    // Optional internal snapshot
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = stats.clone();
        let every = config.snapshot_interval_secs;
        Some(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(every));
            loop {
                t.tick().await;
                let s = stats_clone.snapshot().await;
                debug!(
                    sent = s.sent_count,
                    errors = s.error_count,
                    interval_tps = %format!("{:.2}", s.interval_throughput()),
                    "[multi_topic] snapshot"
                );
            }
        }))
    } else {
        None
    };

    // Prepare publishers (pubs calculated above)

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(pubs as usize);

    if config.share_transport {
        info!("[multi_topic] using shared transport");
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
                return Ok(());
            }
        };
        
        // Optimization: For high publisher counts (e.g. >1000), spawning one task per publisher
        // with its own RateController (timer) creates massive scheduling overhead.
        // Instead, we use a single task to drive all publishers in a round-robin fashion
        // if a rate is specified. If no rate (max speed), we still use per-pub tasks but without timers.
        
        let mut pub_handles = Vec::with_capacity(pubs as usize);
        for i in 0..pubs {
            let (t, r, s, k) = map_index(
                i,
                config.tenants,
                config.regions,
                config.services,
                config.shards,
                config.mapping,
            );
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let pub_handle = transport.create_publisher(&key).await.map_err(|e| {
                anyhow::Error::msg(format!("create_publisher error ({}): {}", key, e))
            })?;
            // Track connection created
            stats.increment_connections();
            pub_handles.push(pub_handle);
        }

        if let Some(rate) = config.rate_per_pub {
            // Sharded driver tasks for publishers to allow concurrency
            // Total target rate = rate * pubs
            let total_rate = rate * (pubs as f64);
            // Use a reasonable number of shards (e.g. 32) to allow concurrency without spawning per-pub tasks
            let num_shards = 32.min(pubs as usize).max(1);
            let rate_per_shard = total_rate / (num_shards as f64);
            
            info!(
                num_shards = num_shards,
                pubs = pubs,
                total_rate = %format!("{:.2}", total_rate),
                rate_per_shard = %format!("{:.2}", rate_per_shard),
                "[multi_topic] optimizing driver tasks"
            );
            
            // Distribute pub_handles into shards
            let mut pub_iter = pub_handles.into_iter();
            let chunk_size = (pubs as usize + num_shards - 1) / num_shards;

            for _ in 0..num_shards {
                let mut shard_pubs = Vec::with_capacity(chunk_size);
                for _ in 0..chunk_size {
                    if let Some(p) = pub_iter.next() {
                        shard_pubs.push(p);
                    }
                }
                if shard_pubs.is_empty() { break; }
                
                let stats_p = stats.clone();
                let payload_size = config.payload_size;
                let stop_flag = stop.clone();
                let shard_size = shard_pubs.len();
                
                handles.push(tokio::spawn(async move {
                    let mut rc = RateController::new(rate_per_shard);
                    // Per-topic sequence numbers (not global across topics).
                    // This keeps gap/duplicate accounting meaningful per topic.
                    let mut pub_idx = 0;
                    let num_pubs = shard_pubs.len();
                    let mut seqs: Vec<u64> = vec![0u64; num_pubs];
                    let mut is_active = false;
                    
                    loop {
                        if stop_flag.load(Ordering::Relaxed) {
                            break;
                        }
                        rc.wait_for_next().await;
                        
                        let seq = seqs[pub_idx];
                        let payload = generate_payload(seq, payload_size);
                        let bytes = Bytes::from(payload);
                        
                        // Round-robin publish within shard
                        if let Some(ph) = shard_pubs.get(pub_idx) {
                            match ph.publish(bytes).await {
                                Ok(_) => {
                                    if !is_active {
                                        // Activate all connections in this shard on first successful publish
                                        for _ in 0..shard_size {
                                            stats_p.increment_active_connections();
                                        }
                                        is_active = true;
                                    }
                                    stats_p.record_sent().await;
                                }
                                Err(e) => {
                                    if seq % 1000 == 0 {
                                        warn!(error = %e, "[multi_topic] send error (sample)");
                                    }
                                    stats_p.record_error().await;
                                }
                            }
                        }

                        seqs[pub_idx] = seqs[pub_idx].wrapping_add(1);
                        pub_idx = (pub_idx + 1) % num_pubs;
                    }
                    
                    // Track connection shutdown for all pubs in shard
                    if is_active {
                        for _ in 0..shard_size {
                            stats_p.decrement_active_connections();
                        }
                    }
                    for _ in 0..shard_size {
                        stats_p.decrement_connections();
                    }
                    
                    for ph in shard_pubs {
                        let _ = ph.shutdown().await;
                    }
                }));
            }
        } else {
            // No rate limit: spawn per-publisher tasks for max throughput
            for (_i, pub_handle) in pub_handles.into_iter().enumerate() {
                let stats_p = stats.clone();
                let payload_size = config.payload_size;
                let stop_flag = stop.clone();
                handles.push(tokio::spawn(async move {
                    let mut seq = 0u64;
                    let mut is_active = false;
                    loop {
                        if stop_flag.load(Ordering::Relaxed) {
                            break;
                        }
                        // No rate controller wait
                        let payload = generate_payload(seq, payload_size);
                        let bytes = Bytes::from(payload);
                        match pub_handle.publish(bytes).await {
                            Ok(_) => {
                                if !is_active {
                                    stats_p.increment_active_connections();
                                    is_active = true;
                                }
                                stats_p.record_sent().await;
                                seq = seq.wrapping_add(1);
                            }
                            Err(_e) => {
                                stats_p.record_error().await;
                            }
                        }
                    }
                    // Track connection shutdown
                    if is_active {
                        stats_p.decrement_active_connections();
                    }
                    stats_p.decrement_connections();
                    let _ = pub_handle.shutdown().await;
                }));
            }
        }

        // Wait and stop
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        stop.store(true, Ordering::Relaxed);
        let _ = join_all(handles).await;
        transport
            .shutdown()
            .await
            .map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;
        if let Some(h) = snapshot_handle {
            h.abort();
        }
        let final_stats = stats.snapshot().await;
        info!(
            sent = final_stats.sent_count,
            errors = final_stats.error_count,
            total_tps = format!("{:.2}", final_stats.total_throughput()),
            "[multi_topic] done"
        );
        return Ok(());
    }
    
    // Per-key transport mode with crash injection support
    // Early exit for shared transport mode if crash injection enabled
    if config.share_transport {
        if config.crash_config.is_enabled() {
            warn!(
                "[multi_topic] Crash injection not supported with share_transport=true. \
                 Use share_transport=false for crash testing."
            );
        }
        // (The code above handles shared transport mode without crashes)
    }

    // Calculate per-connection delay from total ramp-up time
    let ramp_delay_us = if config.ramp_up_secs > 0.0 && pubs > 1 {
        ((config.ramp_up_secs * 1_000_000.0) / (pubs - 1) as f64) as u64
    } else {
        0
    };
    info!(
        ramp_up_secs = config.ramp_up_secs,
        delay_per_conn_us = ramp_delay_us,
        crash_enabled = config.crash_config.is_enabled(),
        "[multi_topic] using per-key transports"
    );

    // Derive a stable base seed for per-topic crash injectors.
    let crash_seed_base: Option<u64> = if config.crash_config.is_enabled() {
        Some(config.crash_config.seed.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(12345)
        }))
    } else {
        None
    };

    let start_time = std::time::Instant::now();

    // Per-topic sequence numbers persist across crash cycles to make
    // offline-queue delivery (QoS1/2 + clean_session=false) measurable.
    let mut seq_vec: Vec<std::sync::atomic::AtomicU64> = Vec::with_capacity(pubs as usize);
    for _ in 0..pubs {
        seq_vec.push(std::sync::atomic::AtomicU64::new(0));
    }
    let seqs = Arc::new(seq_vec);

    // Per-topic crash/reconnect (independent schedules).
    if config.crash_config.is_enabled() && config.crash_per_topic {
        info!(
            stagger_secs = config.crash_stagger_secs,
            "[multi_topic] crash scope: per-topic"
        );

        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(pubs as usize);
        for i in 0..pubs {
            if i > 0 && ramp_delay_us > 0 {
                tokio::time::sleep(Duration::from_micros(ramp_delay_us)).await;
            }
            let (t, r, s, k) = map_index(
                i,
                config.tenants,
                config.regions,
                config.services,
                config.shards,
                config.mapping,
            );
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let engine = config.engine.clone();
            let connect = config.connect.clone();
            let stats_p = stats.clone();
            let rate = config.rate_per_pub;
            let payload_size = config.payload_size;
            let stop_flag = stop.clone();
            let start = start_time;
            let duration_secs = config.duration_secs;
            let idx: usize = i as usize;
            let seqs_p = seqs.clone();
            let mut crash_cfg = config.crash_config.clone();
            if let Some(base) = crash_seed_base {
                crash_cfg.seed = Some(derive_topic_seed(base, i));
            }
            let stagger_secs = config.crash_stagger_secs;

            handles.push(tokio::spawn(async move {
                let mut rc = rate.map(RateController::new);
                let mut is_active = false;
                let mut crash_injector = CrashInjector::new(crash_cfg);
                if stagger_secs > 0.0 {
                    crash_injector.apply_phase_offset(Duration::from_secs_f64(stagger_secs * (i as f64)));
                }

                // Connect loop
                let mut transport: Option<Box<dyn Transport>> = None;
                let mut pub_handle: Option<Box<dyn crate::transport::Publisher>> = None;

                while !stop_flag.load(Ordering::Relaxed) && start.elapsed().as_secs() < duration_secs {
                    // Ensure connected
                    if transport.is_none() || pub_handle.is_none() {
                        stats_p.record_connection_attempt();
                        match TransportBuilder::connect_with_retry(engine.clone(), connect.clone()).await {
                            Ok(t) => {
                                match t.create_publisher(&key).await {
                                    Ok(p) => {
                                        stats_p.increment_connections();
                                        transport = Some(t);
                                        pub_handle = Some(p);
                                        is_active = false;
                                    }
                                    Err(e) => {
                                        warn!(key = %key, error = %e, "Create publisher error");
                                        stats_p.record_connection_failure();
                                        let _ = t.shutdown().await;
                                        tokio::time::sleep(Duration::from_millis(250)).await;
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(key = %key, error = %e, "Transport connect error");
                                stats_p.record_connection_failure();
                                tokio::time::sleep(Duration::from_millis(250)).await;
                                continue;
                            }
                        }
                    }

                    // Crash check
                    if crash_injector.is_enabled() && crash_injector.should_crash() {
                        if crash_injector.consume_crash() {
                            info!(key = %key, "[multi_topic] Topic crash injected");
                            stats_p.record_crash_injected();

                            if let Some(ph) = pub_handle.take() {
                                let _ = ph.force_disconnect().await;
                                if is_active {
                                    stats_p.decrement_active_connections();
                                }
                                stats_p.decrement_connections();
                            }
                            if let Some(t) = transport.take() {
                                let _ = t.force_disconnect().await;
                            }

                            let repair_time = crash_injector.sample_repair_time();
                            tokio::time::sleep(repair_time).await;
                            crash_injector.schedule_next_crash_after(repair_time);
                            stats_p.record_reconnect();
                            continue;
                        }
                    }

                    // Publish
                    if let Some(r) = &mut rc {
                        r.wait_for_next().await;
                    }
                    let seq = seqs_p[idx].fetch_add(1, Ordering::Relaxed);
                    let payload = generate_payload(seq, payload_size);
                    let bytes = Bytes::from(payload);

                    if let Some(ph) = pub_handle.as_ref() {
                        match ph.publish(bytes).await {
                            Ok(_) => {
                                if !is_active {
                                    stats_p.increment_active_connections();
                                    is_active = true;
                                }
                                stats_p.record_sent().await;
                            }
                            Err(e) => {
                                warn!(key = %key, error = %e, "[multi_topic] send error");
                                stats_p.record_error().await;
                            }
                        }
                    }
                }

                // Cleanup
                if let Some(ph) = pub_handle.take() {
                    let _ = ph.shutdown().await;
                    if is_active {
                        stats_p.decrement_active_connections();
                    }
                    stats_p.decrement_connections();
                }
                if let Some(t) = transport.take() {
                    let _ = t.shutdown().await;
                }
            }));
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        stop.store(true, Ordering::Relaxed);
        let _ = join_all(handles).await;

        if let Some(h) = snapshot_handle {
            h.abort();
        }
        let final_stats = stats.snapshot().await;
        info!(
            sent = final_stats.sent_count,
            errors = final_stats.error_count,
            total_tps = format!("{:.2}", final_stats.total_throughput()),
            crashes = final_stats.crashes_injected,
            reconnects = final_stats.reconnects,
            "[multi_topic] done"
        );
        return Ok(());
    }

    // Crash injection setup (process-wide crash/reconnect)
    let mut crash_injector = CrashInjector::new(config.crash_config.clone());

    'reconnect: loop {
        // Check duration limit
        if start_time.elapsed().as_secs() >= config.duration_secs {
            break;
        }

        // Reset handles for this reconnection cycle
        let mut handles = Vec::with_capacity(pubs as usize);
        let stop = Arc::new(AtomicBool::new(false));

        // Create all publishers
        for i in 0..pubs {
            // Apply ramp-up delay between connections (skip first, reset on reconnect)
            if i > 0 && ramp_delay_us > 0 {
                tokio::time::sleep(Duration::from_micros(ramp_delay_us)).await;
            }
            let (t, r, s, k) = map_index(
                i,
                config.tenants,
                config.regions,
                config.services,
                config.shards,
                config.mapping,
            );
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            stats.record_connection_attempt();
            let transport: Box<dyn Transport> = match TransportBuilder::connect_with_retry(
                config.engine.clone(),
                config.connect.clone(),
            )
            .await
            {
                Ok(t) => t,
                Err(e) => {
                    warn!(key = %key, error = %e, "Transport connect error");
                    stats.record_connection_failure();
                    continue; // Skip this publisher but continue with others
                }
            };
            let pub_handle = match transport.create_publisher(&key).await {
                Ok(p) => p,
                Err(e) => {
                    error!(key = %key, error = %e, "Create publisher error");
                    stats.record_connection_failure();
                    continue;
                }
            };
            // Track connection created
            stats.increment_connections();
            let stats_p = stats.clone();
            let rate = config.rate_per_pub;
            let payload_size = config.payload_size;
            let stop_flag = stop.clone();
            let seqs_p = seqs.clone();
            let idx: usize = i as usize;
            handles.push(tokio::spawn(async move {
                let mut rc = rate.map(RateController::new);
                let mut is_active = false;
                loop {
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Some(r) = &mut rc {
                        r.wait_for_next().await;
                    }
                    let seq = seqs_p[idx].fetch_add(1, Ordering::Relaxed);
                    let payload = generate_payload(seq, payload_size);
                    let bytes = Bytes::from(payload);
                    match pub_handle.publish(bytes).await {
                        Ok(_) => {
                            if !is_active {
                                stats_p.increment_active_connections();
                                is_active = true;
                            }
                            stats_p.record_sent().await;
                        }
                        Err(e) => {
                            warn!(key = %key, error = %e, "[multi_topic] send error");
                            stats_p.record_error().await;
                        }
                    }
                }
                // Track connection shutdown
                if is_active {
                    stats_p.decrement_active_connections();
                }
                stats_p.decrement_connections();
                let _ = pub_handle.shutdown().await;
                let _ = transport.shutdown().await;
            }));
        }

        // Run loop: wait for crash or duration
        'run: loop {
            // Check crash condition
            if crash_injector.is_enabled() && crash_injector.should_crash() {
                if crash_injector.consume_crash() {
                    info!("[multi_topic] Crash injection triggered");
                    stats.record_crash_injected();
                    break 'run; // Exit to reconnect
                }
            }

            // Check duration
            if start_time.elapsed().as_secs() >= config.duration_secs {
                break 'reconnect;
            }

            // Check Ctrl+C
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                _ = tokio::signal::ctrl_c() => {
                    stop.store(true, Ordering::Relaxed);
                    break 'reconnect;
                }
            }
        }

        // Crash cleanup: signal all tasks to stop
        info!("[multi_topic] Stopping {} publisher tasks", handles.len());
        stop.store(true, Ordering::Relaxed);
        
        // Wait for all tasks to exit (with timeout)
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            join_all(handles)
        ).await;

        // Exit if duration exceeded
        if start_time.elapsed().as_secs() >= config.duration_secs {
            break 'reconnect;
        }

        // Repair delay
        let repair_time = crash_injector.sample_repair_time();
        info!(
            repair_secs = repair_time.as_secs_f64(),
            "[multi_topic] Simulating repair delay"
        );
        tokio::time::sleep(repair_time).await;

        // Schedule next crash
        crash_injector.schedule_next_crash_after(repair_time);
        stats.record_reconnect();
        info!("[multi_topic] Attempting reconnection after crash");
    }

    // Final cleanup
    if let Some(h) = snapshot_handle {
        h.abort();
    }

    // Final stats
    let final_stats = stats.snapshot().await;
    info!(
        sent = final_stats.sent_count,
        errors = final_stats.error_count,
        total_tps = format!("{:.2}", final_stats.total_throughput()),
        crashes = final_stats.crashes_injected,
        reconnects = final_stats.reconnects,
        "[multi_topic] done"
    );

    Ok(())
}

// ========================= MULTI-TOPIC SUBSCRIBER =========================

pub struct MultiTopicSubConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub topic_prefix: String,
    pub tenants: u32,
    pub regions: u32,
    pub services: u32,
    pub shards: u32,
    pub subscribers: i64, // number of per-key subscriptions (<= T*R*S*K); negative => use total_keys
    pub mapping: KeyMappingMode,
    pub duration_secs: u64,
    pub snapshot_interval_secs: u64,
    pub share_transport: bool, // when true, reuse one transport for all subscriptions
    pub ramp_up_secs: f64, // total ramp-up time in seconds (0 = no delay)
    // Aggregation support
    pub shared_stats: Option<Arc<Stats>>, // when set, aggregate externally
    pub disable_internal_snapshot: bool,
    // Crash injection support
    pub crash_config: CrashConfig,
    /// If true, crash/reconnect each topic independently (per-transport) instead of
    /// crashing the whole multi-topic role at once.
    pub crash_per_topic: bool,
    /// Optional deterministic phase staggering (seconds) applied per topic index.
    /// Effective only when `crash_per_topic=true`.
    pub crash_stagger_secs: f64,
}

use crate::payload::parse_header;
use crate::time_sync::now_unix_ns_estimate;

pub async fn run_multi_topic_sub(config: MultiTopicSubConfig) -> Result<()> {
    // Compute total keys and effective subscriber count
    let total_keys = (config.tenants as u64)
        .saturating_mul(config.regions as u64)
        .saturating_mul(config.services as u64)
        .saturating_mul(config.shards as u64);
    let subs: u64 = if config.subscribers < 0 {
        total_keys
    } else {
        (config.subscribers as u64).min(total_keys)
    };
    info!(
        engine = ?config.engine,
        prefix = %config.topic_prefix,
        tenants = config.tenants,
        regions = config.regions,
        services = config.services,
        shards = config.shards,
        subs = subs,
        duration_secs = config.duration_secs,
        "[multi_topic_sub] starting"
    );

    // Note: shared vs per-subscription transport

    // Stats (use shared aggregator if provided)
    let stats: Arc<Stats> = config
        .shared_stats
        .clone()
        .unwrap_or_else(|| Arc::new(Stats::new()));

    // Optional internal snapshot to stdout when not aggregated
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = stats.clone();
        let every = config.snapshot_interval_secs;
        Some(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(every));
            loop {
                t.tick().await;
                let s = stats_clone.snapshot().await;
                debug!(
                    recv = s.received_count,
                    err = s.error_count,
                    itps = format!("{:.2}", s.total_throughput()),
                    p99_ms = format!("{:.2}", s.latency_ns_p99 as f64 / 1_000_000.0),
                    "[multi_topic_sub] snapshot"
                );
            }
        }))
    } else {
        None
    };

    // Batched stats worker via channel.
    // NOTE: If the channel becomes full, the handler will drop samples; we account
    // that as an error to avoid silently inflating "loss".
    let (tx, rx) = flume::bounded::<(u32, u64, [u8; 24])>(1_000_000);
    let stats_worker = stats.clone();
    
    // Spawn multiple stats workers to parallelize histogram recording if needed
    // But histogram is protected by RwLock, so single writer is better.
    // However, we can optimize the batch size and loop.
    // Per-topic (subscription index) sequence tracking.
    let subs_usize = subs as usize;
    tokio::spawn(async move {
        let mut buf = Vec::with_capacity(4096);
        let mut lats = Vec::with_capacity(4096);
        let mut seq_trackers: Vec<SequenceTracker> = (0..subs_usize)
            .map(|_| SequenceTracker::new())
            .collect();
        let mut batch_counter: u64 = 0;
        loop {
            let first = match rx.recv_async().await {
                Ok(v) => v,
                Err(_) => {
                    // Channel closed: publish final aggregates.
                    let mut dup_sum = 0u64;
                    let mut gap_sum = 0u64;
                    let mut head_sum = 0u64;
                    for tr in &seq_trackers {
                        dup_sum = dup_sum.saturating_add(tr.duplicate_count());
                        gap_sum = gap_sum.saturating_add(tr.gap_count());
                        head_sum = head_sum.saturating_add(tr.head_loss());
                    }
                    stats_worker.set_duplicates(dup_sum);
                    stats_worker.set_gaps(gap_sum);
                    stats_worker.set_head_loss(head_sum);
                    break;
                }
            };
            buf.clear();
            buf.push(first);
            // Drain more aggressively
            while let Ok(v) = rx.try_recv() {
                buf.push(v);
                if buf.len() >= 4096 {
                    break;
                }
            }
            lats.clear();
            for (topic_idx, recv_ns, hdr) in buf.drain(..) {
                if let Ok(h) = parse_header(&hdr) {
                    if (topic_idx as usize) < seq_trackers.len() {
                        seq_trackers[topic_idx as usize].record(h.seq);
                    }
                    lats.push(recv_ns.saturating_sub(h.timestamp_ns));
                }
            }
            stats_worker.record_received_batch(&lats).await;

            // Publish aggregate duplicate/gap/head-loss across topics frequently.
            // (Short runs may only process a handful of batches.)
            batch_counter = batch_counter.wrapping_add(1);
            if batch_counter % 4 == 0 {
                let mut dup_sum = 0u64;
                let mut gap_sum = 0u64;
                let mut head_sum = 0u64;
                for tr in &seq_trackers {
                    dup_sum = dup_sum.saturating_add(tr.duplicate_count());
                    gap_sum = gap_sum.saturating_add(tr.gap_count());
                    head_sum = head_sum.saturating_add(tr.head_loss());
                }
                stats_worker.set_duplicates(dup_sum);
                stats_worker.set_gaps(gap_sum);
                stats_worker.set_head_loss(head_sum);
            }
        }
    });

    // Create per-key subscriptions (subs computed above)

    if config.share_transport {
        info!("[multi_topic_sub] using shared transport");
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
                return Ok(());
            }
        };
        let mut subs_vec: Vec<(Box<dyn crate::transport::Subscription>, Arc<AtomicBool>)> =
            Vec::with_capacity(subs as usize);
        for i in 0..subs {
            let (t, r, s, k) = map_index(
                i,
                config.tenants,
                config.regions,
                config.services,
                config.shards,
                config.mapping,
            );
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let handler_tx = tx.clone();
            let stats_cb = stats.clone();
            let topic_idx: u32 = i as u32;
            let first_received = Arc::new(AtomicBool::new(false));
            let first_received_cb = first_received.clone();
            let sub = transport
                .subscribe(
                    &key,
                    Box::new(move |msg: crate::transport::TransportMessage| {
                        let mut hdr = [0u8; 24];
                        let bytes = msg.payload.as_cow();
                        if bytes.len() >= 24 {
                            hdr.copy_from_slice(&bytes[..24]);
                            let recv = now_unix_ns_estimate();
                            if handler_tx.try_send((topic_idx, recv, hdr)).is_err() {
                                stats_cb.error_count.fetch_add(1, Ordering::Relaxed);
                            }
                            // Track first receive for active connection
                            if !first_received_cb.swap(true, Ordering::Relaxed) {
                                stats_cb.increment_active_connections();
                            }
                        }
                    }),
                )
                .await
                .map_err(|e| anyhow::Error::msg(format!("subscribe error on {}: {}", key, e)))?;
            // Track connection created
            stats.increment_connections();
            subs_vec.push((sub, first_received));
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        for (s, first_received) in &subs_vec {
            let _ = s.shutdown().await;
            // Track connection shutdown
            if first_received.load(Ordering::Relaxed) {
                stats.decrement_active_connections();
            }
            stats.decrement_connections();
        }
        transport
            .shutdown()
            .await
            .map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;
        if let Some(h) = snapshot_handle {
            h.abort();
        }
        let s = stats.snapshot().await;
        info!(
            recv = s.received_count,
            errors = s.error_count,
            total_tps = format!("{:.2}", s.total_throughput()),
            p99_ms = format!("{:.2}", s.latency_ns_p99 as f64 / 1_000_000.0),
            "[multi_topic_sub] done"
        );
        return Ok(());
    }

    // Per-subscription transports
    // Early exit for shared transport mode if crash injection enabled
    if config.share_transport {
        if config.crash_config.is_enabled() {
            warn!(
                "[multi_topic_sub] Crash injection not supported with share_transport=true. \
                 Use share_transport=false for crash testing."
            );
        }
        // ... existing shared transport logic continues unchanged ...
        // (The code above handles shared transport mode without crashes)
    }

    // Per-topic transport mode with crash injection support
    // Calculate per-connection delay from total ramp-up time
    let ramp_delay_us = if config.ramp_up_secs > 0.0 && subs > 1 {
        ((config.ramp_up_secs * 1_000_000.0) / (subs - 1) as f64) as u64
    } else {
        0
    };
    info!(
        ramp_up_secs = config.ramp_up_secs,
        delay_per_conn_us = ramp_delay_us,
        crash_enabled = config.crash_config.is_enabled(),
        "[multi_topic_sub] using per-key transports"
    );

    // Derive a stable base seed for per-topic crash injectors.
    let crash_seed_base: Option<u64> = if config.crash_config.is_enabled() {
        Some(config.crash_config.seed.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(12345)
        }))
    } else {
        None
    };

    let start_time = std::time::Instant::now();

    // Per-topic crash/reconnect (independent schedules).
    if config.crash_config.is_enabled() && config.crash_per_topic {
        info!(
            stagger_secs = config.crash_stagger_secs,
            "[multi_topic_sub] crash scope: per-topic"
        );

        // Spawn one task per subscription; each task owns its transport + subscription.
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(subs as usize);

        for i in 0..subs {
            if i > 0 && ramp_delay_us > 0 {
                tokio::time::sleep(Duration::from_micros(ramp_delay_us)).await;
            }
            let (t, r, s, k) = map_index(
                i,
                config.tenants,
                config.regions,
                config.services,
                config.shards,
                config.mapping,
            );
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);

            let engine = config.engine.clone();
            let connect = config.connect.clone();
            let handler_tx = tx.clone();
            let stats_cb = stats.clone();
            let stop_flag = stop.clone();
            let start = start_time;
            let duration_secs = config.duration_secs;
            let topic_idx: u32 = i as u32;
            let first_received = Arc::new(AtomicBool::new(false));
            let first_received_cb = first_received.clone();

            let mut crash_cfg = config.crash_config.clone();
            if let Some(base) = crash_seed_base {
                crash_cfg.seed = Some(derive_topic_seed(base, i));
            }
            let stagger_secs = config.crash_stagger_secs;

            handles.push(tokio::spawn(async move {
                let mut crash_injector = CrashInjector::new(crash_cfg);
                if stagger_secs > 0.0 {
                    crash_injector.apply_phase_offset(Duration::from_secs_f64(stagger_secs * (i as f64)));
                }

                let mut transport: Option<Box<dyn Transport>> = None;
                let mut sub: Option<Box<dyn crate::transport::Subscription>> = None;

                while !stop_flag.load(Ordering::Relaxed) && start.elapsed().as_secs() < duration_secs {
                    // Ensure connected + subscribed
                    if transport.is_none() || sub.is_none() {
                        stats_cb.record_connection_attempt();
                        match TransportBuilder::connect_with_retry(engine.clone(), connect.clone()).await {
                            Ok(t) => {
                                let handler_tx2 = handler_tx.clone();
                                let stats_cb2 = stats_cb.clone();
                                let first_received_cb2 = first_received_cb.clone();
                                match t
                                    .subscribe(
                                        &key,
                                        Box::new(move |msg: crate::transport::TransportMessage| {
                                            let mut hdr = [0u8; 24];
                                            let bytes = msg.payload.as_cow();
                                            if bytes.len() >= 24 {
                                                hdr.copy_from_slice(&bytes[..24]);
                                                let recv = now_unix_ns_estimate();
                                                if handler_tx2.try_send((topic_idx, recv, hdr)).is_err() {
                                                    stats_cb2.error_count.fetch_add(1, Ordering::Relaxed);
                                                }
                                                if !first_received_cb2.swap(true, Ordering::Relaxed) {
                                                    stats_cb2.increment_active_connections();
                                                }
                                            }
                                        }),
                                    )
                                    .await
                                {
                                    Ok(s) => {
                                        stats_cb.increment_connections();
                                        transport = Some(t);
                                        sub = Some(s);
                                    }
                                    Err(e) => {
                                        warn!(key = %key, error = %e, "Subscribe error");
                                        stats_cb.record_connection_failure();
                                        let _ = t.shutdown().await;
                                        tokio::time::sleep(Duration::from_millis(250)).await;
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(key = %key, error = %e, "Transport connect error");
                                stats_cb.record_connection_failure();
                                tokio::time::sleep(Duration::from_millis(250)).await;
                                continue;
                            }
                        }
                    }

                    // Crash check
                    if crash_injector.is_enabled() && crash_injector.should_crash() {
                        if crash_injector.consume_crash() {
                            info!(key = %key, "[multi_topic_sub] Topic crash injected");
                            stats_cb.record_crash_injected();

                            if let Some(s) = sub.take() {
                                let _ = s.force_disconnect().await;
                                if first_received.load(Ordering::Relaxed) {
                                    stats_cb.decrement_active_connections();
                                }
                                stats_cb.decrement_connections();
                            }
                            if let Some(t) = transport.take() {
                                let _ = t.force_disconnect().await;
                            }

                            let repair_time = crash_injector.sample_repair_time();
                            tokio::time::sleep(repair_time).await;
                            crash_injector.schedule_next_crash_after(repair_time);
                            stats_cb.record_reconnect();
                            continue;
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(50)).await;
                }

                // Cleanup
                if let Some(s) = sub.take() {
                    let _ = s.shutdown().await;
                    if first_received.load(Ordering::Relaxed) {
                        stats_cb.decrement_active_connections();
                    }
                    stats_cb.decrement_connections();
                }
                if let Some(t) = transport.take() {
                    let _ = t.shutdown().await;
                }
            }));
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        stop.store(true, Ordering::Relaxed);
        let _ = join_all(handles).await;

        if let Some(h) = snapshot_handle {
            h.abort();
        }
        let s = stats.snapshot().await;
        info!(
            recv = s.received_count,
            errors = s.error_count,
            total_tps = format!("{:.2}", s.total_throughput()),
            p99_ms = format!("{:.2}", s.latency_ns_p99 as f64 / 1_000_000.0),
            crashes = s.crashes_injected,
            reconnects = s.reconnects,
            "[multi_topic_sub] done"
        );
        return Ok(());
    }

    // Crash injection setup (process-wide crash/reconnect)
    let mut crash_injector = CrashInjector::new(config.crash_config.clone());

    'reconnect: loop {
        // Check duration limit
        if start_time.elapsed().as_secs() >= config.duration_secs {
            break;
        }

        // Hold both the subscription and its own transport to keep the client alive
        let mut clients: Vec<(Box<dyn crate::transport::Subscription>, Box<dyn Transport>, Arc<AtomicBool>)> =
            Vec::with_capacity(subs as usize);
        
        // Create all subscriptions
        for i in 0..subs {
            // Apply ramp-up delay between connections (skip first, reset on reconnect)
            if i > 0 && ramp_delay_us > 0 {
                tokio::time::sleep(Duration::from_micros(ramp_delay_us)).await;
            }
            let (t, r, s, k) = map_index(
                i,
                config.tenants,
                config.regions,
                config.services,
                config.shards,
                config.mapping,
            );
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let handler_tx = tx.clone();
            let stats_cb = stats.clone();
            let topic_idx: u32 = i as u32;
            let first_received = Arc::new(AtomicBool::new(false));
            let first_received_cb = first_received.clone();
            stats.record_connection_attempt();
            let transport: Box<dyn Transport> = match TransportBuilder::connect_with_retry(
                config.engine.clone(),
                config.connect.clone(),
            )
            .await
            {
                Ok(t) => t,
                Err(e) => {
                    warn!(key = %key, error = %e, "Transport connect error");
                    stats.record_connection_failure();
                    continue; // Skip this subscriber but continue with others
                }
            };
            let sub = match transport
                .subscribe(
                    &key,
                    Box::new(move |msg: crate::transport::TransportMessage| {
                        let mut hdr = [0u8; 24];
                        let bytes = msg.payload.as_cow();
                        if bytes.len() >= 24 {
                            hdr.copy_from_slice(&bytes[..24]);
                            let recv = now_unix_ns_estimate();
                            if handler_tx.try_send((topic_idx, recv, hdr)).is_err() {
                                stats_cb.error_count.fetch_add(1, Ordering::Relaxed);
                            }
                            // Track first receive for active connection
                            if !first_received_cb.swap(true, Ordering::Relaxed) {
                                stats_cb.increment_active_connections();
                            }
                        }
                    }),
                )
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    error!(key = %key, error = %e, "Subscribe error");
                    stats.record_connection_failure();
                    continue;
                }
            };
            // Track connection created
            stats.increment_connections();
            clients.push((sub, transport, first_received));
        }

        // Run loop: wait for crash or duration
        'run: loop {
            // Check crash condition
            if crash_injector.is_enabled() && crash_injector.should_crash() {
                if crash_injector.consume_crash() {
                    info!("[multi_topic_sub] Crash injection triggered");
                    stats.record_crash_injected();
                    break 'run; // Exit to reconnect
                }
            }

            // Check duration
            if start_time.elapsed().as_secs() >= config.duration_secs {
                break 'reconnect;
            }

            // Check Ctrl+C
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                _ = tokio::signal::ctrl_c() => {
                    break 'reconnect;
                }
            }
        }

        // Crash cleanup: force disconnect all
        // NOTE: force_disconnect() simulates HARD CRASH (network loss, process kill).
        // This means:
        // - No graceful DISCONNECT packet sent to broker
        // - QoS 1/2 inflight acknowledgments are lost
        // - rumqttc loses all QoS state (PUBACK, PUBREC, PUBREL, PUBCOMP)
        //
        // On reconnect with persistent session (clean_session=false):
        // - Broker expects to resume QoS handshakes
        // - But client has no memory of them → "unsolicited ack" errors
        // - Can cause message loss or connection failures
        //
        // This is EXPECTED BEHAVIOR for hard crash simulation.
        // To avoid this: increase MTTR > session expiry, or use graceful shutdown.
        info!("[multi_topic_sub] Cleaning up {} subscriptions", clients.len());
        for (sub, transport, first_received) in &clients {
            let _ = sub.force_disconnect().await;
            let _ = transport.force_disconnect().await;
            // Track connection shutdown
            if first_received.load(Ordering::Relaxed) {
                stats.decrement_active_connections();
            }
            stats.decrement_connections();
        }
        clients.clear();

        // Exit if duration exceeded
        if start_time.elapsed().as_secs() >= config.duration_secs {
            break 'reconnect;
        }

        // Repair delay
        let repair_time = crash_injector.sample_repair_time();
        info!(
            repair_secs = repair_time.as_secs_f64(),
            "[multi_topic_sub] Simulating repair delay"
        );
        tokio::time::sleep(repair_time).await;

        // Schedule next crash
        crash_injector.schedule_next_crash_after(repair_time);
        stats.record_reconnect();
        info!("[multi_topic_sub] Attempting reconnection after crash");
    }

    // Final cleanup
    if let Some(h) = snapshot_handle {
        h.abort();
    }

    // Final stats
    let s = stats.snapshot().await;
    info!(
        recv = s.received_count,
        errors = s.error_count,
        total_tps = format!("{:.2}", s.total_throughput()),
        p99_ms = format!("{:.2}", s.latency_ns_p99 as f64 / 1_000_000.0),
        crashes = s.crashes_injected,
        reconnects = s.reconnects,
        "[multi_topic_sub] done"
    );

    Ok(())
}
