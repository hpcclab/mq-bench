use anyhow::Result;
use bytes::Bytes;
use futures::future::join_all;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;

use crate::metrics::stats::Stats;
use crate::payload::generate_payload;
use crate::rate::RateController;
use crate::transport::{ConnectOptions, Engine, Transport, TransportBuilder};

#[derive(Clone, Copy, Debug)]
pub enum KeyMappingMode { MDim, Hash }

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
    pub publishers: i64,           // number of logical publishers (<= T*R*S*K); negative => use total_keys
    pub mapping: KeyMappingMode,   // mapping mode from i -> (t,r,s,k)
    pub payload_size: usize,
    pub rate_per_pub: Option<f64>,
    pub duration_secs: u64,
    pub snapshot_interval_secs: u64,
    pub share_transport: bool,     // when true, reuse one transport for all publishers
    // Aggregation support
    pub shared_stats: Option<Arc<Stats>>, // when set, aggregate externally
    pub disable_internal_snapshot: bool,
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

fn map_index(i: u64, t: u32, r: u32, s: u32, k: u32, mode: KeyMappingMode) -> (u32, u32, u32, u32) {
    let t64 = t as u64; let r64 = r as u64; let s64 = s as u64; let k64 = k as u64;
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

    println!(
        "[multi_topic] engine={:?} prefix={} dims=T{}xR{}xS{}xK{} pubs={} payload={} rate={:?} dur={}s",
        config.engine,
        config.topic_prefix,
        config.tenants,
        config.regions,
        config.services,
        config.shards,
        pubs,
        config.payload_size,
        config.rate_per_pub,
        config.duration_secs
    );

    // Shared vs per-key transport depending on config

    // Stats
    let stats: Arc<Stats> = config.shared_stats.clone().unwrap_or_else(|| Arc::new(Stats::new()));

    // Optional internal snapshot
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = stats.clone();
        let every = config.snapshot_interval_secs;
        Some(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(every));
            loop {
                t.tick().await;
                let s = stats_clone.snapshot().await;
                println!(
                    "[multi_topic] sent={} err={} itps={:.2}",
                    s.sent_count,
                    s.error_count,
                    s.interval_throughput()
                );
            }
        }))
    } else { None };

    // Prepare publishers (pubs calculated above)

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(pubs as usize);

    if config.share_transport {
        println!("[multi_topic] using shared transport");
        let transport: Box<dyn Transport> = TransportBuilder::connect(config.engine.clone(), config.connect.clone())
            .await
            .map_err(|e| anyhow::Error::msg(format!("transport connect error: {}", e)))?;
        for i in 0..pubs {
            let (t, r, s, k) = map_index(i, config.tenants, config.regions, config.services, config.shards, config.mapping);
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let pub_handle = transport
                .create_publisher(&key)
                .await
                .map_err(|e| anyhow::Error::msg(format!("create_publisher error ({}): {}", key, e)))?;
            let stats_p = stats.clone();
            let rate = config.rate_per_pub;
            let payload_size = config.payload_size;
            let stop_flag = stop.clone();
            handles.push(tokio::spawn(async move {
                let mut rc = rate.map(RateController::new);
                let mut seq = 0u64;
                loop {
                    if stop_flag.load(Ordering::Relaxed) { break; }
                    if let Some(r) = &mut rc { r.wait_for_next().await; }
                    let payload = generate_payload(seq, payload_size);
                    let bytes = Bytes::from(payload);
                    match pub_handle.publish(bytes).await {
                        Ok(_) => { stats_p.record_sent().await; seq = seq.wrapping_add(1); },
                        Err(e) => { eprintln!("[multi_topic] send error on {}: {}", key, e); stats_p.record_error().await; }
                    }
                }
                let _ = pub_handle.shutdown().await;
            }));
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
        if let Some(h) = snapshot_handle { h.abort(); }
        let final_stats = stats.snapshot().await;
        println!(
            "[multi_topic] done: sent={} errors={} total_tps={:.2}",
            final_stats.sent_count,
            final_stats.error_count,
            final_stats.total_throughput()
        );
        return Ok(());
    } else {
        println!("[multi_topic] using per-key transports");
        for i in 0..pubs {
            let (t, r, s, k) = map_index(i, config.tenants, config.regions, config.services, config.shards, config.mapping);
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let transport: Box<dyn Transport> = TransportBuilder::connect(config.engine.clone(), config.connect.clone())
                .await
                .map_err(|e| anyhow::Error::msg(format!("transport connect error ({}): {}", key, e)))?;
            let pub_handle = transport
                .create_publisher(&key)
                .await
                .map_err(|e| anyhow::Error::msg(format!("create_publisher error ({}): {}", key, e)))?;
            let stats_p = stats.clone();
            let rate = config.rate_per_pub;
            let payload_size = config.payload_size;
            let stop_flag = stop.clone();
            handles.push(tokio::spawn(async move {
                let mut rc = rate.map(RateController::new);
                let mut seq = 0u64;
                loop {
                    if stop_flag.load(Ordering::Relaxed) { break; }
                    if let Some(r) = &mut rc { r.wait_for_next().await; }
                    let payload = generate_payload(seq, payload_size);
                    let bytes = Bytes::from(payload);
                    match pub_handle.publish(bytes).await {
                        Ok(_) => { stats_p.record_sent().await; seq = seq.wrapping_add(1); },
                        Err(e) => { eprintln!("[multi_topic] send error on {}: {}", key, e); stats_p.record_error().await; }
                    }
                }
                let _ = pub_handle.shutdown().await;
                let _ = transport.shutdown().await;
            }));
        }
    }

    // Wait for either duration or Ctrl+C
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
        _ = tokio::signal::ctrl_c() => {},
    }
    stop.store(true, Ordering::Relaxed);

    // Wait for all publisher tasks to exit gracefully
    let _ = join_all(handles).await;

    if let Some(h) = snapshot_handle { h.abort(); }

    // Final stats
    let final_stats = stats.snapshot().await;
    println!(
        "[multi_topic] done: sent={} errors={} total_tps={:.2}",
        final_stats.sent_count,
        final_stats.error_count,
        final_stats.total_throughput()
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
    pub subscribers: i64,            // number of per-key subscriptions (<= T*R*S*K); negative => use total_keys
    pub mapping: KeyMappingMode,
    pub duration_secs: u64,
    pub snapshot_interval_secs: u64,
    pub share_transport: bool,        // when true, reuse one transport for all subscriptions
    // Aggregation support
    pub shared_stats: Option<Arc<Stats>>, // when set, aggregate externally
    pub disable_internal_snapshot: bool,
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
    println!(
        "[multi_topic_sub] engine={:?} prefix={} dims=T{}xR{}xS{}xK{} subs={} dur={}s",
        config.engine,
        config.topic_prefix,
        config.tenants,
        config.regions,
        config.services,
        config.shards,
        subs,
        config.duration_secs
    );

    // Note: shared vs per-subscription transport

    // Stats (use shared aggregator if provided)
    let stats: Arc<Stats> = config.shared_stats.clone().unwrap_or_else(|| Arc::new(Stats::new()));

    // Optional internal snapshot to stdout when not aggregated
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = stats.clone();
        let every = config.snapshot_interval_secs;
        Some(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(every));
            loop {
                t.tick().await;
                let s = stats_clone.snapshot().await;
                println!(
                    "[multi_topic_sub] recv={} err={} itps={:.2} p99={:.2}ms",
                    s.received_count,
                    s.error_count,
                    s.interval_throughput(),
                    s.latency_ns_p99 as f64 / 1_000_000.0
                );
            }
        }))
    } else { None };

    // Batched stats worker via channel
    let (tx, rx) = flume::unbounded::<(u64, [u8; 24])>();
    let stats_worker = stats.clone();
    tokio::spawn(async move {
        let mut buf = Vec::with_capacity(1024);
        loop {
            let first = match rx.recv_async().await { Ok(v) => v, Err(_) => break };
            buf.clear();
            buf.push(first);
            while let Ok(v) = rx.try_recv() {
                buf.push(v);
                if buf.len() >= 2048 { break; }
            }
            let mut lats = Vec::with_capacity(buf.len());
            for (recv_ns, hdr) in buf.drain(..) {
                if let Ok(h) = parse_header(&hdr) { lats.push(recv_ns.saturating_sub(h.timestamp_ns)); }
            }
            stats_worker.record_received_batch(&lats).await;
        }
    });

    // Create per-key subscriptions (subs computed above)

    if config.share_transport {
        println!("[multi_topic_sub] using shared transport");
        let transport: Box<dyn Transport> = TransportBuilder::connect(config.engine.clone(), config.connect.clone())
            .await
            .map_err(|e| anyhow::Error::msg(format!("transport connect error: {}", e)))?;
        let mut subs_vec: Vec<Box<dyn crate::transport::Subscription>> = Vec::with_capacity(subs as usize);
        for i in 0..subs {
            let (t, r, s, k) = map_index(i, config.tenants, config.regions, config.services, config.shards, config.mapping);
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let handler_tx = tx.clone();
            let sub = transport
                .subscribe(&key, Box::new(move |msg: crate::transport::TransportMessage| {
                    let mut hdr = [0u8; 24];
                    let bytes = msg.payload.as_cow();
                    if bytes.len() >= 24 {
                        hdr.copy_from_slice(&bytes[..24]);
                        let recv = now_unix_ns_estimate();
                        let _ = handler_tx.try_send((recv, hdr));
                    }
                }))
                .await
                .map_err(|e| anyhow::Error::msg(format!("subscribe error on {}: {}", key, e)))?;
            subs_vec.push(sub);
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        for s in &subs_vec { let _ = s.shutdown().await; }
        transport
            .shutdown()
            .await
            .map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;
        if let Some(h) = snapshot_handle { h.abort(); }
        let s = stats.snapshot().await;
        println!(
            "[multi_topic_sub] done: recv={} errors={} total_tps={:.2} p99={:.2}ms",
            s.received_count,
            s.error_count,
            s.total_throughput(),
            s.latency_ns_p99 as f64 / 1_000_000.0
        );
        return Ok(());
    }

    // Per-subscription transports
    println!("[multi_topic_sub] using per-key transports");
    // Hold both the subscription and its own transport to keep the client alive
    let mut clients: Vec<(Box<dyn crate::transport::Subscription>, Box<dyn Transport>)> =
        Vec::with_capacity(subs as usize);
    for i in 0..subs {
        let (t, r, s, k) = map_index(i, config.tenants, config.regions, config.services, config.shards, config.mapping);
        let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
        let handler_tx = tx.clone();
        let transport: Box<dyn Transport> = TransportBuilder::connect(config.engine.clone(), config.connect.clone())
            .await
            .map_err(|e| anyhow::Error::msg(format!("transport connect error ({}): {}", key, e)))?;
        let sub = transport
            .subscribe(&key, Box::new(move |msg: crate::transport::TransportMessage| {
                let mut hdr = [0u8; 24];
                let bytes = msg.payload.as_cow();
                if bytes.len() >= 24 {
                    hdr.copy_from_slice(&bytes[..24]);
                    let recv = now_unix_ns_estimate();
                    let _ = handler_tx.try_send((recv, hdr));
                }
            }))
            .await
            .map_err(|e| anyhow::Error::msg(format!("subscribe error on {}: {}", key, e)))?;
        clients.push((sub, transport));
    }

    // Wait for either duration or Ctrl+C
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
        _ = tokio::signal::ctrl_c() => {},
    }

    // Clean up subscriptions and transports
    for (sub, trans) in &clients { let _ = sub.shutdown().await; let _ = trans.shutdown().await; }
    if let Some(h) = snapshot_handle { h.abort(); }

    // Final stats
    let s = stats.snapshot().await;
    println!(
        "[multi_topic_sub] done: recv={} errors={} total_tps={:.2} p99={:.2}ms",
        s.received_count,
        s.error_count,
        s.total_throughput(),
        s.latency_ns_p99 as f64 / 1_000_000.0
    );

    Ok(())
}
