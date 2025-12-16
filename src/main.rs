use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::future::join_all;
use mq_bench::crash::CrashConfig;
use mq_bench::metrics::stats::Stats;
use mq_bench::output::OutputWriter;
use mq_bench::roles::multi_topic::{
    KeyMappingMode, MultiTopicConfig, MultiTopicSubConfig, run_multi_topic, run_multi_topic_sub,
};
use mq_bench::roles::publisher::{PublisherConfig, run_publisher};
use mq_bench::roles::queryable::{QueryableConfig, run_queryable};
use mq_bench::roles::reliable_publisher::{ReliablePublisherConfig, run_reliable_publisher};
use mq_bench::roles::requester::{RequesterConfig, run_requester};
use mq_bench::roles::subscriber::{SubscriberConfig, run_subscriber};
use mq_bench::transport::Engine;
use mq_bench::transport::config::{parse_connect_kv, parse_engine};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "mq-bench")]
#[command(about = "Zenoh cluster stress testing harness")]
struct Cli {
    /// Run ID for tagging outputs
    #[arg(long, default_value = "")]
    run_id: String,

    /// Output directory for artifacts
    #[arg(long, default_value = "./artifacts")]
    out_dir: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Snapshot interval in seconds for periodic stats output
    #[arg(long, default_value = "1")]
    snapshot_interval: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Publisher role
    Pub {
        /// Messaging engine (zenoh|mqtt|redis|nats)
        #[arg(long, default_value = "zenoh")]
        engine: String,

        /// Engine connect options as KEY=VALUE (repeatable)
        #[arg(long, value_parser = clap::builder::NonEmptyStringValueParser::new())]
        connect: Vec<String>,

        /// Back-compat: Zenoh endpoints (maps to connect endpoint=...)
        #[arg(long)]
        endpoint: Vec<String>,

        /// Topic prefix
        #[arg(long, default_value = "bench/topic")]
        topic_prefix: String,

        /// Number of topics
        #[arg(long, default_value = "1")]
        topics: u32,

        /// Number of publishers
        #[arg(long, default_value = "1")]
        publishers: u32,

        /// Payload size in bytes
        #[arg(long, default_value = "1024")]
        payload: u32,

        /// Rate per publisher (msg/s). If omitted or <= 0, runs at max speed (no delay)
        #[arg(long, alias = "qps", allow_hyphen_values = true)]
        rate: Option<i32>,

        /// Duration in seconds
        #[arg(long, default_value = "60")]
        duration: u32,

        /// Share a single transport across all publishers (default: false)
        #[arg(long, default_value = "false")]
        share_transport: bool,

        /// QoS level (0,1,2). Mapped per engine; for zenoh: 0=best effort, 1/2=reliable
        #[arg(long, default_value_t = 0u8)]
        qos: u8,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,

        /// Enable connection retry with exponential backoff
        #[arg(long, default_value = "false")]
        enable_retry: bool,

        /// Maximum number of connection retry attempts
        #[arg(long, default_value = "3")]
        retry_count: u32,

        /// Initial delay between retries in milliseconds
        #[arg(long, default_value = "1000")]
        retry_delay: u64,

        /// Mean Time To Failure in seconds (0 = disabled). Crashes are exponentially distributed.
        #[arg(long, default_value = "0")]
        mttf: f64,

        /// Mean Time To Repair in seconds (wait before reconnect attempt)
        #[arg(long, default_value = "5")]
        mttr: f64,

        /// Number of crashes to simulate (0 = infinite until duration ends)
        #[arg(long, default_value = "0")]
        crash_count: u32,

        /// RNG seed for reproducible crash patterns
        #[arg(long)]
        crash_seed: Option<u64>,
    },
    /// Multi-topic publisher (single process, many keys)
    #[command(name = "mt-pub")]
    MtPub {
        /// Messaging engine (zenoh|mqtt|redis|nats)
        #[arg(long, default_value = "zenoh")]
        engine: String,

        /// Engine connect options as KEY=VALUE (repeatable)
        #[arg(long, value_parser = clap::builder::NonEmptyStringValueParser::new())]
        connect: Vec<String>,

        /// Back-compat: Zenoh endpoints (maps to connect endpoint=...)
        #[arg(long)]
        endpoint: Vec<String>,

        /// Topic prefix, e.g., bench/topic
        #[arg(long, default_value = "bench/topic")]
        topic_prefix: String,

        /// Dimensions: tenants, regions, services, shards
        #[arg(long, default_value = "10")]
        tenants: u32,
        #[arg(long, default_value = "2")]
        regions: u32,
        #[arg(long, default_value = "5")]
        services: u32,
        #[arg(long, default_value = "10")]
        shards: u32,

        /// Number of logical publishers (<= T*R*S*K). Negative => use total_keys
        #[arg(long, default_value = "-1", allow_hyphen_values = true)]
        publishers: i64,

        /// Mapping mode (mdim|hash)
        #[arg(long, default_value = "mdim")]
        mapping: String,

        /// Payload size in bytes
        #[arg(long, default_value = "1024")]
        payload: u32,

        /// Rate per publisher (msg/s). If omitted or <= 0, runs at max speed (no delay)
        #[arg(long, alias = "qps", allow_hyphen_values = true)]
        rate: Option<i32>,

        /// Duration in seconds
        #[arg(long, default_value = "60")]
        duration: u32,

        /// Share a single transport across all subscribers (default: false)
        #[arg(long, default_value = "false")]
        share_transport: bool,

        /// Total ramp-up time in seconds to spread connection creation (default: 0 = no delay)
        #[arg(long, default_value = "0")]
        ramp_up_secs: f64,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,

        /// Enable connection retry with exponential backoff
        #[arg(long, default_value = "false")]
        enable_retry: bool,

        /// Maximum number of connection retry attempts
        #[arg(long, default_value = "3")]
        retry_count: u32,

        /// Initial delay between retries in milliseconds
        #[arg(long, default_value = "1000")]
        retry_delay: u64,

        /// Mean Time To Failure in seconds (0 = disabled). Crashes are exponentially distributed.
        #[arg(long, default_value = "0")]
        mttf: f64,

        /// Mean Time To Repair in seconds (wait before reconnect attempt)
        #[arg(long, default_value = "5")]
        mttr: f64,

        /// Number of crashes to simulate (0 = infinite until duration ends)
        #[arg(long, default_value = "0")]
        crash_count: u32,

        /// RNG seed for reproducible crash patterns
        #[arg(long)]
        crash_seed: Option<u64>,
    },
    /// Multi-topic subscriber: spawn many per-key subscriptions
    #[command(name = "mt-sub")]
    MtSub {
        /// Messaging engine (zenoh|mqtt|redis|nats)
        #[arg(long, default_value = "zenoh")]
        engine: String,

        /// Engine connect options as KEY=VALUE (repeatable)
        #[arg(long, value_parser = clap::builder::NonEmptyStringValueParser::new())]
        connect: Vec<String>,

        /// Back-compat: Zenoh endpoints (maps to connect endpoint=...)
        #[arg(long)]
        endpoint: Vec<String>,

        /// Topic prefix, e.g., bench/mtopic
        #[arg(long, default_value = "bench/mtopic")]
        topic_prefix: String,

        /// Dimensions: tenants, regions, services, shards
        #[arg(long, default_value = "10")]
        tenants: u32,
        #[arg(long, default_value = "2")]
        regions: u32,
        #[arg(long, default_value = "5")]
        services: u32,
        #[arg(long, default_value = "10")]
        shards: u32,

        /// Number of per-key subscribers (<= T*R*S*K). Negative => use total_keys
        #[arg(long, default_value = "-1", allow_hyphen_values = true)]
        subscribers: i64,

        /// Mapping mode (mdim|hash)
        #[arg(long, default_value = "hash")]
        mapping: String,

        /// Duration in seconds
        #[arg(long, default_value = "60")]
        duration: u32,

        /// Share a single transport across all subscribers (default: false)
        #[arg(long, default_value = "false")]
        share_transport: bool,

        /// Total ramp-up time in seconds to spread connection creation (default: 0 = no delay)
        #[arg(long, default_value = "0")]
        ramp_up_secs: f64,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,

        /// Enable connection retry with exponential backoff
        #[arg(long, default_value = "false")]
        enable_retry: bool,

        /// Maximum number of connection retry attempts
        #[arg(long, default_value = "3")]
        retry_count: u32,

        /// Initial delay between retries in milliseconds
        #[arg(long, default_value = "1000")]
        retry_delay: u64,

        /// Mean Time To Failure in seconds (0 = disabled). Crashes are exponentially distributed.
        #[arg(long, default_value = "0")]
        mttf: f64,

        /// Mean Time To Repair in seconds (wait before reconnect attempt)
        #[arg(long, default_value = "5")]
        mttr: f64,

        /// Number of crashes to simulate (0 = infinite until duration ends)
        #[arg(long, default_value = "0")]
        crash_count: u32,

        /// RNG seed for reproducible crash patterns
        #[arg(long)]
        crash_seed: Option<u64>,
    },
    /// Subscriber role
    Sub {
        /// Messaging engine (zenoh|mqtt|redis|nats)
        #[arg(long, default_value = "zenoh")]
        engine: String,

        /// Engine connect options as KEY=VALUE (repeatable)
        #[arg(long, value_parser = clap::builder::NonEmptyStringValueParser::new())]
        connect: Vec<String>,

        /// Back-compat: Zenoh endpoints (maps to connect endpoint=...)
        #[arg(long)]
        endpoint: Vec<String>,

        /// Key expression to subscribe to
        #[arg(long, default_value = "bench/**")]
        expr: String,

        /// Number of subscribers
        #[arg(long, default_value = "1")]
        subscribers: u32,

        /// QoS level (0,1,2). Mapped per engine; for zenoh: 0=best effort, 1/2=reliable
        #[arg(long, default_value_t = 0u8)]
        qos: u8,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,

        /// Enable connection retry with exponential backoff
        #[arg(long, default_value = "false")]
        enable_retry: bool,

        /// Maximum number of connection retry attempts
        #[arg(long, default_value = "3")]
        retry_count: u32,

        /// Initial delay between retries in milliseconds
        #[arg(long, default_value = "1000")]
        retry_delay: u64,

        /// Mean Time To Failure in seconds (0 = disabled). Crashes are exponentially distributed.
        #[arg(long, default_value = "0")]
        mttf: f64,

        /// Mean Time To Repair in seconds (wait before reconnect attempt)
        #[arg(long, default_value = "5")]
        mttr: f64,

        /// Number of crashes to simulate (0 = infinite until duration ends)
        #[arg(long, default_value = "0")]
        crash_count: u32,

        /// RNG seed for reproducible crash patterns
        #[arg(long)]
        crash_seed: Option<u64>,
    },
    /// Requester role
    Req {
        /// Messaging engine (zenoh|mqtt|redis|nats)
        #[arg(long, default_value = "zenoh")]
        engine: String,

        /// Engine connect options as KEY=VALUE (repeatable)
        #[arg(long, value_parser = clap::builder::NonEmptyStringValueParser::new())]
        connect: Vec<String>,

        /// Back-compat: Zenoh endpoints (maps to connect endpoint=...)
        #[arg(long)]
        endpoint: Vec<String>,

        /// Key expression for queries
        #[arg(long, required = true)]
        key_expr: String,

        /// Queries per second. If omitted or <= 0, runs at max speed (no delay)
        #[arg(long, alias = "rate", allow_hyphen_values = true)]
        qps: Option<i32>,

        /// In-flight concurrency
        #[arg(long, default_value = "10")]
        concurrency: u32,

        /// Timeout per query (ms)
        #[arg(long, default_value = "5000")]
        timeout: u64,

        /// Duration in seconds
        #[arg(long, default_value = "60")]
        duration: u32,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,

        /// Enable connection retry with exponential backoff
        #[arg(long, default_value = "false")]
        enable_retry: bool,

        /// Maximum number of connection retry attempts
        #[arg(long, default_value = "3")]
        retry_count: u32,

        /// Initial delay between retries in milliseconds
        #[arg(long, default_value = "1000")]
        retry_delay: u64,
    },
    /// Queryable role
    Qry {
        /// Messaging engine (zenoh|mqtt|redis|nats)
        #[arg(long, default_value = "zenoh")]
        engine: String,

        /// Engine connect options as KEY=VALUE (repeatable)
        #[arg(long, value_parser = clap::builder::NonEmptyStringValueParser::new())]
        connect: Vec<String>,

        /// Back-compat: Zenoh endpoints (maps to connect endpoint=...)
        #[arg(long)]
        endpoint: Vec<String>,

        /// Key prefixes to serve
        #[arg(long, required = true)]
        serve_prefix: Vec<String>,

        /// Reply size in bytes
        #[arg(long, default_value = "1024")]
        reply_size: u32,

        /// Processing delay (ms)
        #[arg(long, default_value = "0")]
        proc_delay: u64,

        /// QoS level (0,1,2). Mapped per engine; for zenoh: 0=best effort, 1/2=reliable
        #[arg(long, default_value_t = 0u8)]
        qos: u8,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,

        /// Enable connection retry with exponential backoff
        #[arg(long, default_value = "false")]
        enable_retry: bool,

        /// Maximum number of connection retry attempts
        #[arg(long, default_value = "3")]
        retry_count: u32,

        /// Initial delay between retries in milliseconds
        #[arg(long, default_value = "1000")]
        retry_delay: u64,
    },
    /// Reliable publisher role (MQTT only)
    /// Waits for broker ACK before considering message confirmed.
    /// On crash/reconnect, resumes from last confirmed sequence.
    #[command(name = "rel-pub")]
    RelPub {
        /// Engine connect options as KEY=VALUE (repeatable)
        /// Required: host, port. Optional: qos (default 1), client_id, clean_session
        #[arg(long, value_parser = clap::builder::NonEmptyStringValueParser::new())]
        connect: Vec<String>,

        /// Topic to publish to
        #[arg(long, default_value = "bench/reliable")]
        topic: String,

        /// Payload size in bytes
        #[arg(long, default_value = "1024")]
        payload: u32,

        /// Rate per publisher (msg/s). If omitted or <= 0, runs at max speed
        #[arg(long, alias = "qps", allow_hyphen_values = true)]
        rate: Option<i32>,

        /// Duration in seconds
        #[arg(long, default_value = "60")]
        duration: u32,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,

        /// Enable connection retry with exponential backoff
        #[arg(long, default_value = "true")]
        enable_retry: bool,

        /// Mean Time To Failure in seconds (0 = disabled)
        #[arg(long, default_value = "0")]
        mttf: f64,

        /// Mean Time To Repair in seconds
        #[arg(long, default_value = "5")]
        mttr: f64,

        /// Number of crashes to simulate (0 = infinite)
        #[arg(long, default_value = "0")]
        crash_count: u32,

        /// RNG seed for reproducible crash patterns
        #[arg(long)]
        crash_seed: Option<u64>,

        /// ACK timeout in seconds
        #[arg(long, default_value = "10")]
        ack_timeout: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    mq_bench::logging::init(&cli.log_level)?;

    println!(
        "mq-bench starting with run_id: {}",
        if cli.run_id.is_empty() {
            "auto"
        } else {
            &cli.run_id
        }
    );

    // Capture snapshot interval once (u64 is Copy)
    let snapshot_interval_secs = cli.snapshot_interval;

    match cli.command {
        Commands::Pub {
            engine,
            connect,
            endpoint,
            topic_prefix,
            topics,
            publishers,
            payload,
            rate,
            duration,
            qos,
            csv,
            share_transport: _,
            enable_retry,
            retry_count,
            retry_delay,
            mttf,
            mttr,
            crash_count,
            crash_seed,
        } => {
            // Parse engine and connect opts (support legacy --endpoint)
            let engine = parse_engine(&engine).unwrap_or(Engine::Zenoh);
            let mut conn = parse_connect_kv(&connect);
            if conn.params.is_empty() {
                if let Some(ep) = endpoint.first() {
                    conn.params.insert("endpoint".into(), ep.clone());
                }
            }
            // Wire retry options
            conn.retry_enabled = enable_retry;
            conn.retry_count = retry_count;
            conn.retry_delay_ms = retry_delay;
            conn.retry_max_delay_ms = 30000; // 30s max delay
            // Inject QoS into connect params if not already provided
            conn.params
                .entry("qos".into())
                .or_insert_with(|| qos.to_string());
            let mut handles = Vec::new();
            // Externalize snapshotting always (single or multiple)
            let shared_stats: Option<Arc<Stats>> = Some(Arc::new(Stats::new()));
            let mut agg_output = if let Some(ref path) = csv {
                // Single aggregate file
                Some(OutputWriter::new_csv(path.clone()).await?)
            } else {
                Some(OutputWriter::new_stdout())
            };
            // snapshot_interval_secs from CLI
            let agg_stats_clone = shared_stats.clone();
            let agg_handle = if let Some(stats) = agg_stats_clone.clone() {
                // Spawn aggregate snapshotter to write combined stats
                let mut out = agg_output.take();
                Some(tokio::spawn(async move {
                    let mut t = tokio::time::interval(std::time::Duration::from_secs(
                        snapshot_interval_secs,
                    ));
                    loop {
                        t.tick().await;
                        let snap = stats.snapshot().await;
                        if let Some(ref mut o) = out {
                            let _ = o.write_snapshot(&snap).await;
                        }
                    }
                }))
            } else {
                None
            };
            for i in 0..publishers {
                let key_expr = if topics > 1 {
                    format!("{}/{}", topic_prefix, (i % topics))
                } else {
                    topic_prefix.clone()
                };
                let crash_cfg = mq_bench::CrashConfig {
                    mttf_secs: mttf,
                    mttr_secs: mttr,
                    crash_count,
                    seed: crash_seed,
                };
                let cfg = PublisherConfig {
                    engine: engine.clone(),
                    connect: conn.clone(),
                    key_expr,
                    payload_size: payload as usize,
                    rate: match rate {
                        Some(v) if v > 0 => Some(v as f64),
                        _ => None,
                    },
                    duration_secs: Some(duration as u64),
                    output_file: None,
                    snapshot_interval_secs: snapshot_interval_secs,
                    shared_stats: shared_stats.clone(),
                    disable_internal_snapshot: true,
                    crash_config: crash_cfg,
                };
                handles.push(tokio::spawn(async move {
                    let _ = run_publisher(cfg).await;
                }));
            }
            // Wait for all publishers to finish
            let _ = join_all(handles).await;
            // Write final snapshot once more and cleanup
            if let Some(stats) = shared_stats {
                if let Some(mut out) = agg_output {
                    let snap = stats.snapshot().await;
                    let _ = out.write_snapshot(&snap).await;
                }
            }
            if let Some(h) = agg_handle {
                h.abort();
            }
            Ok(())
        }
        Commands::MtPub {
            engine,
            connect,
            endpoint,
            topic_prefix,
            tenants,
            regions,
            services,
            shards,
            publishers,
            mapping,
            payload,
            rate,
            duration,
            share_transport,
            ramp_up_secs,
            csv,
            enable_retry,
            retry_count,
            retry_delay,
            mttf,
            mttr,
            crash_count,
            crash_seed,
        } => {
            let engine = parse_engine(&engine).unwrap_or(Engine::Zenoh);
            let mut conn = parse_connect_kv(&connect);
            if conn.params.is_empty() {
                if let Some(ep) = endpoint.first() {
                    conn.params.insert("endpoint".into(), ep.clone());
                }
            }
            // Wire retry options
            conn.retry_enabled = enable_retry;
            conn.retry_count = retry_count;
            conn.retry_delay_ms = retry_delay;
            conn.retry_max_delay_ms = 30000;
            // Build crash config
            let crash_cfg = CrashConfig {
                mttf_secs: mttf,
                mttr_secs: mttr,
                crash_count,
                seed: crash_seed,
            };
            let mapping = match mapping.as_str() {
                "mdim" => KeyMappingMode::MDim,
                _ => KeyMappingMode::Hash,
            };

            // Aggregate CSV via shared stats (like pub/sub)
            let shared_stats: Option<Arc<Stats>> = Some(Arc::new(Stats::new()));
            let mut agg_output = if let Some(ref path) = csv {
                Some(OutputWriter::new_csv(path.clone()).await?)
            } else {
                Some(OutputWriter::new_stdout())
            };
            let agg_stats_clone = shared_stats.clone();
            let agg_handle = if let Some(stats) = agg_stats_clone.clone() {
                let mut out = agg_output.take();
                Some(tokio::spawn(async move {
                    let mut t = tokio::time::interval(std::time::Duration::from_secs(
                        snapshot_interval_secs,
                    ));
                    loop {
                        t.tick().await;
                        let snap = stats.snapshot().await;
                        if let Some(ref mut o) = out {
                            let _ = o.write_snapshot(&snap).await;
                        }
                    }
                }))
            } else {
                None
            };

            let cfg = MultiTopicConfig {
                engine: engine.clone(),
                connect: conn.clone(),
                topic_prefix,
                tenants,
                regions,
                services,
                shards,
                publishers,
                mapping,
                payload_size: payload as usize,
                rate_per_pub: match rate {
                    Some(v) if v > 0 => Some(v as f64),
                    _ => None,
                },
                duration_secs: duration as u64,
                snapshot_interval_secs,
                share_transport,
                ramp_up_secs,
                shared_stats: shared_stats.clone(),
                disable_internal_snapshot: true,
                crash_config: crash_cfg,
            };
            run_multi_topic(cfg).await?;
            if let Some(stats) = shared_stats {
                if let Some(mut out) = agg_output {
                    let snap = stats.snapshot().await;
                    let _ = out.write_snapshot(&snap).await;
                }
            }
            if let Some(h) = agg_handle {
                h.abort();
            }
            Ok(())
        }
        Commands::MtSub {
            engine,
            connect,
            endpoint,
            topic_prefix,
            tenants,
            regions,
            services,
            shards,
            subscribers,
            mapping,
            duration,
            share_transport,
            ramp_up_secs,
            csv,
            enable_retry,
            retry_count,
            retry_delay,
            mttf,
            mttr,
            crash_count,
            crash_seed,
        } => {
            let engine = parse_engine(&engine).unwrap_or(Engine::Zenoh);
            let mut conn = parse_connect_kv(&connect);
            if conn.params.is_empty() {
                if let Some(ep) = endpoint.first() {
                    conn.params.insert("endpoint".into(), ep.clone());
                }
            }
            // Wire retry options
            conn.retry_enabled = enable_retry;
            conn.retry_count = retry_count;
            conn.retry_delay_ms = retry_delay;
            conn.retry_max_delay_ms = 30000;
            // Build crash config
            let crash_cfg = CrashConfig {
                mttf_secs: mttf,
                mttr_secs: mttr,
                crash_count,
                seed: crash_seed,
            };
            let mapping = match mapping.as_str() {
                "mdim" => KeyMappingMode::MDim,
                _ => KeyMappingMode::Hash,
            };

            // Aggregate CSV via shared stats
            let shared_stats: Option<Arc<Stats>> = Some(Arc::new(Stats::new()));
            let mut agg_output = if let Some(ref path) = csv {
                Some(OutputWriter::new_csv(path.clone()).await?)
            } else {
                Some(OutputWriter::new_stdout())
            };
            let agg_stats_clone = shared_stats.clone();
            let agg_handle = if let Some(stats) = agg_stats_clone.clone() {
                let mut out = agg_output.take();
                Some(tokio::spawn(async move {
                    let mut t = tokio::time::interval(std::time::Duration::from_secs(
                        snapshot_interval_secs,
                    ));
                    loop {
                        t.tick().await;
                        let snap = stats.snapshot().await;
                        if let Some(ref mut o) = out {
                            let _ = o.write_snapshot(&snap).await;
                        }
                    }
                }))
            } else {
                None
            };

            let cfg = MultiTopicSubConfig {
                engine: engine.clone(),
                connect: conn.clone(),
                topic_prefix,
                tenants,
                regions,
                services,
                shards,
                subscribers,
                mapping,
                duration_secs: duration as u64,
                snapshot_interval_secs,
                share_transport,
                ramp_up_secs,
                shared_stats: shared_stats.clone(),
                disable_internal_snapshot: true,
                crash_config: crash_cfg,
            };
            run_multi_topic_sub(cfg).await?;
            if let Some(stats) = shared_stats {
                if let Some(mut out) = agg_output {
                    let snap = stats.snapshot().await;
                    let _ = out.write_snapshot(&snap).await;
                }
            }
            if let Some(h) = agg_handle {
                h.abort();
            }
            Ok(())
        }
        Commands::Sub {
            engine,
            connect,
            endpoint,
            expr,
            subscribers,
            qos,
            csv,
            enable_retry,
            retry_count,
            retry_delay,
            mttf,
            mttr,
            crash_count,
            crash_seed,
        } => {
            let engine = parse_engine(&engine).unwrap_or(Engine::Zenoh);
            let mut conn = parse_connect_kv(&connect);
            if conn.params.is_empty() {
                if let Some(ep) = endpoint.first() {
                    conn.params.insert("endpoint".into(), ep.clone());
                }
            }
            // Wire retry options
            conn.retry_enabled = enable_retry;
            conn.retry_count = retry_count;
            conn.retry_delay_ms = retry_delay;
            conn.retry_max_delay_ms = 30000;
            // Inject QoS into connect params if not already provided
            conn.params
                .entry("qos".into())
                .or_insert_with(|| qos.to_string());
            let mut handles = Vec::new();
            // Externalize snapshotting always
            let shared_stats: Option<Arc<Stats>> = Some(Arc::new(Stats::new()));
            let mut agg_output = if let Some(ref path) = csv {
                Some(OutputWriter::new_csv(path.clone()).await?)
            } else {
                Some(OutputWriter::new_stdout())
            };
            // snapshot_interval_secs from CLI
            let agg_stats_clone = shared_stats.clone();
            let agg_handle = if let Some(stats) = agg_stats_clone.clone() {
                let mut out = agg_output.take();
                Some(tokio::spawn(async move {
                    let mut t = tokio::time::interval(std::time::Duration::from_secs(
                        snapshot_interval_secs,
                    ));
                    loop {
                        t.tick().await;
                        let snap = stats.snapshot().await;
                        if let Some(ref mut o) = out {
                            let _ = o.write_snapshot(&snap).await;
                        }
                    }
                }))
            } else {
                None
            };
            for _i in 0..subscribers {
                let crash_cfg = mq_bench::CrashConfig {
                    mttf_secs: mttf,
                    mttr_secs: mttr,
                    crash_count,
                    seed: crash_seed,
                };
                let cfg = SubscriberConfig {
                    engine: engine.clone(),
                    connect: conn.clone(),
                    key_expr: expr.clone(),
                    output_file: None,
                    snapshot_interval_secs: snapshot_interval_secs,
                    shared_stats: shared_stats.clone(),
                    disable_internal_snapshot: true,
                    test_stop_after_secs: None,
                    crash_config: crash_cfg,
                };
                handles.push(tokio::spawn(async move {
                    let _ = run_subscriber(cfg).await;
                }));
            }
            let _ = join_all(handles).await;
            // Final snapshot and cleanup
            if let Some(stats) = shared_stats {
                if let Some(mut out) = agg_output {
                    let snap = stats.snapshot().await;
                    let _ = out.write_snapshot(&snap).await;
                }
            }
            if let Some(h) = agg_handle {
                h.abort();
            }
            Ok(())
        }
        Commands::Req {
            engine,
            connect,
            endpoint,
            key_expr,
            qps,
            concurrency,
            timeout,
            duration,
            csv,
            enable_retry,
            retry_count,
            retry_delay,
        } => {
            let engine = parse_engine(&engine).unwrap_or(Engine::Zenoh);
            let mut conn = parse_connect_kv(&connect);
            if conn.params.is_empty() {
                if let Some(ep) = endpoint.first() {
                    conn.params.insert("endpoint".into(), ep.clone());
                }
            }
            // Wire retry options
            conn.retry_enabled = enable_retry;
            conn.retry_count = retry_count;
            conn.retry_delay_ms = retry_delay;
            conn.retry_max_delay_ms = 30000;
            // Externalize snapshotting even for single requester
            let shared_stats: Option<Arc<Stats>> = Some(Arc::new(Stats::new()));
            let mut agg_output = if let Some(ref path) = csv {
                Some(OutputWriter::new_csv(path.clone()).await?)
            } else {
                Some(OutputWriter::new_stdout())
            };
            // snapshot_interval_secs from CLI
            let agg_stats_clone = shared_stats.clone();
            let agg_handle: Option<tokio::task::JoinHandle<()>> =
                if let Some(stats) = agg_stats_clone.clone() {
                    let mut out = agg_output.take();
                    Some(tokio::spawn(async move {
                        let mut t = tokio::time::interval(std::time::Duration::from_secs(
                            snapshot_interval_secs,
                        ));
                        loop {
                            t.tick().await;
                            let snap = stats.snapshot().await;
                            if let Some(ref mut o) = out {
                                let _ = o.write_snapshot(&snap).await;
                            }
                        }
                    }))
                } else {
                    None
                };
            let config = RequesterConfig {
                engine: engine.clone(),
                connect: conn,
                key_expr,
                qps: match qps {
                    Some(v) if v > 0 => Some(v as u32),
                    _ => None,
                },
                concurrency,
                timeout_ms: timeout,
                duration_secs: duration as u64,
                output_file: None,
                snapshot_interval_secs,
                shared_stats: shared_stats.clone(),
                disable_internal_snapshot: true,
            };
            run_requester(config).await?;
            // Final snapshot and cleanup
            if let Some(stats) = shared_stats {
                if let Some(mut out) = agg_output {
                    let snap = stats.snapshot().await;
                    let _ = out.write_snapshot(&snap).await;
                }
            }
            if let Some(h) = agg_handle {
                h.abort();
            }
            Ok(())
        }
        Commands::Qry {
            engine,
            connect,
            endpoint,
            serve_prefix,
            reply_size,
            proc_delay,
            qos,
            csv,
            enable_retry,
            retry_count,
            retry_delay,
        } => {
            let engine = parse_engine(&engine).unwrap_or(Engine::Zenoh);
            let mut conn = parse_connect_kv(&connect);
            if conn.params.is_empty() {
                if let Some(ep) = endpoint.first() {
                    conn.params.insert("endpoint".into(), ep.clone());
                }
            }
            // Wire retry options
            conn.retry_enabled = enable_retry;
            conn.retry_count = retry_count;
            conn.retry_delay_ms = retry_delay;
            conn.retry_max_delay_ms = 30000;
            // Inject QoS into connect params if not already provided
            conn.params
                .entry("qos".into())
                .or_insert_with(|| qos.to_string());
            // Externalize snapshotting
            let shared_stats: Option<Arc<Stats>> = Some(Arc::new(Stats::new()));
            let mut agg_output = if let Some(ref path) = csv {
                Some(OutputWriter::new_csv(path.clone()).await?)
            } else {
                Some(OutputWriter::new_stdout())
            };
            // snapshot_interval_secs from CLI
            let agg_stats_clone = shared_stats.clone();
            let agg_handle: Option<tokio::task::JoinHandle<()>> =
                if let Some(stats) = agg_stats_clone.clone() {
                    let mut out = agg_output.take();
                    Some(tokio::spawn(async move {
                        let mut t = tokio::time::interval(std::time::Duration::from_secs(
                            snapshot_interval_secs,
                        ));
                        loop {
                            t.tick().await;
                            let snap = stats.snapshot().await;
                            if let Some(ref mut o) = out {
                                let _ = o.write_snapshot(&snap).await;
                            }
                        }
                    }))
                } else {
                    None
                };
            let config = QueryableConfig {
                engine: engine.clone(),
                connect: conn,
                serve_prefix,
                reply_size: reply_size as usize,
                proc_delay_ms: proc_delay,
                output_file: None,
                snapshot_interval_secs,
                shared_stats: shared_stats.clone(),
                disable_internal_snapshot: true,
                test_stop_after_secs: None,
            };
            run_queryable(config).await?;
            if let Some(stats) = shared_stats {
                if let Some(mut out) = agg_output {
                    let snap = stats.snapshot().await;
                    let _ = out.write_snapshot(&snap).await;
                }
            }
            if let Some(h) = agg_handle {
                h.abort();
            }
            Ok(())
        }
        Commands::RelPub {
            connect,
            topic,
            payload,
            rate,
            duration,
            csv,
            enable_retry,
            mttf,
            mttr,
            crash_count,
            crash_seed,
            ack_timeout,
        } => {
            use mq_bench::crash::CrashConfig;

            let mut conn = parse_connect_kv(&connect);
            // Default retry enabled for reliable publishing
            conn.retry_enabled = enable_retry;
            conn.retry_count = 10; // More retries for reliable publishing
            conn.retry_delay_ms = 1000;
            conn.retry_max_delay_ms = 30000;

            // Setup crash config
            let crash_config = CrashConfig {
                mttf_secs: mttf,
                mttr_secs: mttr,
                crash_count,
                seed: crash_seed,
            };

            // Setup output
            if let Some(ref path) = csv {
                println!("Writing CSV output to: {}", path);
            }

            let config = ReliablePublisherConfig {
                engine: Engine::Mqtt, // Only MQTT supported
                connect: conn,
                key_expr: topic,
                payload_size: payload as usize,
                rate: rate.filter(|&r| r > 0).map(|r| r as f64),
                duration_secs: Some(duration as u64),
                output_file: csv,
                snapshot_interval_secs,
                shared_stats: None,
                disable_internal_snapshot: false,
                crash_config,
                ack_timeout_secs: ack_timeout,
            };

            run_reliable_publisher(config).await?;
            Ok(())
        }
    }
}
