use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::future::join_all;
use roles::publisher::{PublisherConfig, run_publisher};
use roles::queryable::{QueryableConfig, run_queryable};
use roles::requester::{RequesterConfig, run_requester};
use roles::subscriber::{SubscriberConfig, run_subscriber};
use std::sync::Arc;

mod config;
mod logging;
mod metrics;
mod output;
mod payload;
mod rate;
mod roles;
mod time_sync;
mod transport;

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
        /// Zenoh endpoints to connect to
        #[arg(long, required = true)]
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

        /// Reliability (best/reliable)
        #[arg(long, default_value = "best")]
        reliability: String,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,
    },
    /// Subscriber role
    Sub {
        /// Zenoh endpoints to connect to
        #[arg(long, required = true)]
        endpoint: Vec<String>,

        /// Key expression to subscribe to
        #[arg(long, default_value = "bench/**")]
        expr: String,

        /// Number of subscribers
        #[arg(long, default_value = "1")]
        subscribers: u32,

        /// Reliability (best/reliable)
        #[arg(long, default_value = "best")]
        reliability: String,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,
    },
    /// Requester role
    Req {
        /// Zenoh endpoints to connect to
        #[arg(long, required = true)]
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
    },
    /// Queryable role
    Qry {
        /// Zenoh endpoints to connect to
        #[arg(long, required = true)]
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

        /// Reliability (best/reliable)
        #[arg(long, default_value = "best")]
        reliability: String,

        /// Optional CSV output file path (stdout if omitted)
        #[arg(long)]
        csv: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    logging::init(&cli.log_level)?;

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
            endpoint,
            topic_prefix,
            topics,
            publishers,
            payload,
            rate,
            duration,
            reliability: _reliability,
            csv,
        } => {
            let endpoint = endpoint.first().unwrap().clone();
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
                let cfg = PublisherConfig {
                    endpoint: endpoint.clone(),
                    key_expr,
                    payload_size: payload as usize,
                    rate: match rate { Some(v) if v > 0 => Some(v as f64), _ => None },
                    duration_secs: Some(duration as u64),
                    output_file: None,
                    snapshot_interval_secs: snapshot_interval_secs,
                    shared_stats: shared_stats.clone(),
                    disable_internal_snapshot: true,
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
        Commands::Sub {
            endpoint,
            expr,
            subscribers,
            reliability: _,
            csv,
        } => {
            let endpoint = endpoint.first().unwrap().clone();
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
                let cfg = SubscriberConfig {
                    endpoint: endpoint.clone(),
                    key_expr: expr.clone(),
                    output_file: None,
                    snapshot_interval_secs: snapshot_interval_secs,
                    shared_stats: shared_stats.clone(),
                    disable_internal_snapshot: true,
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
            endpoint,
            key_expr,
            qps,
            concurrency,
            timeout,
            duration,
            csv,
        } => {
            let endpoint = endpoint.first().unwrap().clone();
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
                endpoint,
                key_expr,
                qps: match qps { Some(v) if v > 0 => Some(v as u32), _ => None },
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
            endpoint,
            serve_prefix,
            reply_size,
            proc_delay,
            reliability: _rel,
            csv,
        } => {
            let endpoint = endpoint.first().unwrap().clone();
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
                endpoint,
                serve_prefix,
                reply_size: reply_size as usize,
                proc_delay_ms: proc_delay,
                output_file: None,
                snapshot_interval_secs,
                shared_stats: shared_stats.clone(),
                disable_internal_snapshot: true,
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
    }
}
