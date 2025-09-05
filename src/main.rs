use clap::{Parser, Subcommand};
use anyhow::Result;
use roles::publisher::{run_publisher, PublisherConfig};
use roles::subscriber::{run_subscriber, SubscriberConfig};

mod roles;
mod payload;
mod rate;
mod metrics;
mod output;
mod time_sync;
mod config;
mod logging;

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
        
        /// Rate per publisher (msg/s)
        #[arg(long, default_value = "1000")]
        rate: u32,
        
        /// Duration in seconds
        #[arg(long, default_value = "60")]
        duration: u32,
        
        /// Reliability (best/reliable)
        #[arg(long, default_value = "best")]
        reliability: String,
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
    },
    /// Requester role
    Req {
        /// Zenoh endpoints to connect to
        #[arg(long, required = true)]
        endpoint: Vec<String>,
        
        /// Key expression for queries
        #[arg(long, required = true)]
        key_expr: String,
        
        /// Queries per second
        #[arg(long, default_value = "100")]
        qps: u32,
        
        /// In-flight concurrency
        #[arg(long, default_value = "10")]
        concurrency: u32,
        
        /// Timeout per query (ms)
        #[arg(long, default_value = "5000")]
        timeout: u64,
        
        /// Duration in seconds
        #[arg(long, default_value = "60")]
        duration: u32,
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
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Initialize logging
    logging::init(&cli.log_level)?;
    
    println!("mq-bench starting with run_id: {}", 
        if cli.run_id.is_empty() { "auto" } else { &cli.run_id });
    
    match cli.command {
        Commands::Pub { endpoint, topic_prefix, topics, publishers, payload, rate, duration, reliability } => {
            // For now, just run single publisher with first endpoint
            let config = PublisherConfig {
                endpoint: endpoint.first().unwrap().clone(),
                key_expr: topic_prefix,
                payload_size: payload as usize,
                rate: rate as f64,
                duration_secs: Some(duration as u64),
                output_file: None, // TODO: Add output file option
                snapshot_interval_secs: 5,
            };
            run_publisher(config).await?;
            Ok(())
        }
        Commands::Sub { endpoint, expr, subscribers: _, reliability: _ } => {
            // For now, just run single subscriber with first endpoint  
            let config = SubscriberConfig {
                endpoint: endpoint.first().unwrap().clone(),
                key_expr: expr,
                output_file: None, // TODO: Add output file option
                snapshot_interval_secs: 5,
            };
            run_subscriber(config).await?;
            Ok(())
        }
        Commands::Req { .. } => {
            println!("Requester role - not implemented yet");
            Ok(())
        }
        Commands::Qry { .. } => {
            println!("Queryable role - not implemented yet");
            Ok(())
        }
    }
}
