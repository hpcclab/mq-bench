use crate::payload::generate_payload;
use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use anyhow::Result;
use bytes::Bytes;
use crate::transport::{TransportBuilder, Engine, ConnectOptions, IncomingQuery};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::{interval, sleep};

pub struct QueryableConfig {
	pub endpoint: String,
	pub serve_prefix: Vec<String>,
	pub reply_size: usize,
	pub proc_delay_ms: u64,
	pub output_file: Option<String>,
	pub snapshot_interval_secs: u64,
	// Aggregation/external snapshot support
	pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
	pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
}

pub async fn run_queryable(config: QueryableConfig) -> Result<()> {
	println!("Starting queryable:");
	println!("  Endpoint: {}", config.endpoint);
	println!("  Prefixes: {:?}", config.serve_prefix);
	println!("  Reply size: {} bytes", config.reply_size);
	println!("  Processing delay: {} ms", config.proc_delay_ms);

	// Transport session
	let mut opts = ConnectOptions::default();
	opts.params.insert("endpoint".into(), config.endpoint.clone());
	let transport = TransportBuilder::connect(Engine::Zenoh, opts)
		.await
		.map_err(|e| anyhow::Error::msg(format!("transport connect error: {}", e)))?;

	// Stats and output
	let stats = if let Some(s) = &config.shared_stats { s.clone() } else { Arc::new(Stats::new()) };
	let mut output = if let Some(ref path) = config.output_file {
		Some(OutputWriter::new_csv(path.clone()).await?)
	} else if config.shared_stats.is_none() {
		Some(OutputWriter::new_stdout())
	} else {
		None
	};

	// Snapshot task (optional)
	let snapshot_handle = if !config.disable_internal_snapshot {
		let stats_clone = Arc::clone(&stats);
		let interval_secs = config.snapshot_interval_secs;
		Some(tokio::spawn(async move {
			let mut interval_timer = interval(Duration::from_secs(interval_secs));
			loop {
				interval_timer.tick().await;
				let snap = stats_clone.snapshot().await;
				println!("Queryable stats - Served: {}, Errors: {}", snap.sent_count, snap.error_count);
			}
		}))
	} else { None };

	// Prepare a reusable payload buffer to avoid per-reply allocations
	let reply_size = config.reply_size;
	let payload_template = generate_payload(0, reply_size);

	// Register queryables with handler-based API, keep guards alive
	let mut _guards = Vec::new();
	for prefix in &config.serve_prefix {
		let stats_worker = stats.clone();
		let proc_delay = config.proc_delay_ms;
		let payload_template = payload_template.clone();
		let guard = transport
			.register_queryable(prefix, Box::new(move |IncomingQuery { responder, .. }| {
				// Minimal handler: spawn to avoid blocking zenoh callback
				let stats_worker = stats_worker.clone();
				let payload_template = payload_template.clone();
				tokio::spawn(async move {
					if proc_delay > 0 { sleep(Duration::from_millis(proc_delay)).await; }
					let payload = Bytes::from(payload_template.clone());
					if let Err(e) = responder.send(payload).await {
						eprintln!("Queryable reply error: {}", e);
						stats_worker.record_error().await;
					} else {
						stats_worker.record_sent().await;
					}
				});
			}))
			.await
			.map_err(|e| anyhow::Error::msg(format!("register_queryable error: {}", e)))?;
		_guards.push(guard);
	}

	println!("Queryable(s) registered. Waiting for queries...");

	// Wait for Ctrl+C
	tokio::select! {
		_ = signal::ctrl_c() => {
			println!("Ctrl+C received, stopping queryable");
		}
	}

	// Final stats
	let final_stats = stats.snapshot().await;
	println!("\nFinal Queryable Statistics:");
	println!("  Queries served: {}", final_stats.sent_count);
	println!("  Errors: {}", final_stats.error_count);
	if let Some(ref mut out) = output {
		out.write_snapshot(&final_stats).await?;
	}

	if let Some(h) = snapshot_handle { h.abort(); }
	// Shutdown queryable registrations then transport
	for g in _guards { let _ = g.shutdown().await; }
	transport.shutdown().await.map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;
	Ok(())
}
