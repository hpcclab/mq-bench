use crate::payload::generate_payload;
use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::{interval, sleep};
use flume;

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

	// Zenoh session
	let mut zenoh_config = zenoh::config::Config::default();
	zenoh_config.insert_json5("mode", "\"client\"").map_err(|e| anyhow::Error::msg(e.to_string()))?;
	zenoh_config.insert_json5("connect/endpoints", &format!("[\"{}\"]", config.endpoint)).map_err(|e| anyhow::Error::msg(e.to_string()))?;
	zenoh_config.insert_json5("scouting/multicast/enabled", "false").map_err(|e| anyhow::Error::msg(e.to_string()))?;
	zenoh_config.insert_json5("scouting/gossip/enabled", "false").map_err(|e| anyhow::Error::msg(e.to_string()))?;
	let session = zenoh::open(zenoh_config).await.map_err(|e| anyhow::Error::msg(e.to_string()))?;

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
	let cached_payload = generate_payload(0, reply_size);
	// Worker channel to process queries without per-query task spawns
	let (tx, rx) = flume::bounded::<zenoh::query::Query>(10_000);
	{
		let stats_worker = stats.clone();
		// Use the cached payload inside the worker; clone per reply to hand to zenoh
		let proc_delay = config.proc_delay_ms;
		let payload_template = cached_payload.clone();
		tokio::spawn(async move {
			while let Ok(query) = rx.recv_async().await {
				if proc_delay > 0 { sleep(Duration::from_millis(proc_delay)).await; }
				let payload = payload_template.clone();
				if let Err(e) = query.reply(query.key_expr(), payload).await {
					eprintln!("Queryable reply error: {}", e);
					stats_worker.record_error().await;
				} else {
					stats_worker.record_sent().await;
				}
			}
		});
	}

	// Register queryables and keep them alive
	let mut _queryables: Vec<_> = Vec::new();
	for prefix in &config.serve_prefix {
		let tx_cb = tx.clone();
		let q = session
			.declare_queryable(prefix)
			.callback(move |query| {
				// Try to enqueue; on backpressure, drop to keep callback cheap
				let _ = tx_cb.try_send(query);
			})
			.await
			.map_err(|e| anyhow::Error::msg(e.to_string()))?;
		_queryables.push(q);
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
	session.close().await.map_err(|e| anyhow::Error::msg(e.to_string()))?;
	Ok(())
}
