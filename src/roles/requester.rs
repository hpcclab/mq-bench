use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use crate::rate::RateController;
use anyhow::Result;
use zenoh::query::ConsolidationMode;
use std::sync::Arc;
use std::pin::Pin;
use futures::Future;
use std::time::{Duration, Instant};
// use tokio::signal;
use tokio::time::interval;
use futures::stream::{FuturesUnordered, StreamExt};
use flume;

pub struct RequesterConfig {
	pub endpoint: String,
	pub key_expr: String,
	pub qps: Option<u32>,
	pub concurrency: u32,
	pub timeout_ms: u64,
	pub duration_secs: u64,
	pub output_file: Option<String>,
	pub snapshot_interval_secs: u64,
	// Aggregation/external snapshot support
	pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
	pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
}

pub async fn run_requester(config: RequesterConfig) -> Result<()> {
	println!("Starting requester:");
	println!("  Endpoint: {}", config.endpoint);
	println!("  Key expr: {}", config.key_expr);
	if let Some(q) = config.qps { println!("  QPS: {}", q); } else { println!("  QPS: unlimited (no delay)"); }
	println!("  Concurrency: {}", config.concurrency);
	println!("  Timeout: {} ms", config.timeout_ms);
	println!("  Duration: {} s", config.duration_secs);

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

	// Stats worker channel: batch updates to avoid contention from many tasks
	#[derive(Clone, Copy)]
	enum Ev { Sent, Recv(u64), Err }
	let (tx, rx) = flume::bounded::<Ev>(10_000);
	{
		let stats_worker = stats.clone();
		tokio::spawn(async move {
			while let Ok(ev) = rx.recv_async().await {
				match ev {
					Ev::Sent => stats_worker.record_sent().await,
					Ev::Recv(ns) => stats_worker.record_received(ns).await,
					Ev::Err => stats_worker.record_error().await,
				}
			}
		});
	}

	// Snapshot task (optional)
	let snapshot_handle = if !config.disable_internal_snapshot {
		let stats_clone = Arc::clone(&stats);
		let interval_secs = config.snapshot_interval_secs;
		Some(tokio::spawn(async move {
			let mut interval_timer = interval(Duration::from_secs(interval_secs));
			loop {
				interval_timer.tick().await;
				let snap = stats_clone.snapshot().await;
				println!(
					"Requester stats - Sent: {}, Received: {}, Errors: {}",
					snap.sent_count, snap.received_count, snap.error_count
				);
			}
		}))
	} else { None };

	// Query loop
	let start = Instant::now();
	let mut rate = config.qps.map(|q| RateController::new(q as f64));
	let mut inflight: FuturesUnordered<Pin<Box<dyn Future<Output = Result<(Option<Duration>, Duration, u32), ()>> + Send>>> = FuturesUnordered::new();
	let mut total_sent = 0u64;
	let mut total_recv = 0u64;

	loop {
		// Check duration
		if start.elapsed().as_secs() >= config.duration_secs {
			println!("Duration limit reached, stopping requester");
			break;
		}

		// Maintain concurrency
		while inflight.len() < config.concurrency as usize {
			if let Some(rc) = &mut rate { rc.wait_for_next().await; }
			let session = session.clone();
			let key_expr = config.key_expr.clone();
			let tx_ev = tx.clone();
			let timeout_ms = config.timeout_ms;
			let fut: Pin<Box<dyn Future<Output = Result<(Option<Duration>, Duration, u32), ()>> + Send>> = Box::pin(async move {
				let t0 = Instant::now();
				// Use handler-based API with a flume channel to receive replies efficiently
				let res = session
					.get(&key_expr)
					.congestion_control(zenoh::qos::CongestionControl::Block)
					.consolidation(ConsolidationMode::None)
					.timeout(Duration::from_millis(timeout_ms))
					.with(flume::bounded(64))
					.await;
				match res {
					Ok(replies) => {
						let mut t_first: Option<Duration> = None;
						let mut t_last: Duration = Duration::from_nanos(0);
						let mut n = 0u32;
						while let Ok(_reply) = replies.recv_async().await {
							let now = t0.elapsed();
							if t_first.is_none() { t_first = Some(now); }
							t_last = now;
							n += 1;
						}
						let _ = tx_ev.try_send(Ev::Sent); // query sent
						if let Some(ttf) = t_first { let _ = tx_ev.try_send(Ev::Recv(ttf.as_nanos() as u64)); }
						Ok((t_first, t_last, n))
					}
					Err(e) => { eprintln!("Requester query error: {}", e); let _ = tx_ev.try_send(Ev::Err); Err(()) }
				}
			});
			inflight.push(fut);
			total_sent += 1;
		}

		// Poll for finished queries
		if let Some(_res) = inflight.next().await {
			total_recv += 1;
		}
	}

	// Drain remaining inflight
	while let Some(_res) = inflight.next().await {
		total_recv += 1;
	}

	// Final stats
	let final_stats = stats.snapshot().await;
	println!("\nFinal Requester Statistics:");
	println!("  Queries sent: {}", total_sent);
	println!("  Queries completed: {}", total_recv);
	println!("  Errors: {}", final_stats.error_count);
	if let Some(ref mut out) = output {
		out.write_snapshot(&final_stats).await?;
	}

	if let Some(h) = snapshot_handle { h.abort(); }
	session.close().await.map_err(|e| anyhow::Error::msg(e.to_string()))?;
	Ok(())
}
