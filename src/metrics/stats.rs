use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Statistics collector for latency and throughput
pub struct Stats {
    // Latency histogram (nanosecond precision)
    latency_hist: RwLock<Histogram<u64>>,

    // Counters
    pub sent_count: AtomicU64,
    pub received_count: AtomicU64,
    pub error_count: AtomicU64,

    // Connection counters
    pub connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub connection_attempts: AtomicU64,
    pub connection_failures: AtomicU64,

    // Crash/recovery counters
    pub crashes_injected: AtomicU64,
    pub reconnects: AtomicU64,
    pub reconnect_failures: AtomicU64,

    // Timing
    start_time: Instant,
    last_snapshot: RwLock<Instant>,

    // Last values captured at previous snapshot to compute deltas
    last_sent_count: RwLock<u64>,
    last_received_count: RwLock<u64>,

    // First activity times
    first_sent_time: RwLock<Option<Instant>>,
    first_received_time: RwLock<Option<Instant>>,

    // Duplicate/gap counters (reported by clients with local SequenceTracker)
    duplicate_count: AtomicU64,
    gap_count: AtomicU64,
    // Head loss: messages lost before first received (min_seq > 0)
    head_loss: AtomicU64,
}

impl Stats {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            // 1ns to 60s range, 3 significant digits
            latency_hist: RwLock::new(Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()),
            sent_count: AtomicU64::new(0),
            received_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            connections: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            connection_attempts: AtomicU64::new(0),
            connection_failures: AtomicU64::new(0),
            crashes_injected: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            reconnect_failures: AtomicU64::new(0),
            start_time: now,
            last_snapshot: RwLock::new(now),
            last_sent_count: RwLock::new(0),
            last_received_count: RwLock::new(0),
            first_sent_time: RwLock::new(None),
            first_received_time: RwLock::new(None),
            duplicate_count: AtomicU64::new(0),
            gap_count: AtomicU64::new(0),
            head_loss: AtomicU64::new(0),
        }
    }

    /// Record a sent message
    pub async fn record_sent(&self) {
        self.sent_count.fetch_add(1, Ordering::Relaxed);
        let mut first = self.first_sent_time.write().await;
        if first.is_none() {
            *first = Some(Instant::now());
        }
    }

    /// Record a received message with latency
    pub async fn record_received(&self, latency_ns: u64) {
        self.received_count.fetch_add(1, Ordering::Relaxed);
        let mut first = self.first_received_time.write().await;
        if first.is_none() {
            *first = Some(Instant::now());
        }

        if let Ok(mut hist) = self.latency_hist.try_write() {
            let _ = hist.record(latency_ns);
        }
    }

    /// Record an error
    pub async fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment connection count (called when publisher/subscriber is created)
    pub fn increment_connections(&self) {
        self.connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement connection count (called on shutdown)
    pub fn decrement_connections(&self) {
        self.connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment active connection count (called on first successful publish/receive)
    pub fn increment_active_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connection count (called on shutdown or error)
    pub fn decrement_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a connection attempt
    pub fn record_connection_attempt(&self) {
        self.connection_attempts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a connection failure
    pub fn record_connection_failure(&self) {
        self.connection_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an injected crash (simulated failure)
    pub fn record_crash_injected(&self) {
        self.crashes_injected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful reconnection after crash
    pub fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed reconnection attempt
    pub fn record_reconnect_failure(&self) {
        self.reconnect_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record duplicates detected by a client's local SequenceTracker
    pub fn record_duplicates(&self, count: u64) {
        self.duplicate_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Record gaps detected by a client's local SequenceTracker
    pub fn record_gaps(&self, count: u64) {
        self.gap_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Set the absolute duplicate count (for periodic updates from SequenceTracker)
    pub fn set_duplicates(&self, count: u64) {
        self.duplicate_count.store(count, Ordering::Relaxed);
    }

    /// Set the absolute gap count (for periodic updates from SequenceTracker)
    pub fn set_gaps(&self, count: u64) {
        self.gap_count.store(count, Ordering::Relaxed);
    }

    /// Set head loss count (messages lost before first received message)
    pub fn set_head_loss(&self, count: u64) {
        self.head_loss.store(count, Ordering::Relaxed);
    }

    /// Record a batch of received latencies with minimal locking
    pub async fn record_received_batch(&self, latencies_ns: &[u64]) {
        if latencies_ns.is_empty() {
            return;
        }
        // Bump received count once
        self.received_count
            .fetch_add(latencies_ns.len() as u64, Ordering::Relaxed);
        // Set first_received_time if unset (double-checked locking optimization)
        if self.first_received_time.read().await.is_none() {
            let mut first = self.first_received_time.write().await;
            if first.is_none() {
                *first = Some(Instant::now());
            }
        }
        // Record all latencies under a single histogram write lock
        let mut hist = self.latency_hist.write().await;
        for &lat in latencies_ns {
            let _ = hist.record(lat);
        }
    }

    /// Get current snapshot of statistics
    pub async fn snapshot(&self) -> StatsSnapshot {
        let now = Instant::now();
        let sent = self.sent_count.load(Ordering::Relaxed);
        let received = self.received_count.load(Ordering::Relaxed);
        let errors = self.error_count.load(Ordering::Relaxed);
        let conns = self.connections.load(Ordering::Relaxed);
        let active_conns = self.active_connections.load(Ordering::Relaxed);
        let conn_attempts = self.connection_attempts.load(Ordering::Relaxed);
        let conn_failures = self.connection_failures.load(Ordering::Relaxed);
        let crashes = self.crashes_injected.load(Ordering::Relaxed);
        let reconnects = self.reconnects.load(Ordering::Relaxed);
        let reconnect_failures = self.reconnect_failures.load(Ordering::Relaxed);
        let duplicates = self.duplicate_count.load(Ordering::Relaxed);
        let gaps = self.gap_count.load(Ordering::Relaxed);
        let head_loss = self.head_loss.load(Ordering::Relaxed);

        let hist = self.latency_hist.read().await;
        let p50 = hist.value_at_quantile(0.5);
        let p95 = hist.value_at_quantile(0.95);
        let p99 = hist.value_at_quantile(0.99);
        let min = hist.min();
        let max = hist.max();
        let mean = hist.mean();

        let total_elapsed = now.duration_since(self.start_time);
        let since_last = {
            let mut last = self.last_snapshot.write().await;
            let duration = now.duration_since(*last);
            *last = now;
            duration
        };
        let interval_sent = {
            let mut last = self.last_sent_count.write().await;
            let delta = sent.saturating_sub(*last);
            *last = sent;
            delta
        };
        let interval_received = {
            let mut last = self.last_received_count.write().await;
            let delta = received.saturating_sub(*last);
            *last = received;
            delta
        };

        let since_first_sent = self
            .first_sent_time
            .read()
            .await
            .map(|t| now.checked_duration_since(t))
            .flatten();
        let since_first_received = self
            .first_received_time
            .read()
            .await
            .map(|t| now.checked_duration_since(t))
            .flatten();

        StatsSnapshot {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            sent_count: sent,
            received_count: received,
            error_count: errors,
            total_duration: total_elapsed,
            interval_duration: since_last,
            interval_sent_count: interval_sent,
            interval_received_count: interval_received,
            since_first_sent,
            since_first_received,
            latency_ns_p50: p50,
            latency_ns_p95: p95,
            latency_ns_p99: p99,
            latency_ns_min: min,
            latency_ns_max: max,
            latency_ns_mean: mean,
            connections: conns,
            active_connections: active_conns,
            connection_attempts: conn_attempts,
            connection_failures: conn_failures,
            crashes_injected: crashes,
            reconnects,
            reconnect_failures,
            duplicate_count: duplicates,
            gap_count: gaps,
            head_loss,
        }
    }

    /// Reset all statistics
    #[allow(dead_code)]
    pub async fn reset(&self) {
        self.sent_count.store(0, Ordering::Relaxed);
        self.received_count.store(0, Ordering::Relaxed);
        self.error_count.store(0, Ordering::Relaxed);
        self.connections.store(0, Ordering::Relaxed);
        self.active_connections.store(0, Ordering::Relaxed);
        self.connection_attempts.store(0, Ordering::Relaxed);
        self.connection_failures.store(0, Ordering::Relaxed);
        self.crashes_injected.store(0, Ordering::Relaxed);
        self.reconnects.store(0, Ordering::Relaxed);
        self.reconnect_failures.store(0, Ordering::Relaxed);
        self.duplicate_count.store(0, Ordering::Relaxed);
        self.gap_count.store(0, Ordering::Relaxed);
        self.latency_hist.write().await.reset();
        *self.last_snapshot.write().await = Instant::now();
    }
}

#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub timestamp: u64,
    pub sent_count: u64,
    pub received_count: u64,
    pub error_count: u64,
    pub total_duration: Duration,
    pub interval_duration: Duration,
    pub interval_sent_count: u64,
    pub interval_received_count: u64,
    pub since_first_sent: Option<Duration>,
    pub since_first_received: Option<Duration>,
    pub latency_ns_p50: u64,
    pub latency_ns_p95: u64,
    pub latency_ns_p99: u64,
    pub latency_ns_min: u64,
    pub latency_ns_max: u64,
    pub latency_ns_mean: f64,
    pub connections: u64,
    pub active_connections: u64,
    pub connection_attempts: u64,
    pub connection_failures: u64,
    pub crashes_injected: u64,
    pub reconnects: u64,
    pub reconnect_failures: u64,
    pub duplicate_count: u64,
    pub gap_count: u64,
    pub head_loss: u64,
}

impl StatsSnapshot {
    /// Calculate throughput (messages per second) for the interval
    pub fn interval_throughput(&self) -> f64 {
        let interval_secs = self.interval_duration.as_secs_f64();
        if interval_secs > 0.0 {
            let base = if self.interval_received_count > 0 {
                self.interval_received_count
            } else {
                self.interval_sent_count
            };
            base as f64 / interval_secs
        } else {
            0.0
        }
    }

    /// Calculate overall throughput
    pub fn total_throughput(&self) -> f64 {
        // Prefer window from first receive for subscribers; fallback to first sent; else start_time
        let duration_opt = if self.received_count > 0 {
            self.since_first_received
        } else if self.sent_count > 0 {
            self.since_first_sent
        } else {
            Some(self.total_duration)
        };
        if let Some(d) = duration_opt {
            let secs = d.as_secs_f64();
            if secs > 0.0 {
                let base = if self.received_count > 0 {
                    self.received_count
                } else {
                    self.sent_count
                };
                base as f64 / secs
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Convert to CSV row
    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},{:.2},{:.2},{},{},{},{},{},{:.2},{},{},{},{},{},{},{},{},{}",
            self.timestamp,
            self.sent_count,
            self.received_count,
            self.error_count,
            self.total_throughput(),
            self.interval_throughput(),
            self.latency_ns_p50,
            self.latency_ns_p95,
            self.latency_ns_p99,
            self.latency_ns_min,
            self.latency_ns_max,
            self.latency_ns_mean,
            self.connections,
            self.active_connections,
            self.connection_attempts,
            self.connection_failures,
            self.crashes_injected,
            self.reconnects,
            self.reconnect_failures,
            self.duplicate_count,
            self.gap_count
        )
    }

    /// CSV header
    pub fn csv_header() -> &'static str {
        "timestamp,sent_count,received_count,error_count,total_throughput,interval_throughput,latency_ns_p50,latency_ns_p95,latency_ns_p99,latency_ns_min,latency_ns_max,latency_ns_mean,connections,active_connections,connection_attempts,connection_failures,crashes_injected,reconnects,reconnect_failures,duplicate_count,gap_count"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep as tokio_sleep;

    #[tokio::test]
    async fn interval_throughput_zero_when_no_new_messages() {
        let stats = Stats::new();

        // Initialize snapshot baseline
        let _ = stats.snapshot().await;

        // Wait for a small interval
        tokio_sleep(Duration::from_millis(50)).await;

        // No new messages received in this window
        let snap = stats.snapshot().await;
        assert_eq!(snap.interval_received_count, 0);
        assert_eq!(snap.interval_throughput(), 0.0);
    }

    #[tokio::test]
    async fn interval_throughput_reflects_delta_counts() {
        let stats = Stats::new();
        // Baseline snapshot to reset counters
        let _ = stats.snapshot().await;

        // Create a measurable interval
        tokio_sleep(Duration::from_millis(100)).await;

        // Receive a burst of 20 messages
        for _ in 0..20u32 {
            stats.record_received(1).await; // latency value doesn't matter for throughput
        }

        // Snapshot and validate interval throughput > 0
        let snap = stats.snapshot().await;
        assert_eq!(snap.interval_received_count, 20);
        let rate = snap.interval_throughput();
        assert!(
            rate > 0.0,
            "interval throughput should be positive, got {}",
            rate
        );

        // Next interval with no messages should go back to ~0
        tokio_sleep(Duration::from_millis(50)).await;
        let snap2 = stats.snapshot().await;
        assert_eq!(snap2.interval_received_count, 0);
        assert_eq!(snap2.interval_throughput(), 0.0);
    }
}
