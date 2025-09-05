use hdrhistogram::Histogram;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Statistics collector for latency and throughput
pub struct Stats {
    // Latency histogram (nanosecond precision)
    latency_hist: RwLock<Histogram<u64>>,
    
    // Counters
    pub sent_count: RwLock<u64>,
    pub received_count: RwLock<u64>,
    pub error_count: RwLock<u64>,
    
    // Timing
    start_time: Instant,
    last_snapshot: RwLock<Instant>,
}

impl Stats {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            // 1ns to 60s range, 3 significant digits
            latency_hist: RwLock::new(Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()),
            sent_count: RwLock::new(0),
            received_count: RwLock::new(0),
            error_count: RwLock::new(0),
            start_time: now,
            last_snapshot: RwLock::new(now),
        }
    }
    
    /// Record a sent message
    pub async fn record_sent(&self) {
        let mut count = self.sent_count.write().await;
        *count += 1;
    }
    
    /// Record a received message with latency
    pub async fn record_received(&self, latency_ns: u64) {
        let mut count = self.received_count.write().await;
        *count += 1;
        
        if let Ok(mut hist) = self.latency_hist.try_write() {
            let _ = hist.record(latency_ns);
        }
    }
    
    /// Record an error
    pub async fn record_error(&self) {
        let mut count = self.error_count.write().await;
        *count += 1;
    }
    
    /// Get current snapshot of statistics
    pub async fn snapshot(&self) -> StatsSnapshot {
        let now = Instant::now();
        let sent = *self.sent_count.read().await;
        let received = *self.received_count.read().await;
        let errors = *self.error_count.read().await;
        
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
            latency_ns_p50: p50,
            latency_ns_p95: p95,
            latency_ns_p99: p99,
            latency_ns_min: min,
            latency_ns_max: max,
            latency_ns_mean: mean,
        }
    }
    
    /// Reset all statistics
    pub async fn reset(&self) {
        *self.sent_count.write().await = 0;
        *self.received_count.write().await = 0;
        *self.error_count.write().await = 0;
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
    pub latency_ns_p50: u64,
    pub latency_ns_p95: u64,
    pub latency_ns_p99: u64,
    pub latency_ns_min: u64,
    pub latency_ns_max: u64,
    pub latency_ns_mean: f64,
}

impl StatsSnapshot {
    /// Calculate throughput (messages per second) for the interval
    pub fn interval_throughput(&self) -> f64 {
        let interval_secs = self.interval_duration.as_secs_f64();
        if interval_secs > 0.0 {
            self.received_count as f64 / interval_secs
        } else {
            0.0
        }
    }
    
    /// Calculate overall throughput
    pub fn total_throughput(&self) -> f64 {
        let total_secs = self.total_duration.as_secs_f64();
        if total_secs > 0.0 {
            self.received_count as f64 / total_secs
        } else {
            0.0
        }
    }
    
    /// Convert to CSV row
    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},{:.2},{:.2},{},{},{},{},{},{:.2}",
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
            self.latency_ns_mean
        )
    }
    
    /// CSV header
    pub fn csv_header() -> &'static str {
        "timestamp,sent_count,received_count,error_count,total_throughput,interval_throughput,latency_ns_p50,latency_ns_p95,latency_ns_p99,latency_ns_min,latency_ns_max,latency_ns_mean"
    }
}
