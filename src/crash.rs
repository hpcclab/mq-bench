//! Crash injection for failure testing.
//!
//! Provides MTTF/MTTR-based failure simulation with exponential distribution
//! for realistic failure patterns. Each `CrashInjector` maintains independent
//! state for per-connection failure simulation.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// Configuration for crash injection behavior.
#[derive(Clone, Debug)]
pub struct CrashConfig {
    /// Mean Time To Failure in seconds (0 = disabled)
    pub mttf_secs: f64,
    /// Mean Time To Repair in seconds (wait before reconnect)
    pub mttr_secs: f64,
    /// Number of crashes to simulate (0 = infinite)
    pub crash_count: u32,
    /// Optional RNG seed for reproducible failure patterns
    pub seed: Option<u64>,
}

impl Default for CrashConfig {
    fn default() -> Self {
        Self {
            mttf_secs: 0.0, // Disabled by default
            mttr_secs: 5.0,
            crash_count: 0,
            seed: None,
        }
    }
}

impl CrashConfig {
    /// Returns true if crash injection is enabled
    pub fn is_enabled(&self) -> bool {
        self.mttf_secs > 0.0
    }
}

/// Per-connection crash injector with independent RNG state.
pub struct CrashInjector {
    config: CrashConfig,
    crashes_remaining: AtomicU32,
    base_time: Instant,
    timeline_offset: Duration,
    next_crash_at: Instant,
    rng_state: u64, // Simple xorshift64 state
}

impl CrashInjector {
    /// Create a new crash injector with the given configuration.
    pub fn new(config: CrashConfig) -> Self {
        let seed = config.seed.unwrap_or_else(|| {
            // Use current time as seed if not specified
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(12345)
        });

        let mut injector = Self {
            crashes_remaining: AtomicU32::new(config.crash_count),
            base_time: Instant::now(),
            timeline_offset: Duration::ZERO,
            next_crash_at: Instant::now(),
            rng_state: seed,
            config,
        };

        if injector.config.is_enabled() {
            // Schedule first crash deterministically relative to injector start.
            injector.schedule_next_crash_after(Duration::ZERO);
        }

        injector
    }

    /// Simple xorshift64 PRNG - returns value in [0, 1)
    fn next_random(&mut self) -> f64 {
        self.rng_state ^= self.rng_state << 13;
        self.rng_state ^= self.rng_state >> 7;
        self.rng_state ^= self.rng_state << 17;
        // Convert to [0, 1) range
        (self.rng_state as f64) / (u64::MAX as f64)
    }

    /// Sample from truncated exponential distribution with given mean.
    /// Capped at 3x mean to avoid extreme outliers that could exceed test duration.
    fn sample_exponential(&mut self, mean_secs: f64) -> Duration {
        if mean_secs <= 0.0 {
            return Duration::ZERO;
        }
        // Exponential: -mean * ln(U) where U ~ Uniform(0,1)
        let u = self.next_random();
        // Avoid ln(0) by clamping
        let u = u.max(1e-10);
        let sample_secs = -mean_secs * u.ln();
        // Truncate at 3x mean to prevent extreme outliers
        let max_secs = mean_secs * 3.0;
        let capped_secs = sample_secs.min(max_secs);
        Duration::from_secs_f64(capped_secs)
    }

    /// Returns true if crash injection is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }

    /// Returns true if a crash should be triggered now.
    pub fn should_crash(&self) -> bool {
        if !self.config.is_enabled() {
            return false;
        }
        Instant::now() >= self.next_crash_at
    }

    /// Returns time until next scheduled crash (for async waiting).
    pub fn time_until_crash(&self) -> Duration {
        if !self.config.is_enabled() {
            return Duration::MAX;
        }
        self.next_crash_at.saturating_duration_since(Instant::now())
    }

    /// Sample repair time (wait duration before reconnect attempt).
    pub fn sample_repair_time(&mut self) -> Duration {
        self.sample_exponential(self.config.mttr_secs)
    }

    /// Apply a deterministic phase offset to the crash timeline.
    ///
    /// This is useful for multi-topic tests where each topic/connection should have an
    /// independent crash schedule that is also intentionally de-synchronized.
    pub fn apply_phase_offset(&mut self, offset: Duration) {
        if !self.config.is_enabled() {
            return;
        }
        if offset.is_zero() {
            return;
        }
        self.timeline_offset += offset;
        self.next_crash_at = self.base_time + self.timeline_offset;
    }

    /// Schedule the next crash time based on MTTF.
    pub fn schedule_next_crash(&mut self) {
        self.schedule_next_crash_after(Duration::ZERO);
    }

    /// Schedule the next crash after a known downtime (e.g., MTTR repair sleep).
    ///
    /// This avoids timing drift when the check loop notices a crash late: crash times are
    /// derived from a deterministic timeline offset rather than `Instant::now()`.
    pub fn schedule_next_crash_after(&mut self, downtime: Duration) {
        if !self.config.is_enabled() {
            return;
        }

        // Advance the deterministic timeline by the downtime we intentionally slept.
        self.timeline_offset += downtime;

        // Then advance by the next failure interval.
        let time_to_failure = self.sample_exponential(self.config.mttf_secs);
        self.timeline_offset += time_to_failure;
        self.next_crash_at = self.base_time + self.timeline_offset;
    }

    /// Consume one crash from the counter.
    /// Returns true if crash should proceed, false if limit reached.
    pub fn consume_crash(&self) -> bool {
        if self.config.crash_count == 0 {
            return true; // Infinite crashes
        }
        let remaining = self.crashes_remaining.fetch_sub(1, Ordering::Relaxed);
        remaining > 0
    }

    /// Check if more crashes are allowed.
    pub fn has_crashes_remaining(&self) -> bool {
        if self.config.crash_count == 0 {
            return true; // Infinite
        }
        self.crashes_remaining.load(Ordering::Relaxed) > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crash_disabled_by_default() {
        let config = CrashConfig::default();
        assert!(!config.is_enabled());

        let injector = CrashInjector::new(config);
        assert!(!injector.should_crash());
    }

    #[test]
    fn test_deterministic_with_seed() {
        let config = CrashConfig {
            mttf_secs: 10.0,
            mttr_secs: 2.0,
            crash_count: 5,
            seed: Some(42),
        };

        let mut injector1 = CrashInjector::new(config.clone());
        let mut injector2 = CrashInjector::new(config);

        // Same seed should produce same sequence
        let repair1 = injector1.sample_repair_time();
        let repair2 = injector2.sample_repair_time();
        assert_eq!(repair1, repair2);
    }

    #[test]
    fn test_crash_count_limit() {
        let config = CrashConfig {
            mttf_secs: 1.0,
            mttr_secs: 0.1,
            crash_count: 3,
            seed: Some(1),
        };

        let injector = CrashInjector::new(config);

        assert!(injector.consume_crash()); // 3 -> 2
        assert!(injector.consume_crash()); // 2 -> 1
        assert!(injector.consume_crash()); // 1 -> 0
        assert!(!injector.consume_crash()); // 0 -> underflow, returns false
    }

    #[test]
    fn test_phase_offset_increases_time_until_crash() {
        let config = CrashConfig {
            mttf_secs: 10.0,
            mttr_secs: 1.0,
            crash_count: 1,
            seed: Some(42),
        };

        let mut injector = CrashInjector::new(config);
        let before = injector.time_until_crash();
        injector.apply_phase_offset(Duration::from_secs(1));
        let after = injector.time_until_crash();
        assert!(after > before);
    }

    #[test]
    fn test_infinite_crashes() {
        let config = CrashConfig {
            mttf_secs: 1.0,
            mttr_secs: 0.1,
            crash_count: 0, // Infinite
            seed: Some(1),
        };

        let injector = CrashInjector::new(config);

        for _ in 0..100 {
            assert!(injector.consume_crash());
        }
    }

    #[test]
    fn test_exponential_distribution_mean() {
        let config = CrashConfig {
            mttf_secs: 10.0,
            mttr_secs: 5.0,
            crash_count: 0,
            seed: Some(12345),
        };

        let mut injector = CrashInjector::new(config);

        // Sample many values and check mean is roughly correct
        let samples: Vec<f64> = (0..1000)
            .map(|_| injector.sample_exponential(10.0).as_secs_f64())
            .collect();

        let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
        // Mean should be close to 10.0 (within 20% for 1000 samples)
        assert!(mean > 8.0 && mean < 12.0, "Mean was {}", mean);
    }
}
