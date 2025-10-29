use std::time::Duration;
use tokio::time::{Interval, MissedTickBehavior, interval};

/// Rate controller for open-loop message sending
pub struct RateController {
    #[allow(dead_code)]
    rate_per_sec: f64,
    ticker: Interval,
    tokens: u32,
    tokens_per_tick: u32,
    frac_per_tick: u32, // fixed-point Q24.8-scale fractional tokens per tick
    frac_accum: u32,    // accumulator for fractional tokens
    max_tokens: u32,
}

impl RateController {
    /// Create new rate controller for target messages per second
    /// Implementation: token bucket with adaptive tick (<=1000 ticks/s) to amortize timer cost.
    pub fn new(msgs_per_second: f64) -> Self {
        // Adaptive tick: aim for <= 100 ticks per second; not less than 10ms
        let ticks_per_sec = msgs_per_second.max(1.0).min(100.0);
        let tick = Duration::from_secs_f64(1.0 / ticks_per_sec);
        let mut ticker = interval(tick);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Burst);

        let per_tick = msgs_per_second * tick.as_secs_f64();
        let tokens_per_tick = per_tick.floor() as u32;
        // Q24.8 fixed point for fractional tokens per tick
        const FP_SCALE: f64 = 256.0; // 2^8
        let frac_per_tick = ((per_tick - per_tick.floor()) * FP_SCALE) as u32;
        // Allow short bursts up to ~10 ticks worth to smooth small scheduling hiccups.
        let max_tokens = (tokens_per_tick.saturating_mul(10)).max(1);

        Self {
            rate_per_sec: msgs_per_second.max(0.0),
            ticker,
            // Start with 1 token to avoid a cold-start delay
            tokens: 1,
            tokens_per_tick,
            frac_per_tick,
            frac_accum: 0,
            max_tokens,
        }
    }

    /// Wait until it's time to send the next message
    #[inline(always)]
    pub async fn wait_for_next(&mut self) {
        // Fast path: if we have tokens, consume and return immediately.
        if self.tokens > 0 {
            self.tokens -= 1;
            return;
        }

        // Otherwise, refill until at least one token is available.
        loop {
            self.ticker.tick().await;
            // integer refill
            let mut new_tokens = self.tokens.saturating_add(self.tokens_per_tick);
            // handle fractional accumulation (Q24.8)
            let acc = self.frac_accum as u32 + self.frac_per_tick as u32;
            let carry = acc >> 8; // divide by FP scale (256)
            self.frac_accum = acc & 0xFF;
            new_tokens = new_tokens.saturating_add(carry);
            self.tokens = new_tokens.min(self.max_tokens);
            if self.tokens > 0 {
                self.tokens -= 1;
                break;
            }
        }
    }

    /// Get configured interval between messages
    #[allow(dead_code)]
    pub fn interval(&self) -> Duration {
        if self.rate_per_sec > 0.0 {
            Duration::from_secs_f64(1.0 / self.rate_per_sec)
        } else {
            Duration::from_secs(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant as TokioInstant;

    // This is a coarse-grained timing check to ensure the controller spaces events out.
    // It doesn't aim for perfect accuracy, just that the average interval is in the right ballpark.
    #[tokio::test]
    async fn rate_controller_spaces_events() {
        // 50 msg/s => ~20ms interval
        let mut rc = RateController::new(50.0);
        let expected = Duration::from_millis(20);

        let mut times: Vec<TokioInstant> = Vec::new();

        // Warm-up first token
        rc.wait_for_next().await;
        times.push(TokioInstant::now());

        // Collect 10 ticks
        for _ in 0..10 {
            rc.wait_for_next().await;
            times.push(TokioInstant::now());
        }

        // Compute total elapsed across 10 intervals
        let total_elapsed = times
            .last()
            .unwrap()
            .saturating_duration_since(*times.first().unwrap());
        // We have 10 intervals (after warm-up) across 10 steps
        let avg = total_elapsed / 10;

        // Allow generous bounds due to scheduler jitter in CI/VMs
        let lower = expected.mul_f32(0.5); // >= 10ms
        let upper = expected.mul_f32(5.0); // <= 100ms

        assert!(
            avg >= lower,
            "avg interval too small: {:?} < {:?}",
            avg,
            lower
        );
        assert!(
            avg <= upper,
            "avg interval too large: {:?} > {:?}",
            avg,
            upper
        );
    }
}
