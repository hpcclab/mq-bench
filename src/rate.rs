use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Rate controller for open-loop message sending
pub struct RateController {
    interval: Duration,
    last_send: Option<Instant>,
}

impl RateController {
    /// Create new rate controller for target messages per second
    pub fn new(msgs_per_second: f64) -> Self {
        let interval = Duration::from_nanos((1_000_000_000.0 / msgs_per_second) as u64);
        Self {
            interval,
            last_send: None,
        }
    }
    
    /// Wait until it's time to send the next message
    pub async fn wait_for_next(&mut self) {
        let now = Instant::now();
        
        if let Some(last) = self.last_send {
            let elapsed = now.duration_since(last);
            if elapsed < self.interval {
                let wait_time = self.interval - elapsed;
                sleep(wait_time).await;
            }
        }
        
        self.last_send = Some(Instant::now());
    }
    
    /// Get configured interval between messages
    pub fn interval(&self) -> Duration {
        self.interval
    }
}
