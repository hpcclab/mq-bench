use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Base {
	instant: Instant,
	unix_ns: u128,
}

fn base() -> &'static Base {
	static BASE: OnceLock<Base> = OnceLock::new();
	BASE.get_or_init(|| {
		let now_sys = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap_or(Duration::from_secs(0))
			.as_nanos();
		Base { instant: Instant::now(), unix_ns: now_sys }
	})
}

/// Fast estimate of current UNIX time in nanoseconds using a cached base and Instant
#[inline]
pub fn now_unix_ns_estimate() -> u64 {
	let b = base();
	let delta = b.instant.elapsed().as_nanos();
	let ns = b.unix_ns.saturating_add(delta);
	// Clamp to u64 (overflows far in the future)
	ns as u64
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn monotonic_increase() {
		let a = now_unix_ns_estimate();
		let b = now_unix_ns_estimate();
		assert!(b >= a);
	}
}
