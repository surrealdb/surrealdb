use web_time::{SystemTime, UNIX_EPOCH};

use crate::dbs::node::Timestamp;

/// SystemClock is a clock that uses the system time.
/// Use this when there are no other alternatives.
#[derive(Clone, Copy)]
pub struct SystemClock;

impl SystemClock {
	pub fn new() -> Self {
		SystemClock
	}
	pub fn now(&self) -> Timestamp {
		// Use a timestamp oracle if available
		let now: u128 = match SystemTime::now().duration_since(UNIX_EPOCH) {
			Ok(duration) => duration.as_millis(),
			Err(error) => panic!("Clock may have gone backwards: {:?}", error.duration()),
		};
		Timestamp {
			value: now as u64,
		}
	}
}

impl Default for SystemClock {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::clock::SystemClock;

	#[test]
	fn get_clock_now() {
		let clock = SystemClock::new();
		let _ = clock.now();
	}
}
