use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(target_family = "wasm"))]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(target_family = "wasm")]
use wasmtimer::std::{SystemTime, UNIX_EPOCH};

use crate::dbs::node::Timestamp;
use crate::val::Duration;

// Traits cannot have async and we need sized structs for Clone + Send + Sync
#[derive(Clone)]
pub enum SizedClock {
	System(SystemClock),
	#[cfg(test)]
	Fake(FakeClock),
	#[cfg(test)]
	Inc(IncFakeClock),
}

impl SizedClock {
	#[allow(dead_code, reason = "Not used when none of the storage backends are enabled.")]
	pub(crate) fn system() -> Self {
		Self::System(Default::default())
	}

	pub async fn now(&self) -> Timestamp {
		match self {
			SizedClock::System(c) => c.now(),
			#[cfg(test)]
			SizedClock::Fake(c) => c.now().await,
			#[cfg(test)]
			SizedClock::Inc(c) => c.now().await,
		}
	}
}

/// FakeClock is a clock that is fully controlled externally.
/// Use this clock for when you are testing timestamps.
pub struct FakeClock {
	// Locks necessary for Send
	now: AtomicU64,
}

impl Clone for FakeClock {
	fn clone(&self) -> Self {
		FakeClock {
			now: AtomicU64::new(self.now.load(Ordering::SeqCst)),
		}
	}
}

#[cfg_attr(not(test), expect(dead_code))]
impl FakeClock {
	pub fn new(now: Timestamp) -> Self {
		FakeClock {
			now: AtomicU64::new(now.value),
		}
	}

	pub async fn now(&self) -> Timestamp {
		Timestamp {
			value: self.now.load(Ordering::SeqCst),
		}
	}

	pub async fn set(&self, timestamp: Timestamp) {
		self.now.store(timestamp.value, Ordering::SeqCst);
	}
}

/// IncFakeClock increments a local clock every time the clock is accessed,
/// similar to a real clock. This is useful when you need unique and partially
/// deterministic timestamps for tests. Partially deterministic, because you do
/// not have direct control over how many times a clock is accessed, and due to
/// the nature of async - you neither have order guarantee.
pub struct IncFakeClock {
	now: AtomicU64,
	increment: Duration,
}

impl Clone for IncFakeClock {
	fn clone(&self) -> Self {
		IncFakeClock {
			now: AtomicU64::new(self.now.load(Ordering::SeqCst)),
			increment: self.increment,
		}
	}
}

#[cfg_attr(not(test), expect(dead_code))]
impl IncFakeClock {
	pub fn new(now: Timestamp, increment: Duration) -> Self {
		IncFakeClock {
			now: AtomicU64::new(now.value),
			increment,
		}
	}

	pub async fn now(&self) -> Timestamp {
		self.now.fetch_add(self.increment.as_millis() as u64, Ordering::SeqCst);
		Timestamp {
			value: self.now.load(Ordering::SeqCst),
		}
	}
}

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
