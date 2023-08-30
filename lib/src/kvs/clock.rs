use crate::dbs::node::Timestamp;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

pub trait Clock: Clone {
	fn now(&self) -> Timestamp;
}

/// FakeClock is a clock that is fully controlled externally.
/// Use this clock for when you are testing timestamps.
#[derive(Clone)]
pub struct FakeClock {
	now: Timestamp,
}

impl Clock for FakeClock {
	fn now(&self) -> Timestamp {
		self.now.clone()
	}
}

impl FakeClock {
	pub fn new(now: Timestamp) -> Self {
		FakeClock {
			now,
		}
	}

	pub fn set(&mut self, timestamp: Timestamp) {
		self.now = timestamp;
	}
}

/// IncFakeClock increments a local clock every time the clock is accessed, similar to a real clock.
/// This is useful when you need unique and partially deterministic timestamps for tests.
/// Partially deterministic, because you do not have direct control over how many times a clock
/// is accessed, and due to the nature of async - you neither have order guarantee.
#[derive(Clone)]
pub struct IncFakeClock {
	now: Arc<Mutex<Timestamp>>,
	increment: Duration,
}

impl Clock for IncFakeClock {
	fn now(&self) -> Timestamp {
		self.now.try_lock().unwrap().get_and_inc(self.increment)
	}
}

impl IncFakeClock {
	pub fn new(now: Timestamp, increment: Duration) -> Self {
		IncFakeClock {
			now: Arc::new(Mutex::new(now)),
			increment,
		}
	}
}

/// SystemClock is a clock that uses the system time.
/// Use this when there are no other alternatives.
#[derive(Clone)]
pub struct SystemClock {}

impl Clock for SystemClock {
	fn now(&self) -> Timestamp {
		// Use a timestamp oracle if available
		let now: u128 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
		Timestamp {
			value: now as u64,
		}
	}
}

impl SystemClock {
	pub fn new() -> Self {
		SystemClock {}
	}
}
