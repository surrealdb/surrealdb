use crate::dbs::node::Timestamp;
use crate::sql;
use sql::Duration;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

// Traits cannot have async and we need sized structs for Clone + Send + Sync
#[derive(Clone)]
pub enum SizedClock {
	Fake(FakeClock),
	Inc(IncFakeClock),
	System(SystemClock),
}

impl SizedClock {
	pub async fn now(&mut self) -> Timestamp {
		match self {
			SizedClock::Fake(c) => c.now(),
			SizedClock::Inc(c) => c.now().await,
			SizedClock::System(c) => c.now(),
		}
	}
}

/// FakeClock is a clock that is fully controlled externally.
/// Use this clock for when you are testing timestamps.
#[derive(Clone)]
pub struct FakeClock {
	now: Arc<RwLock<Timestamp>>,
}

impl FakeClock {
	pub fn new(now: Timestamp) -> Self {
		FakeClock {
			now: Arc::new(RwLock::new(now)),
		}
	}

	pub fn now(&self) -> Timestamp {
		self.now.read().unwrap().clone()
	}

	pub fn set(&mut self, timestamp: Timestamp) {
		self.now.write().unwrap().set(timestamp);
	}
}

/// IncFakeClock increments a local clock every time the clock is accessed, similar to a real clock.
/// This is useful when you need unique and partially deterministic timestamps for tests.
/// Partially deterministic, because you do not have direct control over how many times a clock
/// is accessed, and due to the nature of async - you neither have order guarantee.
#[derive(Clone)]
pub struct IncFakeClock {
	now: Timestamp,
	increment: Duration,
}

impl IncFakeClock {
	pub fn new(now: Timestamp, increment: Duration) -> Self {
		IncFakeClock {
			now,
			increment,
		}
	}

	pub async fn now(&mut self) -> Timestamp {
		self.now.get_and_inc(&self.increment)
	}
}

/// SystemClock is a clock that uses the system time.
/// Use this when there are no other alternatives.
#[derive(Clone)]
pub struct SystemClock {}

impl SystemClock {
	pub fn new() -> Self {
		SystemClock {}
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
