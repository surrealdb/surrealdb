//! The wall-clock seam for time-dependent coordination state.
//!
//! Task leases compare a stored expiration against the current time, so
//! whether a lease is valid, renewable or expired is a pure function of that
//! reading. Reading the clock through a trait lets a test drive those
//! transitions deterministically rather than by sleeping through them, and
//! lets a host that owns time supply its own reading.

use std::fmt::Debug;

use chrono::{DateTime, Utc};

/// A source of the current wall-clock time.
///
/// Implementations must be cheap and non-blocking: a reading is taken on every
/// lease check, including the throttled maintenance checks on the batch path.
pub trait WallClock: Debug + Send + Sync + 'static {
	/// The current time.
	fn now(&self) -> DateTime<Utc>;
}

/// The system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
	fn now(&self) -> DateTime<Utc> {
		Utc::now()
	}
}

/// A clock that only moves when told to, for tests that need to observe a
/// time-dependent transition without waiting for it.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug)]
pub struct ManualClock {
	now: parking_lot::Mutex<DateTime<Utc>>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl ManualClock {
	/// A clock reading `start` until advanced.
	pub fn new(start: DateTime<Utc>) -> Self {
		Self {
			now: parking_lot::Mutex::new(start),
		}
	}

	/// Moves the clock forward by `by`.
	pub fn advance(&self, by: chrono::Duration) {
		let mut now = self.now.lock();
		*now += by;
	}
}

#[cfg(any(test, feature = "test-hooks"))]
impl WallClock for ManualClock {
	fn now(&self) -> DateTime<Utc> {
		*self.now.lock()
	}
}
