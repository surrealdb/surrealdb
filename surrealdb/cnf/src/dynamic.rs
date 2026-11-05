use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use num_traits::ToPrimitive;

/// Thread‑safe container for dynamic Datastore configuration toggles.
///
/// This currently carries the global query timeout which can be adjusted at
/// runtime (e.g. via the `ALTER SYSTEM` statement). The inner values are stored
/// using lock‑free atomics and the configuration is cheap to clone.
#[derive(Default, Debug, Clone)]
pub struct DynamicConfiguration(Arc<Inner>);

#[derive(Default, Debug)]
struct Inner {
	query_timeout: AtomicU64,
}
impl DynamicConfiguration {
	/// Sets the global query timeout enforced by the Datastore.
	///
	/// Passing `None` disables the timeout. Any concrete `Duration` is stored
	/// with millisecond precision; values that do not fit in `u64` are clamped
	/// to `u64::MAX` milliseconds.
	pub fn set_query_timeout(&self, duration: Option<Duration>) {
		let val = match duration {
			None => 0,
			Some(d) => d.as_millis().to_u64().unwrap_or(u64::MAX),
		};
		self.0.query_timeout.store(val, Ordering::Relaxed);
	}

	/// Returns the currently configured global query timeout.
	///
	/// A return value of `None` indicates that no timeout is enforced.
	pub fn get_query_timeout(&self) -> Option<Duration> {
		match self.0.query_timeout.load(Ordering::Relaxed) {
			0 => None,
			d => Some(Duration::from_millis(d)),
		}
	}
}
