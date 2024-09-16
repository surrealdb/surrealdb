use std::time::Duration;

/// Configuration for the engine behaviour
///
/// The defaults are optimal so please only modify these if you know deliberately why you are modifying them.
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
#[non_exhaustive]
pub struct EngineOptions {
	pub tick_interval: Duration,
}

impl Default for EngineOptions {
	fn default() -> Self {
		Self {
			tick_interval: Duration::from_secs(10),
		}
	}
}

impl EngineOptions {
	pub fn with_tick_interval(mut self, tick_interval: Duration) -> Self {
		self.tick_interval = tick_interval;
		self
	}
}
