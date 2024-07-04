use std::time::Duration;

/// Configuration for the engine behaviour
///
/// The defaults are optimal so please only modify these if you know deliberately why you are modifying them.
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
#[non_exhaustive]
pub struct EngineOptions {
	/// The maximum number of live queries that can be created in a single transaction
	pub new_live_queries_per_transaction: u32,
	/// The size of batches being requested per update in order to catch up a live query
	pub live_query_catchup_size: u32,
	pub tick_interval: Duration,
}

impl Default for EngineOptions {
	fn default() -> Self {
		Self {
			new_live_queries_per_transaction: 100,
			live_query_catchup_size: 1000,
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
