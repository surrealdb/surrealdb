use std::time::Duration;

/// Configuration for the engine behaviour
///
/// The defaults are optimal so please only modify these if you know deliberately why you are modifying them.
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
#[non_exhaustive]
pub struct EngineOptions {
	pub node_membership_refresh_interval: Duration,
	pub node_membership_check_interval: Duration,
	pub node_membership_cleanup_interval: Duration,
	pub changefeed_gc_interval: Duration,
}

impl Default for EngineOptions {
	fn default() -> Self {
		Self {
			node_membership_refresh_interval: Duration::from_secs(3),
			node_membership_check_interval: Duration::from_secs(15),
			node_membership_cleanup_interval: Duration::from_secs(300),
			changefeed_gc_interval: Duration::from_secs(10),
		}
	}
}

impl EngineOptions {
	pub fn with_node_membership_refresh_interval(mut self, interval: Duration) -> Self {
		self.node_membership_refresh_interval = interval;
		self
	}
	pub fn with_node_membership_check_interval(mut self, interval: Duration) -> Self {
		self.node_membership_check_interval = interval;
		self
	}
	pub fn with_node_membership_cleanup_interval(mut self, interval: Duration) -> Self {
		self.node_membership_cleanup_interval = interval;
		self
	}
	pub fn with_changefeed_gc_interval(mut self, interval: Duration) -> Self {
		self.changefeed_gc_interval = interval;
		self
	}
}
