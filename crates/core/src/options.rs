use std::time::Duration;

/// Configuration for the engine behaviour
///
/// The defaults are optimal so please only modify these if you know
/// deliberately why you are modifying them.
#[derive(Clone, Copy, Debug)]
pub struct EngineOptions {
	/// Interval for refreshing node membership information
	pub node_membership_refresh_interval: Duration,
	/// Interval for checking node membership status
	pub node_membership_check_interval: Duration,
	/// Interval for cleaning up inactive nodes from the cluster
	pub node_membership_cleanup_interval: Duration,
	/// Interval for garbage collecting expired changefeed data
	pub changefeed_gc_interval: Duration,
	/// Interval for running the index compaction process
	///
	/// The index compaction thread runs at this interval to process indexes
	/// that have been marked for compaction. Compaction helps optimize index
	/// performance, particularly for full-text indexes, by consolidating
	/// changes and removing unnecessary data.
	///
	/// Default: 5 seconds
	pub index_compaction_interval: Duration,
	/// Maximum number of optimiser passes to run on queries
	///
	/// The optimiser runs multiple passes to simplify and optimize query
	/// expressions. Each pass applies a set of optimization rules until
	/// either no more changes occur or the maximum number of passes is reached.
	///
	/// Default: 3 passes
	pub optimiser_max_passes: usize,
}

impl Default for EngineOptions {
	fn default() -> Self {
		Self {
			node_membership_refresh_interval: Duration::from_secs(3),
			node_membership_check_interval: Duration::from_secs(15),
			node_membership_cleanup_interval: Duration::from_secs(300),
			changefeed_gc_interval: Duration::from_secs(30),
			index_compaction_interval: Duration::from_secs(5),
			optimiser_max_passes: 3,
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

	pub fn with_index_compaction_interval(mut self, interval: Duration) -> Self {
		self.index_compaction_interval = interval;
		self
	}

	pub fn with_optimiser_max_passes(mut self, passes: usize) -> Self {
		self.optimiser_max_passes = passes;
		self
	}
}
