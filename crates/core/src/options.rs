use std::time::Duration;

/// Configuration for the engine behaviour
///
/// The defaults are optimal so please only modify these if you know deliberately why you are modifying them.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct EngineOptions {
	pub node_membership_refresh_interval: Duration,
	pub node_membership_check_interval: Duration,
	pub node_membership_cleanup_interval: Duration,
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
	/// Interval at which the reclaim task drains the reclaim queue
	///
	/// `REMOVE NAMESPACE` and `REMOVE DATABASE` delete only the catalog
	/// definition and enqueue a job naming the orphaned data prefix. The
	/// reclaim task wakes at this interval to destroy those prefixes in
	/// bounded, resumable batches, spending a fixed key budget each time, so
	/// raising the interval slows reclaim down and lowering it speeds reclaim
	/// up.
	///
	/// Reclaim has no disabled state: it is what completes a removal, so a
	/// queue nothing drains leaves the data of every removed object on disk and
	/// holds its name against reuse. A zero interval falls back to the default
	/// rate rather than stopping the task.
	///
	/// Default: 60 seconds
	pub reclaim_interval: Duration,
}

impl Default for EngineOptions {
	fn default() -> Self {
		Self {
			node_membership_refresh_interval: Duration::from_secs(3),
			node_membership_check_interval: Duration::from_secs(15),
			node_membership_cleanup_interval: Duration::from_secs(300),
			changefeed_gc_interval: Duration::from_secs(10),
			index_compaction_interval: Duration::from_secs(5),
			reclaim_interval: Duration::from_secs(60),
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

	pub fn with_reclaim_interval(mut self, interval: Duration) -> Self {
		self.reclaim_interval = interval;
		self
	}
}
