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
	/// Interval for processing queued async events.
	///
	/// Default: 5 seconds
	pub event_processing_interval: Duration,
	/// Interval between TiKV MVCC garbage-collection passes.
	///
	/// Each pass calls `cleanup_locks` followed by `update_safepoint`,
	/// allowing TiKV to reclaim space taken by superseded MVCC versions.
	/// Mirrors TiDB's default of 10 minutes. Set to `Duration::ZERO` to
	/// disable scheduling (the value is also gated by
	/// `SURREAL_TIKV_GC_ENABLED`).
	///
	/// Only the TiKV backend acts on this interval; other backends ignore
	/// the task entirely.
	///
	/// Default: 10 minutes
	pub tikv_gc_interval: Duration,
	/// How far behind the current TSO a TiKV GC safepoint is allowed to
	/// sit. The actual safepoint passed to `gc()` is
	/// `current_timestamp - lifetime`.
	///
	/// Default: 10 minutes
	pub tikv_gc_lifetime: Duration,
	/// Interval between standalone TiKV lock-cleanup passes.
	///
	/// Faster cadence than the full GC pass because stale locks block
	/// readers immediately, while version GC can wait.
	///
	/// Default: 60 seconds
	pub tikv_lock_cleanup_interval: Duration,
}

impl Default for EngineOptions {
	fn default() -> Self {
		Self {
			node_membership_refresh_interval: Duration::from_secs(3),
			node_membership_check_interval: Duration::from_secs(15),
			node_membership_cleanup_interval: Duration::from_secs(300),
			changefeed_gc_interval: Duration::from_secs(30),
			index_compaction_interval: Duration::from_secs(5),
			event_processing_interval: Duration::from_secs(5),
			tikv_gc_interval: Duration::from_secs(600),
			tikv_gc_lifetime: Duration::from_secs(600),
			tikv_lock_cleanup_interval: Duration::from_secs(60),
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

	pub fn with_event_processing_interval(mut self, interval: Duration) -> Self {
		self.event_processing_interval = interval;
		self
	}

	pub fn with_tikv_gc_interval(mut self, interval: Duration) -> Self {
		self.tikv_gc_interval = interval;
		self
	}

	pub fn with_tikv_gc_lifetime(mut self, lifetime: Duration) -> Self {
		self.tikv_gc_lifetime = lifetime;
		self
	}

	pub fn with_tikv_lock_cleanup_interval(mut self, interval: Duration) -> Self {
		self.tikv_lock_cleanup_interval = interval;
		self
	}
}
