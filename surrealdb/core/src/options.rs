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
	/// Interval for resuming index builds stranded by a crashed or expired
	/// owner node.
	///
	/// A `CONCURRENTLY` index build runs as a detached task; if its owning node
	/// dies mid-build, nothing waits on that generation again, so the durable
	/// build state is stuck in `Building`/`Closing` and the index reports
	/// `status: indexing` with a frozen counter indefinitely. This task
	/// periodically adopts such builds (once their owner lease has expired) and
	/// drives them to completion. Set to `Duration::ZERO` to disable and recover
	/// stalled builds manually with `REBUILD INDEX`.
	///
	/// Default: 30 seconds
	pub index_build_resume_interval: Duration,
	/// Interval for processing queued async events.
	///
	/// Default: 5 seconds
	pub event_processing_interval: Duration,
	/// Interval at which the per-node live-query router tails the dedicated
	/// `lqe` keyspace and delivers notifications off the write path.
	///
	/// Only active when the live-query engine is `Router`; under the default
	/// `Inline` engine the task is a cheap no-op. This is a poll-based delivery
	/// cadence, so it bounds steady-state notification latency — kept short by
	/// default. (A commit-driven hot path that removes the poll latency is
	/// planned; this interval remains the durable backstop.)
	///
	/// Default: 100 milliseconds
	pub live_query_router_interval: Duration,
	/// Interval for the background reclaim of tombstoned namespace/database/
	/// index data.
	///
	/// `REMOVE NAMESPACE/DATABASE/INDEX` delete only the catalog definition and
	/// enqueue the data prefix; this task periodically destroys the orphaned
	/// data out-of-band.
	///
	/// Default: 60 seconds
	pub reclaim_interval: Duration,
	/// Minimum age a tombstoned namespace/database/index must reach before its
	/// data is physically reclaimed.
	///
	/// This is a snapshot-safety grace period, not a convenience delay. The
	/// reclaim task destroys data out-of-band — on TiKV via `unsafe_destroy_range`,
	/// which bypasses MVCC — so a read transaction whose snapshot predates the
	/// `REMOVE` must be given time to finish before its data is physically
	/// removed. A removal is only reclaimed once it is older than this window,
	/// which is equivalent to requiring its commit timestamp to fall behind the
	/// MVCC GC safepoint.
	///
	/// This MUST be `>= tikv_gc_lifetime` (the safepoint lag): a removal older
	/// than the grace is also older than the safepoint, so any transaction that
	/// could still read it has already expired. The background-task scheduler
	/// enforces this by using `max(reclaim_grace, tikv_gc_lifetime)`, so raising
	/// `tikv_gc_lifetime` alone can never make reclaim unsafe.
	///
	/// Default: 10 minutes (matches the default `tikv_gc_lifetime`)
	pub reclaim_grace: Duration,
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
	/// Interval for purging expired durable RPC sessions.
	///
	/// When RPC session persistence is enabled, client-attached sessions are
	/// mirrored to the KV store with an absolute expiry. Expired entries are
	/// already dropped lazily on load; this task additionally sweeps the
	/// session keyspace so entries that are never loaded again do not
	/// accumulate. Set to `Duration::ZERO` to disable the sweep.
	///
	/// Default: 60 seconds
	pub rpc_session_gc_interval: Duration,
	/// Interval for refreshing the cached process/system utilisation metrics.
	///
	/// Both `INFO FOR ROOT` and the `surrealdb.process.*` gauges read a cached
	/// snapshot rather than sampling on demand, so a periodic refresh is what
	/// keeps them current. Exactly one refresher runs per process, whatever the
	/// number of datastores: `sysinfo` derives CPU percentage as a delta since
	/// the previous refresh of one process-wide handle, so a second concurrent
	/// refresher would shorten that window unpredictably and skew the reading.
	/// Every datastore schedules the job on this interval; the claim in
	/// [`observe::process`](crate::observe::process) settles which one performs
	/// it, and hands it on when that datastore goes away. Construction warms the
	/// cache once per process on top of that, so the first query never reads the
	/// zeroed default.
	///
	/// Default: 30 seconds
	pub system_metrics_refresh_interval: Duration,
	/// How stale this node's cluster heartbeat may get before it is treated as
	/// unhealthy, overriding the value derived from
	/// [`Self::node_membership_refresh_interval`].
	///
	/// `None` derives it as the refresh interval times
	/// [`READINESS_HEARTBEAT_STALENESS_FACTOR`], which suits a storage engine
	/// whose node-row write is local and sub-millisecond: staleness beyond a few
	/// refresh cycles is then a genuine fault.
	///
	/// Set it when the write is not local. Under a consensus engine the same
	/// write is a distributed transaction whose latency moves with cluster
	/// health, and every member's heartbeat slows together — so a window derived
	/// from the refresh interval turns partial degradation into every node
	/// reporting unhealthy at once. Widening the window without slowing the
	/// refresh is only expressible through this field.
	///
	/// Must stay below the interval at which a peer archives a node it considers
	/// expired (see `Datastore::expire_nodes`): a node still reported healthy
	/// after its peers have written it off keeps taking traffic while its
	/// cluster registration and live queries are garbage-collected underneath
	/// it.
	///
	/// Default: `None`
	pub readiness_heartbeat_max_age: Option<Duration>,
}

/// How many node-membership refresh cycles a heartbeat may miss before the node
/// is treated as unhealthy, when no explicit window is configured.
pub const READINESS_HEARTBEAT_STALENESS_FACTOR: u32 = 3;

impl Default for EngineOptions {
	fn default() -> Self {
		Self {
			node_membership_refresh_interval: Duration::from_secs(3),
			node_membership_check_interval: Duration::from_secs(15),
			node_membership_cleanup_interval: Duration::from_secs(300),
			changefeed_gc_interval: Duration::from_secs(30),
			index_compaction_interval: Duration::from_secs(5),
			index_build_resume_interval: Duration::from_secs(30),
			event_processing_interval: Duration::from_secs(5),
			live_query_router_interval: Duration::from_millis(100),
			reclaim_interval: Duration::from_secs(60),
			reclaim_grace: Duration::from_secs(600),
			tikv_gc_interval: Duration::from_secs(600),
			tikv_gc_lifetime: Duration::from_secs(600),
			tikv_lock_cleanup_interval: Duration::from_secs(60),
			rpc_session_gc_interval: Duration::from_secs(60),
			system_metrics_refresh_interval: Duration::from_secs(30),
			readiness_heartbeat_max_age: None,
		}
	}
}

impl EngineOptions {
	/// How stale this node's cluster heartbeat may get before it is unhealthy.
	///
	/// The configured value when there is one, otherwise the refresh interval
	/// times [`READINESS_HEARTBEAT_STALENESS_FACTOR`]. An interval large enough
	/// to overflow that product degrades to [`Duration::MAX`], which is
	/// "never considered stale" rather than a panic.
	pub fn resolved_readiness_heartbeat_max_age(&self) -> Duration {
		self.readiness_heartbeat_max_age.unwrap_or_else(|| {
			self.node_membership_refresh_interval
				.checked_mul(READINESS_HEARTBEAT_STALENESS_FACTOR)
				.unwrap_or(Duration::MAX)
		})
	}

	pub fn with_readiness_heartbeat_max_age(mut self, max_age: Option<Duration>) -> Self {
		self.readiness_heartbeat_max_age = max_age;
		self
	}

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

	pub fn with_index_build_resume_interval(mut self, interval: Duration) -> Self {
		self.index_build_resume_interval = interval;
		self
	}

	pub fn with_event_processing_interval(mut self, interval: Duration) -> Self {
		self.event_processing_interval = interval;
		self
	}

	pub fn with_live_query_router_interval(mut self, interval: Duration) -> Self {
		self.live_query_router_interval = interval;
		self
	}

	pub fn with_reclaim_interval(mut self, interval: Duration) -> Self {
		self.reclaim_interval = interval;
		self
	}

	pub fn with_reclaim_grace(mut self, grace: Duration) -> Self {
		self.reclaim_grace = grace;
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

	pub fn with_rpc_session_gc_interval(mut self, interval: Duration) -> Self {
		self.rpc_session_gc_interval = interval;
		self
	}

	pub fn with_system_metrics_refresh_interval(mut self, interval: Duration) -> Self {
		self.system_metrics_refresh_interval = interval;
		self
	}
}
