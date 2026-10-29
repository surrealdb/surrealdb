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
	/// Interval for garbage collecting live-query events older than the
	/// datastore's `live_query_retention` from the dedicated event keyspace
	/// the Router engine captures into.
	///
	/// Scheduled independently of `changefeed_gc_interval`, so the retention
	/// window holds whether or not changefeed collection runs. The job runs
	/// whichever live-query engine is configured, so events captured before a
	/// switch of engine still age out. Zero disables the job.
	///
	/// Default: 30 seconds
	pub live_query_gc_interval: Duration,
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
	/// Interval between graph adjacency fold passes, which pack queued
	/// (vertex, direction) scopes' per-edge keys into adjacency blocks.
	///
	/// Default: 5 seconds
	pub graph_fold_interval: Duration,
	/// Interval between table migration passes, which rewrite rows stamped
	/// below a table's newest migration step and verify fields queued for
	/// verification. A commit that queues work wakes the job sooner. Zero
	/// disables the job: reads still apply the steps, and no field is
	/// promoted by verification.
	///
	/// Default: 10 seconds
	pub table_migration_interval: Duration,
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
	/// reclaim task destroys data out-of-band, and on a backend offering
	/// out-of-transaction range destruction that destruction bypasses MVCC — so
	/// a read transaction whose snapshot predates the `REMOVE` must be given
	/// time to finish before its data is physically removed. A removal is only
	/// reclaimed once it is older than this window, which is equivalent to
	/// requiring its commit timestamp to fall behind the MVCC GC safepoint.
	///
	/// This MUST be `>= mvcc_gc_lifetime` (the safepoint lag): a removal older
	/// than the grace is also older than the safepoint, so any transaction that
	/// could still read it has already expired. The background-task scheduler
	/// enforces this by using `max(reclaim_grace, mvcc_gc_lifetime)`, so raising
	/// `mvcc_gc_lifetime` alone can never make reclaim unsafe.
	///
	/// Default: 10 minutes (matches the default `mvcc_gc_lifetime`)
	pub reclaim_grace: Duration,
	/// Interval between backend MVCC garbage-collection passes.
	///
	/// Each pass asks the backend's [`Maintenance`](surrealdb_kvs::Maintenance)
	/// capability to advance its safepoint, allowing it to reclaim space taken
	/// by superseded MVCC versions. Set to `Duration::ZERO` to disable
	/// scheduling.
	///
	/// Only backends publishing the capability act on this interval; other
	/// backends ignore the task entirely.
	///
	/// Default: 10 minutes
	pub mvcc_gc_interval: Duration,
	/// How far behind the current time a backend's GC safepoint is allowed to
	/// sit: the safepoint is `now - lifetime`.
	///
	/// Default: 10 minutes
	pub mvcc_gc_lifetime: Duration,
	/// Interval between standalone backend lock-cleanup passes.
	///
	/// Faster cadence than the full GC pass because stale locks block
	/// readers immediately, while version GC can wait.
	///
	/// Default: 60 seconds
	pub lock_cleanup_interval: Duration,
	/// Interval for purging expired durable RPC sessions.
	///
	/// Client-attached RPC sessions are stored in the KV store with an
	/// absolute expiry, and an expired one is refused and dropped when it is
	/// next loaded. This task sweeps the whole session keyspace so entries
	/// that are never loaded again do not accumulate; it reclaims storage
	/// only, so it can run far less often than sessions expire. Set to
	/// `Duration::ZERO` to disable the sweep.
	///
	/// Default: 1 hour
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
	/// How often this node releases the writer leases it no longer
	/// writes under (`Datastore::release_idle_writer_leases`). A lease
	/// is only ever held on a store this datastore is not the sole writer of, so
	/// elsewhere the job finds nothing to do. Zero disables the job.
	///
	/// Default: 5 seconds
	pub diskann_writer_lease_release_interval: Duration,
}

impl Default for EngineOptions {
	fn default() -> Self {
		Self {
			node_membership_refresh_interval: Duration::from_secs(3),
			node_membership_check_interval: Duration::from_secs(15),
			node_membership_cleanup_interval: Duration::from_secs(300),
			changefeed_gc_interval: Duration::from_secs(30),
			live_query_gc_interval: Duration::from_secs(30),
			index_compaction_interval: Duration::from_secs(5),
			index_build_resume_interval: Duration::from_secs(30),
			event_processing_interval: Duration::from_secs(5),
			graph_fold_interval: Duration::from_secs(5),
			table_migration_interval: Duration::from_secs(10),
			live_query_router_interval: Duration::from_millis(100),
			reclaim_interval: Duration::from_secs(60),
			reclaim_grace: Duration::from_secs(600),
			mvcc_gc_interval: Duration::from_secs(600),
			mvcc_gc_lifetime: Duration::from_secs(600),
			lock_cleanup_interval: Duration::from_secs(60),
			rpc_session_gc_interval: Duration::from_secs(60 * 60),
			system_metrics_refresh_interval: Duration::from_secs(30),
			diskann_writer_lease_release_interval: Duration::from_secs(5),
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

	/// Set the live-query event garbage collection interval
	pub fn with_live_query_gc_interval(mut self, interval: Duration) -> Self {
		self.live_query_gc_interval = interval;
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

	/// Set the table migration interval
	pub fn with_table_migration_interval(mut self, interval: Duration) -> Self {
		self.table_migration_interval = interval;
		self
	}

	/// Set the graph adjacency fold interval
	pub fn with_graph_fold_interval(mut self, interval: Duration) -> Self {
		self.graph_fold_interval = interval;
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

	pub fn with_mvcc_gc_interval(mut self, interval: Duration) -> Self {
		self.mvcc_gc_interval = interval;
		self
	}

	pub fn with_mvcc_gc_lifetime(mut self, lifetime: Duration) -> Self {
		self.mvcc_gc_lifetime = lifetime;
		self
	}

	pub fn with_lock_cleanup_interval(mut self, interval: Duration) -> Self {
		self.lock_cleanup_interval = interval;
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

	pub fn with_diskann_writer_lease_release_interval(mut self, interval: Duration) -> Self {
		self.diskann_writer_lease_release_interval = interval;
		self
	}
}
