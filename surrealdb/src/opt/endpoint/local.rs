use std::fmt;
use std::time::Duration;

use crate::engine::local::Db;
use crate::opt::storage::format_duration;
use crate::opt::{AolMode, SnapshotMode};
use crate::{Connect, Error};

impl<R> Connect<Db, R> {
	/// Enable MVCC versioning on the datastore.
	///
	/// Supported by the `SurrealKv` engine.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::local::SurrealKv;
	///
	/// let db = Surreal::new::<SurrealKv>("path/to/database-folder").versioned().await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn versioned(mut self) -> Self {
		self.address = self.address.map(|mut endpoint| {
			endpoint.append_query_param("versioned", "true");
			endpoint
		});
		self
	}

	/// Set the version retention period.
	///
	/// Determines how long old versions are kept before being garbage collected.
	/// A duration of zero means unlimited retention.
	///
	/// Supported by the `SurrealKv` engine. Requires `versioned()`.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// use std::time::Duration;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::local::SurrealKv;
	///
	/// let db = Surreal::new::<SurrealKv>("path/to/database-folder")
	///     .versioned()
	///     .retention(Duration::from_secs(30 * 86400)) // 30 days
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn retention(mut self, duration: Duration) -> Self {
		self.address = self.address.map(|mut endpoint| {
			endpoint.append_query_param("retention", &format_duration(duration));
			endpoint
		});
		self
	}

	/// Set the disk sync mode.
	///
	/// Controls how and when data is flushed to disk. Supported by `SurrealKv`
	/// and `Mem` engines (the `Mem` engine requires a persist path,
	/// e.g. `Surreal::new::<Mem>("/tmp/data")` or `connect("mem:///tmp/data")`,
	/// for sync to take effect).
	///
	/// The `mode` argument can be any type whose `Display` form the engine
	/// parses as a sync mode. The canonical type is [`SyncMode`](crate::opt::SyncMode):
	///
	/// - `SyncMode::Never` - leave flushing to the OS (fastest, least durable).
	/// - `SyncMode::Every` - sync on every transaction commit (fast, most durable).
	/// - `SyncMode::Interval(duration)` - periodic background flushing (fast, less durable).
	///
	/// The default for `SurrealKv` is `SyncMode::Every` on native targets. In the
	/// browser (`wasm32`) it is `SyncMode::Interval` of one second, since a
	/// commit that waited for a durable flush would wait on the origin private
	/// file system. The default for `Mem` is `SyncMode::Never`.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::local::SurrealKv;
	/// use surrealdb::opt::SyncMode;
	///
	/// let db = Surreal::new::<SurrealKv>("path/to/database-folder")
	///     .sync(SyncMode::Every)
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// use std::time::Duration;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::local::SurrealKv;
	/// use surrealdb::opt::SyncMode;
	///
	/// let db = Surreal::new::<SurrealKv>("path/to/database-folder")
	///     .sync(SyncMode::Interval(Duration::from_millis(200)))
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn sync(mut self, mode: impl fmt::Display) -> Self {
		self.address = self.address.map(|mut endpoint| {
			endpoint.append_query_param("sync", &mode.to_string());
			endpoint
		});
		self
	}

	/// Set the AOL (Append-Only Log) mode for the `Mem` engine.
	///
	/// Requires a persist path (e.g. `Surreal::new::<Mem>("/tmp/data")` or
	/// `connect("mem:///tmp/data")`).
	///
	/// - `AolMode::Never` - never use AOL (default).
	/// - `AolMode::Sync` - write synchronously to AOL on every commit.
	/// - `AolMode::Async` - write asynchronously to AOL after commit.
	pub fn aol(mut self, mode: AolMode) -> Self {
		self.address = self.address.and_then(|mut endpoint| match endpoint.url.scheme() {
			"mem" => {
				endpoint.append_query_param("aol", &mode.to_string());
				Ok(endpoint)
			}
			scheme => Err(Error::internal(format!(
				"The 'aol' option is only supported by the 'mem' engine, not '{scheme}'"
			))),
		});
		self
	}

	/// Set the snapshot interval for the `Mem` engine.
	///
	/// Requires a persist path. Periodic snapshots are created at the given interval.
	///
	/// - `SnapshotMode::Never` - never use snapshots (default).
	/// - `SnapshotMode::Interval(duration)` - take snapshots at the given interval.
	pub fn snapshot(mut self, mode: SnapshotMode) -> Self {
		self.address = self.address.and_then(|mut endpoint| match endpoint.url.scheme() {
			"mem" => {
				endpoint.append_query_param("snapshot", &mode.to_string());
				Ok(endpoint)
			}
			scheme => Err(Error::internal(format!(
				"The 'snapshot' option is only supported by the 'mem' engine, not '{scheme}'"
			))),
		});
		self
	}
}
