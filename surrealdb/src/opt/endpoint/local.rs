use std::fmt;
use std::time::Duration;

use surrealdb_core::kvs::config::{AolMode, SnapshotMode, format_duration};

use crate::Connect;
use crate::engine::local::Db;
use crate::Error;

impl<R> Connect<Db, R> {
	/// Enable MVCC versioning on the datastore.
	///
	/// Supported by `SurrealKv` and `Mem` engines.
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
	/// Supported by `SurrealKv` and `Mem` engines. Requires `versioned()`.
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
	/// Controls how and when data is flushed to disk. Supported by `SurrealKv`,
	/// `RocksDb`, and `Mem` engines (the `Mem` engine requires `persist()`
	/// to be set for sync to take effect).
	///
	/// The `mode` argument can be any type that implements `Display`. The
	/// canonical type is `SyncMode`:
	///
	/// - `SyncMode::Never` - leave flushing to the OS (fastest, least durable).
	/// - `SyncMode::Every` - sync on every transaction commit (fast, most durable).
	/// - `SyncMode::Interval(duration)` - periodic background flushing (fast, less durable).
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::local::SurrealKv;
	/// use surrealdb_core::kvs::config::SyncMode;
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
	/// use surrealdb::engine::local::RocksDb;
	/// use surrealdb_core::kvs::config::SyncMode;
	///
	/// let db = Surreal::new::<RocksDb>("path/to/database-folder")
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

	/// Set the persistence directory for the `Mem` engine.
	///
	/// When set, the in-memory database persists data to disk using AOL and/or snapshots.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::local::Mem;
	///
	/// let db = Surreal::new::<Mem>(())
	///     .persist("/tmp/data")
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn persist(mut self, path: &str) -> Self {
		self.address = self.address.and_then(|mut endpoint| match endpoint.url.scheme() {
			"mem" => {
				endpoint.append_query_param("persist", path);
				Ok(endpoint)
			}
			scheme => Err(Error::internal(format!(
				"The 'persist' option is only supported by the 'mem' engine, not '{scheme}'"
			))),
		});
		self
	}

	/// Set the AOL (Append-Only Log) mode for the `Mem` engine.
	///
	/// Requires `persist()` to be set.
	///
	/// - `MemAolMode::Never` - never use AOL (default).
	/// - `MemAolMode::Sync` - write synchronously to AOL on every commit.
	/// - `MemAolMode::Async` - write asynchronously to AOL after commit.
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
	/// Requires `persist()` to be set. Periodic snapshots are created at the given interval.
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
