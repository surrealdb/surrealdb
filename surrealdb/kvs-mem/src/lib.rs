mod cnf;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
pub use cnf::MemoryConfig;
use surrealmx::{Database, DatabaseOptions, KeyIterator, ScanIterator, Transaction as Tx};
use tokio::sync::RwLock;
use tracing::{info, instrument};

const TARGET: &str = "surrealdb::core::kvs::mem";

use surrealdb_kvs::api::{
	BoxFut, GetMultiResult, KeyValSpan, KeysResult, ScanChunkStats, ScanCursorVals, ScanResult,
	Transactable, ValVisitor, ValsBatch,
};
#[cfg(not(target_family = "wasm"))]
use surrealdb_kvs::config::{AolMode, SnapshotMode, SyncMode};
use surrealdb_kvs::cursor::fill_vals_batch;
use surrealdb_kvs::err::{Error, Result};
use surrealdb_kvs::timestamp::{
	BoxTimeStamp, BoxTimeStampImpl, MAX_TIMESTAMP_BYTES, TimeStamp, TimeStampImpl,
};
use surrealdb_kvs::{
	Direction, Key, KeyRange, Metrics, SavepointStack, TransactionBuilder, TransactionType, Val,
};

/// Convert a SurrealMX engine error into the generic KVS error type.
#[expect(
	clippy::needless_pass_by_value,
	reason = "by-value so it can be passed directly to `Result::map_err`"
)]
fn kvs_error(e: surrealmx::Error) -> Error {
	match e {
		surrealmx::Error::TxNotWritable => Error::TransactionReadonly,
		surrealmx::Error::ValNotExpectedValue => Error::TransactionConditionNotMet,
		surrealmx::Error::TxClosed => Error::TransactionFinished,
		surrealmx::Error::KeyAlreadyExists => Error::TransactionKeyAlreadyExists,
		surrealmx::Error::KeyReadConflict => Error::TransactionConflict(e.to_string()),
		surrealmx::Error::KeyWriteConflict => Error::TransactionConflict(e.to_string()),
		_ => Error::Transaction(e.to_string()),
	}
}

pub struct Datastore {
	db: Database,
	/// Whether user-defined timestamps (versioning) are enabled
	versioned: bool,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: AtomicBool,
	/// Is the transaction writeable?
	write: bool,
	/// The underlying datastore transaction
	inner: RwLock<Tx>,
	/// Engine savepoints backing each open savepoint.
	///
	/// Always acquired before `inner` and held across it, so the scope counts
	/// and the engine's savepoint stack cannot drift apart between the two.
	/// Nothing acquires the pair in the opposite order, so they cannot deadlock.
	savepoints: RwLock<SavepointStack>,
	/// Copied from the datastore at transaction creation.
	versioned: bool,
}

impl Transaction {
	fn ensure_versioned(&self, version: Option<u64>) -> Result<()> {
		if !self.versioned && version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		Ok(())
	}
}

impl Datastore {
	/// Open a new database
	pub async fn new(config: MemoryConfig) -> Result<Datastore> {
		// Create the shared blocking threadpool (idempotent)
		surrealdb_kvs::threadpool::initialise();
		if config.versioned {
			if config.retention_ns > 0 {
				info!(
					target: TARGET,
					"Versioning enabled with retention period: {}ns", config.retention_ns
				);
			} else {
				info!(target: TARGET, "Versioning enabled with unlimited retention");
			}
		} else {
			info!(target: TARGET, "Versioning disabled");
		}
		#[cfg(not(target_family = "wasm"))]
		match &config.persist_path {
			Some(path) => {
				info!(target: TARGET, "Persistence path: {path}");
				info!(target: TARGET, "Append-only log mode: {}", config.aol_mode);
				info!(target: TARGET, "Snapshot mode: {}", config.snapshot_mode);
				info!(target: TARGET, "Sync mode: {}", config.sync_mode);
			}
			None => info!(target: TARGET, "Storage mode: in-memory only (no persist path)"),
		}
		// Create new configuration options. The background GC worker is the
		// only place surrealmx reclaims superseded MVCC versions and delete
		// tombstones (there is no inline GC on the commit path), so it must
		// run unless versioning is enabled with unlimited (zero) retention,
		// in which case every version is retained forever.
		let opts = DatabaseOptions {
			enable_gc: !config.versioned || config.retention_ns > 0,
			enable_cleanup: true,
			..Default::default()
		};
		// Create the database, optionally with persistence
		#[cfg(not(target_family = "wasm"))]
		let db = if let Some(ref persist_path) = config.persist_path {
			// Build persistence options from config
			let mut persistence_opts = surrealmx::PersistenceOptions::new(persist_path);
			// Map AOL mode
			persistence_opts.aol_mode = match config.aol_mode {
				AolMode::Never => surrealmx::AolMode::Never,
				AolMode::Sync => surrealmx::AolMode::SynchronousOnCommit,
				AolMode::Async => surrealmx::AolMode::AsynchronousAfterCommit,
			};
			// Map snapshot mode
			persistence_opts.snapshot_mode = match config.snapshot_mode {
				SnapshotMode::Never => surrealmx::SnapshotMode::Never,
				SnapshotMode::Interval(interval) => surrealmx::SnapshotMode::Interval(interval),
			};
			// Map sync mode to fsync mode
			persistence_opts.fsync_mode = match config.sync_mode {
				SyncMode::Never => surrealmx::FsyncMode::Never,
				SyncMode::Every => surrealmx::FsyncMode::EveryAppend,
				SyncMode::Interval(d) => surrealmx::FsyncMode::Interval(d),
			};
			// Create a persistent database
			Database::new_with_persistence(opts, persistence_opts)
				.map_err(|e| Error::Datastore(e.to_string()))?
		} else {
			// Create a non-persistent database
			Database::new_with_options(opts)
		};
		#[cfg(target_family = "wasm")]
		let db = Database::new_with_options(opts);
		// Configure GC retention if a retention period is specified
		let db = if config.retention_ns > 0 {
			db.with_gc_history(Duration::from_nanos(config.retention_ns))
		} else {
			db
		};
		// Return the new datastore
		Ok(Datastore {
			db,
			versioned: config.versioned,
		})
	}

	/// Shutdown the database
	pub async fn shutdown(&self) -> Result<()> {
		// Nothing to do here
		Ok(())
	}

	/// Start a new transaction
	pub async fn transaction(&self, write: TransactionType) -> Result<Box<dyn Transactable>> {
		let write = matches!(write, TransactionType::Write);
		// Create a new transactio
		let txn = self.db.transaction(write).with_snapshot_isolation();
		// Return the new transaction
		Ok(Box::new(Transaction {
			done: AtomicBool::new(false),
			write,
			inner: RwLock::new(txn),
			savepoints: RwLock::new(SavepointStack::default()),
			versioned: self.versioned,
		}))
	}
}

impl TransactionBuilder for Datastore {
	fn name(&self) -> &'static str {
		"memory"
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>> {
		// Transactions are local: the store runs in-process.
		Box::pin(async move { Ok((self.transaction(write).await?, true)) })
	}

	fn shutdown(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(Datastore::shutdown(self))
	}

	fn register_metrics(&self) -> Option<Metrics> {
		None
	}

	fn collect_u64_metric(&self, _metric: &str) -> Option<u64> {
		None
	}
}

impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"memory"
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done.load(Ordering::Relaxed)
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancels the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn cancel(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already closed
			if self.done.swap(true, Ordering::AcqRel) {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Rollback this transaction
			inner.cancel().map_err(kvs_error)?;
			// Continue
			Ok(())
		})
	}

	/// Commits the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn commit(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already closed
			if self.done.swap(true, Ordering::AcqRel) {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Commit this transaction
			inner.commit().map_err(kvs_error)?;
			// Continue
			Ok(())
		})
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<bool>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Get the key
			let res = match version {
				Some(ts) => {
					inner.get_at_version(key.into_inner(), ts).map_err(kvs_error)?.is_some()
				}
				None => inner.get(key.into_inner()).map_err(kvs_error)?.is_some(),
			};
			// Return result
			Ok(res)
		})
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Get the key
			let res = match version {
				Some(ts) => inner.get_at_version(key.into_inner(), ts).map_err(kvs_error)?,
				None => inner.get(key.into_inner()).map_err(kvs_error)?,
			};
			// Return result
			Ok(res.map(Val::from))
		})
	}

	/// Fetch multiple keys from the database.
	#[instrument(
		level = "trace",
		target = "surrealdb::core::kvs::api",
		skip(self),
		fields(keys = keys.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","))
	)]
	fn getm<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			let keys = keys.iter().map(|x| x.as_slice()).collect();
			// Get the keys
			let res = match version {
				Some(ts) => inner.getm_at_version(keys, ts).map_err(kvs_error)?,
				None => inner.getm(keys).map_err(kvs_error)?,
			};
			// Materialise once, accumulating the hit count and value bytes during
			// the same pass so callers do not need to re-walk the result.
			let mut records = 0u64;
			let mut value_bytes = 0u64;
			let values = res
				.into_iter()
				.map(|opt| {
					opt.map(|v| {
						records += 1;
						value_bytes += v.len() as u64;
						Val::from(v)
					})
				})
				.collect();
			Ok(GetMultiResult {
				values,
				records,
				value_bytes,
			})
		})
	}

	/// Insert or update a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Set the key
			inner.set(key.into_inner(), val).map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Insert or replace a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn replace<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Replace the key
			inner.set(key.into_inner(), val).map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Insert a key if it doesn't exist in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Set the key if empty
			inner.put(key.into_inner(), val).map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Insert a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Set the key if valid
			match (inner.get(key.as_slice()).map_err(kvs_error)?, chk) {
				(Some(v), Some(w)) if v == w => {
					inner.set(key.into_inner(), val).map_err(kvs_error)?
				}
				(None, None) => inner.set(key.into_inner(), val).map_err(kvs_error)?,
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Return result
			Ok(())
		})
	}

	/// Delete a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Remove the key
			inner.del(key.into_inner()).map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Delete a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Delete the key if valid
			match (inner.get(key.as_slice()).map_err(kvs_error)?, chk) {
				(Some(v), Some(w)) if v == w => inner.del(key.into_inner()).map_err(kvs_error)?,
				(None, None) => inner.del(key.into_inner()).map_err(kvs_error)?,
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Return result
			Ok(())
		})
	}

	/// Deletes all versions of a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn clr<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Remove the key (use del since delete doesn't exist in SurrealMX)
			inner.del(key.into_inner()).map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Delete all versions of a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn clrc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Delete the key if valid
			match (inner.get(key.as_slice()).map_err(kvs_error)?, chk) {
				(Some(v), Some(w)) if v == w => inner.del(key.into_inner()).map_err(kvs_error)?,
				(None, None) => inner.del(key.into_inner()).map_err(kvs_error)?,
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Return result
			Ok(())
		})
	}

	/// Count the total number of keys within a range.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn count<'a>(&'a self, rng: KeyRange<'a>, version: Option<u64>) -> BoxFut<'a, Result<usize>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start;
			let end = rng.end;
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Execute on the blocking threadpool
			let res = affinitypool::spawn_local(move || -> Result<_> {
				// Count the items in the range
				let res = match version {
					Some(ts) => inner
						.total_at_version(beg.into_inner()..end.into_inner(), None, None, ts)
						.map_err(kvs_error)?,
					None => inner
						.total(beg.into_inner()..end.into_inner(), None, None)
						.map_err(kvs_error)?,
				};
				// Return result
				Ok(res)
			})
			.await?;
			// Return result
			Ok(res)
		})
	}

	/// Retrieve a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start;
			let end = rng.end;
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Create a forward iterator
			let mut iter = match version {
				Some(ts) => inner
					.keys_iter_at_version(beg.into_inner()..end.into_inner(), ts)
					.map_err(kvs_error)?,
				None => inner.keys_iter(beg.into_inner()..end.into_inner()).map_err(kvs_error)?,
			};
			// Consume the iterator
			Ok(consume_keys(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of keys, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn keysr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start;
			let end = rng.end;
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Create a reverse iterator
			let mut iter = match version {
				Some(ts) => inner
					.keys_iter_at_version_reverse(beg.into_inner()..end.into_inner(), ts)
					.map_err(kvs_error)?,
				None => inner
					.keys_iter_reverse(beg.into_inner()..end.into_inner())
					.map_err(kvs_error)?,
			};
			// Consume the iterator
			Ok(consume_keys(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of key-value pairs.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn scan<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start;
			let end = rng.end;
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Create a forward iterator
			let mut iter = match version {
				Some(ts) => inner
					.scan_iter_at_version(beg.into_inner()..end.into_inner(), ts)
					.map_err(kvs_error)?,
				None => inner.scan_iter(beg.into_inner()..end.into_inner()).map_err(kvs_error)?,
			};
			// Consume the iterator
			Ok(consume_vals(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of key-value pairs, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn scanr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start.into_inner();
			let end = rng.end.into_inner();
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Create a reverse iterator
			let mut iter = match version {
				Some(ts) => inner.scan_iter_at_version_reverse(beg..end, ts).map_err(kvs_error)?,
				None => inner.scan_iter_reverse(beg..end).map_err(kvs_error)?,
			};
			// Consume the iterator
			Ok(consume_vals(&mut iter, limit, skip))
		})
	}

	/// Open a stateful key+value scan cursor. Overrides the default
	/// (double-copying) cursor with a resume-by-bound cursor whose `for_each`
	/// streams the engine's refcounted `Bytes` to the visitor with no payload
	/// copy. See [`MemValsCursor`].
	fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			Ok(Box::new(MemValsCursor {
				tx: self,
				rng,
				dir,
				version,
				skip,
				key_buf: Vec::new(),
				val_buf: Vec::new(),
				spans: Vec::new(),
			}) as Box<dyn ScanCursorVals + 'a>)
		})
	}

	/// Set a new save point on the transaction.
	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			let mut savepoints = self.savepoints.write().await;
			self.inner.write().await.set_savepoint().map_err(kvs_error)?;
			// Counted only once the engine has accepted the savepoint, so a
			// refusal cannot leave a scope counted that the engine never took.
			savepoints.open();
			Ok(())
		})
	}

	/// Rollback to the last save point.
	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// A scope that absorbed released savepoints needs one engine
			// rollback per savepoint, as each reverts to the most recent. The
			// stack guard is held across the unwind so the count and the engine
			// cannot diverge partway through.
			let mut savepoints = self.savepoints.write().await;
			let unwind = savepoints.take()?;
			let mut inner = self.inner.write().await;
			for _ in 0..unwind {
				inner.rollback_to_savepoint().map_err(kvs_error)?;
			}
			Ok(())
		})
	}

	/// Release the last save point.
	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			self.savepoints.write().await.release();
			Ok(())
		})
	}

	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		Box::new(SurrealMxTimeStampImpl)
	}
}

struct SurrealMxTimeStamp(u64);

impl TimeStamp for SurrealMxTimeStamp {
	fn as_versionstamp(&self) -> u128 {
		self.0 as u128
	}

	fn as_datetime(&self) -> Option<DateTime<Utc>> {
		Some(DateTime::from_timestamp_nanos(self.0 as i64))
	}

	fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp> {
		let nanos: u64 = duration.as_nanos().try_into().ok()?;
		Some(BoxTimeStamp::new(SurrealMxTimeStamp(self.0.checked_sub(nanos)?)))
	}

	fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8] {
		bytes[..8].copy_from_slice(&self.0.to_be_bytes());
		&bytes[..8]
	}
}

struct SurrealMxTimeStampImpl;

impl TimeStampImpl for SurrealMxTimeStampImpl {
	fn earliest(&self) -> BoxTimeStamp {
		BoxTimeStamp::new(SurrealMxTimeStamp(0))
	}

	fn create_from_versionstamp(&self, version: u128) -> Option<BoxTimeStamp> {
		Some(BoxTimeStamp::new(SurrealMxTimeStamp(version.try_into().ok()?)))
	}

	fn create_from_datetime(&self, dt: DateTime<Utc>) -> Option<BoxTimeStamp> {
		let nanos = dt.timestamp_nanos_opt()?;
		if nanos < 0 {
			return None;
		}
		Some(BoxTimeStamp::new(SurrealMxTimeStamp(nanos as u64)))
	}

	fn decode(&self, bytes: &[u8]) -> Result<BoxTimeStamp> {
		let bytes = <[u8; 8]>::try_from(bytes).map_err(|_| {
			Error::TimestampInvalid("encoded timestamp not a valid length".to_string())
		})?;
		Ok(BoxTimeStamp::new(SurrealMxTimeStamp(u64::from_be_bytes(bytes))))
	}
}

// Consume and iterate over only keys
fn consume_keys(cursor: &mut KeyIterator<'_>, limit: u32, skip: u32) -> KeysResult {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if cursor.next().is_none() {
			return KeysResult::default();
		}
	}
	let mut key_bytes = 0u64;
	// Create the result set
	let mut keys = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while keys.len() < limit as usize {
		if let Some(k) = cursor.next() {
			key_bytes += k.len() as u64;
			keys.push(k.to_vec());
		} else {
			break;
		}
	}
	KeysResult {
		keys,
		key_bytes,
	}
}

// Consume and iterate over keys and values
fn consume_vals(cursor: &mut ScanIterator<'_>, limit: u32, skip: u32) -> ScanResult {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if cursor.next().is_none() {
			return ScanResult::default();
		}
	}
	// Track the cumulative key/value bytes for the scan metrics.
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	// Create the result set
	let mut values = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while values.len() < limit as usize {
		if let Some((k, v)) = cursor.next() {
			key_bytes += k.len() as u64;
			value_bytes += v.len() as u64;
			values.push((k.to_vec(), v.to_vec()));
		} else {
			break;
		}
	}
	ScanResult {
		values,
		key_bytes,
		value_bytes,
	}
}

/// Stateful, resume-by-bound key+value scan cursor for the in-memory engine.
///
/// surrealmx scan iterators borrow the transaction's `RwLock` read guard, so a
/// cursor cannot hold one across calls without a self-referential borrow.
/// Instead this cursor re-seeks the B-tree from the current range bound on each
/// call (a cheap operation) and advances the bound past the last visited key.
/// `for_each` hands the engine's refcounted `Bytes` to the visitor by
/// reference — zero payload copy — while `next_batch` fills the reusable
/// buffers for callers that need an owned-in-buffer batch.
struct MemValsCursor<'a> {
	/// The parent transaction (source of the `RwLock`-guarded inner tx).
	tx: &'a Transaction,
	/// Remaining range to scan; advanced past the last visited key each call.
	rng: KeyRange<'a>,
	/// Fixed scan direction.
	dir: Direction,
	/// Optional historical version timestamp.
	version: Option<u64>,
	/// Entries still to skip before the first visited row (burned once).
	skip: u32,
	/// Reusable concatenated key buffer for the `next_batch` path.
	key_buf: Vec<u8>,
	/// Reusable concatenated value buffer for the `next_batch` path.
	val_buf: Vec<u8>,
	/// Reusable per-pair spans for the `next_batch` path.
	spans: Vec<KeyValSpan>,
}

impl MemValsCursor<'_> {
	/// Build a fresh forward/backward (optionally versioned) iterator over the
	/// current range, borrowing the supplied read guard.
	fn build_iter<'g>(&self, inner: &'g Tx) -> Result<ScanIterator<'g>> {
		let rng = self.rng.start.as_slice()..self.rng.end.as_slice();
		Ok(match (self.version, self.dir) {
			(Some(ts), Direction::Forward) => {
				inner.scan_iter_at_version(rng, ts).map_err(kvs_error)?
			}
			(None, Direction::Forward) => inner.scan_iter(rng).map_err(kvs_error)?,
			(Some(ts), Direction::Backward) => {
				inner.scan_iter_at_version_reverse(rng, ts).map_err(kvs_error)?
			}
			(None, Direction::Backward) => inner.scan_iter_reverse(rng).map_err(kvs_error)?,
		})
	}
}

impl ScanCursorVals for MemValsCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<ValsBatch<'s>>> {
		// The materialising path shares the default cursor's `scan`-based fill;
		// the zero-copy path that borrows the engine's `Bytes` is `for_each`.
		Box::pin(async move {
			if self.tx.closed() {
				return Err(Error::TransactionFinished);
			}
			let (key_bytes, value_bytes) = fill_vals_batch(
				self.tx,
				&mut self.rng,
				self.dir,
				self.version,
				&mut self.skip,
				&mut self.key_buf,
				&mut self.val_buf,
				&mut self.spans,
				limit,
			)
			.await?;
			Ok(ValsBatch::from_parts(
				&self.key_buf,
				&self.val_buf,
				&self.spans,
				key_bytes,
				value_bytes,
			))
		})
	}

	/// Zero-copy chunk drive. Note: the transaction's `RwLock` read guard is
	/// held for the whole chunk — including the visitor's per-row work (e.g.
	/// record decode) — so a same-transaction writer queues behind it for a
	/// bounded stretch: ≤ `skip + limit` rows on the first call (the leading
	/// skip burns under the same guard), ≤ `limit` rows thereafter. Concurrent
	/// readers are unaffected unless a writer is already queued (the lock is
	/// FIFO-fair).
	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			if self.tx.closed() {
				return Err(Error::TransactionFinished);
			}
			let mut stats = ScanChunkStats::default();
			// A zero-budget call must be a pure no-op: bail before burning
			// `skip` or touching the iterator so the cursor state is intact
			// for the next (non-zero) call.
			if limit == 0 || self.rng.is_empty() {
				return Ok(stats);
			}
			let tx = self.tx;
			let mut last = None;
			let mut broke = false;
			{
				let inner = tx.inner.read().await;
				let mut iter = self.build_iter(&inner)?;
				for _ in 0..std::mem::take(&mut self.skip) {
					if iter.next().is_none() {
						self.rng.end = Key::empty();
						return Ok(stats);
					}
				}
				while stats.rows < limit as u64 {
					let Some((k, v)) = iter.next() else {
						self.rng.end = Key::empty();
						break;
					};
					// Count only after the visitor accepts the row, so a
					// visitor error never records an unresumed row.
					let flow = f(&k[..], &v[..])?;
					stats.rows += 1;
					stats.key_bytes += k.len() as u64;
					stats.value_bytes += v.len() as u64;
					last = Some(k);
					if let std::ops::ControlFlow::Break(()) = flow {
						broke = true;
						break;
					}
				}
			}

			if let Some(l) = last {
				match self.dir {
					Direction::Forward => {
						self.rng.start.clone_from_slice(&l);
						self.rng.start.advance();
					}
					Direction::Backward => {
						self.rng.end.clone_from_slice(&l);
					}
				}
			} else {
				self.rng.end = Key::empty();
			}
			// A short chunk without an early `Break` means the iterator dried
			// up before the row budget: the range is exhausted.
			if !broke && stats.rows < limit as u64 {
				self.rng.end = Key::empty();
			}
			Ok(stats)
		})
	}
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
	use super::*;

	/// Number of stored version entries for `key`, including delete
	/// tombstones.
	fn stored_versions(db: &Database, key: &str) -> usize {
		let tx = db.transaction(false);
		let end = format!("{key}\0");
		tx.scan_all_versions(key..end.as_str(), None, None).expect("scan_all_versions failed").len()
	}

	/// Poll until `check` passes or roughly ten seconds elapse. The background
	/// GC worker ticks every 500ms, so a healthy datastore converges within a
	/// tick or two of the relevant commit.
	async fn eventually(mut check: impl FnMut() -> bool) -> bool {
		for _ in 0..200 {
			if check() {
				return true;
			}
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
		false
	}

	/// A non-versioned (zero-retention) datastore must reclaim superseded
	/// MVCC versions and delete tombstones. surrealmx reclaims versions only
	/// on its background GC worker — there is no inline GC on the commit
	/// path — so this pins the worker being enabled for such datastores.
	#[tokio::test]
	async fn non_versioned_datastore_reclaims_superseded_versions() {
		let ds = Datastore::new(MemoryConfig::default()).await.unwrap();
		// Write an initial version of both keys, then supersede one and
		// tombstone the other. Each transaction is dropped after commit so
		// its registered snapshot does not hold back the GC watermark.
		{
			let mut tx = ds.db.transaction(true);
			tx.set("updated", "one").unwrap();
			tx.set("deleted", "one").unwrap();
			tx.commit().unwrap();
		}
		{
			let mut tx = ds.db.transaction(true);
			tx.set("updated", "two").unwrap();
			tx.del("deleted").unwrap();
			tx.commit().unwrap();
		}
		// The superseded version is reclaimed, leaving only the live one.
		assert!(
			eventually(|| stored_versions(&ds.db, "updated") == 1).await,
			"superseded version was not reclaimed by the background GC worker"
		);
		// The tombstoned version chain is reclaimed entirely.
		assert!(
			eventually(|| stored_versions(&ds.db, "deleted") == 0).await,
			"delete tombstone was not reclaimed by the background GC worker"
		);
		// Reclamation must not touch the live state.
		let tx = ds.db.transaction(false);
		assert_eq!(tx.get("updated").unwrap().as_deref(), Some(&b"two"[..]));
		assert_eq!(tx.get("deleted").unwrap(), None);
	}
}
