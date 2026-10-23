// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

#[cfg(not(target_family = "wasm"))]
mod background_flusher;
mod cnf;
mod snapshot;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(not(target_family = "wasm"))]
use background_flusher::BackgroundFlusher;
use chrono::{DateTime, Utc};
pub use cnf::SurrealKvConfig;
use snapshot::SurrealKvSnapshot;
use surrealdb_kvs::api::{
	BoxFut, GetMultiResult, KeysResult, RangeKeyVisitor, ScanChunkStats, ScanResult, Transactable,
};
use surrealdb_kvs::config::SyncMode;
use surrealdb_kvs::err::{Error, Result};
use surrealdb_kvs::snapshot::ViewCapabilities;
use surrealdb_kvs::timestamp::{
	BoxTimeStamp, BoxTimeStampImpl, MAX_TIMESTAMP_BYTES, TimeStamp, TimeStampImpl,
};
use surrealdb_kvs::{
	Direction, Key, KeyRange, Metrics, SavepointStack, Snapshot, TransactionBuilder,
	TransactionType, Val,
};
use surrealkv::{
	CompactRangeOptions, Durability, HistoryOptions, LSMIterator, Mode, Options, Transaction as Tx,
	Tree, TreeBuilder,
};
#[cfg(target_family = "wasm")]
use surrealkv::{FileSystem, IndexedDbMigration, migrate_from_indexeddb};
use tokio::sync::RwLock;
use tracing::{error, info, instrument, warn};

const TARGET: &str = "surrealdb::core::kvs::surrealkv";

/// Convert a SurrealKV engine error into the generic KVS error type.
#[expect(
	clippy::needless_pass_by_value,
	reason = "by-value so it can be passed directly to `Result::map_err`"
)]
fn kvs_error(e: surrealkv::Error) -> Error {
	if matches!(e, surrealkv::Error::TransactionWriteConflict | surrealkv::Error::TransactionRetry)
	{
		Error::TransactionConflict(e.to_string())
	} else if let surrealkv::Error::TransactionReadOnly = e {
		Error::TransactionReadonly
	} else if let surrealkv::Error::TransactionClosed = e {
		Error::TransactionFinished
	} else if let surrealkv::Error::BatchTooLarge = e {
		Error::TransactionTooLarge
	} else {
		Error::Transaction(e.to_string())
	}
}

/// Convert a SurrealKV engine error raised while mounting or opening the
/// database into the generic KVS error type, naming the one condition a
/// caller can act on: another tab or worker holds the database.
#[cfg(target_family = "wasm")]
#[expect(
	clippy::needless_pass_by_value,
	reason = "by-value so it can be passed directly to `Result::map_err`"
)]
fn open_error(name: &str, e: surrealkv::Error) -> Error {
	if e.is_database_locked() {
		Error::ConnectionFailed(format!(
			"The SurrealKV database `{name}` is open in another tab or worker. A database in the \
			 browser's origin private file system can be open in one tab at a time; close the \
			 other tab, or retry once it has released the database"
		))
	} else {
		Error::Datastore(e.to_string())
	}
}

/// Whether `path` holds the files of a RocksDB database: its `CURRENT` file and
/// a `MANIFEST-` file.
#[cfg(not(target_family = "wasm"))]
fn holds_rocksdb_files(path: &str) -> bool {
	let Ok(entries) = std::fs::read_dir(path) else {
		return false;
	};
	let (mut current, mut manifest) = (false, false);
	for entry in entries.flatten() {
		let name = entry.file_name();
		let name = name.to_string_lossy();
		current |= name == "CURRENT";
		manifest |= name.starts_with("MANIFEST-");
	}
	current && manifest
}

/// The error of a failed commit. Only the errors SurrealKV raises before it writes anything are
/// definite; any other may come after the commit reached the WAL, or the memtable, so whether it
/// applied is unknown: recovery may replay it.
///
/// The mapping is conservative. SurrealKV raises `PipelineStall`, `Io` and the other errors of
/// a failed background task both before and after it writes, and the variant does not tell
/// which, so a commit refused before writing anything is also reported as unknown.
fn commit_error(e: surrealkv::Error) -> Error {
	if matches!(
		e,
		surrealkv::Error::TransactionWriteConflict
			| surrealkv::Error::TransactionRetry
			| surrealkv::Error::TransactionReadOnly
			| surrealkv::Error::TransactionClosed
			| surrealkv::Error::BatchTooLarge
	) {
		kvs_error(e)
	} else {
		Error::CommitOutcomeUnknown(e.to_string())
	}
}

pub struct Datastore {
	/// Shared with the transactions that compact it: a `Tree` closes the database when
	/// it is dropped, so a handle must only drop once nothing else uses the database.
	db: Arc<Tree>,
	/// Whether the datastore supports transaction versioning
	versioned: bool,
	/// Whether a commit is durable before it returns (sync=every)
	sync: bool,
	/// Set once shutdown has begun, after which no commit is applied
	shutting_down: Arc<AtomicBool>,
	/// The mounted origin private file system, released after the tree is
	/// closed
	#[cfg(target_family = "wasm")]
	fs: FileSystem,
	/// Ends the interval flusher of a browser database when set
	#[cfg(target_family = "wasm")]
	flusher_stopped: Arc<AtomicBool>,
	/// Background flusher for periodically flushing WAL when sync=<interval>
	#[cfg(not(target_family = "wasm"))]
	background_flusher: Option<Arc<BackgroundFlusher>>,
}

pub struct Transaction {
	/// Is the transaction complete? Shared with the read view, which refuses once it is set.
	done: Arc<AtomicBool>,
	/// Is the transaction writeable?
	write: bool,
	/// Whether the datastore supports transaction versioning
	versioned: bool,
	/// The tree the transaction belongs to, which a manual compaction runs on.
	db: Arc<Tree>,
	/// The underlying datastore transaction, shared with the read view and its cursors.
	inner: Arc<RwLock<Tx>>,
	/// Engine savepoints backing each open savepoint.
	///
	/// Always acquired before `inner` and held across it, so the scope counts
	/// and the engine's savepoint stack cannot drift apart between the two.
	/// Nothing acquires the pair in the opposite order, so they cannot deadlock.
	savepoints: RwLock<SavepointStack>,
	/// Whether a commit is durable before it returns (sync=every)
	sync: bool,
	/// Set once the datastore has begun shutting down
	shutting_down: Arc<AtomicBool>,
}

impl Transaction {
	fn ensure_versioned(&self, version: Option<u64>) -> Result<()> {
		if !self.versioned && version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		Ok(())
	}

	/// Commits the transaction, durably when `sync` and the datastore syncs every commit.
	///
	/// A durable commit is made visible to other transactions only once it is
	/// synced: SurrealKV logs, syncs and only then publishes it, grouping the
	/// syncs of concurrent commits. Syncing after a commit is visible would let
	/// another transaction read a write that a crash then loses.
	async fn commit_with(&self, sync: bool) -> Result<()> {
		// Atomically mark transaction as done and check if it was already closed
		if self.done.swap(true, Ordering::AcqRel) {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Refuse the commit before applying anything once the datastore
		// has begun shutting down, so no new commit runs past this point.
		// Shutdown is treated as a controlled crash — the store is
		// required to be consistent after any crash — so failing here
		// leaves the transaction un-applied and safely retryable.
		if self.shutting_down.load(Ordering::Acquire) {
			return Err(Error::Shutdown);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// An unsynced commit is visible before it is durable, which its caller accepts
		inner.set_durability(if self.sync && sync {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		// Commit the transaction, syncing the WAL before it is visible if durable.
		// A shutdown can still close the tree after the check above, while this
		// commit waits for its transaction or inside SurrealKV's commit pipeline,
		// which then refuses it with `PipelineStall`. Closing the tree lets every
		// admitted commit finish with its real outcome, so that refusal comes
		// before anything is written. It is reported as `Error::Shutdown` too, so
		// that callers treat the commit as interrupted by the shutdown rather
		// than as failed.
		inner.commit().await.map_err(|e| {
			if let surrealkv::Error::PipelineStall = e
				&& self.shutting_down.load(Ordering::Acquire)
			{
				return Error::Shutdown;
			}
			commit_error(e)
		})?;
		// Continue
		Ok(())
	}
}

impl Datastore {
	/// Whether the browser keeps the storage of this origin until the user
	/// clears it. The page must have asked with
	/// `navigator.storage.persist()`, which a worker cannot call; until it is
	/// granted the browser may evict the database when the disk fills.
	#[cfg(target_family = "wasm")]
	pub async fn persisted() -> Result<bool> {
		FileSystem::persisted().await.map_err(|e| Error::Datastore(e.to_string()))
	}

	/// The options that the database is created with and opened with.
	fn options(config: &SurrealKvConfig) -> Options {
		// Enable separated keys and values
		// Determine if vlog should be enabled
		// - Required when versioning is enabled
		// - Can be explicitly enabled via env var even without versioning
		info!(target: TARGET, "Enabling value log separation: {}", config.enable_vlog);
		let opts = Options::default().with_enable_vlog(config.enable_vlog);

		// Configure the maximum value log file size
		info!(target: TARGET, "Setting value log max file size: {}", config.vlog_max_file_size);
		let opts = opts.with_vlog_max_file_size(config.vlog_max_file_size);

		// Configure value log threshold
		info!(target: TARGET, "Setting value log threshold: {}", config.vlog_threshold);
		let opts = opts.with_vlog_value_threshold(config.vlog_threshold);

		// Configure versioned queries with retention period
		let retention_ns = config.retention.as_nanos().try_into().unwrap_or(u64::MAX);
		info!(target: TARGET, "Versioning enabled: {} with retention period: {}ns", config.versioned, retention_ns);
		let opts = opts.with_versioning(config.versioned, retention_ns);

		// Configure the maximum memtable size
		info!(target: TARGET, "Setting max memtable size: {}", config.max_memtable_size);
		let opts = opts.with_max_memtable_size(config.max_memtable_size);
		// Enable the block cache capacity
		info!(target: TARGET, "Setting block cache capacity: {}", config.block_cache_capacity);
		let opts = opts.with_block_cache_capacity(config.block_cache_capacity);
		// Set the block size
		info!(target: TARGET, "Setting block size: {}", config.block_size);
		opts.with_block_size(config.block_size)
	}

	/// Open a new database
	#[cfg(not(target_family = "wasm"))]
	pub async fn new(path: &str, config: SurrealKvConfig) -> Result<Datastore> {
		// Create the shared blocking threadpool (idempotent)
		surrealdb_kvs::threadpool::initialise();
		// Configure custom options and set the data storage directory
		let opts = Self::options(&config).with_path(path.to_string().into());
		// A RocksDB directory is converted in place by the build below
		if holds_rocksdb_files(path) {
			warn!(
				target: TARGET,
				"The directory {path} holds a RocksDB database, which is now being migrated to \
				 SurrealKV in place. This takes time proportional to its size, needs room for both \
				 copies, and the datastore is not ready until it finishes; the original files are \
				 kept in _rocksdb_backup/"
			);
		}
		// Build the database
		let db =
			TreeBuilder::with_options(opts).build().map_err(|e| Error::Datastore(e.to_string()))?;

		// Create sync components based on sync mode
		let background_flusher = match config.sync_mode {
			SyncMode::Every => {
				info!(target: TARGET, "Sync mode: every transaction commit");
				None
			}
			SyncMode::Interval(interval) => {
				info!(target: TARGET, "Sync mode: background syncing on interval ({}ms)", interval.as_millis());
				Some(Arc::new(BackgroundFlusher::new(db.clone(), interval)?))
			}
			SyncMode::Never => {
				info!(target: TARGET, "Sync mode: never (handled by the OS)");
				None
			}
		};

		// Create and return the datastore
		Ok(Datastore {
			db: Arc::new(db),
			versioned: config.versioned,
			sync: matches!(config.sync_mode, SyncMode::Every),
			shutting_down: Arc::new(AtomicBool::new(false)),
			background_flusher,
		})
	}

	/// Open a new database in the browser's origin private file system.
	///
	/// `name` names the database within the origin. It must run in a
	/// dedicated worker, as only a dedicated worker has the synchronous file
	/// handles that the engine needs, and the database can be open in one
	/// tab at a time: a second open fails with a connection error. Data in a
	/// legacy IndexedDB database of the same name is copied in the first time
	/// the database is opened; IndexedDB itself is only read.
	///
	/// There are no threads, so no commit coordinator and no background
	/// thread. A sync mode of `every` makes each commit flush the log. The
	/// interval mode, which is the default in a browser, runs a task on the
	/// browser's event loop that flushes the log on each tick. `never` leaves
	/// flushing to the browser.
	#[cfg(target_family = "wasm")]
	pub async fn new(name: &str, config: SurrealKvConfig) -> Result<Datastore> {
		// The futures of the browser's file handles are not `Send`, which is sound to
		// assert as the browser build is single-threaded
		common::future::assert_send(async move {
			// The path of the database within the mounted file system
			const DB_PATH: &str = "db";
			// Configure custom options and set the data storage directory
			let opts = Self::options(&config).with_path(DB_PATH.into());
			// Move a legacy IndexedDB database of the same name into the origin
			// private file system. This precedes the open, as the copy is made
			// only into a database that does not exist yet.
			match migrate_from_indexeddb(name, name, opts.clone()).await {
				Ok(IndexedDbMigration::Imported {
					entries,
					bytes,
					skipped,
					..
				}) => {
					info!(target: TARGET, "Imported {entries} rows ({bytes} bytes) from the IndexedDB database `{name}`, skipping {skipped}");
				}
				Ok(IndexedDbMigration::Existing) => {
					info!(target: TARGET, "The database `{name}` exists, so IndexedDB was not read");
				}
				Ok(IndexedDbMigration::NoSource) => {
					info!(target: TARGET, "No IndexedDB database `{name}` to import");
				}
				Ok(_) => {}
				Err(e) if e.is_database_locked() => return Err(open_error(name, e)),
				Err(e) => {
					return Err(Error::Datastore(format!(
						"Importing the IndexedDB database `{name}` failed: {e}"
					)));
				}
			}
			// Mount the files of the database
			let fs = FileSystem::opfs(name).await.map_err(|e| open_error(name, e))?;
			// Build the database
			let db = match TreeBuilder::with_options(opts.with_file_system(fs.clone()))
				.build_async()
				.await
			{
				Ok(db) => db,
				Err(e) => {
					// Release the lock so that the database can be opened again
					let _ = fs.close().await;
					return Err(open_error(name, e));
				}
			};
			// A browser has no commit pipeline of its own to group syncs, so a commit is
			// made durable per transaction (sync=every) or by the interval flusher
			let db = Arc::new(db);
			let flusher_stopped = Arc::new(AtomicBool::new(false));
			match config.sync_mode {
				SyncMode::Every => {
					info!(target: TARGET, "Sync mode: flush the log on every transaction commit");
				}
				SyncMode::Interval(interval) => {
					info!(target: TARGET, "Sync mode: flush the log on an interval ({}ms)", interval.as_millis());
					// The task holds the tree weakly, so that dropping the datastore without
					// `shutdown` is not kept open by the flusher: the task ends on its next tick
					let (db, stopped) = (Arc::downgrade(&db), Arc::clone(&flusher_stopped));
					common::rt::spawn_detached(async move {
						loop {
							common::time::sleep(interval).await;
							if stopped.load(Ordering::Acquire) {
								break;
							}
							let Some(db) = db.upgrade() else {
								break;
							};
							if let Err(e) = db.flush_wal(true) {
								error!(target: TARGET, "An error occurred flushing the WAL buffer: {e}");
							}
						}
					});
				}
				SyncMode::Never => {
					info!(target: TARGET, "Sync mode: handled by the browser");
				}
			}
			// Create and return the datastore
			Ok(Datastore {
				db,
				versioned: config.versioned,
				sync: matches!(config.sync_mode, SyncMode::Every),
				shutting_down: Arc::new(AtomicBool::new(false)),
				fs,
				flusher_stopped,
			})
		})
		.await
	}

	/// Shutdown the database
	///
	/// The tree is closed first and then the file system, which releases the
	/// database's lock on the origin private file system in a browser.
	pub async fn shutdown(&self) -> Result<()> {
		// Refuse every commit that has not applied yet
		self.shutting_down.store(true, Ordering::Release);
		// Stop the interval flusher of a browser database
		#[cfg(target_family = "wasm")]
		self.flusher_stopped.store(true, Ordering::Release);
		// Wait for the background flusher to finish
		#[cfg(not(target_family = "wasm"))]
		if let Some(background_flusher) = &self.background_flusher {
			background_flusher.shutdown()?;
		}
		// Flush WAL before closing
		if let Err(e) = self.db.flush_wal(true) {
			error!(target: TARGET, "An error occurred flushing the WAL buffer to disk: {e}");
		}
		// Close the database
		if let Err(e) = self.db.close().await {
			error!(target: TARGET, "An error occurred closing the database: {e}");
		}
		// Release the files of the database
		// A failure to release them leaves the database locked, which the caller must know
		#[cfg(target_family = "wasm")]
		if let Err(e) = self.fs.close().await {
			error!(target: TARGET, "An error occurred closing the file system: {e}");
			return Err(Error::Datastore(format!(
				"The files of the database could not be released, so it stays locked: {e}"
			)));
		}
		// Nothing to do here
		Ok(())
	}

	/// Start a new transaction
	pub async fn transaction(&self, write: TransactionType) -> Result<Box<dyn Transactable>> {
		// Create a new transaction
		let txn = match write {
			TransactionType::Write => self.db.begin_with_mode(Mode::ReadWrite),
			TransactionType::Read => self.db.begin_with_mode(Mode::ReadOnly),
		}
		.map_err(kvs_error)?;
		// Return the new transaction
		Ok(Box::new(Transaction {
			done: Arc::new(AtomicBool::new(false)),
			write: matches!(write, TransactionType::Write),
			versioned: self.versioned,
			db: Arc::clone(&self.db),
			inner: Arc::new(RwLock::new(txn)),
			savepoints: RwLock::new(SavepointStack::default()),
			sync: self.sync,
			shutting_down: Arc::clone(&self.shutting_down),
		}))
	}
}

impl TransactionBuilder for Datastore {
	fn name(&self) -> &'static str {
		"surrealkv"
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

// TODO: answer `estimate`, `estimate_many` and `approximate` from SurrealKV's own metadata once it
// exposes the size of a key range without reading its entries; until then the trait's defaults
// answer, and `approximate` answers `None`.
impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"surrealkv"
	}

	/// SurrealKV holds the exclusive lock of its directory while it is open.
	fn sole_writer(&self) -> bool {
		true
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done.load(Ordering::Relaxed)
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// The commit oracle validates written keys and keys read with `get_for_update`; a plain read
	/// registers nothing.
	fn plain_reads_tracked(&self) -> bool {
		false
	}

	/// A commit hands the keys of every entry of its batch to the commit pipeline, which
	/// validates them in `CommitPipeline::validate` against the keys of each commit in the
	/// transaction's window that its snapshot does not see (`CommitEntry::is_disjoint_writeset`),
	/// and answers `Error::TransactionWriteConflict` (surrealkv 1.0.0, `src/ring/pipeline.rs`).
	/// The keys are the batch's own, not the read set, so a key set or deleted without a read is
	/// validated; a `put` is a plain read of the key and a `set`, so a blind `set` is validated as
	/// the `put` is.
	fn blind_write_conflicts(&self) -> bool {
		true
	}

	/// A chunk read re-seeks the range and copies each entry out, which another task may do while
	/// the caller runs its rows; each chunk takes the transaction's lock for its own length only.
	fn scan_read_ahead(&self) -> bool {
		true
	}

	/// One acquisition of the transaction's lock serves every range of
	/// [`Transactable::visit_key_ranges`], and `getm` reads all its keys under one acquisition, so
	/// a batch pays the lock once where the same reads one by one pay each.
	fn multi_read(&self) -> bool {
		true
	}

	/// A read-only transaction gives a view of its engine snapshot, and a writable one gives none.
	///
	/// A read-only transaction holds one fixed engine snapshot and an empty write set, so every
	/// read through it is the view. The engine's iterators always merge the transaction's write
	/// set, so a view of a writable transaction would show the owner's later writes through it.
	fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
		if self.write || self.done.load(Ordering::Acquire) {
			return None;
		}
		Some(Arc::new(SurrealKvSnapshot {
			inner: Arc::clone(&self.inner),
			done: Arc::clone(&self.done),
			versioned: self.versioned,
		}))
	}

	/// Read-only transactions give a view; writable ones never do.
	fn view_capabilities(&self) -> ViewCapabilities {
		ViewCapabilities {
			read_only: true,
			writable: false,
		}
	}

	/// Compacts the keys of `range`, or of the whole tree, down to the last level.
	///
	/// The log is flushed and the memtables are written out first, then every table
	/// overlapping the range is merged down the tree, and the last level is rewritten
	/// to drop the tombstones and the versions no reader can see, which frees the
	/// space of deleted data. The call returns once the compaction is done; reads and
	/// writes carry on meanwhile.
	fn compact<'a>(&'a self, range: Option<KeyRange<'a>>) -> BoxFut<'a, Result<()>> {
		// The range is owned up front so the work does not borrow from the caller
		let bounds = range.map(|r| (r.start.to_vec(), r.end.to_vec()));
		let db = Arc::clone(&self.db);
		Box::pin(async move {
			let compact = move || -> Result<()> {
				// Make everything committed so far durable before it is compacted
				db.flush_wal(true).map_err(kvs_error)?;
				let (start, end) = match &bounds {
					Some((start, end)) => (Some(start.as_slice()), Some(end.as_slice())),
					None => (None, None),
				};
				db.compact_range(start, end, &CompactRangeOptions::default()).map_err(kvs_error)
			};
			#[cfg(not(target_family = "wasm"))]
			{
				// Blocking file I/O, so it runs off the async workers
				affinitypool::spawn(compact).await
			}
			#[cfg(target_family = "wasm")]
			{
				compact()
			}
		})
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
			inner.rollback();
			// Continue
			Ok(())
		})
	}

	/// Commits the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn commit(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(self.commit_with(true))
	}

	/// Commits the transaction without syncing it. The write-ahead log applies
	/// commits in order, so a later synced commit makes this one durable with it.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn commit_unsynced(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(self.commit_with(false))
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<bool>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Get the key
			let res = match version {
				Some(ts) => inner.get_at(key.as_slice(), ts).map_err(kvs_error)?.is_some(),
				None => inner.get(key.as_slice()).map_err(kvs_error)?.is_some(),
			};
			// Return result
			Ok(res)
		})
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Get the key
			let res = match version {
				Some(ts) => inner.get_at(key.as_slice(), ts).map_err(kvs_error)?,
				None => inner.get(key.as_slice()).map_err(kvs_error)?,
			};
			// Return result
			Ok(res)
		})
	}

	/// Fetch many keys from the database under one acquisition of the
	/// transaction.
	///
	/// Answers one entry per key, in order, as the trait's default does, and fails with
	/// `TransactionFinished` once the transaction is done.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self, keys))]
	fn getm<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Versioned queries require a versioned datastore
			if !keys.is_empty() {
				self.ensure_versioned(version)?;
			}
			// Load the inner transaction once for every key
			let inner = self.inner.read().await;
			let mut values = Vec::with_capacity(keys.len());
			let mut records = 0u64;
			let mut value_bytes = 0u64;
			for key in keys {
				let found = match version {
					Some(ts) => inner.get_at(key.as_slice(), ts).map_err(kvs_error)?,
					None => inner.get(key.as_slice()).map_err(kvs_error)?,
				};
				if let Some(value) = &found {
					records += 1;
					value_bytes += value.len() as u64;
				}
				values.push(found);
			}
			Ok(GetMultiResult {
				values,
				records,
				value_bytes,
			})
		})
	}

	/// Check many keys under one acquisition of the transaction, merging them in key order against
	/// one iterator over their span.
	///
	/// The iterator and `get`, the read `put` makes, see the same thing: the newest live version of
	/// each key with the transaction's own writes over it, and neither joins the conflict check.
	/// The walk steps over the keys the span holds between two checked keys; once it has stepped
	/// over more than twice as many as it was given (a span much fuller than the keys checked),
	/// the keys left are read one at a time, so a sparse set costs no more than reading each.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self, keys))]
	fn assert_absent<'a>(&'a self, keys: &'a [Key<'a>]) -> BoxFut<'a, Result<Option<usize>>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			let mut order: Vec<usize> = (0..keys.len()).collect();
			order.sort_unstable_by(|a, b| keys[*a].as_slice().cmp(keys[*b].as_slice()));
			let (Some(lo), Some(hi)) = (order.first(), order.last()) else {
				return Ok(None);
			};
			// The span ends just past the greatest key.
			let mut end = keys[*hi].as_slice().to_vec();
			end.push(0);
			// Load the inner transaction once for every key
			let inner = self.inner.read().await;
			let mut first: Option<usize> = None;
			let note = |first: &mut Option<usize>, at: usize| {
				if first.is_none_or(|held| at < held) {
					*first = Some(at);
				}
			};
			let mut iter = inner.range(keys[*lo].as_slice(), end.as_slice()).map_err(kvs_error)?;
			iter.seek_first().map_err(kvs_error)?;
			let mut steps = keys.len().saturating_mul(2).saturating_add(64);
			let mut pos = 0;
			'walk: while pos < order.len() {
				let at = order[pos];
				let want = keys[at].as_slice();
				while iter.valid() && iter.key().user_key() < want {
					if steps == 0 {
						break 'walk;
					}
					steps -= 1;
					iter.next().map_err(kvs_error)?;
				}
				if !iter.valid() {
					// The span holds nothing at or past this key.
					return Ok(first);
				}
				if iter.key().user_key() == want {
					note(&mut first, at);
				}
				pos += 1;
			}
			drop(iter);
			for &at in &order[pos..] {
				if inner.get(keys[at].as_slice()).map_err(kvs_error)?.is_some() {
					note(&mut first, at);
				}
			}
			Ok(first)
		})
	}

	/// Fetch a key from the database, registering the key for
	/// commit-time conflict detection.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn getu<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable, as the conflict
			// registration is only validated when the transaction commits
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Get the key, arming commit-time conflict detection
			let res = inner.get_for_update(key.as_slice()).map_err(kvs_error)?;
			// Return result
			Ok(res)
		})
	}

	/// A locked read joins the transaction's read set, which the commit
	/// oracle checks against the writes of transactions that committed after
	/// this one's snapshot. The read set is never visible to other
	/// transactions.
	fn shared_locked_reads(&self) -> bool {
		true
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
			inner.set(key.as_slice(), &val).map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Insert or update many keys under one acquisition of the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip_all)]
	fn set_many<'a>(&'a self, entries: Vec<(Vec<u8>, Val)>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction once for every key
			let mut inner = self.inner.write().await;
			for (key, val) in entries {
				inner.set(key.as_slice(), &val).map_err(kvs_error)?;
			}
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
			inner.replace(key.as_slice(), &val).map_err(kvs_error)?;
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
			match inner.get(key.as_slice()).map_err(kvs_error)? {
				None => inner.set(key.as_slice(), &val).map_err(kvs_error)?,
				_ => return Err(Error::TransactionKeyAlreadyExists),
			}
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
					inner.set(key.as_slice(), &val).map_err(kvs_error)?
				}
				(None, None) => inner.set(key.as_slice(), &val).map_err(kvs_error)?,
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
			// Delete the key
			if self.versioned {
				inner.soft_delete(key.as_slice()).map_err(kvs_error)?;
			} else {
				inner.delete(key.as_slice()).map_err(kvs_error)?;
			}
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
			if self.versioned {
				match (inner.get(key.as_slice()).map_err(kvs_error)?, chk) {
					(Some(v), Some(w)) if v == w => {
						inner.soft_delete(key.as_slice()).map_err(kvs_error)?
					}
					(None, None) => inner.soft_delete(key.as_slice()).map_err(kvs_error)?,
					_ => return Err(Error::TransactionConditionNotMet),
				};
			} else {
				match (inner.get(key.as_slice()).map_err(kvs_error)?, chk) {
					(Some(v), Some(w)) if v == w => {
						inner.delete(key.as_slice()).map_err(kvs_error)?
					}
					(None, None) => inner.delete(key.as_slice()).map_err(kvs_error)?,
					_ => return Err(Error::TransactionConditionNotMet),
				};
			}
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
			// Delete the key
			inner.delete(key.as_slice()).map_err(kvs_error)?;
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
				(Some(v), Some(w)) if v == w => inner.delete(key.as_slice()).map_err(kvs_error)?,
				(None, None) => inner.delete(key.as_slice()).map_err(kvs_error)?,
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
			// Versioned queries require a versioned datastore
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
			// Build the counting closure, which borrows the key range
			let count_range = move || -> Result<_> {
				// Store the count
				let mut count = 0;
				//
				match version {
					Some(ts) => {
						// Create the iterator, with tombstones visible to detect deleted keys
						let mut iter = history_iter(&inner, beg.as_slice(), end.as_slice())?;
						// Seek to the first key
						iter.seek_first().map_err(kvs_error)?;
						// History entries are sorted (key ASC, timestamp DESC),
						// so the first entry with timestamp <= ts is the latest
						// version for each key. We skip newer versions and only
						// count non-tombstone entries.
						while iter.valid() {
							let key_ref = iter.key();
							// This is the latest relevant version for this key
							if key_ref.timestamp() <= ts {
								// Store the current user key
								let user_key = key_ref.user_key().to_vec();
								// Check if this is a tombstone
								let is_tombstone = key_ref.is_tombstone();
								// Skip remaining older versions of this key
								loop {
									iter.next().map_err(kvs_error)?;
									if !iter.valid() || iter.key().user_key() != user_key {
										break;
									}
								}
								// Count values which are not deletes
								if !is_tombstone {
									count += 1;
								}
							} else {
								// This version is newer, skip it
								iter.next().map_err(kvs_error)?;
							}
						}
					}
					None => {
						// Create the iterator
						let mut iter =
							inner.range(beg.as_slice(), end.as_slice()).map_err(kvs_error)?;
						// Seek to the first key
						iter.seek_first().map_err(kvs_error)?;
						// Loop over all keys
						while iter.valid() {
							count += 1;
							iter.next().map_err(kvs_error)?;
						}
					}
				}
				// Return result
				Ok(count)
			};
			// Execute on the blocking threadpool
			//
			// SAFETY: `count_range` borrows `rng` through `beg`/`end` and holds
			// the inner read guard, so the returned future must not be leaked
			// while those borrows are live. It is awaited inline here and never
			// stored, so its destructor — which blocks until the worker has
			// stopped touching the borrows — always runs before they expire.
			let res = unsafe { affinitypool::spawn_local(count_range) }.await?;
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
			// Versioned queries require a versioned datastore
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
			// Retrieve the scan range
			let res = match version {
				Some(ts) => {
					// Create the iterator
					let mut iter = history_iter(&inner, beg.as_slice(), end.as_slice())?;
					// Seek to the first key
					iter.seek_first().map_err(kvs_error)?;
					// Consume the iterator
					let mut cursor = HistoryCursor {
						inner: Box::new(iter),
						dir: Direction::Forward,
						ts,
					};
					consume_keys(&mut cursor, limit, skip)?
				}
				None => {
					// Create the iterator
					let mut iter =
						inner.range(beg.as_slice(), end.as_slice()).map_err(kvs_error)?;
					// Seek to the first key
					iter.seek_first().map_err(kvs_error)?;
					// Consume the iterator
					let mut cursor = RangeCursor {
						inner: Box::new(iter),
						dir: Direction::Forward,
					};
					consume_keys(&mut cursor, limit, skip)?
				}
			};
			// Return result
			Ok(res)
		})
	}

	/// Visit the keys of each range under one acquisition of the transaction's lock, one iterator
	/// per range, as [`Self::keys`] reads each range. The current state is read on the range
	/// iterator, each key borrowed from it; a version is read on the history iterator through
	/// [`history_forward`], its keys copied into one buffer reused across keys and ranges.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self, ranges, f))]
	fn visit_key_ranges<'a>(
		&'a self,
		ranges: &'a [KeyRange<'a>],
		limit: u32,
		version: Option<u64>,
		f: &'a mut dyn RangeKeyVisitor,
	) -> BoxFut<'a, Result<ScanChunkStats>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			let mut stats = ScanChunkStats::default();
			if limit == 0 || ranges.is_empty() {
				return Ok(stats);
			}
			// Load the inner transaction once for every range
			let inner = self.inner.read().await;
			let mut buf = Vec::new();
			'ranges: for (at, rng) in ranges.iter().enumerate() {
				let (beg, end) = (rng.start.as_slice(), rng.end.as_slice());
				let mut taken = 0u32;
				match version {
					Some(ts) => {
						let mut iter = history_iter(&inner, beg, end)?;
						iter.seek_first().map_err(kvs_error)?;
						while taken < limit && history_forward(&mut iter, ts, &mut buf, None)? {
							taken += 1;
							stats.rows += 1;
							stats.key_bytes += buf.len() as u64;
							if f(at, &buf)?.is_break() {
								break 'ranges;
							}
						}
					}
					None => {
						let mut iter = inner.range(beg, end).map_err(kvs_error)?;
						iter.seek_first().map_err(kvs_error)?;
						while taken < limit && iter.valid() {
							let flow = {
								let key = iter.key();
								let key = key.user_key();
								stats.rows += 1;
								stats.key_bytes += key.len() as u64;
								f(at, key)?
							};
							taken += 1;
							if flow.is_break() {
								break 'ranges;
							}
							if taken == limit {
								break;
							}
							iter.next().map_err(kvs_error)?;
						}
					}
				}
			}
			Ok(stats)
		})
	}

	/// Count the keys within a range, examining at most `budget` of them
	/// ([`Transactable::count_bounded`]). The current state is counted on the
	/// range iterator itself, stopping on the key the next call resumes at; a
	/// version is counted on the history with tombstones visible, as
	/// [`Self::count`] counts it, each user key examined charged to the budget
	/// whether it is live or deleted as of the version.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn count_bounded<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
		budget: u32,
	) -> BoxFut<'a, Result<(usize, Option<Key<'static>>)>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			let budget = budget.max(1) as usize;
			let beg = rng.start;
			let end = rng.end;
			let inner = self.inner.read().await;
			let mut counted = 0;
			let resume = match version {
				Some(ts) => {
					// As `count` does: tombstones visible, so a key deleted as
					// of `ts` is examined, charged to the budget, and not
					// counted.
					let mut iter = history_iter(&inner, beg.as_slice(), end.as_slice())?;
					iter.seek_first().map_err(kvs_error)?;
					let mut examined = 0;
					while iter.valid() && examined < budget {
						let key_ref = iter.key();
						if key_ref.timestamp() > ts {
							// A version newer than `ts`: not this key's state.
							iter.next().map_err(kvs_error)?;
							continue;
						}
						// The latest version as of `ts`; its older versions
						// are passed over.
						let user_key = key_ref.user_key().to_vec();
						let is_tombstone = key_ref.is_tombstone();
						loop {
							iter.next().map_err(kvs_error)?;
							if !iter.valid() || iter.key().user_key() != user_key {
								break;
							}
						}
						examined += 1;
						if !is_tombstone {
							counted += 1;
						}
					}
					iter.valid().then(|| iter.key().user_key().to_vec())
				}
				None => {
					let mut iter =
						inner.range(beg.as_slice(), end.as_slice()).map_err(kvs_error)?;
					iter.seek_first().map_err(kvs_error)?;
					while counted < budget && iter.valid() {
						counted += 1;
						iter.next().map_err(kvs_error)?;
					}
					iter.valid().then(|| iter.key().user_key().to_vec())
				}
			};
			Ok((counted, resume.map(Key::from)))
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
			// Versioned queries require a versioned datastore
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
			// Retrieve the scan range
			let res = match version {
				Some(ts) => {
					// Create the iterator
					let mut iter = history_iter(&inner, beg.as_slice(), end.as_slice())?;
					// Seek to the last key
					iter.seek_last().map_err(kvs_error)?;
					// Consume the iterator
					let mut cursor = HistoryCursor {
						inner: Box::new(iter),
						dir: Direction::Backward,
						ts,
					};
					consume_keys(&mut cursor, limit, skip)?
				}
				None => {
					// Create the iterator
					let mut iter =
						inner.range(beg.as_slice(), end.as_slice()).map_err(kvs_error)?;
					// Seek to the last key
					iter.seek_last().map_err(kvs_error)?;
					// Consume the iterator
					let mut cursor = RangeCursor {
						inner: Box::new(iter),
						dir: Direction::Backward,
					};
					consume_keys(&mut cursor, limit, skip)?
				}
			};
			// Return result
			Ok(res)
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
			// Versioned queries require a versioned datastore
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
			// Retrieve the scan range
			let res = match version {
				Some(ts) => {
					// Create the iterator
					let mut iter = history_iter(&inner, beg.as_slice(), end.as_slice())?;
					// Seek to the first key
					iter.seek_first().map_err(kvs_error)?;
					// Consume the iterator
					let mut cursor = HistoryCursor {
						inner: Box::new(iter),
						dir: Direction::Forward,
						ts,
					};
					consume_vals(&mut cursor, limit, skip)?
				}
				None => {
					// Create the iterator
					let mut iter =
						inner.range(beg.as_slice(), end.as_slice()).map_err(kvs_error)?;
					// Seek to the first key
					iter.seek_first().map_err(kvs_error)?;
					// Consume the iterator
					let mut cursor = RangeCursor {
						inner: Box::new(iter),
						dir: Direction::Forward,
					};
					consume_vals(&mut cursor, limit, skip)?
				}
			};
			// Return result
			Ok(res)
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
			// Versioned queries require a versioned datastore
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
			// Retrieve the scan range
			let res = match version {
				Some(ts) => {
					// Create the iterator
					let mut iter = history_iter(&inner, beg.as_slice(), end.as_slice())?;
					// Seek to the last key
					iter.seek_last().map_err(kvs_error)?;
					// Consume the iterator
					let mut cursor = HistoryCursor {
						inner: Box::new(iter),
						dir: Direction::Backward,
						ts,
					};
					consume_vals(&mut cursor, limit, skip)?
				}
				None => {
					// Create the iterator
					let mut iter =
						inner.range(beg.as_slice(), end.as_slice()).map_err(kvs_error)?;
					// Seek to the last key
					iter.seek_last().map_err(kvs_error)?;
					// Consume the iterator
					let mut cursor = RangeCursor {
						inner: Box::new(iter),
						dir: Direction::Backward,
					};
					consume_vals(&mut cursor, limit, skip)?
				}
			};
			// Return result
			Ok(res)
		})
	}

	/// Set a new save point on the transaction.
	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// A read-only transaction takes no write lock except in `cancel`, so views and their
			// cursors never wait on the owner.
			if !self.write {
				return Err(Error::TransactionReadonly);
			}
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

	/// Versions are SurrealKV's commit timestamps, nanoseconds since the Unix
	/// epoch from the engine's own clock. The change feed is keyed by the
	/// default stamps `timestamp` mints instead, which the default
	/// `timestamp_impl` describes.
	fn version_impl(&self) -> BoxTimeStampImpl {
		Box::new(SurrealKvTimeStampImpl)
	}
}

struct SurrealKvTimeStamp(u64);

impl TimeStamp for SurrealKvTimeStamp {
	fn as_versionstamp(&self) -> u128 {
		self.0 as u128
	}

	fn as_datetime(&self) -> Option<DateTime<Utc>> {
		Some(DateTime::from_timestamp_nanos(self.0 as i64))
	}

	fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp> {
		let nanos: u64 = duration.as_nanos().try_into().ok()?;
		Some(BoxTimeStamp::new(SurrealKvTimeStamp(self.0.checked_sub(nanos)?)))
	}

	fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8] {
		bytes[..8].copy_from_slice(&self.0.to_be_bytes());
		&bytes[..8]
	}
}

struct SurrealKvTimeStampImpl;

impl TimeStampImpl for SurrealKvTimeStampImpl {
	fn earliest(&self) -> BoxTimeStamp {
		BoxTimeStamp::new(SurrealKvTimeStamp(0))
	}

	fn create_from_versionstamp(&self, version: u128) -> Option<BoxTimeStamp> {
		Some(BoxTimeStamp::new(SurrealKvTimeStamp(version.try_into().ok()?)))
	}

	fn create_from_datetime(&self, dt: DateTime<Utc>) -> Option<BoxTimeStamp> {
		let nanos = dt.timestamp_nanos_opt()?;
		if nanos < 0 {
			return None;
		}
		Some(BoxTimeStamp::new(SurrealKvTimeStamp(nanos as u64)))
	}

	fn decode(&self, bytes: &[u8]) -> Result<BoxTimeStamp> {
		let bytes = <[u8; 8]>::try_from(bytes).map_err(|_| {
			Error::TimestampInvalid("encoded timestamp not a valid length".to_string())
		})?;
		Ok(BoxTimeStamp::new(SurrealKvTimeStamp(u64::from_be_bytes(bytes))))
	}
}

/// Open an iterator over every version of the keys in `beg..end`, tombstones included. Every
/// versioned read (`keys`, `keysr`, `scan`, `scanr`, `count`, `count_bounded`) needs the
/// tombstones to tell a key deleted as of a version from one which was merely written before it.
fn history_iter<'a>(tx: &'a Tx, beg: &'a [u8], end: &'a [u8]) -> Result<impl LSMIterator + 'a> {
	let opts = HistoryOptions::new().with_tombstones(true);
	tx.history_with_options(beg, end, &opts).map_err(kvs_error)
}

// A cursor advances through entries and returns the next key or key-value pair.
// The cursor abstraction allows consume_keys and consume_vals to work with
// both range iterators and history iterators with timestamp filtering.
trait Cursor {
	/// Returns the next key from the cursor, or None if exhausted
	fn next_key(&mut self) -> Result<Option<Vec<u8>>>;
	/// Returns the next key-value pair from the cursor, or None if exhausted
	fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Val)>>;
}

// A cursor wrapping a range iterator
struct RangeCursor<'a> {
	inner: Box<dyn LSMIterator + 'a>,
	dir: Direction,
}

impl Cursor for RangeCursor<'_> {
	fn next_key(&mut self) -> Result<Option<Vec<u8>>> {
		if self.inner.valid() {
			let key = self.inner.key().user_key().to_vec();
			match self.dir {
				Direction::Forward => self.inner.next().map_err(kvs_error)?,
				Direction::Backward => self.inner.prev().map_err(kvs_error)?,
			};
			return Ok(Some(key));
		}
		Ok(None)
	}

	fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Val)>> {
		if self.inner.valid() {
			let key = self.inner.key().user_key().to_vec();
			let value = self.inner.value().map_err(kvs_error)?;
			match self.dir {
				Direction::Forward => self.inner.next().map_err(kvs_error)?,
				Direction::Backward => self.inner.prev().map_err(kvs_error)?,
			};
			return Ok(Some((key, value)));
		}
		Ok(None)
	}
}

// A cursor wrapping a history iterator with timestamp filtering.
//
// The iterator is opened by `history_iter`, with tombstones visible, so that the newest version
// of a key at or before `ts` is always the state of that key as of `ts`: a value, or a tombstone,
// in which case the key was deleted as of `ts` and is not yielded. A key whose versions all come
// after `ts` is not yielded either. History entries are sorted (key ASC, timestamp DESC), so
// forward iteration meets the newest version of each key first, and backward iteration the oldest.
struct HistoryCursor<'a> {
	inner: Box<dyn LSMIterator + 'a>,
	dir: Direction,
	ts: u64,
}

impl HistoryCursor<'_> {
	/// The next visible entry, with its value when `val` is given.
	fn step(&mut self, key: &mut Vec<u8>, val: Option<&mut Vec<u8>>) -> Result<bool> {
		match self.dir {
			Direction::Forward => history_forward(&mut *self.inner, self.ts, key, val),
			Direction::Backward => history_backward(&mut *self.inner, self.ts, key, val),
		}
	}
}

impl Cursor for HistoryCursor<'_> {
	fn next_key(&mut self) -> Result<Option<Vec<u8>>> {
		let mut key = Vec::new();
		Ok(self.step(&mut key, None)?.then_some(key))
	}

	fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Val)>> {
		let mut key = Vec::new();
		let mut val = Vec::new();
		Ok(self.step(&mut key, Some(&mut val))?.then_some((key, val)))
	}
}

// History entries are sorted (key ASC, timestamp DESC), so forward iteration yields the newest
// version of a key first, and backward iteration yields its oldest version first. These two
// functions are the one rule that chooses which version of a key a read as of `ts` sees; every
// read of a version, whole-range or cursor, goes through them. The iterator has tombstones
// visible, so the newest version at or before `ts` is the state of the key as of `ts`: a value,
// or a tombstone, in which case the key was deleted as of `ts` and is not yielded.
//
// Each leaves the chosen user key in `key` and, when `val` is given, its value in `val`, and
// returns whether there was one. The iterator is left on the first entry of a later user key, or
// invalid.

/// Forward: the first entry at or before `ts` is the key's latest version, and the key's older
/// versions are passed over.
fn history_forward<I: LSMIterator + ?Sized>(
	iter: &mut I,
	ts: u64,
	key: &mut Vec<u8>,
	mut val: Option<&mut Vec<u8>>,
) -> Result<bool> {
	while iter.valid() {
		let entry = iter.key();
		// A version newer than `ts` is not the state of this key as of `ts`
		if entry.timestamp() > ts {
			iter.next().map_err(kvs_error)?;
			continue;
		}
		let live = !entry.is_tombstone();
		key.clear();
		key.extend_from_slice(entry.user_key());
		if live && let Some(val) = &mut val {
			**val = iter.value().map_err(kvs_error)?;
		}
		// Skip the remaining older versions of this key
		loop {
			iter.next().map_err(kvs_error)?;
			if !iter.valid() || iter.key().user_key() != key.as_slice() {
				break;
			}
		}
		// Pass over a key which was deleted as of `ts`
		if live {
			return Ok(true);
		}
	}
	Ok(false)
}

/// Backward: every version of the key is passed, oldest first, and the last one at or before `ts`
/// is the key's latest version. A key with no version at or before `ts`, or whose latest version
/// is a tombstone, is skipped.
fn history_backward<I: LSMIterator + ?Sized>(
	iter: &mut I,
	ts: u64,
	key: &mut Vec<u8>,
	mut val: Option<&mut Vec<u8>>,
) -> Result<bool> {
	while iter.valid() {
		let mut live = false;
		key.clear();
		key.extend_from_slice(iter.key().user_key());
		while iter.valid() && iter.key().user_key() == key.as_slice() {
			let entry = iter.key();
			if entry.timestamp() <= ts {
				live = !entry.is_tombstone();
				if live && let Some(val) = &mut val {
					**val = iter.value().map_err(kvs_error)?;
				}
			}
			// Continue to the previous version
			iter.prev().map_err(kvs_error)?;
		}
		if live {
			return Ok(true);
		}
	}
	Ok(false)
}

// Consume and iterate over only keys
fn consume_keys(cursor: &mut impl Cursor, limit: u32, skip: u32) -> Result<KeysResult> {
	// Skip entries efficiently by discarding cursor results
	for _ in 0..skip {
		if cursor.next_key()?.is_none() {
			return Ok(KeysResult::default());
		}
	}
	let mut key_bytes = 0u64;
	// Create the result set
	let mut keys = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while keys.len() < limit as usize {
		// Check the key
		if let Some(key) = cursor.next_key()? {
			key_bytes += key.len() as u64;
			keys.push(key);
		} else {
			break;
		}
	}
	Ok(KeysResult {
		keys,
		key_bytes,
	})
}

// Consume and iterate over keys and values
fn consume_vals(cursor: &mut impl Cursor, limit: u32, skip: u32) -> Result<ScanResult> {
	// Skip entries efficiently by discarding cursor results
	for _ in 0..skip {
		if cursor.next_entry()?.is_none() {
			return Ok(ScanResult::default());
		}
	}
	// Track the cumulative key/value bytes for the scan metrics.
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	// Create the result set
	let mut values = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while values.len() < limit as usize {
		// Check the key and value
		if let Some((key, value)) = cursor.next_entry()? {
			key_bytes += key.len() as u64;
			value_bytes += value.len() as u64;
			values.push((key, value));
		} else {
			break;
		}
	}
	Ok(ScanResult {
		values,
		key_bytes,
		value_bytes,
	})
}

#[cfg(test)]
mod commit_error_test {
	use super::*;

	#[test]
	fn only_errors_raised_before_writing_are_definite() {
		let conflict = commit_error(surrealkv::Error::TransactionWriteConflict);
		assert!(matches!(conflict, Error::TransactionConflict(_)), "{conflict:?}");
		let retry = commit_error(surrealkv::Error::TransactionRetry);
		assert!(matches!(retry, Error::TransactionConflict(_)), "{retry:?}");
		let readonly = commit_error(surrealkv::Error::TransactionReadOnly);
		assert!(matches!(readonly, Error::TransactionReadonly), "{readonly:?}");
		let closed = commit_error(surrealkv::Error::TransactionClosed);
		assert!(matches!(closed, Error::TransactionFinished), "{closed:?}");
		let too_large = commit_error(surrealkv::Error::BatchTooLarge);
		assert!(matches!(too_large, Error::TransactionTooLarge), "{too_large:?}");

		// Raised both before and after the write, so conservatively unknown.
		let fsync = surrealkv::Error::Io(Arc::new(std::io::Error::other("fsync failed")));
		for e in
			[fsync, surrealkv::Error::Wal("short write".into()), surrealkv::Error::PipelineStall]
		{
			let e = commit_error(e);
			assert!(matches!(e, Error::CommitOutcomeUnknown(_)), "{e:?}");
		}
	}
}
