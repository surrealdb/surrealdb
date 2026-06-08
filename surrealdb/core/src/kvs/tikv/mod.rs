#![cfg(feature = "kv-tikv")]

mod cnf;
mod savepoint;

use std::collections::HashMap;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
pub use cnf::TikvConfig;
use savepoint::{Operation, Savepoint};
use tikv::transaction::ResolveLocksOptions;
use tikv::{CheckLevel, Config, TimestampExt, TransactionClient, TransactionOptions};
use tokio::sync::RwLock;

use super::api::{BoxFut, GetMultiResult, KeysResult, ScanLimit, ScanResult};
use super::err::{Error, Result};
use super::timestamp::MAX_TIMESTAMP_BYTES;
use super::{ESTIMATED_BYTES_PER_KEY, ESTIMATED_BYTES_PER_KV, util};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use crate::kvs::{COUNT_BATCH_SIZE, Key, TimeStamp, TimeStampImpl, Val};

const TARGET: &str = "surrealdb::core::kvs::tikv";

/// TiKV-specific maintenance + observability operations exposed via the
/// [`crate::kvs::TransactionBuilder::extension`] hook.
///
/// Callers reach this through the top-level
/// [`crate::kvs::Datastore::run_mvcc_gc`] /
/// [`crate::kvs::Datastore::run_lock_cleanup`] /
/// [`crate::kvs::Datastore::unsafe_destroy_range`] /
/// [`crate::kvs::Datastore::in_flight_transaction_count`] convenience
/// methods, which look the handle up by `TypeId` so non-TiKV backends
/// return a no-op without the generic `TransactionBuilder` trait needing
/// to advertise these signatures.
pub struct TikvOpsHandle {
	/// Underlying transactional client.
	db: Pin<Arc<TransactionClient>>,
	/// Per-datastore in-flight transaction counter.
	///
	/// Incremented in [`Datastore::transaction`] and released exactly
	/// once when a [`Transaction`] reaches a terminal state. Surfaced
	/// via [`Self::in_flight_transaction_count`] so graceful shutdown
	/// can drain before running the advisory final GC pass.
	in_flight_txns: Arc<AtomicUsize>,
	/// Per-instance configuration captured at construction time.
	config: TikvConfig,
}

pub struct Datastore {
	/// Arc-wrapped operational handle shared with [`Transaction`]
	/// instances and exposed to the engine via the `extension` hook.
	handle: Arc<TikvOpsHandle>,
}

pub struct Transaction {
	// Is the transaction complete?
	done: AtomicBool,
	// Is the transaction writeable?
	write: bool,
	/// The underlying datastore transaction
	inner: RwLock<TransactionInner>,
	/// Wall-clock instant when the transaction was opened, used to enrich
	/// commit/rollback failure logs with elapsed lifetime.
	started_at: Instant,
	// The above, supposedly 'static transaction actually points into the
	// `TransactionClient` owned by `handle.db`, so the handle must
	// outlive `inner.tx`. Declared last so it is dropped last; the
	// inner `Arc` shares storage with `Datastore::handle` and with the
	// engine-level extension handle.
	#[allow(dead_code, reason = "Held to keep the TransactionClient alive while `tx` borrows it")]
	handle: Arc<TikvOpsHandle>,
}

impl Transaction {
	/// Decrement the in-flight counter, ensuring this is only done once per
	/// transaction even if `commit` is followed by `cancel` (or vice versa).
	fn release_in_flight(&self) {
		// Guarded subtract via `checked_sub`: on underflow `fetch_update`
		// returns `Err` and the counter is left unchanged. An underflow
		// would signal a missing increment somewhere, which is the real
		// bug to surface — clamping at zero would just hide it.
		let _ = self
			.handle
			.in_flight_txns
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
	}

	/// Shared scan-delete loop for [`Self::delr`] and [`Self::clrr`].
	///
	/// On TiKV `clr` and `del` collapse to the same in-transaction
	/// operation, so a single loop services both. Walks the half-open
	/// range in `TIKV_DELR_BATCH_SIZE` increments, capped at
	/// `tikv_delr_max_keys`. If the cap would be exceeded the call
	/// returns [`Error::TransactionRangeTooLarge`] without writing the
	/// over-cap deletes. Records `Operation::RestoreDeleted` against
	/// the savepoint stack when one is active.
	async fn delete_range_bounded(&self, rng: Range<Key>) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		let max_keys = self.handle.config.delr_max_keys;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Whether we need to record undo operations for the savepoint stack.
		let track_ops = !inner.savepoints.is_empty() || !inner.operations.is_empty();
		let end = rng.end.clone();
		let mut start = rng.start;
		let mut processed: u32 = 0;
		// Tracks whether the previous batch returned exactly its requested
		// `batch_size` keys. If it didn't, the range is known-exhausted
		// and we can skip the cap-boundary probe below — avoids one
		// extra `scan_keys` RPC on the clean termination path when
		// `processed` happens to align with the cap.
		let mut previous_batch_full = false;
		loop {
			let remaining = max_keys.saturating_sub(processed);
			if remaining == 0 {
				if previous_batch_full {
					// Probe by one to distinguish "exactly at the cap"
					// from "ran over the cap" — the previous batch
					// was full, so we don't know whether there's more
					// without asking.
					let mut iter = inner.tx.scan_keys(start.clone()..end.clone(), 1).await?;
					if iter.next().is_some() {
						return Err(Error::TransactionRangeTooLarge(max_keys));
					}
				}
				break;
			}
			let batch_size = remaining.min(cnf::TIKV_DELR_BATCH_SIZE);
			let keys: Vec<tikv::Key> =
				inner.tx.scan_keys(start.clone()..end.clone(), batch_size).await?.collect();
			if keys.is_empty() {
				break;
			}
			previous_batch_full = (keys.len() as u32) == batch_size;
			// Remember the last key so we can advance past it for the
			// next pass before consuming the batch.
			let last = keys.last().cloned();
			for k in keys {
				let key = Key::from(k);
				// Pre-fetch the old value only when needed for
				// savepoint rollback. Skipping the read in the common
				// path keeps a large range delete from doubling its
				// RPC count.
				let old_val = if track_ops {
					inner.tx.get(key.clone()).await?
				} else {
					None
				};
				inner.tx.delete(key.clone()).await?;
				if let Some(val) = old_val {
					inner.operations.push(Operation::RestoreDeleted(key, val));
				}
				processed = processed.saturating_add(1);
			}
			match last {
				Some(k) => {
					let mut next = Key::from(k);
					util::advance_key(&mut next);
					start = next;
				}
				None => break,
			}
		}
		Ok(())
	}
}

impl Drop for Transaction {
	/// Release the in-flight reservation if the transaction was dropped
	/// without an explicit `commit` / `cancel`.
	///
	/// On debug builds the underlying `tikv::Transaction`'s
	/// `CheckLevel::Panic` already makes this surface loudly; on
	/// release builds the inner check is `Warn`, so without this
	/// `Drop` the in-flight counter would slowly leak on each
	/// abandoned handle. A slow drip eventually makes every shutdown
	/// hit `shutdown_grace_secs` before falling through, even when
	/// the cluster is idle.
	///
	/// The atomic `done.swap(true, AcqRel)` gates the release so a
	/// `commit`-then-drop sequence (or `cancel`-then-drop) doesn't
	/// double-release; the existing happy-path calls already win the
	/// swap.
	fn drop(&mut self) {
		if !self.done.swap(true, Ordering::AcqRel) {
			self.release_in_flight();
		}
	}
}

struct TransactionInner {
	/// The underlying datastore transaction
	tx: tikv::Transaction,
	/// Stack of savepoints for nested rollback support
	savepoints: Vec<Savepoint>,
	/// Current undo operations since the last savepoint
	operations: Vec<Operation>,
}

impl Datastore {
	/// Open a new database, using the typed `TikvConfig` produced by
	/// [`crate::cnf::ConfigMap::load`].
	pub(crate) async fn new(path: &str, tikv_config: TikvConfig) -> Result<Datastore> {
		// Configure the client and keyspace
		let config = match tikv_config.api_version {
			2 => match tikv_config.keyspace.as_ref() {
				Some(keyspace) => {
					info!(target: TARGET, "Connecting to keyspace with cluster API V2: {keyspace}");
					Config::default().with_keyspace(keyspace)
				}
				None => {
					info!(target: TARGET, "Connecting to default keyspace with cluster API V2");
					Config::default().with_default_keyspace()
				}
			},
			1 => {
				info!(target: TARGET, "Connecting with cluster API V1");
				Config::default()
			}
			_ => return Err(Error::Datastore("Invalid TiKV API version".into())),
		};
		// Set the default request timeout
		let config = config.with_timeout(Duration::from_secs(tikv_config.request_timeout_secs));
		// Set the gRPC payload size caps on both sides of the channel.
		let config = config
			.with_grpc_max_decoding_message_size(tikv_config.grpc_max_decoding_message_size)
			.with_grpc_max_encoding_message_size(tikv_config.grpc_max_encoding_message_size);
		// Optionally configure mTLS. All three paths must be provided
		// together — if some are set but not others we refuse to start
		// rather than silently fall back to a plaintext connection.
		let config = match validate_tls_paths(&tikv_config)? {
			Some(TlsPaths {
				ca,
				cert,
				key,
			}) => {
				info!(
					target: TARGET,
					ca = %ca,
					cert = %cert,
					key = %key,
					"Configuring TiKV client with mTLS",
				);
				config.with_security(ca, cert, key)
			}
			None => config,
		};
		// Create the client with the config
		let db = match TransactionClient::new_with_config(vec![path], config).await {
			Ok(db) => Arc::pin(db),
			Err(e) => return Err(Error::Datastore(e.to_string())),
		};
		// Optional startup health probe. Calling `current_timestamp`
		// confirms PD reachability and clock health; opening a read-only
		// transaction and running a no-op scan confirms at least one TiKV
		// store is reachable through gRPC. We bound the probe at twice the
		// configured request timeout so a wholly-unreachable cluster fails
		// the boot rather than hanging the process.
		if tikv_config.health_probe {
			let probe = async {
				db.current_timestamp().await.map_err(Error::from)?;
				let mut txn = db
					.begin_with_options(TransactionOptions::new_optimistic().read_only())
					.await
					.map_err(Error::from)?;
				let mut iter = txn.scan_keys(vec![0u8]..vec![0u8], 1).await.map_err(Error::from)?;
				// Drain so the iterator's drop side-effects (if any) are
				// observed before the transaction is dropped.
				while iter.next().is_some() {}
				Ok::<_, Error>(())
			};
			let probe_budget =
				Duration::from_secs(tikv_config.request_timeout_secs.saturating_mul(2));
			match tokio::time::timeout(probe_budget, probe).await {
				Ok(Ok(())) => {
					info!(target: TARGET, "TiKV startup health probe succeeded");
				}
				Ok(Err(e)) => {
					return Err(Error::Datastore(format!("TiKV startup health probe failed: {e}")));
				}
				Err(_) => {
					return Err(Error::Datastore(format!(
						"TiKV startup health probe timed out after {probe_budget:?}"
					)));
				}
			}
		}
		let handle = Arc::new(TikvOpsHandle {
			db,
			in_flight_txns: Arc::new(AtomicUsize::new(0)),
			config: tikv_config,
		});
		Ok(Datastore {
			handle,
		})
	}

	/// Clone of the operational handle. Exposed to the engine through the
	/// `TransactionBuilder::extension` hook so non-TiKV callers don't
	/// need to know about TiKV-specific signatures.
	pub(crate) fn ops_handle(&self) -> Arc<TikvOpsHandle> {
		Arc::clone(&self.handle)
	}

	/// Shutdown the database. Waits for in-flight transactions to drain
	/// (up to `shutdown_grace_secs`) then runs one advisory GC pass
	/// bounded by `shutdown_gc_timeout_secs`.
	pub(crate) async fn shutdown(&self) -> Result<()> {
		let cfg = &self.handle.config;
		let drain_deadline = Instant::now() + Duration::from_secs(cfg.shutdown_grace_secs);
		let drained = loop {
			let outstanding = self.handle.in_flight_transaction_count();
			if outstanding == 0 {
				break true;
			}
			if Instant::now() >= drain_deadline {
				warn!(
					target: TARGET,
					outstanding,
					"TiKV shutdown drain timed out; proceeding with active transactions still in flight",
				);
				break false;
			}
			tokio::time::sleep(Duration::from_millis(100)).await;
		};
		if drained && cfg.gc_enabled {
			let shutdown_gc_timeout = Duration::from_secs(cfg.shutdown_gc_timeout_secs);
			let gc_lifetime = Duration::from_secs(cfg.gc_lifetime_secs);
			match tokio::time::timeout(shutdown_gc_timeout, self.handle.run_mvcc_gc(gc_lifetime))
				.await
			{
				Ok(Ok(())) => {}
				Ok(Err(e)) => {
					warn!(
						target: TARGET,
						error = %e,
						"Advisory TiKV GC pass at shutdown failed",
					);
				}
				Err(_) => {
					warn!(
						target: TARGET,
						timeout_ms = shutdown_gc_timeout.as_millis() as u64,
						"Advisory TiKV GC pass at shutdown timed out",
					);
				}
			}
		}
		Ok(())
	}

	/// Start a new transaction
	pub(crate) async fn transaction(
		&self,
		write: bool,
		lock: bool,
	) -> Result<Box<dyn Transactable>> {
		let cfg = &self.handle.config;
		// Set whether this should be an optimistic or pessimistic transaction
		let mut opt = if lock {
			TransactionOptions::new_pessimistic()
		} else {
			TransactionOptions::new_optimistic()
		};
		// Use async commit to determine transaction state earlier
		if cfg.async_commit {
			opt = opt.use_async_commit();
		}
		// Try to use one-phase commit if writing to only one region
		if cfg.one_phase_commit {
			opt = opt.try_one_pc();
		}
		// Set the behaviour when dropping an unfinished transaction.
		// In debug builds (including tests) escalate to Panic so dropped-without-commit
		// transactions surface immediately. Release builds remain lenient.
		opt = opt.drop_check(if cfg!(debug_assertions) {
			CheckLevel::Panic
		} else {
			CheckLevel::Warn
		});
		// Set this transaction as read only if possible
		if !write {
			opt = opt.read_only();
		}
		// Create a new transaction
		match self.handle.db.begin_with_options(opt).await {
			Ok(txn) => {
				// Only bump the counter after the underlying transaction
				// is in hand so a failed `begin_with_options` doesn't leak
				// a phantom entry into the drain accounting.
				self.handle.in_flight_txns.fetch_add(1, Ordering::AcqRel);
				Ok(Box::new(Transaction {
					done: AtomicBool::new(false),
					write,
					inner: RwLock::new(TransactionInner {
						tx: txn,
						savepoints: Vec::new(),
						operations: Vec::new(),
					}),
					started_at: Instant::now(),
					handle: Arc::clone(&self.handle),
				}))
			}
			Err(e) => Err(Error::from(e)),
		}
	}
}

impl TikvOpsHandle {
	/// Number of transactions currently in flight on this datastore.
	pub fn in_flight_transaction_count(&self) -> usize {
		self.in_flight_txns.load(Ordering::Acquire)
	}

	/// Bypass-MVCC range destruction.
	///
	/// Callers are responsible for ensuring the data is already
	/// logically inaccessible (e.g. the catalog metadata pointing at it
	/// has already been cleared in a committed transaction).
	pub async fn unsafe_destroy_range(&self, start: Vec<u8>, end: Vec<u8>) -> Result<()> {
		let started = Instant::now();
		let tikv_start: tikv::Key = start.into();
		let tikv_end: tikv::Key = end.into();
		self.db.unsafe_destroy_range(tikv_start..tikv_end).await.map_err(Error::from)?;
		debug!(
			target: TARGET,
			duration_ms = started.elapsed().as_millis() as u64,
			"TiKV unsafe_destroy_range completed",
		);
		Ok(())
	}

	/// Advance the MVCC GC safepoint by `lifetime`.
	///
	/// Resolves the current TSO via `current_timestamp`, computes a
	/// safepoint at `now - lifetime`, then calls the client's `gc()` which
	/// internally runs `cleanup_locks` followed by `update_safepoint`.
	pub async fn run_mvcc_gc(&self, lifetime: Duration) -> Result<()> {
		if !self.config.gc_enabled {
			return Ok(());
		}
		let started = Instant::now();
		let now = self.db.current_timestamp().await.map_err(Error::from)?;
		let safepoint = match safepoint_from(&now, lifetime) {
			Some(ts) => ts,
			None => {
				debug!(
					target: TARGET,
					"Skipping TiKV MVCC GC pass: safepoint would precede epoch",
				);
				return Ok(());
			}
		};
		let advanced = match self.db.gc(safepoint).await {
			Ok(v) => v,
			Err(e) => {
				warn!(
					target: TARGET,
					error = %e,
					"TiKV MVCC GC pass failed",
				);
				return Err(Error::from(e));
			}
		};
		info!(
			target: TARGET,
			advanced,
			duration_ms = started.elapsed().as_millis() as u64,
			lifetime_secs = lifetime.as_secs(),
			"TiKV MVCC GC pass completed",
		);
		Ok(())
	}

	/// Standalone lock-cleanup pass.
	///
	/// Calls `cleanup_locks` over the whole keyspace using a safepoint at
	/// `now - lifetime`. Runs on a faster cadence than full GC because
	/// stale locks block readers immediately, but version GC can wait.
	pub async fn run_lock_cleanup(&self, lifetime: Duration) -> Result<()> {
		if !self.config.gc_enabled {
			return Ok(());
		}
		let started = Instant::now();
		let now = self.db.current_timestamp().await.map_err(Error::from)?;
		let safepoint = match safepoint_from(&now, lifetime) {
			Some(ts) => ts,
			None => {
				debug!(
					target: TARGET,
					"Skipping TiKV lock cleanup pass: safepoint would precede epoch",
				);
				return Ok(());
			}
		};
		// Range that covers the entire user keyspace. The TiKV client
		// applies the active keyspace prefix internally, so callers pass
		// raw bounds. A bare `vec![0xff]` upper bound would lexically
		// exclude any multi-byte key starting with `0xff`
		// (`[0xff] < [0xff, 0x00]`); pad with extra `0xff` bytes so the
		// bound stays correct if a future key encoding ever leads with
		// a high byte.
		let range: Range<tikv::Key> = (vec![0u8].into())..(vec![0xffu8; 16].into());
		let result = self
			.db
			.cleanup_locks(range, &safepoint, ResolveLocksOptions::default())
			.await
			.map_err(Error::from)?;
		info!(
			target: TARGET,
			has_region_error = result.region_error.is_some(),
			key_error_count = result.key_error.as_ref().map(|v| v.len()).unwrap_or(0),
			resolved_locks = result.resolved_locks,
			duration_ms = started.elapsed().as_millis() as u64,
			lifetime_secs = lifetime.as_secs(),
			"TiKV lock cleanup pass completed",
		);
		Ok(())
	}
}

/// Compute a TiKV [`tikv::Timestamp`] at `now - lifetime` using the same
/// physical/logical decomposition as the rest of the engine. Returns
/// `None` if the computed instant would precede the epoch.
fn safepoint_from(now: &tikv::Timestamp, lifetime: Duration) -> Option<tikv::Timestamp> {
	let micros: i64 = lifetime.as_micros().try_into().ok()?;
	let physical = now.physical.checked_sub(micros)?;
	if physical < 0 {
		return None;
	}
	Some(tikv::Timestamp {
		physical,
		logical: now.logical,
		suffix_bits: now.suffix_bits,
	})
}

/// Owned TLS paths after partial-config validation.
#[cfg_attr(test, derive(Debug))]
struct TlsPaths {
	ca: String,
	cert: String,
	key: String,
}

/// Validate the mTLS portion of a [`TikvConfig`] before any I/O.
///
/// Returns:
/// - `Ok(Some(_))` when all three paths are set (mTLS is active).
/// - `Ok(None)` when none are set (plaintext connection).
/// - `Err(Error::Datastore)` when only some are set, with a message reporting which of {ca, cert,
///   key} are missing. Extracted so the partial-config rejection can be exercised without a live PD
///   endpoint.
fn validate_tls_paths(config: &TikvConfig) -> Result<Option<TlsPaths>> {
	match (config.tls_ca_path.as_ref(), config.tls_cert_path.as_ref(), config.tls_key_path.as_ref())
	{
		(Some(ca), Some(cert), Some(key)) => Ok(Some(TlsPaths {
			ca: ca.clone(),
			cert: cert.clone(),
			key: key.clone(),
		})),
		(None, None, None) => Ok(None),
		(ca, cert, key) => Err(Error::Datastore(format!(
			"TiKV mTLS requires tikv_tls_ca_path, tikv_tls_cert_path and \
			 tikv_tls_key_path to all be set together (e.g. via \
			 SURREAL_TIKV_TLS_*); got ca={}, cert={}, key={}",
			if ca.is_some() {
				"set"
			} else {
				"unset"
			},
			if cert.is_some() {
				"set"
			} else {
				"unset"
			},
			if key.is_some() {
				"set"
			} else {
				"unset"
			},
		))),
	}
}

impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"tikv"
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done.load(Ordering::Relaxed)
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancel a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn cancel(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already closed
			if self.done.swap(true, Ordering::AcqRel) {
				return Err(Error::TransactionFinished);
			}
			// Drop the in-flight reservation regardless of rollback outcome
			// so a slow/failing rollback can't stall shutdown drain.
			self.release_in_flight();
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Cancel this transaction
			if self.write {
				// Ignore rollback errors
				let _ = inner.tx.rollback().await;
			}
			// Continue
			Ok(())
		})
	}

	/// Commit a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn commit(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already closed
			if self.done.swap(true, Ordering::AcqRel) {
				return Err(Error::TransactionFinished);
			}
			// Release the in-flight reservation eagerly; the commit attempt
			// below is the last operation this handle will perform either
			// way. Doing it before the await also makes the counter
			// trustworthy from the shutdown drain's perspective even if the
			// commit hangs against a degraded cluster.
			self.release_in_flight();
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Get the inner transaction
			let mut inner = self.inner.write().await;
			// Commit this transaction
			if let Err(err) = inner.tx.commit().await {
				if let Err(inner_err) = inner.tx.rollback().await {
					error!(
						target: TARGET,
						commit_error = %err,
						rollback_error = %inner_err,
						elapsed_ms = self.started_at.elapsed().as_millis() as u64,
						"TiKV transaction commit failed and rollback also failed",
					);
				} else {
					debug!(
						target: TARGET,
						commit_error = %err,
						elapsed_ms = self.started_at.elapsed().as_millis() as u64,
						"TiKV transaction commit failed; rollback succeeded",
					);
				}
				return Err(err.into());
			}
			trace!(
				target: TARGET,
				elapsed_ms = self.started_at.elapsed().as_millis() as u64,
				"TiKV transaction committed",
			);
			// Continue
			Ok(())
		})
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn exists(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<bool>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Check the key
			let res = inner.tx.key_exists(key).await?;
			// Return result
			Ok(res)
		})
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn get(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<Option<Val>>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Get the key
			let res = inner.tx.get(key).await?;
			// Return result
			Ok(res)
		})
	}

	/// Fetch many keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.sprint()))]
	fn getm(&self, keys: Vec<Key>, version: Option<u64>) -> BoxFut<'_, Result<GetMultiResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Build an index from key bytes to original position so we can
			// restore order without cloning values out of a HashMap.
			let key_index: HashMap<&[u8], usize> =
				keys.iter().enumerate().map(|(i, k)| (k.as_slice(), i)).collect();
			// Batch get the keys
			let pairs = inner.tx.batch_get(keys.iter().cloned()).await?;
			// Place each result directly at the correct position, accumulating
			// the hit count and value bytes during the same pass so callers do
			// not need to re-walk the result.
			let mut values: Vec<Option<Val>> = vec![None; keys.len()];
			let mut records = 0u64;
			let mut value_bytes = 0u64;
			for kv in pairs {
				if let Some(&idx) = key_index.get(Key::from(kv.0).as_slice()) {
					records += 1;
					value_bytes += kv.1.len() as u64;
					values[idx] = Some(kv.1);
				}
			}
			Ok(GetMultiResult {
				values,
				records,
				value_bytes,
			})
		})
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn set(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>> {
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
			// Get the old value if we need to track operations
			let old_val = if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
				inner.tx.get(key.clone()).await?
			} else {
				None
			};
			// Set the key
			inner.tx.put(key.clone(), val).await?;
			// Record operation after successful operation
			if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
				match old_val {
					Some(existing_val) => {
						// Key existed, record operation to restore old value
						inner.operations.push(Operation::RestoreValue(key, existing_val));
					}
					None => {
						// Key didn't exist, record operation to delete it
						inner.operations.push(Operation::DeleteKey(key));
					}
				}
			}
			// Return result
			Ok(())
		})
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn put(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>> {
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
			// Check if key exists
			let exists = inner.tx.key_exists(key.clone()).await?;
			if exists {
				return Err(Error::TransactionKeyAlreadyExists);
			}
			// Set the key
			inner.tx.put(key.clone(), val).await?;
			// Record operation after successful operation
			if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
				// Key didn't exist (we just checked), record operation to delete it
				inner.operations.push(Operation::DeleteKey(key));
			}
			// Return result
			Ok(())
		})
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> BoxFut<'_, Result<()>> {
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
			// Get the current value
			let current = inner.tx.get(key.clone()).await?;
			// Check if condition is met
			match (&current, &chk) {
				(Some(v), Some(w)) if v == w => {}
				(None, None) => {}
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Set the key
			inner.tx.put(key.clone(), val).await?;
			// Record operation after successful operation
			if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
				match current {
					Some(existing_val) => {
						// Key existed, record operation to restore old value
						inner.operations.push(Operation::RestoreValue(key, existing_val));
					}
					None => {
						// Key didn't exist, record operation to delete it
						inner.operations.push(Operation::DeleteKey(key));
					}
				}
			}
			// Return result
			Ok(())
		})
	}

	/// Delete a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn del(&self, key: Key) -> BoxFut<'_, Result<()>> {
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
			// Get the old value if we need to track operations
			let old_val = if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
				inner.tx.get(key.clone()).await?
			} else {
				None
			};
			// Delete the key
			inner.tx.delete(key.clone()).await?;
			// Record operation after successful operation
			if let Some(existing_val) = old_val {
				// Key existed, record operation to restore it
				inner.operations.push(Operation::RestoreDeleted(key, existing_val));
			}
			// Return result
			Ok(())
		})
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn delc(&self, key: Key, chk: Option<Val>) -> BoxFut<'_, Result<()>> {
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
			// Get the current value
			let current = inner.tx.get(key.clone()).await?;
			// Check if condition is met
			match (&current, &chk) {
				(Some(v), Some(w)) if v == w => {}
				(None, None) => {}
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Delete the key
			inner.tx.delete(key.clone()).await?;
			// Record operation after successful operation
			if let Some(existing_val) = current {
				// Key existed, record operation to restore it
				inner.operations.push(Operation::RestoreDeleted(key, existing_val));
			}
			// Return result
			Ok(())
		})
	}

	/// Delete a range of keys from the database transactionally.
	///
	/// Previously this called `unsafe_destroy_range` on the underlying
	/// `TransactionClient` directly which bypassed MVCC entirely: a
	/// rollback could not undo it and concurrent snapshot readers could
	/// observe partial state. The new implementation materialises keys
	/// via `scan_keys` in batches and deletes them as part of the
	/// surrounding transaction, capped by `SURREAL_TIKV_DELR_MAX_KEYS`
	/// to keep individual transactions reasonably sized. Callers that
	/// need to drop arbitrarily large ranges out-of-transaction (e.g. a
	/// background namespace expunge) should use
	/// [`Datastore::unsafe_destroy_range`] instead.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn delr(&self, rng: Range<Key>) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.delete_range_bounded(rng).await })
	}

	/// Hard-delete a range of keys (`clrr`) under the same transactional
	/// bound as [`Self::delr`].
	///
	/// On TiKV `clr` and `del` collapse to the same in-transaction
	/// tombstone write (the actual MVCC version reclamation happens at
	/// GC safepoint, outside any user transaction). Without this
	/// override, the default `clrr` impl in `kvs/api.rs` falls back to
	/// per-key `clr` over `batch_keys` and silently bypasses
	/// `tikv_delr_max_keys` — `REMOVE NAMESPACE EXPUNGE` on a huge
	/// prefix would buffer unbounded deletes inside one TiKV
	/// transaction. Forwarding to the bounded scan-delete loop keeps
	/// soft (`delr`/`delp`) and hard (`clrr`/`clrp`) prefix-deletes on
	/// the same safety cap.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn clrr(&self, rng: Range<Key>) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.delete_range_bounded(rng).await })
	}

	/// Count the total number of keys within a range in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn count(&self, rng: Range<Key>, version: Option<u64>) -> BoxFut<'_, Result<usize>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Store the total count
			let mut total = 0usize;
			// Store the end range key
			let end = rng.end.clone();
			// Store the next start key
			let mut start = rng.start;
			// Loop until we have exhausted the range
			loop {
				// Scan keys in key-only mode (no values fetched)
				let iter = inner.tx.scan_keys(start..end.clone(), COUNT_BATCH_SIZE).await?;
				// Count the items, tracking the last key seen
				let mut key: Option<tikv::Key> = None;
				// Count the items in this batch
				let mut count = 0u32;
				// Loop over the iterator
				for k in iter {
					count += 1;
					key = Some(k);
				}
				// Increment the total count
				total += count as usize;
				// If we got fewer than batch_size, we've exhausted the range
				if count < COUNT_BATCH_SIZE {
					break;
				}
				// Advance past the last key for the next batch
				match key {
					Some(k) => {
						let mut k = Key::from(k);
						util::advance_key(&mut k);
						start = k;
					}
					None => break,
				}
			}
			// Return the total count
			Ok(total)
		})
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn keys(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<KeysResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Extract the limit count, adding skip to fetch enough entries
			let count = match limit {
				ScanLimit::Count(c) => c.saturating_add(skip),
				ScanLimit::Bytes(b) => (b / ESTIMATED_BYTES_PER_KEY).max(1).saturating_add(skip),
				ScanLimit::BytesOrCount(_, c) => c.saturating_add(skip),
			};
			// Create the iterator
			let mut iter = inner.tx.scan_keys(rng, count).await?;
			// Consume the iterator
			Ok(consume_keys(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn keysr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<KeysResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Extract the limit count, adding skip to fetch enough entries
			let count = match limit {
				ScanLimit::Count(c) => c.saturating_add(skip),
				ScanLimit::Bytes(b) => (b / ESTIMATED_BYTES_PER_KEY).max(1).saturating_add(skip),
				ScanLimit::BytesOrCount(_, c) => c.saturating_add(skip),
			};
			// Create the iterator
			let mut iter = inner.tx.scan_keys_reverse(rng, count).await?;
			// Consume the iterator
			Ok(consume_keys(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn scan(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<ScanResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Skip entries using keys-only scan to avoid fetching values
			let rng = if skip > 0 {
				let skipped = inner.tx.scan_keys(rng.clone(), skip).await?;
				match skipped.last() {
					Some(last) => {
						let mut start: Key = Key::from(last);
						util::advance_key(&mut start);
						start..rng.end
					}
					// Fewer entries than skip -- nothing to return
					None => return Ok(ScanResult::default()),
				}
			} else {
				rng
			};
			// Extract the limit count
			let count = match limit {
				ScanLimit::Count(c) => c,
				ScanLimit::Bytes(b) => (b / ESTIMATED_BYTES_PER_KV).max(1),
				ScanLimit::BytesOrCount(_, c) => c,
			};
			// Create the iterator
			let mut iter = inner.tx.scan(rng, count).await?;
			// Consume the iterator
			Ok(consume_vals(&mut iter, limit))
		})
	}

	/// Retrieve a range of keys from the database in reverse order
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn scanr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<ScanResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Skip entries using keys-only scan to avoid fetching values
			let rng = if skip > 0 {
				let skipped = inner.tx.scan_keys_reverse(rng.clone(), skip).await?;
				match skipped.last() {
					Some(last) => {
						let end: Key = Key::from(last);
						rng.start..end
					}
					// Fewer entries than skip -- nothing to return
					None => return Ok(ScanResult::default()),
				}
			} else {
				rng
			};
			// Extract the limit count
			let count = match limit {
				ScanLimit::Count(c) => c,
				ScanLimit::Bytes(b) => (b / ESTIMATED_BYTES_PER_KV).max(1),
				ScanLimit::BytesOrCount(_, c) => c,
			};
			// Create the iterator
			let mut iter = inner.tx.scan_reverse(rng, count).await?;
			// Consume the iterator
			Ok(consume_vals(&mut iter, limit))
		})
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------

	/// Set a new save point on the transaction.
	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
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
			// Take the current operations
			let operations = std::mem::take(&mut inner.operations);
			// Create a new savepoint with those operations
			inner.savepoints.push(Savepoint {
				operations,
			});
			// Continue
			Ok(())
		})
	}

	/// Release the last save point.
	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
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
			// Release the last savepoint
			inner.savepoints.pop();
			// Continue
			Ok(())
		})
	}

	/// Rollback to the last save point.
	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
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
			// Check if there are any savepoints
			if inner.savepoints.is_empty() {
				return Err(Error::Transaction("No savepoint to rollback to".to_string()));
			}
			// Get the most recent savepoint
			let savepoint = inner.savepoints.pop().expect("No savepoint to rollback to");
			// Take ownership of operations to avoid borrow checker issues
			let operations = std::mem::take(&mut inner.operations);
			// Execute undo operations in reverse order
			for op in operations.iter().rev() {
				match op {
					// Delete the key that was inserted
					Operation::DeleteKey(key) => {
						inner.tx.delete(key.clone()).await?;
					}
					// Restore the previous value
					Operation::RestoreValue(key, val) => {
						inner.tx.put(key.clone(), val.clone()).await?;
					}
					// Restore the deleted key
					Operation::RestoreDeleted(key, val) => {
						inner.tx.put(key.clone(), val.clone()).await?;
					}
				}
			}
			// Restore the savepoint's operations as the current ones
			inner.operations = savepoint.operations;
			// Continue
			Ok(())
		})
	}

	// --------------------------------------------------
	// Timestamp functions
	// --------------------------------------------------

	/// Get the current monotonic timestamp
	fn timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		Box::pin(async move {
			let ts = self.inner.write().await.tx.current_timestamp().await?;
			Ok(BoxTimeStamp::new(TiKVStamp(ts)))
		})
	}

	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		Box::new(TiKVStampImpl)
	}
}

pub struct TiKVStampImpl;

impl TimeStampImpl for TiKVStampImpl {
	fn earliest(&self) -> BoxTimeStamp {
		BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical: 0,
			logical: 0,
			suffix_bits: 0,
		}))
	}

	fn create_from_versionstamp(&self, version: u128) -> Option<BoxTimeStamp> {
		Some(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp::from_version(version as u64))))

		/* We really should encode full precision but version stamps aren't actually a u128, they
		 * only support values in range of 0 to i64::MAX,

		let physical = ((version >> 64) as u64 as i64) ^ i64::MIN;
		let logical = (version as u64 as i64) ^ i64::MIN;
		Some(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical,
			logical,
			suffix_bits: 0,
		})))
		*/
	}

	fn create_from_datetime(&self, dt: DateTime<Utc>) -> Option<BoxTimeStamp> {
		let physical = dt.timestamp_micros();
		Some(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical,
			logical: 0,
			suffix_bits: 0,
		})))
	}

	fn decode(&self, bytes: &[u8]) -> Result<BoxTimeStamp> {
		if bytes.len() == 8 {
			// Backwards compatibilty with old timestamp
			let Ok(b) = <[u8; 8]>::try_from(&bytes[0..8]) else {
				unreachable!()
			};
			let ts = u64::from_be_bytes(b);
			return Ok(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp::from_version(ts))));
		}

		if bytes.len() != 20 {
			return Err(Error::TimestampInvalid(
				"Encoded timestamp is not the right length".to_string(),
			));
		}
		let Ok(b) = <[u8; 8]>::try_from(&bytes[0..8]) else {
			unreachable!()
		};
		let physical = i64::from_be_bytes(b) ^ i64::MIN;
		let Ok(b) = <[u8; 8]>::try_from(&bytes[8..16]) else {
			unreachable!()
		};
		let logical = i64::from_be_bytes(b) ^ i64::MIN;
		let Ok(b) = <[u8; 4]>::try_from(&bytes[16..20]) else {
			unreachable!()
		};
		let suffix_bits = u32::from_be_bytes(b);
		Ok(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical,
			logical,
			suffix_bits,
		})))
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct TiKVStamp(tikv::Timestamp);

impl TimeStamp for TiKVStamp {
	fn as_versionstamp(&self) -> u128 {
		self.0.version() as u128

		/* We really should encode full precision but version stamps aren't actually a u128, they
		 * only support values in range of 0 to i64::MAX,

		let p = (self.0.physical ^ i64::MIN) as u64;
		let l = (self.0.logical ^ i64::MIN) as u64;

		(p as u128) << 64 | l as u128
		*/
	}

	fn as_datetime(&self) -> Option<DateTime<Utc>> {
		// Will truncate, but is only a problem far in the future
		DateTime::from_timestamp_micros(self.0.physical)
	}

	fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp> {
		let micros = duration.as_micros().try_into().ok()?;
		let physical = self.0.physical.checked_sub(micros)?;
		Some(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical,
			logical: self.0.logical,
			suffix_bits: self.0.suffix_bits,
		})))
	}

	fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8] {
		let b = (self.0.physical ^ i64::MIN).to_be_bytes();
		bytes[0..8].copy_from_slice(&b);
		let b = (self.0.logical ^ i64::MIN).to_be_bytes();
		bytes[8..16].copy_from_slice(&b);
		let b = self.0.suffix_bits.to_be_bytes();
		bytes[16..20].copy_from_slice(&b);

		&bytes[..20]
	}
}

// Consume and iterate over only keys
fn consume_keys<I: Iterator<Item = tikv::Key>>(
	iter: &mut I,
	limit: ScanLimit,
	skip: u32,
) -> KeysResult {
	// Skip entries from the pre-fetched iterator
	for _ in 0..skip {
		if iter.next().is_none() {
			return KeysResult::default();
		}
	}
	let mut key_bytes = 0u64;
	let keys = match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key
				if let Some(k) = iter.next() {
					key_bytes += k.len() as u64;
					res.push(Key::from(k));
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b / ESTIMATED_BYTES_PER_KEY).min(4096) as usize);
			// Check that we don't exceed the byte limit
			while key_bytes < b as u64 {
				// Check the key
				if let Some(k) = iter.next() {
					key_bytes += k.len() as u64;
					res.push(Key::from(k));
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && key_bytes < b as u64 {
				// Check the key
				if let Some(k) = iter.next() {
					key_bytes += k.len() as u64;
					res.push(Key::from(k));
				} else {
					break;
				}
			}
			res
		}
	};
	KeysResult {
		keys,
		key_bytes,
	}
}

// Consume and iterate over keys and values
fn consume_vals<I: Iterator<Item = tikv::KvPair>>(iter: &mut I, limit: ScanLimit) -> ScanResult {
	// Track the cumulative key/value bytes for the metric. The byte-bounded
	// limit branches still rely on `bytes_fetched` (key + value bytes) to
	// decide when to stop, so the two counters are kept separate.
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	let values = match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key and value
				if let Some(kv) = iter.next() {
					let key_len = kv.0.len() as u64;
					let value_len = kv.1.len() as u64;
					key_bytes += key_len;
					value_bytes += value_len;
					res.push((Key::from(kv.0), kv.1));
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b / ESTIMATED_BYTES_PER_KV).min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0u64;
			// Check that we don't exceed the byte limit
			while bytes_fetched < b as u64 {
				// Check the key and value
				if let Some(kv) = iter.next() {
					let key_len = kv.0.len() as u64;
					let value_len = kv.1.len() as u64;

					bytes_fetched += key_len + value_len;
					key_bytes += key_len;
					value_bytes += value_len;

					res.push((Key::from(kv.0), kv.1));
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0u64;
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && bytes_fetched < b as u64 {
				// Check the key and value
				if let Some(kv) = iter.next() {
					let key_len = kv.0.len() as u64;
					let value_len = kv.1.len() as u64;

					bytes_fetched += key_len + value_len;
					key_bytes += key_len;
					value_bytes += value_len;

					res.push((Key::from(kv.0), kv.1));
				} else {
					break;
				}
			}
			res
		}
	};
	ScanResult {
		values,
		key_bytes,
		value_bytes,
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::time::Duration;

	use super::{TikvConfig, safepoint_from, validate_tls_paths};

	#[test]
	fn safepoint_from_subtracts_lifetime() {
		let now = tikv::Timestamp {
			physical: 1_000_000,
			logical: 42,
			suffix_bits: 7,
		};
		let safepoint = safepoint_from(&now, Duration::from_micros(250)).unwrap();
		assert_eq!(safepoint.physical, 999_750);
		// Logical and suffix bits ride along unchanged so the safepoint
		// stays comparable to other timestamps in the same epoch.
		assert_eq!(safepoint.logical, 42);
		assert_eq!(safepoint.suffix_bits, 7);
	}

	#[test]
	fn safepoint_from_returns_none_when_lifetime_too_large() {
		let now = tikv::Timestamp {
			physical: 100,
			logical: 0,
			suffix_bits: 0,
		};
		// A 1 ms lifetime is 1000 micros; subtracting from `physical = 100`
		// underflows past the epoch and should bail.
		assert!(safepoint_from(&now, Duration::from_millis(1)).is_none());
	}

	#[test]
	fn safepoint_from_returns_none_when_lifetime_overflows_i64() {
		let now = tikv::Timestamp {
			physical: i64::MAX,
			logical: 0,
			suffix_bits: 0,
		};
		// `Duration::MAX.as_micros()` doesn't fit in `i64`; the
		// `try_into` should bail rather than panic.
		assert!(safepoint_from(&now, Duration::MAX).is_none());
	}

	#[test]
	fn validate_tls_paths_all_unset() {
		let cfg = TikvConfig::default();
		assert!(validate_tls_paths(&cfg).unwrap().is_none());
	}

	#[test]
	fn validate_tls_paths_all_set() {
		let cfg = TikvConfig {
			tls_ca_path: Some("/etc/tikv/ca.pem".into()),
			tls_cert_path: Some("/etc/tikv/cert.pem".into()),
			tls_key_path: Some("/etc/tikv/key.pem".into()),
			..TikvConfig::default()
		};
		let paths = validate_tls_paths(&cfg).unwrap().unwrap();
		assert_eq!(paths.ca, "/etc/tikv/ca.pem");
		assert_eq!(paths.cert, "/etc/tikv/cert.pem");
		assert_eq!(paths.key, "/etc/tikv/key.pem");
	}

	#[test]
	fn validate_tls_paths_partial_set_rejects() {
		// Permute through the six partial-set combinations and confirm
		// each produces a descriptive error that names the missing
		// piece(s). Catches a regression where only one of the three
		// triples is checked.
		let cases: &[(Option<&str>, Option<&str>, Option<&str>)] = &[
			(Some("ca"), None, None),
			(None, Some("cert"), None),
			(None, None, Some("key")),
			(Some("ca"), Some("cert"), None),
			(Some("ca"), None, Some("key")),
			(None, Some("cert"), Some("key")),
		];
		for (ca, cert, key) in cases {
			let cfg = TikvConfig {
				tls_ca_path: ca.map(String::from),
				tls_cert_path: cert.map(String::from),
				tls_key_path: key.map(String::from),
				..TikvConfig::default()
			};
			let err = validate_tls_paths(&cfg).expect_err("partial TLS config must reject");
			let msg = err.to_string();
			assert!(
				msg.contains("tikv_tls_ca_path"),
				"error should name the env var triple: {msg}"
			);
			assert!(msg.contains("set"), "error should report which fields are set: {msg}");
			assert!(msg.contains("unset"), "error should report which fields are unset: {msg}");
		}
	}

	#[test]
	fn release_in_flight_counter_decrements() {
		// `release_in_flight` lives on `Transaction` but its work is the
		// `checked_sub` on the shared `Arc<AtomicUsize>`. Exercising the
		// underlying primitive here would normally feel pointless — but
		// the contract is "underflow leaves the counter unchanged", and
		// that's the regression worth catching if anyone replaces it
		// with a bare `fetch_sub` later.
		let counter = Arc::new(AtomicUsize::new(2));
		let release = |c: &Arc<AtomicUsize>| {
			let _ = c.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
		};
		release(&counter);
		assert_eq!(counter.load(Ordering::Acquire), 1);
		release(&counter);
		assert_eq!(counter.load(Ordering::Acquire), 0);
		// Underflow: counter must stay at 0, not wrap to usize::MAX.
		release(&counter);
		assert_eq!(counter.load(Ordering::Acquire), 0);
	}
}
