//! Transaction implementation and cache coordination.
//!
//! Cache paths use `Entry::Any(val.clone())` for concrete `Arc<T>` values that must coerce to
//! `Arc<dyn Any + Send + Sync>`; `Arc::clone(&val)` does not perform that unsized coercion.
#![allow(clippy::clone_on_ref_ptr)]
// `Transaction`'s pub methods take `K: KVKey` / `K::ValueType: KVValue`.
// Both traits are `pub(crate)` (their `pub` declarations are gated by
// `pub(crate) use` re-exports in `kvs/mod.rs`), so the lint flags every
// such method. The visibility is intentional — silence at module scope.
#![allow(private_bounds, private_interfaces)]

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Deref, Range};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use futures::future::try_join_all;
use tokio::sync::{Mutex, Notify};
use tokio::time::sleep;
use tracing::Instrument;
use uuid::Uuid;
use web_time::Instant;

use super::api::{KeysBatch, ScanCursorKeys, ScanCursorVals, ScanLimit, ValsBatch};
use super::batch::Batch;
use super::{Key, LockType, TransactionFactory, TransactionType, Val, util};
use crate::catalog::providers::{
	ApiProvider, AuthorisationProvider, BoxProviderFut, BucketProvider, CatalogProvider,
	DatabaseProvider, NamespaceProvider, NodeProvider, RootProvider, TableProvider, UserProvider,
};
use crate::catalog::{
	self, ApiDefinition, ConfigDefinition, DatabaseDefinition, DatabaseId, DefaultConfig, IndexId,
	NamespaceDefinition, NamespaceId, Record, TableDefinition, TableId,
};
use crate::cf::Changefeed;
use crate::cnf::CommonConfig;
use crate::ctx::Context;
use crate::dbs::node::Node;
use crate::doc::CursorRecord;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::planner::ScanDirection;
use crate::key::database::sq::Sq;
use crate::key::index::all as index_all;
use crate::key::table::bg::Bg;
use crate::key::table::br::Br;
use crate::key::table::bs::Bs;
use crate::key::table::ix as table_ix;
use crate::kvs::cache::tx::TransactionCache;
use crate::kvs::index::{
	BuildGeneration, BuildTicket, BuildTicketMutationSeq, IndexBuildPhase, IndexBuildReportStatus,
	IndexBuildState, IndexBuilder,
};
use crate::kvs::sequences::Sequences;
#[cfg(test)]
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::kvs::{
	BoxTimeStamp, BoxTimeStampImpl, Direction, Error as KvsError, KVKey, KVValue, Transactor,
	cache, is_retryable_transaction_conflict,
};
use crate::observe::{
	ExecutionObserver, Outcome, TenantIdentity, TransactionEvent, TransactionEventSafe,
	TransactionMetrics,
};
use crate::val::{RecordId, RecordIdKey, TableName};

/// Controls whether `get_records` populates the transaction cache on miss.
///
/// Point lookups and graph traversals benefit from caching (records are
/// likely re-accessed within the same transaction). Large sequential scans
/// (index range scans, full-text hits) read each record once, so populating
/// the cache wastes time and evicts useful entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CachePolicy {
	/// Check cache on read **and** populate on miss.
	/// Use for point lookups, graph traversal, KNN, and unique-index equality.
	ReadWrite,
	/// Check cache on read but **skip** population on miss.
	/// Use for index range scans, non-unique equality scans, and full-text scans.
	ReadOnly,
}

pub struct Transaction {
	/// Is this is a local datastore transaction?
	local: bool,
	/// The wall-clock instant the transaction was opened. Used to compute
	/// transaction lifetime when emitting the terminal
	/// [`crate::observe::TransactionEvent`].
	started_at: Instant,
	/// Observability hook fired on commit/cancel. Defaults to
	/// [`crate::observe::NoopObserver`] and is otherwise supplied by the
	/// datastore builder.
	observer: Arc<dyn ExecutionObserver>,
	/// Per-transaction KV operation counters, updated by the wrapper methods
	/// on `Transaction` (`get`, `set`, `scan`, ...). Snapshotted once when
	/// the transaction finishes to populate the emitted event.
	metrics: TransactionMetrics,
	/// Pre-resolved tenant identity for this transaction. Sourced from the
	/// originating session via [`crate::ctx::Context::tenant_identity`] and
	/// surfaced as the `*Ctx` half of the emitted [`TransactionEvent`].
	///
	/// `OnceLock` so callers that wrap the [`Transaction`] in an `Arc` before
	/// the session is known (e.g. [`crate::kvs::Datastore::execute_with_transaction`])
	/// can still attach identity after the fact via
	/// [`Self::set_tenant_identity`].
	tenant_identity: OnceLock<Arc<TenantIdentity>>,
	/// The underlying transactor.
	tr: Transactor,
	/// The query cache for this store
	cache: TransactionCache,
	/// The sequences for this store
	sequences: Sequences,
	/// The changefeed buffer.
	changefeed: OnceLock<Changefeed>,
	/// Async event trigger
	async_event_trigger: Arc<Notify>,
	/// Do we have to trigger async events after the commit?
	trigger_async_event: AtomicBool,
	/// Durable index-build reservations to release once this transaction is closed.
	///
	/// Writers enqueue index appendings for a durable concurrent index build after
	/// admission has reserved a ticket in a separate short transaction. Releasing
	/// those reservations after commit/cancel keeps rollback semantics correct and
	/// avoids making the user transaction delete a key that was created after its
	/// snapshot, which can conflict on snapshot-isolated local engines such as
	/// `kv-mem`.
	pending_index_build_reservations: Mutex<Vec<IndexBuildReservationRelease>>,
	/// Per-user-transaction admission reservations, keyed by `(generation, index)`.
	///
	/// One durable `!br` reservation is allocated per user transaction per index;
	/// every indexed mutation in that transaction reuses the cached ticket and
	/// allocates a fresh `mutation_seq`. Avoids paying a reservation commit for
	/// every individual mutation in a multi-row update or insert. Cleared on
	/// commit/cancel — the per-reservation release is queued separately in
	/// [`Self::pending_index_build_reservations`].
	cached_index_build_reservations:
		Mutex<HashMap<CachedIndexBuildReservationKey, CachedIndexBuildReservation>>,
	/// Process-local index builders to abort only after a successful schema commit.
	///
	/// Durable index retirement and catalog deletion are staged in the schema
	/// transaction. The in-process builder is not transactional, so aborting it
	/// before commit would make a later rollback/cancel stop a still-valid
	/// build. These actions are intentionally discarded on cancel or commit
	/// failure.
	pending_index_builder_aborts: Mutex<Vec<PendingIndexBuilderAbort>>,
	/// Index builds started before their catalog definition has committed.
	///
	/// `DEFINE INDEX` starts the builder while the schema transaction is still
	/// open. If that transaction is cancelled or fails to commit, the catalog row
	/// is rolled back but the builder may already have committed durable build
	/// state and index data from separate transactions. These cleanups remove that
	/// provisional state only when the schema transaction does not commit.
	pending_uncommitted_index_builds: Mutex<Vec<PendingUncommittedIndexBuild>>,
}

const INDEX_BUILD_RESERVATION_RELEASE_RETRY_SLEEP: Duration = Duration::from_millis(100);

/// Lookup key for a per-user-transaction admission reservation.
///
/// One cache entry exists per index per user transaction; every indexed
/// mutation that hits the same index in the same transaction shares this
/// entry and uses `next_mutation_seq` to allocate its `!bg` slot. The cache
/// key omits the build generation because reuse is revalidated on every hit
/// against the current `!bs` state: a generation rotation, an `Online` or
/// `Error` transition, or a vanished build aborts the user transaction with
/// `IndexingBuildingCancelled`. Skipping that recheck would let later
/// mutations in the same transaction write `!bg` against a generation no
/// builder will replay — silent index data loss.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CachedIndexBuildReservationKey {
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	pub(crate) tb: TableName,
	pub(crate) ix: IndexId,
}

/// Cached admission reservation reused across an entire user transaction.
///
/// First admission for an index runs the short reservation transaction
/// (CAS-incrementing `!bs.next_ticket` and committing `!br`), then stores
/// the resulting `generation`, `ticket`, `initial_complete`, and prepared
/// release here. Subsequent admissions read the cache, take a fresh
/// `mutation_seq`, and write a `!bg` keyed by `(generation, ticket, seq)`.
pub(crate) struct CachedIndexBuildReservation {
	pub(crate) generation: BuildGeneration,
	pub(crate) ticket: BuildTicket,
	pub(crate) initial_complete: bool,
	pub(crate) next_mutation_seq: BuildTicketMutationSeq,
}

/// Outcome of a per-transaction reservation lookup.
///
/// `FirstUse` is returned the first time admission runs for an index in this
/// transaction; the caller has already registered the prepared release and
/// must still run the durable-admission fence. Subsequent calls return
/// `Reused`, which only carries the ticket and the freshly allocated
/// `mutation_seq`. The shapes are intentionally identical for the fields the
/// caller consumes — the variant tag exists so admission can decide whether
/// to run the fence and emit fault-injection probes.
#[derive(Clone, Copy, Debug)]
pub(crate) enum CachedIndexBuildReservationLookup {
	FirstUse {
		generation: BuildGeneration,
		ticket: BuildTicket,
		mutation_seq: BuildTicketMutationSeq,
		initial_complete: bool,
	},
	Reused {
		generation: BuildGeneration,
		ticket: BuildTicket,
		mutation_seq: BuildTicketMutationSeq,
		initial_complete: bool,
	},
}

struct PendingIndexBuilderAbort {
	builder: IndexBuilder,
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ix: IndexId,
}

impl PendingIndexBuilderAbort {
	async fn abort(self) {
		if let Err(err) = self.builder.remove_index(self.ns, self.db, &self.tb, self.ix).await {
			tracing::warn!(
				target: "surrealdb::core::kvs::tx",
				"failed to abort local index builder after committed schema retirement: {err}"
			);
		}
	}
}

struct PendingUncommittedIndexBuild {
	builder: IndexBuilder,
	tf: TransactionFactory,
	sequences: Sequences,
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ix: IndexId,
}

impl PendingUncommittedIndexBuild {
	async fn cleanup_once(&self) -> Result<()> {
		// Stop the local task first. The durable `!bs` delete below is still the
		// cross-node fence: any in-flight builder write has to read/update that key
		// in the same transaction before it can commit index data.
		if let Err(err) = self.builder.remove_index(self.ns, self.db, &self.tb, self.ix).await {
			tracing::warn!(
				target: "surrealdb::core::kvs::tx",
				"failed to abort uncommitted local index builder during rollback cleanup: {err}"
			);
		}

		let tx = self
			.tf
			.transaction(TransactionType::Write, LockType::Optimistic, self.sequences.clone())
			.await?;
		let ikb = IndexKeyBase::new(self.ns, self.db, self.tb.clone(), self.ix);
		let index_prefix = index_all::new(self.ns, self.db, &self.tb, self.ix).encode_key()?;
		let result: Result<()> = async {
			tx.tr.del(ikb.new_bs_key().encode_key()?).await.map_err(Error::from)?;
			tx.tr.delr(ikb.new_bg_all_generations_range()?).await.map_err(Error::from)?;
			tx.tr.delr(ikb.new_bp_all_generations_range()?).await.map_err(Error::from)?;
			tx.tr.delr(ikb.new_br_all_generations_range()?).await.map_err(Error::from)?;
			tx.tr.delp(index_prefix).await.map_err(Error::from)?;
			tx.tr.commit().await.map_err(Error::from)?;
			Ok(())
		}
		.await;
		if let Err(err) = result {
			let _ = tx.tr.cancel().await;
			return Err(err);
		}
		Ok(())
	}

	async fn cleanup(self) -> Result<()> {
		loop {
			match self.cleanup_once().await {
				Ok(()) => return Ok(()),
				Err(err) if is_retryable_transaction_conflict(&err) => {
					tracing::debug!(
						target: "surrealdb::core::kvs::tx",
						error = %err,
						"retryable conflict while cleaning uncommitted index build, retrying"
					);
					sleep(INDEX_BUILD_RESERVATION_RELEASE_RETRY_SLEEP).await;
				}
				Err(err) => return Err(err),
			}
		}
	}
}

/// Close-time release for a durable index-build reservation owned by a user
/// transaction.
///
/// The release uses a fresh short transaction and a compare-delete against the
/// exact reservation value. That keeps the cleanup idempotent and prevents a
/// late release from deleting a different reservation if ownership changed. The
/// release may run after a queued `!bg` appending commits, or after a write fails
/// before any appending is written. Retryable conflicts are retried so transient
/// cleanup failures do not leave a live-node reservation blocking the index
/// build forever. Non-retryable failures are returned to the transaction close
/// path; if no committed durable appending exists for the reservation, the build
/// is marked `Error` so it cannot remain stuck in `Closing`.
#[derive(Clone)]
pub(crate) struct IndexBuildReservationRelease {
	tf: TransactionFactory,
	sequences: Sequences,
	node: Uuid,
	key: Key,
	val: Val,
}

impl IndexBuildReservationRelease {
	pub(crate) fn new(
		tf: TransactionFactory,
		sequences: Sequences,
		node: Uuid,
		key: Key,
		val: Val,
	) -> Self {
		Self {
			tf,
			sequences,
			node,
			key,
			val,
		}
	}

	async fn release_once(&self) -> Result<()> {
		// Use raw transactor methods here so the cleanup transaction does not
		// recursively run Transaction::commit/cancel and re-enter reservation
		// release handling.
		let tx = self
			.tf
			.transaction(TransactionType::Write, LockType::Optimistic, self.sequences.clone())
			.await?;

		#[cfg(test)]
		if let Err(err) = maybe_inject_non_retryable_error(
			NonRetryableErrorSite::ConcurrentIndexReservationRelease,
			self.node,
		) {
			let _ = tx.tr.cancel().await;
			return Err(err);
		}

		match tx.tr.delc(self.key.clone(), Some(self.val.clone())).await {
			Ok(()) => {}
			Err(KvsError::TransactionConditionNotMet) => {
				let _ = tx.tr.cancel().await;
				return Ok(());
			}
			Err(err) => {
				let _ = tx.tr.cancel().await;
				return Err(err.into());
			}
		}

		#[cfg(test)]
		if let Err(err) = maybe_inject_retryable_conflict(
			RetryableConflictSite::ConcurrentIndexReservationRelease,
			self.node,
		) {
			let _ = tx.tr.cancel().await;
			return Err(err);
		}

		if let Err(err) = tx.tr.commit().await {
			let _ = tx.tr.cancel().await;
			return Err(err.into());
		}
		Ok(())
	}

	async fn mark_build_error_if_uncommitted(&self, release_err: &anyhow::Error) -> Result<()> {
		let br = Br::decode_key(&self.key)?;
		// One reservation now covers an entire user transaction's mutation
		// batch on this index. Any committed `!bg(generation, ticket, *)`
		// entry signals that at least one mutation in that batch became
		// durable, so the build does not need to be marked errored. Use the
		// inclusive scan range, not a point exists check on `mutation_seq = 0`,
		// because the first mutation may not be at index zero on retry paths
		// that allocate a fresh ticket.
		let bg_range_start = Bg::new(
			br.ns,
			br.db,
			br.tb.as_ref(),
			br.ix,
			br.generation,
			br.ticket,
			BuildTicketMutationSeq::MIN,
		)
		.encode_key()?;
		let bg_range_end = Bg::new(
			br.ns,
			br.db,
			br.tb.as_ref(),
			br.ix,
			br.generation,
			br.ticket,
			BuildTicketMutationSeq::MAX,
		)
		.encode_key()?;
		let bs = Bs::new(br.ns, br.db, br.tb.as_ref(), br.ix).encode_key()?;
		let reason = format!(
			"Failed to release durable index-build reservation for generation {} ticket {} after transaction close: {release_err}",
			br.generation, br.ticket
		);

		loop {
			let tx = self
				.tf
				.transaction(TransactionType::Write, LockType::Optimistic, self.sequences.clone())
				.await?;

			let current_reservation = match tx.tr.get(self.key.clone(), None).await {
				Ok(current) => current,
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err.into());
				}
			};
			if current_reservation.as_deref() != Some(self.val.as_slice()) {
				let _ = tx.tr.cancel().await;
				return Ok(());
			}

			match tx
				.tr
				.keys(
					bg_range_start.clone()..bg_range_end.clone(),
					crate::kvs::ScanLimit::Count(1),
					0,
					None,
				)
				.await
			{
				Ok(res) if !res.keys.is_empty() => {
					let _ = tx.tr.cancel().await;
					return Ok(());
				}
				Ok(_) => {}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err.into());
				}
			}

			let current_state = match tx.tr.get(bs.clone(), None).await {
				Ok(Some(current_state)) => current_state,
				Ok(None) => {
					let _ = tx.tr.cancel().await;
					return Ok(());
				}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err.into());
				}
			};
			let current = IndexBuildState::kv_decode_value(&current_state, ())?;
			if current.generation != br.generation
				|| !matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				let _ = tx.tr.cancel().await;
				return Ok(());
			}

			let mut next = current.clone();
			next.phase = IndexBuildPhase::Error;
			next.owner = None;
			next.owner_heartbeat_at = None;
			next.updated_at = Utc::now();
			next.error = Some(reason.clone());
			next.report_status = Some(IndexBuildReportStatus::Error);
			let next_state = next.kv_encode_value()?;

			match tx.tr.putc(bs.clone(), next_state, Some(current_state)).await {
				Ok(()) => {}
				Err(KvsError::TransactionConditionNotMet) => {
					let _ = tx.tr.cancel().await;
					continue;
				}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err.into());
				}
			}

			match tx.tr.commit().await {
				Ok(()) => return Ok(()),
				Err(err) if err.is_retryable() => {
					let _ = tx.tr.cancel().await;
					sleep(INDEX_BUILD_RESERVATION_RELEASE_RETRY_SLEEP).await;
				}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err.into());
				}
			}
		}
	}

	pub(crate) async fn release(self) -> Result<()> {
		loop {
			match self.release_once().await {
				Ok(()) => return Ok(()),
				Err(err) if is_retryable_transaction_conflict(&err) => {
					tracing::debug!(
						target: "surrealdb::core::kvs::tx",
						node = %self.node,
						error = %err,
						"retryable conflict while releasing durable index-build reservation, retrying"
					);
					sleep(INDEX_BUILD_RESERVATION_RELEASE_RETRY_SLEEP).await;
				}
				Err(err) => {
					tracing::warn!(
						target: "surrealdb::core::kvs::tx",
						node = %self.node,
						"failed to release durable index-build reservation: {err}"
					);
					if let Err(mark_err) = self.mark_build_error_if_uncommitted(&err).await {
						tracing::warn!(
							target: "surrealdb::core::kvs::tx",
							node = %self.node,
							"failed to mark durable index build error after reservation release failure: {mark_err}"
						);
					}
					return Err(err);
				}
			}
		}
	}

	/// Delete every queued `!br` reservation in a single short transaction.
	///
	/// A typical user transaction touches a handful of indexes and produces one
	/// reservation per index. Folding the deletes into one commit removes the
	/// `O(reservations)` extra fsync that the per-reservation path would charge
	/// — the per-mutation cost is already amortized by the per-user-txn
	/// reservation cache, and this drops the close-time cost from one commit
	/// per reservation to exactly one commit per user transaction.
	///
	/// Returns `Ok(())` on success. On any failure the original reservations
	/// are returned via the `Err` arm so the caller can run the slow path
	/// (`release()`), which retries retryable conflicts and marks the build
	/// errored if a non-retryable failure leaves an undeleted `!br` behind.
	async fn release_batch(reservations: Vec<Self>) -> Result<(), Vec<Self>> {
		// One reservation: batching has nothing to amortize, and the per-call
		// path already does everything we need including retry+mark-error.
		if reservations.len() <= 1 {
			return Err(reservations);
		}
		let Some(first) = reservations.first() else {
			return Ok(());
		};
		let tf = first.tf.clone();
		let sequences = first.sequences.clone();
		let tx = match tf.transaction(TransactionType::Write, LockType::Optimistic, sequences).await
		{
			Ok(tx) => tx,
			Err(_) => return Err(reservations),
		};
		for reservation in &reservations {
			match tx.tr.delc(reservation.key.clone(), Some(reservation.val.clone())).await {
				Ok(()) => {}
				Err(KvsError::TransactionConditionNotMet) => {
					// The builder already cleaned this reservation up; that
					// is part of the normal release contract, not a failure.
				}
				Err(_) => {
					let _ = tx.tr.cancel().await;
					return Err(reservations);
				}
			}
		}
		match tx.tr.commit().await {
			Ok(()) => Ok(()),
			Err(_) => {
				let _ = tx.tr.cancel().await;
				Err(reservations)
			}
		}
	}
}

impl Deref for Transaction {
	type Target = Transactor;

	fn deref(&self) -> &Self::Target {
		&self.tr
	}
}

/// Caller-owned keys-only scan cursor that records scan metrics into the
/// parent transaction as it pumps batches.
///
/// Returned by [`Transaction::open_keys_cursor`]. Holds a borrow into the
/// parent transaction's metrics counter, so it cannot outlive the
/// transaction at the type level. Each call to `next_batch` advances the
/// underlying RocksDB-or-default cursor and records the batch's keys/bytes
/// against the transaction's scan metrics.
pub struct MeteredKeysCursor<'a> {
	/// The underlying backend-provided cursor (RocksDB-specialised, or the
	/// default impl that wraps single-shot `keys`/`keysr`).
	inner: Box<dyn ScanCursorKeys + 'a>,
	/// Borrow into the parent transaction's metrics counter; updated on
	/// each batch.
	metrics: &'a TransactionMetrics,
}

impl<'a> MeteredKeysCursor<'a> {
	/// Advance the cursor and return up to `limit` keys borrowed from the
	/// cursor's internal buffer. An empty batch signals end of range.
	///
	/// The returned `KeysBatch` borrows from the cursor; the borrow
	/// checker forbids calling `next_batch` again while the previous
	/// batch is still in scope.
	pub async fn next_batch<'s>(&'s mut self, limit: ScanLimit) -> Result<KeysBatch<'s>> {
		let batch = self.inner.next_batch(limit).await.map_err(Error::from)?;
		self.metrics.record_scan(batch.len() as u64, batch.key_bytes, 0);
		Ok(batch)
	}
}

/// Caller-owned key+value scan cursor with metrics recording. See
/// [`MeteredKeysCursor`] for the rationale.
pub struct MeteredValsCursor<'a> {
	/// The underlying backend-provided cursor.
	inner: Box<dyn ScanCursorVals + 'a>,
	/// Borrow into the parent transaction's metrics counter.
	metrics: &'a TransactionMetrics,
}

impl<'a> MeteredValsCursor<'a> {
	/// Advance the cursor and return up to `limit` `(key, value)` pairs
	/// borrowed from the cursor's internal buffer.
	pub async fn next_batch<'s>(&'s mut self, limit: ScanLimit) -> Result<ValsBatch<'s>> {
		let batch = self.inner.next_batch(limit).await.map_err(Error::from)?;
		self.metrics.record_scan(batch.len() as u64, batch.key_bytes, batch.value_bytes);
		Ok(batch)
	}
}

impl Transaction {
	/// Create a new transaction.
	///
	/// `observer` is dispatched to on commit/cancel; pass
	/// `Arc::new(NoopObserver)` when no observer is configured. `write`
	/// should match the `TransactionType` used to open the underlying
	/// transactor so the emitted event carries the correct attribute.
	pub fn new(
		local: bool,
		sequences: Sequences,
		async_event_trigger: Arc<Notify>,
		observer: Arc<dyn ExecutionObserver>,
		tr: Transactor,
		config: &CommonConfig,
	) -> Transaction {
		Transaction {
			local,
			started_at: Instant::now(),
			observer,
			metrics: TransactionMetrics::new(),
			tenant_identity: OnceLock::new(),
			tr,
			cache: TransactionCache::new(config.transaction_cache_size),
			sequences,
			changefeed: OnceLock::new(),
			async_event_trigger,
			trigger_async_event: AtomicBool::new(false),
			pending_index_build_reservations: Mutex::new(Vec::new()),
			cached_index_build_reservations: Mutex::new(HashMap::new()),
			pending_index_builder_aborts: Mutex::new(Vec::new()),
			pending_uncommitted_index_builds: Mutex::new(Vec::new()),
		}
	}

	/// Attach pre-resolved tenant identity so the emitted
	/// [`TransactionEvent`] carries the active session's namespace,
	/// database, user, session id, and client IP. Typically called by the
	/// [`crate::kvs::Datastore`] entry points that create a transaction
	/// against an authenticated session, before the transaction is enclosed
	/// in an `Arc`. Idempotent: subsequent calls are silently ignored.
	pub fn with_tenant_identity(self, identity: Option<Arc<TenantIdentity>>) -> Self {
		if let Some(id) = identity {
			let _ = self.tenant_identity.set(id);
		}
		self
	}

	/// Attach pre-resolved tenant identity to a transaction that is already
	/// wrapped in an `Arc`. Idempotent: subsequent calls are silently
	/// ignored.
	pub fn set_tenant_identity(&self, identity: Arc<TenantIdentity>) {
		let _ = self.tenant_identity.set(identity);
	}

	#[cfg(test)]
	pub(crate) fn metrics_snapshot_for_test(&self) -> crate::observe::TransactionMetricsSnapshot {
		self.metrics.snapshot()
	}

	/// Emit a [`TransactionEvent`] carrying the current counter snapshot and
	/// elapsed lifetime. Invoked from [`Self::commit`] and [`Self::cancel`].
	///
	/// Short-circuits when the installed observer is a no-op so a
	/// process running with no observers attached pays exactly nothing
	/// per commit/cancel beyond the early-return. The `metrics`
	/// snapshot, the event allocation, and the trait-object dispatch
	/// are all skipped in that case.
	fn emit_transaction_event(&self, outcome: Outcome) {
		self.emit_transaction_event_with_class(outcome, None);
	}

	/// Variant of [`Self::emit_transaction_event`] that records a bounded
	/// `error_class` on the resulting [`TransactionEvent`]. Use the
	/// canonical strings published by the server's `error_class` module
	/// (e.g. `txn_conflict`, `storage`, `internal`) so cardinality stays
	/// closed. Pass `None` for non-error outcomes.
	fn emit_transaction_event_with_class(
		&self,
		outcome: Outcome,
		error_class: Option<&'static str>,
	) {
		if self.observer.is_noop() {
			return;
		}
		self.observer.on_transaction_complete(&TransactionEvent {
			safe: TransactionEventSafe {
				outcome,
				write: self.tr.writeable(),
				duration: self.started_at.elapsed(),
				metrics: self.metrics.snapshot(),
				error_class,
			},
			ctx: self.tenant_identity.get().map(|t| t.to_transaction_ctx()).unwrap_or_default(),
		});
	}

	/// Defer release of a durable index-build reservation until close.
	///
	/// Admission commits the reservation before the user transaction writes the
	/// queued appending. Registering the prepared release immediately gives every
	/// admitted ticket a cleanup path even if fence or queue work fails. Releasing
	/// from a fresh transaction after commit/cancel keeps rollbacks from undoing
	/// the release and avoids snapshot conflicts on local engines.
	pub(crate) async fn register_index_build_reservation_release(
		&self,
		release: IndexBuildReservationRelease,
	) {
		self.pending_index_build_reservations.lock().await.push(release);
	}

	/// Look up an admission reservation cached for this user transaction.
	///
	/// Returns `Ok(Some(_))` when this transaction has already reserved a ticket
	/// for the same index earlier in its lifetime. On a hit the caller receives
	/// the cached generation and ticket plus a fresh `mutation_seq`, and `!bg`
	/// can be written without committing a new reservation transaction.
	///
	/// Returns `Ok(None)` on a miss; the caller is expected to run the short
	/// reservation transaction (`reserve_durable_admission`) and then publish
	/// the result via [`Self::insert_cached_index_build_reservation`].
	///
	/// Returns `Err(IndexingBuildingCancelled)` if `next_mutation_seq` would
	/// overflow `u32::MAX`. This caps a single user transaction at
	/// `u32::MAX` mutations on one index — any more would silently collide on
	/// the same `!bg(generation, ticket, MAX)` key, which is data loss.
	///
	/// Calls on a single `Transaction` are sequential in this codebase, so the
	/// lookup-then-allocate sequence has no observable race window.
	pub(crate) async fn lookup_cached_index_build_reservation(
		&self,
		key: &CachedIndexBuildReservationKey,
	) -> Result<Option<CachedIndexBuildReservationLookup>> {
		let mut cache = self.cached_index_build_reservations.lock().await;
		let Some(entry) = cache.get_mut(key) else {
			return Ok(None);
		};
		let mutation_seq = entry.next_mutation_seq;
		let next_seq =
			mutation_seq.checked_add(1).ok_or_else(|| Error::IndexingBuildingCancelled {
				reason: "Per-user-transaction index build mutation sequence overflowed u32::MAX"
					.to_string(),
			})?;
		entry.next_mutation_seq = next_seq;
		Ok(Some(CachedIndexBuildReservationLookup::Reused {
			generation: entry.generation,
			ticket: entry.ticket,
			mutation_seq,
			initial_complete: entry.initial_complete,
		}))
	}

	/// Test-only helper: seed the per-user-transaction reservation cache with
	/// a specific `next_mutation_seq`. Used by overflow regression tests so
	/// the failure mode can be exercised without running `u32::MAX` lookups.
	#[cfg(test)]
	pub(crate) async fn seed_cached_index_build_reservation_for_test(
		&self,
		key: CachedIndexBuildReservationKey,
		generation: BuildGeneration,
		ticket: BuildTicket,
		initial_complete: bool,
		next_mutation_seq: BuildTicketMutationSeq,
	) {
		self.cached_index_build_reservations.lock().await.insert(
			key,
			CachedIndexBuildReservation {
				generation,
				ticket,
				initial_complete,
				next_mutation_seq,
			},
		);
	}

	/// Remove a per-user-transaction admission reservation from the cache.
	///
	/// Called by `consume()` when the first-use fence returns `IndexNormally`:
	/// the cached ticket has already been released by that fence, so subsequent
	/// mutations must re-enter the reservation path and rediscover the online
	/// build phase. Without this, the next mutation would hit a stale cache
	/// entry and write `!bg(old_gen, *)` that no builder will replay.
	pub(crate) async fn remove_cached_index_build_reservation(
		&self,
		key: &CachedIndexBuildReservationKey,
	) {
		self.cached_index_build_reservations.lock().await.remove(key);
	}

	/// Publish a freshly allocated admission reservation into the per-user-txn
	/// cache and return the first-use slot.
	///
	/// The returned `mutation_seq` is always `0` (the first slot for the new
	/// ticket); the cache is advanced so the next call to
	/// [`Self::lookup_cached_index_build_reservation`] returns `mutation_seq = 1`.
	/// The caller still owns registering the release with
	/// [`Self::register_index_build_reservation_release`] and running the
	/// durable-admission fence — only first-use callers should do so.
	pub(crate) async fn insert_cached_index_build_reservation(
		&self,
		key: CachedIndexBuildReservationKey,
		generation: BuildGeneration,
		ticket: BuildTicket,
		initial_complete: bool,
	) -> CachedIndexBuildReservationLookup {
		let mut cache = self.cached_index_build_reservations.lock().await;
		cache.insert(
			key,
			CachedIndexBuildReservation {
				generation,
				ticket,
				initial_complete,
				next_mutation_seq: 1,
			},
		);
		CachedIndexBuildReservationLookup::FirstUse {
			generation,
			ticket,
			mutation_seq: 0,
			initial_complete,
		}
	}

	/// Abort a process-local index builder after this transaction commits.
	///
	/// Schema retirement deletes durable build state and catalog entries
	/// transactionally, but the local builder map is process memory. Deferring
	/// the abort until after commit keeps rollback/cancel semantics correct.
	pub(crate) async fn register_index_builder_abort_after_commit(
		&self,
		builder: IndexBuilder,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: IndexId,
	) {
		self.pending_index_builder_aborts.lock().await.push(PendingIndexBuilderAbort {
			builder,
			ns,
			db,
			tb,
			ix,
		});
	}

	/// Register a provisional index build that should be deleted unless this
	/// transaction commits its catalog definition.
	pub(crate) async fn register_uncommitted_index_build_cleanup(
		&self,
		builder: IndexBuilder,
		tf: TransactionFactory,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: IndexId,
	) {
		self.pending_uncommitted_index_builds.lock().await.push(PendingUncommittedIndexBuild {
			builder,
			tf,
			sequences: self.sequences.clone(),
			ns,
			db,
			tb,
			ix,
		});
	}

	/// Check if the transaction is local or remote
	pub fn is_local(&self) -> bool {
		self.local
	}

	/// Enclose this transaction in an [`Arc`]
	pub fn enclose(self) -> Arc<Transaction> {
		Arc::new(self)
	}

	/// Check if the transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`crate::kvs::Error::TransactionFinished`] error.
	pub fn closed(&self) -> bool {
		self.tr.closed()
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn cancel(&self) -> Result<()> {
		// Clear any buffered changefeed entries
		if let Some(changefeed) = self.changefeed.get() {
			changefeed.clear();
		}
		// Cancel the underlying transactor. Emit a transaction event on
		// either outcome so counters and durations are always reported
		// even when cancel itself reports a driver-level error.
		let result = self.tr.cancel().await.map_err(Error::from);
		let cleanup_result = self.cleanup_uncommitted_index_builds().await;
		let release_result = self.release_index_build_reservations().await;
		self.discard_index_builder_aborts().await;
		self.emit_transaction_event(Outcome::from(&result));
		result?;
		cleanup_result?;
		release_result?;
		Ok(())
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn commit(&self) -> Result<()> {
		// Store any buffered changefeed entries. Failure here falls into
		// `cancel`, which itself emits the transaction event, so avoid
		// double-emission from this path.
		if let Err(e) = self.store_changes().await {
			if let Err(err) = self.cancel().await {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"transaction cleanup failed after changefeed storage failed; preserving original store_changes error {e}: {err}"
				);
			}
			// The cleanup error is secondary here. Callers need the original
			// store_changes error so retry/error classification uses the
			// operation that first made commit impossible.
			return Err(e);
		}
		// Commit the transaction
		if let Err(e) = self.tr.commit().await {
			let cleanup_result = self.cleanup_uncommitted_index_builds().await;
			let release_result = self.release_index_build_reservations().await;
			self.discard_index_builder_aborts().await;
			// Classify the commit failure so the surrealdb.transaction.* metric
			// family can carry an `error_class` attribute. `e` is a concrete
			// `kvs::Error` here -- the transactor's `commit` returns
			// `kvs::Result<()>` (see `kvs/tr.rs`) -- so we apply the
			// kvs-layer rule directly: retryable variants collapse to
			// `txn_conflict`, everything else to `storage`. The shared
			// `classify_anyhow_error` helper applies the same rule from
			// the `anyhow::Error` path used by the executor.
			let class = if e.is_retryable() {
				crate::observe::error_class::TXN_CONFLICT
			} else {
				crate::observe::error_class::STORAGE
			};
			self.emit_transaction_event_with_class(Outcome::Error, Some(class));
			if let Err(err) = release_result {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"durable index-build reservation cleanup failed after transaction commit failed; preserving original commit error {e}: {err}"
				);
			}
			if let Err(err) = cleanup_result {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"uncommitted index-build cleanup failed after transaction commit failed; preserving original commit error {e}: {err}"
				);
			}
			// The cleanup error is secondary here. Callers need the commit
			// error so retryable transaction conflicts keep their retry path.
			anyhow::bail!(e);
		}
		if let Err(err) = self.release_index_build_reservations().await {
			tracing::warn!(
				target: "surrealdb::core::kvs::tx",
				"durable index-build reservation cleanup failed after transaction commit; committed appendings remain recoverable: {err}"
			);
		}
		self.discard_uncommitted_index_builds().await;
		self.run_index_builder_aborts().await;
		if self.trigger_async_event.load(Ordering::Relaxed) {
			// Notify after commit so queued events are visible to workers.
			self.async_event_trigger.notify_one();
		}
		self.emit_transaction_event(Outcome::Success);
		Ok(())
	}

	/// Check if a key exists in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn exists<K>(&self, key: &K, version: Option<u64>) -> Result<bool>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		let found = self.tr.exists(key, version).await.map_err(Error::from)?;
		self.metrics.record_get(u64::from(found), key_bytes, 0);
		Ok(found)
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get<K>(&self, key: &K, version: Option<u64>) -> Result<Option<K::ValueType>>
	where
		K: KVKey + Debug,
	{
		let encoded = key.encode_key()?;
		let key_bytes = encoded.len() as u64;
		let val = self.tr.get(encoded, version).await.map_err(Error::from)?;
		let (keys_found, value_bytes) = match &val {
			Some(v) => (1, v.len() as u64),
			None => (0, 0),
		};
		self.metrics.record_get(keys_found, key_bytes, value_bytes);
		// Build the decode context only on a hit. For `RecordKey` this
		// avoids a `RecordId` clone (table + key) on every miss.
		val.map(|v| K::ValueType::kv_decode_value(&v, key.value_context())).transpose()
	}

	/// Retrieve a batch set of keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getm<K>(
		&self,
		keys: Vec<K>,
		version: Option<u64>,
	) -> Result<Vec<Option<K::ValueType>>>
	where
		K: KVKey + Debug,
	{
		let encoded_keys: Vec<_> = keys.iter().map(|k| k.encode_key()).collect::<Result<_>>()?;
		let key_bytes: u64 = encoded_keys.iter().map(|k| k.len() as u64).sum();
		let res = self.tr.getm(encoded_keys, version).await.map_err(Error::from)?;
		self.metrics.record_get(res.records, key_bytes, res.value_bytes);
		res.values
			.into_iter()
			.zip(keys)
			.map(|(v, k)| match v {
				Some(v) => K::ValueType::kv_decode_value(&v, k.value_context()).map(Some),
				None => Ok(None),
			})
			.collect()
	}

	/// Retrieve a specific prefix of keys from the datastore.
	///
	/// Range scans are intentionally restricted to value types with
	/// `KeyContext = ()`: per-row context isn't available without decoding
	/// each row's storage key. Callers that need to scan a record range
	/// must decode the storage key per row and reconstruct the context
	/// themselves.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getp<K>(&self, key: &K, version: Option<u64>) -> Result<Vec<(Key, K::ValueType)>>
	where
		K: KVKey + Debug,
		K::ValueType: KVValue<KeyContext = ()>,
	{
		let key = key.encode_key()?;
		let res = self.tr.getp(key, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		res.values
			.into_iter()
			.map(|(k, v)| Ok((k, K::ValueType::kv_decode_value(&v, ())?)))
			.collect()
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// As with [`Self::getp`], restricted to value types with
	/// `KeyContext = ()`.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getr<K>(
		&self,
		rng: Range<K>,
		version: Option<u64>,
	) -> Result<Vec<(Key, K::ValueType)>>
	where
		K: KVKey + Debug,
		K::ValueType: KVValue<KeyContext = ()>,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		let res = self.tr.getr(beg..end, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		res.values
			.into_iter()
			.map(|(k, v)| Ok((k, K::ValueType::kv_decode_value(&v, ())?)))
			.collect()
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		self.tr.del(key).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delc<K>(&self, key: &K, chk: Option<&K::ValueType>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		self.tr.delc(key, chk).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delr<K>(&self, rng: Range<K>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		self.tr.delr(beg..end).await.map_err(Error::from)?;
		// Range/prefix deletes don't report the number of affected keys or
		// their byte size.
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Delete a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delp<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		self.tr.delp(key).await.map_err(Error::from)?;
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clr<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		self.tr.clr(key).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrc<K>(&self, key: &K, chk: Option<&K::ValueType>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		self.tr.clrc(key, chk).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrr<K>(&self, rng: Range<K>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		self.tr.clrr(beg..end).await.map_err(Error::from)?;
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Delete all versions of a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrp<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		self.tr.clrp(key).await.map_err(Error::from)?;
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn set<K>(&self, key: &K, val: &K::ValueType) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.set(key, val).await.map_err(Error::from)?;
		self.metrics.record_set(key_bytes, value_bytes);
		Ok(())
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn put<K>(&self, key: &K, val: &K::ValueType) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.put(key, val).await.map_err(Error::from)?;
		self.metrics.record_put(key_bytes, value_bytes);
		Ok(())
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn putc<K>(
		&self,
		key: &K,
		val: &K::ValueType,
		chk: Option<&K::ValueType>,
	) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.putc(key, val, chk).await.map_err(Error::from)?;
		self.metrics.record_put(key_bytes, value_bytes);
		Ok(())
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn replace<K>(&self, key: &K, val: &K::ValueType) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.replace(key, val).await.map_err(Error::from)?;
		self.metrics.record_put(key_bytes, value_bytes);
		Ok(())
	}

	// --------------------------------------------------
	// Raw bytes functions
	// --------------------------------------------------

	/// Fetch a key from the datastore, without decoding.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get_raw<K>(&self, key: &K, version: Option<u64>) -> Result<Option<Val>>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		let val = self.tr.get(key, version).await.map_err(Error::from)?;
		let (keys_found, value_bytes) = match &val {
			Some(v) => (1, v.len() as u64),
			None => (0, 0),
		};
		self.metrics.record_get(keys_found, key_bytes, value_bytes);
		Ok(val)
	}

	/// Retrieve a batch set of keys from the datastor, without decoding.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getm_raw<K>(&self, keys: Vec<K>, version: Option<u64>) -> Result<Vec<Option<Val>>>
	where
		K: KVKey + Debug,
	{
		let keys = keys.iter().map(|k| k.encode_key()).collect::<Result<Vec<_>>>()?;
		let key_bytes: u64 = keys.iter().map(|k| k.len() as u64).sum();
		let res = self.tr.getm(keys, version).await.map_err(Error::from)?;
		self.metrics.record_get(res.records, key_bytes, res.value_bytes);
		Ok(res.values)
	}

	// --------------------------------------------------
	// Range functions
	// --------------------------------------------------

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys, in a single request to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keys<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		let limit = limit.into();
		let res = self.tr.keys(beg..end, limit, skip, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.keys.len() as u64, res.key_bytes, 0);
		Ok(res.keys)
	}

	/// Retrieve a specific range of keys from the datastore in reverse order.
	///
	/// This function fetches the full range of keys, in a single request to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keysr<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		let limit = limit.into();
		let res = self.tr.keysr(beg..end, limit, skip, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.keys.len() as u64, res.key_bytes, 0);
		Ok(res.keys)
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scan<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		let limit = limit.into();
		let res = self.tr.scan(beg..end, limit, skip, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		Ok(res.values)
	}

	/// Retrieve a specific range of keys from the datastore, in reverse order.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scanr<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		let limit = limit.into();
		let res = self.tr.scanr(beg..end, limit, skip, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		Ok(res.values)
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total count, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn count<K>(&self, rng: Range<K>, version: Option<u64>) -> Result<usize>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		let n = self.tr.count(beg..end, version).await.map_err(Error::from)?;
		// `count` only reports the number of keys, not their byte size.
		self.metrics.record_scan(n as u64, 0, 0);
		Ok(n)
	}

	// --------------------------------------------------
	// Cursor functions
	// --------------------------------------------------

	/// Open a stateful keys-only scan cursor over a typed range.
	///
	/// The cursor reuses one underlying iterator across batches for the
	/// duration of a single logical scan (e.g. an outer table walk or one
	/// prefix of a graph traversal). Each [`ScanCursorKeys::next_batch`]
	/// call advances the same iterator instead of re-seeking from scratch.
	/// `skip` is applied once on the first batch.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn open_keys_cursor(
		&self,
		rng: Range<Key>,
		dir: ScanDirection,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredKeysCursor<'_>> {
		let inner = self
			.tr
			.open_keys_cursor(
				rng,
				match dir {
					ScanDirection::Forward => Direction::Forward,
					ScanDirection::Backward => Direction::Backward,
				},
				skip,
				version,
			)
			.await
			.map_err(Error::from)?;
		Ok(MeteredKeysCursor {
			inner,
			metrics: &self.metrics,
		})
	}

	/// Open a stateful key+value scan cursor over a raw-byte range. See
	/// [`Self::open_keys_cursor`].
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn open_vals_cursor(
		&self,
		rng: Range<Key>,
		dir: ScanDirection,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredValsCursor<'_>> {
		let inner = self
			.tr
			.open_vals_cursor(
				rng,
				match dir {
					ScanDirection::Forward => Direction::Forward,
					ScanDirection::Backward => Direction::Backward,
				},
				skip,
				version,
			)
			.await
			.map_err(Error::from)?;
		Ok(MeteredValsCursor {
			inner,
			metrics: &self.metrics,
		})
	}

	// --------------------------------------------------
	// Batch functions
	// --------------------------------------------------

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the keys in batches, with multiple requests to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys<K>(
		&self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Key>>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.batch_keys(beg..end, batch, version).await.map_err(Error::from)?)
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys_vals<K>(
		&self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.batch_keys_vals(beg..end, batch, version).await.map_err(Error::from)?)
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------

	/// Set a new save point on the transaction.
	pub async fn new_save_point(&self) -> Result<()> {
		Ok(self.inner.new_save_point().await.map_err(Error::from)?)
	}

	/// Release the last save point.
	pub async fn release_last_save_point(&self) -> Result<()> {
		Ok(self.inner.release_last_save_point().await.map_err(Error::from)?)
	}

	/// Rollback to the last save point.
	pub async fn rollback_to_save_point(&self) -> Result<()> {
		Ok(self.inner.rollback_to_save_point().await.map_err(Error::from)?)
	}

	// --------------------------------------------------
	// Timestamp functions
	// --------------------------------------------------

	/// Get the current monotonic timestamp
	pub async fn timestamp(&self) -> Result<BoxTimeStamp> {
		Ok(self.tr.timestamp().await.map_err(Error::from)?)
	}

	/// Returns the implementation of timestamp that this transaction uses.
	pub fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.tr.timestamp_impl()
	}

	// --------------------------------------------------
	// Changefeed functions
	// --------------------------------------------------

	/// Records the table (re)definition in the changefeed if enabled.
	pub(crate) fn changefeed_buffer_table_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		dt: &TableDefinition,
	) {
		self.changefeed.get_or_init(Changefeed::new).buffer_table_change(ns, db, tb, dt)
	}

	/// change will record the change in the changefeed if enabled.
	/// To actually persist the record changes into the underlying kvs,
	/// you must call the `complete_changes` function and then commit the
	/// transaction.
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn changefeed_buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordId,
		previous: CursorRecord,
		current: CursorRecord,
		store_difference: bool,
	) {
		self.changefeed.get_or_init(Changefeed::new).buffer_record_change(
			ns,
			db,
			tb,
			id.clone(),
			previous,
			current,
			store_difference,
		)
	}

	/// complete_changes will complete the changefeed recording for the given
	/// namespace and database.
	///
	/// This function writes all buffered changefeed entries to the datastore
	/// with the current transaction timestamp. Every change must be recorded by
	/// calling this struct's `changefeed_buffer_record_change` function beforehand.
	/// If there were no preceding calls for this transaction, this function
	/// will do nothing.
	///
	/// This function should be called only after all the changes have been made to
	/// the transaction. Otherwise, changes are missed in the change feed.
	///
	/// This function should be called immediately before calling the commit function
	/// to ensure the timestamp reflects the actual commit time.
	pub(crate) async fn store_changes(&self) -> Result<()> {
		// If no changefeed writer, there are no changes
		let Some(changefeed) = self.changefeed.get() else {
			return Ok(());
		};
		// Get the changes from the changefeed
		let changes = changefeed.changes()?;
		// For zero-length changes, return early
		if changes.is_empty() {
			return Ok(());
		}
		// Get the current transaction timestamp
		let buf = &mut [0u8; _];
		let ts = self.timestamp().await?.encode(buf);
		// Collect all changefeed write operations as futures
		let futures = changes.into_iter().map(|(ns, db, tb, value)| async move {
			// Create the changefeed key with the current timestamp
			let key = crate::key::change::new(ns, db, ts, &tb).encode_key()?;
			// Write the changefeed entry using the raw transactor API
			self.tr.set(key, value).await.map_err(Error::from)?;
			// Everything succeeded
			Ok::<(), anyhow::Error>(())
		});
		// Execute all write operations concurrently
		try_join_all(futures).await?;
		// All good
		Ok(())
	}

	// --------------------------------------------------
	// Index functions
	// --------------------------------------------------

	/// Drain and release every queued durable index-build reservation.
	///
	/// Called from both the commit and cancel paths so that each admitted
	/// ticket is released exactly once after the user transaction terminates.
	/// All reservations are attempted even when one fails; the first error is
	/// preserved and returned so the caller can surface it while later
	/// releases still get a chance to run.
	///
	/// As an optimization, when there are multiple queued reservations they
	/// are deleted in a single short transaction instead of one transaction
	/// per release. A user transaction that wrote to several indexes therefore
	/// pays one commit at close time instead of one per index. If the batch
	/// commit fails (retryable conflict, snapshot conflict on local engines,
	/// or a non-retryable storage error), the function falls back to the
	/// per-reservation release path so each failure can drive its own
	/// `mark_build_error_if_uncommitted` reasoning.
	async fn release_index_build_reservations(&self) -> Result<()> {
		// Take the queued reservations under the lock so concurrent registrations
		// see an empty queue while releases are in flight.
		let reservations = {
			let mut pending = self.pending_index_build_reservations.lock().await;
			std::mem::take(&mut *pending)
		};
		if reservations.is_empty() {
			return Ok(());
		}
		// Try the batched delete first, then fall back to the per-reservation
		// path on any error. The fallback preserves the per-reservation
		// retry + build-error-marking behavior for any release that didn't
		// already succeed in the batch.
		let reservations = match IndexBuildReservationRelease::release_batch(reservations).await {
			Ok(()) => return Ok(()),
			Err(remaining) => remaining,
		};
		// Per-reservation slow path. Reused for the rare case where the batch
		// failed; each release individually retries on retryable conflicts
		// and marks the build errored if its appendings never landed.
		let mut first_error = None;
		for reservation in reservations {
			if let Err(err) = reservation.release().await
				&& first_error.is_none()
			{
				first_error = Some(err);
			}
		}
		if let Some(err) = first_error {
			Err(err)
		} else {
			Ok(())
		}
	}

	/// Drain and clean up provisional index builds for an uncommitted schema.
	///
	/// `DEFINE INDEX` starts the in-process builder while its schema
	/// transaction is still open, and the builder may have committed durable
	/// build state and index data from separate transactions before the
	/// schema transaction terminates. When that schema transaction is
	/// cancelled or fails to commit, this function removes the orphaned
	/// durable state so a later retry sees a clean slate. Every cleanup is
	/// attempted even when one fails, and the first error is returned.
	async fn cleanup_uncommitted_index_builds(&self) -> Result<()> {
		// Take the queued cleanups under the lock to detach them from any
		// concurrent registrations
		let builds = {
			let mut pending = self.pending_uncommitted_index_builds.lock().await;
			std::mem::take(&mut *pending)
		};
		// Attempt every cleanup, remembering the first failure
		let mut first_error = None;
		for build in builds {
			if let Err(err) = build.cleanup().await
				&& first_error.is_none()
			{
				first_error = Some(err);
			}
		}
		// Surface the first failure once all cleanups have been attempted
		if let Some(err) = first_error {
			Err(err)
		} else {
			Ok(())
		}
	}

	/// Discard queued provisional index-build cleanups without running them.
	///
	/// Invoked on the commit path once the schema transaction has succeeded:
	/// the catalog row is now durable, so the index build is no longer
	/// provisional and its durable state must be retained for the builder to
	/// finish its work.
	async fn discard_uncommitted_index_builds(&self) {
		self.pending_uncommitted_index_builds.lock().await.clear();
	}

	/// Drain and abort every queued in-process index builder.
	///
	/// Used after a successful schema retirement commit to stop the
	/// non-transactional in-process builder for an index whose durable state
	/// and catalog entry have already been removed transactionally. Deferring
	/// the abort until commit avoids stopping a still-valid build if the
	/// schema transaction is rolled back or cancelled.
	async fn run_index_builder_aborts(&self) {
		// Take the queued aborts under the lock so concurrent registrations
		// see an empty queue while aborts are in flight
		let aborts = {
			let mut pending = self.pending_index_builder_aborts.lock().await;
			std::mem::take(&mut *pending)
		};
		// Run every abort; aborting an in-process builder is infallible
		for abort in aborts {
			abort.abort().await;
		}
	}

	/// Discard queued in-process builder aborts without running them.
	///
	/// Invoked on the cancel path and on commit failure so a builder whose
	/// retirement did not become durable is not stopped: the catalog still
	/// references the index, and the build must keep running.
	async fn discard_index_builder_aborts(&self) {
		self.pending_index_builder_aborts.lock().await.clear();
	}

	// --------------------------------------------------
	// Cache functions
	// --------------------------------------------------

	/// Clears all keys from the transaction cache.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub fn clear_cache(&self) {
		self.cache.clear()
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn compact<K>(&self, key: Option<K>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let rng = match key {
			Some(key) => Some(util::to_prefix_range(&key)?),
			None => None,
		};
		self.tr.inner.compact(rng).await
	}

	/// Mark this transaction to wake the async event processor after commit.
	pub(crate) fn trigger_async_event(&self) {
		self.trigger_async_event.store(true, Ordering::Relaxed);
	}
}

// --------------------------------------------------
// Node implementation functions
// --------------------------------------------------

impl NodeProvider for Transaction {
	/// Retrieve all nodes belonging to this cluster.
	fn all_nodes(&self) -> BoxProviderFut<'_, Result<Arc<[Node]>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Nds;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nds(),
					None => {
						let beg = crate::key::root::nd::prefix();
						let end = crate::key::root::nd::suffix();
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Nds(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_nodes")),
		)
	}

	/// Retrieve a specific node in the cluster.
	fn get_node(&self, id: Uuid) -> BoxProviderFut<'_, Result<Arc<Node>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Nd(id);
				match self.cache.get(&qey) {
					Some(val) => val,
					None => {
						let key = crate::key::root::nd::new(id);
						let val = self.get(&key, None).await?.ok_or_else(|| Error::NdNotFound {
							uuid: id.to_string(),
						})?;
						let val = cache::tx::Entry::Any(Arc::new(val));
						self.cache.insert(qey, val.clone());
						val
					}
				}
				.try_into_type()
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_node")),
		)
	}
}

// --------------------------------------------------
// Root implementation functions
// --------------------------------------------------

impl RootProvider for Transaction {
	fn get_default_config(&self) -> BoxProviderFut<'_, Result<Option<Arc<DefaultConfig>>>> {
		Box::pin(async move {
			let qey = cache::tx::Lookup::Rcg("default");
			match self.cache.get(&qey) {
				Some(val) => val,
				None => {
					let key = crate::key::root::root_config::new("default");
					let Some(val) = self.get(&key, None).await? else {
						return Ok(None);
					};
					let ConfigDefinition::Default(val) = val else {
						fail!("Expected a default config but found {val:?} instead");
					};
					let val = cache::tx::Entry::Any(Arc::new(val));
					self.cache.insert(qey, val.clone());
					val
				}
			}
			.try_into_type()
			.map(Option::Some)
		})
	}

	/// Retrieve a specific config definition from the root.
	fn get_root_config<'a>(
		&'a self,
		cg: &'a str,
	) -> BoxProviderFut<'a, Result<Option<Arc<ConfigDefinition>>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Rcg(cg);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Option::Some),
					None => {
						let key = crate::key::root::root_config::new(cg);
						if let Some(val) = self.get(&key, None).await? {
							let val = Arc::new(val);
							let entr = cache::tx::Entry::Any(val.clone());
							self.cache.insert(qey, entr);
							Ok(Some(val))
						} else {
							Ok(None)
						}
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_root_config")),
		)
	}
}

// --------------------------------------------------
// Namespace implementation functions
// --------------------------------------------------

impl NamespaceProvider for Transaction {
	/// Retrieve all namespace definitions in a datastore.
	fn all_ns(
		&self,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[NamespaceDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::root::ns::prefix();
					let end = crate::key::root::ns::suffix();
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Nss;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nss(),
					None => {
						let beg = crate::key::root::ns::prefix();
						let end = crate::key::root::ns::suffix();
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Nss(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_ns")),
		)
	}

	fn get_ns_by_name<'a>(
		&'a self,
		ns: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<NamespaceDefinition>>>> {
		Box::pin(async move {
			if version.is_some() {
				let key = crate::key::root::ns::new(ns);
				let Some(ns) = self.get(&key, version).await? else {
					return Ok(None);
				};
				return Ok(Some(Arc::new(ns)));
			}
			let qey = cache::tx::Lookup::NsByName(ns);
			match self.cache.get(&qey) {
				Some(val) => val.try_into_type().map(Some),
				None => {
					let key = crate::key::root::ns::new(ns);
					let Some(ns) = self.get(&key, None).await? else {
						return Ok(None);
					};

					let ns = Arc::new(ns);
					let entr = cache::tx::Entry::Any(ns.clone());
					self.cache.insert(qey, entr);
					Ok(Some(ns))
				}
			}
		})
	}

	fn expect_ns_by_name<'a>(
		&'a self,
		ns: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<NamespaceDefinition>>> {
		Box::pin(async move {
			match self.get_ns_by_name(ns, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(Error::NsNotFound {
					name: ns.to_owned(),
				}),
			}
		})
	}

	fn put_ns(
		&self,
		ns: NamespaceDefinition,
	) -> BoxProviderFut<'_, Result<Arc<NamespaceDefinition>>> {
		Box::pin(async move {
			let key = crate::key::root::ns::new(&ns.name);
			self.set(&key, &ns).await?;

			// Invalidate the cached list of all namespaces
			let list_key = cache::tx::Lookup::Nss;
			self.cache.remove(&list_key);

			// Populate cache
			let cached_ns = Arc::new(ns.clone());

			let entry = cache::tx::Entry::Any(Arc::clone(&cached_ns) as Arc<dyn Any + Send + Sync>);
			let qey = cache::tx::Lookup::NsByName(&ns.name);
			self.cache.insert(qey, entry);

			Ok(cached_ns)
		})
	}

	fn get_next_ns_id<'a>(
		&'a self,
		ctx: Option<&'a Context>,
	) -> BoxProviderFut<'a, Result<NamespaceId>> {
		Box::pin(async move { self.sequences.next_namespace_id(ctx).await })
	}

	fn del_ns<'a>(&'a self, ns: &'a str, expunge: bool) -> BoxProviderFut<'a, Result<Option<()>>> {
		Box::pin(async move {
			let Some(ns_def) = self.get_ns_by_name(ns, None).await? else {
				return Ok(None);
			};
			let key = crate::key::root::ns::new(&ns_def.name);
			let namespace_root = crate::key::namespace::all::new(ns_def.namespace_id);
			if expunge {
				self.clr(&key).await?;
				self.clrp(&namespace_root).await?;
			} else {
				self.del(&key).await?;
				self.delp(&namespace_root).await?;
			};

			// Invalidate the cached list of all namespaces
			let list_key = cache::tx::Lookup::Nss;
			self.cache.remove(&list_key);

			// Invalidate the cached namespace entry
			let ns_key = cache::tx::Lookup::NsByName(&ns_def.name);
			self.cache.remove(&ns_key);

			Ok(Some(()))
		})
	}
}

// --------------------------------------------------
// Database implementation functions
// --------------------------------------------------

impl DatabaseProvider for Transaction {
	/// Retrieve all database definitions for a specific namespace.
	fn all_db(
		&self,
		ns: NamespaceId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[DatabaseDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::namespace::db::prefix(ns)?;
					let end = crate::key::namespace::db::suffix(ns)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Dbs(ns);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_dbs(),
					None => {
						let beg = crate::key::namespace::db::prefix(ns)?;
						let end = crate::key::namespace::db::suffix(ns)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Dbs(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db")),
		)
	}

	/// Retrieve a specific database definition.
	fn get_db_by_name<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<DatabaseDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let Some(ns) = self.get_ns_by_name(ns, version).await? else {
						return Ok(None);
					};
					let key = crate::key::namespace::db::new(ns.namespace_id, db);
					let Some(db_def) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(db_def)));
				}
				let qey = cache::tx::Lookup::DbByName(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let Some(ns) = self.get_ns_by_name(ns, None).await? else {
							return Ok(None);
						};

						let key = crate::key::namespace::db::new(ns.namespace_id, db);
						let Some(db_def) = self.get(&key, None).await? else {
							return Ok(None);
						};

						let val = Arc::new(db_def);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_by_name")),
		)
	}

	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	fn get_or_add_db_upwards<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: &'a str,
		db: &'a str,
		upwards: bool,
	) -> BoxProviderFut<'a, Result<Arc<DatabaseDefinition>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::DbByName(ns, db);
				match self.cache.get(&qey) {
					// The entry is in the cache
					Some(val) => {
						let t = val.try_into_type()?;
						Ok(t)
					}
					// The entry is not in the cache
					None => {
						let db_def = self.get_db_by_name(ns, db, None).await?;
						if let Some(db_def) = db_def {
							return Ok(db_def);
						}

						let ns_def = if upwards {
							self.get_or_add_ns(ctx, ns).await?
						} else {
							match self.get_ns_by_name(ns, None).await? {
								Some(ns_def) => ns_def,
								None => {
									return Err(Error::NsNotFound {
										name: ns.to_owned(),
									}
									.into());
								}
							}
						};

						let db_def = DatabaseDefinition {
							namespace_id: ns_def.namespace_id,
							database_id: self.get_next_db_id(ctx, ns_def.namespace_id).await?,
							name: db.into(),
							comment: None,
							changefeed: None,
							strict: false,
						};

						return self.put_db(ns_def.name.as_str(), db_def).await;
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_or_add_db_upwards")),
		)
	}

	fn get_next_db_id<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: NamespaceId,
	) -> BoxProviderFut<'a, Result<DatabaseId>> {
		Box::pin(async move { self.sequences.next_database_id(ctx, ns).await })
	}

	fn put_db<'a>(
		&'a self,
		ns: &'a str,
		db: DatabaseDefinition,
	) -> BoxProviderFut<'a, Result<Arc<DatabaseDefinition>>> {
		Box::pin(async move {
			let key = crate::key::namespace::db::new(db.namespace_id, &db.name);
			self.set(&key, &db).await?;

			// Invalidate the cached list of all databases for this namespace
			let list_key = cache::tx::Lookup::Dbs(db.namespace_id);
			self.cache.remove(&list_key);

			// Populate cache
			let cached_db = Arc::new(db.clone());

			let entry = cache::tx::Entry::Any(Arc::clone(&cached_db) as Arc<dyn Any + Send + Sync>);
			let qey = cache::tx::Lookup::DbByName(ns, &db.name);
			self.cache.insert(qey, entry);

			Ok(cached_db)
		})
	}

	fn del_db<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		expunge: bool,
	) -> BoxProviderFut<'a, Result<Option<()>>> {
		Box::pin(async move {
			let Some(db) = self.get_db_by_name(ns, db, None).await? else {
				return Ok(None);
			};
			let key = crate::key::namespace::db::new(db.namespace_id, &db.name);
			let database_root = crate::key::database::all::new(db.namespace_id, db.database_id);
			if expunge {
				self.clr(&key).await?;
				self.clrp(&database_root).await?;
			} else {
				self.del(&key).await?;
				self.delp(&database_root).await?
			};

			// Invalidate the cached list of all databases for this namespace
			let list_key = cache::tx::Lookup::Dbs(db.namespace_id);
			self.cache.remove(&list_key);

			// Invalidate the cached database entry
			let db_key = cache::tx::Lookup::DbByName(ns, &db.name);
			self.cache.remove(&db_key);

			Ok(Some(()))
		})
	}

	/// Retrieve all analyzer definitions for a specific database.
	fn all_db_analyzers(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AnalyzerDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::az::prefix(ns, db)?;
					let end = crate::key::database::az::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Azs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_azs(),
					None => {
						let beg = crate::key::database::az::prefix(ns, db)?;
						let end = crate::key::database::az::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Azs(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_analyzers")),
		)
	}

	/// Retrieve all sequences definitions for a specific database.
	fn all_db_sequences(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::SequenceDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::sq::prefix(ns, db)?;
					let end = crate::key::database::sq::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Sqs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_sqs(),
					None => {
						let beg = crate::key::database::sq::prefix(ns, db)?;
						let end = crate::key::database::sq::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Sqs(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_sequences")),
		)
	}

	/// Retrieve all function definitions for a specific database.
	fn all_db_functions(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::FunctionDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::fc::prefix(ns, db)?;
					let end = crate::key::database::fc::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Fcs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_fcs(),
					None => {
						let beg = crate::key::database::fc::prefix(ns, db)?;
						let end = crate::key::database::fc::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Fcs(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_functions")),
		)
	}

	/// Retrieve all module definitions for a specific database.
	fn all_db_modules(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ModuleDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::md::prefix(ns, db)?;
					let end = crate::key::database::md::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Mds(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_mds(),
					None => {
						let beg = crate::key::database::md::prefix(ns, db)?;
						let end = crate::key::database::md::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Mds(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_modules")),
		)
	}

	/// Retrieve all param definitions for a specific database.
	fn all_db_params(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ParamDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::pa::prefix(ns, db)?;
					let end = crate::key::database::pa::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Pas(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_pas(),
					None => {
						let beg = crate::key::database::pa::prefix(ns, db)?;
						let end = crate::key::database::pa::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Pas(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_params")),
		)
	}

	/// Retrieve all model definitions for a specific database.
	fn all_db_models(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::MlModelDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::ml::prefix(ns, db)?;
					let end = crate::key::database::ml::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Mls(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_mls(),
					None => {
						let beg = crate::key::database::ml::prefix(ns, db)?;
						let end = crate::key::database::ml::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Mls(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_models")),
		)
	}

	/// Retrieve all config definitions for a specific database.
	fn all_db_configs(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[ConfigDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::cg::prefix(ns, db)?;
					let end = crate::key::database::cg::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Cgs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_cgs(),
					None => {
						let beg = crate::key::database::cg::prefix(ns, db)?;
						let end = crate::key::database::cg::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Cgs(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_configs")),
		)
	}

	/// Retrieve a specific model definition from a database.
	fn get_db_model<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ml: &'a str,
		vn: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::MlModelDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::ml::new(ns, db, ml, vn);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ml(ns, db, ml, vn);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::ml::new(ns, db, ml, vn);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_model")),
		)
	}

	/// Retrieve a specific analyzer definition from a database.
	fn get_db_analyzer<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		az: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::AnalyzerDefinition>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::az::new(ns, db, az);
					let val = self.get(&key, version).await?.ok_or_else(|| Error::AzNotFound {
						name: az.to_owned(),
					})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Az(ns, db, az);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::database::az::new(ns, db, az);
						let val = self.get(&key, None).await?.ok_or_else(|| Error::AzNotFound {
							name: az.to_owned(),
						})?;
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_analyzer")),
		)
	}

	/// Retrieve a specific sequence definition from a database.
	fn get_db_sequence<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::SequenceDefinition>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = Sq::new(ns, db, sq);
					let val = self.get(&key, version).await?.ok_or_else(|| Error::SeqNotFound {
						name: sq.to_owned(),
					})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Sq(ns, db, sq);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = Sq::new(ns, db, sq);
						let val =
							self.get(&key, None).await?.ok_or_else(|| Error::SeqNotFound {
								name: sq.to_owned(),
							})?;
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_sequence")),
		)
	}

	/// Retrieve a specific function definition from a database.
	fn get_db_function<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::FunctionDefinition>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::fc::new(ns, db, fc);
					let val = self.get(&key, version).await?.ok_or_else(|| Error::FcNotFound {
						name: fc.to_owned(),
					})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Fc(ns, db, fc);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::database::fc::new(ns, db, fc);
						let val = self.get(&key, None).await?.ok_or_else(|| Error::FcNotFound {
							name: fc.to_owned(),
						})?;
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_function")),
		)
	}

	/// Retrieve a specific module definition from a database.
	fn get_db_module<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		md: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::ModuleDefinition>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::md::new(ns, db, md);
					let val = self.get(&key, version).await?.ok_or_else(|| Error::MdNotFound {
						name: md.to_owned(),
					})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Md(ns, db, md);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::database::md::new(ns, db, md);
						let val = self.get(&key, None).await?.ok_or_else(|| Error::MdNotFound {
							name: md.to_owned(),
						})?;
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_module")),
		)
	}

	/// Retrieve a specific param definition from a database.
	fn get_db_param<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::ParamDefinition>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::pa::new(ns, db, pa);
					let val = self.get(&key, version).await?.ok_or_else(|| Error::PaNotFound {
						name: pa.to_owned(),
					})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Pa(ns, db, pa);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::database::pa::new(ns, db, pa);
						let val = self.get(&key, None).await?.ok_or_else(|| Error::PaNotFound {
							name: pa.to_owned(),
						})?;
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_param")),
		)
	}

	/// Retrieve a specific config definition from a database.
	fn get_db_config<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<ConfigDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::cg::new(ns, db, cg);
					if let Some(val) = self.get(&key, version).await? {
						return Ok(Some(Arc::new(val)));
					} else {
						return Ok(None);
					}
				}
				let qey = cache::tx::Lookup::Cg(ns, db, cg);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Option::Some),
					None => {
						let key = crate::key::database::cg::new(ns, db, cg);
						if let Some(val) = self.get(&key, None).await? {
							let val = Arc::new(val);
							let entr = cache::tx::Entry::Any(val.clone());
							self.cache.insert(qey, entr);
							Ok(Some(val))
						} else {
							Ok(None)
						}
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_config")),
		)
	}

	fn put_db_function<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &'a catalog::FunctionDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = crate::key::database::fc::new(ns, db, &fc.name);
			self.set(&key, fc).await?;

			// Invalidate the cached list of all functions for this database
			let list_key = cache::tx::Lookup::Fcs(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Fc(ns, db, &fc.name);
			let entry = cache::tx::Entry::Any(Arc::new(fc.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}

	fn put_db_module<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		md: &'a catalog::ModuleDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let name = md.get_storage_name()?;
			let key = crate::key::database::md::new(ns, db, &name);
			self.set(&key, md).await?;

			// Invalidate the cached list of all modules for this database
			let list_key = cache::tx::Lookup::Mds(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Md(ns, db, &name);
			let entry = cache::tx::Entry::Any(Arc::new(md.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}

	fn put_db_param<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &'a catalog::ParamDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = crate::key::database::pa::new(ns, db, &pa.name);
			self.set(&key, pa).await?;

			// Invalidate the cached list of all params for this database
			let list_key = cache::tx::Lookup::Pas(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Pa(ns, db, &pa.name);
			let entry = cache::tx::Entry::Any(Arc::new(pa.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}
}

// --------------------------------------------------
// Table implementation functions
// --------------------------------------------------

impl TableProvider for Transaction {
	/// Retrieve all table definitions for a specific database.
	fn all_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[TableDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::tb::prefix(ns, db)?;
					let end = crate::key::database::tb::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Tbs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_tbs(),
					None => {
						let beg = crate::key::database::tb::prefix(ns, db)?;
						let end = crate::key::database::tb::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Tbs(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb")),
		)
	}

	/// Retrieve all view definitions for a specific table.
	fn all_tb_views<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::TableDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::table::ft::prefix(ns, db, tb)?;
					let end = crate::key::table::ft::suffix(ns, db, tb)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Fts(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_fts(),
					None => {
						let beg = crate::key::table::ft::prefix(ns, db, tb)?;
						let end = crate::key::table::ft::suffix(ns, db, tb)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Fts(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_views")),
		)
	}

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode. When a version is specified, skips the auto-create path.
	fn get_or_add_tb<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<TableDefinition>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let Some(db_def) = self.get_db_by_name(ns, db, version).await? else {
						return Err(anyhow::anyhow!(Error::DbNotFound {
							name: db.to_owned(),
						}));
					};
					let table_key =
						crate::key::database::tb::new(db_def.namespace_id, db_def.database_id, tb);
					if let Some(tb_def) = self.get(&table_key, version).await? {
						return Ok(Arc::new(tb_def));
					}
					return Err(Error::TbNotFound {
						name: tb.to_owned(),
					}
					.into());
				}
				let qey = cache::tx::Lookup::TbByName(ns, db, tb);
				match self.cache.get(&qey) {
					// The entry is in the cache
					Some(val) => val.try_into_type(),
					// The entry is not in the cache
					None => {
						let Some(db_def) = self.get_db_by_name(ns, db, None).await? else {
							return Err(anyhow::anyhow!(Error::DbNotFound {
								name: db.to_owned(),
							}));
						};

						let table_key = crate::key::database::tb::new(
							db_def.namespace_id,
							db_def.database_id,
							tb,
						);
						if let Some(tb_def) = self.get(&table_key, None).await? {
							let cached_tb = Arc::new(tb_def);
							let cached_entry = cache::tx::Entry::Any(
								Arc::clone(&cached_tb) as Arc<dyn Any + Send + Sync>
							);
							self.cache.insert(qey, cached_entry);
							return Ok(cached_tb);
						}

						if db_def.strict {
							return Err(Error::TbNotFound {
								name: tb.to_owned(),
							}
							.into());
						}

						let tb_def = TableDefinition::new(
							db_def.namespace_id,
							db_def.database_id,
							self.get_next_tb_id(ctx, db_def.namespace_id, db_def.database_id)
								.await?,
							tb.clone(),
						);
						self.put_tb(ns, db, &tb_def).await
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_or_add_tb")),
		)
	}

	fn get_tb_by_name<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<TableDefinition>>>> {
		Box::pin(async move {
			if version.is_some() {
				let Some(db) = self.get_db_by_name(ns, db, version).await? else {
					return Ok(None);
				};
				let key = crate::key::database::tb::new(db.namespace_id, db.database_id, tb);
				let Some(tb) = self.get(&key, version).await? else {
					return Ok(None);
				};
				return Ok(Some(Arc::new(tb)));
			}
			let qey = cache::tx::Lookup::TbByName(ns, db, tb);
			match self.cache.get(&qey) {
				Some(val) => val.try_into_type().map(Some),
				None => {
					let Some(db) = self.get_db_by_name(ns, db, None).await? else {
						return Ok(None);
					};

					let key = crate::key::database::tb::new(db.namespace_id, db.database_id, tb);
					let Some(tb) = self.get(&key, None).await? else {
						return Ok(None);
					};

					let tb = Arc::new(tb);
					let entr = cache::tx::Entry::Any(tb.clone());
					self.cache.insert(qey, entr);
					Ok(Some(tb))
				}
			}
		})
	}

	fn put_tb<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableDefinition,
	) -> BoxProviderFut<'a, Result<Arc<TableDefinition>>> {
		Box::pin(async move {
			let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
			match self.set(&key, tb).await {
				Ok(_) => {}
				Err(e) => {
					if matches!(
						e.downcast_ref(),
						Some(Error::Kvs(crate::kvs::Error::TransactionReadonly))
					) {
						return Err(Error::TbNotFound {
							name: tb.name.clone(),
						}
						.into());
					}
					return Err(e);
				}
			}

			// Invalidate the cached list of all tables for this database
			let list_key = cache::tx::Lookup::Tbs(tb.namespace_id, tb.database_id);
			self.cache.remove(&list_key);

			// Populate cache
			let cached_tb = Arc::new(tb.clone());
			let cached_entry =
				cache::tx::Entry::Any(Arc::clone(&cached_tb) as Arc<dyn Any + Send + Sync>);

			let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, &tb.name);
			self.cache.insert(qey, cached_entry.clone());

			let qey = cache::tx::Lookup::TbByName(ns, db, &tb.name);
			self.cache.insert(qey, cached_entry);

			Ok(cached_tb)
		})
	}

	fn del_tb<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let Some(tb) = self.get_tb_by_name(ns, db, tb, None).await? else {
				return Err(Error::TbNotFound {
					name: tb.clone(),
				}
				.into());
			};

			let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
			self.del(&key).await?;

			// Invalidate the cached list of all tables for this database
			let list_key = cache::tx::Lookup::Tbs(tb.namespace_id, tb.database_id);
			self.cache.remove(&list_key);

			// Clear the cache
			let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, &tb.name);
			self.cache.remove(&qey);
			let qey = cache::tx::Lookup::TbByName(ns, db, &tb.name);
			self.cache.remove(&qey);

			Ok(())
		})
	}

	fn clr_tb<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let Some(tb) = self.get_tb_by_name(ns, db, tb, None).await? else {
				return Err(Error::TbNotFound {
					name: tb.clone(),
				}
				.into());
			};

			let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
			self.clr(&key).await?;

			// Invalidate the cached list of all tables for this database
			let list_key = cache::tx::Lookup::Tbs(tb.namespace_id, tb.database_id);
			self.cache.remove(&list_key);

			// Clear the cache
			let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, &tb.name);
			self.cache.remove(&qey);
			let qey = cache::tx::Lookup::TbByName(ns, db, &tb.name);
			self.cache.remove(&qey);

			Ok(())
		})
	}

	/// Retrieve all event definitions for a specific table.
	fn all_tb_events<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::EventDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::table::ev::prefix(ns, db, tb)?;
					let end = crate::key::table::ev::suffix(ns, db, tb)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Evs(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_evs(),
					None => {
						let beg = crate::key::table::ev::prefix(ns, db, tb)?;
						let end = crate::key::table::ev::suffix(ns, db, tb)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Evs(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_events")),
		)
	}

	/// Retrieve all field definitions for a specific table.
	fn all_tb_fields<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::FieldDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::table::fd::prefix(ns, db, tb)?;
					let end = crate::key::table::fd::suffix(ns, db, tb)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Fds(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_fds(),
					None => {
						let beg = crate::key::table::fd::prefix(ns, db, tb)?;
						let end = crate::key::table::fd::suffix(ns, db, tb)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Fds(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_fields")),
		)
	}

	/// Retrieve all index definitions for a specific table.
	fn all_tb_indexes<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::IndexDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = table_ix::prefix(ns, db, tb)?;
					let end = table_ix::suffix(ns, db, tb)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Ixs(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_ixs(),
					None => {
						let beg = table_ix::prefix(ns, db, tb)?;
						let end = table_ix::suffix(ns, db, tb)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Ixs(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_indexes")),
		)
	}

	/// Retrieve all live definitions for a specific table.
	fn all_tb_lives<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::SubscriptionDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::table::lq::prefix(ns, db, tb)?;
					let end = crate::key::table::lq::suffix(ns, db, tb)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Lvs(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_lvs(),
					None => {
						let beg = crate::key::table::lq::prefix(ns, db, tb)?;
						let end = crate::key::table::lq::suffix(ns, db, tb)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Lvs(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_lives")),
		)
	}

	/// Retrieve a specific table definition.
	fn get_tb<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<TableDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::tb::new(ns, db, tb);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Tb(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::tb::new(ns, db, tb);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_tb")),
		)
	}

	/// Retrieve an event for a table.
	fn get_tb_event<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ev: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::EventDefinition>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::table::ev::new(ns, db, tb, ev);
					let val = self.get(&key, version).await?.ok_or_else(|| Error::EvNotFound {
						name: ev.to_owned(),
					})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Ev(ns, db, tb, ev);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::table::ev::new(ns, db, tb, ev);
						let val = self.get(&key, None).await?.ok_or_else(|| Error::EvNotFound {
							name: ev.to_owned(),
						})?;
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_tb_event")),
		)
	}

	/// Retrieve a field for a table.
	fn get_tb_field<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		fd: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::FieldDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::table::fd::new(ns, db, tb, fd);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Fd(ns, db, tb, fd);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::table::fd::new(ns, db, tb, fd);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_tb_field")),
		)
	}

	fn put_tb_field<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		fd: &'a catalog::FieldDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let name = fd.name.to_raw_string();
			let key = crate::key::table::fd::new(ns, db, tb, &name);
			self.set(&key, fd).await?;

			// Invalidate the cached list of all fields for this table
			let list_key = cache::tx::Lookup::Fds(ns, db, tb.as_ref());
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Fd(ns, db, tb, &name);
			let entry = cache::tx::Entry::Any(Arc::new(fd.clone()));
			self.cache.insert(qey, entry);
			Ok(())
		})
	}

	/// Retrieve an index for a table.
	fn get_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::IndexDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = table_ix::new(ns, db, tb, ix);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ix(ns, db, tb, ix);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = table_ix::new(ns, db, tb, ix);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_tb_index")),
		)
	}

	fn get_tb_index_by_id<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::IndexDefinition>>>> {
		Box::pin(async move {
			let key = table_ix::IndexNameLookupKey::new(ns, db, tb, ix);
			let Some(index_name) = self.get(&key, version).await? else {
				return Ok(None);
			};

			self.get_tb_index(ns, db, tb, &index_name, version).await
		})
	}

	fn put_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a catalog::IndexDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = table_ix::new(ns, db, tb, &ix.name);
			self.set(&key, ix).await?;

			let name_lookup_key = table_ix::IndexNameLookupKey::new(ns, db, tb, ix.index_id);
			self.set(&name_lookup_key, &ix.name.to_string()).await?;

			// Invalidate the cached list of all indexes for this table
			let list_key = cache::tx::Lookup::Ixs(ns, db, tb.as_ref());
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Ix(ns, db, tb, &ix.name);
			let entry = cache::tx::Entry::Any(Arc::new(ix.clone()));
			self.cache.insert(qey, entry);
			Ok(())
		})
	}

	fn del_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a str,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			// Get the index definition
			let Some(ix) = self.get_tb_index(ns, db, tb, ix, None).await? else {
				return Ok(());
			};

			// Remove the index data
			let key = index_all::new(ns, db, tb, ix.index_id);
			self.delp(&key).await?;

			// Delete the definition
			let key = table_ix::new(ns, db, tb, &ix.name);
			self.del(&key).await?;

			// Delete the id-to-name lookup
			let name_lookup_key = table_ix::IndexNameLookupKey::new(ns, db, tb, ix.index_id);
			self.del(&name_lookup_key).await?;

			// Invalidate the cached list of all indexes for this table
			let list_key = cache::tx::Lookup::Ixs(ns, db, tb.as_ref());
			self.cache.remove(&list_key);

			// Invalidate the cached index entry
			let index_key = cache::tx::Lookup::Ix(ns, db, tb.as_ref(), &ix.name);
			self.cache.remove(&index_key);

			Ok(())
		})
	}

	/// Fetch a specific record value.
	///
	/// This function will return a new default initialized record if non exists.
	fn get_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<Record>>> {
		Box::pin(
			async move {
				// Cache is not versioned
				if version.is_some() {
					// Fetch the record from the datastore. `tx.get` decodes
					// using the storage key, so the canonical `id` is
					// spliced back in automatically (see
					// `RecordKey::value_context`).
					let key = crate::key::record::new(ns, db, tb, id);
					match self.get(&key, version).await? {
						Some(record) => Ok(record.into_read_only()),
						None => Ok(Arc::new(Default::default())),
					}
				} else {
					let qey = cache::tx::Lookup::Record(ns, db, tb, id);
					match self.cache.get(&qey) {
						// The entry is in the cache
						Some(val) => val.try_into_record(),
						// The entry is not in the cache
						None => {
							let key = crate::key::record::new(ns, db, tb, id);
							match self.get(&key, None).await? {
								Some(record) => {
									let record = record.into_read_only();
									let entry = cache::tx::Entry::Val(Arc::clone(&record));
									self.cache.insert(qey, entry);
									Ok(record)
								}
								None => Ok(Arc::new(Default::default())),
							}
						}
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_record")),
		)
	}

	fn get_records<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		rids: &'a [RecordId],
		version: Option<u64>,
		cache_policy: CachePolicy,
	) -> BoxProviderFut<'a, Result<Vec<Arc<Record>>>> {
		Box::pin(
			async move {
				// Nothing to fetch
				if rids.is_empty() {
					return Ok(Vec::new());
				}
				// Cache is not versioned
				if version.is_some() {
					// `tx.getm` decodes each value with its own key's
					// context (`RecordKey::value_context`), so the
					// canonical `id` is spliced into the decoded record
					// automatically.
					let keys: Vec<crate::key::record::RecordKey<'_>> = rids
						.iter()
						.map(|rid| crate::key::record::new(ns, db, &rid.table, &rid.key))
						.collect();
					let values = self.getm(keys, version).await?;
					let out: Vec<Arc<Record>> = values
						.into_iter()
						.map(|opt| match opt {
							Some(record) => record.into_read_only(),
							None => Arc::new(Default::default()),
						})
						.collect();
					return Ok(out);
				}
				// Phase 1: check cache, collect hits and indices of misses
				let mut out: Vec<Option<Arc<Record>>> = vec![None; rids.len()];
				let mut uncached_rids: Vec<(usize, &RecordId)> = Vec::new();
				for (i, rid) in rids.iter().enumerate() {
					let qey = cache::tx::Lookup::Record(ns, db, rid.table.as_str(), &rid.key);
					match self.cache.get(&qey) {
						// The entry is in the cache
						Some(entry) => out[i] = Some(entry.try_into_record()?),
						// The entry is not in the cache
						None => uncached_rids.push((i, rid)),
					}
				}
				// Phase 2: batch fetch the uncached keys from the datastore
				if !uncached_rids.is_empty() {
					let keys: Vec<crate::key::record::RecordKey<'_>> = uncached_rids
						.iter()
						.map(|(_, rid)| crate::key::record::new(ns, db, &rid.table, &rid.key))
						.collect();
					let values = self.getm(keys, None).await?;
					// Phase 3: populate cache + merge into output
					for ((i, rid), opt) in uncached_rids.into_iter().zip(values) {
						let record = match opt {
							Some(record) => {
								let record = record.into_read_only();
								// Only populate the cache when the caller requests
								// ReadWrite; ReadOnly avoids eviction churn during
								// large sequential scans.
								if matches!(cache_policy, CachePolicy::ReadWrite) {
									let qey = cache::tx::Lookup::Record(
										ns,
										db,
										rid.table.as_str(),
										&rid.key,
									);
									let entry = cache::tx::Entry::Val(Arc::clone(&record));
									self.cache.insert(qey, entry);
								}
								record
							}
							None => Arc::new(Default::default()),
						};
						out[i] = Some(record);
					}
				}
				// Every slot should be populated by now
				out.into_iter()
					.map(|o| {
						o.ok_or_else(|| {
							Error::Internal("missing record in multi-get batch".into()).into()
						})
					})
					.collect()
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_records")),
		)
	}

	fn record_exists<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<bool>> {
		Box::pin(async move {
			let key = crate::key::record::new(ns, db, tb, id);
			self.exists(&key, version).await
		})
	}

	fn put_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		record: Arc<Record>,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(
			async move {
				let key = crate::key::record::new(ns, db, tb, id);
				self.put(&key, record.as_ref()).await?;
				// Set the value in the cache
				let qey = cache::tx::Lookup::Record(ns, db, tb, id);
				self.cache.insert(qey, cache::tx::Entry::Val(record));
				// Return nothing
				Ok(())
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "put_record")),
		)
	}

	fn set_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		record: Arc<Record>,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(
			async move {
				// Set the value in the datastore
				let key = crate::key::record::new(ns, db, tb, id);
				self.set(&key, record.as_ref()).await?;
				// Clear the value from the cache
				let qey = cache::tx::Lookup::Record(ns, db, tb, id);
				self.cache.remove(&qey);
				// Return nothing
				Ok(())
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "set_record")),
		)
	}

	fn del_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(
			async move {
				// Delete the value in the datastore
				let key = crate::key::record::new(ns, db, tb, id);
				self.del(&key).await?;
				// Clear the value from the cache
				let qey = cache::tx::Lookup::Record(ns, db, tb, id);
				self.cache.remove(&qey);
				// Return nothing
				Ok(())
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "del_record")),
		)
	}

	fn get_next_tb_id<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: NamespaceId,
		db: DatabaseId,
	) -> BoxProviderFut<'a, Result<TableId>> {
		Box::pin(async move { self.sequences.next_table_id(ctx, ns, db).await })
	}
}

// --------------------------------------------------
// User implementation functions
// --------------------------------------------------

impl UserProvider for Transaction {
	/// Retrieve all ROOT level users in a datastore.
	fn all_root_users(
		&self,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::UserDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::root::us::prefix();
					let end = crate::key::root::us::suffix();
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Rus;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_rus(),
					None => {
						let beg = crate::key::root::us::prefix();
						let end = crate::key::root::us::suffix();
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Rus(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_root_users")),
		)
	}

	/// Retrieve all namespace user definitions for a specific namespace.
	fn all_ns_users(
		&self,
		ns: NamespaceId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::UserDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::namespace::us::prefix(ns)?;
					let end = crate::key::namespace::us::suffix(ns)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Nus(ns);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nus(),
					None => {
						let beg = crate::key::namespace::us::prefix(ns)?;
						let end = crate::key::namespace::us::suffix(ns)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Nus(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_ns_users")),
		)
	}

	/// Retrieve all database user definitions for a specific database.
	fn all_db_users(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::UserDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::us::prefix(ns, db)?;
					let end = crate::key::database::us::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Dus(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_dus(),
					None => {
						let beg = crate::key::database::us::prefix(ns, db)?;
						let end = crate::key::database::us::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Dus(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_users")),
		)
	}

	/// Retrieve a specific root user definition.
	fn get_root_user<'a>(
		&'a self,
		us: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::UserDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::root::us::new(us);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ru(us);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::root::us::new(us);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_root_user")),
		)
	}

	/// Retrieve a specific namespace user definition.
	fn get_ns_user<'a>(
		&'a self,
		ns: NamespaceId,
		us: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::UserDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::namespace::us::new(ns, us);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Nu(ns, us);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::namespace::us::new(ns, us);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};

						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_ns_user")),
		)
	}

	/// Retrieve a specific user definition from a database.
	fn get_db_user<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::UserDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::us::new(ns, db, us);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Du(ns, db, us);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::us::new(ns, db, us);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};

						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_user")),
		)
	}

	fn put_root_user<'a>(
		&'a self,
		us: &'a catalog::UserDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = crate::key::root::us::new(&us.name);
			self.set(&key, us).await?;

			// Invalidate the cached list of all root users
			let list_key = cache::tx::Lookup::Rus;
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Ru(&us.name);
			let entry = cache::tx::Entry::Any(Arc::new(us.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}

	fn put_ns_user<'a>(
		&'a self,
		ns: NamespaceId,
		us: &'a catalog::UserDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = crate::key::namespace::us::new(ns, &us.name);
			self.set(&key, us).await?;

			// Invalidate the cached list of all namespace users
			let list_key = cache::tx::Lookup::Nus(ns);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Nu(ns, &us.name);
			let entry = cache::tx::Entry::Any(Arc::new(us.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}

	fn put_db_user<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &'a catalog::UserDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = crate::key::database::us::new(ns, db, &us.name);
			self.set(&key, us).await?;

			// Invalidate the cached list of all database users
			let list_key = cache::tx::Lookup::Dus(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Du(ns, db, &us.name);
			let entry = cache::tx::Entry::Any(Arc::new(us.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}
}

// --------------------------------------------------
// Authorisation implementation functions
// --------------------------------------------------

impl AuthorisationProvider for Transaction {
	/// Retrieve all ROOT level accesses in a datastore.
	fn all_root_accesses(
		&self,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AccessDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::root::ac::prefix();
					let end = crate::key::root::ac::suffix();
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Ras;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_ras(),
					None => {
						let beg = crate::key::root::ac::prefix();
						let end = crate::key::root::ac::suffix();
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Ras(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_root_accesses")),
		)
	}

	/// Retrieve all root access grants in a datastore.
	fn all_root_access_grants<'a>(
		&'a self,
		ra: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::AccessGrant]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::root::access::gr::prefix(ra)?;
					let end = crate::key::root::access::gr::suffix(ra)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Rgs(ra);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_rag(),
					None => {
						let beg = crate::key::root::access::gr::prefix(ra)?;
						let end = crate::key::root::access::gr::suffix(ra)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Rag(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_root_access_grants")),
		)
	}

	/// Retrieve all namespace access definitions for a specific namespace.
	fn all_ns_accesses(
		&self,
		ns: NamespaceId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AccessDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::namespace::ac::prefix(ns)?;
					let end = crate::key::namespace::ac::suffix(ns)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Nas(ns);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nas(),
					None => {
						let beg = crate::key::namespace::ac::prefix(ns)?;
						let end = crate::key::namespace::ac::suffix(ns)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Nas(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_ns_accesses")),
		)
	}

	/// Retrieve all namespace access grants for a specific namespace.
	fn all_ns_access_grants<'a>(
		&'a self,
		ns: NamespaceId,
		na: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::AccessGrant]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::namespace::access::gr::prefix(ns, na)?;
					let end = crate::key::namespace::access::gr::suffix(ns, na)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Ngs(ns, na);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nag(),
					None => {
						let beg = crate::key::namespace::access::gr::prefix(ns, na)?;
						let end = crate::key::namespace::access::gr::suffix(ns, na)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Nag(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_ns_access_grants")),
		)
	}

	/// Retrieve all database access definitions for a specific database.
	fn all_db_accesses(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AccessDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::ac::prefix(ns, db)?;
					let end = crate::key::database::ac::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Das(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_das(),
					None => {
						let beg = crate::key::database::ac::prefix(ns, db)?;
						let end = crate::key::database::ac::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Das(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_accesses")),
		)
	}

	/// Retrieve all database access grants for a specific database.
	fn all_db_access_grants<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::AccessGrant]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::access::gr::prefix(ns, db, da)?;
					let end = crate::key::database::access::gr::suffix(ns, db, da)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Dgs(ns, db, da);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_dag(),
					None => {
						let beg = crate::key::database::access::gr::prefix(ns, db, da)?;
						let end = crate::key::database::access::gr::suffix(ns, db, da)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Dag(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_access_grants")),
		)
	}

	/// Retrieve a specific root access definition.
	fn get_root_access<'a>(
		&'a self,
		ra: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::root::ac::new(ra);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ra(ra);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::root::ac::new(ra);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_root_access")),
		)
	}

	/// Retrieve a specific root access grant.
	fn get_root_access_grant<'a>(
		&'a self,
		ac: &'a str,
		gr: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessGrant>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::root::access::gr::new(ac, gr);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Rg(ac, gr);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::root::access::gr::new(ac, gr);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_root_access_grant")),
		)
	}

	/// Retrieve a specific namespace access definition.
	fn get_ns_access<'a>(
		&'a self,
		ns: NamespaceId,
		na: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::namespace::ac::new(ns, na);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Na(ns, na);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::namespace::ac::new(ns, na);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_ns_access")),
		)
	}

	/// Retrieve a specific namespace access grant.
	fn get_ns_access_grant<'a>(
		&'a self,
		ns: NamespaceId,
		ac: &'a str,
		gr: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessGrant>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::namespace::access::gr::new(ns, ac, gr);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ng(ns, ac, gr);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::namespace::access::gr::new(ns, ac, gr);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_ns_access_grant")),
		)
	}

	/// Retrieve a specific database access definition.
	fn get_db_access<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::ac::new(ns, db, da);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Da(ns, db, da);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::ac::new(ns, db, da);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_access")),
		)
	}

	/// Retrieve a specific database access grant.
	fn get_db_access_grant<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ac: &'a str,
		gr: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessGrant>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::access::gr::new(ns, db, ac, gr);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Dg(ns, db, ac, gr);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::access::gr::new(ns, db, ac, gr);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_access_grant")),
		)
	}

	fn del_root_access<'a>(&'a self, ra: &'a str) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			// Delete the definition
			let key = crate::key::root::ac::new(ra);
			self.del(&key).await?;
			// Delete any associated data including access grants.
			let key = crate::key::root::access::all::new(ra);
			self.delp(&key).await?;

			// Invalidate the cached list of all root accesses
			let list_key = cache::tx::Lookup::Ras;
			self.cache.remove(&list_key);

			// Invalidate the cached access entry and grants
			let access_key = cache::tx::Lookup::Ra(ra);
			self.cache.remove(&access_key);
			let grants_key = cache::tx::Lookup::Rgs(ra);
			self.cache.remove(&grants_key);

			Ok(())
		})
	}

	fn del_ns_access<'a>(&'a self, ns: NamespaceId, na: &'a str) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			// Delete the definition
			let key = crate::key::namespace::ac::new(ns, na);
			self.del(&key).await?;
			// Delete any associated data including access grants.
			let key = crate::key::namespace::access::all::new(ns, na);
			self.delp(&key).await?;

			// Invalidate the cached list of all namespace accesses
			let list_key = cache::tx::Lookup::Nas(ns);
			self.cache.remove(&list_key);

			// Invalidate the cached access entry and grants
			let access_key = cache::tx::Lookup::Na(ns, na);
			self.cache.remove(&access_key);
			let grants_key = cache::tx::Lookup::Ngs(ns, na);
			self.cache.remove(&grants_key);

			Ok(())
		})
	}

	fn del_db_access<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &'a str,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			// Delete the definition
			let key = crate::key::database::ac::new(ns, db, da);
			self.del(&key).await?;
			// Delete any associated data including access grants.
			let key = crate::key::database::access::all::new(ns, db, da);
			self.delp(&key).await?;

			// Invalidate the cached list of all database accesses
			let list_key = cache::tx::Lookup::Das(ns, db);
			self.cache.remove(&list_key);

			// Invalidate the cached access entry and grants
			let access_key = cache::tx::Lookup::Da(ns, db, da);
			self.cache.remove(&access_key);
			let grants_key = cache::tx::Lookup::Dgs(ns, db, da);
			self.cache.remove(&grants_key);

			Ok(())
		})
	}
}

// --------------------------------------------------
// API implementation functions
// --------------------------------------------------

impl ApiProvider for Transaction {
	/// Retrieve all api definitions for a specific database.
	fn all_db_apis(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[ApiDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::ap::prefix(ns, db)?;
					let end = crate::key::database::ap::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Aps(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val,
					None => {
						let beg = crate::key::database::ap::prefix(ns, db)?;
						let end = crate::key::database::ap::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let val = cache::tx::Entry::Aps(Arc::clone(&val));
						self.cache.insert(qey, val.clone());
						val
					}
				}
				.try_into_aps()
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_apis")),
		)
	}

	/// Retrieve a specific api definition.
	fn get_db_api<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<ApiDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::ap::new(ns, db, ap);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ap(ns, db, ap);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::ap::new(ns, db, ap);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(val);
						let entry = cache::tx::Entry::Any(val.clone());
						self.cache.insert(qey, entry);
						Ok(Some(val))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_api")),
		)
	}

	fn put_db_api<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &'a catalog::ApiDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let name = ap.path.to_string();
			let key = crate::key::database::ap::new(ns, db, &name);
			self.set(&key, ap).await?;

			// Invalidate the cached list of all APIs for this database
			let list_key = cache::tx::Lookup::Aps(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Ap(ns, db, &name);
			let entry = cache::tx::Entry::Any(Arc::new(ap.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}
}

// --------------------------------------------------
// Bucket implementation functions
// --------------------------------------------------

impl BucketProvider for Transaction {
	/// Retrieve all bucket definitions for a specific database.
	fn all_db_buckets(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::BucketDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let beg = crate::key::database::bu::prefix(ns, db)?;
					let end = crate::key::database::bu::suffix(ns, db)?;
					let val = self.getr(beg..end, version).await?;
					return util::deserialize_cache(val.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Bus(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_bus(),
					None => {
						let beg = crate::key::database::bu::prefix(ns, db)?;
						let end = crate::key::database::bu::suffix(ns, db)?;
						let val = self.getr(beg..end, None).await?;
						let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Bus(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_buckets")),
		)
	}

	/// Retrieve a specific bucket definition from a database.
	fn get_db_bucket<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::BucketDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = crate::key::database::bu::new(ns, db, bu);
					let Some(val) = self.get(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Bu(ns, db, bu);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::bu::new(ns, db, bu);
						let Some(val) = self.get(&key, None).await? else {
							return Ok(None);
						};
						let bucket_def = Arc::new(val);
						let entr = cache::tx::Entry::Any(bucket_def.clone());
						self.cache.insert(qey, entr);
						Ok(Some(bucket_def))
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_bucket")),
		)
	}
}

// --------------------------------------------------
// Catalog provider
// --------------------------------------------------

impl CatalogProvider for Transaction {}
